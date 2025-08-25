from io import StringIO
from pathlib import Path
from typing import Any

import pytest
import requests_cache

import intake_esgf
from intake_esgf.base import download_and_verify
from intake_esgf.exceptions import DatasetInitError, StalledDownload


def test_set_requests_cache():
    """Test temporary altering requests_cache settings."""
    default_expire_after = 3600  # 1 hour
    temporary_expire_after = "DO_NOT_CACHE"

    assert intake_esgf.conf["requests_cache"]["expire_after"] == default_expire_after

    with intake_esgf.conf.set(
        requests_cache={
            "expire_after": temporary_expire_after,
        }
    ):
        assert (
            intake_esgf.conf["requests_cache"]["expire_after"] == temporary_expire_after
        )

    assert intake_esgf.conf["requests_cache"]["expire_after"] == default_expire_after


@pytest.mark.parametrize(
    "expire_after",
    [
        "DO_NOT_CACHE",
        "EXPIRE_IMMEDIATELY",
        "NEVER_EXPIRE",
    ],
)
def test_special_cache_expire_after(expire_after, tmp_path):
    """Test disabling the requests_cache."""
    with intake_esgf.conf.set(
        requests_cache={
            "cache_name": str(tmp_path / "requests-cache.sqlite"),
            "expire_after": expire_after,
        }
    ):
        cat = intake_esgf.ESGFCatalog()
        for ind in cat.indices:
            session = ind.session
            assert isinstance(session, requests_cache.CachedSession)
            assert session.expire_after == getattr(requests_cache, expire_after)


def test_set_indices():
    # All off
    intake_esgf.conf.set(no_indices=True)
    num_on = sum(
        [
            enabled
            for index_type in ["globus_indices", "solr_indices", "stac_indices"]
            for _, enabled in intake_esgf.conf[index_type].items()
        ]
    )
    assert num_on == 0
    # All on, doesn't affect STAC at the moment
    intake_esgf.conf.set(all_indices=True)
    num_off = sum(
        [
            not enabled
            for index_type in ["globus_indices", "solr_indices"]
            for _, enabled in intake_esgf.conf[index_type].items()
        ]
    )
    assert num_off == 0
    # All back off and then enable one
    intake_esgf.conf.set(no_indices=True, indices={"ESGF2-US-1.5-Catalog": True})
    num_on = sum(
        [
            enabled
            for index_type in ["globus_indices", "solr_indices", "stac_indices"]
            for _, enabled in intake_esgf.conf[index_type].items()
        ]
    )
    assert num_on == 1


def test_confirm(monkeypatch):
    with intake_esgf.conf.set(confirm_download=True):
        cat = intake_esgf.ESGFCatalog().search(
            experiment_id="historical",
            source_id="CanESM5",
            variable_id=["areacella"],
            variant_label=["r2i1p1f1"],
        )
        # Cancel the download
        monkeypatch.setattr("sys.stdin", StringIO("N\n"))
        ds = cat.to_path_dict()
        assert len(ds) == 0
        # Enable the download
        monkeypatch.setattr("sys.stdin", StringIO("Y\n"))
        ds = cat.to_path_dict()
        assert len(ds) == 1


def test_break_on_error(monkeypatch):
    def fake_open_dataset(filename: Any):
        raise ValueError("Just needs to fail")

    monkeypatch.setattr("xarray.open_dataset", fake_open_dataset)
    monkeypatch.setattr("xarray.open_mfdataset", fake_open_dataset)
    with pytest.raises(DatasetInitError):
        with intake_esgf.conf.set(break_on_error=True):
            cat = intake_esgf.ESGFCatalog().search(
                experiment_id="historical",
                source_id="CanESM5",
                variable_id=["areacella"],
                variant_label=["r2i1p1f1"],
            )
            cat.to_dataset_dict()
    with intake_esgf.conf.set(break_on_error=False):
        cat = intake_esgf.ESGFCatalog().search(
            experiment_id="historical",
            source_id="CanESM5",
            variable_id=["areacella"],
            variant_label=["r2i1p1f1"],
        )
        dsd = cat.to_dataset_dict()
        assert len(dsd) == 0


def test_additional_df_cols():
    extra = ["datetime_start", "datetime_stop"]
    with intake_esgf.conf.set(additional_df_cols=extra):
        cat = intake_esgf.ESGFCatalog().search(
            experiment_id="historical",
            source_id="CanESM5",
            variable_id=["gpp"],
            variant_label=["r1i1p1f1"],
        )
        assert all([col in cat.df.columns for col in extra])


def test_slow_cancel(tmp_path):
    intake_esgf.conf.set(slow_download_threshold=100)  # unreasonably fast speed
    with pytest.raises(StalledDownload):
        download_and_verify(
            url="https://esgf-node.ornl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MIROC/MIROC-ES2L/ssp245/r5i1p1f2/Amon/hus/gn/v20201222/hus_Amon_MIROC-ES2L_ssp245_r5i1p1f2_gn_201501-210012.nc",
            local_file=tmp_path / Path("hus.nc"),
            hash=None,
            hash_algorithm=None,
            content_length=100,
            download_db=Path(intake_esgf.conf["download_db"]).expanduser(),
            logger=intake_esgf.conf.get_logger(),
            break_slow_downloads=True,
        )
