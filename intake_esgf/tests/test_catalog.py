import warnings
from pathlib import Path
from typing import Any

import pytest
import xarray as xr

import intake_esgf
import intake_esgf.base as base
from intake_esgf.catalog import _init_dataset, _simplify_stream_links
from intake_esgf.database import log_download_information
from intake_esgf.exceptions import DatasetLoadError
from intake_esgf.projects import CMIP6


def test_init():
    with intake_esgf.conf.set(no_indices=True):
        with pytest.raises(ValueError):
            intake_esgf.ESGFCatalog()


def test_set_project(df_catalog):
    cat = intake_esgf.ESGFCatalog()
    # no search has been done
    with pytest.raises(ValueError):
        cat._set_project()
    # fake a CMIP6 search
    cat.df = df_catalog
    cat._set_project()
    assert isinstance(cat.project, CMIP6)
    # fake a mixed project search
    cat.df.loc[cat.df.index[-1], "project"] = "CMIP5"
    with pytest.raises(ValueError):
        cat._set_project()


def test_minimal_key_format(catalog):
    assert set(catalog._minimal_key_format()) == set(["table_id", "variable_id"])
    assert set(catalog._minimal_key_format(ignore_facets="table_id")) == set(
        ["variable_id"]
    )


def test_clone(catalog):
    clone = catalog.clone()
    assert len(catalog.indices) == len(clone.indices)
    assert clone.df is None


def test_unique(catalog):
    df = catalog.unique()
    for col, values in df.items():
        if col in ["table_id", "variable_id"]:
            assert len(values) == 2
        else:
            assert len(values) == 1


def test_model_groups(catalog):
    # light testing here, more detailed for each project
    grps = catalog.model_groups()
    assert len(grps) == 1
    assert grps.iloc[0] == 2


# def test_search():
#    pass

# def test_from_tracking_ids():
#    pass

# def test_get_file_info():
#    pass

# def test_to_path_dict():
#    pass

# def test_to_dataset_dict():
#    pass

# def test_remove_incomplete():
#    pass

# def test_remove_ensembles():
#    pass


def test_session_log(catalog):
    log = catalog.session_log()
    assert log == ""


def test_download_summary(catalog):
    log_download_information(catalog.download_db, "summary", 999.0, 1334.0)
    print(catalog.download_summary())
    assert "summary" in catalog.download_summary().index


# def test_variable_info():
#    pass


def _partial_download_catalog(
    df_catalog, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> intake_esgf.ESGFCatalog:
    """
    A catalog of 2 datasets where file info is faked and downloads are mocked.

    The tas dataset has 3 files, of which one will fail to download. The gpp dataset
    has a single file which downloads fine.
    """
    intake_esgf.conf.set(local_cache=[str(tmp_path)])
    cat = intake_esgf.ESGFCatalog()
    cat.df = df_catalog
    cat._set_project()
    cat.df["key"] = cat.df.apply(
        lambda row: ".".join([str(row[f]) for f in cat.project.master_id_facets()]),
        axis=1,
    )
    tas_key, gpp_key = cat.df["key"]
    root = "CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1"

    def _info(key: str, path: str) -> dict[str, Any]:
        return {
            "key": key,
            "path": Path(path),
            "HTTPServer": [f"https://example.org/{path}"],
            "checksum": "",
            "checksum_type": "",
            "size": 0,
        }

    infos = [
        _info(tas_key, f"{root}/Amon/tas/gn/v20190429/tas_185001-189912.nc"),
        _info(tas_key, f"{root}/Amon/tas/gn/v20190429/tas_190001-194912.nc"),
        _info(tas_key, f"{root}/Amon/tas/gn/v20190429/tas_195001-201412.nc"),
        _info(gpp_key, f"{root}/Lmon/gpp/gn/v20190429/gpp_185001-201412.nc"),
    ]
    monkeypatch.setattr(cat, "_get_file_info", lambda **kwargs: infos)

    def fake_download_and_verify(url: str, local_file: Path, *args, **kwargs):
        if "190001-194912" in url:
            raise ValueError("Hash does not match")
        local_file.parent.mkdir(parents=True, exist_ok=True)
        local_file.write_bytes(b"data")

    monkeypatch.setattr(base, "download_and_verify", fake_download_and_verify)
    return cat


def test_partial_download_warns(df_catalog, tmp_path, monkeypatch):
    cat = _partial_download_catalog(df_catalog, tmp_path, monkeypatch)
    with intake_esgf.conf.set(break_on_error=False):
        with pytest.warns(UserWarning, match="tas_190001-194912.nc"):
            dsd = cat.to_path_dict(minimal_keys=False, quiet=True)
    # partial data is still returned, but the missing file is reported
    tas_key, gpp_key = cat.df["key"]
    assert sorted(Path(p).name for p in dsd[tas_key]) == [
        "tas_185001-189912.nc",
        "tas_195001-201412.nc",
    ]
    assert len(dsd[gpp_key]) == 1


def test_partial_download_break_on_error(df_catalog, tmp_path, monkeypatch):
    cat = _partial_download_catalog(df_catalog, tmp_path, monkeypatch)
    tas_key, _ = cat.df["key"]
    with intake_esgf.conf.set(break_on_error=True, print_log_on_error=False):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            with pytest.raises(DatasetLoadError) as exc:
                cat.to_path_dict(minimal_keys=False, quiet=True)
    assert exc.value.problem_keys == [tas_key]
    assert "tas_190001-194912.nc" in str(exc.value)


def test_download_urls_reports_failures(tmp_path, monkeypatch):
    def fake_download_and_verify(url: str, local_file: Path, *args, **kwargs):
        if "bad" in url:
            raise RuntimeError("connection reset")
        local_file.write_bytes(b"data")

    def broken_rate_dataframe(*args, **kwargs):
        raise RuntimeError("unexpected error in a worker thread")

    monkeypatch.setattr(base, "download_and_verify", fake_download_and_verify)
    infos = [
        {
            "key": "a",
            "path": Path("good.nc"),
            "size": 0,
            "checksum": "",
            "checksum_type": "",
            "HTTPServer": ["https://example.org/good.nc"],
        },
        {
            "key": "a",
            "path": Path("bad.nc"),
            "size": 0,
            "checksum": "",
            "checksum_type": "",
            "HTTPServer": ["https://example.org/bad.nc"],
        },
        {
            "key": "b",
            "path": Path("nolinks.nc"),
            "size": 0,
            "checksum": "",
            "checksum_type": "",
        },
    ]
    download_db = Path(intake_esgf.conf["download_db"])
    logger = intake_esgf.conf.get_logger()
    failed = base.download_urls(infos, [tmp_path], download_db, logger, None, True)
    assert sorted(info["path"].name for info in failed) == ["bad.nc", "nolinks.nc"]
    assert (tmp_path / "good.nc").is_file()

    # exceptions raised inside the worker threads must not be swallowed
    (tmp_path / "good.nc").unlink()
    monkeypatch.setattr(base, "get_download_rate_dataframe", broken_rate_dataframe)
    failed = base.download_urls(infos[:1], [tmp_path], download_db, logger, None, True)
    assert [info["path"].name for info in failed] == ["good.nc"]


KWARGS = dict(
    data_vars="minimal",
    coords="minimal",
    compat="override",
    chunks="auto",
)


@pytest.mark.parametrize(
    "conf,kwargs,result",
    [
        (dict(), None, (1,) * 180),
        (dict(), KWARGS, (120, 60)),
        (KWARGS, None, (120, 60)),
    ],
)
def test_open_kwargs(conf, kwargs, result):
    intake_esgf.conf.set(default_open_kwargs=conf)
    cat = intake_esgf.ESGFCatalog()
    cat.search(
        project="CMIP6",
        experiment_id="historical",
        source_id="NorESM2-LM",
        variable_id="tas",
        frequency="mon",
        file_start="2000-01",
    ).remove_ensembles()
    dsd = cat.to_dataset_dict(
        add_measures=False,
        open_kwargs=kwargs,
    )
    assert dsd["tas"]["tas"].chunksizes["time"] == result


@pytest.mark.xfail(reason="only single project at a time")
def test_no_multiple_projects():
    intake_esgf.ESGFCatalog().search(project=["CMIP6", "CMIP7"])


def test_simplify_stream_links(file_info):
    key = file_info["key"]
    for method in ["OPENDAP", "Kerchunk", "CFA"]:
        dsd = _simplify_stream_links([file_info], {key: file_info[method]})
        print(dsd)
        assert len(dsd[key]) == 1


@pytest.mark.parametrize(
    "method",
    ["OPENDAP", "Kerchunk", pytest.param("CFA", marks=pytest.mark.skip(reason="slow"))],
)
def test_init_dataset(method: str):
    methods: dict[str, list[str | Path]] = {
        "OPENDAP": [
            "http://esgf-node.ornl.gov/thredds/dodsC/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/Amon/tas/gn/v20190429/tas_Amon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc"
        ],
        "Kerchunk": [
            "https://dap.ceda.ac.uk/badc/cmip6/metadata/aggregations/kerchunk/CMIP6/ScenarioMIP/MOHC/HadGEM3-GC31-LL/kr1.0/CMIP6.ScenarioMIP.MOHC.HadGEM3-GC31-LL.ssp585.r4i1p1f3.Amon.ua.gn.v20200604.kr1.0.json"
        ],
        "CFA": [
            "https://dap.ceda.ac.uk/badc/cmip6/metadata/aggregations/cfa/CMIP6/ScenarioMIP/MOHC/HadGEM3-GC31-LL/cr1.0/CMIP6.ScenarioMIP.MOHC.HadGEM3-GC31-LL.ssp585.r4i1p1f3.Amon.ua.gn.v20200604.cr1.0.nca"
        ],
    }
    ds = _init_dataset(methods[method])
    assert isinstance(ds, xr.Dataset)
