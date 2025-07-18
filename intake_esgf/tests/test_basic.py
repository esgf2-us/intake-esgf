from pathlib import Path
from time import perf_counter

import pytest
from requests_cache import CachedSession

import intake_esgf
from intake_esgf import ESGFCatalog
from intake_esgf.base import partition_infos
from intake_esgf.exceptions import MissingFileInformation, NoSearchResults


def test_search():
    extra = ["datetime_start", "datetime_stop"]
    with intake_esgf.conf.set(additional_df_cols=extra):
        cat = ESGFCatalog().search(
            experiment_id="historical",
            source_id="CanESM5",
            variable_id=["gpp"],
            variant_label=["r1i1p1f1"],
        )

        # Check that user-configured columns are in the dataframe
        assert all([col in cat.df.columns for col in extra])

        ds = cat.to_dataset_dict()
        assert "gpp" in ds
        assert "sftlf" in ds["gpp"]


@pytest.mark.parametrize(
    "index_type",
    [
        "globus",
        "stac",
        pytest.param("solr", marks=pytest.mark.solr),
    ],
)
def test_search_is_cached(index_type, tmp_path):
    # Use a test specific cache.
    intake_esgf.conf["requests_cache"]["cache_name"] = str(
        tmp_path / "requests-cache.sqlite"
    )
    # Enable only the index type being tested.
    for index_collection in ["globus_indices", "solr_indices", "stac_indices"]:
        for key in intake_esgf.conf[index_collection]:
            intake_esgf.conf[index_collection][key] = (
                index_collection == f"{index_type}_indices"
            )

    facets = {
        "experiment_id": ["historical"],
        "source_id": ["CanESM5"],
        "variable_id": ["tas"],
        "variant_label": ["r1i1p1f1"],
    }

    # Test that the indices are using a cached session.
    for ind in ESGFCatalog().indices:
        assert isinstance(ind.session, CachedSession)

    # Test that a repeat search is faster than the initial one.
    start = perf_counter()
    ESGFCatalog().search(**facets)
    initial_search = perf_counter() - start

    start = perf_counter()
    ESGFCatalog().search(**facets)
    cached_search = perf_counter() - start

    assert cached_search < initial_search


def test_esgroot():
    with intake_esgf.conf.set(esg_dataroot=intake_esgf.conf["local_cache"]):
        cat = ESGFCatalog().search(
            experiment_id="historical",
            source_id="CanESM5",
            variable_id=["gpp"],
            variant_label=["r1i1p1f1"],
        )
        ds = cat.to_dataset_dict(add_measures=False)
        assert "gpp" in ds
        log = cat.session_log()
        assert "download" not in log
        assert f"accessed {cat.esg_dataroot[0]}" in cat.session_log()


def test_noresults():
    cat = ESGFCatalog()
    try:
        cat.search(variable_id="does_not_exist")
    except NoSearchResults:
        pass


def test_tracking_ids():
    cat = ESGFCatalog().from_tracking_ids(
        [
            "hdl:21.14100/0577d84f-9954-494f-8cc8-465aa4fd910e",
            "hdl:21.14100/0972f78b-158e-4c6b-bcdf-7d0d75d7a8cd",
        ]
    )
    assert len(cat.df) == 2


def test_add_cell_measures():
    # these measures are in r1i1p1f2 / piControl
    cat = ESGFCatalog().search(
        variable_id="mrros",
        source_id="UKESM1-0-LL",
        variant_label="r2i1p1f2",
        frequency="mon",
        experiment_id="historical",
    )
    ds = cat.to_dataset_dict()["mrros"]
    assert "sftlf" in ds
    assert "areacella" in ds


def test_download_without_checksum(monkeypatch):
    """Test that downloading a file without a checksum works."""
    _get_file_info = ESGFCatalog._get_file_info

    def _get_stripped_file_info(self, *args, **kwargs):
        """Set the checksum to None in the search result."""
        result = _get_file_info(self, *args, **kwargs)
        for info in result:
            info["checksum"] = None
        return result

    monkeypatch.setattr(ESGFCatalog, "_get_file_info", _get_stripped_file_info)

    cat = ESGFCatalog().search(
        variable_id="areacello",
        source_id="UKESM1-0-LL",
    )

    cat.to_path_dict()


def test_modelgroups():
    cat = ESGFCatalog().search(
        experiment_id="historical",
        source_id=["CanESM5", "GFDL-CM4"],
        variable_id=["tas", "pr"],
        variant_label=["r1i1p1f1", "r2i1p1f1"],
        table_id="day",
    )
    assert len(cat.model_groups()) == 4


def test_remove_ensemble():
    cat = ESGFCatalog().search(
        experiment_id="historical",
        source_id=["UKESM1-0-LL"],
        variable_id=["tas"],
        table_id="Amon",
    )
    assert len(cat.model_groups()) > 1
    cat.remove_ensembles()
    assert len(cat.df) == 1
    assert cat.df.iloc[0]["member_id"] == "r1i1p1f2"


def test_download_dbase():
    cat = ESGFCatalog()
    assert len(cat.download_summary().columns)


def test_variable_info():
    cat = ESGFCatalog()
    df = cat.variable_info("temperature")
    assert df.index.isin(
        [
            "sitemptop",
            "ta",
            "ta850",
            "tas",
            "tasmax",
            "tasmin",
            "thetao",
            "tos",
            "ts",
            "tsl",
        ]
    ).all()


def test_partition_infos():
    # Only https available
    infos = [
        {
            "key": "dataset1",
            "path": Path("file1"),
            "HTTPServer": ["link1", "link2"],
        },
        {
            "key": "dataset1",
            "path": Path("file2"),
            "HTTPServer": ["link1", "link2"],
        },
    ]
    infos_, _ = partition_infos(infos, False, False)
    assert max([len(infos_[p]) for p in ["exist", "stream", "globus"]]) == 0
    assert len(infos_["https"]) == 2
    infos_, _ = partition_infos(infos, True, True)
    assert max([len(infos_[p]) for p in ["exist", "stream", "globus"]]) == 0
    assert len(infos_["https"]) == 2


def test_partition_infos_stream():
    VALID_OPENDAP_LINK = "https://esgf-data1.llnl.gov/thredds/dodsC/css03_data/CMIP6/CMIP/AS-RCEC/TaiESM1/historical/r1i1p1f1/day/tasmax/gn/v20210517/tasmax_day_TaiESM1_historical_r1i1p1f1_gn_20100101-20141231.nc"
    infos = [
        {
            "key": "dataset1",
            "path": Path("file1"),
            "VirtualZarr": [VALID_OPENDAP_LINK, VALID_OPENDAP_LINK],
        },
        {
            "key": "dataset2",
            "path": Path("file1"),
            "HTTPServer": ["link1", "link2"],
            "OPENDAP": [VALID_OPENDAP_LINK, VALID_OPENDAP_LINK],
        },
    ]
    infos_, ds = partition_infos(infos, False, False)
    assert max([len(infos_[p]) for p in ["exist", "stream", "globus"]]) == 0
    assert len(infos_["https"]) == 2
    assert len(ds) == 0
    # The following is meant to test that if streaming is requested, you will
    # get those links. However, the issue is that the code checks that the link
    # is valid and many times OPENDAP fails.
    infos_, ds = partition_infos(infos, True, True)
    assert max([len(infos_[p]) for p in ["exist", "globus"]]) == 0
    assert len(infos_["stream"] + infos_["https"]) == 2
    assert len(ds) == len(infos_["stream"])


@pytest.mark.globus_auth
def test_partition_infos_globus():
    # Check globus options, but with invalid endpoints
    infos = [
        {
            "key": "dataset2",
            "path": Path("file3"),
            "HTTPServer": ["link1", "link2"],
            "Globus": [
                "globus:123456789/blah",
            ],
        }
    ]
    infos_, _ = partition_infos(infos, False, True)
    assert max([len(infos_[p]) for p in ["exist", "stream", "globus"]]) == 0
    assert len(infos_["https"]) == 1

    # Add a valid endpoint (depends on it being active)
    infos[0]["Globus"] = ["globus://8896f38e-68d1-4708-bce4-b1b3a3405809/blah"]
    infos_, _ = partition_infos(infos, False, True)
    assert max([len(infos_[p]) for p in ["exist", "stream", "https"]]) == 0
    assert len(infos_["globus"]) == 1


def test_break():
    with intake_esgf.conf.set(break_on_error=True):
        try:
            cat = (
                ESGFCatalog()
                .search(
                    experiment_id="historical",
                    frequency="mon",
                    variable_id="gpp",
                    source_id=["E3SM-1-1"],
                )
                .remove_ensembles()
            )
            cat.to_path_dict()
        except MissingFileInformation:
            pass


def test_nobreak():
    with intake_esgf.conf.set(break_on_error=False):
        cat = (
            ESGFCatalog()
            .search(
                experiment_id="historical",
                frequency="mon",
                variable_id="gpp",
                source_id=["E3SM-1-1", "CanESM5"],
            )
            .remove_ensembles()
        )
        cat.df.loc[cat.df.index[1], "id"] = []  # fake there being no paths to download
        with pytest.warns(
            UserWarning,
            match="We could not download your entire catalog, missed={'CMIP6.CMIP.E3SM-Project.E3SM-1-1.historical.r1i1p1f1.Lmon.gpp.gr'}",
        ):
            paths = cat.to_path_dict()
        assert len(paths) == 1


def test_file_timestamps():
    cat = ESGFCatalog().search(
        experiment_id="historical",
        variable_id="msftmz",
        source_id="NorESM2-LM",
        variant_label="r2i1p1f1",
        file_start="1960-01",
        file_end="1979-12",
    )
    dpd = cat.to_path_dict()
    paths = dpd[next(iter(dpd))]
    assert len(paths) == 2


def test_config():
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
