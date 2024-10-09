from pathlib import Path

import pytest

import intake_esgf
from intake_esgf import ESGFCatalog
from intake_esgf.base import partition_infos
from intake_esgf.exceptions import NoSearchResults

SOLR_TEST = "esgf-node.llnl.gov"


def test_search():
    extra = ["datetime_start", "datetime_stop"]
    with intake_esgf.conf.set(indices={SOLR_TEST: True}, additional_df_cols=extra):
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
    with intake_esgf.conf.set(indices={SOLR_TEST: True}):
        cat = ESGFCatalog().from_tracking_ids(
            "hdl:21.14100/0577d84f-9954-494f-8cc8-465aa4fd910e"
        )
        assert len(cat.df) == 1
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
    infos = [
        {
            "key": "dataset1",
            "path": Path("file1"),
            "VirtualZarr": ["link1", "link2"],
        },
        {
            "key": "dataset2",
            "path": Path("file1"),
            "HTTPServer": ["link1", "link2"],
            "OPENDAP": ["link1", "link2"],
        },
    ]
    infos_, ds = partition_infos(infos, False, False)
    assert max([len(infos_[p]) for p in ["exist", "stream", "globus"]]) == 0
    assert len(infos_["https"]) == 2
    assert len(ds) == 0
    infos_, ds = partition_infos(infos, True, True)
    assert max([len(infos_[p]) for p in ["exist", "globus", "https"]]) == 0
    assert len(infos_["stream"]) == 2
    assert len(ds) == 2


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
