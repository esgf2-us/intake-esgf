from intake_esgf import ESGFCatalog
from intake_esgf.exceptions import NoSearchResults

SOLR_TEST = "esgf-node.ornl.gov"


def test_search():
    cat = ESGFCatalog(esgf1_indices=SOLR_TEST)
    print(cat)
    cat = ESGFCatalog(esgf1_indices=[SOLR_TEST]).search(
        experiment_id="historical",
        source_id="CanESM5",
        variable_id=["gpp"],
        variant_label=["r1i1p1f1"],
    )
    print(cat)
    ds = cat.to_dataset_dict()
    assert "gpp" in ds
    assert "sftlf" in ds["gpp"]


def test_esgroot():
    cat = ESGFCatalog()
    cat.set_esgf_data_root(cat.local_cache)
    cat.search(
        experiment_id="historical",
        source_id="CanESM5",
        variable_id=["gpp"],
        variant_label=["r1i1p1f1"],
    )
    ds = cat.to_dataset_dict(add_measures=False)
    assert "gpp" in ds
    log = cat.session_log()
    assert "download" not in log
    assert f"accessed {cat.esgf_data_root}" in cat.session_log()


def test_noresults():
    cat = ESGFCatalog()
    try:
        cat.search(variable_id="does_not_exist")
    except NoSearchResults:
        pass


def test_tracking_ids():
    cat = ESGFCatalog(esgf1_indices=SOLR_TEST).from_tracking_ids(
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
