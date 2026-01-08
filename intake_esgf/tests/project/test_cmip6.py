import intake_esgf
from intake_esgf import ESGFCatalog


def test_cmip6_discovery():
    """
    Test a small search that will exercise the discovery functions of intake-esgf.
    """
    intake_esgf.conf.set(no_indices=True, indices={"ESGF2-US-1.5-Catalog": True})
    cat = ESGFCatalog().search(
        project="CMIP6",
        variable_id=["gpp", "mrso"],
        experiment_id=["historical", "ssp585"],
        source_id=["CanESM5", "CESM2"],
        member_id=["r1i1p1f1", "r2i1p1f1"],
    )
    assert len(cat.df) == 12
    assert len(cat.model_groups()) == 4
    cat.remove_incomplete(lambda df: True if len(df) == 4 else False)
    assert len(cat.df) == 8
    assert len(cat.model_groups()) == 2
    cat.remove_ensembles()
    assert len(cat.df) == 4
    assert len(cat.model_groups()) == 1


def test_cmip6_download():
    """
    Test a small file download.
    """
    intake_esgf.conf.set(no_indices=True, indices={"ESGF2-US-1.5-Catalog": True})
    cat = ESGFCatalog().search(
        project="CMIP6",
        variable_id="wetlandFrac",
        experiment_id="historical",
        source_id="CanESM5",
        member_id="r1i1p1f1",
    )
    dsd = cat.to_dataset_dict()
    assert len(dsd) == 1
    _, ds = next(iter(dsd.items()))
    assert "wetlandFrac" in ds
    assert "areacella" in ds  # adding cell measures only works in CMIP6 at the moment
    assert "sftlf" in ds


def test_cmip6_from_tracking_ids():
    """
    Test a single tracking_id.
    """
    intake_esgf.conf.set(no_indices=True, indices={"ESGF2-US-1.5-Catalog": True})
    cat = ESGFCatalog().from_tracking_ids(
        ["hdl:21.14100/d9a7225a-49c3-4470-b7ab-a8180926f839"]
    )
    assert len(cat.df) == 1
    assert cat.unique()["source_id"] == "CESM2"


def test_cmip6_get_variable_info():
    """
    Test a simple variable info.
    """
    intake_esgf.conf.set(no_indices=True, indices={"ESGF2-US-1.5-Catalog": True})
    cat = ESGFCatalog()
    df = cat.variable_info("cWood")
    assert len(df) == 1
    assert df.iloc[0]["cf_standard_name"] == "stem_mass_content_of_carbon"


def test_cmip6_timestamps():
    """
    Test that timestamps effectively filter out files.
    """
    cat = ESGFCatalog().search(
        project="CMIP6",
        experiment_id="historical",
        variable_id="msftmz",
        source_id="NorESM2-LM",
        variant_label="r2i1p1f1",
        file_start="1960-01",
        file_end="1979-12",
    )
    dpd = cat.to_path_dict()
    paths = dpd[next(iter(dpd))]
    for path in paths:
        t0, tf = intake_esgf.base.get_time_extent(str(path))
        assert t0 >= cat.file_start
        assert tf <= cat.file_end


def test_cmip6_add_cell_measures():
    """
    Test that we can add cell measures even far away.
    """
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
