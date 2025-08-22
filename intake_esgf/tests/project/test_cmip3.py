import intake_esgf
from intake_esgf import ESGFCatalog


def test_cmip3_discovery():
    """
    Test a small search that will exercise the discovery functions of intake-esgf.
    """
    intake_esgf.conf.set(no_indices=True, indices={"ESGF2-US-1.5-Catalog": True})
    cat = ESGFCatalog().search(
        project="CMIP3",
        experiment=["historical", "1pctCO2"],
        model=["ncar_ccsm3_0", "ukmo_hadcm3"],
        variable=["tas", "snc"],
        time_frequency="mon",
    )
    assert len(cat.df) == 19
    assert len(cat.model_groups()) == 11
    cat.remove_incomplete(lambda df: True if len(df) >= 2 else False)
    assert len(cat.df) == 14
    assert len(cat.model_groups()) == 6
    cat.remove_ensembles()
    assert len(cat.df) == 4
    assert len(cat.model_groups()) == 1


def test_cmip3_download():
    """
    Test a small file download.
    """
    intake_esgf.conf.set(no_indices=True, indices={"ESGF2-US-1.5-Catalog": True})
    cat = ESGFCatalog().search(
        project="CMIP3",
        experiment="historical",
        variable="sftlf",
        model="bccr_bcm2_0",
        ensemble="run1",
    )
    dsd = cat.to_dataset_dict()
    assert len(dsd) == 1
    _, ds = next(iter(dsd.items()))
    assert "sftlf" in ds
