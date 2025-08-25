import intake_esgf
from intake_esgf import ESGFCatalog


def test_DRCDP_discovery():
    """
    Test a small search that will exercise the discovery functions of intake-esgf.
    """
    intake_esgf.conf.set(
        no_indices=True,
        indices={"ESGF2-US-1.5-Catalog": True},
    )
    cat = ESGFCatalog().search(
        project="DRCDP",
        source_id=["LOCA2--EC-Earth3-Veg", "LOCA2--TaiESM1"],
        driving_experiment_id=["historical", "ssp585"],
        variable_id=["tasmax", "tasmin", "pr"],
    )
    assert len(cat.df) == 9
    assert len(cat.model_groups()) == 2
    cat.remove_incomplete(lambda df: True if len(df) == 6 else False)
    assert len(cat.df) == 6
    assert len(cat.model_groups()) == 1
    cat.remove_ensembles()
    assert len(cat.df) == 6
    assert len(cat.model_groups()) == 1


# def test_DRCDP_download():
#    """
#    Can't test a small file download as it is 900 Mb
#    """
#    pass
