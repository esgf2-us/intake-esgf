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
