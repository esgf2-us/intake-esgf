import intake_esgf
from intake_esgf import ESGFCatalog


def test_cmip5_discovery():
    """
    Test a small search that will exercise the discovery functions of intake-esgf.
    """
    intake_esgf.conf.set(no_indices=True, indices={"ESGF2-US-1.5-Catalog": True})
    cat = ESGFCatalog().search(
        project="CMIP5",
        variable=["gpp", "mrso"],
        experiment=["historical", "rcp85"],
        model=["CanESM2", "CCSM4"],
        ensemble=["r1i1p1", "r2i1p1"],
    )
    assert len(cat.df) == 14
    assert len(cat.model_groups()) == 4
    cat.remove_incomplete(lambda df: True if len(df) == 4 else False)
    assert len(cat.df) == 12
    assert len(cat.model_groups()) == 3
    cat.remove_ensembles()
    assert len(cat.df) == 8
    assert len(cat.model_groups()) == 2


def test_cmip5_download():
    """
    Test a small file download.
    """
    intake_esgf.conf.set(no_indices=True, indices={"ESGF2-US-1.5-Catalog": True})
    cat = ESGFCatalog().search(
        project="CMIP5",
        variable="areacella",
        experiment="amip",
        model="CanAM4",
        ensemble="r0i0p0",
    )
    dsd = cat.to_dataset_dict()
    assert len(dsd) == 1
    _, ds = next(iter(dsd.items()))
    assert "areacella" in ds
