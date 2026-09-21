import intake_esgf
from intake_esgf import ESGFCatalog


def test_cordexcmip6_discovery():
    """
    Test a small search that will exercise the discovery functions of intake-esgf.
    """
    intake_esgf.conf.set(no_indices=True, indices={"discovery.east.esgf.io": True})
    cat = ESGFCatalog().search(
        project="CORDEX-CMIP6",
        variable_id=["pr"],
        driving_experiment_id=["historical", "ssp370"],
        driving_source_id=["NorESM2-MM", "MPI-ESM1-2-LR"],
        driving_variant_label=["r1i1p1f1", "r5i1p1f1"],
        frequency="mon",
    )
    assert len(cat.df) == 11
    assert len(cat.model_groups()) == 6
    cat.remove_incomplete(lambda df: True if len(df) >= 2 else False)
    assert len(cat.df) == 8
    assert len(cat.model_groups()) == 3
    cat.remove_ensembles()
    assert len(cat.df) == 6
    assert len(cat.model_groups()) == 2


def test_cordexcmip6_download():
    """
    Test a small file download.
    """
    intake_esgf.conf.set(no_indices=True, indices={"discovery.east.esgf.io": True})
    cat = ESGFCatalog().search(
        project="CORDEX-CMIP6",
        driving_experiment_id="historical",
        driving_source_id="MIROC6",
        driving_variant_label="r1i1p1f1",
        variable_id="siconca",
        frequency="mon",
    )
    dsd = cat.to_dataset_dict()
    assert len(dsd) == 1
    _, ds = next(iter(dsd.items()))
    assert "siconca" in ds
