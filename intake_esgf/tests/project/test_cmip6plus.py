import intake_esgf
from intake_esgf import ESGFCatalog

# CMIP6Plus is the same as CMIP6, this is just to satisfy coverage


def test_cmip6_discovery():
    """
    Test a small search that will exercise the discovery functions of intake-esgf.
    """
    intake_esgf.conf.set(no_indices=True, indices={"ESGF2-US-1.5-Catalog": True})
    cat = ESGFCatalog().search(
        project="CMIP6Plus",
        experiment_id=["hist-lu", "hist-aer"],
        source_id=["E3SM-2-1", "HadGEM3-GC31-LL"],
        member_id=["f2023-r6i1p1f1", "f2023-r11i1p1f1", "f2023-r11i1p1f3"],
        variable_id=["tasmax", "mrsofc"],
    )
    assert len(cat.df) == 4
    assert len(cat.model_groups()) == 3
    cat.remove_ensembles()
    assert len(cat.df) == 2
    assert len(cat.model_groups()) == 2
