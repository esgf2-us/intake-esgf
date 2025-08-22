import pytest

import intake_esgf
from intake_esgf import ESGFCatalog
from intake_esgf.exceptions import ProjectHasNoFacet


def test_obs4MIPs_discovery():
    """
    Test a small search that will exercise the discovery functions of intake-esgf.
    """
    intake_esgf.conf.set(
        no_indices=True,
        indices={"ESGF2-US-1.5-Catalog": True},
    )
    cat = ESGFCatalog().search(
        project="obs4MIPs",
        variable_id="ta",
        source_id=["AIRS-1-0", "AIRS-2-0", "AIRS-2-1"],
    )
    assert len(cat.df) == 3
    with pytest.raises(ProjectHasNoFacet):
        cat.model_groups()
    with pytest.raises(ProjectHasNoFacet):
        cat.remove_incomplete(lambda df: True)
    with pytest.raises(ProjectHasNoFacet):
        cat.remove_ensembles()


def test_obs4MIPs_download():
    """
    Test a small file download.
    """
    intake_esgf.conf.set(no_indices=True, indices={"ESGF2-US-1.5-Catalog": True})
    cat = ESGFCatalog().search(
        project="obs4MIPs",
        source_id="CERES-EBAF-4-2",
        variable_id="rsdt",
    )
    dsd = cat.to_dataset_dict()
    assert len(dsd) == 1
    _, ds = next(iter(dsd.items()))
    assert "rsdt" in ds
