import pytest

import intake_esgf
from intake_esgf import ESGFCatalog
from intake_esgf.exceptions import ProjectHasNoFacet


def test_input4MIPs_discovery():
    """
    Test a small search that will exercise the discovery functions of intake-esgf.
    """
    intake_esgf.conf.set(
        no_indices=True,
        indices={"ESGF2-US-1.5-Catalog": True},
    )
    cat = ESGFCatalog().search(
        project="input4MIPs", variable_id="ssa", source_id="UOEXETER-CMIP-2-2-1"
    )
    assert len(cat.df) == 2
    with pytest.raises(ProjectHasNoFacet):
        cat.model_groups()
    with pytest.raises(ProjectHasNoFacet):
        cat.remove_incomplete(lambda df: True)
    with pytest.raises(ProjectHasNoFacet):
        cat.remove_ensembles()


def test_input4MIPs_download():
    """
    Test a small file download.
    """
    intake_esgf.conf.set(no_indices=True, indices={"ESGF2-US-1.5-Catalog": True})
    cat = ESGFCatalog().search(
        project="input4MIPs",
        source_id="ImperialCollege-3-0",
        variable_id="delta13co2",
    )
    dsd = cat.to_dataset_dict()
    assert len(dsd) == 1
    _, ds = next(iter(dsd.items()))
    assert "delta13co2" in ds
