import pytest

import intake_esgf
from intake_esgf import ESGFCatalog
from intake_esgf.exceptions import ProjectHasNoFacet


def test_WrPMIP_discovery():
    """
    Test a small search that will exercise the discovery functions of intake-esgf.
    """
    intake_esgf.conf.set(
        no_indices=True,
        indices={"ESGF2-US-1.5-Catalog": True},
    )
    cat = ESGFCatalog().search(
        project="WrPMIP",
        variable_id="er",
        source_id=["CLASSIC"],
        experiment_id="wrp-hist",
    )
    assert len(cat.df) == 1
    with pytest.raises(ProjectHasNoFacet):
        cat.model_groups()
    with pytest.raises(ProjectHasNoFacet):
        cat.remove_incomplete(lambda df: True)
    with pytest.raises(ProjectHasNoFacet):
        cat.remove_ensembles()


def test_WrPMIP_download():
    """
    Test a small file download.
    """
    intake_esgf.conf.set(no_indices=True, indices={"ESGF2-US-1.5-Catalog": True})
    cat = ESGFCatalog().search(
        project="WrPMIP",
        variable_id="er",
        source_id=["CLASSIC"],
        experiment_id="wrp-hist",
    )
    dsd = cat.to_dataset_dict()
    assert len(dsd) == 1
    _, ds = next(iter(dsd.items()))
    assert "er" in ds
