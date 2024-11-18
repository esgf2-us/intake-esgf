import intake_esgf
from intake_esgf import ESGFCatalog
from intake_esgf.exceptions import ProjectHasNoFacet


def test_cmip5():
    cat = ESGFCatalog().search(
        project="CMIP5",
        experiment="historical",
        cmor_table="Amon",
        variable="tas",
    )
    cat.remove_ensembles()
    assert len(cat.model_groups()) == 44


def test_cmip3():
    cat = ESGFCatalog().search(
        project="CMIP3",
        experiment="historical",
        time_frequency="mon",
        variable="tas",
    )
    cat.remove_ensembles()
    assert len(cat.model_groups()) == 24


def test_obs4mips():
    # this project is only on LLNL for now
    with intake_esgf.conf.set(indices={"esgf-node.llnl.gov": True}):
        cat = ESGFCatalog().search(
            project="obs4MIPs", institution_id="NASA-LaRC", variable_id="rlus"
        )
        try:  # this shouldn't work but fail nicely
            cat.model_groups()
        except ProjectHasNoFacet:
            pass
        assert len(cat.df) == 1
