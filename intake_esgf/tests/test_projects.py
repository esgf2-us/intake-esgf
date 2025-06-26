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
    with intake_esgf.conf.set(indices={"ESGF2-US-1.5-Catalog": True}):
        cat = ESGFCatalog().search(
            project="obs4MIPs", institution_id="NASA-LaRC", variable_id="rlus"
        )
    try:  # this shouldn't work but fail nicely
        cat.model_groups()
    except ProjectHasNoFacet:
        pass
    assert len(cat.df) == 1


def test_projectdownscale():
    with intake_esgf.conf.set(indices={"ESGF2-US-1.5-Catalog": True}):
        cat = intake_esgf.ESGFCatalog().search(
            project="DRCDP",
            downscaling_source_id="LOCA2-1",
            driving_source_id="ACCESS-CM2",
            driving_experiment_id="historical",
            frequency="day",
            variable_id="pr",
        )
    try:  # this shouldn't work but fail nicely
        cat.model_groups()
    except ProjectHasNoFacet:
        pass
    assert len(cat.df) == 1
