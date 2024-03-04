from intake_esgf import ESGFCatalog


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
