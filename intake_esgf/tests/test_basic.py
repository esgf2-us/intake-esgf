from intake_esgf import ESGFCatalog


def test_search():
    cat = ESGFCatalog().search(
        activity_id="CMIP",
        experiment_id="historical",
        source_id="CESM2",
        variable_id=["gpp", "areacella", "sftlf"],
        variant_label=["r1i1p1f1"],
    )
    assert len(cat.df) == 3


def test_global_search():
    cat = ESGFCatalog(legacy_nodes=True).search(
        activity_id="CMIP",
        experiment_id="historical",
        source_id="CESM2",
        variable_id=["gpp", "areacella", "sftlf"],
        variant_label=["r1i1p1f1"],
    )
    assert len(cat.df) == 3
