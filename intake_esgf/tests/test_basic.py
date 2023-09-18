from intake_esgf import ESGFCatalog


def test_search():
    cat = ESGFCatalog().search(
        activity_id="CMIP",
        experiment_id="historical",
        source_id="CESM2",
        variable_id=["gpp", "areacella", "sftlf"],
        member_id=["r1i1p1f1"],
    )
    assert len(cat.df) == 11


def test_strict_search():
    cat = ESGFCatalog().search(
        strict=True,
        activity_id="CMIP",
        experiment_id="historical",
        source_id="CESM2",
        variable_id=["gpp", "areacella", "sftlf"],
        member_id=["r1i1p1f1"],
    )
    assert len(cat.df) == 3
