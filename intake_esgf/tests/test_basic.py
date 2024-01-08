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
    cat = ESGFCatalog(esgf1_indices=True).search(
        activity_id="CMIP",
        experiment_id="historical",
        source_id="CESM2",
        variable_id=["gpp", "areacella", "sftlf"],
        variant_label=["r1i1p1f1"],
    )
    assert len(cat.df) == 3


def test_tracking_ids():
    cat = ESGFCatalog().from_tracking_ids(
        "hdl:21.14100/0577d84f-9954-494f-8cc8-465aa4fd910e"
    )
    assert len(cat.df) == 1
    cat = ESGFCatalog().from_tracking_ids(
        [
            "hdl:21.14100/0577d84f-9954-494f-8cc8-465aa4fd910e",
            "hdl:21.14100/0972f78b-158e-4c6b-bcdf-7d0d75d7a8cd",
            "hdl:21.14100/0e4dfb8f-b677-456e-abc7-71e1ebc16deb",
            "hdl:21.14100/17b6c62f-455b-49bc-8674-564f7ca5ed6a",
            "hdl:21.14100/1bd030c9-1761-4fca-911e-6ea2b6407bc7",
            "hdl:21.14100/2844ea5a-4589-4ed4-bbb7-c13e9964a4b7",
        ]
    )
    assert len(cat.df) == 7


def test_add_cell_measures():
    # these measures are in r1i1p1f2 / piControl
    cat = ESGFCatalog().search(
        variable_id="gpp",
        source_id="UKESM1-0-LL",
        variant_label="r2i1p1f2",
        frequency="mon",
        experiment_id="historical",
    )
    ds = cat.to_dataset_dict()["gpp"]
    assert "sftlf" in ds
    assert "areacella" in ds
