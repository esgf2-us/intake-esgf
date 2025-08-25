from intake_esgf.core import STACESGFIndex

INDEX = STACESGFIndex("api.stac.ceda.ac.uk")
# INDEX = STACESGFIndex("api.stac.esgf-west.org")


def test_search():
    df = INDEX.search(
        experiment_id="historical",
        source_id="CanESM5",
        variable_id=["tas", "pr"],
        variant_label="r1i1p1f1",
        frequency="mon",
    )
    assert len(df) == 2
    infos = INDEX.get_file_info(df["id"].to_list())
    assert len(infos) == 2
