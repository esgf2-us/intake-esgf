from intake_esgf.core import SolrESGFIndex
from intake_esgf.exceptions import NoSearchResults

index = SolrESGFIndex("esgf-node.llnl.gov", distrib=False)
cmip6 = dict(
    experiment_id="historical",
    source_id="CanESM5",
    variable_id="tas",
    variant_label="r1i1p1f1",
    frequency="mon",
)
# we test a cmip5 search because there are specialized code branches
cmip5 = dict(
    project="CMIP5",
    experiment="historical",
    model="CanESM2",
    variable="tas",
    ensemble="r1i1p1",
    time_frequency="mon",
)


def test_search():
    df = index.search(**cmip5)
    assert len(df) > 0
    df = index.search(**cmip6)
    assert len(df) > 0



def test_tracking_ids():
    df = index.from_tracking_ids(["hdl:21.14100/872062df-acae-499b-aa0f-9eaca7681abc"])
    assert len(df) > 0


def test_get_file_info():
    dataset_id = "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Amon.tas.gn.v20190429|aims3.llnl.gov"
    infos = index.get_file_info([dataset_id])
    assert isinstance(infos, list)
    assert len(infos) == 1
    assert isinstance(infos[0], dict)
    assert infos[0]["dataset_id"] == dataset_id


def test_null():
    try:
        index.search(variable_id="does_not_exist")
    except NoSearchResults:
        pass
    try:
        index.from_tracking_ids(["does_not_exist"])
    except NoSearchResults:
        pass
    try:
        index.get_file_info(["does_not_exist"])
    except NoSearchResults:
        pass
