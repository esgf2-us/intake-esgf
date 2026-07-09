import pickle

import pytest

from intake_esgf.core import STACESGFIndex
from intake_esgf.exceptions import NoSearchResults

# INDEX = STACESGFIndex("discovery.integration.esgf-west.org")


@pytest.fixture
def ceda():
    return STACESGFIndex("api.stac.esgf.ceda.ac.uk")


@pytest.fixture
def west():
    return STACESGFIndex("discovery.integration.esgf-west.org")


def test_search_cmip6_west(west):
    df = west.search(
        project="CMIP6",
        experiment_id="amip",
        source_id="E3SM-1-0",
        variable_id="tas",
    )
    assert len(df) == 1
    infos = west.get_file_info(df["id"].to_list())
    assert len(infos) == 6


def test_search_cmip7_west(west):
    df = west.search(
        project="CMIP7",
        experiment_id="historical",
        source_id="CanESM5-1",
        variable_id="tas",
    )
    assert len(df) == 1
    infos = west.get_file_info(df["id"].to_list())
    assert len(infos) == 2


def test_search_cmip6plus_west(west):
    df = west.search(
        project="CMIP6Plus",
        experiment_id="hist-lu",
        source_id="HadGEM3-GC31-LL",
        variable_id="areacella",
    )
    assert len(df) == 1
    infos = west.get_file_info(df["id"].to_list())
    assert len(infos) == 1


@pytest.mark.xfail(raises=ValueError)
def test_search_cmip6_ceda(ceda):
    # This test should only return 1 item but because their CMIP6 is incorrectly published, returns many
    df = ceda.search(
        project="CMIP6",
        experiment_id="dcppA-hindcast",
        member_id="s2020-r1i1p1f2",
        source_id="HadGEM3-GC31-MM",
        variable_id="tasmax",
    )
    if len(df) != 1:
        raise ValueError(f"Data is erroneously published on CEDA, {len(df)=} != 1")
    infos = ceda.get_file_info(df["id"].to_list())
    if len(infos) != 12:
        raise ValueError(f"Data is erroneously published on CEDA, {len(infos)=} != 12")


@pytest.mark.parametrize("project", ["CMIP6Plus", "CMIP7"])
@pytest.mark.xfail(raises=NoSearchResults)
def test_search_ceda_fails(ceda, project):
    # Nothing is published in these so they should fail. Wh
    df = ceda.search(
        project=project,
    )
    assert len(df) == 0
    infos = ceda.get_file_info(df["id"].to_list())
    assert len(infos) == 0


def test_pickle(ceda) -> None:
    index = ceda
    pickled = pickle.dumps(index)
    unpickled = pickle.loads(pickled)
    assert repr(index) == repr(unpickled)
    assert str(index.session) == str(unpickled.session)
    assert index.logger == unpickled.logger
