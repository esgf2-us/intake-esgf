import pickle

import pytest

from intake_esgf.core import STACESGFIndex


@pytest.fixture
def east():
    return STACESGFIndex("discovery.east.esgf.io")


@pytest.fixture
def west():
    return STACESGFIndex("discovery.west.esgf.io")


def test_search_cmip6_west(west):
    df = west.search(
        project="CMIP6",
        source_id="UKESM1-0-LL",
        variable_id="tas",
        frequency="mon",
        experiment_id="historical",
        variant_label="r1i1p1f2",
    )
    assert len(df) == 1
    infos = west.get_file_info(df["id"].to_list())
    assert len(infos) == 2


def test_search_cmip7_west(west):
    df = west.search(
        project="CMIP7",
        source_id="UKCM2a-0-HH",
        variable_id="tas",
        frequency="mon",
        experiment_id="historical",
        variant_label="r1i1p1f1",
    )
    assert len(df) == 1
    infos = west.get_file_info(df["id"].to_list())
    assert len(infos) == 18


def test_search_cmip6plus_west(west):
    df = west.search(
        project="CMIP6Plus",
        source_id="ACCESS-CM2",
        variable_id="zg500",
        experiment_id="tbi-pace-I-clim-mod",
        table_id="AE6hrPt",
        variant_label="r1i1p1f1",
    )
    assert len(df) == 1
    infos = west.get_file_info(df["id"].to_list())
    assert len(infos) == 20


@pytest.mark.xfail(raises=ValueError)
def test_search_cmip6_ceda(east):
    # This test should only return 1 item but because their CMIP6 is incorrectly
    # published, returns many. We search for a particular member_id here, but
    # all s1960-s2018 are returned because that field is incorrect in the
    # record.
    df = east.search(
        project="CMIP6",
        experiment_id="dcppA-hindcast",
        member_id="s2020-r1i1p1f2",
        source_id="HadGEM3-GC31-MM",
        variable_id="tasmax",
    )
    if len(df) != 1:
        raise ValueError(f"Data is erroneously published on CEDA, {len(df)=} != 1")
    infos = east.get_file_info(df["id"].to_list())
    if len(infos) != 12:
        raise ValueError(f"Data is erroneously published on CEDA, {len(infos)=} != 12")


def test_pickle(east) -> None:
    index = east
    pickled = pickle.dumps(index)
    unpickled = pickle.loads(pickled)
    assert repr(index) == repr(unpickled)
    assert str(index.session) == str(unpickled.session)
    assert index.logger == unpickled.logger
