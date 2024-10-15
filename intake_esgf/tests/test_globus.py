import pytest

from intake_esgf.core import GlobusESGFIndex
from intake_esgf.exceptions import NoSearchResults

index = GlobusESGFIndex("ornl-dev")
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
    dataset_id = "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Amon.tas.gn.v20190429|esgf-node.ornl.gov"
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


# because anl encodes booleans as strings
def test_anl():
    index = GlobusESGFIndex("anl-dev")
    df = index.search(latest=True, **cmip6)
    assert len(df) > 0


@pytest.mark.globus_auth
def test_globus_transfer():
    import os
    from pathlib import Path

    import intake_esgf
    from intake_esgf import ESGFCatalog

    # make sure this cache does not exist and set configuration
    local_cache = Path().home() / "esgf-test"
    os.system(f"rm -rf {local_cache}")
    intake_esgf.conf.set(local_cache=[str(local_cache)], indices={"ornl-dev": False})

    dsd = (
        ESGFCatalog()
        .search(
            experiment_id="historical",
            source_id="CanESM5",
            frequency="mon",
            variable_id=[
                "pr",
                "tas",
                "gpp",
            ],
            member_id="r1i1p1f1",
        )
        .to_dataset_dict(
            globus_endpoint="285fafe4-ae63-11ee-b085-4bb870e392e2",
            globus_path="esgf-test",
            add_measures=False,
        )
    )
    os.system(f"rm -rf {local_cache}")
    assert not (set(dsd) - set(["Amon.pr", "Amon.tas", "Lmon.gpp"]))
