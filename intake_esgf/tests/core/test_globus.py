import pickle
from pathlib import Path

import pytest

import intake_esgf
from intake_esgf import ESGFCatalog
from intake_esgf.core.globus import GlobusESGFIndex


@pytest.mark.globus_auth
def test_globus_transfer(tmp_path: Path) -> None:
    # make sure this cache does not exist and set configuration
    local_cache = tmp_path / "esgf-test"

    indices = {
        key: False
        for key in (
            intake_esgf.conf["globus_indices"] | intake_esgf.conf["solr_indices"]
        )
    }
    indices["ESGF2-US-1.5-Catalog"] = True
    intake_esgf.conf.set(indices=indices, local_cache=[str(local_cache)])

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
            globus_path=Path("esgf-test"),
            add_measures=False,
        )
    )
    assert not (set(dsd) - set(["Amon.pr", "Amon.tas", "Lmon.gpp"]))


def test_pickle() -> None:
    index = GlobusESGFIndex()
    pickled = pickle.dumps(index)
    unpickled = pickle.loads(pickled)
    assert repr(index) == repr(unpickled)
    assert str(index.session) == str(unpickled.session)
    assert index.logger == unpickled.logger
