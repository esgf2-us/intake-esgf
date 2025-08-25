import pytest


@pytest.mark.globus_auth
def test_globus_transfer():
    import os
    from pathlib import Path

    import intake_esgf
    from intake_esgf import ESGFCatalog

    # make sure this cache does not exist and set configuration
    local_cache = Path().home() / "esgf-test"
    os.system(f"rm -rf {local_cache}")

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
            globus_path="esgf-test",
            add_measures=False,
        )
    )
    os.system(f"rm -rf {local_cache}")
    assert not (set(dsd) - set(["Amon.pr", "Amon.tas", "Lmon.gpp"]))
