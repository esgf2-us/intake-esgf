import intake_esgf


def test_set_requests_cache():
    """Test temporary altering requests_cache settings."""
    default_expire_after = 3600  # 1 hour
    temporary_expire_after = "DO_NOT_CACHE"

    assert intake_esgf.conf["requests_cache"]["expire_after"] == default_expire_after

    with intake_esgf.conf.set(
        requests_cache={
            "expire_after": temporary_expire_after,
        }
    ):
        assert (
            intake_esgf.conf["requests_cache"]["expire_after"] == temporary_expire_after
        )

    assert intake_esgf.conf["requests_cache"]["expire_after"] == default_expire_after


def test_set_indices():
    # All off
    intake_esgf.conf.set(no_indices=True)
    num_on = sum(
        [
            enabled
            for index_type in ["globus_indices", "solr_indices", "stac_indices"]
            for _, enabled in intake_esgf.conf[index_type].items()
        ]
    )
    assert num_on == 0
    # All on, doesn't affect STAC at the moment
    intake_esgf.conf.set(all_indices=True)
    num_off = sum(
        [
            not enabled
            for index_type in ["globus_indices", "solr_indices"]
            for _, enabled in intake_esgf.conf[index_type].items()
        ]
    )
    assert num_off == 0
    # All back off and then enable one
    intake_esgf.conf.set(no_indices=True, indices={"ESGF2-US-1.5-Catalog": True})
    num_on = sum(
        [
            enabled
            for index_type in ["globus_indices", "solr_indices", "stac_indices"]
            for _, enabled in intake_esgf.conf[index_type].items()
        ]
    )
    assert num_on == 1
