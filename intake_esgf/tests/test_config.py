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
