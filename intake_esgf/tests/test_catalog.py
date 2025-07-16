import pytest
import requests_cache

import intake_esgf


@pytest.mark.parametrize(
    "expire_after",
    [
        "DO_NOT_CACHE",
        "EXPIRE_IMMEDIATELY",
        "NEVER_EXPIRE",
    ],
)
def test_special_cache_expire_after(expire_after, tmp_path):
    """Test disabling the requests_cache."""
    with intake_esgf.conf.set(
        requests_cache={
            "cache_name": str(tmp_path / "requests-cache.sqlite"),
            "expire_after": expire_after,
        }
    ):
        cat = intake_esgf.ESGFCatalog()
        for ind in cat.indices:
            session = ind.session
            assert isinstance(session, requests_cache.CachedSession)
            assert session.expire_after == getattr(requests_cache, expire_after)


def test_logger():
    """Test that the catalog and indices are created with the capturing logger."""
    cat = intake_esgf.ESGFCatalog()
    assert isinstance(cat.logger, intake_esgf.logging.Logger)
    assert len(cat.indices)
    for ind in cat.indices:
        assert ind.logger == cat.logger
