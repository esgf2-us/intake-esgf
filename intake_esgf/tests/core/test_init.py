from time import perf_counter

import pytest
from requests_cache import CachedSession

import intake_esgf
from intake_esgf import ESGFCatalog


@pytest.mark.parametrize(
    "index_type",
    [
        "globus",
        "stac",
        pytest.param("solr", marks=pytest.mark.solr),
    ],
)
def test_search_is_cached(index_type, tmp_path):
    # Use a test specific cache.
    intake_esgf.conf["requests_cache"]["cache_name"] = str(
        tmp_path / "requests-cache.sqlite"
    )
    # Enable only the index type being tested.
    for index_collection in ["globus_indices", "solr_indices", "stac_indices"]:
        for key in intake_esgf.conf[index_collection]:
            intake_esgf.conf[index_collection][key] = (
                index_collection == f"{index_type}_indices"
            )

    facets = {
        "experiment_id": ["historical"],
        "source_id": ["CanESM5"],
        "variable_id": ["tas"],
        "variant_label": ["r1i1p1f1"],
    }

    # Test that the indices are using a cached session.
    for ind in ESGFCatalog().indices:
        assert isinstance(ind.session, CachedSession)

    # Test that a repeat search is faster than the initial one.
    start = perf_counter()
    ESGFCatalog().search(**facets)
    initial_search = perf_counter() - start

    start = perf_counter()
    ESGFCatalog().search(**facets)
    cached_search = perf_counter() - start

    assert cached_search < initial_search
