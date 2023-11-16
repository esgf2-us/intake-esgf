from functools import partial

import intake_esgf.operators as ops
from intake_esgf import ESGFCatalog


# just to make these tests run faster
def trim_time(dsd):
    for key, ds in dsd.items():
        dsd[key] = ds.sel(time=slice("1990-01-01", "2000-01-01"))
    return dsd


def test_global_mean():
    cat = ESGFCatalog().search(
        experiment_id=["historical"],
        source_id="CanESM5",
        activity_id="CMIP",
        variant_label="r1i1p1f1",
        variable_id=["nbp", "tas", "fgco2"],
        frequency="mon",
    )
    dsd = cat.to_dataset_dict(ignore_facets=["table_id"])
    dsd = trim_time(dsd)
    dsd = ops.global_mean(dsd)
    assert set(["fgco2", "tas", "nbp"]) == set(dsd.keys())


def test_ensemble_mean():
    cat = ESGFCatalog().search(
        experiment_id="historical",
        source_id=["CESM2", "CanESM5"],
        variant_label=["r1i1p1f1", "r2i1p1f1", "r3i1p1f1"],
        variable_id=["tas", "pr"],
        frequency="mon",
    )
    dsd = cat.to_dataset_dict(ignore_facets=["institution_id", "table_id"])
    dsd = trim_time(dsd)
    dsd = ops.ensemble_mean(dsd)
    assert set(
        ["CESM2.mean.pr", "CESM2.mean.tas", "CanESM5.mean.pr", "CanESM5.mean.tas"]
    ) == set(dsd.keys())


def test_composition():
    """Run a test on composition of operators.

    Operators may be locally defined, but we expect that the only argument taken is a
    dictionary of datasets or a dataset. If a function has arguments, you can use
    `partial` from `functools` to resolve the arugments and then pass the function into
    the operators.

    """
    cat = ESGFCatalog().search(
        experiment_id="historical",
        source_id=["CanESM5"],
        variant_label=["r1i1p1f1", "r2i1p1f1", "r3i1p1f1"],
        variable_id=["tas"],
        frequency="mon",
    )
    ensemble_mean = partial(ops.ensemble_mean, include_std=True)
    dsd = cat.to_dataset_dict(
        ignore_facets=["institution_id", "table_id"],
        operators=[trim_time, ops.global_mean, ensemble_mean],
    )
    assert set(["mean", "std"]) == set(dsd.keys())
