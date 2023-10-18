import re

import xarray as xr


def get_search_criteria(ds: xr.Dataset) -> dict[str, str]:
    """Return a dictionary of facet information from the dataset attributes."""
    keys = [
        "activity_id",
        "experiment_id",
        "frequency",
        "grid_label",
        "institution_id",
        "mip_era",
        "source_id",
        "table_id",
        "variable_id",
        "variant_label",
        "version",
    ]
    search = {key: ds.attrs[key] for key in set(keys).intersection(ds.attrs.keys())}
    return search


def add_variable(variable_id: str, ds: xr.Dataset, catalog) -> xr.Dataset:
    """Search for and add the specified variable to the input dataset.

    This function is intended to be used to add cell measures such as `areacella` and
    `sftlf` to the dataset. As not all model groups upload these variables, we perform a
    sequence of search relaxing facets until we find the variable.

    Parameters
    ----------
    variable_id
        The variable name to add to the input dataset.
    ds
        The dataset to which we will add the `variable_id`.
    catalog
        The ESGFCatalog instance to use to perform the search. This will be cloned so
        that any current search is not altered.
    """
    cat = catalog.clone()  # so we do not interfere with the current search
    search = get_search_criteria(ds)
    for key in [
        "frequency",
        "institution_id",
        "table_id",
        "version",
        "variable_id",
    ]:
        if key in search:
            search.pop(key)
    search.update({"table_id": ["fx", "Ofx"], "variable_id": variable_id})
    # we could just pop all these facets at once, but this way gets you measures which
    # are as 'close' to the original search as we can get
    for relax in ["", "variant_label", "member_id", "experiment_id", "activity_id"]:
        if relax in search:
            search.pop(relax)
        try:
            cat.search(quiet=True, **search)
            cat.df = cat.df.iloc[:1]  # we just need 1
            break
        except ValueError:
            continue
        raise ValueError(f"Could not find {variable_id}")
    # many times the coordinates of the measures differ in only precision of the
    # variables and will lead to unexpected merged results
    var = cat.to_dataset_dict(quiet=True)[variable_id]
    var = var.reindex_like(ds, method="nearest", tolerance=1e-6)
    ds = xr.merge([ds, var[variable_id]])
    return ds


def add_cell_measures(ds: xr.Dataset, catalog) -> xr.Dataset:
    """."""
    to_add = []
    for var, da in ds.items():
        if "cell_measures" not in da.attrs:
            continue
        m = re.search(r"area:\s(.*)", da.attrs["cell_measures"])
        if m:
            to_add.append(m.group(1))
        if "cell_methods" not in da.attrs:
            continue
        if "where land" in da.attrs["cell_methods"]:
            to_add.append("sftlf")
    to_add = set(to_add)
    for add in to_add:
        try:
            ds = add_variable(add, ds, catalog)
        except ValueError:
            pass
    return ds
