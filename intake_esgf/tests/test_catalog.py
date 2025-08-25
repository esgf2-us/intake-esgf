import pytest

import intake_esgf
from intake_esgf.database import log_download_information
from intake_esgf.projects import CMIP6


def test_init():
    with intake_esgf.conf.set(no_indices=True):
        with pytest.raises(ValueError):
            intake_esgf.ESGFCatalog()


def test_set_project(df_catalog):
    cat = intake_esgf.ESGFCatalog()
    # no search has been done
    with pytest.raises(ValueError):
        cat._set_project()
    # fake a CMIP6 search
    cat.df = df_catalog
    cat._set_project()
    assert isinstance(cat.project, CMIP6)
    # fake a mixed project search
    cat.df.loc[cat.df.index[-1], "project"] = "CMIP5"
    with pytest.raises(ValueError):
        cat._set_project()


def test_minimal_key_format(catalog):
    assert set(catalog._minimal_key_format()) == set(["table_id", "variable_id"])
    assert set(catalog._minimal_key_format(ignore_facets="table_id")) == set(
        ["variable_id"]
    )


def test_clone(catalog):
    clone = catalog.clone()
    assert len(catalog.indices) == len(clone.indices)
    assert clone.df is None


def test_unique(catalog):
    df = catalog.unique()
    for col, values in df.items():
        if col in ["table_id", "variable_id"]:
            assert len(values) == 2
        else:
            assert len(values) == 1


def test_model_groups(catalog):
    # light testing here, more detailed for each project
    grps = catalog.model_groups()
    assert len(grps) == 1
    assert grps.iloc[0] == 2


# def test_search():
#    pass

# def test_from_tracking_ids():
#    pass

# def test_get_file_info():
#    pass

# def test_to_path_dict():
#    pass

# def test_to_dataset_dict():
#    pass

# def test_remove_incomplete():
#    pass

# def test_remove_ensembles():
#    pass


def test_session_log(catalog):
    log = catalog.session_log()
    assert log == ""


def test_download_summary(catalog):
    log_download_information(catalog.download_db, "summary", 999.0, 1334.0)
    print(catalog.download_summary())
    assert "summary" in catalog.download_summary().index


# def test_variable_info():
#    pass


# def test_load_into_dsd():
#    pass
