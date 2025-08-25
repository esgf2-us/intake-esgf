import tempfile
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import xarray as xr

import intake_esgf
import intake_esgf.base as base
from intake_esgf import ESGFCatalog
from intake_esgf.exceptions import NoSearchResults, ProjectNotSupported


def test_get_local_file():
    _, local_file = tempfile.mkstemp()
    local_file = Path(local_file)
    # test file is in esgroots
    out = base.get_local_file(Path(local_file.name), [Path(local_file).parent])
    assert Path(out) == local_file
    # test file not in esgroots
    with pytest.raises(FileNotFoundError):
        out = base.get_local_file(Path(local_file.name), [Path(".")])


def test_get_globus_endpoints(file_info):
    endpoints = base.get_globus_endpoints(file_info)
    assert len(endpoints) == 1


def test_select_streaming_links(file_info, df_rate, monkeypatch):
    def fake_head(url: str, timeout: int) -> Any:
        class FakeResponse:
            status_code = 200 if url.startswith("http://esgf-node.ornl.gov") else 400

        return FakeResponse()

    # Replace the head functionality with the above
    monkeypatch.setattr("requests.head", fake_head)

    # Check a link that 'works'
    link = base.select_streaming_link(
        file_info["OPENDAP"],
        df_rate,
    )
    assert link.startswith("http://esgf-node.ornl.gov")

    # Check a link that does not 'work'
    with pytest.raises(ValueError):
        link = base.select_streaming_link(
            file_info["OPENDAP"][1:],
            df_rate,
        )


def test_partition_infos(file_infos, monkeypatch):
    # Partitioning streaming links requires a response check which we will fake
    def fake_head(url: str, timeout: int) -> Any:
        class FakeResponse:
            status_code = 200

        return FakeResponse()

    # Partitioning globus links requires an authorized client which we will fake
    def fake_authorized_transfer_client() -> Any:
        class FakeClient:
            def get_endpoint(self, uuid: int) -> dict[str, Any]:
                return {"acl_available": True}

        return FakeClient()

    monkeypatch.setattr("requests.head", fake_head)
    monkeypatch.setattr(
        "intake_esgf.core.globus.get_authorized_transfer_client",
        fake_authorized_transfer_client,
    )

    # Check only https preference
    partitioned_infos, ds = base.partition_infos(
        file_infos, prefer_streaming=False, prefer_globus=False
    )
    assert max([len(partitioned_infos[p]) for p in ["exist", "stream", "globus"]]) == 0
    assert len(partitioned_infos["https"]) == 3
    assert len(ds) == 0
    # Check streaming preference
    partitioned_infos, ds = base.partition_infos(
        file_infos, prefer_streaming=True, prefer_globus=False
    )
    assert len(partitioned_infos["exist"]) == 0
    assert len(partitioned_infos["https"]) == 2  # 2 do not have stream access
    assert len(partitioned_infos["stream"]) == 1
    assert len(partitioned_infos["globus"]) == 0
    assert len(ds) == 1
    # Check globus preference
    partitioned_infos, ds = base.partition_infos(
        file_infos, prefer_streaming=False, prefer_globus=True
    )
    assert len(partitioned_infos["exist"]) == 0
    assert len(partitioned_infos["https"]) == 2  # 2 do not have globus access
    assert len(partitioned_infos["stream"]) == 0
    assert len(partitioned_infos["globus"]) == 1
    assert len(ds) == 0


def test_combine_results(df_ornl, df_ceda):
    # Nothing to combine
    with pytest.raises(NoSearchResults):
        base.combine_results(
            [pd.DataFrame(), pd.DataFrame()], intake_esgf.conf.get_logger()
        )

    # Single project at a time
    with pytest.raises(ValueError):
        base.combine_results(
            [
                pd.DataFrame([{"project": "obs4mips"}]),
                pd.DataFrame([{"project": "CMIP6"}]),
            ],
            intake_esgf.conf.get_logger(),
        )

    # Only projects we support
    with pytest.raises(ProjectNotSupported):
        base.combine_results(
            [
                pd.DataFrame([{"project": "something_unsupported"}]),
            ],
            intake_esgf.conf.get_logger(),
        )

    # Combine some responses that we have cached
    df = base.combine_results([df_ornl, df_ceda], intake_esgf.conf.get_logger())
    assert len(df) == 1
    assert len(df["id"].iloc[0]) == 8


# def test_download_and_verify():
#    pass


# def test_parallel_download():
#    pass


def test_get_search_criteria(dataset):
    reference_search = dict(
        institution_id="CCCma",
        source_id="CanESM5",
        experiment_id="historical",
        member_id="r1i1p1f1",
        table_id="Amon",
        variable_id="tas",
        grid_label="gn",
    )
    # infer project from attributes
    search = base.get_search_criteria(dataset)
    assert search == reference_search
    # specify project in argument
    dataset.attrs.pop("project")
    search = base.get_search_criteria(dataset, project_id="CMIP6")
    assert search == reference_search
    # project has to be in the file or given in the function call
    with pytest.raises(ValueError):
        search = base.get_search_criteria(dataset)
    # it must be something we support
    with pytest.raises(ProjectNotSupported):
        search = base.get_search_criteria(dataset, project_id="Unsupported")


# def test_add_variable():
#    pass


def test_add_cell_measures(monkeypatch):
    def fake_add_variable(add, ds, catalog):
        ds[add] = xr.DataArray(data=[0], dims=["lat"], coords=dict(lat=[45]))
        return ds

    ds = xr.merge(
        [
            dict(
                junk1=xr.DataArray(
                    name="junk1",
                    data=[0],
                    dims=["lat"],
                    coords=dict(lat=[45]),
                    attrs=dict(
                        cell_measures="something_wrong",
                    ),
                ),
                junk2=xr.DataArray(
                    name="junk2",
                    data=[0],
                    dims=["lat"],
                    coords=dict(lat=[45]),
                ),
                atm=xr.DataArray(
                    name="atm",
                    data=[0],
                    dims=["lat"],
                    coords=dict(lat=[45]),
                    attrs=dict(
                        cell_measures="area: areacella",
                        cell_methods="area: time: mean",
                    ),
                ),
                lnd=xr.DataArray(
                    name="lnd",
                    data=[0],
                    dims=["lat"],
                    coords=dict(lat=[45]),
                    attrs=dict(
                        cell_measures="area: areacella",
                        cell_methods="area: mean where land time: mean",
                    ),
                ),
                ocn=xr.DataArray(
                    name="ocn",
                    data=[0],
                    dims=["lat"],
                    coords=dict(lat=[45]),
                    attrs=dict(
                        cell_measures="area: areacello",
                        cell_methods="area: mean where sea time: mean",
                    ),
                ),
            )
        ]
    )
    monkeypatch.setattr("intake_esgf.base.add_variable", fake_add_variable)

    ds = base.add_cell_measures(ds, None)
    assert "sftof" in ds
    assert "sftlf" in ds
    assert "areacella" in ds
    assert "areacello" in ds


def test_expand_cmip5_record(cmip5_record):
    # just copies records based on the intersection of lists
    assert (
        len(
            base.expand_cmip5_record(
                ["clw"],
                [
                    "clw",
                    "evspsbl",
                ],
                cmip5_record,
            )
        )
        == 1
    )
    # if nothing in the search, we want it all
    assert (
        len(
            base.expand_cmip5_record(
                [],
                [
                    "clw",
                    "evspsbl",
                ],
                cmip5_record,
            )
        )
        == 2
    )


def test_get_content_path(project_contents):
    # TODO: Expand coverage to other projects
    # Normal behavior using template
    path = base.get_content_path(project_contents["CMIP6"])
    assert path == Path(
        "CMIP6/C4MIP/MPI-M/MPI-ESM1-2-LR/esm-ssp585/r4i1p1f1/Emon/nppTree/gn/v20190815/nppTree_Emon_MPI-ESM1-2-LR_esm-ssp585_r4i1p1f1_gn_201501-203412.nc"
    )


def test_get_time_extent():
    # normal case
    t0, _ = base.get_time_extent(
        "tas_Amon_EC-Earth3_historical_r1i1p1f1_gr_185301-185312.nc"
    )
    assert t0 == pd.Timestamp("1853-01")
    # date pattern not found
    t0, _ = base.get_time_extent("tas_Amon_EC-Earth3_historical_r1i1p1f1_gr.nc")
    assert t0 is None
    # date found but invalid
    t0, _ = base.get_time_extent(
        "tas_Amon_EC-Earth3_historical_r1i1p1f1_gr_18530230-185312.nc"
    )
    assert t0 is None


def test_download_without_checksum(monkeypatch):
    """Test that downloading a file without a checksum works."""
    _get_file_info = ESGFCatalog._get_file_info

    def _get_stripped_file_info(self, *args, **kwargs):
        """Set the checksum to None in the search result."""
        result = _get_file_info(self, *args, **kwargs)
        for info in result:
            info["checksum"] = None
        return result

    monkeypatch.setattr(ESGFCatalog, "_get_file_info", _get_stripped_file_info)

    cat = ESGFCatalog().search(
        variable_id="areacello",
        source_id="UKESM1-0-LL",
    )

    cat.to_path_dict()
