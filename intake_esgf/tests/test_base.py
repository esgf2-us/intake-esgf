import tempfile
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

import intake_esgf
import intake_esgf.base as base
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


def test_partition_infos():
    pass


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


def test_download_and_verify():
    pass


def test_parallel_download():
    pass


def test_get_search_criteria():
    pass


def test_add_variable():
    pass


def test_add_cell_measures():
    pass


def test_expand_cmip5_record():
    pass


def test_get_content_path():
    pass


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
