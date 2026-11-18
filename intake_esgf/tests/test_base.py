import sys
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
import requests
import xarray as xr
from rich.progress import TaskID

import intake_esgf
import intake_esgf.base as base
from intake_esgf import ESGFCatalog
from intake_esgf.database import create_download_database, get_url_host
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

            def raise_for_status(self):
                if self.status_code != 200:
                    raise requests.exceptions.HTTPError()

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

            def raise_for_status(self):
                if self.status_code != 200:
                    raise requests.exceptions.HTTPError()

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


DATA = b"data"
DATA_SHA256 = "3a6eb0790f39ac87c94f3856b2dd2c5d110e6811602261a9a923d3bb23adc8b7"


class FakeServer:
    """
    Mocks `requests.get` serving `DATA`, optionally honoring range requests and
    optionally dropping the connection after `fail_after` bytes.
    """

    def __init__(self, support_range: bool = True, fail_after: int | None = None):
        self.support_range = support_range
        self.fail_after = fail_after
        self.requests: list[dict[str, str]] = []

    def get(self, url: str, **kwargs: Any) -> "FakeServer.Response":
        headers = kwargs.get("headers", {})
        self.requests.append(headers)
        start = 0
        if self.support_range and "Range" in headers:
            start = int(headers["Range"].removeprefix("bytes=").rstrip("-"))
        return FakeServer.Response(self, start)

    class Response:
        def __init__(self, server: "FakeServer", start: int):
            self.server = server
            self.start = start
            if start >= len(DATA):
                self.status_code = 416
                self.headers = {"Content-Range": f"bytes */{len(DATA)}"}
                return
            self.status_code = 206 if start else 200
            self.headers = (
                {"Content-Range": f"bytes {start}-{len(DATA) - 1}/{len(DATA)}"}
                if start
                else {}
            )

        def raise_for_status(self) -> None:
            pass

        def close(self) -> None:
            pass

        def iter_content(self, chunk_size: int) -> Iterator[bytes]:
            for i in range(self.start, len(DATA)):
                if self.server.fail_after is not None and i >= self.server.fail_after:
                    raise requests.exceptions.ConnectionError("connection reset")
                yield DATA[i : i + 1]


def _download(
    local_file: Path, checksum: str = DATA_SHA256, size: int = len(DATA)
) -> None:
    download_db = local_file.parent / "download.db"
    create_download_database(download_db)
    base.download_and_verify(
        "https://example.org/data.nc",
        local_file,
        checksum,
        "sha256",
        size,
        download_db,
        intake_esgf.conf.get_logger(),
        task_id=TaskID(-1),
        master_id=TaskID(-1),
    )


def test_download_and_verify_permissions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("requests.get", FakeServer().get)
    monkeypatch.setattr(base, "UMASK", 0o022)
    local_file = tmp_path / "data.nc"
    _download(local_file)
    assert local_file.read_bytes() == DATA
    assert not base._partial_file(local_file).exists()
    if sys.platform != "win32":
        # Skip on Windows, as it does not support POSIX file permissions.
        assert local_file.stat().st_mode & 0o777 == 0o644


def test_download_keeps_partial_on_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("requests.get", FakeServer(fail_after=2).get)
    local_file = tmp_path / "data.nc"
    with pytest.raises(requests.exceptions.ConnectionError):
        _download(local_file)
    assert not local_file.exists()
    assert base._partial_file(local_file).read_bytes() == DATA[:2]


@pytest.mark.parametrize("support_range", [True, False])
def test_download_resumes_partial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, support_range: bool
) -> None:
    server = FakeServer(support_range=support_range)
    monkeypatch.setattr("requests.get", server.get)
    local_file = tmp_path / "data.nc"
    base._partial_file(local_file).write_bytes(DATA[:2])
    _download(local_file)
    # whether or not the server honors the range, the result must be intact
    assert server.requests[0]["Range"] == "bytes=2-"
    assert local_file.read_bytes() == DATA
    assert not base._partial_file(local_file).exists()


def test_download_complete_partial_skips_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    server = FakeServer()
    monkeypatch.setattr("requests.get", server.get)
    local_file = tmp_path / "data.nc"
    base._partial_file(local_file).write_bytes(DATA)
    _download(local_file)
    assert server.requests == []
    assert local_file.read_bytes() == DATA


def test_download_hash_mismatch_discards_partial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("requests.get", FakeServer().get)
    local_file = tmp_path / "data.nc"
    with pytest.raises(ValueError):
        _download(local_file, checksum="0" * 64)
    assert not local_file.exists()
    assert base._partial_file(local_file).stat().st_size == 0


@pytest.mark.parametrize("size", [len(DATA) - 1, len(DATA) + 1])
def test_download_trusts_hash_over_index_size(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, size: int
) -> None:
    # the size in the index is wrong, but the hash matches
    monkeypatch.setattr("requests.get", FakeServer().get)
    local_file = tmp_path / "data.nc"
    _download(local_file, size=size)
    assert local_file.read_bytes() == DATA


@pytest.mark.parametrize("size", [0, len(DATA) + 1])
def test_download_verifies_complete_partial_beyond_range(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, size: int
) -> None:
    # the size is unknown or wrong, a complete partial file gets HTTP 416 and is kept
    server = FakeServer()
    monkeypatch.setattr("requests.get", server.get)
    local_file = tmp_path / "data.nc"
    base._partial_file(local_file).write_bytes(DATA)
    _download(local_file, size=size)
    assert server.requests[0]["Range"] == f"bytes={len(DATA)}-"
    assert local_file.read_bytes() == DATA


def test_download_without_hash_does_not_resume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # a partial file which may stem from another host cannot be verified, restart
    server = FakeServer()
    monkeypatch.setattr("requests.get", server.get)
    local_file = tmp_path / "data.nc"
    base._partial_file(local_file).write_bytes(b"xx")
    _download(local_file, checksum="")
    assert "Range" not in server.requests[0]
    assert local_file.read_bytes() == DATA


@pytest.mark.skipif(base.fcntl is None, reason="no file locking available")
def test_discard_empty_partial_respects_lock(tmp_path: Path) -> None:
    part_file = base._partial_file(tmp_path / "data.nc")
    fh = base._open_partial(part_file)
    assert fh is not None
    with fh:
        base._discard_empty_partial(part_file)
        assert part_file.exists()  # in use, keep it
    base._discard_empty_partial(part_file)
    assert not part_file.exists()


def test_open_partial_reopens_removed_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # the partial file is removed by another process while we wait for the lock
    part_file = base._partial_file(tmp_path / "data.nc")
    lock_partial = base._lock_partial
    calls = []

    def lock_after_removal(fh: Any, wait: bool = True) -> bool:
        if not calls:
            part_file.unlink()
        calls.append(fh)
        return lock_partial(fh, wait)

    monkeypatch.setattr(base, "_lock_partial", lock_after_removal)
    monkeypatch.setattr(base, "fcntl", base.fcntl or object())
    fh = base._open_partial(part_file)
    assert fh is not None
    with fh:
        assert len(calls) == 2
        assert base._is_same_file(fh, part_file)


def test_get_url_host() -> None:
    assert get_url_host("https://esgf.ceda.ac.uk/thredds/x.nc") == "esgf.ceda.ac.uk"
    assert get_url_host("http://host:8080/x.nc") == "host:8080"
    assert get_url_host("https://ab") == "ab"


class NoWaitEvent:
    """Stands in for `base.done_event` so retries do not sleep."""

    def is_set(self) -> bool:
        return False

    def wait(self, timeout: float | None = None) -> bool:
        return False


def _parallel_download(tmp_path: Path, urls: list[str]) -> tuple[Any, Any]:
    download_db = tmp_path / "download.db"
    create_download_database(download_db)
    info = {
        "key": "key",
        "path": Path("data.nc"),
        "HTTPServer": urls,
        "checksum": DATA_SHA256,
        "checksum_type": "sha256",
        "size": len(DATA),
    }
    return base.parallel_download(
        info,
        [tmp_path],
        download_db,
        intake_esgf.conf.get_logger(),
        [],
        task_id=TaskID(-1),
        master_id=TaskID(-1),
    )


def test_parallel_download_retries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # the connection drops after every byte, each attempt resumes one byte further
    server = FakeServer()
    calls = []

    def flaky_get(url: str, **kwargs: Any) -> FakeServer.Response:
        calls.append(url)
        resp = server.get(url, **kwargs)
        server.fail_after = resp.start + 1
        return resp

    monkeypatch.setattr("requests.get", flaky_get)
    monkeypatch.setattr(base, "done_event", NoWaitEvent())
    with intake_esgf.conf.set(download_retries=len(DATA)):
        key, local_file = _parallel_download(tmp_path, ["https://a.org/data.nc"])
    assert key == "key"
    assert local_file.read_bytes() == DATA
    assert len(calls) == len(DATA)

    # without retries a single failed attempt is final
    (tmp_path / "data.nc").unlink()
    server.fail_after = 0
    with intake_esgf.conf.set(download_retries=0):
        key, _ = _parallel_download(tmp_path, ["https://a.org/data.nc"])
    assert key is None


def test_parallel_download_drops_permanent_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = []

    def not_found(url: str, local_file: Path, *args: Any, **kwargs: Any) -> None:
        calls.append(url)
        response = requests.Response()
        response.status_code = 404
        raise requests.HTTPError(response=response)

    monkeypatch.setattr(base, "download_and_verify", not_found)
    monkeypatch.setattr(base, "done_event", NoWaitEvent())
    urls = ["https://a.org/data.nc", "https://b.org/data.nc"]
    with intake_esgf.conf.set(download_retries=3):
        key, _ = _parallel_download(tmp_path, urls)
    assert key is None
    assert sorted(calls) == sorted(urls)  # each link tried once, never retried


def test_parallel_download_retries_hash_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # corrupt data may stem from an earlier attempt, so the link is not dropped
    calls = []

    def corrupt(url: str, local_file: Path, *args: Any, **kwargs: Any) -> None:
        calls.append(url)
        raise ValueError("Hash does not match")

    monkeypatch.setattr(base, "download_and_verify", corrupt)
    monkeypatch.setattr(base, "done_event", NoWaitEvent())
    with intake_esgf.conf.set(download_retries=2):
        key, _ = _parallel_download(tmp_path, ["https://a.org/data.nc"])
    assert key is None
    assert len(calls) == 3


def test_get_search_criteria(dataset):
    reference_search = dict(
        activity_id="CMIP",
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

    ds = base.add_cell_measures(ds, ESGFCatalog())
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
    expected = {
        "CMIP6": Path(
            "CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/fx/sftlf/gn/v20190429/sftlf_fx_CanESM5_historical_r1i1p1f1_gn.nc"
        ),
        "CMIP5": Path(
            "CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/Emon/wetlandFrac/gn/v20190429/wetlandFrac_Emon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc"
        ),
        "CMIP3": Path(
            "cmip3/BCCR/bccr_bcm2_0/historical/fx/land/run1/sftlf/v1/sftlf_A1.nc"
        ),
        "obs4MIPs": Path(
            "obs4MIPs/NASA-LaRC/CERES-EBAF-4-2/mon/rsdt/gn/v20240513/rsdt_mon_CERES-EBAF-4-2_RSS_gn_200003-202310.nc"
        ),
        "input4MIPs": Path(
            "input4MIPs/CMIP7/C4MIP/ImperialCollege/ImperialCollege-3-0/atmos/yr/delta13co2/gm/v20250502/delta13co2_input4MIPs_atmosphericState_C4MIP_ImperialCollege-3-0_gm_1700-2023.nc"
        ),
    }
    for project in project_contents:
        path = base.get_content_path(project_contents[project])
        assert path == expected[project]


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
        project="CMIP6",
        variable_id="areacello",
        source_id="UKESM1-0-LL",
    )

    cat.to_path_dict()


@pytest.mark.parametrize(
    "hash,hash_algorithm",
    [
        ["3a6eb0790f39ac87c94f3856b2dd2c5d110e6811602261a9a923d3bb23adc8b7", "sha256"],
        [
            "sha256:3a6eb0790f39ac87c94f3856b2dd2c5d110e6811602261a9a923d3bb23adc8b7",
            None,
        ],
        ["12203a6eb0790f39ac87c94f3856b2dd2c5d110e6811602261a9a923d3bb23adc8b7", None],
    ],
)
def test_hashing(hash: str, hash_algorithm: str | None):
    hasher = base._setup_hasher(hash, hash_algorithm)
    if hasher is None:
        raise ValueError("Unexpected None multihash")
    assert hasher.verify(b"data")


def test_get_total_size():
    size, unit = base.get_total_size([{"size": 1e9}])
    assert np.allclose(size, 1000)
    assert unit == "Mb"
    size, unit = base.get_total_size([{"size": 1e12}])
    assert np.allclose(size, 1000)
    assert unit == "Gb"
