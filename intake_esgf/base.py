"""General functions used in various parts of intake-esgf."""

import contextlib
import os
import re
import signal
import time
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from threading import Event
from typing import Any

import multihash  # type: ignore
import pandas as pd
import requests
import xarray as xr
from globus_sdk import TransferAPIError
from multihash import Multihash  # type: ignore
from rich.progress import Progress, TaskID

import intake_esgf
import intake_esgf.core.globus as globus
import intake_esgf.logging
from intake_esgf.database import (
    get_download_rate_dataframe,
    log_download_information,
    sort_download_links,
)
from intake_esgf.exceptions import (
    DownloadIncomplete,
    NoSearchResults,
    ProjectNotSupported,
    StalledDownload,
)
from intake_esgf.projects import projects


def _get_umask() -> int:
    """Return the process umask (os.umask can only be read by setting it)."""
    umask = os.umask(0o777)
    os.umask(umask)
    return umask


# Read once at import time since setting the umask is not thread-safe.
UMASK = _get_umask()


def get_local_file(path: Path, dataroots: list[Path]) -> Path:
    """
    Return the local path to a file if it exists.

    Parameters
    ----------
    path : Path
        The path of the file relative to a `esgroot`.
    dataroots : list[Path]
        A list of roots to prepend to `path` to check for existence.

    Returns
    -------
    Path
        A local path to a file which exists.

    Raises
    ------
    FileNotFoundError
        If the file does not exist at any of the dataroots.
    """
    for root in dataroots:
        local_file = (root / path).expanduser()
        if local_file.is_file():
            return local_file
    raise FileNotFoundError


def get_globus_endpoints(info: dict) -> list[str]:
    """
    Return the Globus endpoints found in the file info.

    Parameters
    ----------
    info : dict
        A file info record as returned by the Solr/Globus responses.

    Returns
    -------
    list[str]
        A list of the Globus endpoint UUIDs where this file may be found.

    Raises
    ------
    ValueError
        If the UUID cannot be parsed from the Globus 'link'.
    """
    if "Globus" not in info:
        return []
    globus_endpoints = []
    for entry in info["Globus"]:
        m = re.search(r"globus:/*([a-z0-9\-]+)/(.*)", entry)
        if not m:
            raise ValueError(f"Globus 'link' count not be parsed: {entry}")
        uuid = m.group(1)
        globus_endpoints.append(uuid)
    # Blacklist the OLCF endpoint because the paths in the index are not correct
    # and lead to file not found errors.
    BLACKLIST = ["dea29ae8-bb92-4c63-bdbc-260522c92fe8"]
    globus_endpoints = list(set(globus_endpoints) - set(BLACKLIST))
    return globus_endpoints


def select_streaming_link(links: list[str], df_rate: pd.DataFrame) -> str:
    """
    Select a streaming link from a list of options based on fastest download speeds.
    """
    # There are some .nc and .nc.html links in these records, only the .nc links
    # can be read by xarray.
    links = [link for link in links if not link.endswith(".html")]
    # Sort the links by the fastest hostname we have seen when transferring.
    links = sorted(
        links, key=partial(sort_download_links, df_rate=df_rate), reverse=True
    )
    # Return the first of these links whose html page returns a valid response.
    # This is particular to OPENDAP and will need rethought for other virtual
    # methods.
    for link in links:
        resp = requests.head(link, timeout=10)
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError:
            continue
        return link
    raise ValueError(f"None of these links appears functional {links}")


def partition_infos(
    infos: list[dict], prefer_streaming: bool, prefer_globus: bool
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[str | Path]]]:
    """
    Partition the file info based on how it will be handled.

    Each file may have many access options. Here we partition the file infos according
    to the access method we will use. This is based on a priority as well as user
    options. The priority uses: (1) local files if present, (2) streaming if possible
    and requested, (3) globus transfer if possible and request, and finally (4) https
    download. If no transfer is needed, then we also begin building the output
    dictionary of paths/datasets.

    Parameters
    ----------
    infos : list[dict]
        A list of file info as returned in the Solr/Globus response.
    prefer_streaming : bool
        Enable to use OPENDAP/VirtualZarr options if present in the info.
    prefer_globus : bool
        Enable to use Globus transfer options if present in the info.

    Returns
    -------
    dict[list]
        A dicionary of partitioned infos based on the access method.
    ds
        A dictionary of access paths for use in xarray.open_dataset.
    """
    # this routine will eventually return a dictionary of key -> list[paths]
    ds: dict[str, list[str | Path]] = {}

    # as we iterate through the infos we will partition them
    infos_exist = []
    infos_stream = []
    infos_globus = []
    infos_https = []

    # to keep from checking globus endpoints active status too much, we will store them
    client = None
    active_endpoints: set[str] = set()

    # Partition and setup all the file infos based on a priority
    for i, info in enumerate(infos):
        key = info["key"]

        # 1) does the file already exist locally?
        try:
            local_path = get_local_file(
                info["path"],
                intake_esgf.conf["esg_dataroot"] + intake_esgf.conf["local_cache"],
            )
            if key not in ds:
                ds[key] = []
            ds[key].append(local_path)
            infos_exist.append(info)
            continue
        except FileNotFoundError:
            pass

        # 2) does the user prefer to stream data?
        if prefer_streaming:
            # What possible links are there to stream from, sorted by types
            links = []
            for src in intake_esgf.conf["streaming_preference"]:
                links += [link for link in info.get(src, [])]
                if links:
                    # If we find any kind of link we prefer, stop looking
                    break

            if links:
                if key not in ds:
                    ds[key] = []
                ds[key] += links
                infos_stream.append(info)
                continue

        # 3) does the user prefer to use globus transfer?
        if prefer_globus:
            source_endpoints = get_globus_endpoints(info)
            # before removing these from infos of what we will download, check that
            # their endpoints actually work
            for uuid in source_endpoints:
                if uuid in active_endpoints:
                    continue
                client = (
                    globus.get_authorized_transfer_client()
                    if client is None
                    else client
                )
                try:
                    ep = client.get_endpoint(uuid)
                    if ep["acl_available"]:
                        active_endpoints = active_endpoints | set([uuid])
                except TransferAPIError:
                    pass
            # if at least one endpoint is active, then we will use globus
            source_endpoints = list(active_endpoints & set(source_endpoints))
            if source_endpoints:
                # store this information for later
                infos[i]["active_endpoints"] = source_endpoints
                infos_globus.append(info)
                continue

        # 4) the rest we need to download using https, even if no https links are
        #    available. We will error this condititon later.
        infos_https.append(info)

    # was the data properly partitioned?
    assert len(infos) == (
        len(infos_exist) + len(infos_stream) + len(infos_globus) + len(infos_https)
    )
    return {
        "exist": infos_exist,
        "stream": infos_stream,
        "globus": infos_globus,
        "https": infos_https,
    }, ds


def combine_results(
    dfs: Iterable[pd.DataFrame],
    logger: intake_esgf.logging.Logger,
) -> pd.DataFrame:
    """Return a combined dataframe where ids are now a list."""
    # combine and remove duplicate entries
    df = pd.concat(dfs)
    if len(df) == 0:
        logger.info("\x1b[36;32msearch end \x1b[91;20mno results\033[0m")
        raise NoSearchResults()
    # retrieve project information about how to combine results
    project_id = df["project"].unique()
    if len(project_id) != 1:
        raise ValueError(
            f"Only single project queries are supported, but found {project_id}"
        )
    project_id = project_id[0]
    project = projects.get(project_id.lower(), None)
    if project is None:
        raise ProjectNotSupported(project_id)
    variable_facet = project.variable_facet()
    combine_time = time.time()
    df = df.drop_duplicates(subset=[variable_facet, "id"]).reset_index(drop=True)
    # now convert groups to list, if we initialize a column with empty lists...
    df["locations"] = [[] for _ in range(len(df))]
    for _, grp in df.groupby(project.master_id_facets(), dropna=False):
        df = df.drop(grp.iloc[1:].index)
        # ... then here we can overwrite lists of varying lengths...
        df.at[grp.index[0], "locations"] = grp.id.to_list()  # type: ignore
    # ... and finally overwrite the id column and drop
    df["id"] = df["locations"]
    df = df.drop(columns=["data_node", "locations"])
    combine_time = time.time() - combine_time
    logger.info(f"{combine_time=:.2f}")
    return df


def _get_mh_func_by_name(name: str | None) -> multihash.Func:
    """
    Get the hashing function from multihash, compatible with hashlib's names.
    """
    if name is None:
        raise KeyError(f"Could not find '{name}' in multihash's database of functions.")

    # First try to simple return one of the mh funcs
    try:
        return multihash.funcs.FuncReg.get(name)
    except KeyError:
        pass

    # Then check if a hashlib compatible name was used and return if found
    possible = [
        fcn
        for fcn, fname, _ in multihash.FuncReg._std_func_data
        if fname == name.lower()
    ]
    if len(possible) == 1:
        return possible[0]
    raise KeyError(f"Could not find '{name}' in multihash's database of functions.")


def _setup_hasher(hash: str | None, hash_algorithm: str | None) -> Multihash | None:
    """
    Given hash information, return a multihasher compatible with hashlib if needed.
    """
    # We may have been passed no hashing information at all, so just return
    if hash is None:
        return None

    # Some indices are publishing hashes with a `hash_algorithm:hash` convention.
    if ":" in hash and hash_algorithm is None:
        hash_algorithm, hash = hash.split(":")

    # If a hash has been given, try to decode it as a multihash. This is being
    # introduced in ESGF-NG as it is part of the STAC file extension.
    byte_hash = multihash.from_hex_string(hash)
    if byte_hash and multihash.is_valid(byte_hash):
        mh = multihash.decode(byte_hash)
        return mh

    # If we made it here, we need to create multihash compatible with regular hashes
    try:
        hash_fnc = _get_mh_func_by_name(hash_algorithm)
        mh = Multihash(func=hash_fnc, digest=byte_hash)
    except Exception:
        return None
    return mh


PROGRESS = Progress()
MASTER_ID = 0
done_event = Event()


def handle_sigint(signum, frame):
    done_event.set()


signal.signal(signal.SIGINT, handle_sigint)


def _partial_file(local_file: Path) -> Path:
    """Return the path where a partial download of `local_file` is stored."""
    return local_file.with_name(f"{local_file.name}.part")


def _remove_if_empty(path: Path) -> None:
    """Remove a file if it exists and has no content."""
    try:
        if path.stat().st_size == 0:
            path.unlink()
    except OSError:
        pass


def _lock_partial(fh: Any) -> bool:
    """
    Take an exclusive lock on an open partial download file.

    This keeps several processes which share a cache from writing into the same
    partial file. The lock is released by the OS when the file is closed or the
    process dies. Where locking is not available (Windows, some network
    filesystems) we proceed without it.

    Returns
    -------
    bool
        False if we were interrupted while waiting for the lock.
    """
    try:
        import fcntl
    except ImportError:
        return True
    while True:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except BlockingIOError:
            # another process is downloading this file, wait for it to finish
            if done_event.wait(1.0):
                return False
        except OSError:
            return True


def _url_host(url: str) -> str:
    """Return the host part of a url."""
    return url[: url.index("/", 10)].replace("http://", "").replace("https://", "")


def _resume_offset(fdl: Any, content_length: int) -> int:
    """
    Return the byte offset from which to continue a partial download.

    A partial file larger than the expected size cannot be right and is discarded.
    """
    offset = os.fstat(fdl.fileno()).st_size
    if content_length > 0 and offset > content_length:
        fdl.truncate(0)
        return 0
    return offset


def _request_from(url: str, fdl: Any, offset: int) -> tuple[requests.Response, int]:
    """
    Request the url starting at the byte `offset` of a partial file.

    Returns the response and the offset at which its content starts. If the server
    ignores the range request we get the whole file and restart the partial file.
    """
    headers = {"Accept-Encoding": "identity"}  # byte offsets must be of raw data
    if offset:
        headers["Range"] = f"bytes={offset}-"
    resp = requests.get(url, stream=True, timeout=10, headers=headers)
    if offset and resp.status_code == 206:
        match = re.match(r"bytes\s+(\d+)-", resp.headers.get("Content-Range", ""))
        if match and int(match.group(1)) == offset:
            return resp, offset
    if offset and resp.status_code in (206, 416):
        # the server cannot continue where we left off, start over next attempt
        resp.close()
        fdl.truncate(0)
        raise DownloadIncomplete(f"cannot resume at {offset} [bytes] from {url}")
    try:
        resp.raise_for_status()
    except requests.HTTPError:
        resp.close()
        raise
    if offset:
        # the server ignored the range and sends the whole file
        fdl.truncate(0)
    return resp, 0


class _SlowDownloadDetector:
    """Track the transfer rate with exponential smoothing to detect stalls."""

    SMOOTHING = 0.3  # must be in [0,1]
    SAFETY = 2.0  # [s] grace period before we judge a transfer

    def __init__(self, threshold: float):
        self.threshold = threshold  # [Mb s-1]
        self.rate = threshold
        self.start = self.prev = time.perf_counter()

    def is_slow(self, nbytes: int) -> bool:
        """Register `nbytes` received now, return if the transfer is too slow."""
        now = time.perf_counter()
        rate = nbytes * 1e-6 / max(now - self.prev, 1e-9)
        self.rate = self.SMOOTHING * rate + (1 - self.SMOOTHING) * self.rate
        self.prev = now
        return (now - self.start) > self.SAFETY and self.rate < self.threshold


def _stream_to_file(
    resp: requests.Response,
    fdl: Any,
    url: str,
    download_db: Path,
    logger: intake_esgf.logging.Logger,
    break_slow_downloads: bool,
    task_id: TaskID | None,
) -> bool:
    """
    Append the content of the response to the open file.

    Returns
    -------
    bool
        False if the download was interrupted by the user.

    Raises
    ------
    StalledDownload
        If `break_slow_downloads` and the transfer rate falls below the threshold.
    """
    CHUNKSIZE = 2**20  # 1 [Mb]
    threshold = intake_esgf.conf["slow_download_threshold"]
    detector = _SlowDownloadDetector(threshold)
    with contextlib.closing(resp):
        for chunk in resp.iter_content(chunk_size=CHUNKSIZE):
            if not chunk:
                continue
            if break_slow_downloads and detector.is_slow(len(chunk)):
                logger.info(
                    f"\x1b[38;5;209mbreaking slow download\033[0m {detector.rate:.2f} < {threshold:.2f} [Mb s-1] {url}"
                )
                elapsed = time.perf_counter() - detector.start
                log_download_information(
                    download_db, _url_host(url), elapsed, detector.rate * elapsed
                )
                raise StalledDownload()
            fdl.write(chunk)
            if task_id is not None:
                PROGRESS.update(task_id, advance=len(chunk))
            if done_event.is_set():
                return False
    fdl.flush()
    return True


def _verify_partial(
    fdl: Any,
    part_file: Path,
    content_length: int,
    hash: str,
    hash_algorithm: str,
    url: str,
    logger: intake_esgf.logging.Logger,
) -> None:
    """Check size and hash of a finished partial file, emptying it if corrupt."""
    size = os.fstat(fdl.fileno()).st_size
    if content_length > 0 and size != content_length:
        raise DownloadIncomplete(
            f"received {size} of {content_length} [bytes] from {url}"
        )
    hasher = _setup_hasher(hash, hash_algorithm)
    if hasher is None:
        logger.info(
            f"\x1b[91;20m{part_file=} could not be verified with given {hash_algorithm=} and {hash=}, skipping.\033[0m"
        )
        return
    with open(part_file, "rb") as fin:
        verified = hasher.verify(fin.read())
    if not verified:
        logger.info(f"\x1b[91;20mHash error\033[0m {url}")
        fdl.truncate(0)  # start over, the data is corrupt
        raise ValueError("Hash does not match")


def _finalize(part_file: Path, local_file: Path) -> None:
    """Move a verified partial file into place, applying the default umask."""
    part_file.chmod(0o666 & ~UMASK)
    part_file.replace(local_file)


def download_and_verify(
    url: str,
    local_file: str | Path,
    hash: str,
    hash_algorithm: str,
    content_length: int,
    download_db: Path,
    logger: intake_esgf.logging.Logger,
    task_id: TaskID,
    master_id: TaskID,
    break_slow_downloads: bool = False,
    quiet: bool = False,
) -> None:
    """
    Download the url to a local file and check for validity, removing if not.

    Data is written to a partial file (`<name>.part`) next to the local file which
    is renamed once verified. If a partial file is already present, for example
    from an interrupted or stalled earlier attempt, we resume from where it left
    off using an HTTP range request.
    """
    local_file = Path(local_file)
    local_file.parent.mkdir(parents=True, exist_ok=True)
    part_file = _partial_file(local_file)
    show_progress = not (quiet or task_id < 0 or master_id < 0)

    # Append mode never truncates existing data and always writes at the end.
    with open(part_file, "ab") as fdl:
        if not _lock_partial(fdl):
            return  # interrupted while waiting
        if local_file.is_file():
            # Another process finished the download while we waited for the lock.
            # Nobody writes the partial file anymore, an empty one is safe to remove.
            _remove_if_empty(part_file)
            return
        offset = _resume_offset(fdl, content_length)
        transfer_time = time.time()
        if content_length <= 0 or offset < content_length:
            resp, offset = _request_from(url, fdl, offset)
            if offset:
                logger.info(f"resuming download at {offset} [bytes] {url}")
            if show_progress:
                PROGRESS.update(task_id, total=content_length, completed=offset)
                PROGRESS.start_task(task_id)
            if not _stream_to_file(
                resp,
                fdl,
                url,
                download_db,
                logger,
                break_slow_downloads,
                task_id if show_progress else None,
            ):
                return  # interrupted, keep the partial file to pick up later
        size = os.fstat(fdl.fileno()).st_size
        _verify_partial(
            fdl, part_file, content_length, hash, hash_algorithm, url, logger
        )
        if os.name == "nt":
            fdl.close()  # Windows cannot rename an open file (and has no lock)
        # rename while still holding the lock so no other process can interfere
        _finalize(part_file, local_file)

    # Log transfer information, only counting what we transferred in this call
    transfer_time = time.time() - transfer_time
    transferred = size - offset
    MIN_TRANSFER_TIME = 1e-3  # 1 [ms]
    rate = transferred * 1e-6 / max(transfer_time, MIN_TRANSFER_TIME)
    logger.info(f"{transfer_time=:.2f} [s] at {rate:.2f} [Mb s-1] {url}")
    if transferred > 0:
        log_download_information(
            download_db, _url_host(url), transfer_time, transferred * 1e-6
        )
    if show_progress:
        PROGRESS.update(master_id, advance=1)


def _is_permanent_failure(exc: Exception) -> bool:
    """Is this an error for which retrying the same link is pointless?"""
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        status = exc.response.status_code
        return 400 <= status < 500 and status not in (408, 429)
    # a hash mismatch means this host serves a corrupt copy
    return isinstance(exc, ValueError)


def parallel_download(
    info: dict[str, Any],
    local_cache: list[Path],
    download_db: Path,
    logger: intake_esgf.logging.Logger,
    esg_dataroot: list[Path],
    task_id: TaskID,
    master_id: TaskID,
):
    """
    Download a single file, trying all of its links and retrying on failure.

    Each round tries every link in order of expected transfer speed. Links which
    failed permanently (HTTP 4xx, hash mismatch) are dropped, and we wait with an
    exponential backoff between rounds. Partial downloads are kept between attempts
    so that a later attempt, possibly from another host, resumes where the previous
    one stopped.
    """
    # does this exist on a copy we have access to?
    try:
        local_file = get_local_file(info["path"], esg_dataroot + local_cache)
        logger.info(f"accessed {local_file}")
        return info["key"], local_file
    except FileNotFoundError:
        pass

    # files may end up here without any https links, nothing we can do for them
    if not info.get("HTTPServer"):
        logger.info(f"\x1b[91;20mno https links\033[0m {info['path']}")
        return None, None

    # else we try to download it, first we sort links by the fastest host to you
    df_rate = get_download_rate_dataframe(download_db)
    urls = sorted(
        info["HTTPServer"],
        key=partial(sort_download_links, df_rate=df_rate),
        reverse=True,
    )
    local_file = local_cache[0] / info["path"]
    retries = max(int(intake_esgf.conf["download_retries"]), 0)
    MAX_BACKOFF = 60.0  # [s]
    for attempt in range(retries + 1):
        if attempt:
            wait = min(2.0**attempt, MAX_BACKOFF)
            logger.info(
                f"retrying download in {wait:.0f} [s] (attempt {attempt + 1}/{retries + 1}) {info['path']}"
            )
            if done_event.wait(wait):
                break
        for url in list(urls):
            if done_event.is_set():
                break
            try:
                download_and_verify(
                    url,
                    local_file,
                    info["checksum"],
                    info["checksum_type"],
                    info["size"],
                    download_db=download_db,
                    logger=logger,
                    break_slow_downloads=(url != urls[-1]),
                    task_id=task_id,
                    master_id=master_id,
                )
            except StalledDownload:
                continue
            except Exception as e:
                logger.info(
                    f"\x1b[91;20mdownload failed\033[0m {type(e).__name__}: {e} {url}"
                )
                if _is_permanent_failure(e):
                    urls.remove(url)
                continue
            if local_file.is_file():
                return info["key"], local_file
        if not urls or done_event.is_set():
            break
    _remove_if_empty(_partial_file(local_file))
    logger.info(f"\x1b[91;20mall download links failed\033[0m {info['path']}")
    return None, None


def download_urls(
    infos: list[dict[str, Any]],
    local_cache: list[Path],
    download_db: Path,
    logger: intake_esgf.logging.Logger,
    esg_dataroot: None | list[Path],
    quiet: bool = False,
) -> list[dict[str, Any]]:
    """
    Download URLs in parallel using a thread pool.

    Returns
    -------
    list[dict]
        The file infos which we failed to download.
    """
    download = partial(
        parallel_download,
        local_cache=local_cache,
        download_db=download_db,
        logger=logger,
        esg_dataroot=esg_dataroot if esg_dataroot is not None else [],
    )
    PROGRESS.disable = quiet
    with PROGRESS:
        download_size, download_unit = get_total_size(infos)
        master_id = PROGRESS.add_task(
            f"Downloading {len(infos)} file(s) totalling {download_size:.1f} [{download_unit}]",
            total=len(infos),
        )
        with ThreadPoolExecutor(intake_esgf.conf["num_threads"]) as pool:
            futures = {}
            for info in infos:
                task_id = PROGRESS.add_task(
                    info["path"].name,
                    filename=info["path"].name,
                    start=False,
                )
                future = pool.submit(
                    download, info=info, task_id=task_id, master_id=master_id
                )
                futures[future] = info

    # The pool has shut down, so every future is complete. Any exception raised in
    # a worker thread is stored on its future and would be lost if not retrieved.
    failed = []
    for future, info in futures.items():
        exc = future.exception()
        if exc is not None:
            logger.info(
                f"\x1b[91;20mdownload failed\033[0m {type(exc).__name__}: {exc} {info['path']}"
            )
            failed.append(info)
            continue
        key, _ = future.result()
        if key is None:
            failed.append(info)
    return failed


def get_search_criteria(
    ds: xr.Dataset, project_id: str | None = None
) -> dict[str, str]:
    """Return a dictionary of facet information from the dataset attributes."""
    if "project" in ds.attrs:
        project_id = ds.attrs["project"]
    if project_id is None:
        raise ValueError(
            "Cannot get search criteria if 'project' not in the attributes or specified."
        )
    project = projects.get(project_id.lower())
    if project is None:
        raise ProjectNotSupported(project_id)
    search = {
        key: ds.attrs[key]
        for key in set(project.master_id_facets()).intersection(ds.attrs.keys())
    }
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
    # extract the project information from the catalog
    project_id = catalog.df["project"].unique()
    if len(project_id) != 1:
        raise ValueError(
            f"Only single project queries are supported, found: {project_id}"
        )
    project_id = project_id[0]
    project = projects.get(project_id.lower())
    if project is None:
        raise ProjectNotSupported(project_id)
    # populate the search
    search = get_search_criteria(ds, project_id)
    [search.pop(key) for key in project.variable_description_facets() if key in search]
    search[project.variable_facet()] = variable_id
    search["project"] = project_id
    # relax search criteria
    relaxation = [
        "",
    ] + project.relaxation_facets()
    for i in range(len(relaxation)):
        if i:
            catalog.logger.info(f"relaxing {relaxation[i]}")
        try:
            cat.search(
                quiet=True,
                **{
                    key: val
                    for key, val in search.items()
                    if key not in relaxation[: (i + 1)]
                },
            )
            cat.df = cat.df.iloc[:1]  # we just need 1
            break
        except NoSearchResults:
            continue
    if cat.df is None or len(cat.df) == 0:
        raise NoSearchResults
    # many times the coordinates of the measures differ in only precision of the
    # variables and will lead to unexpected merged results
    msr_file = cat.to_path_dict(quiet=True)[variable_id]
    msr_file = next(iter(msr_file))
    catalog.logger.info(f"accessed {msr_file}")
    var = xr.open_dataset(msr_file)
    var = var.reindex_like(ds, method="nearest", tolerance=1e-5)
    ds = xr.merge([ds, var[variable_id]], compat="override")
    return ds


def add_cell_measures(ds: xr.Dataset, catalog) -> xr.Dataset:
    """Search the catalog for variables needed by the cell measures/methods.

    Parameters
    ----------
    ds
        The dataset whose dataarrays we will check for cell measures/methods.
    catalog
        The ESGFCatalog instance to search. We will clone this catalog so no copy is
        made.

    Returns
    -------
    ds
        The same dataset with the required measures added and downloaded if needed.

    """
    to_add = []
    for _, da in ds.items():
        to_add += [
            varname
            for _, varname in re.findall(
                r"(\w+):\s*(\S+)", da.attrs.get("cell_measures", "")
            )
        ]
        to_add += [
            val
            for key, val in {"where land": "sftlf", "where sea": "sftof"}.items()
            if key in da.attrs.get("cell_methods", "")
        ]
    if not to_add:
        catalog.logger.info("dataset has no detectable measures to add")
    missing = []
    for add in set(to_add):
        catalog.logger.info(f"adding {add}")
        try:
            ds = add_variable(add, ds, catalog)
        except NoSearchResults:
            missing.append(add)
    if missing:
        catalog.logger.info(f"Failed to locate all measures, {missing=}")
    return ds


def expand_cmip5_record(
    search_vars: list[str], content_vars: list[str], record: dict[str, Any]
) -> list[dict[str, Any]]:
    """Expand the CMIP5 record to include variables."""
    assert record["project"] == "CMIP5"
    variables = list(set(search_vars).intersection(content_vars))
    if not variables:
        variables = content_vars.copy()
    records = []
    for var in variables:
        r = record.copy()
        r["variable"] = var
        records.append(r)
    return records


def get_content_path(content: dict[str, Any]) -> Path:
    """Get the local path where the data is to be stored."""
    parts = content["id"].split("|")[0].split(".")
    # I hate that I have to address bad publishing with software fixes, but for
    # some reason we just won't fix the index. Some files are published with a
    # `.nc_0` as the suffix, which causes problems as we think they are
    # different files.
    path = Path(*parts[:-1]).with_suffix(".nc")
    return path


def get_time_extent(
    filename: str,
) -> tuple[pd.Timestamp, pd.Timestamp] | tuple[None, None]:
    """
    Parse the timespan out of the filename, if possible, using pandas Timestamp.
    """

    def _to_timestamp(s: str) -> pd.Timestamp:
        if len(s) == 6:
            s += "01"
        return pd.Timestamp(s)

    # Try to parse out the filename
    match = re.search(r"_(\d+)-(\d+)", filename)
    if not match:
        return None, None

    # Try to convert to a pandas timestamp
    try:
        t0, tf = _to_timestamp(match.group(1)), _to_timestamp(match.group(2))
    except Exception:
        return None, None
    return t0, tf


def get_total_size(infos: list[dict[str, Any]]) -> tuple[float, str]:
    """
    Get the total size of all files to be downloaded.
    """
    download_size = sum([info["size"] for info in infos]) * 1e-6
    download_unit = "Mb"
    if download_size > 1e3:
        download_size *= 1e-3
        download_unit = "Gb"
    return download_size, download_unit
