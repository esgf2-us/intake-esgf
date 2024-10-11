"""General functions used in various parts of intake-esgf."""

import hashlib
import re
import time
from functools import partial
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import xarray as xr
from globus_sdk import TransferAPIError

import intake_esgf
from intake_esgf.core.globus import get_authorized_transfer_client
from intake_esgf.database import (
    get_download_rate_dataframe,
    log_download_information,
    sort_download_links,
)
from intake_esgf.exceptions import NoSearchResults, ProjectNotSupported
from intake_esgf.projects import projects

if intake_esgf.IN_NOTEBOOK:
    from tqdm import tqdm_notebook as tqdm
else:
    from tqdm import tqdm

bar_format = "{desc:>20}: {percentage:3.0f}%|{bar}|{n_fmt}/{total_fmt} [{rate_fmt:>15s}{postfix}]"


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
    return globus_endpoints


def partition_infos(
    infos: list[dict], prefer_streaming: bool, prefer_globus: bool
) -> tuple[dict, dict]:
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
    ds = {}

    # as we iterate through the infos we will partition them
    infos_exist = []
    infos_stream = []
    infos_globus = []
    infos_https = []

    # to keep from checking globus endpoints active status too much, we will store them
    client = None
    active_endpoints = set()

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
            infos_exist.append(info)  # maybe not needed
            continue
        except FileNotFoundError:
            pass

        # 2) does the user prefer to stream data?
        if prefer_streaming:
            # how do we choose a link?
            preferred_sources = ["VirtualZarr", "OPENDAP"]  # move to configure
            links = [
                link
                for src in (set(preferred_sources) & set(info))
                for link in info[src]
            ]
            if links:
                # for now just use first link, we need to do better
                ds[key] = [links[0]]
                infos_stream.append(info)  # maybe not needed
                continue

        # 3) does the user prefer to use globus transfer?
        if prefer_globus:
            source_endpoints = get_globus_endpoints(info)
            # before removing these from infos of what we will download, check that
            # their endpoints actually work
            for uuid in source_endpoints:
                if uuid in active_endpoints:
                    continue
                client = get_authorized_transfer_client() if client is None else client
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


def combine_results(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Return a combined dataframe where ids are now a list."""
    # combine and remove duplicate entries
    logger = intake_esgf.conf.get_logger()
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
    # now convert groups to list
    for _, grp in df.groupby(project.master_id_facets(), dropna=False):
        df = df.drop(grp.iloc[1:].index)
        df.loc[grp.index[0], "id"] = grp.id.to_list()
    df = df.drop(columns="data_node")
    combine_time = time.time() - combine_time
    logger.info(f"{combine_time=:.2f}")
    return df


def get_file_hash(filepath: str | Path, algorithm: str) -> str:
    """Get the file has using the given algorithm."""
    algorithm = algorithm.lower()
    assert algorithm in hashlib.algorithms_available
    sha = hashlib.__dict__[algorithm]()
    with open(filepath, "rb") as fp:
        while True:
            data = fp.read(64 * 1024)
            if not data:
                break
            sha.update(data)
    return sha.hexdigest()


def download_and_verify(
    url: str,
    local_file: str | Path,
    hash: str,
    hash_algorithm: str,
    content_length: int,
    download_db: Path,
    quiet: bool = False,
) -> None:
    """Download the url to a local file and check for validity, removing if not."""
    logger = intake_esgf.conf.get_logger()
    if not isinstance(local_file, Path):
        local_file = Path(local_file)
    max_file_length = 40
    desc = (
        local_file.name
        if len(local_file.name) < max_file_length
        else f"{local_file.name[:(max_file_length-3)]}..."
    )
    local_file.parent.mkdir(parents=True, exist_ok=True)
    resp = requests.get(url, stream=True, timeout=10)
    resp.raise_for_status()
    transfer_time = time.time()
    with open(local_file, "wb") as fdl:
        with tqdm(
            disable=quiet,
            bar_format="{desc}: {percentage:3.0f}%|{bar}|{n_fmt}/{total_fmt} [{rate_fmt}{postfix}]",
            total=content_length,
            unit="B",
            unit_scale=True,
            desc=desc,
            ascii=False,
            leave=False,
        ) as pbar:
            for chunk in resp.iter_content(chunk_size=1024):
                if chunk:
                    fdl.write(chunk)
                    pbar.update(len(chunk))
    transfer_time = time.time() - transfer_time
    rate = content_length * 1e-6 / transfer_time
    if get_file_hash(local_file, hash_algorithm) != hash:
        logger.info(f"\x1b[91;20mHash error\033[0m {url}")
        local_file.unlink()
        raise ValueError("Hash does not match")
    logger.info(f"{transfer_time=:.2f} [s] at {rate:.2f} [Mb s-1] {url}")
    host = url[: url.index("/", 10)].replace("http://", "").replace("https://", "")
    log_download_information(download_db, host, transfer_time, content_length * 1e-6)


def parallel_download(
    info: dict[str, Any],
    local_cache: list[Path],
    download_db: Path,
    esg_dataroot: None | list[Path] = None,
):
    """."""
    logger = intake_esgf.conf.get_logger()
    # does this exist on a copy we have access to?
    for path in esg_dataroot:
        if esg_dataroot is not None:
            local_file = path / info["path"]
            if local_file.exists():
                logger.info(f"accessed {local_file}")
                return info["key"], local_file
    # have we already downloaded this?
    for path in local_cache:
        local_file = path / info["path"]
        if local_file.exists():
            logger.info(f"accessed {local_file}")
            return info["key"], local_file
    # else we try to download it, first we sort links by the fastest host to you
    df_rate = get_download_rate_dataframe(download_db)
    info["HTTPServer"] = sorted(
        info["HTTPServer"], key=partial(sort_download_links, df_rate=df_rate)
    )
    # keep trying to download until one works out
    for url in info["HTTPServer"]:
        try:
            download_and_verify(
                url,
                local_cache[0] / info["path"],
                info["checksum"],
                info["checksum_type"],
                info["size"],
                download_db=download_db,
            )
        except Exception:
            logger.info(f"\x1b[91;20mdownload failed\033[0m {url}")
            continue
        if local_file.exists():
            return info["key"], local_file
    return None, None


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
    # relax search criteria
    relaxation = project.relaxation_facets()
    while True:
        try:
            cat.search(quiet=True, **search)
            cat.df = cat.df.iloc[:1]  # we just need 1
            break
        except NoSearchResults:
            while True:
                # no more criteria to relax... just can't find it
                if not relaxation:
                    raise NoSearchResults
                relax = relaxation.pop(0)
                if relax in search:
                    search.pop(relax)
                    break
    # many times the coordinates of the measures differ in only precision of the
    # variables and will lead to unexpected merged results
    var = cat.to_dataset_dict(quiet=True, add_measures=False)[variable_id]
    var = var.reindex_like(ds, method="nearest", tolerance=1e-5)
    ds = xr.merge([ds, var[variable_id]])
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
        if "where sea" in da.attrs["cell_methods"]:
            to_add.append("sftof")
    to_add = set(to_add)
    for add in to_add:
        try:
            ds = add_variable(add, ds, catalog)
        except NoSearchResults:
            pass
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
    """Get the local path where the data is to be stored.

    In CMIP6 we get a directory template, we just fill in values from the content. In
    older projects we do not, but can search for the project name and grab all the text
    following it. In the end, as long as we are consistent it does not matter.

    """

    def _form_from_template(content) -> Path:
        template = re.findall(r"%\((\w+)\)s", content["directory_format_template_"][0])
        template = [
            content[t][0] if isinstance(content[t], list) else content[t]
            for t in template
            if t in content
        ]
        return Path("/".join(template)) / content["title"]

    # the file `_version_` is not the same as the dataset `version` so we parse it out
    # of the `dataset_id`
    content["version"] = [content["dataset_id"].split("|")[0].split(".")[-1]]
    if "directory_format_template_" in content:
        return _form_from_template(content)

    # otherwise we look for the project text in the url and return everything following
    # it
    urls = [url for url in content["url"] if "application/netcdf|HTTPServer"]
    project = (
        content["project"][0]
        if isinstance(content["project"], list)
        else content["project"]
    )
    if not urls:
        raise ValueError(f"Could not find a http link in {content['url']}")
    match = re.search(rf".*({project.lower()}.*.nc)|.*", urls[0])
    if not match:
        raise ValueError(f"Could not parse out the path from {urls[0]}")
    # try to fix records with case-insensitive paths
    path = match.group(1).replace(project.lower(), project)
    return Path(path)
