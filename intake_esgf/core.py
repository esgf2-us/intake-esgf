import hashlib
import logging
import re
import time
from pathlib import Path
from typing import Any, Union

import pandas as pd
import requests
from globus_sdk import SearchClient
from globus_sdk.response import GlobusHTTPResponse
from pyesgf.search import SearchConnection
from tqdm import tqdm

logger = logging.getLogger("intake-esgf")


def get_dataset_pattern() -> str:
    """Return the dataset id regular expression pattern."""
    COLUMNS = [
        "mip_era",
        "activity_id",
        "institution_id",
        "source_id",
        "experiment_id",
        "member_id",
        "table_id",
        "variable_id",
        "grid_label",
        "version",
        "data_node",
    ]
    pattern = r"\.".join([rf"(?P<{c}>\S[^.|]+)" for c in COLUMNS[:-1]])
    pattern += rf"\|(?P<{COLUMNS[-1]}>\S+)"
    return pattern


class SolrESGFIndex:
    def __init__(self, index_node: str = "esgf-node.llnl.gov", distrib: bool = True):
        self.conn = SearchConnection(
            f"https://{index_node}/esg-search", distrib=distrib
        )
        self.response = None

    def search(self, **search: Union[str, list[str]]) -> pd.DataFrame:
        if "latest" not in search:
            search["latest"] = True
        ctx = self.conn.new_context(facets=list(search.keys()), **search)
        response = ctx.search()
        self.response = response
        if not ctx.hit_count:
            raise ValueError("Search returned no results.")
        assert ctx.hit_count == len(response)
        pattern = get_dataset_pattern()
        df = []
        for dsr in response:
            m = re.search(pattern, dsr.dataset_id)
            if m:
                df.append(m.groupdict())
                df[-1]["id"] = dsr.dataset_id
        return pd.DataFrame(df)

    def get_file_info(self, dataset_ids: list[str]) -> tuple[Path, str, str, list[str]]:
        # rel_path, checksum, alg, list of links
        if self.response is None:
            raise ValueError("You need to run search() first.")
        checksums = []
        checksum_types = []
        for dsr in self.response:
            if dsr.dataset_id not in dataset_ids:
                continue
            for fr in dsr.file_context().search(ignore_facet_check=True):
                [u[0][0] for t, u in fr.urls.items() if t != "GridFTP"]
        assert all([c == checksums[0] for c in checksums])
        assert all([c == checksum_types[0] for c in checksum_types])


class GlobusESGFIndex:
    GLOBUS_INDEX_IDS = {
        "anl-dev": "d927e2d9-ccdb-48e4-b05d-adbc3d97bbc5",
    }

    def __init__(self, index_id="anl-dev"):
        if index_id in GlobusESGFIndex.GLOBUS_INDEX_IDS:
            index_id = GlobusESGFIndex.GLOBUS_INDEX_IDS[index_id]
        self.index_id = index_id
        self.client = SearchClient()

    def search(self, **search: Union[str, list[str]]) -> pd.DataFrame:
        limit = 2000
        if "project" in search:
            project = search.pop("project")
            if project != "CMIP6":
                raise ValueError("ANL Globus index only for CMIP6")
        search["type"] = "Dataset"
        if "latest" not in search:
            search["latest"] = True
        for key, val in search.items():
            if isinstance(val, bool):
                search[key] = str(val)
        query_data = {
            "q": "",
            "filters": [
                {
                    "type": "match_any",
                    "field_name": key,
                    "values": [val] if isinstance(val, str) else val,
                }
                for key, val in search.items()
            ],
            "facets": [],
            "sort": [],
        }
        response = SearchClient().post_search(self.index_id, query_data, limit=limit)
        if not response["total"]:
            raise ValueError("Search returned no results.")
        pattern = get_dataset_pattern()
        df = []
        for g in response["gmeta"]:
            m = re.search(pattern, g["subject"])
            if m:
                df.append(m.groupdict())
                df[-1]["id"] = g["subject"]
        df = pd.DataFrame(df)
        return df

    def get_file_info(dataset_id: str) -> tuple[Path, str, str, list[str]]:
        # rel_path, checksum, alg, list of links
        pass


def combine_results(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Return a combined dataframe where ids are now a list."""
    # combine and remove duplicate entries
    df = pd.concat(dfs).drop_duplicates(subset="id").reset_index(drop=True)
    # remove earlier versions if present
    for lbl, grp in df.groupby(list(df.columns[:-3])):
        df = df.drop(grp[grp.version != grp.version.max()].index)
    # now convert groups to list
    for lbl, grp in df.groupby(list(df.columns[:-3])):
        df = df.drop(grp.iloc[1:].index)
        df.loc[grp.index[0], "id"] = grp.id.to_list()
    df = df.drop(columns="data_node")
    return df


def get_file_hash(filepath: Union[str, Path], algorithm: str) -> str:
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
    local_file: Union[str, Path],
    hash: str,
    hash_algorithm: str,
    content_length: Union[None, int] = None,
) -> None:
    """Download the url to a local file and check for validity, removing if not."""
    if not isinstance(local_file, Path):
        local_file = Path(local_file)
    local_file.parent.mkdir(parents=True, exist_ok=True)
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    if content_length is None and "content-length" in resp.headers:
        content_length = int(resp.headers.get("content-length"))
    transfer_time = time.time()
    with open(local_file, "wb") as fdl:
        with tqdm(
            total=content_length,
            unit="B",
            unit_scale=True,
            desc=local_file.name,
            ascii=True,
        ) as pbar:
            for chunk in resp.iter_content(chunk_size=1024):
                if chunk:
                    fdl.write(chunk)
                    pbar.update(len(chunk))
    transfer_time = time.time() - transfer_time
    rate = content_length * 1e-6 / transfer_time
    if get_file_hash(local_file, hash_algorithm) != hash:
        logger.info(f"{local_file} failed validation, removing")
        local_file.unlink()
        return
    logger.info(
        f"Downloaded {url} to {local_file} in {transfer_time:.2f} [s] at {rate:.2f} [Mb s-1] with {hash_algorithm.lower()}={hash}"  # noqa: E501
    )


def get_relative_esgf_path(entry: dict[str, Any]) -> Path:
    """Return the relative ESGF path from the Globus entry."""
    if "content" not in entry:
        raise ValueError("'content' not part of the entry.")
    content = entry["content"]
    if set(["version", "dataset_id", "directory_format_template_"]).difference(
        content.keys()
    ):
        raise ValueError("Entry content does not contain expected keys.")
    # For some reason, the `version` in the globus response is just an integer and not
    # what is used in the file path so I have to parse it out of the `dataset_id`
    content["version"] = [content["dataset_id"].split("|")[0].split(".")[-1]]
    # Format the file path using the template in the response
    file_path = content["directory_format_template_"][0]
    file_path = Path(
        file_path.replace("%(root)s/", "")
        .replace("%(", "{")
        .replace(")s", "[0]}")
        .format(**content)
    )
    return file_path


def response_to_local_filelist(
    response: GlobusHTTPResponse, data_root: Union[str, Path]
) -> list[Path]:
    """Return a list of local paths to netCDF files described in the Globus response.

    This function is used to check if the files for which we searched are already
    available locally. This could be because we have previously downloaded them and they
    are in the local cache. It could also be because we are on a resource that has
    direct access to the ESGF data and we have called `set_esgf_data_root()`. This
    function uses the `directory_format_template_` and values in the response to form a
    relative location of the files represented in the Globus response. We then prepend
    the given `data_root` to make the path absolute and check for existence.

    """
    assert data_root is not None
    if isinstance(data_root, str):
        data_root = Path(data_root)
    if not data_root.is_dir():
        raise FileNotFoundError(f"Directory {data_root} does not exist.")
    paths = []
    for g in response["gmeta"]:
        assert len(g["entries"]) == 1
        entry = g["entries"][0]
        if entry["entry_id"] != "file":
            continue
        file_path = data_root / get_relative_esgf_path(entry)
        if not file_path.is_dir():
            raise FileNotFoundError(f"Directory {file_path} does not exist.")
        for file_name in file_path.glob("*.nc"):
            paths.append(file_name)
        if paths:
            logger.info(f"Using files from {file_path}")
    return paths


def response_to_https_download(
    response: GlobusHTTPResponse, local_root: Union[str, Path]
) -> list[Path]:
    """Download the file using the links found in the globus response."""
    if isinstance(local_root, str):
        local_root = Path(local_root)
    if not local_root.is_dir():
        raise FileNotFoundError(f"Directory {local_root} does not exist.")
    paths = []
    for g in response["gmeta"]:
        assert len(g["entries"]) == 1
        entry = g["entries"][0]
        if entry["entry_id"] != "file":
            continue
        content = entry["content"]
        url = [u for u in content["url"] if u.endswith("|HTTPServer")]
        if len(url) != 1:
            continue
        url = url[0].replace("|HTTPServer", "")
        local_file = local_root / get_relative_esgf_path(entry) / Path(Path(url).name)
        local_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            download_and_verify(
                url,
                local_file,
                content["checksum"][0],
                content["checksum_type"][0],
                int(content["size"]),
            )
            paths.append(local_file)
        except requests.exceptions.HTTPError:
            logger.info(f"HTTP error {url}")
    return paths
