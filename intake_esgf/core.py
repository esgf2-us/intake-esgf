import hashlib
import logging
import re
import time
from functools import partial
from pathlib import Path
from typing import Any, Union

import pandas as pd
import requests
from globus_sdk import SearchClient
from pyesgf.search import SearchConnection
from pyesgf.search.exceptions import EsgfSearchException
from tqdm import tqdm

from intake_esgf.database import (
    get_download_rate_dataframe,
    log_download_information,
    sort_download_links,
)
from intake_esgf.logging import setup_logging


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
        self.repr = f"SolrESGFIndex('{index_node}'{',distrib=True' if distrib else ''})"
        self.conn = SearchConnection(
            f"https://{index_node}/esg-search", distrib=distrib
        )
        self.response = None
        self.logger = None

    def __repr__(self):
        return self.repr

    def search(self, **search: Union[str, list[str]]) -> pd.DataFrame:
        total_time = time.time()
        if "latest" not in search:
            search["latest"] = True
        response_time = time.time()
        ctx = self.conn.new_context(facets=list(search.keys()), **search)
        response = ctx.search()
        response_time = time.time() - response_time
        self.response = response
        if not ctx.hit_count:
            if self.logger is not None:
                self.logger.info(f"└─{self} no results")
            raise ValueError("Search returned no results.")
        assert ctx.hit_count == len(response)
        pattern = get_dataset_pattern()
        df = []
        process_time = time.time()
        for dsr in response:
            m = re.search(pattern, dsr.dataset_id)
            if m:
                df.append(m.groupdict())
                df[-1]["id"] = dsr.dataset_id
        process_time = time.time() - process_time
        df = pd.DataFrame(df)
        total_time = time.time() - total_time
        if self.logger is not None:
            self.logger.info(f"└─{self} {response_time=:.2f} {total_time=:.2f}")
        return df

    def from_tracking_ids(self, tracking_ids: Union[str, list[str]]) -> pd.DataFrame:
        if isinstance(tracking_ids, str):
            tracking_ids = [tracking_ids]
        raise NotImplementedError

    def get_file_info(self, dataset_ids: list[str]) -> dict[str, Any]:
        """Return a file information dictionary.

        Parameters
        ----------
        dataset_ids
            A list of datasets IDs which scientifically refer to the same files.

        """
        if self.response is None:
            return []
        infos = []
        for dsr in self.response:
            if dsr.dataset_id not in dataset_ids:
                continue
            for fr in dsr.file_context().search(ignore_facet_check=True):
                info = {}
                info["checksum_type"] = fr.checksum_type
                info["checksum"] = fr.checksum
                info["size"] = fr.size
                fr.json["version"] = [
                    fr.json["dataset_id"].split("|")[0].split(".")[-1]
                ]
                file_path = fr.json["directory_format_template_"][0]
                info["path"] = (
                    Path(
                        file_path.replace("%(root)s/", "")
                        .replace("%(", "{")
                        .replace(")s", "[0]}")
                        .format(**fr.json)
                    )
                    / fr.json["title"]
                )
                for link_type, link in fr.urls.items():
                    if link_type not in info:
                        info[link_type] = []
                    info[link_type].append(link[0][0])
                infos.append(info)
        return infos


class GlobusESGFIndex:
    GLOBUS_INDEX_IDS = {
        "anl-dev": "d927e2d9-ccdb-48e4-b05d-adbc3d97bbc5",
    }

    def __init__(self, index_id="anl-dev"):
        self.repr = f"GlobusESGFIndex('{index_id}')"
        if index_id in GlobusESGFIndex.GLOBUS_INDEX_IDS:
            index_id = GlobusESGFIndex.GLOBUS_INDEX_IDS[index_id]
        self.index_id = index_id
        self.client = SearchClient()
        self.logger = None

    def __repr__(self):
        return self.repr

    def search(self, **search: Union[str, list[str]]) -> pd.DataFrame:
        # We need to implement the pagination here, I am just using a large limit for
        # now but that isn't safe and I am not sure of performance implications.
        total_time = time.time()
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
        response_time = time.time()
        response = SearchClient().post_search(self.index_id, query_data, limit=limit)
        response_time = time.time() - response_time
        if not response["total"]:
            if self.logger is not None:
                self.logger.info(f"└─{self} no results")
            raise ValueError("Search returned no results.")
        pattern = get_dataset_pattern()
        df = []
        for g in response["gmeta"]:
            m = re.search(pattern, g["subject"])
            if m:
                df.append(m.groupdict())
                df[-1]["id"] = g["subject"]
        df = pd.DataFrame(df)
        total_time = time.time() - total_time
        if self.logger is not None:
            self.logger.info(f"└─{self} {response_time=:.2f} {total_time=:.2f}")
        return df

    def get_file_info(self, dataset_ids: list[str]) -> dict[str, Any]:
        """"""
        response = SearchClient().post_search(
            self.index_id,
            {
                "q": "",
                "filters": [
                    {
                        "type": "match_any",
                        "field_name": "dataset_id",
                        "values": dataset_ids,
                    }
                ],
                "facets": [],
                "sort": [],
            },
            limit=1000,
        )
        infos = []
        for g in response["gmeta"]:
            info = {}
            assert len(g["entries"]) == 1
            entry = g["entries"][0]
            if entry["entry_id"] != "file":
                continue
            content = entry["content"]
            info["checksum_type"] = content["checksum_type"][0]
            info["checksum"] = content["checksum"][0]
            info["size"] = content["size"]
            for url in content["url"]:
                link, link_type = url.split("|")
                if link_type not in info:
                    info[link_type] = []
                info[link_type].append(link)
            # For some reason, the `version` in the globus response is just an integer
            # and not what is used in the file path so I have to parse it out of the
            # `dataset_id`
            content["version"] = [content["dataset_id"].split("|")[0].split(".")[-1]]
            file_path = content["directory_format_template_"][0]
            info["path"] = (
                Path(
                    file_path.replace("%(root)s/", "")
                    .replace("%(", "{")
                    .replace(")s", "[0]}")
                    .format(**content)
                )
                / content["title"]
            )
            infos.append(info)
        return infos

    def from_tracking_ids(self, tracking_ids: Union[str, list[str]]) -> pd.DataFrame:
        if isinstance(tracking_ids, str):
            tracking_ids = [tracking_ids]
        response = SearchClient().post_search(
            self.index_id,
            {
                "q": "",
                "filters": [
                    {
                        "type": "match_any",
                        "field_name": "tracking_id",
                        "values": tracking_ids,
                    }
                ],
                "facets": [],
                "sort": [],
            },
            limit=1000,
        )
        pattern = get_dataset_pattern()
        df = []
        for g in response["gmeta"]:
            try:
                dataset_id = g["entries"][0]["content"]["dataset_id"]
            except Exception:
                continue
            m = re.search(pattern, dataset_id)
            if m:
                df.append(m.groupdict())
                df[-1]["id"] = dataset_id
        df = pd.DataFrame(df)
        return df


def combine_results(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Return a combined dataframe where ids are now a list."""
    # combine and remove duplicate entries
    df = pd.concat(dfs).drop_duplicates(subset="id").reset_index(drop=True)
    if len(df) == 0:
        logger = setup_logging()
        logger.info("\x1b[36;32msearch end \x1b[91;20mno results\033[0m")
        raise ValueError("Search returned no results.")
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
    content_length: int,
    download_db: Path,
    quiet: bool = False,
    logger: Union[logging.Logger, None] = None,
) -> None:
    """Download the url to a local file and check for validity, removing if not."""
    if not isinstance(local_file, Path):
        local_file = Path(local_file)
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
            desc=local_file.name,
            ascii=False,
        ) as pbar:
            for chunk in resp.iter_content(chunk_size=1024):
                if chunk:
                    fdl.write(chunk)
                    pbar.update(len(chunk))
    transfer_time = time.time() - transfer_time
    rate = content_length * 1e-6 / transfer_time
    if get_file_hash(local_file, hash_algorithm) != hash:
        if logger is not None:
            logger.info(f"\x1b[91;20mHash error\033[0m {url}")
        local_file.unlink()
        raise ValueError("Hash does not match")
    if logger is not None:
        logger.info(f"{transfer_time=:.2f} [s] at {rate:.2f} [Mb s-1] {url}")
    host = url[: url.index("/", 10)].replace("http://", "").replace("https://", "")
    log_download_information(download_db, host, transfer_time, content_length * 1e-6)


def parallel_download(
    info: dict[str, Any],
    local_cache: Path,
    download_db: Path,
    esg_dataroot: Union[None, Path] = None,
):
    """."""
    logger = setup_logging()
    # does this exist on a copy we have access to?
    if esg_dataroot is not None:
        local_file = esg_dataroot / info["path"]
        if local_file.exists():
            if logger is not None:
                logger.info(f"accessed {local_file}")
            return info["key"], local_file
    # have we already downloaded this?
    local_file = local_cache / info["path"]
    if local_file.exists():
        if logger is not None:
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
                local_file,
                info["checksum"],
                info["checksum_type"],
                info["size"],
                download_db=download_db,
                logger=logger,
            )
        except Exception as exc:
            print(exc)
            logger.info(f"\x1b[91;20mdownload failed\033[0m {url}")
            continue
        if local_file.exists():
            return info["key"], local_file
    return None, None


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


def combine_file_info(
    indices: list[Union[SolrESGFIndex, GlobusESGFIndex]], dataset_ids: list[str]
) -> dict[str, Any]:
    """"""
    merged_info = {}
    for ind in indices:
        try:
            infos = ind.get_file_info(dataset_ids)
        except EsgfSearchException:
            continue
        # loop thru all the infos and uniquely add by path
        for info in infos:
            path = info["path"]
            if path not in merged_info:
                merged_info[path] = {}
            for key, val in info.items():
                if isinstance(val, list):
                    if key not in merged_info[path]:
                        merged_info[path][key] = val
                    else:
                        merged_info[path][key] += val
                else:
                    if key not in merged_info[path]:
                        merged_info[path][key] = val
    return [info for key, info in merged_info.items()]
