import hashlib
import logging
import time
from functools import partial
from pathlib import Path
from typing import Any, Union

import pandas as pd
import requests
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


def combine_file_info(indices, dataset_ids: list[str]) -> dict[str, Any]:
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
