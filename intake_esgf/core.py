import hashlib
import logging
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Union

import pandas as pd
import requests
from globus_sdk.response import GlobusHTTPResponse
from tqdm import tqdm

logger = logging.getLogger("intake-esgf")


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
    transfer_time = time.process_time()
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
    transfer_time = time.process_time() - transfer_time
    rate = content_length * 1e-6 / transfer_time
    if get_file_hash(local_file, hash_algorithm) != hash:
        logger.info(f"{local_file} failed validation, removing")
        local_file.unlink()
        return
    logger.info(
        f"Downloaded {url} to {local_file} in {transfer_time:.2f} [s] at {rate:.2f} [Mb s-1] with {hash_algorithm}={hash}"  # noqa: E501
    )


def get_relative_esgf_path(entry: Dict[str, Any]) -> Path:
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


def response_to_dataframe(response: GlobusHTTPResponse, pattern: str) -> pd.DataFrame:
    """Return a pandas dataframe from the response of a Globus search."""
    df = []
    for g in response["gmeta"]:
        assert len(g["entries"]) == 1  # A check on the assumption of a single entry
        # Manually remove entries which are not datasets. With the way I am using
        # search, this is not necesary but if a file type gets through the regular
        # expression pattern will break and pollute the dataframe.
        if g["entries"][0]["entry_id"] != "dataset":
            continue
        m = re.search(pattern, g["subject"])
        if m:
            df.append(m.groupdict())
            # Also include the globus 'subject' so we can find files later when we are
            # ready to download.
            df[-1]["globus_subject"] = g["subject"]
    df = pd.DataFrame(df)
    return df


def response_to_local_filelist(
    response: GlobusHTTPResponse, data_root: Union[str, Path]
) -> List[Path]:
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
) -> List[Path]:
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


def get_dataset_pattern() -> str:
    """Return the Globus subject regular expression pattern for datasets."""
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
    pattern = "\.".join([f"(?P<{c}>\S[^.|]+)" for c in COLUMNS[:-1]])  # noqa: W605
    pattern += f"\|(?P<{COLUMNS[-1]}>\S+)"  # noqa: W605
    return pattern
