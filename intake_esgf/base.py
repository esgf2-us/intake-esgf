"""General functions used in various parts of intake-esgf."""
import hashlib
import logging
import re
import time
from functools import partial
from pathlib import Path
from typing import Any, Union

import pandas as pd
import requests
import xarray as xr

from intake_esgf import IN_NOTEBOOK
from intake_esgf.database import (
    get_download_rate_dataframe,
    log_download_information,
    sort_download_links,
)
from intake_esgf.exceptions import NoSearchResults
from intake_esgf.logging import setup_logging

if IN_NOTEBOOK:
    from tqdm import tqdm_notebook as tqdm
else:
    from tqdm import tqdm

bar_format = "{desc:>20}: {percentage:3.0f}%|{bar}|{n_fmt}/{total_fmt} [{rate_fmt:>15s}{postfix}]"


def get_dataset_pattern() -> str:
    """Return the dataset id regular expression pattern.

    This function is used to get facet information in a dictionary from the dataset_id.

    """
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
    logger = setup_logging()
    combine_time = time.time()
    df = pd.concat(dfs).drop_duplicates(subset="id").reset_index(drop=True)
    if len(df) == 0:
        logger.info("\x1b[36;32msearch end \x1b[91;20mno results\033[0m")
        raise ValueError("Search returned no results.")
    # now convert groups to list
    for _, grp in df.groupby(list(df.columns[:-3])):
        df = df.drop(grp.iloc[1:].index)
        df.loc[grp.index[0], "id"] = grp.id.to_list()
    df = df.drop(columns="data_node")
    combine_time = time.time() - combine_time
    logger.info(f"{combine_time=:.2f}")
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


def check_for_esgf_dataroot() -> Union[Path, None]:
    """Return a direct path to the ESGF data is it exists."""
    to_check = [
        "/p/css03/esgf_publish",  # Nimbus
        "/eagle/projects/ESGF2/esg_dataroot",  # ALCF
        "/global/cfs/projectdirs/m3522/cmip6/",  # NERSC data lake
    ]
    for check in to_check:
        if Path(check).is_dir():
            return check
    return None


def get_search_criteria(ds: xr.Dataset) -> dict[str, str]:
    """Return a dictionary of facet information from the dataset attributes."""
    keys = [
        "activity_id",
        "experiment_id",
        "frequency",
        "grid_label",
        "institution_id",
        "mip_era",
        "source_id",
        "table_id",
        "variable_id",
        "variant_label",
        "version",
    ]
    search = {key: ds.attrs[key] for key in set(keys).intersection(ds.attrs.keys())}
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

    search = get_search_criteria(ds)
    # some keys we get rid of either because we do not need them or they are wrong
    for key in [
        "frequency",
        "institution_id",
        "table_id",
        "version",
        "variable_id",
    ]:
        if key in search:
            search.pop(key)
    # now we update for this search
    search.update({"table_id": ["fx", "Ofx"], "variable_id": variable_id})
    # relax search criteria
    relaxation = ["variant_label", "experiment_id", "activity_id"]
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
    var = var.reindex_like(ds, method="nearest", tolerance=1e-6)
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
        except ValueError:
            pass
    return ds


def get_cell_measure(var: str, ds: xr.Dataset) -> Union[xr.DataArray, None]:
    """Return the dataarray of the measures required by the given var.

    This routine will examine the `cell_measures` attribute of the specified `var` as
    well as the `cell_measures` applying any land/sea fractions that are necesary. This
    assumes that these variables are already part of the input dataset.

    Parameters
    ----------
    var
        The variable whose measures we will return.
    ds
        The dataset from which we will find the measures.

    """
    # if the dataarray has a cell_measures attribute and 'area' in it, we can
    # integrate it
    da = ds[var]
    if "cell_measures" not in da.attrs:
        return None
    m = re.search(r"area:\s(\w+)\s*", da.attrs["cell_measures"])
    if not m:
        return None
    msr = m.group(1)
    if msr not in ds:
        raise ValueError(f"{var} cell_measures={msr} but not in dataset")
    measure = ds[msr]
    # apply land/sea fractions if applicable, this is messy and there are maybe
    # others we need to find
    for domain, vid in zip(["land", "sea"], ["sftlf", "sftof"]):
        if "cell_methods" in da.attrs and f"where {domain}" in da.attrs["cell_methods"]:
            if vid not in ds:
                raise ValueError(f"{var} is land but {vid} not in dataset")
            # if fractions aren't [0,1], convert % to 1
            if ds[vid].max() > 2.0:
                ds[vid] *= 0.01
            measure *= ds[vid]
    return measure
