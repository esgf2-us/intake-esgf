import os
import re
from collections.abc import Iterable
from pathlib import Path
from typing import Callable

from pyesgf.search import SearchConnection
from requests import ReadTimeout
from tqdm import tqdm

from intake_esgf.core import get_file_hash
from intake_esgf.logging import setup_logging

logger = setup_logging(filename="check.log")


def get_directory_search_pattern() -> str:
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
    ]
    pattern = r".*/"
    pattern += r"/".join([rf"(?P<{c}>\S[^.|]+)" for c in COLUMNS])
    return pattern


def get_dataset_info(path: Path, file_list: list[str], data_node: str) -> None:
    """Check consistency of the dataset and files associated with this path.

    Anomalies will be logged in `${HOME}/.esgf/check.log`.

    Parameters
    ----------
    path
        The path in which the netCDF files are stored. The standard CMIP6 structure is
        assumed.
    file_list
        The list of files in this dircetory.
    data_node
        The name of the ESGF1 index node to use to check consistency.

    """
    # The search critera can be harvested from the path.
    m = re.search(get_directory_search_pattern(), str(path))
    if not m:
        logger.info(f"Could not get dataset search critera from {path}")
        return
    search = m.groupdict()
    dataset_id = f"{'.'.join([val for _,val in search.items()])}|{data_node}"

    # However, you cannot search for a version and so we pop it and build the full
    # dataset_id.
    search.pop("version")
    search["data_node"] = data_node

    # Search the index node for the dataset record
    conn = SearchConnection(f"https://{data_node}/esg-search", distrib=False)
    try:
        ctx = conn.new_context(facets=list(search.keys()), **search)
        if not ctx.hit_count:
            logger.info(f"{dataset_id=} does not exist")
            return
    except ReadTimeout:
        logger.info(f"Timeout for dataset search for {dataset_id=}")
        return
    try:
        response = ctx.search()
    except ReadTimeout:
        logger.info(f"Timeout for dataset search for {dataset_id=}")
        return

    # Now we make sure the version string is in the id
    dsr = [dsr for dsr in response if dataset_id == dsr.dataset_id]
    if len(dsr) != 1:
        logger.info(f"Search returned datasets but no matches for {dataset_id=}")
    dsr = dsr[0]

    # Do we have the same number of files in the directory as is in the index record?
    if dsr.number_of_files != len(file_list):
        logger.info(
            f"{dataset_id=} record has {dsr.number_of_files} files but there are {len(file_list)} file(s) in {path}"
        )

    # Checks on the files
    ctx = dsr.file_context()
    try:
        frs = [fr for fr in ctx.search(ignore_facet_check=True)]
    except ReadTimeout:
        logger.info(f"Timeout for file search for {dataset_id=}")
        return
    if dsr.number_of_files != len(frs):
        logger.info(
            f"{dataset_id=} record has {dsr.number_of_files} files but there are {len(frs)} associated file records"
        )
    for fr in tqdm(
        frs,
        bar_format="{desc}: {percentage:3.0f}%|{bar}|{n_fmt}/{total_fmt} [{rate_fmt}{postfix}]",
        total=len(frs),
        unit="files",
        unit_scale=False,
        desc=f"...{str(path)[-70:]}",
        ascii=True,
    ):
        fpath = path / fr.json["title"]

        # Check that the file actually exists here, if not we cannot do further checks
        if not fpath.is_file():
            logger.info(
                f"{fr.json['title']} is a file record in {dataset_id=} but not a file in {path}"
            )
            continue

        # Remove the file from the list, it should be in the file_list, but if a
        # duplicate file appears in the record it will be missing the 2nd time around.
        if fr.json["title"] in file_list:
            file_list.pop(file_list.index(fr.json["title"]))
        else:
            logger.info(
                f"{fr.json['title']} appears in multiple file records in {dataset_id=}"
            )

        # Does the file pass checksum
        if fr.checksum != get_file_hash(path / fr.json["title"], fr.checksum_type):
            logger.info(f"{fpath} fails checksum {fr.checksum} | {fr.checksum_type}")

    # If there are files leftover, we didn't find them in the index record
    if file_list:
        logger.info(
            f"{path} lists files not part of the files for {dataset_id=}: {file_list}"
        )
    return


def walk_directory(
    root: Path,
    visit_dir: Iterable[Callable] = [],
    visit_file: Iterable[Callable] = [],
    extensions: Iterable[str] = [".nc"],
):
    logger.info(f"Begin search {root}")
    for path, dirs, files in os.walk(root):
        files = [f for f in files if any([f.endswith(ext) for ext in extensions])]
        if not files:
            continue
        for visit in visit_dir:
            visit(Path(path), files)
        for visit in visit_file:
            for filename in files:
                visit(Path(path) / filename)
    logger.info(f"End search {root}")
