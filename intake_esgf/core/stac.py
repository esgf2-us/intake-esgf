"""An ESGF STAC index class."""

import logging
import re
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from pystac_client import Client, ItemSearch
from pystac_client.stac_api_io import StacApiIO

import intake_esgf.base as base
import intake_esgf.logging
from intake_esgf.projects import projects

# the stac extension additions that need `properties.cmip6:` prepended
CMIP6_PREPENDS = [
    "activity_id",
    "citation_url",
    "conventions",
    "data_specs_version",
    "experiment",
    "experiment_id",
    "forcing_index",
    "frequency",
    "further_info_url",
    "grid",
    "grid_label",
    "initialization_index",
    "institution",
    "institution_id",
    "license",
    "member_id",
    "nominal_resolution",
    "physics_index",
    "pid",
    "product",
    "realization_index",
    "realm",
    "source",
    "source_id",
    "source_type",
    "sub_experiment",
    "sub_experiment_id",
    "table_id",
    "variable_cf_standard_name",
    "variable",
    "variable_id",
    "variable_long_name",
    "variable_units",
    "variant_label",
    "version",
]


def metadata_fixes(**search_facets: Any) -> dict[str, Any]:
    """
    Remove
    """
    # There is no such thing as Dataset/File 'types'
    if "type" in search_facets:
        search_facets.pop("type")
    return search_facets


def add_defaults(**search_facets: Any) -> dict[str, Any]:
    """
    Safely add some default search behavior.
    """
    # if "retracted" not in search_facets:
    #    search_facets["retracted"] = False
    return search_facets


def search_cmip6(
    session: requests.Session,
    base_url: str,
    items_per_page: int = 100,
    **search_facets: Any,
) -> ItemSearch:
    """
    Returns a STAC client item search filtered by the search facets.

    Parameters
    ----------
    session:
        The requests session to use for the STAC API.
    base_url : str
        The URL of the STAC API.
    items_per_page : int, optional
        The number of items to return per page.
    **search_facets : str, list[str]
        The traditional search facts expressed as additional keyword arguments,
        for example `variable_id=['tas','pr']` or `source_id='UKESM1-0-LL'`.

    Returns
    -------
    ItemSearch
        The STAC search results.
    """
    # Create a filter using Common Query Language 2
    cql_filter = {
        "op": "and",
        "args": [
            {
                "op": "in",
                "args": [
                    {
                        "property": f"properties.{'cmip6:' if facet in CMIP6_PREPENDS else ''}{facet}"
                    },
                    facet_values if isinstance(facet_values, list) else [facet_values],
                ],
            }
            for facet, facet_values in search_facets.items()
        ],
    }
    # Initialize the client and search
    stac_io = StacApiIO()
    stac_io.session = session
    results = Client.open(base_url, stac_io=stac_io).search(
        collections="CMIP6", limit=items_per_page, filter=cql_filter
    )
    return results


def get_content_path(url: str, project: str) -> Path:
    """
    Return the local file path parsed from a https url.
    """
    match = re.search(rf".*({project}.*.nc)|.*", url)
    if not match:
        raise ValueError(f"Could not parse out the path from {url}")
    return Path(match.group(1))


def _delist_row(row: dict[str, Any]) -> dict[str, str | int]:
    row = {
        key: val[0] if (isinstance(val, list) and len(val) == 1) else val
        for key, val in row.items()
    }
    return row


class STACESGFIndex:
    def __init__(self, url: str = "api.stac.ceda.ac.uk"):
        self.url = url
        self.cache: dict[str, Any] = {}
        self.session = requests.Session()
        self.logger = logging.getLogger(intake_esgf.logging.NAME)

    def __repr__(self):
        return f"STACESGFIndex('{self.url}')"

    def search(self, **search) -> pd.DataFrame:
        total_time = time.time()

        # only for CMIP6 for now
        limit = search.pop("limit") if "limit" in search else 100
        project = search.pop("project") if "project" in search else "CMIP6"
        if project.lower() != "cmip6":
            raise ValueError("STAC index only for CMIP6")

        # add some default facets if not given
        search = metadata_fixes(**search)
        search = add_defaults(**search)

        items = search_cmip6(self.session, f"https://{self.url}", limit, **search)

        # what facets do we expect?
        facets = (
            [
                "project",
            ]
            + projects[project.lower()].master_id_facets()
            + intake_esgf.conf["additional_df_cols"]
        )

        # populate the dataframe with hacks
        dfs = []
        for page in items.pages_as_dicts():
            for item in page["features"]:
                print(" --------- ")
                properties = item["properties"]
                for key, val in properties.items():
                    print(f"{key:>30} {val}")
                row = {}
                for col in facets:
                    lookup = f"cmip6:{col}" if col in CMIP6_PREPENDS else col
                    row[col] = properties[lookup] if lookup in properties else None
                # manual fixes, maybe not problems for STAC but will show up for compatibility with other clients
                row["activity_drs"] = properties["cmip6:activity_id"]
                row["mip_era"] = row["project"]
                row["data_node"] = self.url
                rowid = f"{item['id']}|{row['data_node']}"
                row["id"] = rowid
                dfs += [_delist_row(row)]
                self.cache[rowid] = item

        df = pd.DataFrame(dfs)
        total_time = time.time() - total_time
        print(df)
        return df

    def from_tracking_ids(self, tracking_ids: list[str]) -> pd.DataFrame:
        raise NotImplementedError(
            "The STAC catalogs don't even have tracking ids in the items."
        )

    def get_file_info(
        self, dataset_ids: list[str], **facets: Any
    ) -> list[dict[str, Any]]:
        infos: dict[str, Any] = {}
        for dataset_id in dataset_ids:
            # Load the file info from the saved items
            if dataset_id not in self.cache:
                raise ValueError(f"{dataset_id=} not in the STAC index cache")
            item = self.cache[dataset_id]
            for _, asset in item["assets"].items():
                # Only files for now
                if not asset["href"].endswith(".nc"):
                    continue

                url = str(asset["href"])
                path = get_content_path(asset["href"], "CMIP6")

                # We could need to append to an existing location
                if str(path) in infos:
                    info = infos[str(path)]
                else:
                    info = {}

                # Append location information
                info["dataset_id"] = dataset_id
                if "HTTPServer" not in info:
                    info["HTTPServer"] = []
                info["HTTPServer"] += [url]

                # File information
                info["path"] = path
                checksum = asset["file:checksum"]
                checksum_type, checksum = (
                    checksum.split(":") if ":" in checksum else None,
                    checksum,
                )
                info["checksum_type"] = checksum_type
                info["checksum"] = checksum
                info["size"] = asset["file:size"]

                # Parse out the file time begin/end from the filename
                tstart, tend = base.get_time_extent(str(info["path"]))
                info["file_start"] = info["file_end"] = None
                if tstart is not None:
                    info["file_start"] = tstart
                    info["file_end"] = tend
                infos[str(path)] = info
        out = [info for _, info in infos.items()]
        print(out)
        return out
