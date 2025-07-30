"""An ESGF STAC index class.

Issues
------
- I cannot include `latest` in a filter. According to the STAC extension README,
  it should be there, but I do not see it in the extension schema (but neither
  is `retracted`, which I can query).
- How am I supposed to distinguish between assets in the index? Do I assume if
  it ends in .json it is Kerchunk? `reference_file` is not very descriptive.
- There is no checksum or size information stored in the assets. CEDA has
  assumed people will use kerchunk and not need these things.
- There is no `member_id` in CEDA's index. I am guesing that CEDA saw that
  `variant_label` is often the same (but not always) and removed one if favor of
  the other. The problem is that the former is part of the IDs of the records
  and needed so I have to hack it.

"""

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
    "data_specs_version",
    "experiment_id",
    "experiment",
    "frequency",
    "further_info_url",
    "grid_label",
    "grid",
    "institution_id",
    "mip_era",
    "nominal_resolution",
    "retracted",
    "source_id",
    "source_type",
    "table_id",
    "variable_id",
    "variable_long_name",
    "variant_label",
]


def metadata_fixes(**search_facets: Any) -> dict[str, Any]:
    """
    Remove
    """
    # FIX: `latest` is not in the STAC record
    if "latest" in search_facets:
        search_facets.pop("latest")
    # FIX: the CMIP6 stac extension does not include `member_id`
    if "member_id" in search_facets and "variant_label" not in search_facets:
        search_facets["variant_label"] = search_facets.pop("member_id")
    # There is no such thing as Dataset/File 'types'
    if "type" in search_facets:
        search_facets.pop("type")
    return search_facets


def add_defaults(**search_facets: Any) -> dict[str, Any]:
    """
    Safely add some default search behavior.
    """
    if "retracted" not in search_facets:
        search_facets["retracted"] = False
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
        collections="cmip6", limit=items_per_page, filter=cql_filter
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
        facets = projects[project.lower()].id_facets()

        # populate the dataframe with hacks
        dfs = []
        for page in items.pages():
            for item in page.items:
                row = {}
                for col in facets[:-2]:
                    lhs = col  # .replace("member_id", "variant_label")
                    rhs = "cmip6:" + col.replace("member_id", "variant_label").replace(
                        "activity_drs", "activity_id"
                    )
                    row[lhs] = item.properties[rhs]
                row["project"] = "cmip6"
                row["version"] = item.id.split(".")[-1]
                row["data_node"] = self.url
                row["id"] = f"{item.id}|{row['data_node']}"
                dfs += [row]
                self.cache[row["id"]] = item

        df = pd.DataFrame(dfs)
        total_time = time.time() - total_time
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
            for _, asset in item.assets.items():
                # Only files for now
                if not asset.href.endswith(".nc"):
                    continue

                url = str(asset.href)
                path = get_content_path(asset.href, "CMIP6")

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
                info["path"] = path

                # Not currently part of the STAC item
                info["checksum_type"] = None
                info["checksum"] = None
                info["size"] = 0

                # Parse out the file time begin/end from the filename
                tstart, tend = base.get_time_extent(str(info["path"]))
                info["file_start"] = info["file_end"] = None
                if tstart is not None:
                    info["file_start"] = tstart
                    info["file_end"] = tend
                infos[str(path)] = info
        out = [info for _, info in infos.items()]
        return out
