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

import intake_esgf
import intake_esgf.base as base
import intake_esgf.logging
from intake_esgf.projects import projects

# the stac extension additions that need `properties.cmip6:` prepended
CMIP6_PREPENDS = [
    "activity_id",
    "cf_standard_name",
    "citation_url",
    "data_specs_version",
    "experiment_id",
    "experiment_title",
    "frequency",
    "further_info_url",
    "grid",
    "grid_label",
    "institution_id",
    "member_id",
    "nominal_resolution",
    "product",
    "realm",
    "source_id",
    "source_type",
    "table_id",
    "variable",
    "variable_id",
    "variable_long_name",
    "variable_units",
    "variant_label",
]


def _search_facet_fixes(**search_facets: Any) -> dict[str, Any]:
    """
    Make changes to the search facets based on STAC differences.
    """
    # There is no such thing as Dataset/File 'types'
    if "type" in search_facets:
        search_facets.pop("type")
    return search_facets


def _pre_search_hacks(**search_facets: Any) -> dict[str, Any]:
    # The dc06 index uses variable and not variable_id
    if "variable_id" in search_facets:
        search_facets["variable"] = search_facets.pop("variable_id")
    return search_facets


def _post_search_hacks(
    row: dict[str, Any], properties: dict[str, Any]
) -> dict[str, Any]:
    # activity_drs is not in the extension
    if "cmip6:activity_id" in properties:
        row["activity_drs"] = properties["cmip6:activity_id"]
    # The dc06 index uses variable and not variable_id
    if "cmip6:variable" in properties:
        row["variable_id"] = properties["cmip6:variable"]
    # mip_era is not in the extension
    row["mip_era"] = row["project"]
    return row


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


def _get_content_path(url: str, project: str) -> Path:
    """
    Return the local file path parsed from a https url.
    """
    match = re.search(rf".*({project}.*.nc)|.*", url)
    if not match:
        raise ValueError(f"Could not parse out the path from {url}")
    return Path(match.group(1))


def _parse_file_validation(
    info: dict[str, Any], asset: dict[str, Any]
) -> dict[str, Any]:
    """
    Assumes a SHA256 hash was generated if not given as that what was done
    before.
    """
    if "file:checksum" not in asset:
        return info
    checksum = asset["file:checksum"]
    info["checksum_type"] = None
    info["checksum"] = checksum
    return info


def _parse_file_size(info: dict[str, Any], asset: dict[str, Any]) -> dict[str, Any]:
    if "file:size" not in asset:
        return info
    info["size"] = asset["file:size"]
    return info


def _parse_file_daterange(info: dict[str, Any]) -> dict[str, Any]:
    """
    Parse out the file time begin/end from the filename
    """
    if "path" not in info:
        info["file_start"] = info["file_end"] = None
        return info
    tstart, tend = base.get_time_extent(str(info["path"]))
    if tstart is not None:
        info["file_start"] = tstart
    if tend is not None:
        info["file_end"] = tend
    return info


class STACESGFIndex:
    def __init__(self, url: str = "api.stac.ceda.ac.uk"):
        self.url = url
        self.cache: dict[str, Any] = {}
        self.session = intake_esgf.conf.get_cached_session()
        self.logger = logging.getLogger(intake_esgf.logging.NAME)

    def __getstate__(self) -> dict[str, Any]:
        state = self.__dict__.copy()
        state.pop("session")
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.__dict__.update(state)
        self.session = intake_esgf.conf.get_cached_session()

    def __repr__(self):
        return f"STACESGFIndex('{self.url}')"

    def search(self, **search) -> pd.DataFrame:
        response_time = time.time()

        # only for CMIP6 for now
        limit = search.pop("limit") if "limit" in search else 100
        project = search.pop("project") if "project" in search else "CMIP6"
        if project.lower() != "cmip6":
            raise ValueError("STAC index only for CMIP6")

        # add some default facets if not given
        search = _search_facet_fixes(**search)
        search = _pre_search_hacks(**search)
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
                properties = item["properties"]
                row = {}
                for col in facets:
                    lookup = f"cmip6:{col}" if col in CMIP6_PREPENDS else col
                    row[col] = properties[lookup] if lookup in properties else None
                row = _post_search_hacks(row, properties)
                # to make STAC consistent with other index types
                row["data_node"] = self.url
                row["id"] = f"{item['id']}|{row['data_node']}"
                dfs.append(
                    {
                        key: val[0]
                        if (isinstance(val, list) and len(val) == 1)
                        else val
                        for key, val in row.items()
                    }
                )
                self.cache[row["id"]] = item

        df = pd.DataFrame(dfs)
        response_time = time.time() - response_time
        self.logger.info(f"└─{self} results={len(df)} {response_time=:.2f}")
        return df

    def from_tracking_ids(self, tracking_ids: list[str]) -> pd.DataFrame:
        raise NotImplementedError(
            "The STAC catalogs encode this as `pid` in the properties."
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
                # Only http links
                if not asset["description"] == "HTTPServer Link":
                    continue

                url = str(asset["href"])
                path = _get_content_path(asset["href"], "CMIP6")

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

                # Parse out information
                info = _parse_file_validation(info, asset)
                info = _parse_file_size(info, asset)
                info = _parse_file_daterange(info)

                infos[str(path)] = info
        out = [info for _, info in infos.items()]
        return out
