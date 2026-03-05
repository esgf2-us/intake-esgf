"""An ESGF STAC index class."""

import logging
import re
import time
from pathlib import Path
from typing import Any

import pandas as pd
from pystac_client import Client
from pystac_client.stac_api_io import StacApiIO

import intake_esgf
import intake_esgf.base as base
import intake_esgf.logging
from intake_esgf.projects import projects


def _get_endpoint_collections(client: Client) -> list[str]:
    """
    Return the collections of a client.
    """
    collection_search = client.collection_search()
    collections = [
        col["id"]
        for page in collection_search.pages_as_dicts()
        for col in page["collections"]
    ]
    return collections


def _get_collection_queryables(client: Client, project: str) -> list[str]:
    """
    Return the queryables of the client's collection.

    Note
    ----
    Ultimately we hope to be able to get this information from the /queryables
    endpoint of the catalog/collection. For the moment we write this function
    and can switch the implementation later if/when this capability is
    implemented.
    """
    items = client.search(collections=project, max_items=1).item_collection_as_dict()
    if not items["features"]:
        raise ValueError(f"No queryables for {project=}")
    queryables = [prop for prop in items["features"][0]["properties"]]
    return queryables


def _fix_facets(
    search_facets: dict[str, Any], project: str, queryables: list[str]
) -> dict[str, Any]:
    """
    Transform the traditional search facets for a given project into
    STAC-compliant facets.

    Note
    ----
    1. They should be prepended with `properties` unless not querying in the
       properies. At the moment, I am not sure users will do this. FIX.
    2. If part of the project's extension, they should also be prepended with
       `project:`
    """
    project_prepends = [
        q.split(":")[-1] for q in queryables if q.startswith(f"{project.lower()}:")
    ]
    search_facets = {
        f"{project.lower() + ':' if key in project_prepends else ''}{key}": value
        for key, value in search_facets.items()
    }
    not_valid = set(search_facets) - set(queryables)
    if not_valid:
        possible = [q.replace(f"{project.lower()}:", "") for q in queryables]
        raise ValueError(
            f"Some of your search criteria {not_valid=} are not supported in this {project=}. These are {possible=}."
        )
    search_facets = {
        f"properties.{key}": value if isinstance(value, list) else [value]
        for key, value in search_facets.items()
    }
    return search_facets


def _search_facets_to_cql_filter(
    search_facets: dict[str, Any], project: str, queryables: list[str]
) -> dict[str, Any]:
    """
    Convert traditional search facets to a STAC filter.

    Note
    ----
    STAC extensions require prepending additional properties with a namespace.
    This makes sense in a world where you need to be able to search across
    projects, but in our case is not needed.
    """
    search_facets = _fix_facets(search_facets, project, queryables)
    cql_filter = {
        "op": "and",
        "args": [
            {
                "op": "in",
                "args": [{"property": facet}, facet_values],
            }
            for facet, facet_values in search_facets.items()
        ],
    }
    return cql_filter


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

        # Intercept some options, some have special handling, others aren't used
        limit = search.pop("limit") if "limit" in search else 100
        project = search.pop("project") if "project" in search else "CMIP6"
        _ = search.pop("type") if "type" in search else ""

        # Initialize the client
        stac_io = StacApiIO()
        stac_io.session = self.session
        client = Client.open(f"https://{self.url}", stac_io=stac_io)

        # Ensure there is something to find
        collections = _get_endpoint_collections(client)
        if project not in collections:
            response_time = time.time() - response_time
            self.logger.info(f"└─{self} project not found {response_time=:.2f}")
            return pd.DataFrame()

        # Form the filter and search
        queryables = _get_collection_queryables(client, project)
        cql_filter = _search_facets_to_cql_filter(search, project, queryables)
        items = client.search(collections=project, limit=limit, filter=cql_filter)

        # What facets do we expect in the output?
        facets = (
            [
                "project",
            ]
            + projects[project.lower()].master_id_facets()
            + intake_esgf.conf["additional_df_cols"]
        )

        # Populate the dataframe
        dfs = []
        for page in items.pages_as_dicts():
            for item in page["features"]:
                properties = item["properties"]
                row = {}
                for col in facets:
                    if col in queryables:
                        lookup = col
                    elif f"cmip6:{col}" in queryables:
                        lookup = f"cmip6:{col}"
                    else:
                        lookup = ""  # Not in there, just skip
                    row[col] = properties[lookup] if lookup in properties else None
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
                self.cache[str(row["id"])] = item

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
