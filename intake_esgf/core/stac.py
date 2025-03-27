"""An ESGF STAC index class."""

import time
from typing import Any

import pandas as pd
from pystac_client import Client, ItemSearch

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
    "source_id",
    "source_type",
    "table_id",
    "variable_id",
    "variable_long_name",
    "variant_label",
]


def add_defaults(**search_facets: str | list[str]) -> dict[str, str | list[str]]:
    """
    Safely add some default search behavior.
    """
    return search_facets
    if "latest" not in search_facets:
        search_facets["latest"] = True
    if "retracted" not in search_facets:
        search_facets["retracted"] = False
    return search_facets


def search_cmip6(
    base_url: str, items_per_page: int = 100, **search_facets: str | list[str]
) -> ItemSearch:
    """
    Returns a STAC client item search filtered by the search facets.

    Parameters
    ----------
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
    results = Client.open(base_url).search(
        collections="cmip6", limit=items_per_page, filter=cql_filter
    )
    return results


class STACESGFIndex:
    def __init__(self, url: str = "api.stac.ceda.ac.uk"):
        self.url = url

    def __repr__(self):
        return f"STACESGFIndex('{self.url}')"

    def search(self, **search) -> pd.DataFrame:
        total_time = time.time()

        # only for CMIP6 for now
        limit = search.pop("limit") if "limit" in search else 100
        project = search.pop("project") if "project" in search else "CMIP6"
        if project.lower() != "cmip6":
            raise ValueError("STAC index only for CMIP6")

        # the CMIP6 stac extension does not include `member_id`
        if "member_id" in search and "variant_label" not in search:
            search["variant_label"] = search.pop("member_id")

        # add some default facets if not given
        search = add_defaults(**search)
        items = search_cmip6(f"https://{self.url}", limit, **search)

        # what facets do we expect?
        facets = projects[project.lower()].id_facets()

        # populate the dataframe with hacks
        df = []
        for page in items.pages():
            for item in page.items:
                row = {}
                for col in facets[:-2]:
                    lhs = col.replace("member_id", "variant_label")
                    rhs = "cmip6:" + col.replace("member_id", "variant_label").replace(
                        "activity_drs", "activity_id"
                    )
                    row[lhs] = item.properties[rhs]
                row["project"] = "cmip6"
                row["version"] = item.id.split(".")[-1]
                row["data_node"] = self.url
                df += [row]

        df = pd.DataFrame(df)
        total_time = time.time() - total_time
        return df

    def from_tracking_ids(self, tracking_ids: list[str]) -> pd.DataFrame:
        raise NotImplementedError

    def get_file_info(self, dataset_ids: list[str], **facets) -> dict[str, Any]:
        raise NotImplementedError


if __name__ == "__main__":
    ind = STACESGFIndex()
    df = ind.search(
        mip_era="CMIP6",
        activity_id="CMIP",
        institution_id="AS-RCEC",
        source_id="TaiESM1",
        experiment_id="historical",
        variant_label="r1i1p1f1",
        # frequency="mon",
        # variable_id="clt",
        limit=1,
    )
