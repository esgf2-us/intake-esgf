"""A ESGF1 Solr index class."""

import time
from typing import Any, Union

import pandas as pd
import requests

from intake_esgf.base import (
    expand_cmip5_record,
    get_content_path,
    get_dataframe_columns,
)
from intake_esgf.exceptions import NoSearchResults


def esg_search(base_url, **search):
    """Return an esg-search response as a dictionary."""
    if "format" not in search:
        search["format"] = "application/solr+json"
    response = requests.get(f"{base_url}/esg-search/search", params=search)
    response.raise_for_status()
    return response.json()


class SolrESGFIndex:
    def __init__(self, index_node: str = "esgf-node.llnl.gov", distrib: bool = False):
        self.repr = f"SolrESGFIndex('{index_node}'{',distrib=True' if distrib else ''})"
        self.url = f"https://{index_node}"
        self.distrib = distrib
        self.logger = None

    def __repr__(self):
        return self.repr

    def search(self, **search: Union[str, list[str]]) -> pd.DataFrame:
        search["distrib"] = search["distrib"] if "distrib" in search else self.distrib
        total_time = time.time()
        response = esg_search(self.url, limit=1000, **search)["response"]
        if not response["numFound"]:
            if self.logger is not None:
                self.logger.info(f"└─{self} no results")
            raise NoSearchResults
        assert response["numFound"] == len(response["docs"])  # FIX: need to paginate
        df = []
        for doc in response["docs"]:
            record = {
                facet: doc[facet][0] if isinstance(doc[facet], list) else doc[facet]
                for facet in get_dataframe_columns(doc)
                if facet in doc
            }
            record["project"] = doc["project"][0]
            record["id"] = doc["id"]
            if record["project"] == "CMIP5":
                variables = search["variable"] if "variable" in search else []
                if not isinstance(variables, list):
                    variables = [variables]
                record = expand_cmip5_record(
                    variables,
                    doc["variable"],
                    record,
                )
            df += record if isinstance(record, list) else [record]
        df = pd.DataFrame(df)
        total_time = time.time() - total_time
        if self.logger is not None:
            self.logger.info(f"└─{self} results={len(df)} {total_time=:.2f}")
        return df

    def from_tracking_ids(self, tracking_ids: list[str]) -> pd.DataFrame:
        total_time = time.time()
        response = esg_search(self.url, type="File", tracking_id=tracking_ids)[
            "response"
        ]
        if not response["numFound"]:
            if self.logger is not None:
                self.logger.info(f"└─{self} no results")
            raise NoSearchResults
        df = []
        for doc in response["docs"]:
            record = {
                facet: doc[facet][0] if isinstance(doc[facet], list) else doc[facet]
                for facet in get_dataframe_columns(doc)
                if facet in doc
            }
            record["project"] = doc["project"][0]
            record["id"] = doc["id"]
            df.append(record)
        df = pd.DataFrame(df)
        total_time = time.time() - total_time
        if self.logger is not None:
            self.logger.info(f"└─{self} results={len(df)} {total_time=:.2f}")
        return df

    def get_file_info(self, dataset_ids: list[str], **facets) -> dict[str, Any]:
        total_time = time.time()
        search = dict(
            type="File",
            limit=1000,  # FIX: need to manually paginate
            latest=True,
            retracted=False,
            distrib=self.distrib,
            dataset_id=dataset_ids,
        )
        search.update(facets)
        response = esg_search(self.url, **search)["response"]
        if not response["numFound"]:
            if self.logger is not None:
                self.logger.info(f"└─{self} no results")
            raise NoSearchResults
        assert response["numFound"] == len(response["docs"])  # FIX: paginate
        infos = []
        for doc in response["docs"]:
            info = {}
            info["dataset_id"] = doc["dataset_id"]
            info["checksum_type"] = doc["checksum_type"][0]
            info["checksum"] = doc["checksum"][0]
            info["size"] = doc["size"]
            info["path"] = get_content_path(doc)
            for entry in doc["url"]:
                link, _, link_type = entry.split("|")
                if link_type not in info:
                    info[link_type] = []
                info[link_type].append(link)
            infos.append(info)
        if self.logger is not None:
            self.logger.info(f"└─{self} results={len(infos)} {total_time=:.2f}")
        return infos
