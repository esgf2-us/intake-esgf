"""A ESGF1 Solr index class."""

import time
from typing import Any

import pandas as pd
import requests

import intake_esgf
import intake_esgf.base as base
from intake_esgf.exceptions import NoSearchResults
from intake_esgf.projects import get_project_facets


def esg_search(base_url, **search):
    """Yields paginated responses using the ESGF REST API."""
    if "format" not in search:
        search["format"] = "application/solr+json"
    offset = search.get("offset", 0)
    limit = search.get("limit", 10)
    total = offset + limit + 1
    while (offset + limit) < total:
        response = requests.get(f"{base_url}/esg-search/search", params=search)
        response.raise_for_status()
        response = response.json()
        yield response
        limit = len(response["response"]["docs"])
        total = response["response"]["numFound"]
        offset = response["response"]["start"]
        search["offset"] = offset + limit


class SolrESGFIndex:
    def __init__(self, index_node: str = "esgf-node.llnl.gov", distrib: bool = False):
        self.repr = f"SolrESGFIndex('{index_node}'{',distrib=True' if distrib else ''})"
        self.url = f"https://{index_node}"
        self.distrib = distrib

    def __repr__(self):
        return self.repr

    def search(self, **search: str | list[str]) -> pd.DataFrame:
        logger = intake_esgf.conf.get_logger()
        search["distrib"] = search["distrib"] if "distrib" in search else self.distrib
        facets = get_project_facets(search) + intake_esgf.conf.get(
            "additional_df_cols", []
        )
        if "project" not in facets:
            facets = ["project"] + facets
        response_time = time.time()
        df = []
        for response in esg_search(self.url, **search):
            response = response["response"]
            if not response["numFound"]:
                logger.info(f"└─{self} no results")
                raise NoSearchResults
            for doc in response["docs"]:
                record = {
                    facet: doc[facet][0] if isinstance(doc[facet], list) else doc[facet]
                    for facet in facets
                    if facet in doc
                }
                record["project"] = doc["project"][0]
                record["id"] = doc["id"]
                if record["project"] == "CMIP5":
                    variables = search["variable"] if "variable" in search else []
                    if not isinstance(variables, list):
                        variables = [variables]
                    record = base.expand_cmip5_record(
                        variables,
                        doc["variable"],
                        record,
                    )
                df += record if isinstance(record, list) else [record]
        df = pd.DataFrame(df)
        response_time = time.time() - response_time
        logger = intake_esgf.conf.get_logger()
        logger.info(f"└─{self} results={len(df)} {response_time=:.2f}")
        return df

    def from_tracking_ids(self, tracking_ids: list[str]) -> pd.DataFrame:
        logger = intake_esgf.conf.get_logger()
        total_time = time.time()
        df = []
        for response in esg_search(self.url, type="File", tracking_id=tracking_ids):
            response = response["response"]
            if not response["numFound"]:
                logger.info(f"└─{self} no results")
                raise NoSearchResults
            for doc in response["docs"]:
                facets = get_project_facets(doc)
                if "project" not in facets:
                    facets = ["project"] + facets
                record = {
                    facet: doc[facet][0] if isinstance(doc[facet], list) else doc[facet]
                    for facet in facets
                    if facet in doc
                }
                record["project"] = doc["project"][0]
                record["id"] = doc["id"]
                df.append(record)
        df = pd.DataFrame(df)
        total_time = time.time() - total_time
        logger.info(f"└─{self} results={len(df)} {total_time=:.2f}")
        return df

    def get_file_info(self, dataset_ids: list[str], **facets) -> dict[str, Any]:
        logger = intake_esgf.conf.get_logger()
        response_time = time.time()
        search = dict(
            type="File",
            latest=True,
            retracted=False,
            distrib=self.distrib,
            dataset_id=dataset_ids,
        )
        search.update(facets)
        infos = []
        for response in esg_search(self.url, **search):
            response = response["response"]
            if not response["numFound"]:
                logger.info(f"└─{self} no results")
                raise NoSearchResults
            for doc in response["docs"]:
                info = {}
                info["dataset_id"] = doc["dataset_id"]
                info["checksum_type"] = doc["checksum_type"][0]
                info["checksum"] = doc["checksum"][0]
                info["size"] = doc["size"]
                info["path"] = base.get_content_path(doc)
                for entry in doc["url"]:
                    link, _, link_type = entry.split("|")
                    if link_type not in info:
                        info[link_type] = []
                    info[link_type].append(link)
                infos.append(info)
        response_time = time.time() - response_time
        logger.info(f"└─{self} results={len(infos)} {response_time=:.2f}")
        return infos
