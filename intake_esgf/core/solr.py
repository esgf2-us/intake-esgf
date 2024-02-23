"""A ESGF1 Solr index class."""

import time
from pathlib import Path
from typing import Any, Union

import pandas as pd
import requests

from intake_esgf.base import get_dataframe_columns
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
                facet: doc[facet][0]
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

    def from_tracking_ids(self, tracking_ids: Union[str, list[str]]) -> pd.DataFrame:
        total_time = time.time()
        if isinstance(tracking_ids, str):
            tracking_ids = [tracking_ids]
        response = esg_search(self.url, tracking_id=tracking_ids)["response"]
        if not response["numFound"]:
            if self.logger is not None:
                self.logger.info(f"└─{self} no results")
            raise NoSearchResults
        df = []
        for doc in response["docs"]:
            record = {
                facet: doc[facet][0]
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

    def get_file_info(self, dataset_ids: list[str]) -> dict[str, Any]:
        total_time = time.time()
        search = dict(
            type="File",
            format="application/solr+json",
            limit=1000,  # FIX: need to manually paginate
            latest=True,
            retracted=False,
            distrib=self.distrib,
            dataset_id=dataset_ids,
        )
        response = esg_search(self.url, params=search)["response"]
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
            doc["version"] = [doc["dataset_id"].split("|")[0].split(".")[-1]]
            file_path = doc["directory_format_template_"][0]
            info["path"] = (
                Path(
                    file_path.replace("%(root)s/", "")
                    .replace("%(", "{")
                    .replace(")s", "[0]}")
                    .format(**doc)
                )
                / doc["title"]
            )
            for entry in doc["url"]:
                link, _, link_type = entry.split("|")
                if link_type not in info:
                    info[link_type] = []
                info[link_type].append(link)
            infos.append(info)
        if self.logger is not None:
            self.logger.info(f"└─{self} results={len(infos)} {total_time=:.2f}")
        return infos
