"""A ESGF1 Solr index class."""
import re
import time
from pathlib import Path
from typing import Any, Union

import pandas as pd
import requests

from intake_esgf.base import get_dataset_pattern


class SolrESGFIndex:
    def __init__(self, index_node: str = "esgf-node.llnl.gov", distrib: bool = True):
        self.repr = f"SolrESGFIndex('{index_node}'{',distrib=True' if distrib else ''})"
        self.url = f"https://{index_node}/esg-search/search"
        self.distrib = distrib
        self.logger = None

    def __repr__(self):
        return self.repr

    def search(self, **search: Union[str, list[str]]) -> pd.DataFrame:
        total_time = time.time()
        search.update(
            dict(
                type="Dataset",
                format="application/solr+json",
                limit=1000,  # FIX: need to manually paginate
                latest=search["latest"] if "latest" in search else True,
                retracted=search["retracted"] if "retracted" in search else False,
                distrib=search["distrib"] if "distrib" in search else self.distrib,
            )
        )
        response_time = time.time()
        response = requests.get(self.url, params=search)
        response.raise_for_status()
        response = response.json()["response"]
        response_time = time.time() - response_time
        if not response["numFound"]:
            if self.logger is not None:
                self.logger.info(f"└─{self} no results")
            raise ValueError("Search returned no results.")
        assert response["numFound"] == len(response["docs"])
        pattern = get_dataset_pattern()
        df = []
        process_time = time.time()
        for doc in response["docs"]:
            m = re.search(pattern, doc["id"])
            if m:
                df.append(m.groupdict())
                df[-1]["id"] = doc["id"]
        process_time = time.time() - process_time
        df = pd.DataFrame(df)
        total_time = time.time() - total_time
        if self.logger is not None:
            self.logger.info(f"└─{self} {response_time=:.2f} {total_time=:.2f}")
        return df

    def from_tracking_ids(self, tracking_ids: Union[str, list[str]]) -> pd.DataFrame:
        if isinstance(tracking_ids, str):
            tracking_ids = [tracking_ids]
        raise NotImplementedError

    def get_file_info(self, dataset_ids: list[str]) -> dict[str, Any]:
        """Return a file information dictionary.

        Parameters
        ----------
        dataset_ids
            A list of datasets IDs which scientifically refer to the same files.

        """
        response_time = time.time()
        search = dict(
            type="File",
            format="application/solr+json",
            limit=1000,  # FIX: need to manually paginate
            latest=True,
            retracted=False,
            distrib=self.distrib,
            dataset_id=dataset_ids,
        )
        response = requests.get(self.url, params=search)
        response.raise_for_status()
        response = response.json()["response"]
        response_time = time.time() - response_time
        if not response["numFound"]:
            if self.logger is not None:
                self.logger.info(f"└─{self} no results")
            raise ValueError("Search returned no results.")
        assert response["numFound"] == len(response["docs"])
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
            self.logger.info(f"└─{self} results={len(infos)} {response_time=:.2f}")
        return infos
