"""A ESGF1 Solr index class."""
import re
import time
from pathlib import Path
from typing import Any, Union

import pandas as pd
from pyesgf.search import SearchConnection

from intake_esgf.base import get_dataset_pattern


class SolrESGFIndex:
    def __init__(self, index_node: str = "esgf-node.llnl.gov", distrib: bool = True):
        self.repr = f"SolrESGFIndex('{index_node}'{',distrib=True' if distrib else ''})"
        self.conn = SearchConnection(
            f"https://{index_node}/esg-search", distrib=distrib
        )
        self.response = None
        self.logger = None

    def __repr__(self):
        return self.repr

    def search(self, **search: Union[str, list[str]]) -> pd.DataFrame:
        total_time = time.time()
        if "latest" not in search:
            search["latest"] = True
        response_time = time.time()
        ctx = self.conn.new_context(facets=list(search.keys()), **search)
        response = ctx.search()
        response_time = time.time() - response_time
        self.response = response
        if not ctx.hit_count:
            if self.logger is not None:
                self.logger.info(f"└─{self} no results")
            raise ValueError("Search returned no results.")
        assert ctx.hit_count == len(response)
        pattern = get_dataset_pattern()
        df = []
        process_time = time.time()
        for dsr in response:
            m = re.search(pattern, dsr.dataset_id)
            if m:
                df.append(m.groupdict())
                df[-1]["id"] = dsr.dataset_id
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
        if self.response is None:
            return []
        infos = []
        response_time = time.time()
        for dsr in self.response:
            if dsr.dataset_id not in dataset_ids:
                continue
            for fr in dsr.file_context().search(ignore_facet_check=True):
                info = {}
                info["dataset_id"] = fr.json["dataset_id"]
                info["checksum_type"] = fr.checksum_type
                info["checksum"] = fr.checksum
                info["size"] = fr.size
                fr.json["version"] = [
                    fr.json["dataset_id"].split("|")[0].split(".")[-1]
                ]
                file_path = fr.json["directory_format_template_"][0]
                info["path"] = (
                    Path(
                        file_path.replace("%(root)s/", "")
                        .replace("%(", "{")
                        .replace(")s", "[0]}")
                        .format(**fr.json)
                    )
                    / fr.json["title"]
                )
                for link_type, link in fr.urls.items():
                    if link_type not in info:
                        info[link_type] = []
                    info[link_type].append(link[0][0])
                infos.append(info)
        response_time = time.time() - response_time
        if self.logger is not None:
            self.logger.info(f"└─{self} results={len(infos)} {response_time=:.2f}")

        return infos
