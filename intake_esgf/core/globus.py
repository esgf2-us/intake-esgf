"""A Globus-based ESGF1 style index."""
import re
import time
from pathlib import Path
from typing import Any, Union

import pandas as pd
from globus_sdk import SearchClient, SearchQuery

from intake_esgf.base import get_dataset_pattern


def _form_path(content):
    content["version"] = [content["dataset_id"].split("|")[0].split(".")[-1]]
    file_path = content["directory_format_template_"][0]
    return (
        Path(
            file_path.replace("%(root)s/", "")
            .replace("%(", "{")
            .replace(")s", "[0]}")
            .format(**content)
        )
        / content["title"]
    )


class GlobusESGFIndex:
    GLOBUS_INDEX_IDS = {
        "anl-dev": "d927e2d9-ccdb-48e4-b05d-adbc3d97bbc5",
        "ornl-dev": "ea4595f4-7b71-4da7-a1f0-e3f5d8f7f062",
    }

    def __init__(self, index_id="anl-dev"):
        self.repr = f"GlobusESGFIndex('{index_id}')"
        if index_id in GlobusESGFIndex.GLOBUS_INDEX_IDS:
            index_id = GlobusESGFIndex.GLOBUS_INDEX_IDS[index_id]
        self.index_id = index_id
        self.client = SearchClient()
        self.logger = None

    def __repr__(self):
        return self.repr

    def search(self, **search: Union[str, list[str]]) -> pd.DataFrame:
        """Search the index and return as a pandas dataframe.

        This function uses the Globus `post_search()` function where our query consists
        of a `match_any` filter for each of the keywords given in the input `search`. We
        manually add constraints to only look for Dataset entries that are flagged as
        the latest version. Note that this version of the index only contains CMIP6
        entries.

        """
        # process inputs
        search["type"] = "Dataset"
        if "latest" not in search:
            search["latest"] = True

        # the ALCF index encodes booleans as strings
        if "anl-dev" in self.repr:
            for key, val in search.items():
                if isinstance(val, bool):
                    search[key] = str(val)

        # build up the query and search
        query_data = SearchQuery("")
        search = {k: v for k, v in search.items() if len(v) > 0}
        for key, val in search.items():
            query_data.add_filter(
                key, val if isinstance(val, list) else [val], type="match_any"
            )
        response_time = time.time()
        sc = SearchClient()
        paginator = sc.paginated.post_search(self.index_id, query_data)
        paginator.limit = 1000
        pattern = get_dataset_pattern()
        df = []
        for response in paginator:
            for g in response["gmeta"]:
                m = re.search(pattern, g["subject"])
                if m:
                    df.append(m.groupdict())
                    df[-1]["id"] = g["subject"]
        df = pd.DataFrame(df)
        response_time = time.time() - response_time

        # logging
        if self.logger is not None:
            self.logger.info(f"└─{self} results={len(df)} {response_time=:.2f}")
        return df

    def get_file_info(self, dataset_ids: list[str]) -> dict[str, Any]:
        """"""
        response_time = time.time()
        sc = SearchClient()
        paginator = sc.paginated.post_search(
            self.index_id,
            SearchQuery("")
            .add_filter("type", ["File"])
            .add_filter("dataset_id", dataset_ids, type="match_any"),
        )
        paginator.limit = 1000
        infos = []
        for response in paginator:
            for g in response.get("gmeta"):
                assert len(g["entries"]) == 1
                content = g["entries"][0]["content"]
                info = {
                    "dataset_id": content["dataset_id"],
                    "checksum_type": content["checksum_type"][0],
                    "checksum": content["checksum"][0],
                    "size": content["size"],
                    "HTTPServer": [
                        url.split("|")[0]
                        for url in content["url"]
                        if "HTTPServer" in url
                    ],
                }
                # build the path from the template
                content["version"] = [
                    content["dataset_id"].split("|")[0].split(".")[-1]
                ]
                info["path"] = _form_path(content)
                infos.append(info)
        response_time = time.time() - response_time
        if self.logger is not None:
            self.logger.info(f"└─{self} results={len(infos)} {response_time=:.2f}")
        return infos

    def from_tracking_ids(self, tracking_ids: Union[str, list[str]]) -> pd.DataFrame:
        if isinstance(tracking_ids, str):
            tracking_ids = [tracking_ids]
        response = SearchClient().post_search(
            self.index_id,
            SearchQuery("").add_filter("tracking_id", tracking_ids, type="match_any"),
        )
        pattern = get_dataset_pattern()
        df = []
        for g in response["gmeta"]:
            try:
                dataset_id = g["entries"][0]["content"]["dataset_id"]
            except Exception:
                continue
            m = re.search(pattern, dataset_id)
            if m:
                df.append(m.groupdict())
                df[-1]["id"] = dataset_id
        df = pd.DataFrame(df)
        return df
