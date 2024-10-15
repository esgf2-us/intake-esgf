"""A Globus-based ESGF1 style index."""

import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from globus_sdk import (
    GlobusHTTPResponse,
    NativeAppAuthClient,
    RefreshTokenAuthorizer,
    SearchClient,
    SearchQuery,
    TransferAPIError,
    TransferClient,
    TransferData,
)
from globus_sdk.tokenstorage import SimpleJSONFileAdapter

import intake_esgf
import intake_esgf.base as base
from intake_esgf.exceptions import GlobusTransferError
from intake_esgf.projects import get_project_facets

CLIENT_ID = "81a13009-8326-456e-a487-2d1557d8eb11"  # intake-esgf


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

    def __repr__(self):
        return self.repr

    def search(self, **search: str | list[str]) -> pd.DataFrame:
        """Search the index and return as a pandas dataframe.

        This function uses the Globus `post_search()` function where our query consists
        of a `match_any` filter for each of the keywords given in the input `search`. We
        manually add constraints to only look for Dataset entries that are flagged as
        the latest version. Note that this version of the index only contains CMIP6
        entries.

        """
        # the ALCF index encodes booleans as strings
        if "anl-dev" in self.repr:
            for key, val in search.items():
                if isinstance(val, bool):
                    search[key] = str(val)

        # build up the query and search
        query_data = SearchQuery("")
        for key, val in search.items():
            query_data.add_filter(
                key, val if isinstance(val, list) else [val], type="match_any"
            )

        facets = get_project_facets(search) + intake_esgf.conf.get(
            "additional_df_cols", []
        )
        if "project" not in facets:
            facets = ["project"] + facets

        response_time = time.time()
        sc = SearchClient()
        paginator = sc.paginated.post_search(self.index_id, query_data)
        paginator.limit = 1000
        df = []
        for response in paginator:
            for g in response["gmeta"]:
                content = g["entries"][0]["content"]
                record = {
                    facet: (
                        content[facet][0]
                        if isinstance(content[facet], list)
                        else content[facet]
                    )
                    for facet in facets
                    if facet in content
                }
                record["project"] = content["project"][0]
                record["id"] = g["subject"]
                if record["project"] == "CMIP5":
                    variables = search["variable"] if "variable" in search else []
                    if not isinstance(variables, list):
                        variables = [variables]
                    record = base.expand_cmip5_record(
                        variables,
                        content["variable"],
                        record,
                    )
                df += record if isinstance(record, list) else [record]
        df = pd.DataFrame(df)
        response_time = time.time() - response_time
        logger = intake_esgf.conf.get_logger()
        logger.info(f"└─{self} results={len(df)} {response_time=:.2f}")
        return df

    def get_file_info(self, dataset_ids: list[str], **facets) -> dict[str, Any]:
        """Get file information for the given datasets."""
        response_time = time.time()
        sc = SearchClient()
        query = (
            SearchQuery("")
            .add_filter("type", ["File"])
            .add_filter("dataset_id", dataset_ids, type="match_any")
        )
        for facet, val in facets.items():
            query.add_filter(
                facet, val if isinstance(val, list) else [val], type="match_any"
            )
        paginator = sc.paginated.post_search(self.index_id, query)
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
                    "OPENDAP": [
                        url.split("|")[0].replace(".html", "")
                        for url in content["url"]
                        if "OPENDAP" in url
                    ],
                    "Globus": [
                        url.split("|")[0] for url in content["url"] if "Globus" in url
                    ],
                }
                info["path"] = base.get_content_path(content)
                infos.append(info)
        response_time = time.time() - response_time
        logger = intake_esgf.conf.get_logger()
        logger.info(f"└─{self} results={len(infos)} {response_time=:.2f}")
        return infos

    def from_tracking_ids(self, tracking_ids: list[str]) -> pd.DataFrame:
        response = SearchClient().post_search(
            self.index_id,
            SearchQuery("").add_filter("tracking_id", tracking_ids, type="match_any"),
        )
        df = []
        for g in response["gmeta"]:
            content = g["entries"][0]["content"]
            facets = get_project_facets(content)
            if "project" not in facets:
                facets = ["project"] + facets
            record = {
                facet: (
                    content[facet][0]
                    if isinstance(content[facet], list)
                    else content[facet]
                )
                for facet in facets
                if facet in content
            }
            record["project"] = content["project"][0]
            record["id"] = content["dataset_id"]
            df.append(record)
        df = pd.DataFrame(df)
        return df


def variable_info(query: str, project: str = "CMIP6") -> pd.DataFrame:
    """Return a dataframe with variable information from a query."""
    # first we populate a list of related veriables
    q = (
        SearchQuery(query)
        .add_filter("type", ["Dataset"])
        .add_filter("project", [project])
        .add_facet("variable_id", "variable_id")
        .add_facet("variable", "variable")
        .set_limit(0)
    )
    response = SearchClient().post_search("ea4595f4-7b71-4da7-a1f0-e3f5d8f7f062", q)
    variables = list(
        set(
            [
                bucket["value"]
                for fr in response.data["facet_results"]
                for bucket in fr["buckets"]
            ]
        )
    )
    # which facet do we use for variables?
    var_facet = [fr["name"] for fr in response.data["facet_results"] if fr["buckets"]]
    assert var_facet
    var_facet = var_facet[0]
    # then we loop through them and extract information for the user
    df = []
    for v in variables:
        q = (
            SearchQuery("")
            .add_filter("type", ["Dataset"])
            .add_filter("project", [project])
            .add_filter(var_facet, [v])  # need to abstract this
            .set_limit(1)
        )
        response = SearchClient().post_search("ea4595f4-7b71-4da7-a1f0-e3f5d8f7f062", q)
        for doc in response.get("gmeta"):
            content = doc["entries"][0]["content"]
            columns = [var_facet]
            columns += [key for key in content if "variable_" in key]
            columns += [key for key in content if "name" in key]
            df.append({key: content[key][0] for key in set(columns)})
    df = pd.DataFrame(df).sort_values(var_facet).set_index(var_facet)
    return df


def get_authorized_transfer_client() -> TransferClient:
    """Return a transfer client authorized to make transfers."""
    config_path = Path.home() / ".config" / "intake-esgf"
    token_adapter = SimpleJSONFileAdapter(config_path / "tokens.json")
    client = NativeAppAuthClient(CLIENT_ID)
    tokens = None
    if token_adapter.file_exists():
        tokens = token_adapter.get_token_data("transfer.api.globus.org")
    if (
        tokens is None
        or datetime.fromtimestamp(tokens["expires_at_seconds"]) < datetime.now()
    ):
        client.oauth2_start_flow()
        authorize_url = client.oauth2_get_authorize_url()
        print(
            f"""
All interactions with Globus must be authorized. To ensure that we have permission to faciliate your transfer, please open the following link in your browser.

{authorize_url}

You will have to login (or be logged in) to your Globus account. Globus will also request that you give a label for this authorization. You may pick anything of your choosing. After following the instructions in your browser, Globus will generate a code which you must copy and paste here and then hit <enter>.\n"""
        )
        auth_code = input("> ").strip()
        token_response = client.oauth2_exchange_code_for_tokens(auth_code)
        token_adapter.store(token_response)
        tokens = token_response.by_resource_server["transfer.api.globus.org"]
    authorizer = RefreshTokenAuthorizer(
        tokens["refresh_token"],
        client,
        access_token=tokens["access_token"],
        expires_at=tokens["expires_at_seconds"],
        on_refresh=token_adapter.on_refresh,
    )
    transfer_client = TransferClient(authorizer=authorizer)
    return transfer_client


def create_globus_transfer(
    infos: list[dict], globus_endpoint: str, globus_path: str | Path = ""
) -> list[GlobusHTTPResponse]:
    """
    Create

    Parameters
    ----------
    infos : list[dict]
        The file information which has come from the
    """
    # if nothing to do just return
    if not infos:
        return []

    # is the destination endpoint active?
    client = get_authorized_transfer_client()
    try:
        client.get_endpoint(globus_endpoint)
    except TransferAPIError as exc:
        print(exc)
        raise ValueError(
            f"There was a Globus error associated with your destination endpoint: {globus_endpoint}"
        )

    # we want to launch as few tasks as we can, so let's see how many files are
    # available on each endpoint.
    active_endpoints = {}
    for i, info in enumerate(infos):
        infos[i]["added"] = False  # has this file been added to a task?
        for uuid in info["active_endpoints"]:
            if uuid not in active_endpoints:
                active_endpoints[uuid] = 0
            active_endpoints[uuid] += 1

    # create globus transfers, starting with the endpoint that has the most files
    tasks = []
    for source_uuid in sorted(active_endpoints, key=active_endpoints.get, reverse=True):
        task_data = TransferData(
            source_endpoint=source_uuid, destination_endpoint=globus_endpoint
        )
        for i, info in enumerate(infos):
            if info["added"]:
                continue
            possible = [g for g in info["Globus"] if source_uuid in g]
            if not possible:
                continue
            m = re.search(r"globus:/*([a-z0-9\-]+)/(.*)", possible[0])
            if not m:
                continue
            task_data.add_item(m.group(2), str(Path(globus_path) / info["path"]))
            infos[i]["added"] = True

        # only submit the transfer if there is data
        if task_data["DATA"]:
            task_doc = client.submit_transfer(task_data)
            tasks.append(task_doc)

    # make sure everything was submitted
    assert min([info["added"] for info in infos]) == 1
    return tasks


def monitor_globus_transfer(tasks: list[GlobusHTTPResponse]) -> None:
    """
    Monitor the status of the provided Globus transfers.


    """
    client = get_authorized_transfer_client()
    for task_doc in tasks:
        time_interval = 5.0
        response = client.get_task(task_doc["task_id"])
        while response["status"] == "ACTIVE":
            time.sleep(time_interval)
            time_interval = min(time_interval * 1.1, 30.0)
            response = client.get_task(task_doc["task_id"])
        if response.data["status"] != "SUCCEEDED":
            raise GlobusTransferError(response)
