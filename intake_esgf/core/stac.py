"""An ESGF STAC index class."""

import time
from typing import Any

import pandas as pd
import pystac
import pystac_client


def unbundle_item(item: pystac.Item) -> dict | dict:
    """Create a and a file info from the STAC item."""
    # temporary hacks to make STAC response compatible with what my other functions return
    cmip6_facets = [
        "mip_era",
        "activity_drs",
        "institution_id",
        "source_id",
        "experiment_id",
        "member_id",
        "table_id",
        "variable_id",
        "grid_label",
        "version",
    ]
    print(item.__dict__)
    dataset_row = {
        key: item.properties[key] for key in cmip6_facets if key in item.properties
    }
    dataset_row["member_id"] = item.properties["variant_label"]
    dataset_row["project"] = "CMIP6"
    dataset_row["mip_era"] = "CMIP6"
    dataset_row["table_id"] = item.id.split(".")[-4]
    dataset_row["grid_label"] = item.id.split(".")[-2]
    dataset_row["version"] = item.id.split(".")[-1]
    dataset_row["id"] = f"{item.id}|stac.ceda.ack.uk"  # another temporary hack
    return dataset_row, {}


class STACESGFIndex:
    def __init__(self, url: str = "https://api.stac.ceda.ac.uk"):
        self.url = url

    def __repr__(self):
        return f"STACESGFIndex('{self.url}')"

    def search(self, **search) -> pd.DataFrame:
        total_time = time.time()
        client = pystac_client.Client.open(self.url)
        project = search.pop("project") if "project" in search else "CMIP6"
        limit = search.pop("limit") if "limit" in search else 10
        results = client.search(
            collections=[project.lower()],  # called projects in the current indices
            max_items=limit,
            query=[f"{key}={value}" for key, value in search.items()],
        )
        df = []
        for item in results.items():
            row, _ = unbundle_item(item)
            df.append(row)
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
