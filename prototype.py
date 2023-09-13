import re

import pandas as pd
from globus_sdk import SearchClient
from globus_sdk.response import GlobusHTTPResponse

# create an alias for the different indices we want to support. Could easily extend to
# obs4mips or whatever else we wish to support.
GLOBUS_INDEX_IDS = {
    "anl-dev": "d927e2d9-ccdb-48e4-b05d-adbc3d97bbc5",
}


def response_to_dataframe(response: GlobusHTTPResponse, pattern: str) -> pd.DataFrame:
    """Return the dataset entries from the response of a Globus search."""
    df = []
    for g in response.get("gmeta"):
        assert len(g["entries"]) == 1  # A check on the assumption of a single entry
        if g["entries"][0]["entry_id"] != "dataset":  # Manually remove files
            continue
        m = re.search(pattern, g["subject"])
        if m:
            df.append(m.groupdict())
            df[-1]["subject"] = g["subject"]
    df = pd.DataFrame(df)
    return df


def get_dataset_pattern() -> str:
    """Return the Globus subject regular expression pattern for datasets."""
    COLUMNS = [
        "mip_era",
        "activity_id",
        "institution_id",
        "source_id",
        "experiment_id",
        "member_id",
        "table_id",
        "variable_id",
        "grid_label",
        "version",
        "data_node",
    ]
    pattern = "\.".join([f"(?P<{c}>\S[^.|]+)" for c in COLUMNS[:-1]])
    pattern += f"\|(?P<{COLUMNS[-1]}>\S+)"
    return pattern


class ESGFCatalog:
    def __init__(self, index_id="anl-dev"):
        assert index_id in GLOBUS_INDEX_IDS
        self.index_id = GLOBUS_INDEX_IDS[index_id]
        self.df = None
        self.total_results = 0

    def __repr__(self):
        if self.df is None:
            return "Perform a search() to populate the catalog"
        repr = f"Displaying summary info for {len(self.df)} out of {self.total_results} results:\n"  # noqa: E501
        return repr + self.unique().__repr__()

    def unique(self):
        out = {}
        for col in self.df.columns:
            if col in ["subject", "version", "data_node"]:
                continue
            out[col] = self.df[col].unique()
        return pd.Series(out)

    def search(self, **search):
        search["type"] = "Dataset"  # only search for datasets, not files
        if "latest" not in search:  # by default only find the latest
            search["latest"] = True
        # convert booleans to strings
        for key, val in search.items():
            if isinstance(val, bool):
                search[key] = str(val)
        query = " AND ".join(
            [
                f'({key}: "{val}")'
                if isinstance(val, str)
                else "(" + " OR ".join([f'({key}: "{v}")' for v in val]) + ")"
                for key, val in search.items()
            ]
        )
        client = SearchClient()
        result = client.search(self.index_id, query, limit=1000, advanced=True)
        self.total_results = result["total"]
        if not self.total_results:
            return self
        self.df = response_to_dataframe(result, get_dataset_pattern())
        return self

    def to_dataset_dict(self):
        if self.df is None or len(self.df) == 0:
            raise ValueError("No entries to retrieve.")


if __name__ == "__main__":
    # The ESGF catalog initializes to nothing and needs populated with an initial
    # search. This uses the globus sdk to query their index and the response is parsed
    # into a pandas dataframe for viewing.
    cat = ESGFCatalog().search(
        activity_id="CMIP",
        experiment_id="historical",
        source_id="CESM2",
        variable_id=["gpp", "areacella", "sftlf"],
        member_id=["r1i1p1f1"],
        latest=True,
    )
    # Printing the catalog will list the unique values in each column of the dataframe,
    # can also call unique() to get this information.
    print(cat)
