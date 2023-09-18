import logging
import re
import warnings
from pathlib import Path
from typing import Any, Dict, List, Union

import pandas as pd
import requests
import xarray as xr
from globus_sdk import SearchClient
from globus_sdk.response import GlobusHTTPResponse
from tqdm import tqdm

# CESM2 has used multiple fill values and throws lots of ugly warnings
warnings.simplefilter("ignore", category=xr.SerializationWarning)

# create an alias for the different indices we want to support. Could easily extend to
# obs4mips or whatever else we wish to support.
GLOBUS_INDEX_IDS = {
    "anl-dev": "d927e2d9-ccdb-48e4-b05d-adbc3d97bbc5",
}

# setup logging
local_cache = Path.home() / ".esgf"
logging.basicConfig(
    filename=local_cache / "search_history.log",
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)
logger.setLevel("INFO")


def get_relative_esgf_path(entry: Dict[str, Any]) -> Path:
    """Return the relative ESGF path from the Globus entry."""
    if "content" not in entry:
        raise ValueError("'content' not part of the entry.")
    content = entry["content"]
    if set(["version", "dataset_id", "directory_format_template_"]).difference(
        content.keys()
    ):
        raise ValueError("Entry content does not contain expected keys.")
    # For some reason, the `version` in the globus response is just an integer and not
    # what is used in the file path so I have to parse it out of the `dataset_id`
    content["version"] = [content["dataset_id"].split("|")[0].split(".")[-1]]
    # Format the file path using the template in the response
    file_path = content["directory_format_template_"][0]
    file_path = Path(
        file_path.replace("%(root)s/", "")
        .replace("%(", "{")
        .replace(")s", "[0]}")
        .format(**content)
    )
    return file_path


def response_to_dataframe(response: GlobusHTTPResponse, pattern: str) -> pd.DataFrame:
    """Return the dataset entries from the response of a Globus search."""
    df = []
    for g in response["gmeta"]:
        assert len(g["entries"]) == 1  # A check on the assumption of a single entry
        # Manually remove entries which are not datasets. With the way I am using
        # search, this is not necesary but if a file type gets through the regular
        # expression pattern will break and pollute the dataframe.
        if g["entries"][0]["entry_id"] != "dataset":
            continue
        m = re.search(pattern, g["subject"])
        if m:
            df.append(m.groupdict())
            # Also include the globus 'subject' so we can find files later when we are
            # ready to download.
            df[-1]["globus_subject"] = g["subject"]
    df = pd.DataFrame(df)
    return df


def response_to_local_filelist(
    response: GlobusHTTPResponse, data_root: Union[str, Path]
) -> List[Path]:
    """Return a list of full paths to netCDF files from the Globus response.

    This function uses the `directory_format_template_` and values in the response to
    form a relative location of the files represented in the Globus response. We then
    prepend the given data route to make the path absolute and check for existence.

    If we had something beyond https logs to track file access, at this point we could
    iterate some counter that the dataset had been accessed. If a problem, we could use
    the https link and start to download a chunk of data and then break it just so there
    is evidence of use.

    """
    if data_root is None:
        return []
    if isinstance(data_root, str):
        data_root = Path(data_root)
    if not data_root.is_dir():
        raise FileNotFoundError(f"Directory {data_root} does not exist.")
    paths = []
    for g in response["gmeta"]:
        assert len(g["entries"]) == 1
        entry = g["entries"][0]
        if entry["entry_id"] != "file":
            continue
        file_path = data_root / get_relative_esgf_path(entry)
        if not file_path.is_dir():
            raise FileNotFoundError(f"Directory {file_path} does not exist.")
        for file_name in file_path.glob("*.nc"):
            paths.append(file_name)
    return paths


def response_to_https_download(response: GlobusHTTPResponse) -> List[Path]:
    """"""
    paths = []
    for g in response["gmeta"]:
        assert len(g["entries"]) == 1
        entry = g["entries"][0]
        if entry["entry_id"] != "file":
            continue
        entry = entry["content"]
        url = [u for u in entry["url"] if u.endswith("|HTTPServer")]
        if len(url) != 1:
            continue
        url = url[0].replace("|HTTPServer", "")
        local_file = Path(Path(url).name)
        if not local_file.is_file():
            resp = requests.get(url, stream=True)
            with open(local_file, "wb") as fdl:
                for chunk in resp.iter_content(chunk_size=1024):
                    if chunk:
                        fdl.write(chunk)
        paths.append(local_file)
    return paths


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
        self.data_root = None

    def __repr__(self):
        if self.df is None:
            return "Perform a search() to populate the catalog."
        repr = ""
        # The globus search returns the first 'page' of the total results. This may be
        # very many if only a few facets were specified.
        if len(self.df) != self.total_results:
            repr = f"Displaying summary info for {len(self.df)} out of {self.total_results} results:\n"  # noqa: E501
        return repr + self.unique().__repr__()

    def unique(self) -> pd.Series:
        """Return the the unique values in each facet of the search."""
        out = {}
        for col in self.df.columns:
            if col in ["globus_subject", "version", "data_node"]:
                continue
            out[col] = self.df[col].unique()
        return pd.Series(out)

    def search(
        self, strict: bool = False, limit: int = 1000, **search: Union[str, list[str]]
    ):
        """Populate the catalog by specifying search facets and values.

        Keyword values may be strings or lists of strings. Keyword arguments can be the
        familiar facets (`experiment_id`, `source_id`, etc.) but are not limited to
        this. You may specify anything included in the dataset metadata including
        `cf_standard_name`, `variable_units`, and `variable_long_name`. Multiple calls
        to this function are not cumulative. Each call will overwrite the dataframe.

        Parameters
        ----------
        strict
            Enable to match search values exactly.
        limit
            The number of results to parse from the response.
        search
            Further keyword arguments which specify

        Examples
        --------
        >>> cat = ESGFCatalog()
        >>> cat.search(
                experiment_id=["historical","ssp585"],
                source_id="CESM2",
                variable_id="tas",
                table_id="Amon",
            )
        mip_era                                                     [CMIP6]
        activity_id                      [ScenarioMIP, C4MIP, ISMIP6, CMIP]
        institution_id                                               [NCAR]
        source_id          [CESM2, CESM2-WACCM, CESM2-FV2, CESM2-WACCM-FV2]
        experiment_id     [ssp585, esm-ssp585, ssp585-withism, historica...
        member_id         [r11i1p1f1, r10i1p1f1, r4i1p1f1, r5i1p1f1, r3i...
        table_id                                                     [Amon]
        variable_id                                                   [tas]
        grid_label                                                     [gn]
        dtype: object

        Notice that the result contains variants of `CESM2` and `ssp585`. This is
        because by default the search is permissive to allow for flexible queries in
        case the user is not sure for what they are searching. Repeating the search with
        `strict=True` will remove the variants.

        >>> cat.search(
                strict=True,
                experiment_id=["historical","ssp585"],
                source_id="CESM2",
                variable_id="tas",
                table_id="Amon",
            )
        mip_era                                                     [CMIP6]
        activity_id                                     [CMIP, ScenarioMIP]
        institution_id                                               [NCAR]
        source_id                                                   [CESM2]
        experiment_id                                  [historical, ssp585]
        member_id         [r9i1p1f1, r1i1p1f1, r5i1p1f1, r8i1p1f1, r11i1...
        table_id                                                     [Amon]
        variable_id                                                   [tas]
        grid_label
        dtype: object

        This can be useful if you are not sure what variable you need, but have an idea
        of what it is called. The follow search reveals that there are several choices
        for `variable_id` (ta, ts, ta500, ta850, ta700) that have 'temperature' in the
        long name.

        >>> cat.search(variable_long_name='temperature')
        Displaying summary info for 1000 out of 480459 results:
        mip_era                                                     [CMIP6]
        activity_id       [ScenarioMIP, RFMIP, DCPP, DAMIP, CMIP, HighRe...
        institution_id    [MPI-M, DKRZ, MOHC, CSIRO, CMCC, CCCma, CNRM-C...
        source_id         [MPI-ESM1-2-LR, UKESM1-0-LL, ACCESS-ESM1-5, Ha...
        experiment_id     [ssp370, ssp119, ssp245, ssp126, ssp585, ssp43...
        member_id         [r10i1p1f1, r3i1p1f1, r9i1p1f1, r8i1p1f1, r2i1...
        table_id          [CFmon, Emon, AERmonZ, Eday, Amon, 6hrPlevPt, ...
        variable_id                           [ta, ts, ta500, ta850, ta700]
        grid_label                             [gn, gnz, gr, gr1, grz, gr2]
        dtype: object

        """
        search["type"] = "Dataset"  # only search for datasets, not files
        if "latest" not in search:  # by default only find the latest
            search["latest"] = True
        # convert booleans to strings
        for key, val in search.items():
            if isinstance(val, bool):
                search[key] = str(val)
        if strict:
            query_data = {
                "q": "",
                "filters": [
                    {
                        "type": "match_any",
                        "field_name": key,
                        "values": [val] if isinstance(val, str) else val,
                    }
                    for key, val in search.items()
                ],
                "facets": [],
                "sort": [],
            }
            result = SearchClient().post_search(self.index_id, query_data, limit=1000)
        else:
            query = " AND ".join(
                [
                    f'({key}: "{val}")'
                    if isinstance(val, str)
                    else "(" + " OR ".join([f'({key}: "{v}")' for v in val]) + ")"
                    for key, val in search.items()
                ]
            )
            result = SearchClient().search(
                self.index_id, query, limit=1000, advanced=True
            )
        self.total_results = result["total"]
        if not self.total_results:
            raise ValueError("Search returned no results.")
        self.df = response_to_dataframe(result, get_dataset_pattern())
        # Ultimately here I think we want to remove the notion of where the data is
        # located and just present the user with what data is available. When it comes
        # time to download then we can implement some logic about where/how the data is
        # downloaded/accessed that is best for the user. This could be based on ping or
        # some other latency measure, but also could involve a system check to see if
        # the user has direct access or some globus endpoint specified.
        return self

    def set_data_root(self, root: Union[str, Path]) -> None:
        """Set the root directory of the ESGF data local to your system.

        It may be that you are working on a resource that has direct access to a copy of
        the ESGF data. If you set this root, then when calling `to_dataset_dict()`, we
        will check if the requested dataset is available through direct access.

        """
        # This function could also require you to specify the node to which you are
        # locally connected. This is because if you query the index and find a dataset,
        # it does not mean that this dataset exists on your local copy. For now, we will
        # just check if the file exists and revert to other transfer means if it does
        # not.
        if isinstance(root, str):
            root = Path(root)
        assert root.is_dir()
        self.data_root = root

    def to_dataset_dict(self) -> Dict[str, xr.Dataset]:
        if self.df is None or len(self.df) == 0:
            raise ValueError("No entries to retrieve.")
        ds = {}
        for _, row in tqdm(self.df.iterrows(), total=len(self.df)):
            response = SearchClient().search(
                self.index_id, f'dataset_id: "{row.globus_subject}"', advanced=True
            )
            file_list = []
            # 1) Look for direct access to files
            try:
                file_list = response_to_local_filelist(response, self.data_root)
            except FileNotFoundError:
                pass
            # 2) Use THREDDS links, but there are none in this index and so we will put
            #    this on the list to do.

            # 3) Use Globus for transfer? I know that we could use the sdk to
            #    authenticate but I am not clear on if we could automatically setup the
            #    current location as an endpoint.

            # 4) Use the https links to download data locally.
            if not file_list:
                try:
                    file_list = response_to_https_download(response)
                except OSError:
                    pass

            # Now open datasets and add to the return dictionary
            key = row.globus_subject.split("|")[0]
            if len(file_list) == 1:
                ds[key] = xr.open_dataset(file_list[0])
            elif len(file_list) > 1:
                ds[key] = xr.open_mfdataset(file_list)
        return ds


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
    )

    # Printing the catalog will list the unique values in each column of the dataframe,
    # can also call unique() to get this information.
    print(cat)

    # Our version of `to_dataset_dict()` should try to do the best thing for the user
    # automatically. If you are running on a resource that has direct access to data,
    # then you can set the root with this function and then internally we will prefer
    # this direct access.
    cat.set_data_root("/home/nate/data/ILAMB/MODELS")  # I put sftlf in here for a test
    ds = cat.to_dataset_dict()
