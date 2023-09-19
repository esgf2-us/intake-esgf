import logging
import time
import warnings
from pathlib import Path
from typing import Union

import pandas as pd
import xarray as xr
from globus_sdk import SearchClient
from tqdm import tqdm

from intake_esgf.core import (
    get_dataset_pattern,
    response_to_dataframe,
    response_to_https_download,
    response_to_local_filelist,
)

warnings.simplefilter("ignore", category=xr.SerializationWarning)

# create an alias for the different indices we want to support.
GLOBUS_INDEX_IDS = {
    "anl-dev": "d927e2d9-ccdb-48e4-b05d-adbc3d97bbc5",
}

# setup logging, not sure if this belongs here but if the catalog gets used I want logs
# dumped to this location.
local_cache = Path.home() / ".esgf"
local_cache.mkdir(parents=True, exist_ok=True)
logger = logging.getLogger("intake-esgf")
log_file = local_cache / "esgf.log"
if not log_file.is_file():
    log_file.touch()
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(
    logging.Formatter(
        "\x1b[36;20m%(asctime)s \x1b[36;32m%(funcName)s()\033[0m %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
)
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)


class ESGFCatalog:
    def __init__(self, index_id="anl-dev"):
        assert index_id in GLOBUS_INDEX_IDS
        self.index_id = GLOBUS_INDEX_IDS[index_id]
        self.df = None  # dataframe which stores the results of the last call to search
        self.total_results = 0  # the total number of results from the last search
        self.esgf_data_root = None  # the path where the esgf data already exists
        self.local_cache = Path.home() / ".esgf"  # the path to the local cache

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

        Leaving `strict=False` can be useful if you are not sure what variable you need,
        but have an idea of what it is called. The follow search reveals that there are
        several choices for `variable_id` that have 'temperature' in the long name.

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
        search_time = time.time()
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
            result = SearchClient().post_search(self.index_id, query_data, limit=limit)
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
                self.index_id, query, limit=limit, advanced=True
            )
        search_time = time.time() - search_time
        self.total_results = result["total"]
        if not self.total_results:
            raise ValueError("Search returned no results.")
        process_time = time.time()
        self.df = response_to_dataframe(result, get_dataset_pattern())
        process_time = time.time() - process_time
        logger.info(
            f"{strict=}, {limit=}, {search_time=:.3f}, {process_time=:.3f}, {str(search)}"
        )
        return self

    def set_esgf_data_root(self, root: Union[str, Path]) -> None:
        """Set the root directory of the ESGF data local to your system.

        It may be that you are working on a resource that has direct access to a copy of
        the ESGF data. If you set this root, then when calling `to_dataset_dict()`, we
        will check if the requested dataset is available through direct access.

        """
        if isinstance(root, str):
            root = Path(root)
        assert root.is_dir()
        self.esgf_data_root = root

    def to_dataset_dict(self) -> dict[str, xr.Dataset]:
        if self.df is None or len(self.df) == 0:
            raise ValueError("No entries to retrieve.")
        # Prefer the esgf data root if set, otherwise check the local cache
        data_root = (
            self.esgf_data_root if self.esgf_data_root is not None else self.local_cache
        )
        ds = {}
        for _, row in tqdm(self.df.iterrows(), total=len(self.df)):
            response = SearchClient().search(
                self.index_id, f'dataset_id: "{row.globus_subject}"', advanced=True
            )
            file_list = []
            # 1) Look for direct access to files
            try:
                file_list = response_to_local_filelist(response, data_root)
            except FileNotFoundError:
                pass
            # 2) Use THREDDS links, but there are none in this index and so we will put
            #    this on the list to do.

            # 3) Use Globus for transfer? I know that we could use the sdk to
            #    authenticate but I am not clear on if we could automatically setup the
            #    current location as an endpoint.

            # 4) Use the https links to download data locally.
            if not file_list:
                file_list = response_to_https_download(response, self.local_cache)

            # Now open datasets and add to the return dictionary
            key = row.globus_subject.split("|")[0]
            if len(file_list) == 1:
                ds[key] = xr.open_dataset(file_list[0])
            elif len(file_list) > 1:
                ds[key] = xr.open_mfdataset(file_list)
            else:
                ds[key] = "Could not obtain this file."
        return ds
