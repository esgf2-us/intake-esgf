import logging
import warnings
from functools import partial
from multiprocessing.pool import ThreadPool
from pathlib import Path
from typing import Callable, Union

import pandas as pd
import xarray as xr
from datatree import DataTree
from globus_sdk import SearchAPIError
from requests import ConnectTimeout, ReadTimeout
from tqdm import tqdm

from intake_esgf.core import (
    GlobusESGFIndex,
    SolrESGFIndex,
    combine_file_info,
    combine_results,
    parallel_download,
)
from intake_esgf.util import add_cell_measures

warnings.simplefilter("ignore", category=xr.SerializationWarning)
BAR_FORMAT = "{desc:>20}: {percentage:3.0f}%|{bar}|{n_fmt}/{total_fmt} [{rate_fmt:>15s}{postfix}]"

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
    """A data catalog for searching ESGF nodes and downloading data.

    This catalog is largely experimental. We are using it to test capabilities and
    understand consequences of index design. Please feel free to use it but understand
    that the API will likely change.

    legacy_nodes
        Set to True (defaults to False) to use all ESGF1 nodes in the federation or
        specify a node or list of nodes.

    """

    _legacy_nodes = [
        "esgf.ceda.ac.uk",
        "esgf-data.dkrz.de",
        "esgf-node.ipsl.upmc.fr",
        "esg-dn1.nsc.liu.se",
        "esgf-node.llnl.gov",
        "esgf.nci.org.au",
        "esgf-node.ornl.gov",
    ]

    def __init__(
        self,
        legacy_nodes: Union[bool, str, list[str]] = False,
    ):
        self.indices = [GlobusESGFIndex()]
        if isinstance(legacy_nodes, bool) and legacy_nodes:
            self.indices += [
                SolrESGFIndex(node, distrib=False) for node in ESGFCatalog._legacy_nodes
            ]
        if isinstance(legacy_nodes, str):
            legacy_nodes = [legacy_nodes]
        if isinstance(legacy_nodes, list):
            self.indices += [
                SolrESGFIndex(node, distrib=False) for node in legacy_nodes
            ]
        self.df = None  # dataframe which stores the results of the last call to search
        self.local_cache = Path.home() / ".esgf"  # the path to the local cache

        # the path where the esgf data already exists
        self.esgf_data_root = check_for_esgf_dataroot()

    def __repr__(self):
        if self.df is None:
            return "Perform a search() to populate the catalog."
        return self.unique().__repr__()

    def clone(self):
        """Return a new instance of a catalog with the same indices and settings."""
        cat = ESGFCatalog()
        cat.indices = self.indices
        cat.esgf_data_root = self.esgf_data_root
        cat.local_cache = self.local_cache
        return cat

    def unique(self) -> pd.Series:
        """Return the the unique values in each facet of the search."""
        out = {}
        for col in self.df.drop(columns=["id", "version"]).columns:
            out[col] = self.df[col].unique()
        return pd.Series(out)

    def model_groups(self) -> pd.Series:
        """Return counts for unique combinations of (source_id,member_id,grid_label)."""
        lower = self.df.source_id.str.lower()
        lower.name = "lower"
        return (
            pd.concat(
                [
                    self.df,
                    self.df.member_id.str.extract(r"r(\d+)i(\d+)p(\d+)f(\d+)").astype(
                        int
                    ),
                    lower,
                ],
                axis=1,
            )
            .sort_values(["lower", 0, 1, 2, 3, "grid_label"])
            .drop(columns=["lower", 0, 1, 2, 3])
            .groupby(["source_id", "member_id", "grid_label"], sort=False)
            .count()["variable_id"]
        )

    def search(self, quiet: bool = False, **search: Union[str, list[str]]):
        """Populate the catalog by specifying search facets and values."""

        def _search(index):
            try:
                df = index.search(**search)
            except ValueError:
                return pd.DataFrame([])
            except (SearchAPIError, ConnectionError, ReadTimeout, ConnectTimeout):
                warnings.warn(
                    f"{index} failed to return a response, results may be incomplete"
                )
                return pd.DataFrame([])
            return df

        dfs = ThreadPool(len(self.indices)).imap_unordered(_search, self.indices)
        self.df = combine_results(
            tqdm(
                dfs,
                disable=quiet,
                bar_format=BAR_FORMAT,
                unit="index",
                unit_scale=False,
                desc="Searching indices",
                ascii=True,
                total=len(self.indices),
            )
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

    def to_dataset_dict(
        self,
        minimal_keys: bool = True,
        ignore_facets: Union[None, str, list[str]] = None,
        separator: str = ".",
        num_threads: int = 6,
        quiet: bool = False,
        add_measures: bool = True,
    ) -> dict[str, xr.Dataset]:
        """Return the current search as a dictionary of datasets.

        By default, the keys of the returned dictionary are the minimal set of facets
        required to uniquely describe the search. If you prefer to use a full set of
        facets, set `minimal_keys=False`. You can also specify

        Parameters
        ----------
        minimal_keys
            Disable to return a dictonary whose keys are formed using all facets, by
            default we use a minimal set of facets to build the simplest keys.
        ignore_facets
            When constructing the dictionary keys, which facets should we ignore?
        separator
            When generating the keys, the string to use as a seperator of facets.
        num_threads
            The number of threads to use when downloading files.
        """
        if self.df is None or len(self.df) == 0:
            raise ValueError("No entries to retrieve.")

        # The keys returned will be just the items that are different.
        output_key_format = []
        if ignore_facets is None:
            ignore_facets = []
        if isinstance(ignore_facets, str):
            ignore_facets = [ignore_facets]
        ignore_facets += [
            "version",
            "id",
        ]  # these we always ignore
        for col in self.df.drop(columns=ignore_facets):
            if minimal_keys:
                if not (self.df[col].iloc[0] == self.df[col]).all():
                    output_key_format.append(col)
            else:
                output_key_format.append(col)
        if not output_key_format:  # at minimum we have the variable id as a key
            output_key_format = ["variable_id"]

        # Query the nodes to get the file information for download
        infos = []
        for _, row in tqdm(
            self.df.iterrows(),
            disable=quiet,
            bar_format=BAR_FORMAT,
            unit="dataset",
            unit_scale=False,
            desc="Obtaining file info",
            ascii=True,
            total=len(self.df),
        ):
            # get file info from each index and then add in a unique key
            info = combine_file_info(self.indices, row.id)
            for i, _ in enumerate(info):
                info[i]["key"] = separator.join([row[k] for k in output_key_format])
            infos += info

        # Run parallel download if needed
        fetch = partial(
            parallel_download, local_cache=local_cache, esg_dataroot=self.esgf_data_root
        )
        results = ThreadPool(num_threads).imap_unordered(fetch, infos)
        ds = {}
        for key, local_file in results:
            if key in ds:
                ds[key].append(local_file)
            else:
                ds[key] = [local_file]

        # Return xarray objects
        for key, files in ds.items():
            if len(files) == 1:
                ds[key] = xr.open_dataset(files[0])
            elif len(files) > 1:
                ds[key] = xr.open_mfdataset(sorted(files))
            else:
                ds[key] = "Error in opening"

        # Attempt to add cell measures
        if add_measures:
            for key in tqdm(
                ds,
                disable=quiet,
                bar_format=BAR_FORMAT,
                unit="dataset",
                unit_scale=False,
                desc="Adding cell measures",
                ascii=True,
                total=len(ds),
            ):
                ds[key] = add_cell_measures(ds[key], self)
        return ds

    def to_datatree(
        self,
        minimal_keys: bool = True,
        ignore_facets: Union[None, str, list[str]] = None,
    ) -> DataTree:
        """Return the current search as a datatree.

        Parameters
        ----------
        minimal_keys
            Disable to return a dictonary whose keys are formed using all facets, by
            default we use a minimal set of facets to build the simplest keys.
        ignore_facets
            When constructing the dictionary keys, which facets should we ignore?

        See Also
        --------
        `to_dataset_dict`

        """
        return DataTree.from_dict(
            self.to_dataset_dict(
                minimal_keys=minimal_keys, ignore_facets=ignore_facets, separator="/"
            )
        )

    def remove_incomplete(self, complete: Callable[[pd.DataFrame], bool]):
        """Remove the incomplete search results as defined by the `complete` function.

        While the ESGF search results will return anything matching the criteria, we are
        typically interested in unique combinations of `source_id`, `member_id`, and
        `grid_label`. Many times modeling groups upload different realizations but they
        do not contain all the variables either by oversight or design. This function
        will internally group the results by these criteria and then call the
        user-provided `complete` function on the grouped dataframe and remove entries
        deemed incomplete.

        """
        for lbl, grp in self.df.groupby(["source_id", "member_id", "grid_label"]):
            if not complete(grp):
                self.df = self.df.drop(grp.index)
        return self

    def remove_ensembles(self):
        """Remove higher numeric ensembles for each `source_id`.

        Many times an ESGF search will return possible many ensembles, but you only need
        1 for your analysis, usually the smallest numeric values in the `member_id`.
        While in most cases it will simply be `r1i1p1f1`, this is not always the case.
        This function will select the *smallest* `member_id` (in terms of the smallest 4
        integer values) for each `source_id` in your search and remove all others.

        """
        for source_id, grp in self.df.groupby("source_id"):
            member_id = "r{}i{}p{}f{}".format(
                *(
                    grp.member_id.str.extract(r"r(\d+)i(\d+)p(\d+)f(\d+)")
                    .astype(int)
                    .sort_values([0, 1, 2, 3])
                    .iloc[0]
                )
            )
            self.df = self.df.drop(grp[grp.member_id != member_id].index)
        return self


def check_for_esgf_dataroot() -> Union[Path, None]:
    """Return a direct path to the ESGF data is it exists."""
    to_check = [
        "/gpfs/alpine/cli137/proj-shared/ESGF/esg_dataroot/css03_data/",  # OLCF
    ]
    for check in to_check:
        if Path(check).is_dir():
            return check
    return None
