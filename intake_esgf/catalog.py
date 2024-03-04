"""The primary object in intake-esgf."""

import re
import time
import warnings
from functools import partial
from multiprocessing.pool import ThreadPool
from pathlib import Path
from typing import Any, Callable, Literal, Union

import pandas as pd
import requests
import xarray as xr
from datatree import DataTree
from numpy import argmax

from intake_esgf import IN_NOTEBOOK
from intake_esgf.base import (
    add_cell_measures,
    bar_format,
    check_for_esgf_dataroot,
    combine_results,
    get_facet_by_type,
    parallel_download,
)
from intake_esgf.core import GlobusESGFIndex, SolrESGFIndex
from intake_esgf.database import create_download_database, get_download_rate_dataframe
from intake_esgf.exceptions import NoSearchResults
from intake_esgf.logging import setup_logging

if IN_NOTEBOOK:
    from tqdm import tqdm_notebook as tqdm
else:
    from tqdm import tqdm


class ESGFCatalog:
    """A data catalog for searching ESGF nodes and downloading data.

    This catalog is largely experimental. We are using it to test capabilities and
    understand consequences of index design. Please feel free to use it but understand
    that the API will likely change.

    Parameters
    ----------
    esgf1_indices
        Set to True (defaults to False) to use all ESGF1 nodes in the federation or
        specify a node or list of nodes.

    Attributes
    ----------
    indices : list[Union[SolrESGFIndex, GlobusESGFIndex]]
        A list of indices to search, implementations are in `intake_esgf.core`. The test
        Globus index `anl-dev` is default and always included.
    df : pd.DataFrame
        A pandas dataframe into which the results from the search are parsed. Once you
        are satisfied with the datasets listed in this dataframe, calling
        `to_dataset_dict()` will then requery the indices to obtain file information and
        then download files in parallel.
    local_cache : pathlib.Path
        The location to which this package will download files if necessary, mirroring
        the directory structure of the ESGF node.
    esgf_data_root : Union[pathlib.Path, None]
        The location where a portion of the ESGF holdings are available locally. This
        allows for the same search interface to be used in a Jupyter notebook. Once the
        file information is obtained for the datasets in a search, we check this
        location for existence before downloading the files to `local_cache`.
    session_time : pd.Timestamp
        The time that the class instance was initialized. Used in `session_log()` to
        parse out only the portion of the log used in this session.
    """

    _esgf1_indices = [
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
        esgf1_indices: Union[bool, str, list[str]] = False,
    ):
        self.indices = [GlobusESGFIndex("anl-dev"), GlobusESGFIndex("ornl-dev")]
        if isinstance(esgf1_indices, bool) and esgf1_indices:
            self.indices += [
                SolrESGFIndex(node, distrib=False)
                for node in ESGFCatalog._esgf1_indices
            ]
        elif isinstance(esgf1_indices, list):
            self.indices += [
                SolrESGFIndex(node, distrib=False) for node in esgf1_indices
            ]
        elif isinstance(esgf1_indices, str):
            self.indices.append(SolrESGFIndex(esgf1_indices, distrib=False))
        self.df = None
        self.local_cache = None
        self.set_local_cache_directory()
        self.esgf_data_root = check_for_esgf_dataroot()
        if self.esgf_data_root is not None:
            self.logger.info(f"ESGF dataroot set {self.esgf_data_root}")
        self.session_time = pd.Timestamp.now()
        self.last_search = {}

    def __repr__(self):
        """Return the unique facets and values from the search."""
        if self.df is None:
            return "Perform a search() to populate the catalog."
        repr = f"Summary information for {len(self.df)} results:\n"
        return repr + self.unique().__repr__()

    def set_local_cache_directory(self, path: Union[str, Path, None] = None):
        """Set the local cache directory and create it if it does not exist."""
        if path is None:
            path = Path.home() / ".esgf"
        if not isinstance(path, Path):
            path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        # a change to the local cache means a change to logging
        self.local_cache = path
        self.logger = setup_logging(self.local_cache)
        for index in self.indices:
            index.logger = self.logger
        # initialize the local database
        self.download_db = path / "download.db"
        if not self.download_db.is_file():
            create_download_database(self.download_db)

    def clone(self):
        """Return a new instance of a catalog with the same indices and settings.

        This is used internally for when we want to find cell measures relating to the
        previous search but not overwrite the results.

        """
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

        def _extract_int_pattern(sample: str) -> str:
            ints = re.findall(r"\d+", sample)
            match = re.match("".join([rf"(\S+){i}" for i in ints]), sample)
            if not match:
                raise ValueError("Failed to find the pattern")
            return "".join([rf"{s}(\d+)" for s in match.groups()])

        # sort by the lower-case version of the 'model' name
        model_facet = get_facet_by_type(self.df, "model")
        lower = self.df[model_facet].str.lower()
        lower.name = "lower"

        # sort the variants but extract out the integer values, assume the first result
        # is representative of the whole
        variant_facet = get_facet_by_type(self.df, "variant")
        int_pattern = _extract_int_pattern(self.df.iloc[0][variant_facet])

        # add in these new data to a temporary dataframe
        df = pd.concat(
            [
                self.df,
                lower,
                self.df[variant_facet].str.extract(int_pattern).astype(int),
            ],
            axis=1,
        )

        # what columns will we sort/drop/groupby
        added_columns = list(df.columns[len(self.df.columns) :])
        sort_columns = list(df.columns[len(self.df.columns) :])
        group_columns = [model_facet, variant_facet]
        try:
            grid_facet = get_facet_by_type(self.df, "grid")
            sort_columns.append(grid_facet)
            group_columns.append(grid_facet)
        except ValueError:
            pass

        return (
            df.sort_values(sort_columns)
            .drop(columns=added_columns)
            .groupby(group_columns, sort=False)
            .count()
            .iloc[:, 0]
        )

    def search(self, quiet: bool = False, **search: Union[str, list[str]]):
        """Populate the catalog by specifying search facets and values.

        Parameters
        ----------
        quiet
            Enable to silence the progress bar.
        search
            Any number of facet keywords and values.

        """

        def _search(index):
            try:
                df = index.search(**search)
            except NoSearchResults:
                return pd.DataFrame([])
            except requests.exceptions.RequestException:
                self.logger.info(f"└─{index} \x1b[91;20mno response\033[0m")
                warnings.warn(
                    f"{index} failed to return a response, results may be incomplete"
                )
                return pd.DataFrame([])
            return df

        # drop empty search fields
        search = {
            k: v
            for k, v in search.items()
            if (isinstance(v, str) and len(v) > 0) or not isinstance(v, str)
        }

        # apply defaults
        search.update(
            dict(
                type="Dataset",
                project=search["project"] if "project" in search else "CMIP6",
                latest=search["latest"] if "latest" in search else True,
                retracted=search["retracted"] if "retracted" in search else False,
            )
        )
        if isinstance(search["project"], list):
            if len(search["project"]) > 1:
                raise ValueError("For now, projects may only be searched one at a time")

        # log what is being searched for
        search_str = ", ".join(
            [
                f"{key}={val if isinstance(val,list) else [val]}"
                for key, val in search.items()
            ]
        )
        self.logger.info(f"\x1b[36;32msearch begin\033[0m {search_str}")

        # threaded search over indices
        search_time = time.time()
        dfs = ThreadPool(len(self.indices)).imap_unordered(_search, self.indices)
        self.df = combine_results(
            tqdm(
                dfs,
                disable=quiet,
                bar_format=bar_format,
                unit="index",
                unit_scale=False,
                desc="Searching indices",
                ascii=False,
                total=len(self.indices),
            )
        )

        # even though we are using latest=True, because the search is distributed, we
        # may have different versions.
        for r, row in self.df.iterrows():
            latest = max([x.split("|")[0].split(".")[-1] for x in row.id])
            self.df.loc[r, "id"] = [x for x in row.id if latest in x]

        search_time = time.time() - search_time
        self.logger.info(f"\x1b[36;32msearch end\033[0m total_time={search_time:.2f}")
        self.last_search = search
        return self

    def from_tracking_ids(
        self, tracking_ids: Union[str, list[str]], quiet: bool = False
    ):
        """Populate the catalog by speciying tracking ids.

        While tracking_ids should uniquely define individual files, we observe that some
        centers erronsouly reuse ids on multiple files. For this reason, you may find
        that more dataset records are returned that tracking_ids you specify.

        Parameters
        ----------
        tracking_ids
            The ids whose datasets will form the items in the catalog.
        quiet
            Enable to silence the progress bar.

        """

        def _from_tracking_ids(index):
            try:
                df = index.from_tracking_ids(tracking_ids)
            except NoSearchResults:
                return pd.DataFrame([])
            except requests.exceptions.RequestException:
                self.logger.info(f"└─{index} \x1b[91;20mno response\033[0m")
                warnings.warn(
                    f"{index} failed to return a response, results may be incomplete"
                )
                return pd.DataFrame([])
            return df

        if isinstance(tracking_ids, str):
            tracking_ids = [tracking_ids]

        # log what is being searched for
        self.logger.info("\x1b[36;32mfrom_tracking_ids begin\033[0m")

        # threaded search over indices
        search_time = time.time()
        dfs = ThreadPool(len(self.indices)).imap_unordered(
            _from_tracking_ids, self.indices
        )
        self.df = combine_results(
            tqdm(
                dfs,
                disable=quiet,
                bar_format=bar_format,
                unit="index",
                unit_scale=False,
                desc="Searching indices",
                ascii=False,
                total=len(self.indices),
            )
        )
        search_time = time.time() - search_time
        if len(self.df) != len(tracking_ids):
            self.logger.info(
                "One or more of the tracking_ids resolve to multiple files."
            )
        self.logger.info(
            f"\x1b[36;32mfrom_tracking_ids end\033[0m total_time={search_time:.2f}"
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
        operators: list[Any] = [],
    ) -> dict[str, xr.Dataset]:
        """Return the current search as a dictionary of datasets.

        By default, the keys of the returned dictionary are the minimal set of facets
        required to uniquely describe the search. If you prefer to use a full set of
        facets, set `minimal_keys=False`.

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

        # The keys of the returned dictionary should only consist of the facets that are
        # different.
        output_key_format = []
        if ignore_facets is None:
            ignore_facets = []
        if isinstance(ignore_facets, str):
            ignore_facets = [ignore_facets]
        # ...but these we always ignore
        ignore_facets += [
            "version",
            "id",
        ]
        for col in self.df.drop(columns=ignore_facets):
            if minimal_keys:
                if not (self.df[col].iloc[0] == self.df[col]).all():
                    output_key_format.append(col)
            else:
                output_key_format.append(col)
        if not output_key_format:  # at minimum we have the variable id as a key
            output_key_format = [get_facet_by_type(self.df, "variable")]

        # Populate a dictionary of dataset_ids in this search and which keys they will
        # map to in the output dictionary. This is complicated by CMIP5 where the
        # dataset_id -> variable mapping is not unique.
        dataset_ids = {}
        for _, row in self.df.iterrows():
            key = separator.join([row[k] for k in output_key_format])
            for dataset_id in row["id"]:
                if dataset_id in dataset_ids:
                    if isinstance(dataset_ids[dataset_id], str):
                        dataset_ids[dataset_id] = [dataset_ids[dataset_id]]
                    dataset_ids[dataset_id].append(key)
                else:
                    dataset_ids[dataset_id] = key

        # Some projects use dataset_ids to refer to collections of variables. So we need
        # to pass the variables to the file info search to make sure we do not get more
        # than we want.
        search_facets = {}
        variable_facet = get_facet_by_type(self.df, "variable")
        if variable_facet in self.last_search:
            search_facets[variable_facet] = self.last_search[variable_facet]

        def _get_file_info(index, dataset_ids, **search_facets):
            try:
                info = index.get_file_info(list(dataset_ids.keys()), **search_facets)
            except NoSearchResults:
                return []
            except requests.exceptions.RequestException:
                self.logger.info(f"└─{index} \x1b[91;20mno response\033[0m")
                warnings.warn(
                    f"{index} failed to return a response, info may be incomplete"
                )
                return []
            return info

        self.logger.info("\x1b[36;32mfile info begin\033[0m")

        # threaded file info over indices and flatten output
        info_time = time.time()
        get_file_info = ThreadPool(len(self.indices)).imap_unordered(
            partial(_get_file_info, dataset_ids=dataset_ids, **search_facets),
            self.indices,
        )
        index_infos = list(
            tqdm(
                get_file_info,
                disable=quiet,
                bar_format=bar_format,
                unit="index",
                unit_scale=False,
                desc="Get file information",
                ascii=False,
                total=len(self.indices),
            )
        )
        index_infos = [info for index_info in index_infos for info in index_info]

        # now we merge this info together.
        def _which_key(path, keys):
            """Return the key that is likely correct based on counts in the path."""
            counts = []
            for key in keys:
                counts.append(sum([str(path).count(k) for k in key.split(separator)]))
            return keys[argmax(counts)]

        merged_info = {}
        for info in index_infos:
            path = info["path"]
            if path not in merged_info:
                # Because CMIP5 is annoying, we may have multiple dataset_ids for each
                # path (file). So here we choose which key is more likely correct.
                if isinstance(dataset_ids[info["dataset_id"]], list):
                    merged_info[path] = {
                        "key": _which_key(path, dataset_ids[info["dataset_id"]])
                    }
                else:
                    merged_info[path] = {"key": dataset_ids[info["dataset_id"]]}

            for key, val in info.items():
                if isinstance(val, list):
                    if key not in merged_info[path]:
                        merged_info[path][key] = val
                    else:
                        merged_info[path][key] += val
                else:
                    if key not in merged_info[path]:
                        merged_info[path][key] = val
        infos = [info for _, info in merged_info.items()]
        info_time = time.time() - info_time
        self.logger.info(f"\x1b[36;32mfile info end\033[0m total_time={info_time:.2f}")

        # Download in parallel using threads
        fetch = partial(
            parallel_download,
            local_cache=self.local_cache,
            download_db=self.download_db,
            esg_dataroot=self.esgf_data_root,
        )
        results = ThreadPool(min(num_threads, len(infos))).imap_unordered(fetch, infos)
        ds = {}
        for key, local_file in results:
            if local_file is None:  # there was a problem getting this file
                continue
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

        # Attempt to add cell measures (serial)
        if add_measures:
            for key in tqdm(
                ds,
                disable=quiet,
                bar_format=bar_format,
                unit="dataset",
                unit_scale=False,
                desc="Adding cell measures",
                ascii=False,
                total=len(ds),
            ):
                ds[key] = add_cell_measures(ds[key], self)

        # If the user specifies operators, apply them now
        for op in operators:
            ds = op(ds)

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
        group = [
            get_facet_by_type(self.df, "model"),
            get_facet_by_type(self.df, "variant"),
        ]
        try:
            group.append(get_facet_by_type(self.df, "grid"))
        except ValueError:
            pass
        for _, grp in self.df.groupby(group):
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
        df = self.model_groups()
        variant_facet = get_facet_by_type(self.df, "variant")
        names = [name for name in df.index.names if name != variant_facet]
        for lbl, grp in df.to_frame().reset_index().groupby(names):
            variant = grp.iloc[0][variant_facet]
            q = " & ".join([f"({n} == '{v}')" for n, v in zip(names, lbl)])
            q += f" & ({variant_facet} != '{variant}')"
            self.df = self.df.drop(self.df.query(q).index)
        return self

    def session_log(self) -> str:
        """Return the log since the instantiation of `ESGFCatalog()`."""
        log = open(self.local_cache / "esgf.log").readlines()[::-1]
        for n, line in enumerate(log):
            m = re.search(r"\x1b\[36;20m(.*)\s\033\[0m", line)
            if not m:
                continue
            if pd.to_datetime(m.group(1)) < (
                self.session_time - pd.Timedelta(1, "s")  # little pad
            ):
                break
        return "".join(log[:n][::-1])

    def download_summary(
        self,
        history: Literal[None, "day", "week", "month"] = None,
        minimum_size: float = 0,
    ) -> pd.DataFrame:
        """Return the per host download summary statistics as a dataframe.

        Parameters
        ----------
        history
            How much download history should we use in computing rates. Leave `None` to
            use the entire history.
        minimum_size
            The minimum size in Mb to include in the reported record.

        """
        df = get_download_rate_dataframe(
            self.download_db, history=history, minimum_size=minimum_size
        )
        df = df.sort_values("rate", ascending=False)
        df = df.rename(
            columns=dict(
                transfer_time="transfer_time [s]",
                transfer_size="transfer_size [Mb]",
                rate="rate [Mb s-1]",
            )
        )
        return df
