"""The primary object in intake-esgf."""

import re
import sys
import time
import warnings
from collections.abc import Callable
from functools import partial
from multiprocessing.pool import ThreadPool
from pathlib import Path

# Self isn't available in typing in 3.10
if sys.version_info[1] == 10:
    from typing import Literal

    from typing_extensions import Self
else:
    from typing import Literal, Self

import pandas as pd
import requests
import xarray as xr

import intake_esgf
import intake_esgf.base as base
from intake_esgf import IN_NOTEBOOK
from intake_esgf.core import GlobusESGFIndex, SolrESGFIndex
from intake_esgf.core.globus import (
    create_globus_transfer,
    monitor_globus_transfer,
    variable_info,
)
from intake_esgf.database import (
    create_download_database,
    get_download_rate_dataframe,
)
from intake_esgf.exceptions import (
    DatasetInitError,
    LocalCacheNotWritable,
    MissingFileInformation,
    NoSearchResults,
)
from intake_esgf.projects import projects as esgf_projects

if IN_NOTEBOOK:
    from tqdm import tqdm_notebook as tqdm
else:
    from tqdm import tqdm


class ESGFCatalog:
    """
    A data catalog for searching ESGF nodes and downloading data.

    Attributes
    ----------
    indices : list[SolrESGFIndex | GlobusESGFIndex]
        A list of indices to search, implementations are in `intake_esgf.core`.
    df : pd.DataFrame
        A pandas dataframe into which the results from the search are parsed. You may
        manipulate this dataframe by removing rows or adding columns.
    project : ESGFProject
        A class which provides facet usage in the current project.
    session_time : pd.Timestamp
        The time that the class instance was initialized.
    last_search: dict
        The keywords and values used in the previous call to `search()`.
    local_cache: list[Path]
        The local caches to which data will be read/downloaded.
    esg_dataroot: list[Path]
        A lits of locations from which data is loaded.
    download_db: Path
        The path to a database into which download hosts, sizes, and transfer times are recored.
    """

    def __init__(self):
        self.indices = []
        self.indices += [
            GlobusESGFIndex(ind)
            for ind in intake_esgf.conf["globus_indices"]
            if intake_esgf.conf["globus_indices"][ind]
        ]
        self.indices += [
            SolrESGFIndex(ind)
            for ind in intake_esgf.conf["solr_indices"]
            if intake_esgf.conf["solr_indices"][ind]
        ]
        if not self.indices:
            raise ValueError("You must have at least 1 search index configured")
        self.df = None
        self.project = None
        self.session_time = pd.Timestamp.now()
        self.last_search = {}
        self.local_cache = []
        self.esg_dataroot = []
        self.download_db = None
        self._initialize()

    def __repr__(self) -> str:
        """
        Return the unique facets and values from the search.
        """
        if self.df is None:
            return "Perform a search() to populate the catalog."
        repr = f"Summary information for {len(self.df)} results:\n"
        return repr + self.unique().__repr__()

    def _initialize(self) -> None:
        """
        Ensure that directories and pertinent files are created.
        """

        def is_writable(dir: Path) -> bool:
            test = dir / "tmp.txt"
            try:
                test.touch()
                test.unlink()
                return True
            except Exception:
                pass
            return False

        # ensure the local_cache directories exist, we will use the first one that is
        # writeable
        for path in intake_esgf.conf["local_cache"]:
            path = Path(path).expanduser()
            try:
                path.mkdir(parents=True, exist_ok=True)
            except Exception:
                continue
            if not path.is_dir():
                continue
            if is_writable(path):
                self.local_cache.append(path)
        if not self.local_cache:
            raise LocalCacheNotWritable(intake_esgf.conf["local_cache"])

        # check which esg_dataroot's exist and store with catalog
        for path in intake_esgf.conf["esg_dataroot"]:
            path = Path(path).expanduser()
            if path.is_dir():
                self.esg_dataroot.append(path)

        # initialize the local database
        download_db = Path(intake_esgf.conf["download_db"]).expanduser()
        download_db.parent.mkdir(parents=True, exist_ok=True)
        if not download_db.is_file():
            create_download_database(download_db)
        self.download_db = download_db

    def _set_project(self) -> None:
        """
        Parse the project from the last search.
        """
        if self.df is None:
            raise ValueError("Perform a search() to populate the dataframe.")
        project = self.df["project"].unique()
        if len(project) != 1:
            raise ValueError("Only single project searches are supported")
        self.project = esgf_projects[project[0].lower()]

    def _minimal_key_format(
        self, ignore_facets: list[str] | str | None = None
    ) -> list[str]:
        """
        Return the facets which have different values in the current catalog.

        Parameters
        ----------
        ignore_facets: list[str] or str, optional
            The facets you wish to ignore. This is useful if, for example, you have
            variables from different tables, but do not wish the `table_id` to be part of
            the keys.

        Returns
        -------
        list[str]
        """
        if ignore_facets is None:
            ignore_facets = []
        if isinstance(ignore_facets, str):
            ignore_facets = [ignore_facets]
        output_key_format = [
            col
            for col in self.project.master_id_facets()
            if (
                (self.df[col].iloc[0] != self.df[col]).any()
                and col not in ignore_facets
            )
        ]
        if not output_key_format:
            output_key_format = [self.project.variable_facet()]
        return output_key_format

    def clone(self) -> "ESGFCatalog":
        """
        Return a new instance of a catalog with the same indices and settings.

        This is used internally for when we want to find cell measures relating to the
        previous search but not overwrite the results.
        """
        cat = ESGFCatalog()
        cat.indices = self.indices
        cat.local_cache = self.local_cache
        cat.esg_dataroot = self.esg_dataroot
        return cat

    def unique(self) -> pd.Series:
        """
        Return the the unique values in each facet of the search.
        """
        if self.df is None:
            raise ValueError("Perform a search() to populate the catalog.")
        out = {}
        drops = [
            col
            for col in self.df.columns
            if col
            not in (
                self.project.master_id_facets()
                + intake_esgf.conf.get("additional_df_cols")
            )
        ]
        for col in self.df.drop(columns=drops).columns:
            out[col] = self.df[col].unique()
        return pd.Series(out)

    def model_groups(self) -> pd.Series:
        """
        Return counts for unique combinations of (source_id,member_id,grid_label).
        """

        def _extract_int_pattern(sample: str) -> str:
            ints = re.findall(r"\d+", sample)
            match = re.match("".join([rf"(\S+){i}" for i in ints]), sample)
            if not match:
                raise ValueError("Failed to find the pattern")
            return "".join([rf"{s}(\d+)" for s in match.groups()])

        # sort by the lower-case version of the 'model' name
        model_facet = self.project.model_facet()
        lower = self.df[model_facet].str.lower()
        lower.name = "lower"

        # sort the variants but extract out the integer values, assume the first result
        # is representative of the whole
        variant_facet = self.project.variant_facet()
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

        grid_facet = self.project.grid_facet()
        if grid_facet is not None:
            sort_columns.append(grid_facet)
            group_columns.append(grid_facet)

        return (
            df.sort_values(sort_columns)
            .drop(columns=added_columns)
            .groupby(group_columns, sort=False)
            .count()
            .iloc[:, 0]
        )

    def search(self, quiet: bool = False, **search) -> Self:
        """
        Populate the catalog by specifying search facets and values.

        Parameters
        ----------
        quiet
            Enable to silence the progress bar.
        **search
            Any number of facet keywords and values.
        """
        logger = intake_esgf.conf.get_logger()

        def _search(index):
            try:
                df = index.search(**search)
            except NoSearchResults:
                return pd.DataFrame([])
            except requests.exceptions.RequestException:
                logger.info(f"└─{index} \x1b[91;20mno response\033[0m")
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
        logger.info(f"\x1b[36;32msearch begin\033[0m {search_str}")

        # threaded search over indices
        search_time = time.time()
        dfs = ThreadPool(len(self.indices)).imap_unordered(_search, self.indices)
        self.df = base.combine_results(
            tqdm(
                dfs,
                disable=quiet,
                bar_format=base.bar_format,
                unit="index",
                unit_scale=False,
                desc="Searching indices",
                ascii=False,
                total=len(self.indices),
            )
        )
        self._set_project()

        # even though we are using latest=True, because the search is distributed, we
        # may have different versions from different indices.
        for r, row in self.df.iterrows():
            latest = max([x.split("|")[0].split(".")[-1] for x in row.id])
            self.df.loc[r, "id"] = [x for x in row.id if latest in x]

        search_time = time.time() - search_time
        logger.info(f"\x1b[36;32msearch end\033[0m total_time={search_time:.2f}")
        self.last_search = search
        return self

    def from_tracking_ids(
        self, tracking_ids: str | list[str], quiet: bool = False
    ) -> Self:
        """
        Populate the catalog by speciying tracking ids.

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
        logger = intake_esgf.conf.get_logger()

        def _from_tracking_ids(index):
            try:
                df = index.from_tracking_ids(tracking_ids)
            except NoSearchResults:
                return pd.DataFrame([])
            except requests.exceptions.RequestException:
                logger.info(f"└─{index} \x1b[91;20mno response\033[0m")
                warnings.warn(
                    f"{index} failed to return a response, results may be incomplete"
                )
                return pd.DataFrame([])
            return df

        if isinstance(tracking_ids, str):
            tracking_ids = [tracking_ids]

        # log what is being searched for
        logger.info("\x1b[36;32mfrom_tracking_ids begin\033[0m")

        # threaded search over indices
        search_time = time.time()
        dfs = ThreadPool(len(self.indices)).imap_unordered(
            _from_tracking_ids, self.indices
        )
        self.df = base.combine_results(
            tqdm(
                dfs,
                disable=quiet,
                bar_format=base.bar_format,
                unit="index",
                unit_scale=False,
                desc="Searching indices",
                ascii=False,
                total=len(self.indices),
            )
        )
        search_time = time.time() - search_time
        if len(self.df) != len(tracking_ids):
            logger.info("One or more of the tracking_ids resolve to multiple files.")
        logger.info(
            f"\x1b[36;32mfrom_tracking_ids end\033[0m total_time={search_time:.2f}"
        )
        self._set_project()

        return self

    def _get_file_info(self, separator: str = ".", quiet: bool = False) -> list[dict]:
        """
        Query and return file information datasets present in the catalog.

        Parameters
        ----------
        separator: str
            The character to use in between facets in the key values.
        quiet: bool
            Enable to silence the progress bar.

        Returns
        -------
        list[dict]
            The file info combined from all sources.
        """

        def _get_file_info(index, dataset_ids, **search_facets):
            try:
                info = index.get_file_info(list(dataset_ids.keys()), **search_facets)
            except NoSearchResults:
                return []
            except requests.exceptions.RequestException:
                logger.info(f"└─{index} \x1b[91;20mno response\033[0m")
                warnings.warn(
                    f"{index} failed to return a response, info may be incomplete"
                )
                return []
            return info

        logger = intake_esgf.conf.get_logger()
        logger.info("\x1b[36;32mfile info begin\033[0m")

        # Eventually we will return a dictionary of paths/datasets. The keys of this
        # dictionary we will initialize to the master_id facets joined together by the
        # `separator` and placed in a new column in the dataframe called `key`
        if self.project is None:
            self._set_project()
        self.df["key"] = self.df.apply(
            lambda row: separator.join(
                [row[f] for f in self.project.master_id_facets()]
            ),
            axis=1,
        )

        # From the catalog dataframe, we have the mapping `key` -> `dataset_id` but in
        # order to pass information back we need the inverse.
        dataset_ids = {
            dataset_id: row["key"]
            for _, row in self.df.iterrows()
            for dataset_id in row["id"]
        }

        # Some projects (CMIP5 for example) use dataset_ids to refer to collections of
        # variables. This means that the user may get many more variables than they want
        # simply because the variable name is not part of the dataset_id. We fix this
        # by including the variable facet from the last search when we get file
        # information.
        search_facets = {}
        variable_facet = self.project.variable_facet()
        if variable_facet in self.last_search:
            search_facets[variable_facet] = self.last_search[variable_facet]

        # The index nodes are again queried for file information. Each file points back
        # to the dataset_id to which it belongs.
        info_time = time.time()
        get_file_info = ThreadPool(len(self.indices)).imap_unordered(
            partial(_get_file_info, dataset_ids=dataset_ids, **search_facets),
            self.indices,
        )
        index_infos = list(
            tqdm(
                get_file_info,
                disable=quiet,
                bar_format=base.bar_format,
                unit="index",
                unit_scale=False,
                desc="Get file information",
                ascii=False,
                total=len(self.indices),
            )
        )
        index_infos = [info for index_info in index_infos for info in index_info]

        # Now we merge the access/validation information, but where the primary key is
        # not longer dataset_id, but rather the file path.
        combine_time = time.time()
        merged_info = {}
        for info in index_infos:
            path = info["path"]
            if path not in merged_info:
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
        combine_time = time.time() - combine_time
        info_time = time.time() - info_time
        logger.info(f"{combine_time=:.2f}")
        logger.info(f"\x1b[36;32mfile info end\033[0m total_time={info_time:.2f}")
        return infos

    def to_path_dict(
        self,
        prefer_streaming: bool = False,
        globus_endpoint: str | None = None,
        globus_path: Path | None = None,
        minimal_keys: bool = True,
        ignore_facets: None | str | list[str] = None,
        separator: str = ".",
        quiet: bool = False,
    ) -> dict[str, list[Path]]:
        """
        Return the current search as a dictionary of paths to files.

        Parameters
        ----------
        prefer_streaming: bool
            Enable to use streaming links (OPENDAP, Zarr, Kerchunk, etc.) instead of
            downloading data.
        globus_endpoint: str
            A destination globus endpoint UUID to which we will transfer files using
            Globus Transfers where possible.
        globus_path: Path
            The relative path to the endpoint root into which we will save files in the
            Globus Transfer.
        minimal_keys: bool
            Disable to return a dictonary whose keys are formed using all facets in the
            master_id.
        ignore_facets: str or list of str
            When constructing the dictionary keys, which facets should we ignore?
        separator: str
            When generating the keys, the string to use as a seperator of facets.
        quiet: bool
            Enable to quiet the progress bars.
        """
        if self.df is None or len(self.df) == 0:
            raise ValueError("No entries to retrieve.")
        prefer_globus = globus_endpoint is not None

        # get and partition file info based on access method
        infos = self._get_file_info(separator=separator, quiet=quiet)
        infos, dsd = base.partition_infos(
            infos,
            prefer_streaming,
            prefer_globus,
        )

        # optionally use globus to transfer what we can
        tasks = []
        if prefer_globus:
            tasks = create_globus_transfer(
                infos["globus"], globus_endpoint, globus_path
            )

        # download in parallel using threads
        if infos["https"]:
            # inform user of download sizes
            download_size = sum([info["size"] for info in infos["https"]]) * 1e-6
            download_unit = "Mb"
            if download_size > 1e3:
                download_size *= 1e-3
                download_unit = "Gb"
            if not quiet:
                print(f"Downloading {download_size:.1f} [{download_unit}]...")

            list(
                ThreadPool(
                    min(intake_esgf.conf["num_threads"], len(infos["https"]))
                ).imap_unordered(
                    partial(
                        base.parallel_download,
                        local_cache=self.local_cache,
                        download_db=self.download_db,
                        esg_dataroot=self.esg_dataroot,
                    ),
                    infos["https"],
                )
            )

        # unpack the https files which should now exist in local cache
        dsd = _load_into_dsd(dsd, infos["https"])

        if prefer_globus and infos["globus"]:
            monitor_globus_transfer(tasks)  # blocks while transfer continues

            # unpack the globus files which should now exist in local cache
            dsd = _load_into_dsd(dsd, infos["globus"])

        # did we get everything that was in the catalog?
        missed = set(self.df["key"]) - set(dsd)
        if missed:
            warnings.warn(f"We could not download your entire catalog, {missed=}")
            if intake_esgf.conf["break_on_error"]:
                raise MissingFileInformation(missed)

        # optionally simplify the keys
        if minimal_keys:
            key_format = self._minimal_key_format(ignore_facets)
            key_map = {
                row["key"]: separator.join(row[key_format])
                for _, row in self.df.iterrows()
            }
            dsd = {key_new: dsd[key_old] for key_old, key_new in key_map.items()}

        return dsd

    def to_dataset_dict(
        self,
        prefer_streaming: bool = False,
        globus_endpoint: str | None = None,
        globus_path: Path | None = None,
        add_measures: bool = True,
        minimal_keys: bool = True,
        ignore_facets: None | str | list[str] = None,
        separator: str = ".",
        quiet: bool = False,
    ) -> dict[str, xr.Dataset]:
        """
        Return the current search as a dictionary of datasets.

        Parameters
        ----------
        prefer_streaming: bool
            Enable to use streaming links (OPENDAP, Zarr, Kerchunk, etc.) instead of
            downloading data.
        globus_endpoint: str
            A destination globus endpoint UUID to which we will transfer files using
            Globus Transfers where possible.
        globus_path: Path
            The relative path to the endpoint root into which we will save files in the
            Globus Transfer.
        add_measures: bool
            Disable to supress the automated search for cell measure information.
        minimal_keys: bool
            Disable to return a dictonary whose keys are formed using all facets in the
            master_id.
        ignore_facets: str or list of str
            When constructing the dictionary keys, which facets should we ignore?
        separator: str
            When generating the keys, the string to use as a seperator of facets.
        quiet: bool
            Enable to quiet the progress bars.
        """
        logger = intake_esgf.conf.get_logger()
        ds = self.to_path_dict(
            prefer_streaming=prefer_streaming,
            globus_endpoint=globus_endpoint,
            globus_path=globus_path,
            minimal_keys=False,
            ignore_facets=ignore_facets,
            separator=separator,
            quiet=quiet,
        )

        # load paths into xarray objects (also log files accessed)
        exceptions = []
        failed_keys = []
        for key, files in ds.items():
            [logger.info(f"accessed {f}") for f in files]
            if len(files) == 1:
                try:
                    ds[key] = xr.open_dataset(files[0])
                except Exception as ex:
                    warnings.warn(
                        f"xarray threw an exception opening this file: {files[0]}"
                    )
                    failed_keys.append(key)
                    exceptions.append(ex)
            elif len(files) > 1:
                try:
                    ds[key] = xr.open_mfdataset(sorted(files))
                except Exception as ex:
                    warnings.warn(
                        f"xarray threw an exception opening these files: {files}"
                    )
                    failed_keys.append(key)
                    exceptions.append(ex)
        if intake_esgf.conf["break_on_error"] and exceptions:
            for ex in exceptions:
                print(ex)
            raise DatasetInitError(failed_keys)

        # master_id facets should be in the global attributes of each file, but
        # sometimes they aren't
        master_id_facets = self.project.master_id_facets()
        for key in ds:
            row = self.df.loc[self.df["key"] == key]
            assert len(row) == 1
            row = row.iloc[0]
            ds[key].attrs.update(row[master_id_facets].to_dict())

        # attempt to add cell measures (serial), only work for CMIP6 for now
        if ds and add_measures and "cmip6" in str(self.project.__class__).lower():
            for key in tqdm(
                ds,
                disable=quiet,
                bar_format=base.bar_format,
                unit="dataset",
                unit_scale=False,
                desc="Adding cell measures",
                ascii=False,
                total=len(ds),
            ):
                ds[key] = base.add_cell_measures(ds[key], self)

        # optionally simplify the keys
        if minimal_keys:
            key_format = self._minimal_key_format(ignore_facets)
            key_map = {
                row["key"]: separator.join(row[key_format])
                for _, row in self.df.iterrows()
            }
            ds = {
                key_new: ds[key_old]
                for key_old, key_new in key_map.items()
                if key_old in ds
            }

        return ds

    def remove_incomplete(self, complete: Callable[[pd.DataFrame], bool]) -> Self:
        """
        Remove the incomplete search results as defined by the `complete` function.

        While the ESGF search results will return anything matching the criteria, we are
        typically interested in unique combinations of `source_id`, `member_id`, and
        `grid_label`. Many times modeling groups upload different realizations but they
        do not contain all the variables either by oversight or design. This function
        will internally group the results by these criteria and then call the
        user-provided `complete` function on the grouped dataframe and remove entries
        deemed incomplete.

        """
        group = [
            self.project.model_facet(),
            self.project.variant_facet(),
        ]
        try:
            group.append(self.project.grid_facet())
        except ValueError:
            pass
        for _, grp in self.df.groupby(group):
            if not complete(grp):
                self.df = self.df.drop(grp.index)
        return self

    def remove_ensembles(self) -> Self:
        """Remove higher numeric ensembles for each `source_id`.

        Many times an ESGF search will return possible many ensembles, but you only need
        1 for your analysis, usually the smallest numeric values in the `member_id`.
        While in most cases it will simply be `r1i1p1f1`, this is not always the case.
        This function will select the *smallest* `member_id` (in terms of the smallest 4
        integer values) for each `source_id` in your search and remove all others.

        """
        df = self.model_groups()
        variant_facet = self.project.variant_facet()
        names = [name for name in df.index.names if name != variant_facet]
        for lbl, grp in df.to_frame().reset_index().groupby(names):
            variant = grp.iloc[0][variant_facet]
            q = " & ".join([f"({n} == '{v}')" for n, v in zip(names, lbl)])
            q += f" & ({variant_facet} != '{variant}')"
            self.df = self.df.drop(self.df.query(q).index)
        return self

    def session_log(self) -> str:
        """
        Return the log since the instantiation of this catalog.
        """
        log = open(Path(intake_esgf.conf["logfile"]).expanduser()).readlines()[::-1]
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
        """
        Return the per host download summary statistics as a dataframe.

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

    def variable_info(self, query: str, project: str = "CMIP6") -> pd.DataFrame:
        """
        Return a dataframe with variable information from a query.

        If you are new to searching for data in ESGF, you may not know how to figure out
        what variables you need for your purpose.

        Parameters
        ----------
        query
            A search string whose contents we will use to search all record fields.
        project
            The project whose records we will search, defaults to `CMIP6`.

        Returns
        -------
        df
            A dataframe with the possibly relevant variables, their units, and various
            name and description fields.
        """
        return variable_info(query, project)


def _load_into_dsd(
    dsd: dict[str, list[Path]], infos: list[dict]
) -> dict[str, list[Path]]:
    """
    Insert the local path into the dictinoary if the file exists.

    Parameters
    ----------
    dsd: dict[str,list[Path]]
        A dictionary of a list of paths whose keys are datasets.
    infos: list[dict]
        A list of file info to parse.

    Returns
    -------
    dict[str,list[Path]]
        The dataset dictionary with paths from the file info loaded.
    """
    for info in infos:
        try:
            path = base.get_local_file(info["path"], intake_esgf.conf["local_cache"])
        except Exception:
            # This means that the file isn't there, will be reported down the line
            continue
        key = info["key"]
        if key not in dsd:
            dsd[key] = []
        dsd[key] += [path]
    return dsd


def _minimal_key_format(
    cat: ESGFCatalog, ignore_facets: list[str] | str | None = None
) -> list[str]:
    """ """
    if ignore_facets is None:
        ignore_facets = []
    if isinstance(ignore_facets, str):
        ignore_facets = [ignore_facets]
    output_key_format = [
        col
        for col in cat.project.master_id_facets()
        if ((cat.df[col].iloc[0] != cat.df[col]).any() and col not in ignore_facets)
    ]
    if not output_key_format:
        output_key_format = [cat.project.variable_facet()]
    return output_key_format
