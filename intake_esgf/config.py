"""Configuration for intake-esgf"""

import contextlib
import copy
from pathlib import Path

import yaml

import intake_esgf
import intake_esgf.logging

defaults = {
    "stac_indices": {
        "api.stac.ceda.ac.uk": False,
    },
    "globus_indices": {
        "ESGF2-US-1.5-Catalog": True,
        "anl-dev": False,
        "ornl-dev": False,
    },
    "solr_indices": {
        "esgf.ceda.ac.uk": False,
        "esgf-data.dkrz.de": False,
        "esgf-node.ipsl.upmc.fr": False,
        "esg-dn1.nsc.liu.se": False,
        "esgf.nci.org.au": False,
        "esgf-node.ornl.gov": False,
        "esgf-node.llnl.gov": False,
    },
    "additional_df_cols": [],
    "esg_dataroot": [
        "/p/css03/esgf_publish",
        "/eagle/projects/ESGF2/esg_dataroot",
        "/global/cfs/projectdirs/m3522/cmip6/",
        "/glade/campaign/collections/cmip.mirror",
    ],
    "local_cache": [
        "~/.esgf/",
    ],
    "requests_cache": {
        "expire_after": 3600,  # 1 hour
        "cache_name": "intake-esgf/requests-cache.sqlite",
        "use_cache_dir": True,
    },
    "logfile": "~/.config/intake-esgf/esgf.log",
    "download_db": "~/.config/intake-esgf/download.db",
    "num_threads": 6,
    "break_on_error": True,
    "confirm_download": False,
    "slow_download_threshold": 0.5,  # [Mb s-1]
    "print_log_on_error": False,
}


class Config(dict):
    """A global configuration object used in the package."""

    def __init__(self, filename: Path | None = None, **kwargs):
        self.filename = (
            Path(filename)
            if filename is not None
            else Path.home() / ".config/intake-esgf/conf.yaml"
        )
        self.filename.parent.mkdir(parents=True, exist_ok=True)
        self.reload_all()
        self.temp = None
        super().__init__(**kwargs)

    def __repr__(self):
        return yaml.dump(dict(self))

    def reset(self):
        """Return to defaults."""
        self.clear()
        self.update(copy.deepcopy(defaults))

    def save(self, filename: Path | None = None):
        """Save current configuration to file as YAML."""
        filename = filename or self.filename
        filename.parent.mkdir(parents=True, exist_ok=True)
        with open(filename, "w") as f:
            yaml.dump(dict(self), f)

    @contextlib.contextmanager
    def _unset(self, temp):
        yield
        self.clear()
        self.update(temp)

    def set(
        self,
        *,
        indices: dict[str, bool] = {},
        no_indices: bool = False,
        all_indices: bool = False,
        esg_dataroot: list[str] | None = None,
        local_cache: list[str] | None = None,
        requests_cache: dict | None = None,
        additional_df_cols: list[str] | None = None,
        num_threads: int | None = None,
        break_on_error: bool | None = None,
        confirm_download: bool | None = None,
        slow_download_threshold: float | None = None,
        print_log_on_error: bool | None = None,
    ):
        """Change intake-esgf configuration options.

        Parameters
        ----------
        indices: dict
            Indices whose use status you wish to change.
        no_indices: bool
            Enable to disable all indices, useful when you want to subsequently
            enable a single index that isn't on by default.
        all_indices: bool
            Enable to check all indices for dataset information.
        esg_dataroot: list
            Read-only locations that we will check for ESGF data.
        local_cache: list
            Locations where we read and write data to, prefering the first
            entry.
        requests_cache: dict
            By default, intake-esgf will cache search requests sent to speed
            up repeated searches. This is done using the requests-cache library.
            To configure the cache, you can pass a dictionary with arguments to
            :class:`requests_cache.session.CachedSession`. The most important setting is
            "expire_after", which defines the time after which the cache expires.
            It can be set to any of the following time values:
            - A positive number (in seconds)
            - A timedelta
            - A datetime
            Or one of the following special values:
            - "DO_NOT_CACHE": Skip both reading from and writing to the cache
            - "EXPIRE_IMMEDIATELY": Consider the response already expired, but potentially usable
            - "NEVER_EXPIRE": Store responses indefinitely
        additional_df_cols: list
            Additional columns to include in the dataframe. Must be part of the
            search results.
        num_threads: int
            The number of threads to use when downloading via https.
        break_on_error: bool
            Should a user script continue if any of the datasets fail to load?
        confirm_download: bool
            Enable to require the user to confirm before downloads occur.
        slow_download_threshold: float
            The download rate in [Mb s-1] below which a download will be
            canceled in favor of another link.
        print_log_on_error: bool
            Enable to print the session log when a DatasetLoadError is
            encountered. This is meant to be used in debugging CI to understand
            what is happening as tests fail.

        Examples
        --------
        >>> intake-esgf.conf.set(indices={'esgf-node.ornl.gov': True})

        """
        temp = copy.deepcopy(self)
        if no_indices:
            for index_type in ["globus_indices", "solr_indices", "stac_indices"]:
                for key in self[index_type]:
                    self[index_type][key] = False
        self["globus_indices"].update(
            {
                key: value
                for key, value in indices.items()
                if key in self["globus_indices"]
            }
        )
        self["solr_indices"].update(
            {
                key: value
                for key, value in indices.items()
                if key in self["solr_indices"]
            }
        )
        self["stac_indices"].update(
            {
                key: value
                for key, value in indices.items()
                if key in self["stac_indices"]
            }
        )
        if all_indices:
            for index_type in [
                "globus_indices",
                "solr_indices",
            ]:  # exclude STAC for now
                for key in self[index_type]:
                    self[index_type][key] = True
        if esg_dataroot is not None:
            self["esg_dataroot"] = (
                esg_dataroot if isinstance(esg_dataroot, list) else [esg_dataroot]
            )
        if local_cache is not None:
            self["local_cache"] = (
                local_cache if isinstance(local_cache, list) else [local_cache]
            )
        if requests_cache is not None:
            self["requests_cache"] = dict(requests_cache)
        if additional_df_cols is not None:
            self["additional_df_cols"] = (
                additional_df_cols
                if isinstance(additional_df_cols, list)
                else [additional_df_cols]
            )
        if num_threads is not None:
            self["num_threads"] = int(num_threads)
        if break_on_error is not None:
            self["break_on_error"] = bool(break_on_error)
        if confirm_download is not None:
            self["confirm_download"] = bool(confirm_download)
        if slow_download_threshold is not None:
            self["slow_download_threshold"] = float(slow_download_threshold)
        if print_log_on_error is not None:
            self["print_log_on_error"] = bool(print_log_on_error)
        return self._unset(temp)

    def __getitem__(self, item):
        if item in self:
            return super().__getitem__(item)
        elif item in defaults:
            return defaults[item]
        else:
            raise KeyError(item)

    def get(self, key, default=None):
        if key in self:
            return super().__getitem__(key)
        return default

    def reload_all(self):
        self.reset()
        self.load()

    def load(self, filename: Path | None = None):
        """Update global config from YAML file or default file if None."""
        filename = filename or self.filename
        if filename.is_file():
            with open(filename) as f:
                try:
                    self.update(yaml.safe_load(f))
                except Exception:
                    pass

    def get_logger(self) -> intake_esgf.logging.Logger:
        """Setup the location and logging for this package."""
        return intake_esgf.logging.Logger()


conf = Config()
conf.reload_all()
