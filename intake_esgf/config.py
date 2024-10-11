"""Configuration for intake-esgf"""

import contextlib
import copy
import logging
from pathlib import Path

import yaml

defaults = {
    "globus_indices": {
        "anl-dev": True,
        "ornl-dev": True,
    },
    "solr_indices": {
        "esgf.ceda.ac.uk": False,
        "esgf-data.dkrz.de": False,
        "esgf-node.ipsl.upmc.fr": False,
        "esg-dn1.nsc.liu.se": False,
        "esgf-node.llnl.gov": False,
        "esgf.nci.org.au": False,
        "esgf-node.ornl.gov": False,
    },
    "additional_df_cols": [],
    "esg_dataroot": [
        "/p/css03/esgf_publish",
        "/eagle/projects/ESGF2/esg_dataroot",
        "/global/cfs/projectdirs/m3522/cmip6/",
    ],
    "local_cache": [
        "~/.esgf/",
    ],
    "logfile": "~/.config/intake-esgf/esgf.log",
    "download_db": "~/.config/intake-esgf/download.db",
    "num_threads": 6,
    "break_on_error": True,
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
        all_indices: bool = False,
        esg_dataroot: list[str] | None = None,
        local_cache: list[str] | None = None,
        additional_df_cols: list[str] | None = None,
        num_threads: int | None = None,
        break_on_error: bool | None = None
    ):
        """Change intake-esgf configuration options.

        Parameters
        ----------
        indices: dict
            Indices whose use status you wish to change.
        all_indices: bool
            Enable to check all indices for dataset information.
        esg_dataroot: list
            Read-only locations that we will check for ESGF data.
        local_cache: list
            Locations where we read and write data to, prefering the first
            entry.
        additional_df_cols: list
            Additional columns to include in the dataframe. Must be part
            of the search results.
        num_threads: int
            The number of threads to use when downloading via https.
        break_on_error: bool
            Should a user script continue if any of the datasets fail to load?

        Examples
        --------
        >>> intake-esgf.conf.set(indices={'esgf-node.ornl.gov': True})

        """
        temp = copy.deepcopy(self)
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
        if all_indices:
            for key in self["globus_indices"]:
                self["globus_indices"][key] = True
            for key in self["solr_indices"]:
                self["solr_indices"][key] = True
        if esg_dataroot is not None:
            self["esg_dataroot"] = (
                esg_dataroot if isinstance(esg_dataroot, list) else [esg_dataroot]
            )
        if local_cache is not None:
            self["local_cache"] = (
                local_cache if isinstance(local_cache, list) else [local_cache]
            )
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

    def get_logger(self) -> logging.Logger:
        """Setup the location and logging for this package."""

        # Where will the log be written?
        log_file = Path(self["logfile"]).expanduser()
        log_file.parent.mkdir(parents=True, exist_ok=True)
        if not log_file.exists():
            log_file.touch()

        # We need a named logger to avoid other packages that use the root logger
        logger = logging.getLogger("intake-esgf")
        if not logger.handlers:
            # Now setup the file into which we log relevant information
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(
                logging.Formatter(
                    "\x1b[36;20m%(asctime)s \033[0m%(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
            )
            file_handler.setLevel(logging.INFO)
            logger.addHandler(file_handler)
        logger.setLevel(logging.INFO)

        # This is probably wrong, but when I log from my logger it logs from parent also
        logger.parent.handlers = []
        return logger


conf = Config()
conf.reload_all()
