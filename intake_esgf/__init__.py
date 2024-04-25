# An intake and intake-esm inpsired catalog for ESGF

import warnings

import xarray as xr

warnings.simplefilter("ignore", category=xr.SerializationWarning)


def in_notebook() -> bool:
    """Check if the code is running in a jupyter notebook"""
    try:
        shell = get_ipython().__class__.__name__
        if shell == "ZMQInteractiveShell":  # Jupyter notebook, Spyder or qtconsole
            return True
        elif shell == "TerminalInteractiveShell":
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False  # Probably standard Python interpreter


IN_NOTEBOOK = in_notebook()

from intake_esgf.catalog import ESGFCatalog  # noqa
from intake_esgf.config import conf  # noqa
from intake_esgf._version import __version__  # noqa

__all__ = ["ESGFCatalog", "conf", "IN_NOTEBOOK"]
