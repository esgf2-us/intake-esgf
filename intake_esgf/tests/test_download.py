from pathlib import Path

import pytest

import intake_esgf
from intake_esgf.base import download_and_verify
from intake_esgf.exceptions import StalledDownload


def test_slow_cancel():
    intake_esgf.conf.set(slow_download_threshold=100)  # unreasonably fast speed
    with pytest.raises(StalledDownload):
        download_and_verify(
            url="https://esgf-node.ornl.gov/thredds/fileServer/css03_data/CMIP6/ScenarioMIP/MIROC/MIROC-ES2L/ssp245/r5i1p1f2/Amon/prc/gn/v20201222/prc_Amon_MIROC-ES2L_ssp245_r5i1p1f2_gn_201501-210012.nc",
            local_file=Path("tmp.nc"),
            hash=None,
            hash_algorithm=None,
            content_length=100,
            download_db=Path(intake_esgf.conf["download_db"]).expanduser(),
            logger=intake_esgf.conf.get_logger(),
            break_slow_downloads=True,
        )
