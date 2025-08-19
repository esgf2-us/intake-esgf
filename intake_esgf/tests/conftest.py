import tempfile
from pathlib import Path

import pandas as pd
import pytest

import intake_esgf


@pytest.fixture(autouse=True)
def reset_intake_esgf_config():
    """Reset the intake_esgf configuration before each test."""
    intake_esgf.conf.reset()
    intake_esgf.conf.set(local_cache=[tempfile.gettempdir()])


@pytest.fixture
def file_info():
    """A typical file information dict, built by combining information from multiple sources."""
    yield {
        "key": "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Amon.tas.gn",
        "dataset_id": "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Amon.tas.gn.v20190429|esgf-node.ornl.gov",
        "checksum_type": "SHA256",
        "checksum": "1d4052b00baac5d9d383070effe6bb36d5194568706e4c6088ee65b6f7dcb52b",
        "size": 52443019,
        "HTTPServer": [
            "http://esgf-node.ornl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/Amon/tas/gn/v20190429/tas_Amon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc",
            "http://crd-esgf-drc.ec.gc.ca/thredds/fileServer/esgC_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/Amon/tas/gn/v20190429/tas_Amon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc",
            "https://g-52ba3.fd635.8443.data.globus.org/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/Amon/tas/gn/v20190429/tas_Amon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc",
            "http://esgf-data04.diasjp.net/thredds/fileServer/esg_dataroot/CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/Amon/tas/gn/v20190429/tas_Amon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc",
        ],
        "OPENDAP": [
            "http://esgf-node.ornl.gov/thredds/dodsC/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/Amon/tas/gn/v20190429/tas_Amon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc",
            "http://crd-esgf-drc.ec.gc.ca/thredds/dodsC/esgC_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/Amon/tas/gn/v20190429/tas_Amon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc",
            "http://esgf-data04.diasjp.net/thredds/dodsC/esg_dataroot/CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/Amon/tas/gn/v20190429/tas_Amon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc",
        ],
        "Globus": [
            "globus:dea29ae8-bb92-4c63-bdbc-260522c92fe8/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/Amon/tas/gn/v20190429/tas_Amon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc",
            "globus:8896f38e-68d1-4708-bce4-b1b3a3405809/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/Amon/tas/gn/v20190429/tas_Amon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc",
        ],
        "path": Path(
            "CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/Amon/tas/gn/v20190429/tas_Amon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc"
        ),
        "file_start": pd.Timestamp("1850-01-01 00:00:00"),
        "file_end": pd.Timestamp("2014-12-01 00:00:00"),
    }


@pytest.fixture
def df_rate():
    """A typical download summary."""
    yield pd.DataFrame(
        {
            "transfer_time": {
                "8896f38e-68d1-4708-bce4-b1b3a3405809": 53.0,
                "aims3.llnl.gov": 121.31305503845215,
                "crd-esgf-drc.ec.gc.ca": 465.55214977264404,
                "dap.ceda.ac.uk": 29.870222568511963,
                "esgf-data.ucar.edu": 36.27675199508667,
                "esgf-data03.diasjp.net": 40.45653414726257,
                "esgf-data04.diasjp.net": 8304.751965761185,
                "esgf-data1.llnl.gov": 79.31723427772522,
                "esgf-data2.llnl.gov": 1921.2127192020416,
                "esgf-node.ornl.gov": 3036.9145703315735,
                "esgf.ceda.ac.uk": 258.69592690467834,
                "esgf.nci.org.au": 58.44198822975159,
                "esgf3.dkrz.de": 52.36517524719238,
                "g-52ba3.fd635.8443.data.globus.org": 4769.454469442368,
            },
            "transfer_size": {
                "8896f38e-68d1-4708-bce4-b1b3a3405809": 526.10342,
                "aims3.llnl.gov": 19.153249,
                "crd-esgf-drc.ec.gc.ca": 1375.396099,
                "dap.ceda.ac.uk": 52.443019,
                "esgf-data.ucar.edu": 243.03448699999998,
                "esgf-data03.diasjp.net": 225.35780799999998,
                "esgf-data04.diasjp.net": 21171.360985,
                "esgf-data1.llnl.gov": 61.17716,
                "esgf-data2.llnl.gov": 2605.1381549999996,
                "esgf-node.ornl.gov": 12714.51236497792,
                "esgf.ceda.ac.uk": 243.03448699999998,
                "esgf.nci.org.au": 95.369614,
                "esgf3.dkrz.de": 132.716558,
                "g-52ba3.fd635.8443.data.globus.org": 20862.797727999998,
            },
            "rate": {
                "8896f38e-68d1-4708-bce4-b1b3a3405809": 9.92647962264151,
                "aims3.llnl.gov": 0.15788283457150645,
                "crd-esgf-drc.ec.gc.ca": 2.9543330423276646,
                "dap.ceda.ac.uk": 1.7556956222778002,
                "esgf-data.ucar.edu": 6.6994555365076955,
                "esgf-data03.diasjp.net": 5.570368612884464,
                "esgf-data04.diasjp.net": 2.5493068392993847,
                "esgf-data1.llnl.gov": 0.7712971910466686,
                "esgf-data2.llnl.gov": 1.3559863147699855,
                "esgf-node.ornl.gov": 4.186654603059755,
                "esgf.ceda.ac.uk": 0.9394600444928956,
                "esgf.nci.org.au": 1.6318680607695228,
                "esgf3.dkrz.de": 2.5344431174631796,
                "g-52ba3.fd635.8443.data.globus.org": 4.374252414330988,
            },
        }
    )


@pytest.fixture
def df_ornl():
    yield pd.DataFrame(
        {
            "project": {
                1: "CMIP6",
                2: "CMIP6",
                5: "CMIP6",
                7: "CMIP6",
                8: "CMIP6",
                9: "CMIP6",
                10: "CMIP6",
            },
            "mip_era": {
                1: "CMIP6",
                2: "CMIP6",
                5: "CMIP6",
                7: "CMIP6",
                8: "CMIP6",
                9: "CMIP6",
                10: "CMIP6",
            },
            "activity_drs": {
                1: "CMIP",
                2: "CMIP",
                5: "CMIP",
                7: "CMIP",
                8: "CMIP",
                9: "CMIP",
                10: "CMIP",
            },
            "institution_id": {
                1: "CCCma",
                2: "CCCma",
                5: "CCCma",
                7: "CCCma",
                8: "CCCma",
                9: "CCCma",
                10: "CCCma",
            },
            "source_id": {
                1: "CanESM5",
                2: "CanESM5",
                5: "CanESM5",
                7: "CanESM5",
                8: "CanESM5",
                9: "CanESM5",
                10: "CanESM5",
            },
            "experiment_id": {
                1: "historical",
                2: "historical",
                5: "historical",
                7: "historical",
                8: "historical",
                9: "historical",
                10: "historical",
            },
            "member_id": {
                1: "r1i1p1f1",
                2: "r1i1p1f1",
                5: "r1i1p1f1",
                7: "r1i1p1f1",
                8: "r1i1p1f1",
                9: "r1i1p1f1",
                10: "r1i1p1f1",
            },
            "table_id": {
                1: "Amon",
                2: "Amon",
                5: "Amon",
                7: "Amon",
                8: "Amon",
                9: "Amon",
                10: "Amon",
            },
            "variable_id": {
                1: "tas",
                2: "tas",
                5: "tas",
                7: "tas",
                8: "tas",
                9: "tas",
                10: "tas",
            },
            "grid_label": {
                1: "gn",
                2: "gn",
                5: "gn",
                7: "gn",
                8: "gn",
                9: "gn",
                10: "gn",
            },
            "version": {
                1: "20190306",
                2: "20190306",
                5: "20190429",
                7: "20190429",
                8: "20190429",
                9: "20190429",
                10: "20190429",
            },
            "data_node": {
                1: "crd-esgf-drc.ec.gc.ca",
                2: "esgf-node.ornl.gov",
                5: "eagle.alcf.anl.gov",
                7: "crd-esgf-drc.ec.gc.ca",
                8: "eagle.alcf.anl.gov",
                9: "esgf-data04.diasjp.net",
                10: "esgf-node.ornl.gov",
            },
            "id": {
                1: "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Amon.tas.gn.v20190306|crd-esgf-drc.ec.gc.ca",
                2: "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Amon.tas.gn.v20190306|esgf-node.ornl.gov",
                5: "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Amon.tas.gn.v20190429.tas_Amon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc|eagle.alcf.anl.gov",
                7: "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Amon.tas.gn.v20190429|crd-esgf-drc.ec.gc.ca",
                8: "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Amon.tas.gn.v20190429|eagle.alcf.anl.gov",
                9: "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Amon.tas.gn.v20190429|esgf-data04.diasjp.net",
                10: "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Amon.tas.gn.v20190429|esgf-node.ornl.gov",
            },
        }
    )


@pytest.fixture
def df_ceda():
    yield pd.DataFrame(
        {
            "project": {0: "CMIP6"},
            "mip_era": {0: "CMIP6"},
            "activity_drs": {0: "CMIP"},
            "institution_id": {0: "CCCma"},
            "source_id": {0: "CanESM5"},
            "experiment_id": {0: "historical"},
            "member_id": {0: "r1i1p1f1"},
            "table_id": {0: "Amon"},
            "variable_id": {0: "tas"},
            "grid_label": {0: "gn"},
            "version": {0: "20190429"},
            "data_node": {0: "esgf.ceda.ac.uk"},
            "id": {
                0: "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Amon.tas.gn.v20190429|esgf.ceda.ac.uk"
            },
        }
    )
