import tempfile
from pathlib import Path

import pandas as pd
import pytest
import xarray as xr

import intake_esgf
import intake_esgf.database as db


@pytest.fixture(autouse=True)
def reset_intake_esgf_config():
    """Reset the intake_esgf configuration before each test."""
    intake_esgf.conf.reset()
    intake_esgf.conf.set(local_cache=[tempfile.gettempdir()], print_log_on_error=True)
    intake_esgf.conf["download_db"] = str(Path(tempfile.gettempdir()) / "download.db")
    db.create_download_database(Path(intake_esgf.conf["download_db"]))


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


@pytest.fixture
def file_infos():
    """A reduced set of file infos to check partitioning."""
    yield [
        {  # only https is available, file1 in a dataset
            "key": "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Amon.tas.gn",
            "HTTPServer": [
                "http://esgf-node.ornl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/Amon/tas/gn/v20190429/tas_Amon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc",
            ],
            "path": Path(
                "CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/Amon/tas/gn/v20190429/tas_Amon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc"
            ),
        },
        {  # only https is available, file2 in the same dataset
            "key": "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Amon.tas.gn",
            "HTTPServer": [
                "http://esgf-node.ornl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/Amon/tas/gn/v20190429/tas_Amon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc",
            ],
            "path": Path(
                "CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/Amon/tas/gn/v20190429/tas_Amon_CanESM5_historical_r1i1p1f1_gn_201501-202012.nc"
            ),
        },
        {  # all options are available, file in a different dataset
            "key": "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Amon.pr.gn",
            "HTTPServer": [
                "http://esgf-node.ornl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/Amon/pr/gn/v20190429/pr_Amon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc",
            ],
            "OPENDAP": [
                "http://esgf-node.ornl.gov/thredds/dodsC/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/Amon/pr/gn/v20190429/pr_Amon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc",
            ],
            "Globus": [
                "globus:dea29ae8-bb92-4c63-bdbc-260522c92fe8/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/Amon/pr/gn/v20190429/pr_Amon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc",
                "globus:8896f38e-68d1-4708-bce4-b1b3a3405809/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/Amon/pr/gn/v20190429/pr_Amon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc",
            ],
            "path": Path(
                "CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/Amon/pr/gn/v20190429/pr_Amon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc"
            ),
        },
    ]


@pytest.fixture
def dataset():
    yield xr.Dataset(
        data_vars=dict(
            temperature=(["lat"], [280.0]),
        ),
        coords=dict(
            lat=("lat", [42.0]),
        ),
        attrs=dict(
            project="CMIP6",
            activity_id="CMIP",
            institution_id="CCCma",
            source_id="CanESM5",
            experiment_id="historical",
            member_id="r1i1p1f1",
            table_id="Amon",
            variable_id="tas",
            grid_label="gn",
        ),
    )


@pytest.fixture
def cmip5_record():
    yield {
        "project": "CMIP5",
        "institute": "NSF-DOE-NCAR",
        "model": "CESM1(WACCM)",
        "experiment": "rcp85",
        "time_frequency": "mon",
        "realm": "atmos",
        "cmor_table": "Amon",
        "ensemble": "r3i1p1",
        "variable": "clw",
        "version": "20130314",
        "data_node": "eagle.alcf.anl.gov",
        "id": "cmip5.output1.NSF-DOE-NCAR.CESM1-WACCM.rcp85.mon.atmos.Amon.r3i1p1.v20130314|eagle.alcf.anl.gov",
    }


@pytest.fixture
def project_contents():
    yield {
        "CMIP6": {
            "master_id": "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.fx.sftlf.gn.sftlf_fx_CanESM5_historical_r1i1p1f1_gn.nc",
            "experiment_title": ["all-forcing simulation of the recent past"],
            "replica": False,
            "data_specs_version": ["01.00.29"],
            "variable_units": ["%"],
            "project": ["CMIP6"],
            "pid": ["hdl:21.14100/055f024b-36c4-36be-b3f7-23be4d494e3f"],
            "table_id": ["fx"],
            "type": "File",
            "frequency": ["fx"],
            "institution_id": ["CCCma"],
            "score": 1.1461977,
            "variable_id": ["sftlf"],
            "activity_id": ["CMIP"],
            "id": "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.fx.sftlf.gn.v20190429.sftlf_fx_CanESM5_historical_r1i1p1f1_gn.nc|crd-esgf-drc.ec.gc.ca",
            "_timestamp": "2019-06-27T00:47:22.466Z",
            "data_node": "crd-esgf-drc.ec.gc.ca",
            "variable_long_name": [
                "Percentage of the grid  cell occupied by land (including lakes)"
            ],
            "dataset_id": "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.fx.sftlf.gn.v20190429|crd-esgf-drc.ec.gc.ca",
            "source_type": ["AOGCM"],
            "cf_standard_name": ["land_area_fraction"],
            "version": 20190429,
            "further_info_url": [
                "https://furtherinfo.es-doc.org/CMIP6.CCCma.CanESM5.historical.none.r1i1p1f1"
            ],
            "instance_id": "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.fx.sftlf.gn.v20190429.sftlf_fx_CanESM5_historical_r1i1p1f1_gn.nc",
            "size": 59765,
            "grid_label": ["gn"],
            "nominal_resolution": ["500 km"],
            "citation_url": [
                "http://cera-www.dkrz.de/WDCC/meta/CMIP6/CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.fx.sftlf.gn.v20190429.json"
            ],
            "directory_format_template_": [
                "%(root)s/%(mip_era)s/%(activity_drs)s/%(institution_id)s/%(source_id)s/%(experiment_id)s/%(member_id)s/%(table_id)s/%(variable_id)s/%(grid_label)s/%(version)s"
            ],
            "index_node": "us-index",
            "title": "sftlf_fx_CanESM5_historical_r1i1p1f1_gn.nc",
            "retracted": False,
            "checksum_type": ["SHA256"],
            "variant_label": ["r1i1p1f1"],
            "checksum": [
                "86b2d45490252d5b75fd420204a7fce063ef1bb1287cac16c2a866af4feb6208"
            ],
            "tracking_id": ["hdl:21.14100/90f93a05-357c-4ea2-b61f-bf2418700791"],
            "latest": True,
            "timestamp": "2019-05-02T01:36:43Z",
            "member_id": ["r1i1p1f1"],
            "sub_experiment_id": ["none"],
            "mip_era": ["CMIP6"],
            "product": ["model-output"],
            "model_cohort": ["Registered"],
            "experiment_id": ["historical"],
            "url": [
                "http://crd-esgf-drc.ec.gc.ca/thredds/fileServer/esgC_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/fx/sftlf/gn/v20190429/sftlf_fx_CanESM5_historical_r1i1p1f1_gn.nc|application/netcdf|HTTPServer",
                "gsiftp://crd-esgf-drc.ec.gc.ca:2811//esgC_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/fx/sftlf/gn/v20190429/sftlf_fx_CanESM5_historical_r1i1p1f1_gn.nc|application/gridftp|GridFTP",
                "http://crd-esgf-drc.ec.gc.ca/thredds/dodsC/esgC_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/fx/sftlf/gn/v20190429/sftlf_fx_CanESM5_historical_r1i1p1f1_gn.nc.html|application/opendap-html|OPENDAP",
            ],
            "activity_drs": ["CMIP"],
            "dataset_id_template_": [
                "%(mip_era)s.%(activity_drs)s.%(institution_id)s.%(source_id)s.%(experiment_id)s.%(member_id)s.%(table_id)s.%(variable_id)s.%(grid_label)s"
            ],
            "_version_": 1637452551255228416,
            "grid": [
                "T63L49 native atmosphere, T63 Linear Gaussian Grid; 128 x 64 longitude/latitude; 49 levels; top level 1 hPa"
            ],
            "variable": ["sftlf"],
            "realm": ["atmos"],
            "source_id": ["CanESM5"],
        },
        "CMIP5": {
            "master_id": "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Emon.wetlandFrac.gn.wetlandFrac_Emon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc",
            "experiment_title": ["all-forcing simulation of the recent past"],
            "replica": False,
            "data_specs_version": ["01.00.29"],
            "variable_units": ["%"],
            "project": ["CMIP6"],
            "pid": ["hdl:21.14100/8ea2418c-2375-31cf-97c1-6b9784bd3a72"],
            "table_id": ["Emon"],
            "type": "File",
            "frequency": ["mon"],
            "institution_id": ["CCCma"],
            "score": 1.1461977,
            "variable_id": ["wetlandFrac"],
            "activity_id": ["CMIP"],
            "id": "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Emon.wetlandFrac.gn.v20190429.wetlandFrac_Emon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc|crd-esgf-drc.ec.gc.ca",
            "_timestamp": "2019-06-27T00:47:17.378Z",
            "data_node": "crd-esgf-drc.ec.gc.ca",
            "variable_long_name": ["Wetland Percentage Cover"],
            "dataset_id": "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Emon.wetlandFrac.gn.v20190429|crd-esgf-drc.ec.gc.ca",
            "source_type": ["AOGCM"],
            "cf_standard_name": ["area_fraction"],
            "version": 20190429,
            "further_info_url": [
                "https://furtherinfo.es-doc.org/CMIP6.CCCma.CanESM5.historical.none.r1i1p1f1"
            ],
            "instance_id": "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Emon.wetlandFrac.gn.v20190429.wetlandFrac_Emon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc",
            "size": 9885815,
            "grid_label": ["gn"],
            "nominal_resolution": ["500 km"],
            "citation_url": [
                "http://cera-www.dkrz.de/WDCC/meta/CMIP6/CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Emon.wetlandFrac.gn.v20190429.json"
            ],
            "directory_format_template_": [
                "%(root)s/%(mip_era)s/%(activity_drs)s/%(institution_id)s/%(source_id)s/%(experiment_id)s/%(member_id)s/%(table_id)s/%(variable_id)s/%(grid_label)s/%(version)s"
            ],
            "index_node": "us-index",
            "title": "wetlandFrac_Emon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc",
            "retracted": False,
            "checksum_type": ["SHA256"],
            "variant_label": ["r1i1p1f1"],
            "checksum": [
                "f7cdc50186900f029bd2ed5f0a5aa18852435bd464636f9957c3f071291567ab"
            ],
            "tracking_id": ["hdl:21.14100/19b257d3-3eed-4726-ba7f-a9022017dab5"],
            "latest": True,
            "timestamp": "2019-05-31T01:36:37Z",
            "member_id": ["r1i1p1f1"],
            "sub_experiment_id": ["none"],
            "mip_era": ["CMIP6"],
            "product": ["model-output"],
            "model_cohort": ["Registered"],
            "experiment_id": ["historical"],
            "url": [
                "http://crd-esgf-drc.ec.gc.ca/thredds/fileServer/esgC_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/Emon/wetlandFrac/gn/v20190429/wetlandFrac_Emon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc|application/netcdf|HTTPServer",
                "gsiftp://crd-esgf-drc.ec.gc.ca:2811//esgC_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/Emon/wetlandFrac/gn/v20190429/wetlandFrac_Emon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc|application/gridftp|GridFTP",
                "http://crd-esgf-drc.ec.gc.ca/thredds/dodsC/esgC_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p1f1/Emon/wetlandFrac/gn/v20190429/wetlandFrac_Emon_CanESM5_historical_r1i1p1f1_gn_185001-201412.nc.html|application/opendap-html|OPENDAP",
            ],
            "activity_drs": ["CMIP"],
            "dataset_id_template_": [
                "%(mip_era)s.%(activity_drs)s.%(institution_id)s.%(source_id)s.%(experiment_id)s.%(member_id)s.%(table_id)s.%(variable_id)s.%(grid_label)s"
            ],
            "_version_": 1637452545920073728,
            "grid": [
                "T63L49 native atmosphere, T63 Linear Gaussian Grid; 128 x 64 longitude/latitude; 49 levels; top level 1 hPa"
            ],
            "variable": ["wetlandFrac"],
            "realm": ["land"],
            "source_id": ["CanESM5"],
        },
        "CMIP3": {
            "master_id": "cmip3.BCCR.bccr_bcm2_0.historical.fx.land.run1.sftlf.sftlf_A1.nc",
            "replica": True,
            "time_frequency": ["fx"],
            "variable_units": ["%"],
            "description": [
                "BCCR model output prepared for IPCC Fourth Assessment Report climate of the 20th Century experiment (20C3M)"
            ],
            "project": ["CMIP3"],
            "ensemble": ["run1"],
            "index_node": "us-index",
            "title": "sftlf_A1.nc",
            "type": "File",
            "experiment_family": ["All", "Historical"],
            "retracted": False,
            "score": 6.7849283,
            "checksum_type": ["SHA256"],
            "experiment": ["historical"],
            "checksum": [
                "57a18099d2336a96c9b73afb691da224579f96f0cd329622c0ad5fcb2ed5df69"
            ],
            "model": ["bccr_bcm2_0"],
            "id": "cmip3.BCCR.bccr_bcm2_0.historical.fx.land.run1.sftlf.v1.sftlf_A1.nc|eagle.alcf.anl.gov",
            "_timestamp": "2024-02-20T14:13:40.062Z",
            "latest": True,
            "timestamp": "2011-09-04T09:33:20Z",
            "data_node": "eagle.alcf.anl.gov",
            "variable_long_name": ["Land Area Fraction"],
            "dataset_id": "cmip3.BCCR.bccr_bcm2_0.historical.fx.land.run1.sftlf.v1|eagle.alcf.anl.gov",
            "format": ["netCDF, CF-1.0"],
            "cf_standard_name": ["land_area_fraction"],
            "version": 1,
            "url": [
                "https://g-52ba3.fd635.8443.data.globus.org/cmip3_data/data2/20c3m/land/fixed/sftlf/bccr_bcm2_0/run1/sftlf_A1.nc|application/netcdf|HTTPServer",
                "globus:8896f38e-68d1-4708-bce4-b1b3a3405809/cmip3_data/data2/20c3m/land/fixed/sftlf/bccr_bcm2_0/run1/sftlf_A1.nc|Globus|Globus",
            ],
            "dataset_id_template_": [
                "%(project)s.%(institute)s.%(model)s.%(experiment)s.%(time_frequency)s.%(realm)s.%(ensemble)s.%(variable)s"
            ],
            "instance_id": "cmip3.BCCR.bccr_bcm2_0.historical.fx.land.run1.sftlf.v1.sftlf_A1.nc",
            "size": 41200,
            "_version_": 1791427524754931712,
            "variable": ["sftlf"],
            "institute": ["BCCR"],
            "realm": ["land"],
        },
        "input4MIPs": {
            "master_id": "input4MIPs.CMIP7.C4MIP.ImperialCollege.ImperialCollege-3-0.atmos.yr.delta13co2.gm.delta13co2_input4MIPs_atmosphericState_C4MIP_ImperialCollege-3-0_gm_1700-2023.nc",
            "Conventions": ["CF-1.6"],
            "replica": True,
            "deprecated": False,
            "variable_units": ["1"],
            "project": ["input4MIPs"],
            "pid": ["hdl:21.14100/d7f1a6f8-8f95-34a3-86f3-70d57c9192be"],
            "source": ["Imperial College 3.0: delta13co2"],
            "type": "File",
            "institution_id": ["ImperialCollege"],
            "frequency": ["yr"],
            "variable_id": ["delta13co2"],
            "contact": ["h.graven@imperial.ac.uk"],
            "activity_id": ["input4MIPs"],
            "id": "input4MIPs.CMIP7.C4MIP.ImperialCollege.ImperialCollege-3-0.atmos.yr.delta13co2.gm.v20250502.delta13co2_input4MIPs_atmosphericState_C4MIP_ImperialCollege-3-0_gm_1700-2023.nc|esgf-node.ornl.gov",
            "dataset_category": ["atmosphericState"],
            "_timestamp": "2025-07-15T18:19:03.847Z",
            "target_mip_list": ["CMIP"],
            "target_mip": ["C4MIP"],
            "data_node": "esgf-node.ornl.gov",
            "variable_long_name": ["delta13C in atmospheric CO2"],
            "dataset_id": "input4MIPs.CMIP7.C4MIP.ImperialCollege.ImperialCollege-3-0.atmos.yr.delta13co2.gm.v20250502|esgf-node.ornl.gov",
            "dataset_status": ["latest"],
            "creation_date": "2025-05-02T09:09:02Z",
            "version": 20250502,
            "further_info_url": ["https://profiles.imperial.ac.uk/h.graven"],
            "instance_id": "input4MIPs.CMIP7.C4MIP.ImperialCollege.ImperialCollege-3-0.atmos.yr.delta13co2.gm.v20250502.delta13co2_input4MIPs_atmosphericState_C4MIP_ImperialCollege-3-0_gm_1700-2023.nc",
            "size": 31313,
            "grid_label": ["gm"],
            "nominal_resolution": ["10000 km"],
            "publish_path": [
                "user_pub_work/input4MIPs/CMIP7/C4MIP/ImperialCollege/ImperialCollege-3-0/atmos/yr/delta13co2/gm/v20250502/delta13co2_input4MIPs_atmosphericState_C4MIP_ImperialCollege-3-0_gm_1700-2023.nc"
            ],
            "short_description": [
                "Imperial College 3.0: CMIP7 historical delta13C in atmospheric CO2, global-mean annual-mean"
            ],
            "index_node": "us-index",
            "directory_format_template_": [
                "%(root)s/%(activity_id)s/%(mip_era)s/%(target_mip)s/%(institution_id)s/%(source_id)s/%(realm)s/%(frequency)s/%(variable_id)s/%(grid_label)s/%(version)s"
            ],
            "title": "delta13co2_input4MIPs_atmosphericState_C4MIP_ImperialCollege-3-0_gm_1700-2023.nc",
            "source_version": ["3.0"],
            "retracted": False,
            "checksum_type": ["SHA256"],
            "checksum": [
                "29f9d01da0f82b5e10b95e750ac2d1afb95d213b603b9306fabf6d5b98ff45be"
            ],
            "tracking_id": ["hdl:21.14100/9949bb24-b740-47e8-a301-b1069b1c5e21"],
            "latest": True,
            "timestamp": "2025-05-05T21:22:51Z",
            "mip_era": ["CMIP7"],
            "product": [
                "Imperial College 3.0: CMIP7 historical delta13C in atmospheric CO2, global-mean annual-mean"
            ],
            "url": [
                "https://esgf-node.ornl.gov/thredds/fileServer/user_pub_work/input4MIPs/CMIP7/C4MIP/ImperialCollege/ImperialCollege-3-0/atmos/yr/delta13co2/gm/v20250502/delta13co2_input4MIPs_atmosphericState_C4MIP_ImperialCollege-3-0_gm_1700-2023.nc|application/netcdf|HTTPServer",
                "https://esgf-node.ornl.gov/thredds/dodsC/user_pub_work/input4MIPs/CMIP7/C4MIP/ImperialCollege/ImperialCollege-3-0/atmos/yr/delta13co2/gm/v20250502/delta13co2_input4MIPs_atmosphericState_C4MIP_ImperialCollege-3-0_gm_1700-2023.nc|application/opendap-html|OPENDAP",
                "globus:dea29ae8-bb92-4c63-bdbc-260522c92fe8/user_pub_work/input4MIPs/CMIP7/C4MIP/ImperialCollege/ImperialCollege-3-0/atmos/yr/delta13co2/gm/v20250502/delta13co2_input4MIPs_atmosphericState_C4MIP_ImperialCollege-3-0_gm_1700-2023.nc|Globus|Globus",
            ],
            "dataset_id_template_": [
                "%(activity_id)s.%(mip_era)s.%(target_mip)s.%(institution_id)s.%(source_id)s.%(realm)s.%(frequency)s.%(variable_id)s.%(grid_label)s"
            ],
            "variable": ["delta13co2"],
            "realm": ["atmos"],
            "source_id": ["ImperialCollege-3-0"],
        },
        "obs4MIPs": {
            "master_id": "obs4MIPs.NASA-LaRC.CERES-EBAF-4-2.mon.rsdt.gn.rsdt_mon_CERES-EBAF-4-2_RSS_gn_200003-202310.nc",
            "replica": True,
            "data_specs_version": ["ODS-2.5"],
            "variable_units": ["W m-2"],
            "project": ["obs4MIPs"],
            "index_node": "us-index",
            "directory_format_template_": [
                "%(root)s/%(activity_id)s/%(institution_id)s/%(source_id)s/%(frequency)s/%(variable_id)s/%(grid_label)s/%(version)s"
            ],
            "source": [
                "CERES-EBAF-4-2 4.2 (2022): CERES EBAF (Energy Balanced and Filled) TOA Fluxes. Monthly Averages"
            ],
            "title": "rsdt_mon_CERES-EBAF-4-2_RSS_gn_200003-202310.nc",
            "type": "File",
            "institution_id": ["NASA-LaRC"],
            "frequency": ["mon"],
            "source_version_number": ["4.2"],
            "institution": ["NASA-LaRC (Langley Research Center) Hampton, Va"],
            "retracted": False,
            "score": 11.922695,
            "checksum_type": ["SHA256"],
            "variable_id": ["rsdt"],
            "contact": ["RSS (support@remss.com)"],
            "activity_id": ["obs4MIPs"],
            "checksum": [
                "80c23e9873ce4578b276a4ccdea6d69bab0817dffa0d79ed165a25db8dbf0b09"
            ],
            "id": "obs4MIPs.NASA-LaRC.CERES-EBAF-4-2.mon.rsdt.gn.v20240513.rsdt_mon_CERES-EBAF-4-2_RSS_gn_200003-202310.nc|esgf-node.ornl.gov",
            "_timestamp": "2025-07-15T16:18:11.948Z",
            "tracking_id": ["hdl:21.14102/5ce2141b-98e2-4d2b-928a-0f6391b6a56f"],
            "latest": True,
            "timestamp": "2024-09-18T14:53:27Z",
            "product": ["observations"],
            "data_node": "esgf-node.ornl.gov",
            "variable_long_name": ["TOA Incident Shortwave Radiation"],
            "dataset_id": "obs4MIPs.NASA-LaRC.CERES-EBAF-4-2.mon.rsdt.gn.v20240513|esgf-node.ornl.gov",
            "source_type": ["satellite_blended"],
            "creation_date": "2024-05-13T19:19:22Z",
            "cf_standard_name": ["toa_incoming_shortwave_flux"],
            "version": 20240513,
            "url": [
                "https://esgf-node.ornl.gov/thredds/fileServer/user_pub_work/obs4MIPs/NASA-LaRC/CERES-EBAF-4-2/mon/rsdt/gn/v20240513/rsdt_mon_CERES-EBAF-4-2_RSS_gn_200003-202310.nc|application/netcdf|HTTPServer",
                "https://esgf-node.ornl.gov/thredds/dodsC/user_pub_work/obs4MIPs/NASA-LaRC/CERES-EBAF-4-2/mon/rsdt/gn/v20240513/rsdt_mon_CERES-EBAF-4-2_RSS_gn_200003-202310.nc|application/opendap-html|OPENDAP",
                "globus:dea29ae8-bb92-4c63-bdbc-260522c92fe8/user_pub_work/obs4MIPs/NASA-LaRC/CERES-EBAF-4-2/mon/rsdt/gn/v20240513/rsdt_mon_CERES-EBAF-4-2_RSS_gn_200003-202310.nc|Globus|Globus",
            ],
            "dataset_id_template_": [
                "%(activity_id)s.%(institution_id)s.%(source_id)s.%(frequency)s.%(variable_id)s.%(grid_label)s"
            ],
            "further_info_url": ["."],
            "instance_id": "obs4MIPs.NASA-LaRC.CERES-EBAF-4-2.mon.rsdt.gn.v20240513.rsdt_mon_CERES-EBAF-4-2_RSS_gn_200003-202310.nc",
            "size": 5520301,
            "_version_": 1810643896752930816,
            "grid_label": ["gn"],
            "variable": ["rsdt"],
            "realm": ["atmos"],
            "source_id": ["CERES-EBAF-4-2"],
            "nominal_resolution": ["100 km"],
            "region": ["global"],
            "publish_path": [
                "user_pub_work/obs4MIPs/NASA-LaRC/CERES-EBAF-4-2/mon/rsdt/gn/v20240513/rsdt_mon_CERES-EBAF-4-2_RSS_gn_200003-202310.nc"
            ],
        },
    }


@pytest.fixture
def download_db():
    download_db = Path(intake_esgf.conf["download_db"]).expanduser()
    download_db.parent.mkdir(parents=True, exist_ok=True)
    yield download_db


@pytest.fixture
def df_catalog():
    yield pd.DataFrame(
        {
            "project": {0: "CMIP6", 4: "CMIP6"},
            "mip_era": {0: "CMIP6", 4: "CMIP6"},
            "activity_drs": {0: "CMIP", 4: "CMIP"},
            "institution_id": {0: "CCCma", 4: "CCCma"},
            "source_id": {0: "CanESM5", 4: "CanESM5"},
            "experiment_id": {0: "historical", 4: "historical"},
            "member_id": {0: "r1i1p1f1", 4: "r1i1p1f1"},
            "table_id": {0: "Amon", 4: "Lmon"},
            "variable_id": {0: "tas", 4: "gpp"},
            "grid_label": {0: "gn", 4: "gn"},
            "version": {0: "20190429", 4: "20190429"},
            "id": {
                0: [
                    "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Amon.tas.gn.v20190429|crd-esgf-drc.ec.gc.ca",
                    "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Amon.tas.gn.v20190429|eagle.alcf.anl.gov",
                    "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Amon.tas.gn.v20190429|esgf-data04.diasjp.net",
                    "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Amon.tas.gn.v20190429|esgf-node.ornl.gov",
                ],
                4: [
                    "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Lmon.gpp.gn.v20190429|crd-esgf-drc.ec.gc.ca",
                    "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Lmon.gpp.gn.v20190429|eagle.alcf.anl.gov",
                    "CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Lmon.gpp.gn.v20190429|esgf-node.ornl.gov",
                ],
            },
        }
    )


@pytest.fixture
def catalog(df_catalog):
    cat = intake_esgf.ESGFCatalog()
    cat.df = df_catalog
    cat._set_project()
    yield cat
