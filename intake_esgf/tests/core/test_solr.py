from typing import Any

import numpy as np
import pytest
import requests

from intake_esgf.core.solr import SolrESGFIndex


@pytest.fixture
def solr_dataset_response():
    yield {
        "responseHeader": {
            "status": 0,
            "QTime": 10,
            "params": {
                "df": "text",
                "q.alt": "*:*",
                "indent": "true",
                "echoParams": "all",
                "fl": "*,score",
                "start": "0",
                "fq": [
                    "type:Dataset",
                    'source_id:"CESM2"',
                    'variable_id:"tas"',
                    'experiment_id:"historical"',
                    'member_id:"r1i1p1f1"',
                    'frequency:"mon"',
                    'data_node:"esgf.ceda.ac.uk"',
                ],
                "rows": "10",
                "q": "*:*",
                "shards": "esgf-solr1.ceda.ac.uk:8983/solr/datasets,esgf-solr1.ceda.ac.uk:8996/solr/datasets,esgf-solr1.ceda.ac.uk:9000/solr/datasets,esgf-solr1.ceda.ac.uk:9001/solr/datasets",
                "tie": "0.01",
                "facet.limit": "-1",
                "qf": "text",
                "facet.method": "enum",
                "facet.mincount": "1",
                "wt": "json",
                "facet": "true",
                "facet.sort": "lex",
            },
        },
        "response": {
            "numFound": 1,
            "start": 0,
            "maxScore": 1.0,
            "docs": [
                {
                    "id": "CMIP6.CMIP.NCAR.CESM2.historical.r1i1p1f1.Amon.tas.gn.v20190308|esgf.ceda.ac.uk",
                    "version": "20190308",
                    "access": ["HTTPServer", "OPENDAP"],
                    "activity_drs": ["CMIP"],
                    "activity_id": ["CMIP"],
                    "cf_standard_name": ["air_temperature"],
                    "citation_url": [
                        "http://cera-www.dkrz.de/WDCC/meta/CMIP6/CMIP6.CMIP.NCAR.CESM2.historical.r1i1p1f1.Amon.tas.gn.v20190308.json"
                    ],
                    "data_node": "esgf.ceda.ac.uk",
                    "data_specs_version": ["01.00.29"],
                    "dataset_id_template_": [
                        "%(mip_era)s.%(activity_drs)s.%(institution_id)s.%(source_id)s.%(experiment_id)s.%(member_id)s.%(table_id)s.%(variable_id)s.%(grid_label)s"
                    ],
                    "datetime_start": "1850-01-15T12:00:00Z",
                    "datetime_stop": "2014-12-15T12:00:00Z",
                    "directory_format_template_": [
                        "%(root)s/%(mip_era)s/%(activity_drs)s/%(institution_id)s/%(source_id)s/%(experiment_id)s/%(member_id)s/%(table_id)s/%(variable_id)s/%(grid_label)s/%(version)s"
                    ],
                    "east_degrees": 358.75,
                    "experiment_id": ["historical"],
                    "experiment_title": ["all-forcing simulation of the recent past"],
                    "frequency": ["mon"],
                    "further_info_url": [
                        "https://furtherinfo.es-doc.org/CMIP6.NCAR.CESM2.historical.none.r1i1p1f1"
                    ],
                    "geo": [
                        "ENVELOPE(-180.0, -1.25, 90.0, -90.0)",
                        "ENVELOPE(0.0, 180.0, 90.0, -90.0)",
                    ],
                    "geo_units": ["degrees_east"],
                    "grid": ["native 0.9x1.25 finite volume grid (192x288 latxlon)"],
                    "grid_label": ["gn"],
                    "index_node": "esgf.ceda.ac.uk",
                    "instance_id": "CMIP6.CMIP.NCAR.CESM2.historical.r1i1p1f1.Amon.tas.gn.v20190308",
                    "institution_id": ["NCAR"],
                    "latest": True,
                    "master_id": "CMIP6.CMIP.NCAR.CESM2.historical.r1i1p1f1.Amon.tas.gn",
                    "member_id": ["r1i1p1f1"],
                    "metadata_format": "THREDDS",
                    "mip_era": ["CMIP6"],
                    "model_cohort": ["Registered"],
                    "nominal_resolution": ["100 km"],
                    "north_degrees": 90.0,
                    "number_of_files": 1,
                    "pid": ["hdl:21.14100/8e650f5b-d139-33a9-b0d8-edcb0bfd0987"],
                    "product": ["model-output"],
                    "project": ["CMIP6"],
                    "realm": ["atmos"],
                    "replica": True,
                    "size": 243034487,
                    "source_id": ["CESM2"],
                    "source_type": ["AOGCM", "BGC"],
                    "south_degrees": -90.0,
                    "sub_experiment_id": ["none"],
                    "table_id": ["Amon"],
                    "title": "CMIP6.CMIP.NCAR.CESM2.historical.r1i1p1f1.Amon.tas.gn",
                    "type": "Dataset",
                    "variable": ["tas"],
                    "variable_id": ["tas"],
                    "variable_long_name": ["Near-Surface Air Temperature"],
                    "variable_units": ["K"],
                    "variant_label": ["r1i1p1f1"],
                    "west_degrees": 0.0,
                    "xlink": [
                        "http://cera-www.dkrz.de/WDCC/meta/CMIP6/CMIP6.CMIP.NCAR.CESM2.historical.r1i1p1f1.Amon.tas.gn.v20190308.json|Citation|citation",
                        "http://hdl.handle.net/hdl:21.14100/8e650f5b-d139-33a9-b0d8-edcb0bfd0987|PID|pid",
                    ],
                    "retracted": False,
                    "_timestamp": "2019-04-09T20:07:55.229Z",
                    "score": 1.0,
                    "_version_": 1803475891868663808,
                }
            ],
        },
        "facet_counts": {
            "facet_queries": {},
            "facet_fields": {},
            "facet_ranges": {},
            "facet_intervals": {},
            "facet_heatmaps": {},
        },
    }


@pytest.fixture
def solr_file_response():
    yield {
        "responseHeader": {
            "status": 0,
            "QTime": 5,
            "params": {
                "df": "text",
                "q.alt": "*:*",
                "indent": "true",
                "echoParams": "all",
                "fl": "*,score",
                "start": "0",
                "fq": [
                    "type:File",
                    'dataset_id:"CMIP6.CMIP.NCAR.CESM2.historical.r1i1p1f1.Amon.tas.gn.v20190308|esgf.ceda.ac.uk"',
                ],
                "sort": "id asc",
                "rows": "10",
                "q": "*:*",
                "shards": "esgf-solr1.ceda.ac.uk:8983/solr/files,esgf-solr1.ceda.ac.uk:8996/solr/files,esgf-solr1.ceda.ac.uk:9000/solr/files,esgf-solr1.ceda.ac.uk:9001/solr/files",
                "tie": "0.01",
                "facet.limit": "-1",
                "qf": "text",
                "facet.method": "enum",
                "facet.mincount": "1",
                "wt": "json",
                "facet": "true",
                "facet.sort": "lex",
            },
        },
        "response": {
            "numFound": 1,
            "start": 0,
            "maxScore": 1.0,
            "docs": [
                {
                    "id": "CMIP6.CMIP.NCAR.CESM2.historical.r1i1p1f1.Amon.tas.gn.v20190308.tas_Amon_CESM2_historical_r1i1p1f1_gn_185001-201412.nc|esgf.ceda.ac.uk",
                    "version": "1",
                    "activity_drs": ["CMIP"],
                    "activity_id": ["CMIP"],
                    "cf_standard_name": ["air_temperature"],
                    "checksum": [
                        "796e8816468100543767249460913afaa09a32644804a5598f98fe854533a0a2"
                    ],
                    "checksum_type": ["SHA256"],
                    "citation_url": [
                        "http://cera-www.dkrz.de/WDCC/meta/CMIP6/CMIP6.CMIP.NCAR.CESM2.historical.r1i1p1f1.Amon.tas.gn.v20190308.json"
                    ],
                    "data_node": "esgf.ceda.ac.uk",
                    "data_specs_version": ["01.00.29"],
                    "dataset_id": "CMIP6.CMIP.NCAR.CESM2.historical.r1i1p1f1.Amon.tas.gn.v20190308|esgf.ceda.ac.uk",
                    "dataset_id_template_": [
                        "%(mip_era)s.%(activity_drs)s.%(institution_id)s.%(source_id)s.%(experiment_id)s.%(member_id)s.%(table_id)s.%(variable_id)s.%(grid_label)s"
                    ],
                    "directory_format_template_": [
                        "%(root)s/%(mip_era)s/%(activity_drs)s/%(institution_id)s/%(source_id)s/%(experiment_id)s/%(member_id)s/%(table_id)s/%(variable_id)s/%(grid_label)s/%(version)s"
                    ],
                    "experiment_id": ["historical"],
                    "experiment_title": ["all-forcing simulation of the recent past"],
                    "frequency": ["mon"],
                    "further_info_url": [
                        "https://furtherinfo.es-doc.org/CMIP6.NCAR.CESM2.historical.none.r1i1p1f1"
                    ],
                    "grid": ["native 0.9x1.25 finite volume grid (192x288 latxlon)"],
                    "grid_label": ["gn"],
                    "index_node": "esgf.ceda.ac.uk",
                    "instance_id": "CMIP6.CMIP.NCAR.CESM2.historical.r1i1p1f1.Amon.tas.gn.v20190308.tas_Amon_CESM2_historical_r1i1p1f1_gn_185001-201412.nc",
                    "institution_id": ["NCAR"],
                    "latest": True,
                    "master_id": "CMIP6.CMIP.NCAR.CESM2.historical.r1i1p1f1.Amon.tas.gn.tas_Amon_CESM2_historical_r1i1p1f1_gn_185001-201412.nc",
                    "member_id": ["r1i1p1f1"],
                    "metadata_format": "THREDDS",
                    "mip_era": ["CMIP6"],
                    "model_cohort": ["Registered"],
                    "nominal_resolution": ["100 km"],
                    "pid": ["hdl:21.14100/8e650f5b-d139-33a9-b0d8-edcb0bfd0987"],
                    "product": ["model-output"],
                    "project": ["CMIP6"],
                    "realm": ["atmos"],
                    "replica": True,
                    "size": 243034487,
                    "source_id": ["CESM2"],
                    "source_type": ["AOGCM", "BGC"],
                    "sub_experiment_id": ["none"],
                    "table_id": ["Amon"],
                    "timestamp": "2019-04-09T19:01:36Z",
                    "title": "tas_Amon_CESM2_historical_r1i1p1f1_gn_185001-201412.nc",
                    "tracking_id": [
                        "hdl:21.14100/d9a7225a-49c3-4470-b7ab-a8180926f839"
                    ],
                    "type": "File",
                    "url": [
                        "https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/CMIP/NCAR/CESM2/historical/r1i1p1f1/Amon/tas/gn/v20190308/tas_Amon_CESM2_historical_r1i1p1f1_gn_185001-201412.nc|application/netcdf|HTTPServer",
                        "https://esgf.ceda.ac.uk/thredds/dodsC/esg_cmip6/CMIP6/CMIP/NCAR/CESM2/historical/r1i1p1f1/Amon/tas/gn/v20190308/tas_Amon_CESM2_historical_r1i1p1f1_gn_185001-201412.nc.html|application/opendap-html|OPENDAP",
                    ],
                    "variable": ["tas"],
                    "variable_id": ["tas"],
                    "variable_long_name": ["Near-Surface Air Temperature"],
                    "variable_units": ["K"],
                    "variant_label": ["r1i1p1f1"],
                    "retracted": False,
                    "_timestamp": "2019-04-09T20:07:55.506Z",
                    "score": 1.0,
                    "_version_": 1803417166384463872,
                }
            ],
        },
        "facet_counts": {
            "facet_queries": {},
            "facet_fields": {},
            "facet_ranges": {},
            "facet_intervals": {},
            "facet_heatmaps": {},
        },
    }


def test_solr_search(solr_dataset_response, monkeypatch):
    def fake_get(*args, **kwargs) -> Any:
        class FakeResponse:
            def raise_for_status(self) -> None:
                return None

            def json(self, **kwargs) -> Any:
                return solr_dataset_response

        return FakeResponse()

    monkeypatch.setattr(requests.Session, "get", fake_get)
    ind = SolrESGFIndex()
    df = ind.search(
        source_id="CESM2",
        variable_id="tas",
        experiment_id="historical",
        member_id="r1i1p1f1",
        frequency="mon",
        data_node="esgf.ceda.ac.uk",
    )
    assert len(df) == 1


def test_solr_from_tracking_ids(solr_dataset_response, monkeypatch):
    def fake_get(*args, **kwargs) -> Any:
        class FakeResponse:
            def raise_for_status(self) -> None:
                return None

            def json(self, **kwargs) -> Any:
                return solr_dataset_response

        return FakeResponse()

    monkeypatch.setattr(requests.Session, "get", fake_get)
    ind = SolrESGFIndex()
    df = ind.from_tracking_ids(["hdl:21.14100/d9a7225a-49c3-4470-b7ab-a8180926f839"])
    assert len(df) == 1


def test_solr_file_info(solr_file_response, monkeypatch):
    def fake_get(*args, **kwargs) -> Any:
        class FakeResponse:
            def raise_for_status(self) -> None:
                return None

            def json(self, **kwargs) -> Any:
                return solr_file_response

        return FakeResponse()

    monkeypatch.setattr(requests.Session, "get", fake_get)
    ind = SolrESGFIndex()
    infos = ind.get_file_info(
        [
            "CMIP6.CMIP.NCAR.CESM2.historical.r1i1p1f1.Amon.tas.gn.v20190308|esgf.ceda.ac.uk"
        ]
    )
    assert len(infos) == 1
    info = infos[0]
    assert (
        info["dataset_id"]
        == "CMIP6.CMIP.NCAR.CESM2.historical.r1i1p1f1.Amon.tas.gn.v20190308|esgf.ceda.ac.uk"
    )
    assert np.allclose(info["size"], 243034487)
