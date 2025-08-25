import sqlite3

import numpy as np
import pandas as pd

import intake_esgf.database as db


def test_create_download_database(download_db):
    if download_db.is_file():
        download_db.unlink()
    db.create_download_database(download_db)
    assert download_db.is_file()


def test_log_download_information(download_db):
    db.log_download_information(download_db, "hostname", 123.0, 456.0)
    con = sqlite3.connect(download_db)
    df = pd.read_sql_query(
        "SELECT * FROM downloads",
        con,
    )
    con.close()
    assert df.iloc[-1].host == "hostname"


def test_get_download_rate_dataframe(download_db):
    df = db.get_download_rate_dataframe(download_db)
    assert np.allclose(df.loc["hostname"]["rate"], 3.707317073170732)


def test_sort_globus_endpoints(download_db):
    if not download_db.is_file():
        db.create_download_database(download_db)
    db.log_download_information(download_db, "abcd1234", 10, 1000)
    db.log_download_information(download_db, "efgh5678", 5, 1000)
    df = db.get_download_rate_dataframe(download_db)
    assert np.allclose(db.sort_globus_endpoints("efgh5678", df), 200)
