"""Database functions which interact with SQLite."""

import sqlite3
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd


def create_download_database(path: Path) -> None:
    """Create a SQLite database for logging downloading information.

    Parameters
    ----------
    path
        The full path of the database file.

    """
    if path.is_file():
        return
    with sqlite3.connect(path) as con:
        cur = con.cursor()
        cur.execute(
            "CREATE TABLE downloads(timestamp TEXT NULL DEFAULT (datetime('now', 'localtime')), host, transfer_time, transfer_size)"
        )


def log_download_information(
    path: Path, host: str, transfer_time: float, transfer_size: float
):
    """Log an entry into the download database.

    Parameters
    ----------
    host
        The name of the host.
    transfer_time
        The time the transfer took in seconds.
    transfer_size
        The number of Mb transferred"""
    with sqlite3.connect(path) as con:
        cur = con.cursor()
        cur.execute(
            f"INSERT INTO downloads ('host','transfer_time','transfer_size') VALUES ('{host}',{transfer_time},{transfer_size})"
        )
        con.commit()


def get_download_rate_dataframe(
    path: Path,
    history: Literal[None, "day", "week", "month"] = None,
    minimum_size: float = 10,
) -> pd.DataFrame:
    """Get a dataframe with average download rates per host.

    Parameters
    ----------
    path
        The full path of the database file.
    history
        How much download history should we use in computing rates.
    minimum_size
        The minimum size in Mb to include in the reported record.
    """
    assert history in [None, "day", "week", "month"]
    condition = [f"transfer_size > {minimum_size}"]
    if history is not None:
        condition.append(f"timestamp > datetime('now', '-1 {history}', 'localtime')")
    condition = " AND ".join(condition)
    with sqlite3.connect(path) as con:
        df = pd.read_sql_query(
            f"SELECT * FROM downloads WHERE {condition}",
            con,
        )
    if not len(df):
        df = df.set_index("host")
        return df
    df = df.groupby("host").sum(numeric_only=True)
    df["rate"] = df["transfer_size"] / df["transfer_time"]
    return df


def sort_download_links(link: str, df_rate: pd.DataFrame) -> float:
    """Return the average download rate for the given link.

    This function is to be used to sort the list of links in terms of what is fastest
    for the user. If a host is not part of the dataframe, we return a random number
    larger than the fastest server. This is so that future downloads from this host will
    have entries in the database.

    Parameters
    ----------
    link
        The link to the file to download.
    df_rate
        The dataframe whose indices are hosts and contains a `rate` column.

    """
    if not len(df_rate):
        return np.random.rand(1)[0]
    host = link[: link.index("/", 10)].replace("http://", "").replace("https://", "")
    if host not in df_rate.index:
        return df_rate["rate"].max() + np.random.rand(1)[0]
    return df_rate.loc[host, "rate"]


def sort_globus_endpoints(uuid: str, df_rate: pd.DataFrame) -> float:
    """Return the average download rate for the given endpoint uuid.

    This function is to be used to sort the list of endpoints in terms of what is fastest
    for the user. If a endpoint is not part of the dataframe, we return a random number
    larger than the fastest server. This is so that future downloads from this host will
    have entries in the database.

    Parameters
    ----------
    uuid
        The endpoint where the file(s) may be accessed download.
    df_rate
        The dataframe whose indices are hosts and contains a `rate` column.

    """
    if not len(df_rate):
        return np.random.rand(1)[0]
    if uuid not in df_rate.index:
        return df_rate["rate"].max() + np.random.rand(1)[0]
    return df_rate.loc[uuid, "rate"]
