"""Tools for generating search facets from CV terms."""

import sqlite3
from pathlib import Path
from typing import Literal

import pandas as pd
import requests

import intake_esgf


def create_cv_universe(path: Path, ingest_data: list[tuple[str, str, str]]) -> None:
    """
    Create a SQLite database with CV terms.

    Parameters
    ----------
    path
        The full path to the database file.
    ingest_data
        A list of tuples of the form (term, collection, project) to be ingested.
        For example, [('CESM2','source_id','CMIP6'),('tas','variable','CMIP5')].
    """
    con = sqlite3.connect(path)
    cur = con.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Collections(
    CollectionId   INTEGER PRIMARY KEY,
    CollectionName TEXT NOT NULL,
    ProjectName    TEXT NOT NULL,
    UNIQUE(CollectionName, ProjectName)
    );""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Terms(
    TermId    INTEGER PRIMARY KEY,
    TermName  TEXT NOT NULL,
    CollectionId INTEGER NOT NULL,
    FOREIGN KEY(CollectionId) REFERENCES Collections(CollectionId),
    UNIQUE(TermName, CollectionId)
    );""")
    for term, collection, project in ingest_data:
        # Try to insert a new category, ignore if already present
        cur.execute(
            f"INSERT INTO Collections (CollectionId,CollectionName,ProjectName) VALUES (NULL,'{collection}','{project}') ON CONFLICT DO NOTHING"
        )
        # Get the collection id so we can insert the term
        collection_id = cur.execute(
            f"SELECT CollectionId FROM Collections WHERE CollectionName='{collection}' AND ProjectName='{project}'"
        ).fetchone()
        assert len(collection_id) == 1
        # Now insert the term
        cur.execute(
            f"INSERT INTO Terms (TermId,TermName,CollectionId) VALUES (NULL,'{term}','{collection_id[0]}') ON CONFLICT DO NOTHING"
        )
        con.commit()
    cur.close()
    con.close()


def ingest_by_facet_query(path: Path) -> None:
    """
    Create a database by making a faceted search.

    Parameters
    ----------
    path
        The full path to the database file.

    Note
    ----
    For the moment we will harvest the CV by hard-coding the collections per
    project and then using a facet query to populate a list of terms to ingest.
    This allows us to include projects which have no formal CV repository. Later
    we may consider creating another ingest routine that reads the json files
    directly.
    """
    FACETS_BY_PROJECT = {
        "CMIP6": [
            "mip_era",
            "activity_drs",
            "institution_id",
            "source_id",
            "experiment_id",
            "member_id",
            "table_id",
            "variable_id",
            "grid_label",
            "frequency",
            "realm",
        ],
        "CMIP5": [
            "institute",
            "model",
            "experiment",
            "time_frequency",
            "realm",
            "cmor_table",
            "ensemble",
            "variable",
        ],
        "obs4MIPs": [
            "activity_id",
            "institution_id",
            "source_id",
            "frequency",
            "variable_id",
            "grid_label",
            "realm",
        ],
        "input4MIPs": [
            "activity_id",
            "mip_era",
            "target_mip",
            "institution_id",
            "source_id",
            "realm",
            "frequency",
            "variable_id",
            "grid_label",
        ],
        "CMIP3": [
            "project",
            "institute",
            "model",
            "experiment",
            "time_frequency",
            "realm",
            "ensemble",
            "variable",
        ],
    }
    for project, facets in FACETS_BY_PROJECT.items():
        url = f"https://esgf-node.ornl.gov/esgf-1-5-bridge/?project={project}&limit=0&facets={','.join(facets)}"
        resp = requests.get(url)
        resp.raise_for_status()
        create_cv_universe(
            path,
            [
                (term, collection, project)
                for collection, terms in resp.json()["facet_counts"][
                    "facet_fields"
                ].items()
                for term in terms[::2]
            ],
        )


def query_cv(
    terms: str | list[str],
    project: Literal["CMIP6", "CMIP5", "CMIP3", "obs4MIPs", "input4MIPs"] = "CMIP6",
) -> dict[str, str | list[str]]:
    """
    Return a dictionary of collections and terms, found by a query of the CV
    universe.

    Terms are case insensitive and may contain wildcards like `*` or `%`. The
    returned dictionary is only built by a query on the CV databse and may
    return no records when used to search an index.

    Parameters
    ----------
    terms
        A space-delimited string or list of terms to find in the CV universe.
    project
        The project whose CVs we will query.

    Returns
    -------

    """
    cv_db = Path(intake_esgf.conf["cv_db"]).expanduser()
    if not cv_db.is_file():
        ingest_by_facet_query(cv_db)
    con = sqlite3.connect(cv_db)
    terms = terms.split(" ") if isinstance(terms, str) else terms
    q = " OR ".join([f"TermName LIKE '{t.replace('*', '%')}'" for t in terms])
    q = f"ProjectName='{project}' AND ({q})"
    df = pd.read_sql_query(
        f"""
    SELECT TermName, CollectionName, ProjectName
    FROM Terms
    INNER JOIN Collections
    ON Terms.CollectionId = Collections.CollectionId
    WHERE {q}
    ORDER BY ProjectName;""",
        con,
    )
    con.close()
    df = df.groupby(["ProjectName", "CollectionName"]).agg(lambda gr: gr)
    out = {
        key: list(val) if pd.api.types.is_list_like(val) else val
        for key, val in df.loc[project].to_dict()["TermName"].items()
    }
    return out
