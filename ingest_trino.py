"""Ingest Trino catalog metadata into Neo4j (neocarta data model).

Usage:
    .venv/Scripts/python ingest_trino.py                       # catalog postgres, all schemas
    .venv/Scripts/python ingest_trino.py --schemas raw mart
    .venv/Scripts/python ingest_trino.py --values 5 --exclude-columns email
"""

from __future__ import annotations

import argparse
import logging
import os

import trino
from neo4j import GraphDatabase

from trino_auth import trino_connection_kwargs
from trino_neocarta import TrinoSchemaConnector

SYSTEM_SCHEMAS = {"information_schema", "pg_catalog", "system"}

log = logging.getLogger("ingest_trino")


def add_source_args(parser: argparse.ArgumentParser) -> None:
    """Arguments selecting what to read from Trino (shared with export_cypher.py)."""
    parser.add_argument("--catalog", default="postgres")
    parser.add_argument("--schemas", nargs="*", help="Schemas to ingest (default: all non-system schemas)")
    parser.add_argument("--values", type=int, default=0, help="Distinct sample values per column (0 = off)")
    parser.add_argument("--exclude-columns", nargs="*", default=[], help="Columns never sampled (PII)")


def connect_trino(catalog: str) -> trino.dbapi.Connection:
    auth_kwargs = trino_connection_kwargs()
    # With authentication, Trino takes the user from the token; a different TRINO_USER is
    # treated as impersonation and must be allowed by Trino's access control.
    default_user = None if "auth" in auth_kwargs else "neocarta"
    return trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "localhost"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", default_user),
        catalog=catalog,
        **auth_kwargs,
    )


def ingest_catalog(conn: trino.dbapi.Connection, driver, args: argparse.Namespace) -> None:
    """Run the Trino schema connector for every selected schema of ``args.catalog``.

    ``driver`` is a real Neo4j driver, or any object with a compatible ``execute_query``
    (export_cypher.py passes one that records the Cypher instead of executing it).
    """
    cur = conn.cursor()
    cur.execute("SELECT connector_name FROM system.metadata.catalogs WHERE catalog_name = ?", [args.catalog])
    (connector_name,) = cur.fetchone()
    # PK/FK are only readable through pass-through on PostgreSQL-backed catalogs.
    key_source = "postgresql" if connector_name == "postgresql" else None

    schemas = args.schemas
    if not schemas:
        cur.execute(f'SELECT schema_name FROM "{args.catalog}".information_schema.schemata ORDER BY 1')
        schemas = [r[0] for r in cur.fetchall() if r[0] not in SYSTEM_SCHEMAS]
    log.info("Catalog %s (%s) -> schemas %s, key_source=%s", args.catalog, connector_name, schemas, key_source)

    for schema in schemas:
        connector = TrinoSchemaConnector(
            conn,
            args.catalog,
            driver,
            database_name=os.getenv("NEO4J_DATABASE", "neo4j"),
            value_sample_limit=args.values,
            exclude_value_columns=args.exclude_columns,
            key_source=key_source,
        )
        connector.ingest(schema=schema)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_source_args(parser)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    # Silence "index or constraint already exists" notices on re-runs.
    logging.getLogger("neo4j.notifications").setLevel(logging.WARNING)

    conn = connect_trino(args.catalog)
    driver = GraphDatabase.driver(
        os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        auth=(os.getenv("NEO4J_USERNAME", "neo4j"), os.getenv("NEO4J_PASSWORD", "password123")),
    )
    try:
        ingest_catalog(conn, driver, args)
    finally:
        driver.close()
        conn.close()


if __name__ == "__main__":
    main()
