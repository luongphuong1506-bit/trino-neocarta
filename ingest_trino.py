"""Ingest Trino catalog metadata into Neo4j (neocarta data model).

Settings come from command-line flags, or from a config file (--env-file, see
config.example.env) / environment variables; flags win over the file.

Usage:
    .venv/Scripts/python ingest_trino.py --env-file prod.env
    .venv/Scripts/python ingest_trino.py --catalog postgres iceberg --values 5 --exclude-columns email
    .venv/Scripts/python ingest_trino.py --catalog postgres --schemas raw mart
"""

from __future__ import annotations

import argparse
import logging
import os

import trino
from dotenv import load_dotenv
from neo4j import GraphDatabase

from trino_auth import trino_connection_kwargs
from trino_neocarta import TrinoSchemaConnector

SYSTEM_SCHEMAS = {"information_schema", "pg_catalog", "system"}

log = logging.getLogger("ingest_trino")


def _split(value: str | None) -> list[str]:
    """Split a space/comma separated config value."""
    return [v for v in (value or "").replace(",", " ").split() if v]


def parse_args(parser: argparse.ArgumentParser) -> argparse.Namespace:
    """Parse flags after loading ``--env-file``, so config-file values become the defaults."""
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--env-file")
    known, _ = pre.parse_known_args()
    if known.env_file:
        if not os.path.isfile(known.env_file):
            parser.error(f"--env-file {known.env_file} not found")
        # The file is an explicit choice, so it wins over variables left in the shell.
        load_dotenv(known.env_file, override=True)
    parser.set_defaults(
        catalog=_split(os.getenv("CATALOGS")) or ["postgres"],
        schemas=_split(os.getenv("SCHEMAS")),
        values=int(os.getenv("SAMPLE_VALUES") or 0),
        exclude_columns=_split(os.getenv("EXCLUDE_COLUMNS")),
    )
    return parser.parse_args()


def add_source_args(parser: argparse.ArgumentParser) -> None:
    """Arguments selecting what to read from Trino (shared with export_cypher.py)."""
    parser.add_argument("--env-file", help="Config file with the settings below (see config.example.env)")
    parser.add_argument("--catalog", nargs="+", help="Trino catalog(s) to read [CATALOGS]")
    parser.add_argument(
        "--schemas",
        nargs="*",
        help="Schemas to read, 'schema' or 'catalog.schema' (default: all non-system schemas) [SCHEMAS]",
    )
    parser.add_argument("--values", type=int, help="Distinct sample values per column, 0 = off [SAMPLE_VALUES]")
    parser.add_argument("--exclude-columns", nargs="*", help="Columns never sampled, e.g. PII [EXCLUDE_COLUMNS]")


def connect_trino() -> trino.dbapi.Connection:
    """One connection for every catalog: with OAuth2 browser login you sign in only once."""
    auth_kwargs = trino_connection_kwargs()
    # With authentication, Trino takes the user from the token; a different TRINO_USER is
    # treated as impersonation and must be allowed by Trino's access control.
    default_user = None if "auth" in auth_kwargs else "neocarta"
    return trino.dbapi.connect(
        host=os.getenv("TRINO_HOST") or "localhost",
        port=int(os.getenv("TRINO_PORT") or 8080),
        user=os.getenv("TRINO_USER") or default_user,
        **auth_kwargs,
    )


def select_schemas(conn: trino.dbapi.Connection, catalog: str, wanted: list[str]) -> list[str]:
    """Non-system schemas of ``catalog``, filtered by ``wanted`` ('schema' or 'catalog.schema')."""
    cur = conn.cursor()
    cur.execute(f'SELECT schema_name FROM "{catalog}".information_schema.schemata ORDER BY 1')
    available = [r[0] for r in cur.fetchall() if r[0] not in SYSTEM_SCHEMAS]
    if not wanted:
        return available
    return [s for s in available if s in wanted or f"{catalog}.{s}" in wanted]


def ingest_catalog(conn: trino.dbapi.Connection, driver, catalog: str, args: argparse.Namespace) -> list[str]:
    """Run the Trino schema connector for every selected schema of ``catalog``.

    ``driver`` is a real Neo4j driver, or any object with a compatible ``execute_query``
    (export_cypher.py passes one that records the Cypher instead of executing it).
    Returns the qualified names of the schemas processed.
    """
    cur = conn.cursor()
    cur.execute("SELECT connector_name FROM system.metadata.catalogs WHERE catalog_name = ?", [catalog])
    row = cur.fetchone()
    if row is None:
        raise SystemExit(f"Catalog {catalog!r} not found in Trino (check SHOW CATALOGS and your permissions)")
    (connector_name,) = row
    # PK/FK are only readable through pass-through on PostgreSQL-backed catalogs.
    key_source = "postgresql" if connector_name == "postgresql" else None

    schemas = select_schemas(conn, catalog, args.schemas)
    log.info("Catalog %s (%s) -> schemas %s, key_source=%s", catalog, connector_name, schemas, key_source)

    for schema in schemas:
        connector = TrinoSchemaConnector(
            conn,
            catalog,
            driver,
            database_name=os.getenv("NEO4J_DATABASE") or "neo4j",
            value_sample_limit=args.values,
            exclude_value_columns=args.exclude_columns,
            key_source=key_source,
        )
        connector.ingest(schema=schema)
    return [f"{catalog}.{s}" for s in schemas]


def ingest_all(conn: trino.dbapi.Connection, driver, args: argparse.Namespace) -> list[str]:
    """Ingest every catalog in ``args.catalog``; warns about --schemas entries that matched nothing."""
    done: list[str] = []
    for catalog in args.catalog:
        done += ingest_catalog(conn, driver, catalog, args)
    unmatched = [w for w in args.schemas if not any(d == w or d.endswith("." + w) for d in done)]
    if unmatched:
        log.warning("Schemas not found in catalogs %s: %s", args.catalog, unmatched)
    return done


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_source_args(parser)
    args = parse_args(parser)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    # Silence "index or constraint already exists" notices on re-runs.
    logging.getLogger("neo4j.notifications").setLevel(logging.WARNING)

    conn = connect_trino()
    driver = GraphDatabase.driver(
        os.getenv("NEO4J_URI") or "bolt://localhost:7687",
        auth=(os.getenv("NEO4J_USERNAME") or "neo4j", os.getenv("NEO4J_PASSWORD") or "password123"),
    )
    try:
        ingest_all(conn, driver, args)
    finally:
        driver.close()
        conn.close()


if __name__ == "__main__":
    main()
