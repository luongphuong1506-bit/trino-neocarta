"""Trino schema connector for neocarta."""

from __future__ import annotations

from typing import TYPE_CHECKING

from neocarta.connectors.utils.rdbms_schema_connector import RdbmsSchemaConnector
from neocarta.errors import ConfigError

from .extract import TrinoSchemaExtractor
from .transform import TrinoSchemaTransformer

if TYPE_CHECKING:
    from neo4j import Driver
    from trino.dbapi import Connection


class TrinoSchemaConnector(RdbmsSchemaConnector):
    """Ingest the schema metadata of one Trino catalog into the neocarta graph.

    Produces ``Database`` (= Trino catalog) / ``Schema`` / ``Table`` / ``Column`` /
    ``Value`` nodes and ``HAS_SCHEMA`` / ``HAS_TABLE`` / ``HAS_COLUMN`` / ``HAS_VALUE`` /
    ``REFERENCES`` edges. One schema per :meth:`ingest` call, like the other
    RDBMS connectors; node ids equal Trino's ``catalog.schema.table.column`` names
    (lower-cased).

    Parameters
    ----------
    connection : trino.dbapi.Connection
        An open Trino DB-API connection. Owned by the caller.
    catalog : str
        The Trino catalog to ingest.
    neo4j_driver : Driver
        Neo4j driver instance.
    database_name : str, default "neo4j"
        Target Neo4j database name.
    value_sample_limit : int, default 0
        Distinct sample values per column; ``0`` disables sampling.
    exclude_value_columns : list[str], optional
        Column names never sampled (e.g. PII).
    key_source : str, optional
        ``"postgresql"`` to read PK/FK constraints from a PostgreSQL-backed catalog.
    """

    _DISPLAY_NAME = "Trino"

    def __init__(
        self,
        connection: Connection,
        catalog: str,
        neo4j_driver: Driver,
        database_name: str = "neo4j",
        *,
        value_sample_limit: int = 0,
        exclude_value_columns: list[str] | None = None,
        key_source: str | None = None,
    ) -> None:
        """Initialize the Trino schema connector."""
        if neo4j_driver is None:
            raise ConfigError(
                "neo4j_driver is required for the Trino schema connector.",
                suggestion="Pass neo4j_driver=GraphDatabase.driver(...).",
            )
        self.catalog = catalog
        self._init_pipeline(
            connection=connection,
            neo4j_driver=neo4j_driver,
            database_name=database_name,
            extractor=TrinoSchemaExtractor(
                connection,
                catalog,
                value_sample_limit=value_sample_limit,
                exclude_value_columns=exclude_value_columns,
                key_source=key_source,
            ),
            transformer=TrinoSchemaTransformer(),
        )
