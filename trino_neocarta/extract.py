"""Trino schema extractor for neocarta.

Reads structural metadata for one Trino catalog through the ``trino`` DB-API
connection and returns pandas frames whose column names match what
:class:`neocarta.connectors.utils.rdbms_schema_transform.RdbmsSchemaTransformer`
consumes.

Metadata sources inside Trino:

* schemas  -> ``<catalog>.information_schema.schemata``
* tables   -> ``<catalog>.information_schema.tables`` + ``system.metadata.table_comments``
* columns  -> ``system.jdbc.columns`` (``information_schema.columns`` has no comment)
* PK / FK  -> Trino exposes no constraint metadata. For PostgreSQL-backed catalogs it
  is read from the remote database via the ``system.query`` pass-through table
  function (``key_source="postgresql"``).
* values   -> ``slice(array_agg(DISTINCT col), 1, n)`` per table (off by default).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import pandas as pd
from neocarta.connectors.utils.generate_id import generate_column_id, generate_value_id
from neocarta.errors import ConfigError, StateError

if TYPE_CHECKING:
    from trino.dbapi import Connection

logger = logging.getLogger(__name__)

# Trino types that cannot be DISTINCT-aggregated / are not useful as sample values.
_NON_SAMPLEABLE_TYPES = (
    "array",
    "map",
    "row",
    "varbinary",
    "json",
    "hyperloglog",
    "qdigest",
    "tdigest",
    "setdigest",
    "geometry",
    "sphericalgeography",
)

_VALUE_COLUMNS = ["column_name", "unique_value", "column_id", "value_id"]
_REFERENCE_COLUMNS = [
    "constraint_type",
    "table_catalog",
    "table_schema",
    "table_name",
    "column_name",
    "ordinal_position",
    "referenced_catalog",
    "referenced_schema",
    "referenced_table",
    "referenced_column",
]

#: Remote databases whose ``information_schema`` constraint views can be read through
#: ``<catalog>.system.query``. Only PostgreSQL is implemented/tested for now.
SUPPORTED_KEY_SOURCES = ("postgresql",)


def quote_identifier(identifier: str) -> str:
    """Double-quote a Trino identifier, escaping embedded double quotes."""
    return '"' + identifier.replace('"', '""') + '"'


def _sql_literal(value: str) -> str:
    """Render a string as a single-quoted SQL literal."""
    return "'" + value.replace("'", "''") + "'"


class TrinoSchemaExtractor:
    """Extractor for the schema metadata of one Trino catalog.

    Parameters
    ----------
    connection : trino.dbapi.Connection
        An open Trino DB-API connection. Owned by the caller.
    catalog : str
        The Trino catalog to read (becomes the ``:Database`` node).
    value_sample_limit : int, default 0
        Distinct sample values per column. ``0`` disables sampling (no table-data reads).
    exclude_value_columns : list[str], optional
        Column names never sampled (e.g. PII such as ``email``).
    key_source : str, optional
        Remote database type used to read PK/FK constraints via pass-through
        (``"postgresql"``). ``None`` skips keys, so no ``REFERENCES`` edges.
    """

    def __init__(
        self,
        connection: Connection,
        catalog: str,
        *,
        value_sample_limit: int = 0,
        exclude_value_columns: list[str] | None = None,
        key_source: str | None = None,
    ) -> None:
        """Initialize the extractor."""
        if connection is None:
            raise ConfigError(
                "connection is required for the Trino schema extractor.",
                suggestion="Pass connection=trino.dbapi.connect(...).",
            )
        if not catalog:
            raise ConfigError(
                "catalog is required for the Trino schema extractor.",
                suggestion="Pass catalog=... (the Trino catalog name).",
            )
        if not isinstance(value_sample_limit, int) or isinstance(value_sample_limit, bool) or value_sample_limit < 0:
            raise ConfigError(
                "value_sample_limit must be a non-negative integer.",
                suggestion="Pass value_sample_limit=0 to disable value sampling.",
            )
        if key_source is not None and key_source not in SUPPORTED_KEY_SOURCES:
            raise ConfigError(
                f"Unsupported key_source {key_source!r}.",
                suggestion=f"Use one of {SUPPORTED_KEY_SOURCES} or None.",
            )
        self.connection = connection
        self.catalog = catalog
        self.value_sample_limit = value_sample_limit
        self.exclude_value_columns = {c.lower() for c in (exclude_value_columns or [])}
        self.key_source = key_source
        self._cache: dict[str, pd.DataFrame] = {}

    # ------------------------------------------------------------------ accessors
    @property
    def database_info(self) -> pd.DataFrame:
        """Get the database (catalog) information."""
        return self._cache.get("database_info", pd.DataFrame())

    @property
    def schema_info(self) -> pd.DataFrame:
        """Get the schema information."""
        return self._cache.get("schema_info", pd.DataFrame())

    @property
    def table_info(self) -> pd.DataFrame:
        """Get the table information."""
        return self._cache.get("table_info", pd.DataFrame())

    @property
    def column_info(self) -> pd.DataFrame:
        """Get the column information."""
        return self._cache.get("column_info", pd.DataFrame())

    @property
    def column_references_info(self) -> pd.DataFrame:
        """Get the column references (foreign-key) information."""
        return self._cache.get("column_references_info", pd.DataFrame(columns=_REFERENCE_COLUMNS))

    @property
    def column_unique_values(self) -> pd.DataFrame:
        """Get the column unique values."""
        return self._cache.get("column_unique_values", pd.DataFrame(columns=_VALUE_COLUMNS))

    # ------------------------------------------------------------------ helpers
    def _run_query(self, sql: str, params: list[Any] | None = None) -> pd.DataFrame:
        """Execute a read query and return the result as a DataFrame."""
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            columns = [d[0] for d in cursor.description]
            return pd.DataFrame(rows, columns=columns)
        finally:
            cursor.close()

    def _info_schema(self) -> str:
        return f"{quote_identifier(self.catalog)}.information_schema"

    @staticmethod
    def _blank_to_none(series: pd.Series) -> pd.Series:
        """Trino reports missing comments as NULL or ''; normalise both to None."""
        return series.where(series.notna() & (series.astype(str).str.strip() != ""), None)

    def _passthrough(self, remote_sql: str) -> pd.DataFrame:
        """Run ``remote_sql`` on the catalog's remote database via ``system.query``."""
        return self._run_query(
            f"SELECT * FROM TABLE({quote_identifier(self.catalog)}.system.query("
            f"query => {_sql_literal(remote_sql)}))"
        )

    # ------------------------------------------------------------------ stages
    def extract_database_info(self, cache: bool = True) -> pd.DataFrame:
        """Build the database (catalog) frame, incl. the backing connector name."""
        df = self._run_query(
            "SELECT catalog_name AS catalog, connector_name FROM system.metadata.catalogs "
            "WHERE catalog_name = ?",
            [self.catalog],
        )
        if df.empty:
            raise ConfigError(
                f"Catalog {self.catalog!r} was not found in Trino.",
                suggestion="Check SHOW CATALOGS.",
            )
        if cache:
            self._cache["database_info"] = df
        return df

    def extract_schema_info(self, schema: str, cache: bool = True) -> pd.DataFrame:
        """Extract one schema. Trino has no schema comments, so description is None."""
        df = self._run_query(
            f"SELECT catalog_name, schema_name FROM {self._info_schema()}.schemata "
            "WHERE schema_name = ?",
            [schema],
        )
        if df.empty:
            raise ConfigError(
                f"Schema {schema!r} was not found in catalog {self.catalog!r}.",
                suggestion=f"Check SHOW SCHEMAS FROM {self.catalog}.",
            )
        df["description"] = None
        if cache:
            self._cache["schema_info"] = df
        return df

    def extract_table_info(self, schema: str, cache: bool = True) -> pd.DataFrame:
        """Extract tables and views of one schema with their comments."""
        df = self._run_query(
            f"""
SELECT
    t.table_catalog,
    t.table_schema,
    t.table_name,
    t.table_type,
    c.comment AS description
FROM {self._info_schema()}.tables t
LEFT JOIN system.metadata.table_comments c
    ON c.catalog_name = t.table_catalog
    AND c.schema_name = t.table_schema
    AND c.table_name = t.table_name
WHERE t.table_schema = ?
ORDER BY t.table_name
""",
            [schema],
        )
        df["description"] = self._blank_to_none(df["description"])
        if cache:
            self._cache["table_info"] = df
        return df

    def extract_column_info(self, schema: str, cache: bool = True) -> pd.DataFrame:
        """Extract columns of one schema (with comments) plus PK/FK flags."""
        df = self._run_query(
            """
SELECT
    table_cat AS table_catalog,
    table_schem AS table_schema,
    table_name,
    column_name,
    is_nullable,
    type_name AS data_type,
    remarks AS description
FROM system.jdbc.columns
WHERE table_cat = ?
    AND table_schem = ?
ORDER BY table_name, ordinal_position
""",
            [self.catalog, schema],
        )
        # Only an explicit 'NO' means NOT NULL; anything else defaults to nullable.
        df["is_nullable"] = df["is_nullable"].astype(str).str.strip().str.upper().ne("NO")
        df["description"] = self._blank_to_none(df["description"])

        pk_columns, fk_columns = self._extract_key_columns(schema)
        pairs = list(zip(df["table_name"], df["column_name"], strict=True))
        df["is_primary_key"] = pd.Series([p in pk_columns for p in pairs], index=df.index, dtype=bool)
        df["is_foreign_key"] = pd.Series([p in fk_columns for p in pairs], index=df.index, dtype=bool)

        if cache:
            self._cache["column_info"] = df
        return df

    def _extract_key_columns(self, schema: str) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
        """Return (table, column) pairs that are PKs and FKs in the remote database."""
        if self.key_source is None:
            return set(), set()
        df = self._passthrough(
            f"""
SELECT kcu.table_name, kcu.column_name, tc.constraint_type
FROM information_schema.key_column_usage kcu
JOIN information_schema.table_constraints tc
    ON kcu.constraint_schema = tc.constraint_schema
    AND kcu.constraint_name = tc.constraint_name
WHERE kcu.table_schema = {_sql_literal(schema)}
    AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')
"""
        )
        pk = {(r.table_name, r.column_name) for r in df.itertuples() if r.constraint_type == "PRIMARY KEY"}
        fk = {(r.table_name, r.column_name) for r in df.itertuples() if r.constraint_type == "FOREIGN KEY"}
        return pk, fk

    def extract_column_references_info(self, schema: str, cache: bool = True) -> pd.DataFrame:
        """Extract foreign-key column pairs for one schema (remote pass-through)."""
        if self.key_source is None:
            df = pd.DataFrame(columns=_REFERENCE_COLUMNS)
        else:
            df = self._passthrough(
                f"""
SELECT
    'FOREIGN KEY' AS constraint_type,
    fk.table_schema,
    fk.table_name,
    fk.column_name,
    fk.ordinal_position,
    pk.table_schema AS referenced_schema,
    pk.table_name AS referenced_table,
    pk.column_name AS referenced_column
FROM information_schema.referential_constraints rc
JOIN information_schema.key_column_usage fk
    ON fk.constraint_schema = rc.constraint_schema
    AND fk.constraint_name = rc.constraint_name
JOIN information_schema.key_column_usage pk
    ON pk.constraint_schema = rc.unique_constraint_schema
    AND pk.constraint_name = rc.unique_constraint_name
    AND pk.ordinal_position = fk.position_in_unique_constraint
WHERE fk.table_schema = {_sql_literal(schema)}
ORDER BY fk.table_name, fk.constraint_name, fk.ordinal_position
"""
            )
            # The remote database name differs from the Trino catalog name; ids must use
            # the Trino catalog so they line up with the rest of the graph.
            df["table_catalog"] = self.catalog
            df["referenced_catalog"] = self.catalog
            df = df[_REFERENCE_COLUMNS]
        if cache:
            self._cache["column_references_info"] = df
        return df

    def extract_column_unique_values_for_table(
        self, table_name: str, schema: str, column_info: pd.DataFrame
    ) -> pd.DataFrame:
        """Sample up to ``value_sample_limit`` distinct values per column of one table."""
        limit = self.value_sample_limit
        cols = column_info[column_info["table_name"] == table_name]
        select_clauses = []
        for row in cols.itertuples():
            if row.column_name.lower() in self.exclude_value_columns:
                continue
            if str(row.data_type).lower().startswith(_NON_SAMPLEABLE_TYPES):
                continue
            q = quote_identifier(row.column_name)
            # ORDER BY makes the sample deterministic; otherwise every re-run picks different
            # values and the stale :Value nodes from earlier runs accumulate in the graph.
            select_clauses.append(f"slice(array_agg(DISTINCT {q} ORDER BY {q}), 1, {limit}) AS {q}")
        if not select_clauses:
            return pd.DataFrame(columns=_VALUE_COLUMNS)

        relation = ".".join(quote_identifier(p) for p in (self.catalog, schema, table_name))
        df = self._run_query(f"SELECT {', '.join(select_clauses)} FROM {relation}")

        result = df.melt(var_name="column_name", value_name="unique_value")
        result = result.explode("unique_value").dropna().reset_index(drop=True)
        if result.empty:
            return pd.DataFrame(columns=_VALUE_COLUMNS)
        result["unique_value"] = result["unique_value"].astype(str)
        result["column_id"] = result["column_name"].apply(
            lambda col: generate_column_id(self.catalog, schema, table_name, col)
        )
        result["value_id"] = result.apply(
            lambda r: generate_value_id(self.catalog, schema, table_name, r["column_name"], r["unique_value"]),
            axis=1,
        )
        return result

    def extract_column_unique_values_for_all_tables(self, schema: str, cache: bool = True) -> pd.DataFrame:
        """Sample distinct values for every table in the schema (no-op when limit is 0)."""
        if "column_info" not in self._cache or "table_info" not in self._cache:
            raise StateError(
                "Table and column information are required before sampling values.",
                suggestion="Run connector.extract(schema=...), which orders the stages.",
            )
        if self.value_sample_limit <= 0:
            values = pd.DataFrame(columns=_VALUE_COLUMNS)
        else:
            per_table = [
                self.extract_column_unique_values_for_table(t, schema, self.column_info)
                for t in self.table_info["table_name"].unique()
            ]
            values = pd.concat(per_table, ignore_index=True) if per_table else pd.DataFrame(columns=_VALUE_COLUMNS)
            values = values.drop_duplicates(subset="value_id", ignore_index=True)
        if cache:
            self._cache["column_unique_values"] = values
        return values
