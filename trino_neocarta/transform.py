"""Transform Trino schema metadata into neocarta graph nodes and relationships."""

from __future__ import annotations

from typing import TYPE_CHECKING

from neocarta.connectors.utils.generate_id import generate_database_id
from neocarta.connectors.utils.rdbms_schema_transform import RdbmsSchemaTransformer
from neocarta.data_model.schema.rdbms import Database

if TYPE_CHECKING:
    import pandas as pd


class TrinoSchemaTransformer(RdbmsSchemaTransformer):
    """Maps Trino metadata frames onto the core RDBMS data model.

    Every stage is inherited; only ``:Database`` differs: ``platform`` is ``TRINO`` and
    ``service`` is the connector backing the catalog (e.g. ``POSTGRESQL``).
    """

    _PLATFORM = "TRINO"
    _SERVICE = "TRINO"
    _DATABASE_COLUMN = "catalog"

    def transform_to_database_nodes(self, database_info: pd.DataFrame, cache: bool = True) -> list[Database]:
        """Transform catalog information into ``:Database`` nodes."""
        database_nodes = [
            Database(
                id=generate_database_id(row.catalog),
                name=row.catalog,
                description=None,
                platform=self._PLATFORM,
                service=getattr(row, "connector_name", None) or self._SERVICE,
            )
            for row in database_info.itertuples()
        ]
        if cache:
            self._node_cache["database_nodes"] = database_nodes
        return database_nodes
