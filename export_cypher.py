"""Export Trino metadata as a Cypher script instead of writing to Neo4j directly.

For environments where the Neo4j server cannot run Python: run this script on a
machine that can reach Trino, copy the generated .cypher file, and execute it
against Neo4j (DBeaver "Execute script", Neo4j Browser, or `cypher-shell -f`).

The script is produced by running the real neocarta pipeline against a recording
driver, so it contains exactly the statements a direct ingest would execute
(constraints, indexes, UNWIND ... MERGE batches), with parameters inlined.

Settings come from a config file (--env-file, see config.example.env) or flags;
all selected catalogs share one Trino connection (one OAuth2 login) and go into
one output file.

Usage:
    .venv/Scripts/python export_cypher.py --env-file prod.env
    .venv/Scripts/python export_cypher.py --catalog postgres iceberg -o out/metadata.cypher
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import math
import os
import re
from pathlib import Path
from typing import Any

from ingest_trino import add_source_args, connect_trino, ingest_all, parse_args

log = logging.getLogger("export_cypher")

_PARAM_RE = re.compile(r"\$(\w+)")
# Escapes inside single-quoted Cypher strings. Note: \uXXXX cannot be used for the quote
# itself - Cypher decodes unicode escapes before tokenizing, so ' would end the string.
# Control characters are escaped so every statement stays on predictable lines.
_STRING_ESCAPES = {
    "\\": "\\\\",
    "'": "\\'",
    "\n": "\\n",
    "\r": "\\r",
    "\t": "\\t",
}


def cypher_literal(value: Any) -> str:
    """Render a Python value as a Cypher literal."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return "null" if math.isnan(value) or math.isinf(value) else repr(value)
    if isinstance(value, str):
        return "'" + "".join(_STRING_ESCAPES.get(ch, ch) for ch in value) + "'"
    if isinstance(value, dict):
        return "{" + ", ".join(f"`{k}`: {cypher_literal(v)}" for k, v in value.items()) + "}"
    if isinstance(value, (list, tuple, set)):
        return "[" + ", ".join(cypher_literal(v) for v in value) + "]"
    return cypher_literal(str(value))


class _Counters:
    """Stand-in for neo4j SummaryCounters (only read for logging)."""

    nodes_created = 0
    relationships_created = 0
    properties_set = 0


class _Summary:
    counters = _Counters()


class _DateTime:
    def to_native(self) -> dt.datetime:
        return dt.datetime.now(dt.timezone.utc)


class RecordingDriver:
    """Duck-typed replacement for ``neo4j.Driver`` that records Cypher instead of running it.

    neocarta only calls ``execute_query``. Reads (the edition check) are answered
    locally; every write is rendered with its parameters inlined and appended to
    ``statements``. ``UNWIND $rows`` writes are split into batches of ``batch_size``.
    """

    def __init__(self, *, edition: str = "community", batch_size: int = 500) -> None:
        self.edition = edition
        self.batch_size = batch_size
        self.statements: list[str] = []

    def execute_query(self, query_: str, parameters_: dict | None = None, *, result_transformer_=None, **_: Any):
        params = parameters_ or {}
        if "dbms.components()" in query_:
            # Edition probe: decides UNIQUE (community) vs NODE KEY (enterprise) constraints.
            return [{"name": "Neo4j Kernel", "versions": [], "edition": self.edition}]

        rows = params.get("rows")
        if isinstance(rows, list) and len(rows) > self.batch_size:
            for i in range(0, len(rows), self.batch_size):
                self._record(query_, {**params, "rows": rows[i : i + self.batch_size]})
        else:
            self._record(query_, params)

        if "RETURN n.initial_version" in query_:  # upsert_neocarta_graph_node reads one record back
            version = params.get("version")
            record = {"initial_version": version, "latest_version": version,
                      "create_date": _DateTime(), "last_updated": _DateTime()}
            return [record], _Summary(), []
        return [], _Summary(), []

    def _record(self, query: str, params: dict) -> None:
        if isinstance(params.get("rows"), list) and not params["rows"]:
            return  # nothing to write
        # some neocarta constraint strings already end with ';' - the writer adds its own
        rendered = _PARAM_RE.sub(lambda m: self._render_param(m, params), query.strip().rstrip(";").rstrip())
        if rendered.startswith("UNWIND [") and isinstance(params.get("rows"), list):
            # one row per line keeps big batches readable
            rows = ",\n  ".join(cypher_literal(r) for r in params["rows"])
            rendered = rendered.replace(cypher_literal(params["rows"]), f"[\n  {rows}\n]", 1)
        self.statements.append(rendered)

    @staticmethod
    def _render_param(match: re.Match, params: dict) -> str:
        name = match.group(1)
        if name not in params:
            raise KeyError(f"Cypher parameter ${name} has no value")
        return cypher_literal(params[name])

    def close(self) -> None:
        pass


def dedupe_schema_statements(statements: list[str]) -> list[str]:
    """Drop repeated identical statements (constraints/indexes are re-issued per schema)."""
    seen: set[str] = set()
    out = []
    for s in statements:
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_source_args(parser)
    parser.add_argument("-o", "--output", help="Path of the .cypher file to write [OUTPUT]")
    parser.add_argument("--batch-size", type=int, help="Rows per UNWIND statement, default 500 [BATCH_SIZE]")
    parser.add_argument(
        "--neo4j-edition",
        choices=["community", "enterprise"],
        help="community (default) -> UNIQUE constraints, work on both editions; "
        "enterprise -> NODE KEY constraints [NEO4J_EDITION]",
    )
    args = parse_args(parser)
    args.output = args.output or os.getenv("OUTPUT") or "out/metadata.cypher"
    args.batch_size = args.batch_size or int(os.getenv("BATCH_SIZE") or 500)
    args.neo4j_edition = args.neo4j_edition or os.getenv("NEO4J_EDITION") or "community"

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    driver = RecordingDriver(edition=args.neo4j_edition, batch_size=args.batch_size)
    conn = connect_trino()
    try:
        schemas = ingest_all(conn, driver, args)
    finally:
        conn.close()
    if not schemas:
        raise SystemExit("No schema matched - nothing exported (check CATALOGS / SCHEMAS)")

    statements = dedupe_schema_statements(driver.statements)
    header = (
        f"// neocarta metadata export - {len(schemas)} schema(s): {', '.join(schemas)}\n"
        f"// generated {dt.datetime.now().isoformat(timespec='seconds')} by export_cypher.py\n"
        f"// {len(statements)} statements, run as a script (statements end with semicolons)\n"
    ).replace(";", ",")  # a ';' inside a comment would split the first statement
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(header + ";\n\n".join(statements) + ";\n", encoding="utf-8")
    log.info("Wrote %d statements to %s", len(statements), out)


if __name__ == "__main__":
    main()
