import logging
from collections import defaultdict
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


def extract_sources(table_refs: dict[str, list[dict]], output_dir: Path) -> None:
    """
    Extract table references into dbt sources YAML files.
    Groups tables by database and schema, writes one sources.yml per output model directory.
    """
    sources_by_db_schema: dict[tuple[str, str], set[str]] = defaultdict(set)

    for refs in table_refs.values():
        for ref in refs:
            db = ref["database"]
            schema = ref["schema"]
            table = ref["table"]
            sources_by_db_schema[(db, schema)].add(table)

    sources_list = []
    for (db, schema), tables in sorted(sources_by_db_schema.items()):
        source_name = _build_source_name(db, schema)

        source_entry = {
            "name": source_name,
            "tables": [{"name": t} for t in sorted(tables)],
        }

        if db:
            source_entry["database"] = db
        if schema:
            source_entry["schema"] = schema

        sources_list.append(source_entry)

    sources_yaml = {"version": 2, "sources": sources_list}

    sources_path = output_dir / "sources.yml"
    sources_path.parent.mkdir(parents=True, exist_ok=True)

    with open(sources_path, "w", encoding="utf-8") as f:
        yaml.dump(sources_yaml, f, default_flow_style=False, sort_keys=False)

    logger.info(f"Wrote sources to {sources_path}")

    _log_incomplete_sources(sources_by_db_schema)


def _build_source_name(db: str, schema: str) -> str:
    """Build a source name from database and schema."""
    parts = [p for p in [db, schema] if p]
    if parts:
        return "_".join(parts).lower()
    return "unknown_source"


def _log_incomplete_sources(sources: dict[tuple[str, str], set[str]]) -> None:
    """Log warning for sources with missing database or schema."""
    for (db, schema), tables in sources.items():
        if not db or not schema:
            location = f"{db or '?'}.{schema or '?'}"
            logger.warning(
                f"Incomplete source reference ({location}): {sorted(tables)}"
            )
