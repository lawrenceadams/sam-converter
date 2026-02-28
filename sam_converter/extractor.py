import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from sam_converter.converter import ConversionResult, TableRef

logger = logging.getLogger(__name__)


@dataclass
class CategorizedRefs:
    """Categorized table references for a model."""
    model_name: str
    output_path: Path
    refs: list[str] = field(default_factory=list)  # references to other models
    sources: list[TableRef] = field(default_factory=list)  # external source tables


def categorize_refs(results: list[ConversionResult]) -> list[CategorizedRefs]:
    """
    Categorize table references as either refs (internal models) or sources (external).

    A table is considered a ref if:
    1. Its table name matches another model in the project (case-insensitive)
    2. AND either:
       - The reference is unqualified (no database/schema), OR
       - The reference's database/schema matches how that model is referenced elsewhere

    This prevents false positives where an external table happens to share a name
    with a model but lives in a different database/schema.
    """
    model_names = {r.model_name.lower(): r.model_name for r in results}

    # Build a map of how each model is typically referenced (by db/schema)
    # If a model is referenced with qualification, record it
    model_qualifications: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for result in results:
        for table_ref in result.table_refs:
            table_lower = table_ref.table.lower()
            if table_lower in model_names:
                if table_ref.database or table_ref.schema:
                    model_qualifications[table_lower].add(
                        (table_ref.database.lower(), table_ref.schema.lower())
                    )

    categorized: list[CategorizedRefs] = []

    for result in results:
        refs: list[str] = []
        sources: list[TableRef] = []

        for table_ref in result.table_refs:
            table_lower = table_ref.table.lower()

            if table_lower in model_names and table_lower != result.model_name.lower():
                # Check if this is likely a ref or an external source with same name
                is_ref = _is_likely_ref(table_ref, table_lower, model_qualifications)

                if is_ref:
                    refs.append(model_names[table_lower])
                    logger.info(
                        f"Model '{result.model_name}' refs model '{model_names[table_lower]}'"
                    )
                else:
                    sources.append(table_ref)
                    logger.info(
                        f"Model '{result.model_name}' references external '{table_ref.database}.{table_ref.schema}.{table_ref.table}' "
                        f"(same name as model but different qualification)"
                    )
            else:
                sources.append(table_ref)

        categorized.append(CategorizedRefs(
            model_name=result.model_name,
            output_path=result.output_path,
            refs=refs,
            sources=sources,
        ))

    return categorized


def _is_likely_ref(
    table_ref: TableRef,
    table_lower: str,
    model_qualifications: dict[str, set[tuple[str, str]]],
) -> bool:
    """
    Determine if a table reference is likely a ref to another model.

    Returns True if:
    - The reference is unqualified (no db/schema), OR
    - All qualified references to this model use the SAME db/schema (consistent),
      and this reference matches that pattern

    If there are conflicting qualifications (same model name referenced from
    different db/schemas), qualified references are treated as sources to be safe.
    """
    # Unqualified references are assumed to be refs
    if not table_ref.database and not table_ref.schema:
        return True

    ref_qual = (table_ref.database.lower(), table_ref.schema.lower())
    known_quals = model_qualifications.get(table_lower, set())

    # If we have no qualification data, assume it's a ref
    if not known_quals:
        return True

    # If there are multiple different qualifications, we can't determine
    # which is the "real" model - treat qualified refs as sources
    if len(known_quals) > 1:
        return False

    # Single consistent qualification - check if this matches
    return ref_qual in known_quals


def extract_sources(categorized: list[CategorizedRefs], output_dir: Path) -> None:
    """
    Extract external table references into dbt sources YAML file.
    Only includes tables that are not refs to other models.
    """
    sources_by_db_schema: dict[tuple[str, str], set[str]] = defaultdict(set)

    for cat in categorized:
        for ref in cat.sources:
            sources_by_db_schema[(ref.database, ref.schema)].add(ref.table)

    sources_list = []
    for (db, schema), tables in sorted(sources_by_db_schema.items()):
        source_name = _build_source_name(db, schema)

        table_entries = []
        for t in sorted(tables):
            entry = {"name": t.lower() if _is_mixed_case(t) else t}
            if _is_mixed_case(t):
                entry["identifier"] = t
            table_entries.append(entry)

        source_entry = {
            "name": source_name,
            "tables": table_entries,
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


def extract_refs(categorized: list[CategorizedRefs], output_dir: Path) -> None:
    """
    Write model dependencies (refs) to a YAML file for reference.
    This helps understand the dependency graph between models.
    """
    refs_list = []

    for cat in categorized:
        if cat.refs:
            refs_list.append({
                "model": cat.model_name,
                "depends_on": sorted(cat.refs),
            })

    if not refs_list:
        logger.info("No inter-model refs found")
        return

    refs_yaml = {"version": 2, "model_refs": refs_list}

    refs_path = output_dir / "model_refs.yml"
    refs_path.parent.mkdir(parents=True, exist_ok=True)

    with open(refs_path, "w", encoding="utf-8") as f:
        yaml.dump(refs_yaml, f, default_flow_style=False, sort_keys=False)

    logger.info(f"Wrote model refs to {refs_path}")


def inject_dbt_macros(categorized: list[CategorizedRefs]) -> None:
    """
    Inject {{ ref() }} and {{ source() }} macros into the converted SQL files.
    Replaces table references with appropriate dbt macros.
    """
    for cat in categorized:
        output_path = cat.output_path
        if not output_path.exists():
            logger.warning(f"Output file not found: {output_path}")
            continue

        sql = output_path.read_text(encoding="utf-8")

        # Replace refs (references to other models)
        for ref_model in cat.refs:
            sql = _replace_table_with_ref(sql, ref_model)

        # Replace sources (external tables)
        for source in cat.sources:
            source_name = _build_source_name(source.database, source.schema)
            table_name = source.table.lower() if _is_mixed_case(source.table) else source.table
            sql = _replace_table_with_source(sql, source, source_name, table_name)

        output_path.write_text(sql, encoding="utf-8")
        logger.info(f"Injected dbt macros into {output_path}")


def _replace_table_with_ref(sql: str, model_name: str) -> str:
    """Replace table reference with {{ ref('model_name') }}."""
    ref_macro = f"{{{{ ref('{model_name}') }}}}"

    # Match the table name with optional alias, case-insensitive
    # Handles: table_name, table_name AS alias, table_name alias
    pattern = rf'\b{re.escape(model_name)}\b(?!\s*\()'

    return re.sub(pattern, ref_macro, sql, flags=re.IGNORECASE)


def _replace_table_with_source(sql: str, table_ref: TableRef, source_name: str, table_name: str) -> str:
    """Replace table reference with {{ source('source_name', 'table_name') }}."""
    source_macro = f"{{{{ source('{source_name}', '{table_name}') }}}}"

    # Build patterns for different qualification levels
    patterns = []

    # Fully qualified: database.schema.table
    if table_ref.database and table_ref.schema:
        pattern = rf'\b{re.escape(table_ref.database)}\.{re.escape(table_ref.schema)}\.{re.escape(table_ref.table)}\b'
        patterns.append(pattern)

    # Schema qualified: schema.table
    if table_ref.schema:
        pattern = rf'\b{re.escape(table_ref.schema)}\.{re.escape(table_ref.table)}\b'
        patterns.append(pattern)

    # Unqualified: just table (be careful not to replace parts of other identifiers)
    pattern = rf'\b{re.escape(table_ref.table)}\b(?!\s*\()'
    patterns.append(pattern)

    # Try patterns from most specific to least specific
    for pattern in patterns:
        if re.search(pattern, sql, flags=re.IGNORECASE):
            sql = re.sub(pattern, source_macro, sql, flags=re.IGNORECASE)
            break

    return sql


def _is_mixed_case(name: str) -> bool:
    """Check if a name has mixed case (not all upper, not all lower)."""
    return name != name.lower() and name != name.upper()


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
