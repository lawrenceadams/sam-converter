import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

import sqlglot
from sqlglot import exp

logger = logging.getLogger(__name__)


@dataclass
class TableRef:
    database: str
    schema: str
    table: str

    def __hash__(self):
        return hash((self.database, self.schema, self.table))

    def __eq__(self, other):
        if not isinstance(other, TableRef):
            return False
        return (self.database, self.schema, self.table) == (other.database, other.schema, other.table)


@dataclass
class ConversionResult:
    model_name: str
    output_path: Path
    table_refs: list[TableRef] = field(default_factory=list)


def transpile_tsql_to_snowflake(sql: str) -> str:
    """Convert T-SQL to Snowflake SQL using sqlglot.

    Uses identify=True to quote all identifiers, preserving mixed case
    column names in Snowflake (which uppercases unquoted identifiers).
    """
    try:
        return sqlglot.transpile(
            sql, read="tsql", write="snowflake", pretty=True, identify=True
        )[0]
    except sqlglot.errors.ParseError as e:
        logger.error(f"Failed to parse SQL: {e}")
        raise


def extract_table_references(sql: str) -> list[TableRef]:
    """
    Extract all table references from SQL.
    Returns a list of TableRef objects with database, schema, and table names.
    Excludes CTE names since they are defined within the query, not external tables.
    """
    tables: list[TableRef] = []
    try:
        parsed = sqlglot.parse(sql, read="tsql")
    except sqlglot.errors.ParseError as e:
        logger.error(f"Failed to parse SQL for table extraction: {e}")
        return tables

    for statement in parsed:
        if statement is None:
            continue

        # Collect CTE names to exclude them from table references
        cte_names: set[str] = set()
        for cte in statement.find_all(exp.CTE):
            if cte.alias:
                cte_names.add(cte.alias.lower())

        for table in statement.find_all(exp.Table):
            table_name = table.name
            schema_name = table.db
            database_name = table.catalog

            if not table_name:
                continue

            # Skip CTEs - they're defined in the query, not external tables
            if table_name.lower() in cte_names:
                continue

            table_ref = TableRef(
                database=database_name or "",
                schema=schema_name or "",
                table=table_name,
            )

            if table_ref not in tables:
                tables.append(table_ref)

            if not database_name or not schema_name:
                logger.warning(
                    f"Incomplete table reference: {database_name or '?'}.{schema_name or '?'}.{table_name}"
                )

    return tables


def strip_base_suffix(name: str) -> str:
    """
    Strip the 'BASE' suffix from a name if present (case-insensitive).

    This handles legacy naming conventions where tables/views
    were suffixed with 'BASE' or 'Base' (e.g., 'PopulationSpellBASE' -> 'PopulationSpell').
    """
    if name.upper().endswith("BASE"):
        return name[:-4]
    return name


def to_snake_case(name: str) -> str:
    """Convert MixedCase/PascalCase to snake_case.

    Examples:
        PatientData -> patient_data
        PopulationSpell -> population_spell
        HTTPServer -> http_server
        already_snake -> already_snake
    """
    # Insert underscore before uppercase letters, then lowercase
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def extract_model_name(filename_stem: str) -> str:
    """
    Extract the model name from a filename stem.

    Handles filenames in the format 'db.schema.TableNameBASE' by:
    1. Taking only the last part (the actual table name)
    2. Stripping the 'BASE' suffix if present
    """
    parts = filename_stem.split(".")
    name = parts[-1]
    return strip_base_suffix(name)


def convert_file(input_path: Path, output_path: Path) -> ConversionResult:
    """
    Convert a single T-SQL file to Snowflake SQL.
    Returns ConversionResult with model name and table references.
    """
    logger.info(f"Converting {input_path}")

    model_name = extract_model_name(input_path.stem)
    sql = input_path.read_text(encoding="utf-8")

    table_refs = extract_table_references(sql)

    converted_sql = transpile_tsql_to_snowflake(sql)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(converted_sql, encoding="utf-8")

    logger.info(f"Wrote converted SQL to {output_path}")

    return ConversionResult(
        model_name=model_name,
        output_path=output_path,
        table_refs=table_refs,
    )


def convert_directory(input_dir: Path, output_dir: Path) -> list[ConversionResult]:
    """
    Convert all SQL files in a directory.
    Returns a list of ConversionResult objects.
    """
    results: list[ConversionResult] = []

    sql_files = list(input_dir.rglob("*.sql"))

    if not sql_files:
        logger.warning(f"No SQL files found in {input_dir}")
        return results

    for input_file in sql_files:
        model_name = extract_model_name(input_file.stem)
        output_file = output_dir / f"{model_name}.sql"

        try:
            result = convert_file(input_file, output_file)
            results.append(result)
        except Exception as e:
            logger.error(f"Failed to convert {input_file}: {e}")
            continue

    return results
