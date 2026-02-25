import logging
from pathlib import Path

import sqlglot
from sqlglot import exp

logger = logging.getLogger(__name__)


def transpile_tsql_to_snowflake(sql: str) -> str:
    """Convert T-SQL to Snowflake SQL using sqlglot."""
    try:
        return sqlglot.transpile(sql, read="tsql", write="snowflake", pretty=True)[0]
    except sqlglot.errors.ParseError as e:
        logger.error(f"Failed to parse SQL: {e}")
        raise


def extract_table_references(sql: str) -> list[dict]:
    """
    Extract all table references from SQL.
    Returns a list of dicts with database, schema, and table names.
    """
    tables = []
    try:
        parsed = sqlglot.parse(sql, read="tsql")
    except sqlglot.errors.ParseError as e:
        logger.error(f"Failed to parse SQL for table extraction: {e}")
        return tables

    for statement in parsed:
        if statement is None:
            continue
        for table in statement.find_all(exp.Table):
            table_name = table.name
            schema_name = table.db
            database_name = table.catalog

            if not table_name:
                continue

            table_ref = {
                "database": database_name or "",
                "schema": schema_name or "",
                "table": table_name,
            }

            if table_ref not in tables:
                tables.append(table_ref)

            if not database_name or not schema_name:
                logger.warning(
                    f"Incomplete table reference: {database_name or '?'}.{schema_name or '?'}.{table_name}"
                )

    return tables


def convert_file(input_path: Path, output_path: Path) -> list[dict]:
    """
    Convert a single T-SQL file to Snowflake SQL.
    Returns list of table references found in the file.
    """
    logger.info(f"Converting {input_path}")

    sql = input_path.read_text(encoding="utf-8")

    table_refs = extract_table_references(sql)

    converted_sql = transpile_tsql_to_snowflake(sql)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(converted_sql, encoding="utf-8")

    logger.info(f"Wrote converted SQL to {output_path}")

    return table_refs


def convert_directory(input_dir: Path, output_dir: Path) -> dict[str, list[dict]]:
    """
    Convert all SQL files in a directory.
    Returns a dict mapping output file paths to their table references.
    """
    all_refs: dict[str, list[dict]] = {}

    sql_files = list(input_dir.rglob("*.sql"))

    if not sql_files:
        logger.warning(f"No SQL files found in {input_dir}")
        return all_refs

    for input_file in sql_files:
        relative_path = input_file.relative_to(input_dir)
        output_file = output_dir / relative_path.stem / f"{relative_path.stem}.sql"

        try:
            refs = convert_file(input_file, output_file)
            all_refs[str(output_file)] = refs
        except Exception as e:
            logger.error(f"Failed to convert {input_file}: {e}")
            continue

    return all_refs
