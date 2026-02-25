import pytest
from pathlib import Path

from sam_converter.converter import (
    transpile_tsql_to_snowflake,
    extract_table_references,
    convert_file,
    convert_directory,
)


class TestTranspileTsqlToSnowflake:
    def test_isnull_to_coalesce(self):
        sql = "SELECT ISNULL(col, 'default') FROM t"
        result = transpile_tsql_to_snowflake(sql)
        assert "COALESCE" in result
        assert "ISNULL" not in result

    def test_getdate_to_current_timestamp(self):
        sql = "SELECT GETDATE() AS now"
        result = transpile_tsql_to_snowflake(sql)
        assert "CURRENT_TIMESTAMP()" in result
        assert "GETDATE" not in result

    def test_dateadd(self):
        sql = "SELECT DATEADD(DAY, 1, col) FROM t"
        result = transpile_tsql_to_snowflake(sql)
        assert "DATEADD" in result

    def test_top_to_limit(self):
        sql = "SELECT TOP 10 * FROM t"
        result = transpile_tsql_to_snowflake(sql)
        assert "LIMIT 10" in result
        assert "TOP" not in result

    def test_square_brackets_removed(self):
        sql = "SELECT [column] FROM [schema].[table]"
        result = transpile_tsql_to_snowflake(sql)
        assert "[" not in result
        assert "]" not in result

    def test_convert_to_cast(self):
        sql = "SELECT CONVERT(VARCHAR(50), col) FROM t"
        result = transpile_tsql_to_snowflake(sql)
        assert "CAST" in result or "TRY_CAST" in result


class TestExtractTableReferences:
    def test_fully_qualified_table(self):
        sql = "SELECT * FROM database.schema.table"
        refs = extract_table_references(sql)
        assert len(refs) == 1
        assert refs[0] == {"database": "database", "schema": "schema", "table": "table"}

    def test_schema_qualified_table(self):
        sql = "SELECT * FROM schema.table"
        refs = extract_table_references(sql)
        assert len(refs) == 1
        assert refs[0] == {"database": "", "schema": "schema", "table": "table"}

    def test_unqualified_table(self):
        sql = "SELECT * FROM my_table"
        refs = extract_table_references(sql)
        assert len(refs) == 1
        assert refs[0] == {"database": "", "schema": "", "table": "my_table"}

    def test_multiple_tables_from_join(self):
        sql = """
        SELECT * FROM db1.schema1.table1 t1
        JOIN db2.schema2.table2 t2 ON t1.id = t2.id
        LEFT JOIN schema3.table3 t3 ON t1.id = t3.id
        """
        refs = extract_table_references(sql)
        assert len(refs) == 3

        tables = {r["table"] for r in refs}
        assert tables == {"table1", "table2", "table3"}

    def test_subquery_tables(self):
        sql = """
        SELECT * FROM (
            SELECT * FROM db.schema.inner_table
        ) sub
        JOIN db.schema.outer_table ON sub.id = outer_table.id
        """
        refs = extract_table_references(sql)
        tables = {r["table"] for r in refs}
        assert "inner_table" in tables
        assert "outer_table" in tables

    def test_cte_with_table_references(self):
        sql = """
        WITH cte AS (
            SELECT * FROM db.schema.source_table
        )
        SELECT * FROM cte
        JOIN db.schema.other_table ON cte.id = other_table.id
        """
        refs = extract_table_references(sql)
        tables = {r["table"] for r in refs}
        assert "source_table" in tables
        assert "other_table" in tables

    def test_no_duplicate_references(self):
        sql = """
        SELECT * FROM db.schema.table1
        UNION ALL
        SELECT * FROM db.schema.table1
        """
        refs = extract_table_references(sql)
        assert len(refs) == 1

    def test_empty_sql(self):
        refs = extract_table_references("")
        assert refs == []


class TestConvertFile:
    def test_converts_file_and_returns_refs(self, tmp_input_dir: Path, tmp_output_dir: Path):
        input_file = tmp_input_dir / "test.sql"
        input_file.write_text("SELECT GETDATE() FROM db.schema.my_table")

        output_file = tmp_output_dir / "test.sql"
        refs = convert_file(input_file, output_file)

        assert output_file.exists()
        content = output_file.read_text()
        assert "CURRENT_TIMESTAMP()" in content

        assert len(refs) == 1
        assert refs[0]["table"] == "my_table"

    def test_creates_output_directory(self, tmp_input_dir: Path, tmp_path: Path):
        input_file = tmp_input_dir / "test.sql"
        input_file.write_text("SELECT 1")

        output_file = tmp_path / "nested" / "dir" / "test.sql"
        convert_file(input_file, output_file)

        assert output_file.exists()


class TestConvertDirectory:
    def test_converts_all_sql_files(self, tmp_input_dir: Path, tmp_output_dir: Path):
        (tmp_input_dir / "file1.sql").write_text("SELECT * FROM db.schema.table1")
        (tmp_input_dir / "file2.sql").write_text("SELECT * FROM db.schema.table2")

        refs = convert_directory(tmp_input_dir, tmp_output_dir)

        assert len(refs) == 2
        assert (tmp_output_dir / "file1" / "file1.sql").exists()
        assert (tmp_output_dir / "file2" / "file2.sql").exists()

    def test_ignores_non_sql_files(self, tmp_input_dir: Path, tmp_output_dir: Path):
        (tmp_input_dir / "file.sql").write_text("SELECT 1")
        (tmp_input_dir / "file.txt").write_text("not sql")

        refs = convert_directory(tmp_input_dir, tmp_output_dir)

        assert len(refs) == 1

    def test_empty_directory(self, tmp_input_dir: Path, tmp_output_dir: Path):
        refs = convert_directory(tmp_input_dir, tmp_output_dir)
        assert refs == {}

    def test_handles_nested_sql_files(self, tmp_input_dir: Path, tmp_output_dir: Path):
        nested = tmp_input_dir / "subdir"
        nested.mkdir()
        (nested / "nested.sql").write_text("SELECT * FROM db.schema.nested_table")

        refs = convert_directory(tmp_input_dir, tmp_output_dir)

        assert len(refs) == 1
        assert (tmp_output_dir / "nested" / "nested.sql").exists()
