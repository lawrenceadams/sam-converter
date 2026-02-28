import pytest
from pathlib import Path

from sam_converter.converter import (
    transpile_tsql_to_snowflake,
    extract_table_references,
    extract_model_name,
    strip_base_suffix,
    to_snake_case,
    convert_file,
    convert_directory,
    TableRef,
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


class TestToSnakeCase:
    def test_mixed_case(self):
        assert to_snake_case("PatientData") == "patient_data"

    def test_multiple_words(self):
        assert to_snake_case("PopulationSpell") == "population_spell"

    def test_acronym(self):
        assert to_snake_case("HTTPServer") == "http_server"

    def test_already_snake_case(self):
        assert to_snake_case("already_snake") == "already_snake"

    def test_single_word(self):
        assert to_snake_case("Patient") == "patient"

    def test_lowercase(self):
        assert to_snake_case("lowercase") == "lowercase"

    def test_all_uppercase(self):
        assert to_snake_case("UPPERCASE") == "uppercase"

    def test_mixed_with_numbers(self):
        assert to_snake_case("Patient2Data") == "patient2_data"


class TestStripBaseSuffix:
    def test_strips_base_suffix(self):
        assert strip_base_suffix("PopulationSpellBASE") == "PopulationSpell"

    def test_no_base_suffix(self):
        assert strip_base_suffix("PopulationSpell") == "PopulationSpell"

    def test_base_in_middle(self):
        # Only strip if at the end
        assert strip_base_suffix("BASEPopulation") == "BASEPopulation"

    def test_lowercase_base_not_stripped(self):
        # Only strip uppercase BASE
        assert strip_base_suffix("PopulationSpellbase") == "PopulationSpellbase"

    def test_empty_string(self):
        assert strip_base_suffix("") == ""


class TestExtractModelName:
    def test_simple_name(self):
        assert extract_model_name("my_model") == "my_model"

    def test_qualified_db_schema_table(self):
        assert extract_model_name("db.schema.TableName") == "table_name"

    def test_schema_table(self):
        assert extract_model_name("schema.TableName") == "table_name"

    def test_multiple_dots(self):
        assert extract_model_name("a.b.c.d.TableName") == "table_name"

    def test_converts_to_snake_case(self):
        assert extract_model_name("SAM.dbo.PatientData") == "patient_data"

    def test_strips_base_suffix_and_converts(self):
        assert extract_model_name("SAM.MEG.PopulationSpellBASE") == "population_spell"

    def test_strips_base_from_simple_name(self):
        assert extract_model_name("PatientDataBASE") == "patient_data"


class TestExtractTableReferences:
    def test_fully_qualified_table(self):
        sql = "SELECT * FROM database.schema.table"
        refs = extract_table_references(sql)
        assert len(refs) == 1
        assert refs[0] == TableRef(database="database", schema="schema", table="table")

    def test_schema_qualified_table(self):
        sql = "SELECT * FROM schema.table"
        refs = extract_table_references(sql)
        assert len(refs) == 1
        assert refs[0] == TableRef(database="", schema="schema", table="table")

    def test_unqualified_table(self):
        sql = "SELECT * FROM my_table"
        refs = extract_table_references(sql)
        assert len(refs) == 1
        assert refs[0] == TableRef(database="", schema="", table="my_table")

    def test_multiple_tables_from_join(self):
        sql = """
        SELECT * FROM db1.schema1.table1 t1
        JOIN db2.schema2.table2 t2 ON t1.id = t2.id
        LEFT JOIN schema3.table3 t3 ON t1.id = t3.id
        """
        refs = extract_table_references(sql)
        assert len(refs) == 3

        tables = {r.table for r in refs}
        assert tables == {"table1", "table2", "table3"}

    def test_subquery_tables(self):
        sql = """
        SELECT * FROM (
            SELECT * FROM db.schema.inner_table
        ) sub
        JOIN db.schema.outer_table ON sub.id = outer_table.id
        """
        refs = extract_table_references(sql)
        tables = {r.table for r in refs}
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
        tables = {r.table for r in refs}
        assert "source_table" in tables
        assert "other_table" in tables

    def test_cte_names_excluded_from_refs(self):
        """CTE names should not appear as table references."""
        sql = """
        WITH patient_cte AS (
            SELECT * FROM db.schema.patients
        ),
        encounter_cte AS (
            SELECT * FROM db.schema.encounters
        )
        SELECT * FROM patient_cte
        JOIN encounter_cte ON patient_cte.id = encounter_cte.patient_id
        """
        refs = extract_table_references(sql)
        tables = {r.table for r in refs}

        # Real tables should be included
        assert "patients" in tables
        assert "encounters" in tables

        # CTE names should NOT be included
        assert "patient_cte" not in tables
        assert "encounter_cte" not in tables

    def test_cte_name_same_as_real_table(self):
        """If a CTE has the same name as referenced elsewhere, only non-CTE refs count."""
        sql = """
        WITH my_table AS (
            SELECT * FROM db.schema.source
        )
        SELECT * FROM my_table
        """
        refs = extract_table_references(sql)
        tables = {r.table for r in refs}

        assert "source" in tables
        assert "my_table" not in tables

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
    def test_converts_file_and_returns_result(self, tmp_input_dir: Path, tmp_output_dir: Path):
        input_file = tmp_input_dir / "test.sql"
        input_file.write_text("SELECT GETDATE() FROM db.schema.my_table")

        output_file = tmp_output_dir / "test.sql"
        result = convert_file(input_file, output_file)

        assert output_file.exists()
        content = output_file.read_text()
        assert "CURRENT_TIMESTAMP()" in content

        assert result.model_name == "test"
        assert len(result.table_refs) == 1
        assert result.table_refs[0].table == "my_table"

    def test_qualified_filename_extracts_model_name(self, tmp_input_dir: Path, tmp_output_dir: Path):
        """Files like db.schema.TableName.sql should have model_name = table_name."""
        input_file = tmp_input_dir / "SAM.dbo.PatientData.sql"
        input_file.write_text("SELECT * FROM SAM.dbo.Encounters")

        output_file = tmp_output_dir / "patient_data.sql"
        result = convert_file(input_file, output_file)

        assert result.model_name == "patient_data"

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

        results = convert_directory(tmp_input_dir, tmp_output_dir)

        assert len(results) == 2
        assert (tmp_output_dir / "file1.sql").exists()
        assert (tmp_output_dir / "file2.sql").exists()

    def test_ignores_non_sql_files(self, tmp_input_dir: Path, tmp_output_dir: Path):
        (tmp_input_dir / "file.sql").write_text("SELECT 1")
        (tmp_input_dir / "file.txt").write_text("not sql")

        results = convert_directory(tmp_input_dir, tmp_output_dir)

        assert len(results) == 1

    def test_empty_directory(self, tmp_input_dir: Path, tmp_output_dir: Path):
        results = convert_directory(tmp_input_dir, tmp_output_dir)
        assert results == []

    def test_handles_nested_sql_files(self, tmp_input_dir: Path, tmp_output_dir: Path):
        nested = tmp_input_dir / "subdir"
        nested.mkdir()
        (nested / "nested.sql").write_text("SELECT * FROM db.schema.nested_table")

        results = convert_directory(tmp_input_dir, tmp_output_dir)

        assert len(results) == 1
        assert (tmp_output_dir / "nested.sql").exists()

    def test_results_contain_model_names(self, tmp_input_dir: Path, tmp_output_dir: Path):
        (tmp_input_dir / "my_model.sql").write_text("SELECT 1")

        results = convert_directory(tmp_input_dir, tmp_output_dir)

        assert len(results) == 1
        assert results[0].model_name == "my_model"

    def test_qualified_filenames_produce_snake_case_output_names(self, tmp_input_dir: Path, tmp_output_dir: Path):
        """Files like SAM.dbo.TableName.sql should output as table_name.sql."""
        (tmp_input_dir / "SAM.dbo.PatientData.sql").write_text("SELECT * FROM SAM.dbo.Encounters")
        (tmp_input_dir / "SAM.dbo.Encounters.sql").write_text("SELECT * FROM SAM.dbo.External")

        results = convert_directory(tmp_input_dir, tmp_output_dir)

        assert len(results) == 2

        # Output files should have snake_case names
        assert (tmp_output_dir / "patient_data.sql").exists()
        assert (tmp_output_dir / "encounters.sql").exists()

        # Model names should be snake_case
        model_names = {r.model_name for r in results}
        assert model_names == {"patient_data", "encounters"}
