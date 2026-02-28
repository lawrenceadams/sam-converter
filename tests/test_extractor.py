import pytest
from pathlib import Path

import yaml

from sam_converter.converter import ConversionResult, TableRef
from sam_converter.extractor import (
    categorize_refs,
    extract_sources,
    extract_refs,
    inject_dbt_macros,
    _build_source_name,
    _is_mixed_case,
    CategorizedRefs,
)


class TestIsMixedCase:
    def test_mixed_case(self):
        assert _is_mixed_case("MyTable") is True
        assert _is_mixed_case("myTable") is True
        assert _is_mixed_case("PatientID") is True

    def test_all_lowercase(self):
        assert _is_mixed_case("mytable") is False

    def test_all_uppercase(self):
        assert _is_mixed_case("MYTABLE") is False

    def test_with_underscores(self):
        assert _is_mixed_case("My_Table") is True
        assert _is_mixed_case("my_table") is False
        assert _is_mixed_case("MY_TABLE") is False


class TestBuildSourceName:
    def test_full_qualification(self):
        assert _build_source_name("mydb", "myschema") == "mydb_myschema"

    def test_schema_only(self):
        assert _build_source_name("", "myschema") == "myschema"

    def test_database_only(self):
        assert _build_source_name("mydb", "") == "mydb"

    def test_no_qualification(self):
        assert _build_source_name("", "") == "unknown_source"

    def test_lowercase(self):
        assert _build_source_name("MyDB", "MySchema") == "mydb_myschema"


class TestCategorizeRefs:
    def test_identifies_refs_between_models(self):
        results = [
            ConversionResult(
                model_name="model_a",
                output_path=Path("model_a/model_a.sql"),
                table_refs=[TableRef(database="", schema="", table="model_b")],
            ),
            ConversionResult(
                model_name="model_b",
                output_path=Path("model_b/model_b.sql"),
                table_refs=[TableRef(database="db", schema="schema", table="external")],
            ),
        ]

        categorized = categorize_refs(results)

        assert len(categorized) == 2

        cat_a = next(c for c in categorized if c.model_name == "model_a")
        assert cat_a.refs == ["model_b"]
        assert cat_a.sources == []

        cat_b = next(c for c in categorized if c.model_name == "model_b")
        assert cat_b.refs == []
        assert len(cat_b.sources) == 1

    def test_case_insensitive_ref_matching(self):
        results = [
            ConversionResult(
                model_name="ModelA",
                output_path=Path("ModelA/ModelA.sql"),
                table_refs=[TableRef(database="", schema="", table="MODELB")],
            ),
            ConversionResult(
                model_name="modelb",
                output_path=Path("modelb/modelb.sql"),
                table_refs=[],
            ),
        ]

        categorized = categorize_refs(results)

        cat_a = next(c for c in categorized if c.model_name == "ModelA")
        assert cat_a.refs == ["modelb"]

    def test_does_not_self_reference(self):
        results = [
            ConversionResult(
                model_name="model_a",
                output_path=Path("model_a/model_a.sql"),
                table_refs=[TableRef(database="", schema="", table="model_a")],
            ),
        ]

        categorized = categorize_refs(results)

        assert categorized[0].refs == []
        assert len(categorized[0].sources) == 1

    def test_external_tables_are_sources(self):
        results = [
            ConversionResult(
                model_name="model_a",
                output_path=Path("model_a/model_a.sql"),
                table_refs=[
                    TableRef(database="db", schema="schema", table="external_table"),
                    TableRef(database="", schema="raw", table="another"),
                ],
            ),
        ]

        categorized = categorize_refs(results)

        assert categorized[0].refs == []
        assert len(categorized[0].sources) == 2

    def test_qualified_refs_same_db_schema(self):
        """SAM.x.mytable referencing SAM.x.myothertable should be a ref."""
        results = [
            ConversionResult(
                model_name="mytable",
                output_path=Path("mytable/mytable.sql"),
                table_refs=[TableRef(database="SAM", schema="x", table="myothertable")],
            ),
            ConversionResult(
                model_name="myothertable",
                output_path=Path("myothertable/myothertable.sql"),
                table_refs=[TableRef(database="SAM", schema="x", table="external")],
            ),
        ]

        categorized = categorize_refs(results)

        cat_mytable = next(c for c in categorized if c.model_name == "mytable")
        assert cat_mytable.refs == ["myothertable"]
        assert cat_mytable.sources == []

    def test_conflicting_qualifications_become_sources(self):
        """If a model name is referenced with different db/schemas, all qualified refs become sources."""
        results = [
            ConversionResult(
                model_name="model_a",
                output_path=Path("model_a/model_a.sql"),
                table_refs=[
                    # This references "users" in EXTERNAL.raw
                    TableRef(database="EXTERNAL", schema="raw", table="users"),
                ],
            ),
            ConversionResult(
                model_name="users",
                output_path=Path("users/users.sql"),
                table_refs=[],
            ),
            ConversionResult(
                model_name="model_b",
                output_path=Path("model_b/model_b.sql"),
                table_refs=[
                    # This references "users" in SAM.dbt - different qualification!
                    TableRef(database="SAM", schema="dbt", table="users"),
                ],
            ),
        ]

        categorized = categorize_refs(results)

        # Both should be sources since there are conflicting qualifications
        cat_a = next(c for c in categorized if c.model_name == "model_a")
        assert cat_a.refs == []
        assert len(cat_a.sources) == 1

        cat_b = next(c for c in categorized if c.model_name == "model_b")
        assert cat_b.refs == []
        assert len(cat_b.sources) == 1

    def test_mixed_qualified_and_unqualified_refs(self):
        """Unqualified refs to models should always be refs."""
        results = [
            ConversionResult(
                model_name="model_a",
                output_path=Path("model_a/model_a.sql"),
                table_refs=[
                    TableRef(database="", schema="", table="model_b"),  # unqualified
                    TableRef(database="SAM", schema="x", table="model_b"),  # qualified
                ],
            ),
            ConversionResult(
                model_name="model_b",
                output_path=Path("model_b/model_b.sql"),
                table_refs=[],
            ),
        ]

        categorized = categorize_refs(results)

        cat_a = next(c for c in categorized if c.model_name == "model_a")
        # Both should be refs (unqualified always is, qualified matches)
        assert cat_a.refs == ["model_b", "model_b"]


class TestExtractSources:
    def _make_categorized(self, sources: list[TableRef]) -> list[CategorizedRefs]:
        return [CategorizedRefs(model_name="test", output_path=Path("test.sql"), refs=[], sources=sources)]

    def test_creates_sources_yml(self, tmp_output_dir: Path):
        categorized = self._make_categorized([
            TableRef(database="db1", schema="schema1", table="table1"),
        ])

        extract_sources(categorized, tmp_output_dir)

        sources_path = tmp_output_dir / "sources.yml"
        assert sources_path.exists()

    def test_groups_tables_by_source(self, tmp_output_dir: Path):
        categorized = [
            CategorizedRefs(
                model_name="model1",
                output_path=Path("model1.sql"),
                refs=[],
                sources=[
                    TableRef(database="db1", schema="schema1", table="table1"),
                    TableRef(database="db1", schema="schema1", table="table2"),
                ],
            ),
            CategorizedRefs(
                model_name="model2",
                output_path=Path("model2.sql"),
                refs=[],
                sources=[
                    TableRef(database="db1", schema="schema1", table="table3"),
                ],
            ),
        ]

        extract_sources(categorized, tmp_output_dir)

        sources_path = tmp_output_dir / "sources.yml"
        with open(sources_path) as f:
            sources = yaml.safe_load(f)

        assert sources["version"] == 2
        assert len(sources["sources"]) == 1

        source = sources["sources"][0]
        assert source["name"] == "db1_schema1"
        assert source["database"] == "db1"
        assert source["schema"] == "schema1"

        table_names = {t["name"] for t in source["tables"]}
        assert table_names == {"table1", "table2", "table3"}

    def test_multiple_sources(self, tmp_output_dir: Path):
        categorized = self._make_categorized([
            TableRef(database="db1", schema="schema1", table="table1"),
            TableRef(database="db2", schema="schema2", table="table2"),
        ])

        extract_sources(categorized, tmp_output_dir)

        sources_path = tmp_output_dir / "sources.yml"
        with open(sources_path) as f:
            sources = yaml.safe_load(f)

        assert len(sources["sources"]) == 2
        source_names = {s["name"] for s in sources["sources"]}
        assert source_names == {"db1_schema1", "db2_schema2"}

    def test_incomplete_references(self, tmp_output_dir: Path):
        categorized = self._make_categorized([
            TableRef(database="", schema="schema_only", table="table1"),
            TableRef(database="", schema="", table="unqualified"),
        ])

        extract_sources(categorized, tmp_output_dir)

        sources_path = tmp_output_dir / "sources.yml"
        with open(sources_path) as f:
            sources = yaml.safe_load(f)

        assert len(sources["sources"]) == 2

        source_names = {s["name"] for s in sources["sources"]}
        assert "schema_only" in source_names
        assert "unknown_source" in source_names

    def test_no_database_or_schema_keys_when_empty(self, tmp_output_dir: Path):
        categorized = self._make_categorized([
            TableRef(database="", schema="", table="table1"),
        ])

        extract_sources(categorized, tmp_output_dir)

        sources_path = tmp_output_dir / "sources.yml"
        with open(sources_path) as f:
            sources = yaml.safe_load(f)

        source = sources["sources"][0]
        assert "database" not in source
        assert "schema" not in source

    def test_empty_sources(self, tmp_output_dir: Path):
        categorized = [CategorizedRefs(model_name="test", output_path=Path("test.sql"), refs=[], sources=[])]
        extract_sources(categorized, tmp_output_dir)

        sources_path = tmp_output_dir / "sources.yml"
        with open(sources_path) as f:
            sources = yaml.safe_load(f)

        assert sources["sources"] == []

    def test_tables_sorted_alphabetically(self, tmp_output_dir: Path):
        categorized = self._make_categorized([
            TableRef(database="db", schema="schema", table="zebra"),
            TableRef(database="db", schema="schema", table="alpha"),
            TableRef(database="db", schema="schema", table="middle"),
        ])

        extract_sources(categorized, tmp_output_dir)

        sources_path = tmp_output_dir / "sources.yml"
        with open(sources_path) as f:
            sources = yaml.safe_load(f)

        table_names = [t["name"] for t in sources["sources"][0]["tables"]]
        assert table_names == ["alpha", "middle", "zebra"]

    def test_mixed_case_tables_get_identifier(self, tmp_output_dir: Path):
        categorized = self._make_categorized([
            TableRef(database="db", schema="schema", table="PatientData"),
            TableRef(database="db", schema="schema", table="lowercase"),
            TableRef(database="db", schema="schema", table="UPPERCASE"),
        ])

        extract_sources(categorized, tmp_output_dir)

        sources_path = tmp_output_dir / "sources.yml"
        with open(sources_path) as f:
            sources = yaml.safe_load(f)

        tables = {t["name"]: t for t in sources["sources"][0]["tables"]}

        # Mixed case gets lowercase name + identifier
        assert tables["patientdata"]["name"] == "patientdata"
        assert tables["patientdata"]["identifier"] == "PatientData"

        # All lowercase - no identifier
        assert tables["lowercase"]["name"] == "lowercase"
        assert "identifier" not in tables["lowercase"]

        # All uppercase - no identifier
        assert tables["UPPERCASE"]["name"] == "UPPERCASE"
        assert "identifier" not in tables["UPPERCASE"]


class TestExtractRefs:
    def test_creates_refs_yml_when_refs_exist(self, tmp_output_dir: Path):
        categorized = [
            CategorizedRefs(model_name="model_a", output_path=Path("model_a.sql"), refs=["model_b", "model_c"], sources=[]),
        ]

        extract_refs(categorized, tmp_output_dir)

        refs_path = tmp_output_dir / "model_refs.yml"
        assert refs_path.exists()

        with open(refs_path) as f:
            refs = yaml.safe_load(f)

        assert refs["version"] == 2
        assert len(refs["model_refs"]) == 1
        assert refs["model_refs"][0]["model"] == "model_a"
        assert refs["model_refs"][0]["depends_on"] == ["model_b", "model_c"]

    def test_no_file_when_no_refs(self, tmp_output_dir: Path):
        categorized = [
            CategorizedRefs(model_name="model_a", output_path=Path("model_a.sql"), refs=[], sources=[]),
        ]

        extract_refs(categorized, tmp_output_dir)

        refs_path = tmp_output_dir / "model_refs.yml"
        assert not refs_path.exists()

    def test_multiple_models_with_refs(self, tmp_output_dir: Path):
        categorized = [
            CategorizedRefs(model_name="model_a", output_path=Path("model_a.sql"), refs=["model_c"], sources=[]),
            CategorizedRefs(model_name="model_b", output_path=Path("model_b.sql"), refs=["model_c"], sources=[]),
            CategorizedRefs(model_name="model_c", output_path=Path("model_c.sql"), refs=[], sources=[]),
        ]

        extract_refs(categorized, tmp_output_dir)

        refs_path = tmp_output_dir / "model_refs.yml"
        with open(refs_path) as f:
            refs = yaml.safe_load(f)

        assert len(refs["model_refs"]) == 2
        models_with_refs = {r["model"] for r in refs["model_refs"]}
        assert models_with_refs == {"model_a", "model_b"}

    def test_refs_sorted_alphabetically(self, tmp_output_dir: Path):
        categorized = [
            CategorizedRefs(model_name="model_a", output_path=Path("model_a.sql"), refs=["zebra", "alpha", "middle"], sources=[]),
        ]

        extract_refs(categorized, tmp_output_dir)

        refs_path = tmp_output_dir / "model_refs.yml"
        with open(refs_path) as f:
            refs = yaml.safe_load(f)

        assert refs["model_refs"][0]["depends_on"] == ["alpha", "middle", "zebra"]


class TestInjectDbtMacros:
    def test_injects_ref_macro(self, tmp_output_dir: Path):
        sql_file = tmp_output_dir / "model_a.sql"
        sql_file.write_text("SELECT * FROM staging_users JOIN staging_orders ON id = id")

        categorized = [
            CategorizedRefs(
                model_name="model_a",
                output_path=sql_file,
                refs=["staging_users", "staging_orders"],
                sources=[],
            ),
        ]

        inject_dbt_macros(categorized)

        result = sql_file.read_text()
        assert "{{ ref('staging_users') }}" in result
        assert "{{ ref('staging_orders') }}" in result

    def test_injects_source_macro_fully_qualified(self, tmp_output_dir: Path):
        sql_file = tmp_output_dir / "model_a.sql"
        sql_file.write_text("SELECT * FROM HealthCatalyst.raw.Patients")

        categorized = [
            CategorizedRefs(
                model_name="model_a",
                output_path=sql_file,
                refs=[],
                sources=[TableRef(database="HealthCatalyst", schema="raw", table="Patients")],
            ),
        ]

        inject_dbt_macros(categorized)

        result = sql_file.read_text()
        assert "{{ source('healthcatalyst_raw', 'patients') }}" in result

    def test_injects_source_macro_schema_qualified(self, tmp_output_dir: Path):
        sql_file = tmp_output_dir / "model_a.sql"
        sql_file.write_text("SELECT * FROM raw.patients")

        categorized = [
            CategorizedRefs(
                model_name="model_a",
                output_path=sql_file,
                refs=[],
                sources=[TableRef(database="", schema="raw", table="patients")],
            ),
        ]

        inject_dbt_macros(categorized)

        result = sql_file.read_text()
        assert "{{ source('raw', 'patients') }}" in result

    def test_preserves_mixed_case_in_source(self, tmp_output_dir: Path):
        sql_file = tmp_output_dir / "model_a.sql"
        sql_file.write_text("SELECT * FROM db.schema.PatientData")

        categorized = [
            CategorizedRefs(
                model_name="model_a",
                output_path=sql_file,
                refs=[],
                sources=[TableRef(database="db", schema="schema", table="PatientData")],
            ),
        ]

        inject_dbt_macros(categorized)

        result = sql_file.read_text()
        # Mixed case gets lowercased name in source macro
        assert "{{ source('db_schema', 'patientdata') }}" in result

    def test_handles_both_refs_and_sources(self, tmp_output_dir: Path):
        sql_file = tmp_output_dir / "dim_patients.sql"
        sql_file.write_text(
            "SELECT * FROM staging_patients "
            "JOIN HealthCatalyst.raw.Encounters ON id = id"
        )

        categorized = [
            CategorizedRefs(
                model_name="dim_patients",
                output_path=sql_file,
                refs=["staging_patients"],
                sources=[TableRef(database="HealthCatalyst", schema="raw", table="Encounters")],
            ),
        ]

        inject_dbt_macros(categorized)

        result = sql_file.read_text()
        assert "{{ ref('staging_patients') }}" in result
        assert "{{ source('healthcatalyst_raw', 'encounters') }}" in result
