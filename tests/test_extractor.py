import pytest
from pathlib import Path

import yaml

from sam_converter.extractor import extract_sources, _build_source_name


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


class TestExtractSources:
    def test_creates_sources_yml(self, tmp_output_dir: Path):
        table_refs = {
            "model1.sql": [
                {"database": "db1", "schema": "schema1", "table": "table1"},
            ]
        }

        extract_sources(table_refs, tmp_output_dir)

        sources_path = tmp_output_dir / "sources.yml"
        assert sources_path.exists()

    def test_groups_tables_by_source(self, tmp_output_dir: Path):
        table_refs = {
            "model1.sql": [
                {"database": "db1", "schema": "schema1", "table": "table1"},
                {"database": "db1", "schema": "schema1", "table": "table2"},
            ],
            "model2.sql": [
                {"database": "db1", "schema": "schema1", "table": "table3"},
            ],
        }

        extract_sources(table_refs, tmp_output_dir)

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
        table_refs = {
            "model1.sql": [
                {"database": "db1", "schema": "schema1", "table": "table1"},
                {"database": "db2", "schema": "schema2", "table": "table2"},
            ],
        }

        extract_sources(table_refs, tmp_output_dir)

        sources_path = tmp_output_dir / "sources.yml"
        with open(sources_path) as f:
            sources = yaml.safe_load(f)

        assert len(sources["sources"]) == 2
        source_names = {s["name"] for s in sources["sources"]}
        assert source_names == {"db1_schema1", "db2_schema2"}

    def test_incomplete_references(self, tmp_output_dir: Path):
        table_refs = {
            "model1.sql": [
                {"database": "", "schema": "schema_only", "table": "table1"},
                {"database": "", "schema": "", "table": "unqualified"},
            ],
        }

        extract_sources(table_refs, tmp_output_dir)

        sources_path = tmp_output_dir / "sources.yml"
        with open(sources_path) as f:
            sources = yaml.safe_load(f)

        assert len(sources["sources"]) == 2

        source_names = {s["name"] for s in sources["sources"]}
        assert "schema_only" in source_names
        assert "unknown_source" in source_names

    def test_no_database_or_schema_keys_when_empty(self, tmp_output_dir: Path):
        table_refs = {
            "model1.sql": [
                {"database": "", "schema": "", "table": "table1"},
            ],
        }

        extract_sources(table_refs, tmp_output_dir)

        sources_path = tmp_output_dir / "sources.yml"
        with open(sources_path) as f:
            sources = yaml.safe_load(f)

        source = sources["sources"][0]
        assert "database" not in source
        assert "schema" not in source

    def test_empty_refs(self, tmp_output_dir: Path):
        extract_sources({}, tmp_output_dir)

        sources_path = tmp_output_dir / "sources.yml"
        with open(sources_path) as f:
            sources = yaml.safe_load(f)

        assert sources["sources"] == []

    def test_tables_sorted_alphabetically(self, tmp_output_dir: Path):
        table_refs = {
            "model1.sql": [
                {"database": "db", "schema": "schema", "table": "zebra"},
                {"database": "db", "schema": "schema", "table": "alpha"},
                {"database": "db", "schema": "schema", "table": "middle"},
            ],
        }

        extract_sources(table_refs, tmp_output_dir)

        sources_path = tmp_output_dir / "sources.yml"
        with open(sources_path) as f:
            sources = yaml.safe_load(f)

        table_names = [t["name"] for t in sources["sources"][0]["tables"]]
        assert table_names == ["alpha", "middle", "zebra"]
