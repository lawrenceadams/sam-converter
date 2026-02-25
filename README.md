# SAM Converter

Convert SAM Designer T-SQL files to Snowflake SQL for dbt projects.

## Features

- Converts T-SQL syntax to Snowflake SQL using sqlglot
- Extracts table references from FROM/JOIN clauses into dbt `sources.yml`
- Handles incomplete table references (missing database/schema) with warnings
- Outputs flat directory structure suitable for dbt models

## Installation

```bash
uv pip install .
```

Or for development:

```bash
uv pip install -e .
```

## Usage

```bash
sam-converter <input_directory> [-o <output_directory>]
```

### Options

- `-o, --output`: Output directory (default: `output`)
- `-v, --verbose`: Enable verbose logging

### Example

```bash
sam-converter ./sam_project -o ./dbt_project/models
```

This will:
1. Convert all `.sql` files in `./sam_project` from T-SQL to Snowflake SQL
2. Write converted files to `./dbt_project/models/<filename>/<filename>.sql`
3. Generate `sources.yml` with all referenced tables

## Output Structure

```
output/
├── sources.yml          # dbt sources file
├── model_one/
│   └── model_one.sql    # converted Snowflake SQL
├── model_two/
│   └── model_two.sql
└── ...
```

## Incomplete References

When a table reference is missing database or schema (e.g., `SCHEMA.table` or just `table`), the converter will:
1. Log a warning with the incomplete reference
2. Include the table in `sources.yml` with blank database/schema fields
