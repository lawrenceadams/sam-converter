import logging
import sys
from pathlib import Path

import click

from sam_converter.converter import convert_directory
from sam_converter.extractor import categorize_refs, extract_sources, extract_refs


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(levelname)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stderr)],
    )


@click.command()
@click.argument("input_dir", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option(
    "-o", "--output",
    type=click.Path(file_okay=False, path_type=Path),
    default=Path("output"),
    help="Output directory (default: output)",
)
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose logging")
def main(input_dir: Path, output: Path, verbose: bool) -> None:
    """
    Convert SAM Designer T-SQL files to Snowflake SQL for dbt.

    INPUT_DIR is the directory containing .sql files to convert.
    """
    setup_logging(verbose)
    logger = logging.getLogger(__name__)

    logger.info(f"Converting SQL files from {input_dir} to {output}")

    results = convert_directory(input_dir, output)

    if results:
        categorized = categorize_refs(results)
        extract_sources(categorized, output)
        extract_refs(categorized, output)

        ref_count = sum(len(c.refs) for c in categorized)
        source_count = sum(len(c.sources) for c in categorized)
        logger.info(
            f"Conversion complete. {len(results)} files processed. "
            f"{ref_count} model refs, {source_count} source refs found."
        )
    else:
        logger.warning("No files were converted.")


if __name__ == "__main__":
    main()
