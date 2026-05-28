"""Tests for skipif/onlyif directive handling in parse_slt."""

import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from validate_slt import parse_slt, QueryRecord, ErrorRecord, StatementRecord


def _write(content: str) -> str:
    fd, path = tempfile.mkstemp(suffix=".slt")
    with os.fdopen(fd, "w") as f:
        f.write(content)
    return path


def test_skipif_spark_marks_record():
    path = _write(
        "skipif spark\n"
        "query I\n"
        "SELECT 1;\n"
        "----\n"
        "1\n"
    )
    records = parse_slt(path)
    assert len(records) == 1
    assert isinstance(records[0], QueryRecord)
    assert records[0].skip_engines == {"spark"}
    assert records[0].only_engines == set()


def test_onlyif_spark_marks_record():
    path = _write(
        "onlyif spark\n"
        "query I\n"
        "SELECT 1;\n"
        "----\n"
        "1\n"
    )
    records = parse_slt(path)
    assert len(records) == 1
    assert records[0].only_engines == {"spark"}
    assert records[0].skip_engines == set()


def test_directive_only_applies_to_next_record():
    path = _write(
        "skipif spark\n"
        "query I\n"
        "SELECT 1;\n"
        "----\n"
        "1\n"
        "\n"
        "query I\n"
        "SELECT 2;\n"
        "----\n"
        "2\n"
    )
    records = parse_slt(path)
    assert len(records) == 2
    assert records[0].skip_engines == {"spark"}
    assert records[1].skip_engines == set()


def test_no_directive_means_empty_engines():
    path = _write(
        "query I\n"
        "SELECT 1;\n"
        "----\n"
        "1\n"
    )
    records = parse_slt(path)
    assert records[0].skip_engines == set()
    assert records[0].only_engines == set()


def test_skipif_spark_records_are_filtered_during_process():
    """Both directives interact correctly with the engine routing."""
    from validate_slt import process_file

    path = _write(
        "skipif spark\n"
        "query I\n"
        "SELECT 9999999999999;\n"  # would error if executed
        "----\n"
        "9999999999999\n"
        "\n"
        "onlyif datafusion\n"
        "query I\n"
        "SELECT 8888888888888;\n"  # would error if executed
        "----\n"
        "8888888888888\n"
    )

    # Both records should be skipped before any execution.
    # Both records are filtered by directive check before any Spark SQL is executed,
    # so process_file runs to completion without needing a SparkSession.
    result = process_file(path, verbose=False, show_skipped=True)
    assert result.passed == 0
    assert result.failed == 0
    assert result.skipped == 2
