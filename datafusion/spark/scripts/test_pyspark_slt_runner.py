# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

"""Unit tests for pyspark_slt_runner.py. They do not need PySpark or Java.

Run with: python3 -m unittest discover -s datafusion/spark/scripts
"""

import contextlib
import datetime
import io
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pyspark_slt_runner as r  # noqa: E402


def parse(text: str) -> list:
    with tempfile.NamedTemporaryFile("w", suffix=".slt", delete=False) as f:
        f.write(text)
    try:
        return r.parse_slt(Path(f.name))
    finally:
        Path(f.name).unlink()


class ParseTest(unittest.TestCase):
    def test_records(self):
        records = parse(
            "# a comment\n"
            "statement ok\n"
            "CREATE TABLE t AS VALUES (1)\n"
            "\n"
            "query IT rowsort\n"
            "SELECT a,\n"
            "  b FROM t\n"
            "----\n"
            "1 a\n"
            "# not a comment\n"
            "\n"
            "query error DataFusion error: .*\n"
            "SELECT x\n"
            "\n"
            "skipif spark\n"
            "query I\n"
            "SELECT 1\n"
            "----\n"
            "1\n"
        )
        self.assertEqual(len(records), 4)
        self.assertEqual(records[0].kind, "statement")
        self.assertEqual(records[0].sql, "CREATE TABLE t AS VALUES (1)")
        self.assertEqual(records[1].types, "IT")
        self.assertEqual(records[1].sort_mode, "rowsort")
        self.assertEqual(records[1].sql, "SELECT a,\n  b FROM t")
        self.assertEqual(records[1].expected, ["1 a", "# not a comment"])
        self.assertEqual(records[1].line, 5)
        self.assertTrue(records[2].expect_error)
        self.assertEqual(records[2].sql, "SELECT x")
        self.assertEqual(records[3].skip_reason, "skipif spark")

    def test_onlyif(self):
        records = parse("onlyif postgres\nquery I\nSELECT 1\n----\n1\n\nquery I\nSELECT 2\n")
        self.assertEqual(records[0].skip_reason, "onlyif postgres")
        self.assertIsNone(records[1].skip_reason)


class TranslateTest(unittest.TestCase):
    def test_spark_parses_double_colon_casts(self):
        sql = "SELECT abs(-128::TINYINT), 'a'::STRING"
        self.assertEqual(r.translate(sql), sql)

    def test_type_aliases(self):
        self.assertEqual(
            r.translate("SELECT 1::FLOAT8, CAST(x AS text), TRY_CAST(y AS Bytea)"),
            "SELECT 1::DOUBLE, CAST(x AS STRING), TRY_CAST(y AS BINARY)",
        )
        self.assertEqual(r.translate("SELECT 'a'::VARCHAR"), "SELECT 'a'::STRING")
        self.assertEqual(r.translate("SELECT CAST(x AS VARCHAR(3))"), "SELECT CAST(x AS VARCHAR(3))")

    def test_only_types_are_renamed(self):
        # An alias named like a type, and text inside a string, stay as they are.
        self.assertEqual(r.translate("SELECT 1 AS text, 'x::TEXT'"), "SELECT 1 AS text, 'x::TEXT'")

    def test_values_column_names(self):
        self.assertEqual(
            r.translate("SELECT column1, t.column2, 'column3' FROM t"),
            "SELECT col1, t.col2, 'column3' FROM t",
        )

    def test_skips(self):
        with self.assertRaisesRegex(r.Skip, "backslash"):
            r.translate(r"SELECT quote('a\tb')")
        with self.assertRaisesRegex(r.Skip, "DECIMAL precision"):
            r.translate("SELECT 1::DECIMAL(76, 10)")
        self.assertEqual(r.translate("SELECT 1::DECIMAL(38, 10)"), "SELECT 1::DECIMAL(38, 10)")

    def test_create_table(self):
        self.assertEqual(
            r.translate_statement("CREATE TABLE t AS VALUES (1, 'a')"),
            "CREATE OR REPLACE TEMPORARY VIEW t AS VALUES (1, 'a')",
        )
        self.assertEqual(
            r.translate_statement(
                "CREATE TABLE t (id int, val TEXT, d decimal(10,3)) AS VALUES (1, 'a', 1.5);"
            ),
            "CREATE OR REPLACE TEMPORARY VIEW t AS SELECT CAST(id AS int) AS id, "
            "CAST(val AS STRING) AS val, CAST(d AS decimal(10,3)) AS d "
            "FROM (VALUES (1, 'a', 1.5)) AS __slt_values(id, val, d)",
        )
        self.assertEqual(r.translate_statement("DROP TABLE t;"), "DROP VIEW IF EXISTS t")
        self.assertEqual(r.translate_statement("drop table if exists t"), "DROP VIEW IF EXISTS t")

    def test_settings(self):
        self.assertEqual(
            r.parse_setting("set datafusion.execution.enable_ansi_mode = true;"),
            ("datafusion.execution.enable_ansi_mode", "true"),
        )
        self.assertEqual(
            r.parse_setting("SET datafusion.execution.time_zone TO '+05:00'"),
            ("datafusion.execution.time_zone", "+05:00"),
        )
        self.assertEqual(
            r.parse_setting("RESET datafusion.execution.time_zone;"),
            ("datafusion.execution.time_zone", None),
        )
        self.assertIsNone(r.parse_setting("SELECT 1"))


class FormatTest(unittest.TestCase):
    def cell(self, value, tree, time_zone="UTC"):
        return r.format_cell(value, tree, time_zone)

    def test_doubles(self):
        # Expected strings from conversion.rs tests and DataFusion's float_to_str.
        double = ("double",)
        self.assertEqual(self.cell(11.0, double), "11")
        self.assertEqual(self.cell(-0.011, double), "-0.011")
        self.assertEqual(self.cell(0.12345678901234567, double), "0.123456789012346")
        self.assertEqual(self.cell(1e20, double), "100000000000000000000")
        self.assertEqual(self.cell(-0.0, double), "0")
        self.assertEqual(self.cell(float("nan"), double), "NaN")
        self.assertEqual(self.cell(float("-inf"), double), "-Infinity")

    def test_floats(self):
        # 0.1 as a 32-bit float, as PySpark returns it.
        self.assertEqual(self.cell(0.10000000149011612, ("float",)), "0.1")

    def test_decimals_keep_scale(self):
        self.assertEqual(self.cell(Decimal("123"), ("decimal", 5, 2)), "123.00")
        self.assertEqual(self.cell(Decimal("0E-10"), ("decimal", 38, 10)), "0.0000000000")

    def test_strings(self):
        self.assertEqual(self.cell("", ("string",)), "(empty)")
        self.assertEqual(self.cell("a\n", ("string",)), "a")
        self.assertEqual(r.format_nested("", ("string",), "UTC"), "")

    def test_nested(self):
        tree = ("array", ("double",))
        self.assertEqual(self.cell([1.0, 2.5, None, 1e20, 1e-7], tree), "[1.0, 2.5, NULL, 1e20, 1e-7]")
        tree = ("struct", (("a", ("int",)), ("b", ("map", ("string",), ("boolean",)))))
        self.assertEqual(self.cell((1, {"x": True}), tree), "{a: 1, b: {x: true}}")
        self.assertEqual(self.cell(bytearray(b"ab"), ("binary",)), "6162")

    def test_ryu(self):
        self.assertEqual(r._ryu(123.0, single=False), "123.0")
        self.assertEqual(r._ryu(0.001234, single=False), "0.001234")
        self.assertEqual(r._ryu(1.2345e30, single=False), "1.2345e30")
        self.assertEqual(r._ryu(-0.0, single=False), "-0.0")
        self.assertEqual(r._ryu(float("inf"), single=False), "inf")
        self.assertEqual(r._ryu(0.10000000149011612, single=True), "0.1")

    def test_timestamps(self):
        ntz = datetime.datetime(2024, 1, 2, 3, 4, 5, 120000)
        self.assertEqual(self.cell(ntz, ("timestamp_ntz",)), "2024-01-02T03:04:05.120")
        # TIMESTAMP values arrive as naive UTC and print in the session time zone.
        utc = datetime.datetime(2024, 1, 2, 3, 4, 5, 123456)
        self.assertEqual(self.cell(utc, ("timestamp",), "Asia/Seoul"), "2024-01-02T12:04:05.123456")
        self.assertEqual(self.cell(utc, ("timestamp",), "-01:30"), "2024-01-02T01:34:05.123456")
        self.assertEqual(self.cell(datetime.date(2024, 1, 2), ("date",)), "2024-01-02")

    def test_localize_timestamps(self):
        self.assertEqual(
            r.localize_timestamps("2024-07-15T00:00:00Z [1970-01-01T00:00:01.500Z]", "UTC"),
            "2024-07-15T00:00:00 [1970-01-01T00:00:01.500]",
        )
        self.assertEqual(
            r.localize_timestamps("2016-08-30T15:00:00+09:00", "UTC"), "2016-08-30T06:00:00"
        )
        self.assertEqual(r.localize_timestamps("2016-08-30T15:00:00", "UTC"), "2016-08-30T15:00:00")

    def test_multiline_last_cell(self):
        lines = r.format_rows([("plan", "a\n  b")], [("string",), ("string",)], "UTC")
        self.assertEqual(lines, ["plan", "01)a", "02)--b"])

    def test_type_letters(self):
        trees = [("int",), ("decimal", 3, 1), ("string",), ("timestamp",), ("array", ("int",))]
        self.assertEqual("".join(r.type_letter(t) for t in trees), "IRTP?")


class CompareTest(unittest.TestCase):
    def test_rowsort(self):
        self.assertTrue(r.results_match(["2", "1 "], ["1", "2"], "rowsort"))
        self.assertFalse(r.results_match(["2", "1"], ["1", "2"], "nosort"))

    def test_numeric_difference(self):
        self.assertEqual(r.numeric_difference(["7 a", "0"], ["7.0 a", "0.0"], "nosort"), "scale")
        self.assertEqual(
            r.numeric_difference(["0.123456789012346 a"], ["0.123456789012345 a"], "nosort"), "close"
        )
        self.assertIsNone(r.numeric_difference(["0.1235"], ["0.1234"], "nosort"))
        self.assertIsNone(r.numeric_difference(["NULL"], ["0"], "nosort"))


def spark_type(name: str, **attributes):
    """An object whose class name matches a PySpark DataType, for type_tree."""
    return type(name, (), attributes)()


class FakeSchema:
    def __init__(self, *types):
        types = [spark_type(t) if isinstance(t, str) else t for t in types]
        self.fields = [type("Field", (), {"dataType": t})() for t in types]

    def simpleString(self):
        return "struct<...>"


class FakeRunner:
    """Stands in for SparkRunner: returns a fixed result or raises a SparkError."""

    time_zone = "UTC"

    def __init__(self, result=None):
        self.result = result
        self.settings = {}

    def set(self, key, value):
        self.settings[key] = value

    def run(self, sql):
        self.sql = sql
        if isinstance(self.result, r.SparkError):
            raise self.result
        return self.result


class RunRecordTest(unittest.TestCase):
    def outcome(self, slt: str, result=None) -> r.Outcome:
        [record] = parse(slt)
        return r.run_record(FakeRunner(result), record)

    def test_pass_and_mismatch(self):
        result = (FakeSchema("IntegerType"), [(1,), (2,)])
        self.assertEqual(self.outcome("query I\nSELECT a\n----\n1\n2\n", result).status, "pass")
        self.assertEqual(self.outcome("query I\nSELECT a\n----\n1\n3\n", result).status, "mismatch")
        self.assertEqual(self.outcome("query I\nSELECT a\n----\n2\n1\n", result).status, "order")

    def test_type_letters_differ(self):
        result = (FakeSchema("DoubleType"), [(1.0,)])
        outcome = self.outcome("query I\nSELECT a\n----\n1\n", result)
        self.assertEqual(outcome.status, "type")
        self.assertIn("DataFusion `I`, Spark `R`", outcome.detail)

    def test_errors(self):
        error = r.SparkError("error", "[DIVIDE_BY_ZERO] Division by zero.")
        self.assertEqual(self.outcome("query I\nSELECT 1/0\n----\nNULL\n", error).status, "spark-error")
        self.assertEqual(self.outcome("query error .*\nSELECT 1/0\n", error).status, "pass")
        rows = (FakeSchema("IntegerType"), [(1,)])
        self.assertEqual(self.outcome("query error .*\nSELECT 1\n", rows).status, "no-spark-error")
        parse_error = r.SparkError("parse", "Spark cannot parse it (PARSE_SYNTAX_ERROR)")
        outcome = self.outcome("query ?\nSELECT [1]\n----\n[1]\n", parse_error)
        self.assertEqual((outcome.status, outcome.detail), ("skip", parse_error.message))

    def test_settings(self):
        runner = FakeRunner()
        [record] = parse("statement ok\nset datafusion.execution.enable_ansi_mode = true;\n")
        self.assertEqual(r.run_record(runner, record).status, "pass")
        self.assertEqual(runner.settings, {"spark.sql.ansi.enabled": "true"})

    def test_zoned_timestamps(self):
        value = datetime.datetime(2024, 7, 15)
        result = (FakeSchema("TimestampType"), [(value,)])
        self.assertEqual(self.outcome("query P\nSELECT ts\n----\n2024-07-15T00:00:00Z\n", result).status, "pass")

    def test_scale(self):
        # Spark reads 1.5 as a DECIMAL, DataFusion as a DOUBLE.
        decimal = spark_type("DecimalType", precision=12, scale=1)
        result = (FakeSchema(decimal), [(Decimal("7.0"),)])
        self.assertEqual(self.outcome("query R\nSELECT try_sum(x)\n----\n7\n", result).status, "scale")

    def test_row_limit(self):
        # A query that DataFusion rejects and Spark answers with many rows.
        result = (FakeSchema("IntegerType"), [(1,)] * (r.MAX_ROWS + 1))
        outcome = self.outcome("query error .*\nSELECT explode(sequence(1, 2000))\n", result)
        self.assertEqual(outcome.status, "no-spark-error")
        self.assertEqual(len(outcome.actual), r.MAX_ROWS + 1)
        self.assertEqual(outcome.actual[-1], f"... (more than {r.MAX_ROWS} rows)")
        self.assertIn("... 981 more line(s)", r.report(outcome, "4.2.0"))

    def test_session_died(self):
        died = type("Py4JNetworkError", (Exception,), {})("Answer from Java side is empty")
        self.assertTrue(r._session_died(died))
        self.assertTrue(r._session_died(ConnectionRefusedError(61, "Connection refused")))
        self.assertFalse(r._session_died(ValueError("bug")))

    def test_out_of_memory_is_skipped(self):
        error = r.SparkError("memory", "Spark ran out of memory")
        outcome = self.outcome("query error .*\nSELECT stack(1, 1)\n", error)
        self.assertEqual((outcome.status, outcome.detail), ("skip", "Spark ran out of memory"))

    def test_large_stack_is_not_run(self):
        runner = FakeRunner()
        [record] = parse("query error .*\nSELECT stack(2000000000, 1)\n")
        outcome = r.run_record(runner, record)
        self.assertEqual((outcome.status, outcome.detail), ("skip", "stack() of more than 1000 rows"))
        self.assertFalse(hasattr(runner, "sql"))
        # Spark rejects a BIGINT row count before running anything, so that one runs.
        [record] = parse("query error .*\nSELECT stack(2147483648, 1)\n")
        error = r.SparkError("error", "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]")
        self.assertEqual(r.run_record(FakeRunner(error), record).status, "pass")


class DyingRunner(FakeRunner):
    """A fake session that fails the way a real one does after an OutOfMemoryError.

    `SELECT die` kills the JVM. `SELECT oom` runs it out of memory, which leaves
    it alive but failing every call, so `reset` raises an internal error.
    """

    version = "4.2.0"

    def __init__(self):
        super().__init__((FakeSchema("IntegerType"), [(1,)]))
        self.dead = False
        self.broken = False
        self.restarts = 0

    def reset(self):
        if self.dead:
            raise ConnectionRefusedError(61, "Connection refused")
        if self.broken:
            raise RuntimeError("An error occurred while calling o49.listTables.\n: [INTERNAL_ERROR]")

    def restart(self):
        self.dead = self.broken = False
        self.restarts += 1

    def run(self, sql):
        if self.dead or sql == "SELECT die":
            self.dead = True
            raise type("Py4JNetworkError", (Exception,), {})("Answer from Java side is empty")
        if sql == "SELECT oom":
            self.broken = True
            raise r.SparkError("memory", "Spark ran out of memory")
        return super().run(sql)


class RunFilesTest(unittest.TestCase):
    QUERY = "query I\nSELECT {}\n----\n1\n\n"

    def run_files(self, *contents):
        with tempfile.TemporaryDirectory() as d:
            paths = []
            for i, queries in enumerate(contents):
                paths.append(Path(d, f"{i}.slt"))
                paths[-1].write_text("".join(self.QUERY.format(q) for q in queries))
            runner = DyingRunner()
            with contextlib.redirect_stdout(io.StringIO()) as out:
                result = r.run_files(runner, paths)
        return runner, result, out.getvalue()

    def test_new_session_after_the_jvm_dies(self):
        runner, (statuses, reasons, stopped), out = self.run_files([1, "die", 1], [1])
        self.assertIsNone(stopped)
        self.assertEqual(runner.restarts, 1)
        self.assertEqual(statuses["pass"], 2)
        self.assertEqual(
            reasons,
            {"the Spark session died while running it": 1, "Spark failed earlier in the file": 1},
        )
        self.assertIn("0.slt:6: the Spark session died", out)

    def test_new_session_after_out_of_memory(self):
        runner, (statuses, reasons, stopped), out = self.run_files([1, "oom", 1], [1])
        self.assertIsNone(stopped)
        self.assertEqual(runner.restarts, 1)
        self.assertEqual(statuses["pass"], 2)
        self.assertEqual(
            reasons, {"Spark ran out of memory": 1, "Spark failed earlier in the file": 1}
        )
        self.assertIn("0.slt:6: Spark ran out of memory", out)

    def test_new_session_when_reset_fails(self):
        # The JVM ran out of memory in a background thread, as in a real run.
        runner = DyingRunner()
        runner.broken = True
        with tempfile.TemporaryDirectory() as d:
            path = Path(d, "0.slt")
            path.write_text(self.QUERY.format(1))
            with contextlib.redirect_stdout(io.StringIO()) as out:
                statuses, _, stopped = r.run_files(runner, [path])
        self.assertIsNone(stopped)
        self.assertEqual((runner.restarts, statuses["pass"]), (1, 1))
        self.assertIn("Spark failed (An error occurred while calling o49.listTables.)", out.getvalue())


if __name__ == "__main__":
    unittest.main()
