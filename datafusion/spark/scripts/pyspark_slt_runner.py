#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

"""Run the datafusion-spark sqllogictest files against Apache Spark 4.2.

Each record in datafusion/sqllogictest/test_files/spark/**/*.slt is run on a
local PySpark session, and Spark's result is compared with the expected result
in the file. The expected results come from DataFusion, so every difference
reported here is a query where DataFusion and Spark disagree, or where the
test expects the wrong value.

This is a manual tool and does not run in CI. See README.md for setup.
"""

from __future__ import annotations

import argparse
import datetime
import math
import os
import re
import struct
import sys
import tempfile
import time
import traceback
from collections import Counter
from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal, localcontext
from pathlib import Path
from typing import Optional

SPARK_VERSION = "4.2"

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[2]
DEFAULT_TEST_DIR = REPO_ROOT / "datafusion" / "sqllogictest" / "test_files" / "spark"

# The label that `skipif` and `onlyif` directives use for this runner.
LABEL = "spark"

# DataFusion runs these files with ANSI mode off and no time zone (UTC), so
# every file starts from the same Spark settings.
SPARK_DEFAULTS = {
    "spark.sql.ansi.enabled": "false",
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.mapKeyDedupPolicy": "EXCEPTION",
}

# DataFusion settings that have a Spark equivalent.
SETTINGS = {
    "datafusion.execution.enable_ansi_mode": "spark.sql.ansi.enabled",
    "datafusion.execution.time_zone": "spark.sql.session.timeZone",
    "datafusion.spark.map_key_dedup_policy": "spark.sql.mapKeyDedupPolicy",
}

# Type names DataFusion accepts and Spark does not.
TYPE_ALIASES = {
    "BYTEA": "BINARY",
    "FLOAT4": "FLOAT",
    "FLOAT8": "DOUBLE",
    "INT2": "SMALLINT",
    "INT4": "INT",
    "INT8": "BIGINT",
    "TEXT": "STRING",
}

# Spark's maximum decimal precision.
MAX_DECIMAL_PRECISION = 38


class Skip(Exception):
    """The record cannot be run on Spark. The message says why."""


class Unsupported(Exception):
    """Spark returned a value of a type this runner does not format."""


# ---------------------------------------------------------------------------
# Parsing .slt files
# ---------------------------------------------------------------------------


@dataclass
class Record:
    """A `statement` or `query` record from an .slt file."""

    path: Path
    line: int
    kind: str  # "statement" or "query"
    sql: str = ""
    expect_error: bool = False
    types: str = ""
    sort_mode: str = "nosort"
    expected: list[str] = field(default_factory=list)
    skip_reason: Optional[str] = None


def parse_slt(path: Path) -> list[Record]:
    """Parse the `statement` and `query` records of an .slt file."""
    lines = path.read_text().split("\n")
    records: list[Record] = []
    conditions: list[tuple[str, str]] = []
    i = 0
    while i < len(lines):
        words = lines[i].split()
        if not words or words[0].startswith("#"):
            i += 1
            continue
        if words[0] in ("skipif", "onlyif") and len(words) > 1:
            conditions.append((words[0], words[1]))
            i += 1
            continue
        if words[0] not in ("statement", "query"):
            # Directives such as `include` and `halt`, which these files do not use.
            conditions = []
            i += 1
            continue

        record = Record(path=path, line=i + 1, kind=words[0])
        args = words[1:]
        if args and args[0] == "error":
            record.expect_error = True
        elif record.kind == "query" and args:
            record.types = args[0]
            if len(args) > 1 and args[1] in ("nosort", "rowsort", "valuesort"):
                record.sort_mode = args[1]
        for directive, label in conditions:
            if directive == "skipif" and label == LABEL:
                record.skip_reason = f"skipif {label}"
            elif directive == "onlyif" and label != LABEL:
                record.skip_reason = f"onlyif {label}"
        conditions = []

        # The SQL runs until a blank line or the `----` separator.
        i += 1
        sql = []
        while i < len(lines) and lines[i].strip() and lines[i] != "----":
            if not lines[i].lstrip().startswith("#"):
                sql.append(lines[i])
            i += 1
        record.sql = "\n".join(sql)

        # Expected results (or an expected error message) run until a blank
        # line. A result line may start with `#`, so it is not a comment here.
        if i < len(lines) and lines[i] == "----":
            i += 1
            while i < len(lines) and lines[i].strip():
                record.expected.append(lines[i])
                i += 1
        records.append(record)
    return records


# ---------------------------------------------------------------------------
# Translating DataFusion SQL to Spark SQL
#
# Spark 4.2 parses most of these files as written, including `::` casts.
# Only a few renames are applied here. Anything else Spark cannot parse, and
# any function Spark does not have (such as `arrow_cast`), is skipped.
# ---------------------------------------------------------------------------

_TOKEN_RE = re.compile(
    r"""
      (?P<string>'(?:[^']|'')*')
    | (?P<quoted>`[^`]*`|"[^"]*")
    | (?P<comment>--[^\n]*)
    | (?P<word>[A-Za-z_][A-Za-z0-9_]*)
    | (?P<number>\d+(?:\.\d*)?(?:[eE][+-]?\d+)?|\.\d+(?:[eE][+-]?\d+)?)
    | (?P<cast>::)
    | (?P<space>\s+)
    | (?P<other>.)
    """,
    re.VERBOSE | re.DOTALL,
)

_COLUMN_RE = re.compile(r"column(\d+)", re.IGNORECASE)


def _tokenize(sql: str) -> list[tuple[str, str]]:
    return [(m.lastgroup, m.group()) for m in _TOKEN_RE.finditer(sql)]


def _next_significant(tokens: list[tuple[str, str]], start: int) -> int:
    """Return the index of the next token that is not whitespace or a comment."""
    i = start
    while i < len(tokens) and tokens[i][0] in ("space", "comment"):
        i += 1
    return i


def _translate_type(tokens: list[tuple[str, str]], i: int) -> str:
    """Translate the type name at tokens[i]."""
    text = tokens[i][1]
    upper = text.upper()
    if upper in TYPE_ALIASES:
        return TYPE_ALIASES[upper]
    j = _next_significant(tokens, i + 1)
    has_args = j < len(tokens) and tokens[j][1] == "("
    if upper == "VARCHAR" and not has_args:
        return "STRING"
    if upper in ("DECIMAL", "DEC", "NUMERIC") and has_args:
        k = _next_significant(tokens, j + 1)
        if k < len(tokens) and tokens[k][0] == "number":
            if int(tokens[k][1]) > MAX_DECIMAL_PRECISION:
                raise Skip(f"DECIMAL precision above {MAX_DECIMAL_PRECISION}")
    return text


def translate(sql: str) -> str:
    """Translate the parts of DataFusion's SQL that Spark spells differently."""
    tokens = _tokenize(sql)
    out = []
    # The function name before each open parenthesis, to find CAST(x AS type).
    calls: list[Optional[str]] = []
    last: Optional[tuple[str, str]] = None
    expect_type = False
    for i, (kind, text) in enumerate(tokens):
        if kind == "string" and "\\" in text:
            raise Skip(
                "a string literal contains a backslash, which Spark reads as an "
                "escape and DataFusion does not"
            )
        if kind in ("space", "comment"):
            out.append(text)
            continue
        if kind == "word":
            if expect_type:
                text = _translate_type(tokens, i)
            else:
                m = _COLUMN_RE.fullmatch(text)
                if m:
                    # Spark names VALUES columns col1, col2, ...
                    text = f"col{m.group(1)}"
            expect_type = text.upper() == "AS" and bool(calls) and calls[-1] in (
                "CAST",
                "TRY_CAST",
            )
        elif kind == "cast":
            expect_type = True
        else:
            expect_type = False
            if text == "(":
                calls.append(last[1].upper() if last and last[0] == "word" else None)
            elif text == ")" and calls:
                calls.pop()
        last = (kind, text)
        out.append(text)
    return "".join(out)


_NAME = r"[A-Za-z_][A-Za-z0-9_]*"
_CREATE_RE = re.compile(
    rf"\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?({_NAME})\s*",
    re.IGNORECASE,
)
_DROP_RE = re.compile(
    rf"\s*DROP\s+(?:TABLE|VIEW)\s+(?:IF\s+EXISTS\s+)?({_NAME})\s*;?\s*$",
    re.IGNORECASE,
)
_AS_RE = re.compile(r"AS\b\s*", re.IGNORECASE)


def _split_top_level(text: str) -> list[str]:
    """Split on commas that are not inside parentheses."""
    parts, depth, start = [], 0, 0
    for i, ch in enumerate(text):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(text[start:i])
            start = i + 1
    parts.append(text[start:])
    return [p.strip() for p in parts]


def translate_statement(sql: str) -> str:
    """Translate a statement, turning DataFusion tables into Spark temporary views."""
    m = _DROP_RE.match(sql)
    if m:
        return f"DROP VIEW IF EXISTS {m.group(1)}"
    m = _CREATE_RE.match(sql)
    if not m:
        return translate(sql)
    name, rest = m.group(1), sql[m.end() :]
    columns = None
    if rest.startswith("("):
        depth = 0
        for end, ch in enumerate(rest):
            depth += {"(": 1, ")": -1}.get(ch, 0)
            if depth == 0:
                break
        columns = _split_top_level(rest[1:end])
        rest = rest[end + 1 :].lstrip()
    m = _AS_RE.match(rest)
    if not m:
        raise Skip("CREATE TABLE without AS")
    query = rest[m.end() :].rstrip().rstrip(";")
    if columns is None:
        return translate(f"CREATE OR REPLACE TEMPORARY VIEW {name} AS {query}")
    names, casts = [], []
    for column in columns:
        parts = column.split(None, 1)
        if len(parts) != 2:
            raise Skip(f"unsupported column definition: {column}")
        names.append(parts[0])
        casts.append(f"CAST({parts[0]} AS {parts[1]}) AS {parts[0]}")
    return translate(
        f"CREATE OR REPLACE TEMPORARY VIEW {name} AS SELECT {', '.join(casts)} "
        f"FROM ({query}) AS __slt_values({', '.join(names)})"
    )


_SET_RE = re.compile(
    r"\s*SET\s+([A-Za-z0-9_.]+)\s*(?:=|\s+TO\s+)\s*(.*?)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_RESET_RE = re.compile(r"\s*RESET\s+([A-Za-z0-9_.]+)\s*;?\s*$", re.IGNORECASE)


def parse_setting(sql: str) -> Optional[tuple[str, Optional[str]]]:
    """Parse `SET key = value` or `RESET key`. A reset has the value None."""
    m = _SET_RE.match(sql)
    if m:
        value = m.group(2)
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        return m.group(1).lower(), value
    m = _RESET_RE.match(sql)
    if m:
        return m.group(1).lower(), None
    return None


# ---------------------------------------------------------------------------
# Formatting Spark values the way DataFusion's sqllogictest engine prints them
#
# Top-level values follow datafusion/sqllogictest/src/engines/conversion.rs and
# datafusion_engine/normalize.rs. Values nested in arrays, maps and structs
# follow arrow-rs's ArrayFormatter.
# ---------------------------------------------------------------------------

_SIMPLE_TYPES = {
    "BinaryType": "binary",
    "BooleanType": "boolean",
    "ByteType": "int",
    "CharType": "string",
    "DateType": "date",
    "DoubleType": "double",
    "FloatType": "float",
    "IntegerType": "int",
    "LongType": "int",
    "NullType": "null",
    "ShortType": "int",
    "StringType": "string",
    "TimeType": "time",
    "TimestampNTZType": "timestamp_ntz",
    "TimestampType": "timestamp",
    "VarcharType": "string",
}

# The type letters DataFusion's sqllogictest engine uses.
_TYPE_LETTERS = {
    "boolean": "B",
    "date": "D",
    "decimal": "R",
    "double": "R",
    "float": "R",
    "int": "I",
    "string": "T",
    "time": "D",
    "timestamp": "P",
    "timestamp_ntz": "P",
}


def type_tree(data_type) -> tuple:
    """Describe a PySpark DataType as nested tuples, so formatting needs no PySpark."""
    name = type(data_type).__name__
    if name == "ArrayType":
        return ("array", type_tree(data_type.elementType))
    if name == "MapType":
        return ("map", type_tree(data_type.keyType), type_tree(data_type.valueType))
    if name == "StructType":
        fields = tuple((f.name, type_tree(f.dataType)) for f in data_type.fields)
        return ("struct", fields)
    if name == "DecimalType":
        return ("decimal", data_type.precision, data_type.scale)
    return (_SIMPLE_TYPES.get(name, name),)


def type_letter(tree: tuple) -> str:
    return _TYPE_LETTERS.get(tree[0], "?")


def _contains_timestamp(tree: tuple) -> bool:
    if tree[0] == "timestamp":
        return True
    if tree[0] == "struct":
        return any(_contains_timestamp(t) for _, t in tree[1])
    return any(isinstance(t, tuple) and _contains_timestamp(t) for t in tree[1:])


def _shortest_f32(value: float) -> str:
    """The shortest decimal text that round-trips to the same 32-bit float."""
    for precision in range(1, 10):
        text = f"{value:.{precision}g}"
        try:
            if struct.unpack("<f", struct.pack("<f", float(text)))[0] == value:
                return text
        except OverflowError:
            pass
    return repr(value)


def _round_float(value: float, single: bool, digits: int) -> str:
    """DataFusion's float_to_str: round, drop trailing zeros, no exponent."""
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    text = _shortest_f32(value) if single else repr(value)
    with localcontext() as ctx:
        ctx.prec = 1000
        rounded = Decimal(text).quantize(Decimal(1).scaleb(-digits), ROUND_HALF_UP)
        text = format(rounded.normalize(), "f")
    # BigDecimal has no negative zero.
    return "0" if text == "-0" else text


def _ryu(value: float, single: bool) -> str:
    """Format a float the way the Rust `ryu` crate does (used by arrow-rs)."""
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "inf" if value > 0 else "-inf"
    if value == 0:
        return "-0.0" if math.copysign(1, value) < 0 else "0.0"
    sign, digit_tuple, exponent = Decimal(_shortest_f32(value) if single else repr(value)).as_tuple()
    digits = "".join(map(str, digit_tuple)).rstrip("0")
    exponent += len(digit_tuple) - len(digits)
    length = len(digits)
    kk = length + exponent
    limit, low = (13, -6) if single else (16, -5)
    if 0 <= exponent and kk <= limit:
        text = digits + "0" * exponent + ".0"
    elif 0 < kk <= limit:
        text = digits[:kk] + "." + digits[kk:]
    elif low < kk <= 0:
        text = "0." + "0" * -kk + digits
    elif length == 1:
        text = f"{digits}e{kk - 1}"
    else:
        text = f"{digits[0]}.{digits[1:]}e{kk - 1}"
    return ("-" if sign else "") + text


def _plain_decimal(value: Decimal, scale: int) -> str:
    with localcontext() as ctx:
        ctx.prec = 1000
        return format(value.quantize(Decimal(1).scaleb(-scale)), "f")


def _fraction(microsecond: int) -> str:
    """chrono prints fractional seconds in groups of 3 digits, and none for zero."""
    if microsecond == 0:
        return ""
    if microsecond % 1000 == 0:
        return f".{microsecond // 1000:03d}"
    return f".{microsecond:06d}"


def _naive_timestamp(value: datetime.datetime) -> str:
    return (
        f"{value.year:04d}-{value.month:02d}-{value.day:02d}T"
        f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}"
        f"{_fraction(value.microsecond)}"
    )


def zone(name: str) -> datetime.tzinfo:
    """Resolve a Spark/DataFusion time zone name or offset such as +05:30."""
    if name.upper() in ("UTC", "Z", "GMT", "+00:00"):
        return datetime.timezone.utc
    m = re.fullmatch(r"([+-])(\d{1,2}):?(\d{2})", name)
    if m:
        offset = datetime.timedelta(hours=int(m.group(2)), minutes=int(m.group(3)))
        return datetime.timezone(-offset if m.group(1) == "-" else offset)
    from zoneinfo import ZoneInfo

    return ZoneInfo(name)


def _local_timestamp(value: datetime.datetime, time_zone: str) -> str:
    """Spark TIMESTAMP values, which PySpark returns as naive UTC (TZ=UTC)."""
    utc = value.replace(tzinfo=datetime.timezone.utc)
    return _naive_timestamp(utc.astimezone(zone(time_zone)).replace(tzinfo=None))


def format_nested(value, tree: tuple, time_zone: str) -> str:
    """Format a value the way arrow-rs's ArrayFormatter displays it."""
    if value is None:
        return "NULL"
    kind = tree[0]
    if kind == "boolean":
        return "true" if value else "false"
    if kind == "int":
        return str(value)
    if kind in ("float", "double"):
        return _ryu(value, kind == "float")
    if kind == "decimal":
        return _plain_decimal(value, tree[2])
    if kind == "string":
        return value
    if kind == "binary":
        return bytes(value).hex()
    if kind == "date":
        return value.isoformat()
    if kind == "timestamp":
        return _local_timestamp(value, time_zone)
    if kind == "timestamp_ntz":
        return _naive_timestamp(value)
    if kind == "time":
        return (
            f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}"
            f"{_fraction(value.microsecond)}"
        )
    if kind == "array":
        return "[" + ", ".join(format_nested(v, tree[1], time_zone) for v in value) + "]"
    if kind == "map":
        entries = (
            f"{format_nested(k, tree[1], time_zone)}: {format_nested(v, tree[2], time_zone)}"
            for k, v in value.items()
        )
        return "{" + ", ".join(entries) + "}"
    if kind == "struct":
        entries = (
            f"{name}: {format_nested(value[i], t, time_zone)}"
            for i, (name, t) in enumerate(tree[1])
        )
        return "{" + ", ".join(entries) + "}"
    if kind == "null":
        return "NULL"
    raise Unsupported(kind)


def format_cell(value, tree: tuple, time_zone: str) -> str:
    """Format a top-level value the way DataFusion's sqllogictest engine prints it."""
    if value is None:
        return "NULL"
    kind = tree[0]
    if kind == "string":
        if value == "":
            return "(empty)"
        return value.rstrip("\n").replace("\x00", "\\0")
    if kind == "float":
        return _round_float(value, single=True, digits=12)
    if kind == "double":
        # The engine keeps 15 decimal places for files under spark/.
        return _round_float(value, single=False, digits=15)
    return format_nested(value, tree, time_zone)


def format_rows(rows, trees: list[tuple], time_zone: str) -> list[str]:
    """Format rows as result lines, splitting a multi-line last cell like the engine."""
    lines = []
    for row in rows:
        cells = [format_cell(value, tree, time_zone) for value, tree in zip(row, trees)]
        parts = cells[-1].split("\n") if cells else []
        if len(parts) < 2:
            lines.append(" ".join(cells).rstrip())
            continue
        lines.append(" ".join(cells[:-1]).rstrip())
        for n, part in enumerate(parts, 1):
            content = part.lstrip()
            indent = "-" * (len(part) - len(content))
            lines.append(f"{n:02d}){indent}{content}".rstrip())
    return lines


_ZONED_TIMESTAMP_RE = re.compile(
    r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})"
)


def localize_timestamps(line: str, time_zone: str) -> str:
    """Rewrite DataFusion timestamps with an offset as wall-clock time in time_zone.

    Spark's TIMESTAMP has no zone of its own, so it is printed in the session
    time zone. DataFusion prints a zoned timestamp in its type's zone with an
    offset (`2024-07-15T00:00:00Z`). This puts both on the same footing.
    """

    def replace(m: re.Match) -> str:
        fraction = m.group(7) or ""
        if len(fraction) > 6 and int(fraction[6:]):
            return m.group(0)
        micro = int(fraction[:6].ljust(6, "0")) if fraction else 0
        offset = datetime.timezone.utc if m.group(8) == "Z" else zone(m.group(8))
        parts = [int(m.group(g)) for g in range(1, 7)]
        value = datetime.datetime(*parts, micro, tzinfo=offset)
        return _naive_timestamp(value.astimezone(zone(time_zone)).replace(tzinfo=None))

    return _ZONED_TIMESTAMP_RE.sub(replace, line)


# ---------------------------------------------------------------------------
# Comparing results
# ---------------------------------------------------------------------------


def _sorted(lines: list[str], sort_mode: str) -> list[str]:
    lines = [line.rstrip() for line in lines]
    if sort_mode == "rowsort":
        return sorted(lines)
    if sort_mode == "valuesort":
        return sorted(value for line in lines for value in line.split())
    return lines


def results_match(expected: list[str], actual: list[str], sort_mode: str) -> bool:
    return _sorted(expected, sort_mode) == _sorted(actual, sort_mode)


def nearly_equal(expected: list[str], actual: list[str], sort_mode: str) -> bool:
    """True if the results differ only in the last digits of floating-point values."""
    expected, actual = _sorted(expected, sort_mode), _sorted(actual, sort_mode)
    if len(expected) != len(actual):
        return False
    for e_line, a_line in zip(expected, actual):
        e_values, a_values = e_line.split(" "), a_line.split(" ")
        if len(e_values) != len(a_values):
            return False
        for e, a in zip(e_values, a_values):
            if e == a:
                continue
            try:
                e_float, a_float = float(e), float(a)
            except ValueError:
                return False
            if not math.isclose(e_float, a_float, rel_tol=1e-12, abs_tol=1e-15):
                return False
    return True


# ---------------------------------------------------------------------------
# Running records on Spark
# ---------------------------------------------------------------------------


class SparkError(Exception):
    """Spark rejected or failed a query. `kind` is parse, function, table or error."""

    def __init__(self, kind: str, message: str):
        super().__init__(message)
        self.kind = kind
        self.message = message


class SparkRunner:
    """A local Spark session reset to SPARK_DEFAULTS before each file."""

    def __init__(self):
        # A SPARK_HOME for another Spark version breaks the pip-installed
        # PySpark with "'JavaPackage' object is not callable".
        os.environ.pop("SPARK_HOME", None)
        # PySpark returns TIMESTAMP values in the Python process's time zone.
        os.environ["TZ"] = "UTC"
        if hasattr(time, "tzset"):
            time.tzset()

        install = f"pip install -r {SCRIPT_DIR / 'requirements.txt'}"
        try:
            import pyspark
            from pyspark.sql import SparkSession
        except ImportError:
            raise SystemExit(f"error: PySpark is not installed. Install it with `{install}`.")

        if not pyspark.__version__.startswith(SPARK_VERSION + "."):
            raise SystemExit(
                f"error: this runner needs PySpark {SPARK_VERSION}, found "
                f"{pyspark.__version__}. Install it with `{install}`."
            )
        builder = (
            SparkSession.builder.master("local[1]")
            .appName("datafusion-pyspark-slt-runner")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.warehouse.dir", tempfile.mkdtemp(prefix="pyspark-slt-"))
        )
        for key, value in SPARK_DEFAULTS.items():
            builder = builder.config(key, value)
        self.spark = builder.getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        self.version = self.spark.version
        self.time_zone = SPARK_DEFAULTS["spark.sql.session.timeZone"]

    def reset(self) -> None:
        for key, value in SPARK_DEFAULTS.items():
            self.spark.conf.set(key, value)
        self.time_zone = SPARK_DEFAULTS["spark.sql.session.timeZone"]
        for table in self.spark.catalog.listTables():
            if table.isTemporary:
                self.spark.catalog.dropTempView(table.name)

    def set(self, key: str, value: Optional[str]) -> None:
        if value is None:
            value = SPARK_DEFAULTS[key]
        elif key == "spark.sql.ansi.enabled":
            value = value.lower()
        self.spark.conf.set(key, value)
        if key == "spark.sql.session.timeZone":
            self.time_zone = value

    def run(self, sql: str):
        """Run SQL and return (schema, rows), or raise SparkError."""
        from py4j.protocol import Py4JJavaError
        from pyspark.errors import ParseException, PySparkException

        try:
            df = self.spark.sql(sql)
            return df.schema, df.collect()
        except ParseException as e:
            raise SparkError("parse", f"Spark cannot parse it ({_condition(e)})")
        except PySparkException as e:
            condition = _condition(e)
            if condition == "UNRESOLVED_ROUTINE":
                name = _parameter(e, "routineName") or "?"
                raise SparkError("function", f"Spark has no function {name}")
            if condition == "TABLE_OR_VIEW_NOT_FOUND":
                raise SparkError("table", "needs a table that Spark could not create")
            message = str(e).strip().split("\n")[0]
            raise SparkError("error", message)
        except Py4JJavaError as e:
            raise SparkError("error", str(e.java_exception).strip().split("\n")[0])


def _condition(e) -> Optional[str]:
    getter = getattr(e, "getCondition", None) or getattr(e, "getErrorClass", None)
    return getter() if getter else None


def _parameter(e, name: str) -> Optional[str]:
    getter = getattr(e, "getMessageParameters", None)
    parameters = (getter() if getter else None) or {}
    return parameters.get(name)


@dataclass
class Outcome:
    record: Record
    status: str
    detail: str = ""
    spark_sql: Optional[str] = None
    actual: list[str] = field(default_factory=list)


# Statuses that mean DataFusion and Spark disagree, in report order.
DIVERGENT = {
    "mismatch": "results differ",
    "spark-error": "Spark raises an error",
    "no-spark-error": "DataFusion expects an error, Spark returns a result",
    "type": "results match, column types differ",
    "close": "results differ only in floating-point digits",
    "order": "same rows in a different order",
    "runner-error": "the runner failed",
}


def run_record(runner: SparkRunner, record: Record) -> Outcome:
    if record.skip_reason:
        return Outcome(record, "skip", record.skip_reason)
    setting = parse_setting(record.sql) if record.kind == "statement" else None
    if setting is not None:
        key, value = setting
        if key not in SETTINGS:
            return Outcome(record, "skip", f"no Spark equivalent for {key}")
        runner.set(SETTINGS[key], value)
        return Outcome(record, "pass")
    try:
        if record.kind == "statement":
            spark_sql = translate_statement(record.sql)
        else:
            spark_sql = translate(record.sql)
    except Skip as e:
        return Outcome(record, "skip", str(e))
    if any("<slt:ignore>" in line for line in record.expected):
        return Outcome(record, "skip", "uses <slt:ignore>")

    try:
        schema, rows = runner.run(spark_sql)
    except SparkError as e:
        if e.kind != "error":
            return Outcome(record, "skip", e.message, spark_sql)
        if record.expect_error:
            return Outcome(record, "pass", spark_sql=spark_sql)
        return Outcome(record, "spark-error", e.message, spark_sql)

    trees = [type_tree(f.dataType) for f in schema.fields]
    try:
        actual = format_rows(rows, trees, runner.time_zone)
    except Unsupported as e:
        return Outcome(record, "skip", f"Spark returned a {e} value", spark_sql)
    if record.expect_error:
        return Outcome(record, "no-spark-error", spark_sql=spark_sql, actual=actual)
    if record.kind == "statement":
        return Outcome(record, "pass", spark_sql=spark_sql)

    expected = record.expected
    if not results_match(expected, actual, record.sort_mode) and any(
        _contains_timestamp(t) for t in trees
    ):
        expected = [localize_timestamps(line, runner.time_zone) for line in expected]
    if results_match(expected, actual, record.sort_mode):
        letters = "".join(type_letter(t) for t in trees)
        if letters != record.types:
            detail = f"DataFusion `{record.types}`, Spark `{letters}` ({schema.simpleString()})"
            return Outcome(record, "type", detail, spark_sql, actual)
        return Outcome(record, "pass", spark_sql=spark_sql)
    if nearly_equal(expected, actual, record.sort_mode):
        status = "close"
    elif results_match(expected, actual, "rowsort"):
        status = "order"
    else:
        status = "mismatch"
    return Outcome(record, status, spark_sql=spark_sql, actual=actual)


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def _display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _indent(lines, prefix: str = "    ") -> str:
    lines = list(lines) or ["(no rows)"]
    return "\n".join(prefix + line for line in lines)


def report(outcome: Outcome, spark_version: str) -> str:
    record = outcome.record
    location = f"{_display_path(record.path)}:{record.line}"
    if outcome.status == "skip":
        return f"{location}: skipped: {outcome.detail}"
    if outcome.status == "pass":
        return f"{location}: pass"
    parts = [f"{location}: {DIVERGENT[outcome.status]}", _indent(record.sql.split("\n"))]
    if outcome.spark_sql and outcome.spark_sql != record.sql:
        parts += ["  Spark SQL:", _indent(outcome.spark_sql.split("\n"))]
    if outcome.status in ("mismatch", "close", "order"):
        parts += ["  DataFusion (expected):", _indent(record.expected)]
        parts += [f"  Spark {spark_version}:", _indent(outcome.actual)]
    elif outcome.status == "no-spark-error":
        parts += [f"  Spark {spark_version}:", _indent(outcome.actual)]
    else:
        parts.append(f"  {outcome.detail}")
    return "\n".join(parts)


def discover(paths: list[str]) -> list[Path]:
    files = []
    for name in paths:
        path = Path(name)
        if path.is_dir():
            files.extend(sorted(path.rglob("*.slt")))
        elif path.is_file():
            files.append(path)
        else:
            raise SystemExit(f"error: {name} is not a file or directory")
    return files


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            f"Run the datafusion-spark .slt files on Spark {SPARK_VERSION} and "
            "report where Spark disagrees with the expected results."
        )
    )
    parser.add_argument(
        "paths",
        nargs="*",
        default=[str(DEFAULT_TEST_DIR)],
        help=".slt files or directories (default: all of test_files/spark)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="also list skipped and passing records",
    )
    args = parser.parse_args(argv)
    files = discover(args.paths)
    if not files:
        raise SystemExit("error: no .slt files found")

    runner = SparkRunner()
    print(f"Running {len(files)} file(s) on Spark {runner.version}\n")
    statuses: Counter = Counter()
    skip_reasons: Counter = Counter()
    for path in files:
        runner.reset()
        for record in parse_slt(path):
            try:
                outcome = run_record(runner, record)
            except Exception:
                # A bug in this runner must never look like Spark behavior.
                detail = traceback.format_exc(limit=1).strip().split("\n")[-1]
                outcome = Outcome(record, "runner-error", detail)
            statuses[outcome.status] += 1
            if outcome.status == "skip":
                skip_reasons[outcome.detail] += 1
            if outcome.status in DIVERGENT:
                print(report(outcome, runner.version) + "\n")
            elif args.verbose:
                print(report(outcome, runner.version))

    total = sum(statuses.values())
    print(f"Spark {runner.version}: {total} records in {len(files)} file(s)")
    print(f"  {statuses['pass']:6d}  pass")
    for status, description in DIVERGENT.items():
        if statuses[status]:
            print(f"  {statuses[status]:6d}  {description}")
    print(f"  {statuses['skip']:6d}  skipped")
    for reason, count in skip_reasons.most_common(15):
        print(f"  {count:14d}  {reason}")
    if len(skip_reasons) > 15:
        print(f"  {'':>14}  ... {len(skip_reasons) - 15} more reasons (use --verbose)")
    return 1 if any(statuses[s] for s in DIVERGENT) else 0


if __name__ == "__main__":
    sys.exit(main())
