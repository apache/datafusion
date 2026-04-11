#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Validate hardcoded expected values in .slt (sqllogictest) test files
by running the same queries against PySpark and comparing results.

Usage:
    python validate_slt.py                          # Run all .slt files
    python validate_slt.py --path math/abs.slt      # Single file
    python validate_slt.py --path string/           # All files in subdirectory
    python validate_slt.py --verbose                 # Show details
    python validate_slt.py --show-skipped            # Show skipped queries
"""

import argparse
import math
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Arrow type -> Spark type mapping
# ---------------------------------------------------------------------------
ARROW_TO_SPARK_TYPE = {
    "Int8": "TINYINT",
    "Int16": "SMALLINT",
    "Int32": "INT",
    "Int64": "BIGINT",
    "UInt8": "SMALLINT",
    "UInt16": "INT",
    "UInt32": "BIGINT",
    "UInt64": "BIGINT",
    "Float16": "FLOAT",
    "Float32": "FLOAT",
    "Float64": "DOUBLE",
    "Utf8": "STRING",
    "Boolean": "BOOLEAN",
    "Binary": "BINARY",
    "Date32": "DATE",
    "Date64": "DATE",
    "Utf8View": "STRING",
    "LargeUtf8": "STRING",
    "BinaryView": "BINARY",
    "LargeBinary": "BINARY",
}

# DataFusion cast type -> Spark type mapping
DF_TO_SPARK_CAST_TYPE = {
    "TINYINT": "TINYINT",
    "SMALLINT": "SMALLINT",
    "INT": "INT",
    "INTEGER": "INT",
    "BIGINT": "BIGINT",
    "FLOAT": "FLOAT",
    "REAL": "FLOAT",
    "DOUBLE": "DOUBLE",
    "STRING": "STRING",
    "VARCHAR": "STRING",
    "TEXT": "STRING",
    "BOOLEAN": "BOOLEAN",
    "BINARY": "BINARY",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP",
    # PostgreSQL-style aliases used in some .slt files
    "FLOAT8": "DOUBLE",
    "FLOAT4": "FLOAT",
    "INT8": "BIGINT",
    "INT4": "INT",
    "INT2": "SMALLINT",
    "BYTEA": "BINARY",
}

# Unsupported Arrow types for Spark (no direct equivalent)
UNSUPPORTED_ARROW_TYPES: set[str] = set()

# ---------------------------------------------------------------------------
# SLT record types
# ---------------------------------------------------------------------------


@dataclass
class QueryRecord:
    """A 'query <TYPE_CODES> [rowsort]' block."""

    type_codes: str
    sql: str
    expected: list[str]
    rowsort: bool
    line_number: int
    in_ansi_block: bool = False


@dataclass
class ErrorRecord:
    """A 'query error <pattern>' or 'statement error <pattern>' block."""

    pattern: str
    sql: str
    line_number: int
    kind: str = "query"  # "query" or "statement"
    in_ansi_block: bool = False


@dataclass
class StatementRecord:
    """A 'statement ok' block (DDL/config)."""

    sql: str
    line_number: int
    in_ansi_block: bool = False


# ---------------------------------------------------------------------------
# 1. SLT Parser
# ---------------------------------------------------------------------------


def parse_slt(filepath: str) -> list:
    """Parse an .slt file into a list of records."""
    with open(filepath) as f:
        lines = f.readlines()

    records = []
    i = 0
    in_ansi_mode = False

    while i < len(lines):
        line = lines[i].rstrip("\n")

        # Skip blank lines and comments
        if not line.strip() or line.strip().startswith("#"):
            i += 1
            continue

        # query error <pattern>
        m = re.match(r"^query\s+error\s+(.*)", line)
        if m:
            pattern = m.group(1).strip()
            line_num = i + 1
            i += 1
            sql_lines = []
            while i < len(lines) and lines[i].strip() and not lines[i].strip().startswith("#"):
                stripped = lines[i].rstrip("\n")
                if (
                    re.match(r"^query\s", stripped)
                    or re.match(r"^statement\s", stripped)
                ):
                    break
                sql_lines.append(stripped)
                i += 1
            records.append(
                ErrorRecord(
                    pattern=pattern,
                    sql="\n".join(sql_lines),
                    line_number=line_num,
                    kind="query",
                    in_ansi_block=in_ansi_mode,
                )
            )
            continue

        # statement error <pattern>
        m = re.match(r"^statement\s+error\s*(.*)", line)
        if m:
            pattern = m.group(1).strip()
            line_num = i + 1
            i += 1
            sql_lines = []
            while i < len(lines) and lines[i].strip() and not lines[i].strip().startswith("#"):
                stripped = lines[i].rstrip("\n")
                if (
                    re.match(r"^query\s", stripped)
                    or re.match(r"^statement\s", stripped)
                ):
                    break
                sql_lines.append(stripped)
                i += 1
            records.append(
                ErrorRecord(
                    pattern=pattern,
                    sql="\n".join(sql_lines),
                    line_number=line_num,
                    kind="statement",
                    in_ansi_block=in_ansi_mode,
                )
            )
            continue

        # statement ok
        m = re.match(r"^statement\s+ok\s*$", line)
        if m:
            line_num = i + 1
            i += 1
            sql_lines = []
            while i < len(lines) and lines[i].strip() and not lines[i].strip().startswith("#"):
                stripped = lines[i].rstrip("\n")
                if (
                    re.match(r"^query\s", stripped)
                    or re.match(r"^statement\s", stripped)
                ):
                    break
                sql_lines.append(stripped)
                i += 1
            sql = "\n".join(sql_lines)

            # Track ANSI mode from statements
            if re.search(
                r"set\s+datafusion\.execution\.enable_ansi_mode\s*=\s*true",
                sql,
                re.IGNORECASE,
            ):
                in_ansi_mode = True
            elif re.search(
                r"set\s+datafusion\.execution\.enable_ansi_mode\s*=\s*false",
                sql,
                re.IGNORECASE,
            ):
                in_ansi_mode = False

            records.append(
                StatementRecord(
                    sql=sql, line_number=line_num, in_ansi_block=in_ansi_mode
                )
            )
            continue

        # query <TYPE_CODES> [rowsort]
        m = re.match(r"^query\s+(\S+)(\s+rowsort)?\s*$", line)
        if m:
            type_codes = m.group(1)
            rowsort = m.group(2) is not None
            line_num = i + 1
            i += 1

            # Collect SQL lines until ----
            sql_lines = []
            while i < len(lines) and lines[i].rstrip("\n") != "----":
                sql_lines.append(lines[i].rstrip("\n"))
                i += 1

            # Skip the ---- separator
            if i < len(lines) and lines[i].rstrip("\n") == "----":
                i += 1

            # Collect expected result lines until blank line or next record.
            # Note: do NOT treat # as a comment here — result values can
            # start with # (e.g., soundex('#') -> '#').
            expected = []
            while i < len(lines):
                result_line = lines[i].rstrip("\n")
                if result_line == "":
                    i += 1
                    break
                if re.match(r"^(query|statement)\s", result_line):
                    break
                # A ## comment line in the results section signals end of results
                if result_line.startswith("##"):
                    break
                expected.append(result_line)
                i += 1

            records.append(
                QueryRecord(
                    type_codes=type_codes,
                    sql="\n".join(sql_lines),
                    expected=expected,
                    rowsort=rowsort,
                    line_number=line_num,
                    in_ansi_block=in_ansi_mode,
                )
            )
            continue

        # Unknown line, skip
        i += 1

    return records


# ---------------------------------------------------------------------------
# 2. SQL Translator (DataFusion -> PySpark)
# ---------------------------------------------------------------------------


def _translate_cast_type(df_type: str) -> Optional[str]:
    """Map a DataFusion type name to a Spark SQL type name."""
    upper = df_type.upper().strip()
    # Direct match
    if upper in DF_TO_SPARK_CAST_TYPE:
        return DF_TO_SPARK_CAST_TYPE[upper]
    # DECIMAL(p, s) - skip if precision > 38 (Spark max)
    if upper.startswith("DECIMAL"):
        m = re.match(r"DECIMAL\(\s*(\d+)", upper)
        if m and int(m.group(1)) > 38:
            raise _SkipQuery(f"Decimal precision {m.group(1)} exceeds Spark max of 38")
        return df_type  # pass through
    # Unknown type - skip rather than produce a misleading test failure
    raise _SkipQuery(f"unsupported cast type: {df_type}")


class _SkipQuery(Exception):
    """Signal that a query should be skipped."""

    pass


def _replace_arrow_cast_nested(sql: str) -> str:
    """Replace arrow_cast(...) handling nested parentheses properly."""
    result = []
    i = 0
    while i < len(sql):
        if sql[i:].startswith("arrow_cast("):
            start = i
            i += len("arrow_cast(")
            depth = 1
            inner_start = i
            while i < len(sql) and depth > 0:
                if sql[i] == "(":
                    depth += 1
                elif sql[i] == ")":
                    depth -= 1
                i += 1
            inner = sql[inner_start : i - 1]

            # Find the last top-level comma (outside parens and quotes)
            depth = 0
            in_quote = False
            last_comma = -1
            for idx, ch in enumerate(inner):
                if ch in ("'", '"') and not in_quote:
                    in_quote = ch
                elif ch == in_quote:
                    in_quote = False
                elif not in_quote:
                    if ch == "(":
                        depth += 1
                    elif ch == ")":
                        depth -= 1
                    elif ch == "," and depth == 0:
                        last_comma = idx

            if last_comma == -1:
                result.append(sql[start:i])
                continue

            expr = inner[:last_comma].strip()
            arrow_type_raw = inner[last_comma + 1 :].strip().strip("'\"")

            if arrow_type_raw in UNSUPPORTED_ARROW_TYPES:
                raise _SkipQuery(f"unsupported Arrow type: {arrow_type_raw}")
            if arrow_type_raw.startswith("Dictionary("):
                raise _SkipQuery(f"unsupported Arrow type: {arrow_type_raw}")
            if arrow_type_raw.startswith(("LargeList(", "FixedSizeList(")):
                raise _SkipQuery(f"unsupported Arrow type: {arrow_type_raw}")
            if arrow_type_raw.startswith("List("):
                # List(X) -> ARRAY<spark_type> - skip for now
                raise _SkipQuery(f"unsupported Arrow type: {arrow_type_raw}")

            # Handle Decimal types: Decimal32(p,s), Decimal64(p,s),
            # Decimal128(p,s), Decimal256(p,s) -> DECIMAL(p,s)
            decimal_match = re.match(
                r"Decimal(?:32|64|128|256)\((\d+),\s*(\d+)\)", arrow_type_raw
            )
            if decimal_match:
                p, s = decimal_match.group(1), decimal_match.group(2)
                if int(p) > 38:
                    raise _SkipQuery(f"Decimal precision {p} exceeds Spark max of 38")
                spark_type = f"DECIMAL({p}, {s})"
            else:
                spark_type = ARROW_TO_SPARK_TYPE.get(arrow_type_raw)
            if spark_type is None:
                raise _SkipQuery(f"unmapped Arrow type: {arrow_type_raw}")

            result.append(f"CAST({expr} AS {spark_type})")
        else:
            result.append(sql[i])
            i += 1

    return "".join(result)


def _translate_casts(sql: str) -> str:
    """Translate DataFusion :: cast syntax to Spark CAST() syntax."""
    # Order matters: most specific patterns first

    result = sql
    changed = True
    while changed:
        changed = False

        # 1. Parenthesized expressions: (expr)::TYPE
        # Walk through looking for ):: and then find matching (
        i = 0
        while i < len(result):
            if result[i] == ")" and result[i + 1 : i + 3] == "::":
                # Walk backwards to find matching (
                depth = 0
                j = i
                while j >= 0:
                    if result[j] == ")":
                        depth += 1
                    elif result[j] == "(":
                        depth -= 1
                        if depth == 0:
                            break
                    j -= 1
                if j >= 0 and depth == 0:
                    # Check if ( is preceded by a function name
                    func_start = j
                    while func_start > 0 and (result[func_start - 1].isalnum() or result[func_start - 1] == "_"):
                        func_start -= 1

                    paren_expr = result[j + 1 : i]
                    # Extract type after ::
                    type_start = i + 3
                    type_end = type_start
                    while type_end < len(result) and (
                        result[type_end].isalnum() or result[type_end] == "_"
                    ):
                        type_end += 1
                    # Check for DECIMAL(p,s) style
                    if type_end < len(result) and result[type_end] == "(":
                        paren_depth = 1
                        type_end += 1
                        while type_end < len(result) and paren_depth > 0:
                            if result[type_end] == "(":
                                paren_depth += 1
                            elif result[type_end] == ")":
                                paren_depth -= 1
                            type_end += 1

                    cast_type = result[i + 3 : type_end]
                    spark_type = _translate_cast_type(cast_type)

                    if func_start < j:
                        # func(...)::TYPE -> CAST(func(...) AS TYPE)
                        func_call = result[func_start : i + 1]
                        replacement = f"CAST({func_call} AS {spark_type})"
                        result = result[:func_start] + replacement + result[type_end:]
                    else:
                        # (expr)::TYPE -> CAST(expr AS TYPE)
                        replacement = f"CAST({paren_expr} AS {spark_type})"
                        result = result[:j] + replacement + result[type_end:]
                    changed = True
                    break
            i += 1
        if changed:
            continue

        # 2. String literals: 'val'::TYPE (handles escaped quotes like 'Andy''s')
        m = re.search(r"'((?:[^']|'')*)'::(\w+(?:\([^)]*\))?)", result)
        if m:
            cast_type = m.group(2)
            spark_type = _translate_cast_type(cast_type)
            replacement = f"CAST('{m.group(1)}' AS {spark_type})"
            result = result[: m.start()] + replacement + result[m.end() :]
            changed = True
            continue

        # 3. Numbers (including negative, scientific notation): -?123::TYPE or 1.23e10::TYPE
        m = re.search(r"(?<![.\w])(-?\d+\.?\d*(?:[eE][+-]?\d+)?)::(\w+(?:\([^)]*\))?)", result)
        if m:
            cast_type = m.group(2)
            spark_type = _translate_cast_type(cast_type)
            replacement = f"CAST({m.group(1)} AS {spark_type})"
            result = result[: m.start()] + replacement + result[m.end() :]
            changed = True
            continue

        # 4. NULL::TYPE
        m = re.search(r"\bNULL::(\w+(?:\([^)]*\))?(?:\[\])?)", result, re.IGNORECASE)
        if m:
            cast_type = m.group(1)
            # Handle array types like int[]
            if cast_type.endswith("[]"):
                # Skip - Spark doesn't have this syntax
                raise _SkipQuery(f"unsupported cast type: {cast_type}")
            spark_type = _translate_cast_type(cast_type)
            replacement = f"CAST(NULL AS {spark_type})"
            result = result[: m.start()] + replacement + result[m.end() :]
            changed = True
            continue

        # 5. Identifiers: col::TYPE
        m = re.search(r"(?<![:\w])(\w+)::(\w+(?:\([^)]*\))?)", result)
        if m:
            ident = m.group(1)
            cast_type = m.group(2)
            spark_type = _translate_cast_type(cast_type)
            replacement = f"CAST({ident} AS {spark_type})"
            result = result[: m.start()] + replacement + result[m.end() :]
            changed = True
            continue

    return result


def _translate_make_array(sql: str) -> str:
    """Translate make_array(...) -> array(...)."""
    return sql.replace("make_array(", "array(")


def _translate_array_literals(sql: str) -> str:
    """Translate SQL array literal syntax [...] -> array(...).

    Handles nested arrays like [[1,2],[3,4]] -> array(array(1,2),array(3,4)).
    Only translates square brackets that are array literals, not array subscripts
    like column[0]. Skips content inside string literals.
    """
    result = []
    i = 0
    while i < len(sql):
        # Skip string literals (handling escaped quotes like 'Andy''s')
        if sql[i] == "'":
            result.append(sql[i])
            i += 1
            while i < len(sql):
                if sql[i] == "'" and i + 1 < len(sql) and sql[i + 1] == "'":
                    result.append(sql[i])
                    result.append(sql[i + 1])
                    i += 2
                elif sql[i] == "'":
                    break
                else:
                    result.append(sql[i])
                    i += 1
            if i < len(sql):
                result.append(sql[i])
                i += 1
            continue
        if sql[i] == "[":
            # Check if this is an array subscript (preceded by identifier or ])
            if i > 0 and (sql[i - 1].isalnum() or sql[i - 1] == "_" or sql[i - 1] == ")"):
                result.append(sql[i])
                i += 1
                continue
            # Array literal: find matching ]
            depth = 1
            i += 1
            inner_start = i
            while i < len(sql) and depth > 0:
                if sql[i] == "[":
                    depth += 1
                elif sql[i] == "]":
                    depth -= 1
                i += 1
            inner = sql[inner_start : i - 1]
            # Recursively translate nested array literals
            inner = _translate_array_literals(inner)
            result.append(f"array({inner})")
        else:
            result.append(sql[i])
            i += 1
    return "".join(result)


def _translate_decimal_literals(sql: str) -> str:
    """Translate DECIMAL(p,s) 'value' literal syntax -> CAST('value' AS DECIMAL(p,s))."""
    return re.sub(
        r"DECIMAL\((\d+\s*,\s*\d+)\)\s+'([^']*)'",
        r"CAST('\2' AS DECIMAL(\1))",
        sql,
        flags=re.IGNORECASE,
    )


def _translate_column_names(sql: str) -> str:
    """Translate DataFusion column1/column2 names to PySpark col1/col2."""
    return re.sub(r"\bcolumn(\d+)\b", r"col\1", sql)


# Type aliases that appear in CAST() expressions but aren't standard Spark SQL
_TYPE_ALIAS_PATTERN = re.compile(
    r"\bCAST\((.+?)\s+AS\s+(FLOAT8|FLOAT4|INT8|INT4|INT2|BYTEA)\b",
    re.IGNORECASE,
)


def _translate_type_aliases_in_cast(sql: str) -> str:
    """Translate non-standard type aliases inside CAST() expressions."""
    def replace_alias(m):
        expr = m.group(1)
        alias = m.group(2).upper()
        spark_type = DF_TO_SPARK_CAST_TYPE.get(alias, alias)
        return f"CAST({expr} AS {spark_type}"
    return _TYPE_ALIAS_PATTERN.sub(replace_alias, sql)


def translate_sql(sql: str) -> tuple[str, Optional[str]]:
    """Translate DataFusion SQL to PySpark SQL.

    Returns (translated_sql, skip_reason). If skip_reason is not None,
    the query should be skipped.
    """
    # Skip queries using DataFusion-specific functions
    if "spark_cast(" in sql.lower():
        return sql, "uses spark_cast()"
    if "arrow_typeof(" in sql.lower():
        return sql, "uses arrow_typeof()"
    # bitwise_not is DataFusion's name; Spark uses bitnot() or ~ operator
    if "bitwise_not(" in sql.lower():
        return sql, "uses bitwise_not() (DataFusion-specific name)"
    # Functions only available in Spark 4.0+
    if spark_version() < (4, 0):
        for func_name in ("try_parse_url", "try_url_decode"):
            if func_name + "(" in sql.lower():
                return sql, f"uses {func_name}() (requires Spark 4.0+)"
    # Skip DataFusion config statements
    if re.search(r"set\s+datafusion\.", sql, re.IGNORECASE):
        return sql, "DataFusion config statement"

    try:
        result = sql

        # Translate arrow_cast first (before :: casts)
        if "arrow_cast(" in result:
            result = _replace_arrow_cast_nested(result)

        # Translate :: cast syntax
        result = _translate_casts(result)

        # Translate make_array
        result = _translate_make_array(result)

        # Translate array literal syntax [...] -> array(...)
        result = _translate_array_literals(result)

        # Translate DECIMAL(p,s) 'value' literal syntax -> CAST('value' AS DECIMAL(p,s))
        result = _translate_decimal_literals(result)

        # Translate column1/column2 -> col1/col2 (PySpark naming for VALUES columns)
        result = _translate_column_names(result)

        # Translate type aliases inside CAST() expressions (e.g., CAST(x AS FLOAT8) -> CAST(x AS DOUBLE))
        result = _translate_type_aliases_in_cast(result)

        return result, None
    except _SkipQuery as e:
        return sql, str(e)


# ---------------------------------------------------------------------------
# 3. PySpark Runner and Result Formatter
# ---------------------------------------------------------------------------

_spark_session = None
_spark_version: Optional[tuple[int, ...]] = None


def get_spark():
    """Create or return a local SparkSession."""
    global _spark_session, _spark_version
    if _spark_session is None:
        from pyspark.sql import SparkSession

        _spark_session = (
            SparkSession.builder.master("local[1]")
            .appName("slt-validator")
            .config("spark.sql.ansi.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.log.level", "WARN")
            .getOrCreate()
        )
        # Suppress Spark logging
        _spark_session.sparkContext.setLogLevel("WARN")
        # Detect Spark version
        ver_str = _spark_session.version  # e.g. "3.5.8" or "4.0.2"
        _spark_version = tuple(int(x) for x in ver_str.split(".")[:2])
    return _spark_session


def spark_version() -> tuple[int, ...]:
    """Return the Spark major.minor version as a tuple, e.g. (4, 0)."""
    if _spark_version is None:
        get_spark()
    return _spark_version


_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?$")
_FLOAT_RE = re.compile(r"^-?\d+\.0$")
_DECIMAL_TRAILING_ZEROS_RE = re.compile(r"^(-?\d+\.\d*?)0+$")
_DECIMAL_SCIENTIFIC_ZERO_RE = re.compile(r"^0E-?\d+$")


def _normalize_numeric_string(val: str) -> str:
    """Normalize Spark's cast-to-string output for numeric types to match .slt.

    Spark's cast(numeric AS string) preserves scale for decimals (0 -> 0.00)
    and trailing .0 for floats (1.0 instead of 1). The .slt files strip these.
    """
    # 0E-10, 0E+5, etc. -> 0
    if _DECIMAL_SCIENTIFIC_ZERO_RE.match(val):
        return "0"
    # 1.0 -> 1 (float/double whole numbers)
    if _FLOAT_RE.match(val):
        return val[:-2]  # strip .0
    # 99999999.990000 -> 99999999.99 (strip trailing zeros after decimal)
    m = _DECIMAL_TRAILING_ZEROS_RE.match(val)
    if m:
        result = m.group(1)
        if result.endswith("."):
            return result[:-1]  # 0. -> 0
        return result
    return val


def format_value(val) -> str:
    """Format a single value to match .slt conventions.

    All values arrive as strings (from Spark's cast-to-string), None, or
    occasionally native Python types for complex columns.
    """
    if val is None:
        return "NULL"
    if isinstance(val, str):
        if val == "":
            return "(empty)"
        # Normalize timestamp format: Spark uses space separator,
        # .slt uses T separator (e.g., 2018-03-13 04:18:23 -> 2018-03-13T04:18:23)
        if _TIMESTAMP_RE.match(val):
            return val.replace(" ", "T", 1)
        # Normalize timestamps inside complex types (arrays, maps)
        # e.g., [2001-09-28 01:00:00, ...] -> [2001-09-28T01:00:00, ...]
        val = re.sub(
            r"(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}(?:\.\d+)?)",
            r"\1T\2",
            val,
        )
        # Normalize lowercase null -> NULL inside complex types
        val = re.sub(r"\bnull\b", "NULL", val)
        # Normalize numeric strings (float .0, decimal trailing zeros)
        return _normalize_numeric_string(val)
    # Fallbacks for non-string types (complex columns that Spark
    # cast-to-string may still return as native Python types)
    if isinstance(val, bool):
        return "true" if val else "false"
    if isinstance(val, float):
        if math.isnan(val):
            return "NaN"
        if math.isinf(val):
            return "Infinity" if val > 0 else "-Infinity"
        if val == int(val):
            return str(int(val))
        return str(val)
    if isinstance(val, (list, tuple)):
        inner = ", ".join(format_value(v) for v in val)
        return f"[{inner}]"
    if isinstance(val, dict):
        entries = ", ".join(
            f"{format_value(k)}: {format_value(v)}" for k, v in val.items()
        )
        return "{" + entries + "}"
    if isinstance(val, (bytes, bytearray)):
        try:
            return val.decode("utf-8")
        except UnicodeDecodeError:
            return val.hex()
    return str(val)


def format_result(rows, num_cols: int) -> list[str]:
    """Format PySpark result rows to match .slt output format.

    Returns list of strings, one per output line.
    """
    if not rows:
        return []

    result_lines = []
    for row in rows:
        values = []
        for i in range(num_cols):
            values.append(format_value(row[i]))
        result_lines.append(" ".join(values))

    return result_lines


def run_query(sql: str, num_cols: int) -> tuple[Optional[list[str]], Optional[str]]:
    """Run a query against PySpark and return formatted results.

    Returns (result_lines, error_message).

    Casts all columns to string inside Spark before collecting to avoid
    PySpark's collect() converting timestamps to the local Python timezone.
    """
    spark = get_spark()
    try:
        df = spark.sql(sql)
        # Cast all columns to string inside Spark to preserve Spark's
        # internal representation (especially for timestamps which
        # collect() would convert to local Python timezone).
        # Rename columns to unique _c0, _c1, ... names first to avoid
        # ambiguous reference errors when column names contain special
        # characters or are duplicated.
        unique_names = [f"_c{i}" for i in range(len(df.columns))]
        schema_types = [f.dataType.simpleString() for f in df.schema.fields]
        df = df.toDF(*unique_names)
        from pyspark.sql import functions as F

        # Use hex() for binary columns to match .slt convention (which
        # displays binary as lowercase hex), cast(string) for everything else
        cast_exprs = []
        for i, c in enumerate(unique_names):
            if schema_types[i] == "binary":
                cast_exprs.append(F.lower(F.hex(F.col(c))).alias(c))
            else:
                cast_exprs.append(F.col(c).cast("string").alias(c))
        string_df = df.select(cast_exprs)
        rows = string_df.collect()
        return format_result(rows, num_cols), None
    except Exception as e:
        return None, str(e)


def run_statement(sql: str) -> Optional[str]:
    """Run a statement (DDL) against PySpark. Returns error message or None."""
    spark = get_spark()
    try:
        spark.sql(sql)
        return None
    except Exception as e:
        return str(e)


# ---------------------------------------------------------------------------
# 4. File Orchestration and CLI
# ---------------------------------------------------------------------------


@dataclass
class FileResult:
    """Results for a single .slt file."""

    filepath: str
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    errors: list = field(default_factory=list)
    skipped_details: list = field(default_factory=list)


def _try_parse_float(s: str) -> Optional[float]:
    """Try to parse a string as a float, handling special values."""
    if s in ("NULL", "(empty)"):
        return None
    try:
        return float(s)
    except (ValueError, OverflowError):
        return None


def _values_match(exp_val: str, act_val: str, rel_tol: float = 1e-6) -> bool:
    """Compare two individual values with float tolerance.

    For values that parse as floats, uses relative tolerance comparison.
    Also handles scientific notation vs decimal mismatch (e.g., 8.165e15 vs 8165619676597685).
    Non-numeric values are compared as exact strings.
    """
    if exp_val == act_val:
        return True
    # Try numeric comparison
    exp_f = _try_parse_float(exp_val)
    act_f = _try_parse_float(act_val)
    if exp_f is not None and act_f is not None:
        # Handle NaN
        if math.isnan(exp_f) and math.isnan(act_f):
            return True
        # Handle infinity
        if math.isinf(exp_f) and math.isinf(act_f):
            return (exp_f > 0) == (act_f > 0)
        # Handle zero
        if exp_f == 0.0 and act_f == 0.0:
            return True
        # Relative tolerance
        if exp_f != 0.0:
            return abs(exp_f - act_f) / abs(exp_f) < rel_tol
        return abs(exp_f - act_f) < rel_tol
    return False


def _lines_match(exp_line: str, act_line: str) -> bool:
    """Compare a single result line with float tolerance.

    Splits multi-column lines by space and compares each value.
    """
    exp_parts = exp_line.split(" ")
    act_parts = act_line.split(" ")
    if len(exp_parts) != len(act_parts):
        return False
    return all(_values_match(e, a) for e, a in zip(exp_parts, act_parts))


def compare_results(
    expected: list[str], actual: list[str], rowsort: bool
) -> tuple[bool, str]:
    """Compare expected vs actual results. Returns (match, detail).

    Uses exact string matching first, then falls back to float-tolerant
    comparison for numeric values.
    """
    exp = expected[:]
    act = actual[:]

    if rowsort:
        exp = sorted(exp)
        act = sorted(act)

    if exp == act:
        return True, ""

    # Try tolerant comparison
    if len(exp) == len(act) and all(
        _lines_match(e, a) for e, a in zip(exp, act)
    ):
        return True, ""

    detail_lines = []
    detail_lines.append(f"  Expected ({len(exp)} lines):")
    for line in exp[:10]:
        detail_lines.append(f"    {line}")
    if len(exp) > 10:
        detail_lines.append(f"    ... ({len(exp) - 10} more)")
    detail_lines.append(f"  Actual ({len(act)} lines):")
    for line in act[:10]:
        detail_lines.append(f"    {line}")
    if len(act) > 10:
        detail_lines.append(f"    ... ({len(act) - 10} more)")

    return False, "\n".join(detail_lines)


def process_file(
    filepath: str, verbose: bool = False, show_skipped: bool = False
) -> FileResult:
    """Process a single .slt file and validate against PySpark."""
    result = FileResult(filepath=filepath)
    records = parse_slt(filepath)

    # Track tables created with untranslatable SQL
    skip_tables: set[str] = set()

    rel_path = os.path.relpath(filepath)

    for record in records:
        # Skip ANSI mode blocks
        if record.in_ansi_block:
            result.skipped += 1
            if show_skipped:
                result.skipped_details.append(
                    f"  Line {record.line_number}: skipped (ANSI mode block)"
                )
            continue

        if isinstance(record, StatementRecord):
            # Translate and execute DDL statements
            translated, skip_reason = translate_sql(record.sql)
            if skip_reason:
                # Check if this creates a table - track it
                create_match = re.search(
                    r"CREATE\s+TABLE\s+(\w+)", record.sql, re.IGNORECASE
                )
                if create_match:
                    skip_tables.add(create_match.group(1).lower())
                result.skipped += 1
                if show_skipped:
                    result.skipped_details.append(
                        f"  Line {record.line_number}: skipped ({skip_reason})"
                    )
                continue

            err = run_statement(translated)
            if err:
                if verbose:
                    print(
                        f"  Line {record.line_number}: statement error (non-fatal): {err[:100]}"
                    )
                # Track the table as skip if creation failed
                create_match = re.search(
                    r"CREATE\s+TABLE\s+(\w+)", record.sql, re.IGNORECASE
                )
                if create_match:
                    skip_tables.add(create_match.group(1).lower())

        elif isinstance(record, QueryRecord):
            # Check if query references a skipped table
            sql_lower = record.sql.lower()
            refs_skip_table = any(
                re.search(rf"\b{re.escape(t)}\b", sql_lower) for t in skip_tables
            )
            if refs_skip_table:
                result.skipped += 1
                if show_skipped:
                    result.skipped_details.append(
                        f"  Line {record.line_number}: skipped (references skipped table)"
                    )
                continue

            translated, skip_reason = translate_sql(record.sql)
            if skip_reason:
                result.skipped += 1
                if show_skipped:
                    result.skipped_details.append(
                        f"  Line {record.line_number}: skipped ({skip_reason})"
                    )
                continue

            num_cols = len(record.type_codes)
            actual, err = run_query(translated, num_cols)

            if err:
                result.failed += 1
                result.errors.append(
                    f"  Line {record.line_number}: PySpark error: {err}\n"
                    f"    SQL: {translated}"
                )
                continue

            match, detail = compare_results(record.expected, actual, record.rowsort)
            if match:
                result.passed += 1
                if verbose:
                    print(f"  Line {record.line_number}: PASS")
            else:
                result.failed += 1
                result.errors.append(
                    f"  Line {record.line_number}: MISMATCH\n"
                    f"    SQL: {record.sql}\n"
                    f"    Translated: {translated}\n"
                    f"{detail}"
                )

        elif isinstance(record, ErrorRecord):
            # For error queries, verify Spark also throws
            translated, skip_reason = translate_sql(record.sql)
            if skip_reason:
                result.skipped += 1
                if show_skipped:
                    result.skipped_details.append(
                        f"  Line {record.line_number}: skipped ({skip_reason})"
                    )
                continue

            if record.kind == "query":
                _, err = run_query(translated, 1)
            else:
                err = run_statement(translated)

            if err:
                result.passed += 1
                if verbose:
                    print(
                        f"  Line {record.line_number}: PASS (error expected and received)"
                    )
            else:
                # Spark succeeded where DataFusion expected error - note but don't fail
                result.skipped += 1
                if show_skipped:
                    result.skipped_details.append(
                        f"  Line {record.line_number}: skipped (Spark did not error, DataFusion expects error)"
                    )

    return result


def discover_slt_files(test_dir: str, path_filter: Optional[str] = None) -> list[str]:
    """Find .slt files under test_dir, optionally filtered."""
    test_path = Path(test_dir)
    if path_filter:
        target = test_path / path_filter
        if target.is_file():
            return [str(target)]
        elif target.is_dir():
            return sorted(str(f) for f in target.rglob("*.slt"))
        else:
            print(f"Error: {target} is not a file or directory", file=sys.stderr)
            sys.exit(1)
    return sorted(str(f) for f in test_path.rglob("*.slt"))


def cleanup_tables():
    """Drop all tables in the Spark session."""
    spark = get_spark()
    try:
        for table in spark.catalog.listTables():
            spark.sql(f"DROP TABLE IF EXISTS {table.name}")
    except Exception:
        pass


_VERSION_CONDITION_RE = re.compile(
    r"^\[spark([<>=!]+)(\d+\.\d+)\]\s+(.+)$"
)


def _check_version_condition(op: str, ver: tuple[int, ...]) -> bool:
    """Check if the current Spark version satisfies the condition."""
    current = spark_version()
    if op == ">=":
        return current >= ver
    if op == "<=":
        return current <= ver
    if op == ">":
        return current > ver
    if op == "<":
        return current < ver
    if op == "==":
        return current == ver
    if op == "!=":
        return current != ver
    return True  # unknown op, include by default


def load_known_failures(filepath: str) -> set[str]:
    """Load known failure file paths from a text file.

    Each non-blank, non-comment line is a .slt file path relative to the
    spark test directory (e.g., 'string/format_string.slt').

    Lines can optionally have a version condition prefix:
        [spark>=4.0] math/abs.slt
    which means the entry only applies when running against Spark >= 4.0.
    """
    known = set()
    if not os.path.isfile(filepath):
        return known
    with open(filepath) as f:
        for line in f:
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                # Check for version condition
                m = _VERSION_CONDITION_RE.match(stripped)
                if m:
                    op, ver_str, path = m.group(1), m.group(2), m.group(3)
                    ver = tuple(int(x) for x in ver_str.split("."))
                    if not _check_version_condition(op, ver):
                        continue
                    stripped = path
                # Normalize path separators
                known.add(stripped.replace("\\", "/"))
    return known


def main():
    parser = argparse.ArgumentParser(
        description="Validate .slt test files against PySpark"
    )

    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent.parent.parent
    default_test_dir = repo_root / "datafusion" / "sqllogictest" / "test_files" / "spark"
    default_known_failures = script_dir / "known-failures.txt"

    parser.add_argument(
        "--path",
        help="Relative path to .slt file or directory (relative to spark test dir)",
    )
    parser.add_argument(
        "--test-dir",
        default=str(default_test_dir),
        help="Root directory for .slt test files",
    )
    parser.add_argument("--verbose", action="store_true", help="Show detailed output")
    parser.add_argument(
        "--show-skipped", action="store_true", help="Show skipped query details"
    )
    parser.add_argument(
        "--known-failures",
        default=str(default_known_failures),
        help="Path to known-failures.txt file (use --known-failures=none to disable)",
    )
    args = parser.parse_args()

    # Load known failures
    if args.known_failures.lower() == "none":
        known_failures = set()
    else:
        known_failures = load_known_failures(args.known_failures)
        if known_failures:
            print(f"Loaded {len(known_failures)} known failure file(s)")

    files = discover_slt_files(args.test_dir, args.path)
    if not files:
        print("No .slt files found")
        sys.exit(1)

    print(f"Found {len(files)} .slt file(s) to validate\n")

    total_passed = 0
    total_failed = 0
    total_skipped = 0
    total_known_failures = 0
    failed_files = []
    known_failure_files = []

    for filepath in files:
        rel = os.path.relpath(filepath, args.test_dir)
        rel_normalized = rel.replace("\\", "/")
        is_known_failure = rel_normalized in known_failures

        if is_known_failure:
            print(f"--- {rel} [known failure, skipping] ---\n")
            total_known_failures += 1
            known_failure_files.append(rel)
            continue

        print(f"--- {rel} ---")

        cleanup_tables()
        file_result = process_file(
            filepath, verbose=args.verbose, show_skipped=args.show_skipped
        )

        # Print errors
        for err in file_result.errors:
            print(err)

        # Print skipped details
        for detail in file_result.skipped_details:
            print(detail)

        # Print summary for this file
        status_parts = []
        if file_result.passed:
            status_parts.append(f"{file_result.passed} passed")
        if file_result.failed:
            status_parts.append(f"{file_result.failed} FAILED")
        if file_result.skipped:
            status_parts.append(f"{file_result.skipped} skipped")
        print(f"  Result: {', '.join(status_parts)}\n")

        total_passed += file_result.passed
        total_failed += file_result.failed
        total_skipped += file_result.skipped
        if file_result.failed > 0:
            failed_files.append(rel)

    # Overall summary
    print("=" * 60)
    print(
        f"Overall: {total_passed} passed, {total_failed} failed, "
        f"{total_skipped} skipped, {total_known_failures} known failures"
    )
    if failed_files:
        print(f"\nUnexpected failures:")
        for f in failed_files:
            print(f"  {f}")
    if known_failure_files and args.verbose:
        print(f"\nKnown failures (skipped):")
        for f in known_failure_files:
            print(f"  {f}")
    print()

    # Exit 0 if no unexpected failures
    sys.exit(1 if total_failed > 0 else 0)


if __name__ == "__main__":
    main()
