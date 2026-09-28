---
name: audit-datafusion-spark-expression
description: Audit a datafusion-spark function implementation for correctness against Apache Spark 4.2.0. Studies the Spark source across versions, reviews the Rust implementation and its signature, verifies expected values against a real PySpark, fixes divergences, and captures the rest as issues and disabled tests.
argument-hint: <function-name>
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

Audit the `datafusion-spark` implementation of the `$ARGUMENTS` function for
correctness against Apache Spark.

## Baseline Version

The baseline is **Spark 4.2.0**. Every expected value written into a `.slt`
file asserts Spark 4.2.0 behavior.

Spark 3.5.8, 4.0.4, and 4.1.3 are also read, but only to detect that behavior
changed between versions. They never set an expectation. There is currently no
mechanism for version-specific expectations in DataFusion's SLT files, which is
tracked by https://github.com/apache/datafusion/issues/23887.

Older versions are read rather than ignored because `datafusion-spark` is meant
to be reusable by engines that target more than one Spark release. Comet
supports Spark 3.4 through 4.0. A function audited against 4.2.0 alone can be
correct there and silently wrong for such a consumer, and nothing in the crate
records which of the two it is. Finding the divergence is useful even though
the test suite cannot yet assert it.

## Overview

1. Study the Spark implementation across versions
2. Harvest Spark's own test coverage
3. Review the DataFusion implementation
4. Cross-check Comet and Sail
5. Review existing SLT coverage
6. Gap analysis
7. Establish ground truth with PySpark
8. Apply findings

Audit one function per invocation.

## Paths in This Skill Are Hints

Every file path below was correct when this skill was written and several will
rot. `FunctionRegistry.scala`, the golden-file directories, and
`datafusion/common/src/config.rs` all move on someone else's schedule.

**A path that does not resolve is a stop condition, not a step to skip.** The
failure mode this exists to prevent is a command that quietly matches nothing,
an audit that reads the silence as "no ANSI branch" or "Spark does not test
this", and a confident report built on an empty grep. A `grep` over a
nonexistent directory and a `grep` that genuinely found nothing are the same
output.

So when a path in this skill does not exist, or a command returns nothing where
something was expected:

1. Say so out loud in the audit report. Name the path and the step.
2. Search for where the thing moved to, by content rather than by path. The
   grep patterns here are more durable than the directories they are pointed at.
3. If it cannot be found, mark every conclusion that depended on that step as
   unestablished. Do not let an unread source become an absence of findings.

The same rule covers a `.slt` file, an implementation file, or a Spark class
that is not where this skill says it is.

## Step 1: Study the Spark Implementation

Shallow-clone the four version tags. Skip any already present.

```bash
set -eu
for tag in v3.5.8 v4.0.4 v4.1.3 v4.2.0; do
  dir="/tmp/spark-${tag}"
  [ -d "$dir" ] || git clone --depth 1 --branch "$tag" \
    https://github.com/apache/spark.git "$dir"
done
```

Find the expression class in each version. Spark registers SQL function names
in `FunctionRegistry.scala`, so start there to map `$ARGUMENTS` to its Scala
class name, then locate the class.

```bash
for tag in v3.5.8 v4.0.4 v4.1.3 v4.2.0; do
  dir="/tmp/spark-${tag}"
  echo "=== $tag ==="
  grep -rn "\"$ARGUMENTS\"" \
    "$dir/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/FunctionRegistry.scala"
done
```

With the class name in hand, read its source in each version:

```bash
for tag in v3.5.8 v4.0.4 v4.1.3 v4.2.0; do
  dir="/tmp/spark-${tag}"
  echo "=== $tag ==="
  find "$dir/sql/catalyst/src/main/scala" -name "*.scala" \
    | xargs grep -lE "case class <ClassName>[ (\[]" 2>/dev/null
done
```

A Scala `case class` name is always followed by a space, a `(`, or a `[`, so
that bracket expression is the portable way to avoid matching a longer class
name that starts with the same prefix. Do not reach for `\b`, which is a GNU
and ugrep extension and is not available in POSIX or BSD `grep`.

If it is not in catalyst, widen the search to `$dir/sql`.

For each version, record:

- `eval` / `nullSafeEval` / `doGenCode` logic
- `inputTypes` and `dataType`
- `nullable`, and how nulls propagate
- ANSI branches (`ansiEnabled`, `failOnError`)
- guards, `require` assertions, thrown exceptions
- any SQLConf the expression reads
- for string functions in 4.0+, whether it routes through
  `CollationSupport.X.exec(..., collationId)` or declares
  `StringTypeWithCollation` inputs

Produce a diff summary across 3.5.8 → 4.0.4 → 4.1.3 → 4.2.0. Note new or
removed input types, changed edge-case behavior, new ANSI branches, new
parameters, and collation changes.

Two changes affect nearly every function and are easy to miss:

- ANSI mode became the **default** in Spark 4.0. A function whose behavior
  differs under ANSI has a different default behavior in 3.5 than in 4.x.
- Collation support landed for many string functions in 4.0. Non-default
  collations are a divergence axis that does not exist in 3.5.

## Step 2: Harvest Spark's Own Test Coverage

Spark's tests are a ready-made edge-case inventory. Two sources matter.

Unit tests:

```bash
dir=/tmp/spark-v4.2.0
find "$dir/sql" -name "*.scala" -path "*/test/*" \
  | xargs grep -ln "$ARGUMENTS" 2>/dev/null
```

Golden files, which carry Spark's own verified query results and are the
highest-value source:

```bash
for tag in v3.5.8 v4.0.4 v4.1.3 v4.2.0; do
  dir="/tmp/spark-${tag}"
  echo "=== $tag ==="
  grep -rln "$ARGUMENTS" "$dir/sql/core/src/test/resources/sql-tests/results/" 2>/dev/null
done
```

**The ANSI and non-ANSI golden directories swapped meaning in Spark 4.0.** Read
the version banner before trusting a path:

| Version | ANSI-mode results        | Non-ANSI results            |
| ------- | ------------------------ | --------------------------- |
| 3.5.8   | `results/ansi/X.sql.out` | `results/X.sql.out`         |
| 4.x     | `results/X.sql.out`      | `results/nonansi/X.sql.out` |

ANSI became the default in 4.0, so the unqualified path flipped from meaning
non-ANSI to meaning ANSI. An auditor who learns the layout on 3.5.8 and then
reads `results/X.sql.out` on 4.2.0 will record exactly inverted expectations,
and they will look plausible. Confirm the mode by checking whether the file
contains error output or `NULL` for a known-invalid input.

Read every match and extract:

- input types covered
- edge cases exercised: null, empty, overflow, negative, boundary, special
  characters
- ANSI-mode cases
- error cases and their exact messages

This list is the reference for the gap matrix in Step 6.

## Step 3: Review the DataFusion Implementation

```bash
grep -rln "$ARGUMENTS" datafusion/spark/src/function/ --include="*.rs"
```

The implementation lives at
`datafusion/spark/src/function/<category>/<function>.rs`. **Read every other
file that grep returns as well.** In particular
`datafusion/spark/src/function/<category>/mod.rs` carries the `export_functions!`
registration and the user-facing doc string, and unimplemented-behavior `TODO`
comments are written there too, not only in the function's own file. An audit
that reads only `<function>.rs` will miss them and will leave a doc string that
contradicts the code after a fix.

**A `.slt` file under `test_files/spark/` does not imply an implementation in
`datafusion/spark`.** `SessionStateBuilderSpark::with_spark_features` layers the
Spark functions on top of `with_default_features`, so a name the spark crate
does not define resolves to DataFusion's own function of that name. `md5`,
`coalesce`, and `reverse` are served this way today, and `date_format` resolves
through an alias on core's `to_char`. The query under audit still runs, and the
code it runs is not in the directory the grep above searched. If that grep comes
back empty, widen it before concluding the function is unimplemented:

```bash
grep -rln "\"$ARGUMENTS\"" \
  datafusion/functions/src datafusion/functions-nested/src \
  datafusion/functions-aggregate/src datafusion/functions-window/src \
  datafusion/functions-table/src --include="*.rs"
```

A hit may be in a file named after something else, because core registers
aliases. `date_format` lives in `to_char.rs`. Read the `aliases` vector, not
just the file name.

If that grep is also empty, the function is genuinely unimplemented and the
`.slt` file is a porting stub with every query commented out. Check with
`grep -cE '^(query|statement)' <file>.slt`, which returns zero for a stub.
There is no implementation to audit in that case. Say so and stop, rather than
reporting a clean audit of nothing.

Otherwise audit what the grep found against Spark with the rest of this step's
checklist. The findings land differently. A core function has non-Spark users,
so Spark's semantics cannot be imposed on it. Where the two agree, record the
coverage and leave the core function alone. Where they diverge, the fix is a
Spark-specific implementation under `datafusion/spark/src/function/<category>/`
that shadows the core one, never an edit to the core function.
`datafusion/spark/src/function/math/abs.rs` shadowing
`datafusion/functions/src/math/abs.rs` is the in-repo precedent. Say in the
audit report which crate the audited code came from, because a reader who
assumes `datafusion/spark` will look for a file that does not exist.

Check each of the following. The first two have no Comet analog and are the
most common source of silent divergence.

**Signature and coercion.** Compare `Signature` (and `coerce_types`, if
implemented) against Spark's `inputTypes`. Two failure directions, both real:

- DataFusion rejects a type Spark accepts. The user gets a coercion error
  where Spark returns a value.
- DataFusion accepts a type Spark rejects. The user gets a value where Spark
  raises an analysis error.

Note that `Signature::exact` is narrow. A Spark function accepting any string
type needs all of `Utf8`, `LargeUtf8`, and `Utf8View` handled, and a function
accepting any integral type needs the full width range.

**Return nullability.** Compare `return_field_from_args` (or `return_type`)
against Spark's `nullable`. A function that reports non-nullable while Spark
can return NULL produces wrong plans, not merely wrong values: downstream
optimizer rules will fold away null checks that were load-bearing.

**Null propagation.** Does every input-null combination return what Spark
returns? Check the order in which nullness and validity are tested. Spark
expressions that are null intolerant (a `NullIntolerant` mixin in 3.5, a
`nullIntolerant` override in 4.x) short circuit to NULL *before* validating the
other arguments, so a NULL in one argument suppresses an error the other
argument would otherwise raise, even under ANSI mode. An implementation that
validates first and checks nullness second raises where Spark returns NULL.

**Arrow kernel semantics are not Java semantics.** A `datafusion-spark`
function is usually a thin arrangement of Arrow kernels, and the kernels do not
share Java's rules. Check each one the implementation leans on:

- **Comparison kernels order floats by total order**, so `-0.0` compares as
  *less than* `0.0` rather than equal to it. Any `eq`/`lt` used to detect a
  special value, such as a zero divisor, silently misses `-0.0`. Java compares
  them as equal. This is easy to miss because `-0.0` almost never appears in
  hand-written test values, so write it in deliberately.
- **Arithmetic kernels are checked, not wrapping.** Java wraps silently on
  overflow, so a checked Arrow kernel raises where Spark returns a wrapped
  value. Where Spark's own expression is written in `Int` arithmetic, a
  wrapping kernel is the faithful choice, not the reckless one.
- **Java promotes `byte` and `short` operands to `int`.** A Spark expression
  whose Scala source reads `(r + n)` on `Byte` operands is evaluated at `Int`
  width and cannot overflow, while the same source on `Int` operands can.
  Reproducing it at `Int8` width wraps where Spark does not.
- **Kernel coverage is type dependent.** Arrow's `rem` raises on a zero divisor
  for integers and decimals but returns `NaN` for floats, so an ANSI check that
  relies on the kernel to raise silently does nothing on the float path.

Read the Scala source as arithmetic to be reproduced, not as pseudocode to be
paraphrased. Then test each of these against a live Spark rather than reasoning
about them.

**`ColumnarValue` shape dispatch.** A two-argument function has four shape
combinations, and each needs its own branch:
`(Scalar, Scalar)`, `(Array, Scalar)`, `(Scalar, Array)`, `(Array, Array)`.
A missing arm falls through to a catch-all `exec_err!` and turns a working
Spark query into a hard error. `(Scalar, Array)` is the one most often
forgotten, because the natural test query puts the column first. Write a query
for every combination and run it. Do not infer coverage from reading the
`match`.

**How to run an ad-hoc query.** The only in-repo way to execute a
`datafusion-spark` function is to add the query to
`datafusion/sqllogictest/test_files/spark/<category>/$ARGUMENTS.slt` and run

```bash
cargo test --test sqllogictests -- spark/<category>/$ARGUMENTS
```

`datafusion-cli` does **not** register the `datafusion-spark` function library,
so reaching for it fails in a way that looks like the function does not exist:

```
$ ./target/debug/datafusion-cli -c "SELECT next_day(arrow_cast(95026236,'Date32'),'Mon');"
Error during planning: Invalid function 'next_day'. Did you mean 'today'?
```

Giving the CLI an opt-in for the Spark library is tracked by
https://github.com/apache/datafusion/issues/24146. Until that lands, treat the
error above as a registration gap and not as evidence about the function.

For a throwaway probe that should not end up in the committed file, add a
temporary `.slt` alongside it, run it by name, and delete it afterwards. To
exercise a raw storage value that has no literal syntax, such as a `Date32`
past the last date DataFusion can render, wrap it in `arrow_cast`.

**Cross the shapes with the NULL cases, and with ANSI mode.** These are not
three independent checklists to tick off separately, they are one product to
enumerate. The bug this instruction exists to catch is a branch that gets NULL
handling right in the row-wise path and wrong in the vectorized one, so the two
shapes disagree with each other while each looks correct in isolation. For
every shape, ask what happens when the *other* argument is NULL, and when it is
NULL in only some rows. A shape that takes an array and a scalar has to make
the scalar's validation depend on the array's null mask, because a per-row null
intolerant function skips validation on the rows it short circuits. All-NULL,
some-NULL, and no-NULL versions of the same array are three different tests.

Note that a `(Array, Scalar)` branch returning `ColumnarValue::Scalar` is *not*
by itself a bug. In DataFusion a returned `Scalar` means "this value for every
row" and the caller broadcasts it to the batch length. Before reporting one,
run an N-row query and count the rows that come back. Report it only if the
row count is actually wrong, or if the scalar's value depends on per-row input.
Otherwise it is at most a readability cleanup, and reporting it as a
correctness bug costs the audit credibility.

**Overflow and underflow.** Does it return `Err`, wrap, or panic? Spark's
answer depends on ANSI mode. A panic is always a bug.

**Type dispatch.** Every type the signature admits must have a branch. An
`exec_err!` fallthrough for a type the signature accepts is a bug.

**ANSI handling.** Compare against `datafusion.execution.enable_ansi_mode`
(defined in `datafusion/common/src/config.rs`). The crate has scattered `TODO`
comments about unimplemented ANSI paths. Treat every one found in this
function as a finding, not as a known-acceptable state. Grep for them across
every file Step 3's grep returned, including `mod.rs`:

```bash
grep -rn "TODO" datafusion/spark/src/function/<category>/$ARGUMENTS.rs \
                datafusion/spark/src/function/<category>/mod.rs
```

Wiring ANSI up is usually mechanical rather than a semantics decision. The flag
is already threaded through `ScalarFunctionArgs`, so the implementation reads
it directly:

```rust
let ScalarFunctionArgs { args, config_options, .. } = args;
let ansi_mode = config_options.execution.enable_ansi_mode;
```

`datafusion/spark/src/function/math/abs.rs` and
`datafusion/spark/src/function/math/modulus.rs` are the in-repo precedents for
both the plumbing and the test layout. DataFusion does not model Spark's error
classes or SQLSTATE values, so say that in the audit either way.

Whether to match Spark's message text depends on who constructs the error:

- **The function constructs it** (an `exec_err!` you write). Match Spark's text
  exactly, including trailing punctuation, and assert it in the `.slt`.
- **An Arrow kernel constructs it** (`rem`, `add`, a cast). You do not control
  the wording. Assert only that an error is raised, note the divergence in a
  comment, and file an issue for the message text rather than contorting the
  implementation to restate an Arrow error in Spark's words.

Do not read the `abs.rs` precedent as licence to invent DataFusion-flavoured
text for an error you *do* construct. `abs` reports `Int32 overflow on
abs(-2147483648)` where Spark 4.2.0 reports `[ARITHMETIC_OVERFLOW] overflow.`,
which is a known divergence tracked by
https://github.com/apache/datafusion/issues/24145. Follow its ANSI plumbing and
test layout, not its wording.

`abs` also shows why the "who constructs it" question is worth asking rather
than assuming. Its scalar path builds the message in the spark crate, but its
array path borrows `make_try_abs_function!` from
`datafusion/functions/src/math/abs.rs`, so that half of the text is owned by
core DataFusion and shared with core's own `abs`. Wherever a spark function
reuses a core macro or kernel, Spark-specific error text is not available
without un-sharing the code first. Check where the message is actually built
before promising to match Spark's.

**Rust unit tests.** Read the `#[test]` blocks in the same file. Per the crate
README, direct invocation through `test_scalar_function!()` bypasses input
coercion, so passing unit tests never establish end-to-end correctness. Note
which cases are covered by unit tests only.

## Step 4: Cross-Check Comet and Sail

The `datafusion-spark` README recommends checking both projects for existing
implementations. Diff against them to surface edge cases they handle that this
crate misses.

Neither project needs to be checked out. Both are searched over HTTP, by
content rather than by path, because both have moved their directory layouts
more than once:

```bash
for repo in apache/datafusion-comet lakehq/sail; do
  echo "=== $repo ==="
  gh api -X GET search/code -f q="$ARGUMENTS repo:$repo" --jq '.items[].path'
done

# Then read a specific hit
gh api "repos/<repo>/contents/<path from above>" --jq '.content' | base64 -d
```

If a clone happens to be on disk, grep it instead. It is faster and it is not
rate limited. Do not silence the failure with `2>/dev/null`, though: a grep over
a directory that is not there prints an error, and swallowing it turns "Comet
not checked out" into "Comet has no implementation".

```bash
[ -d ../datafusion-comet ] \
  && grep -rln "$ARGUMENTS" ../datafusion-comet/native/spark-expr/src/ --include="*.rs" \
  || echo "no local Comet clone, use the API above"
```

`gh api search/code` requires authentication and is rate limited to a handful of
requests per minute. If it fails, say so in the audit report and mark Step 4 as
not performed rather than reporting no differences found.

As of this writing the implementations live under
`crates/sail-function/src/scalar/<category>/spark_<function>.rs`, and Sail also
keeps behavior fixtures at
`python/pysail/tests/spark/function/features/<function>.feature`. Treat both
paths as hints, not as addresses.

Sail matters more than a generic peer here: many `datafusion-spark` functions
were ported from Sail, so Sail's current code is often the same code with
subsequent fixes applied. A difference between the two is a strong lead.

Treat both as peer implementations, not as authority. Each has its own audit
backlog, and a shared assumption between them is not evidence of correctness.
Spark's source and a running Spark are the only authorities.

## Step 5: Review Existing SLT Coverage

```bash
ls datafusion/sqllogictest/test_files/spark/*/$ARGUMENTS.slt
```

Read the file and list the queries, the types exercised, which cases are
commented out, and whether the file has an audit header comment from a
previous audit.

Also read `datafusion/sqllogictest/test_files/spark/README.md`. It is the
binding local convention for this tree and it prescribes things this skill does
not repeat: every function must be tested on both scalar and array inputs,
every literal needs an explicit cast because DataFusion and Spark infer types
differently, test cases contain `SELECT` statements only, and a function whose
behavior differs under ANSI is wrapped in

```sql
statement ok
set datafusion.execution.enable_ansi_mode = true;

# queries

statement ok
set datafusion.execution.enable_ansi_mode = false;
```

Reset the flag to `false` at the end of the block. The setting leaks to
whatever follows it in the file otherwise.

Many files in this tree were machine-generated from Sail's gold data. Their
expected values were not necessarily verified against Spark, so an existing
passing expectation is not evidence of correctness. Re-verify in Step 7.

## Step 6: Gap Analysis

Build this matrix, filling each cell with yes, no, or n/a:

| Dimension                                      | Spark tests it | DataFusion SLT | Rust unit test | Gap? |
| ---------------------------------------------- | -------------- | -------------- | -------------- | ---- |
| All-scalar arguments                           |                |                |                |      |
| All-array arguments                            |                |                |                |      |
| Array first argument, scalar second            |                |                |                |      |
| Scalar first argument, array second            |                |                |                |      |
| NULL input, each argument independently        |                |                |                |      |
| Empty string / array / map                     |                |                |                |      |
| Zero, negative zero, negatives                 |                |                |                |      |
| Overflow, underflow                            |                |                |                |      |
| Boundary values (INT_MIN, INT_MAX, i64 bounds) |                |                |                |      |
| NaN, Infinity, -Infinity, subnormal            |                |                |                |      |
| Multibyte / special UTF-8                      |                |                |                |      |
| ANSI mode enabled                              |                |                |                |      |
| ANSI mode disabled                             |                |                |                |      |
| Every type in Spark's `inputTypes`             |                |                |                |      |
| Types Spark rejects                            |                |                |                |      |
| Return nullability                             |                |                |                |      |
| Cross-version divergence                       |                |                |                |      |

Rows marked n/a need a one-line justification, for example "no float inputs"
against the NaN row.

Both ANSI rows matter independently. DataFusion defaults `enable_ansi_mode` to
`false` while Spark 4.x defaults ANSI to `true`, so the mode users actually hit
on Spark 4 is the one most likely to be untested here.

## Step 7: Establish Ground Truth with PySpark

No expected value goes into a `.slt` file without being observed from a real
Spark. Reading `nullSafeEval` and predicting the output is exactly how wrong
golden values get written in the first place.

### Set up the environment

Requires a JDK 17 or later on `PATH`. Use `uv` to create the venv rather than
`python3 -m venv`: `pyspark==4.2.0` requires Python 3.10 or later, and the
system `python3` on many machines is older. `uv` is already used by this
repository and will fetch a suitable interpreter if none is installed.

Unset `SPARK_HOME` and `PYSPARK_PYTHON` before doing anything else. If either
is set, a local Spark install on `SPARK_HOME` silently overrides the
pip-installed `pyspark` JARs, and `PYSPARK_PYTHON` can point PySpark's workers
at a different Python than the venv you just built. The `SPARK_HOME` case is
not hypothetical: it reproduces reliably on a machine with any other Spark
install on `PATH`, and it fails late and unhelpfully, inside `getOrCreate()`,
as `TypeError: 'JavaPackage' object is not callable`. If you hit that exact
error, this is the cause: check `echo $SPARK_HOME` and unset it.

**`unset` does not persist between tool calls.** Each shell invocation starts
from the user's profile, which is where `SPARK_HOME` was exported in the first
place. Unsetting it once in the setup block and then running the harness in a
later call reintroduces the failure. Put `unset SPARK_HOME PYSPARK_PYTHON` in
the same command as every JVM-starting invocation, not in a preceding one.

```bash
unset SPARK_HOME PYSPARK_PYTHON
uv venv --python 3.12 /tmp/pyspark-4.2.0-venv
uv pip install --python /tmp/pyspark-4.2.0-venv --quiet 'pyspark==4.2.0'
/tmp/pyspark-4.2.0-venv/bin/python -c "import pyspark; print(pyspark.__version__)"
```

The last command must print `4.2.0`. If it prints anything else, stop and fix
the environment: an audit against the wrong Spark version is worse than no
audit, because it writes confident wrong expectations.

That check is necessary but not sufficient. It only reads the installed
package's metadata: it never starts a JVM, so it passes cleanly even on a
machine primed to hit the `SPARK_HOME` shadowing failure above. The check
that actually proves which Spark you are talking to is the
`# Spark 4.2.0 tz=UTC` banner the harness prints from a live `SparkSession` in
the next section. Do not trust the environment until you have seen that
banner.

### The harness

Write this to the scratchpad as `spark_ground_truth.py`. It is deliberately not
committed to the repo: `pyspark` is too heavy a dependency to add to the `uv`
workspace in `dev/`, and `datafusion/spark/scripts/` is claimed by other
in-flight work.

Write it unconditionally, overwriting whatever is already there. A previous
audit may have left an older copy in the same scratchpad, and silently reusing
it means auditing with a harness whose fixes you do not have.

```python
#!/usr/bin/env python3
"""Run SQL queries against a local PySpark and print SLT-ready results.

Usage: spark_ground_truth.py QUERIES.sql

Input is a file of SQL statements, each terminated by a semicolon at the end
of a line. Each query is run under both spark.sql.ansi.enabled=true and
=false, and the observed result is printed in a form that can be pasted into
a .slt file.

The session time zone is pinned to UTC so that results do not depend on the
auditing machine's local zone. DataFusion's sqllogictest runner leaves
execution.time_zone unset, which is effectively UTC, so anything time zone
sensitive would otherwise produce golden values that are wrong everywhere
except on the machine that generated them.

Statement splitting is naive: it splits on a semicolon followed by a newline.
That keeps a semicolon inside a string literal intact, as in
SELECT ascii(';'), as long as the literal is not the last thing on the line.
A query whose string literal ends a line with a semicolon still splits
wrongly. Put such a query on a single line with trailing text after the
literal, or run it on its own.
"""

import sys

from pyspark.sql import SparkSession


def format_value(value):
    """Render one cell the way sqllogictest expects it."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def run_query(spark, sql):
    try:
        rows = spark.sql(sql).collect()
    except Exception as exc:  # noqa: BLE001 - any failure is a result here
        first_line = str(exc).strip().split("\n")[0]
        return f"ERROR: {type(exc).__name__}: {first_line}"
    if not rows:
        return "(no rows)"
    return "\n".join("\t".join(format_value(v) for v in row) for row in rows)


def main(path):
    spark = (
        SparkSession.builder.appName("slt-ground-truth")
        .master("local[1]")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("ERROR")
    print(f"# Spark {spark.version} tz={spark.conf.get('spark.sql.session.timeZone')}")

    raw = open(path).read()
    if not raw.endswith("\n"):
        raw += "\n"
    queries = [q.strip() for q in raw.split(";\n") if q.strip()]

    for sql in queries:
        for ansi in (True, False):
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            print(f"### ansi={str(ansi).lower()}")
            print(f"# {sql}")
            print(run_query(spark, sql))
            print()

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: spark_ground_truth.py QUERIES.sql")
    main(sys.argv[1])
```

### Using it

Write every candidate query, including the ones already in the `.slt` file, to
a scratch `.sql` file and run:

```bash
cd <scratchpad>
unset SPARK_HOME PYSPARK_PYTHON
/tmp/pyspark-4.2.0-venv/bin/python spark_ground_truth.py queries.sql \
  > ground_truth.txt 2> spark_logs.txt
```

Keep the streams separate. Spark writes log4j output, including structured JSON
error records, on a stream that will interleave with the results if you merge
them with `2>&1`. The merged form is what an agent reaches for by reflex and it
makes the output unreadable exactly on the error cases that matter most.

Verify the banner line reads `# Spark 4.2.0 tz=UTC` before trusting any
output. Both halves matter. The version half proves you are not talking to a
Spark install that shadowed the pip-installed one, and the time zone half
proves the session is not silently inheriting the auditing machine's local
zone. DataFusion's sqllogictest runner leaves `execution.time_zone` unset,
which behaves as UTC, so a session on any other zone generates golden values
that only reproduce on the machine that generated them. This is invisible for
functions that take a `DATE` and never touch a time zone, and it is a silent
corruption for `to_utc_timestamp`, `date_format`, `from_unixtime`, and every
other timestamp function. If the banner shows anything but `tz=UTC`, stop and
fix the harness before recording a single expected value.

**Wrap date, timestamp, and interval results in `CAST(... AS STRING)`.** The
harness calls `collect()`, which converts every cell to a Python object, and
Python's `datetime.date` cannot represent everything Spark's `DateType` can. A
value outside Python's range surfaces as a line like

```
ERROR: ValueError: year 10000 is out of range
```

which reads exactly like a Spark error and is not one. Spark returned a value
fine. Any `ValueError` or `OverflowError` in the output is a harness artifact,
never a Spark result. Re-run the query wrapped in a cast to see what Spark
actually produced:

```sql
SELECT CAST(next_day(DATE '9999-12-31', 'Mon') AS STRING);
-- +10000-01-03
```

Re-verify expectations that already exist in the `.slt` file. Many files in
this tree were machine-generated and their values were never checked against
Spark. An existing expectation is a claim, not evidence.

Write the queries in Spark SQL. The `.slt` file needs DataFusion syntax, which
differs: DataFusion uses `'2015-01-14'::DATE` where Spark SQL uses
`DATE '2015-01-14'`. Translate when transcribing, and keep the semantics
identical. If a query cannot be expressed in both dialects, note it as a
finding rather than silently testing something different.

### Never use `--complete`

The sqllogictest runner supports `--complete`, which fills in expected results
with whatever DataFusion currently returns.

**Never run it on files under `test_files/spark/`.** On an audit it would
silently cement the bug being audited as the golden answer, which inverts the
purpose of the whole exercise. Expected values come from PySpark and are
written by hand.

### If PySpark cannot be installed

Continue the audit using values derived from the Spark source and the golden
files from Step 2, and mark every such expectation as unverified in both the
audit report and a comment in the `.slt` file:

```sql
# UNVERIFIED: derived from Spark source, not confirmed against a running Spark
```

Never present a derived value as a verified one. An audit that overstates its
confidence is worse than one that admits a gap.

## Step 8: Apply Findings

Findings do not survive as prose. A recommendation in a PR description is
buried the moment the PR merges. Every finding becomes code, a test, or a
filed issue.

### Fix what can be fixed

Divergences from Spark 4.2.0 are fixed in the audit PR. Attempt the fix
regardless of size. Fall back to filing an issue only when the fix needs a
semantics decision a human should make, for example choosing which of two
defensible behaviors DataFusion should adopt when Spark's own behavior is
inconsistent across versions.

Every fix is locked in by an SLT test that fails before the fix and passes
after. A test that passes before the fix was testing something else.

Verify both directions before committing. Stash only the implementation
change, so the new `.slt` query runs against the unfixed code:

```bash
# Stash the implementation only, leaving the new .slt query in place
git stash push -- datafusion/spark/src/function/<category>/<function>.rs
cargo test --test sqllogictests -- spark/<category>/$ARGUMENTS   # expect FAIL
git stash pop
cargo test --test sqllogictests -- spark/<category>/$ARGUMENTS   # expect PASS
```

This verifies SLT tests only. Rust unit tests live inside the implementation
file, so stashing that file stashes the tests with it and the run proves
nothing about them. For a unit test, get the fail-before evidence by reverting
the implementation body by hand, or by writing the test first and watching it
fail before the fix exists.

Read the failure list from the first run and confirm it names the queries you
added. Do not judge the run by an exit code you captured through a pipe:
`cargo test ... | head` reports the exit status of `head`, not of `cargo`, so a
piped command that "succeeded" tells you nothing. Either run it unpiped or
inspect the `External error: N errors in file ...` block directly.

If the first run passes, the test is not exercising the fix. Either the
query was stashed along with the implementation, or the assertion does not
actually cover the divergent case. Investigate before continuing: a test
that passes against the unfixed code locks in nothing.

If the fix also changed a `mod.rs` doc string or a helper in another file, that
file is not covered by the stash above. Add it to the `git stash push` argument
list, or the "expect FAIL" run may fail for the wrong reason.

Run this before committing the fix. If `git stash push` reports
`No local changes to save`, the change is already committed and the check is
invalid, because the "expect FAIL" run just tested the fixed code. Redo it
against the pre-fix commit by checking that one file out of history and back,
which touches only the working tree and never moves `HEAD` or a branch:

```bash
# <pre-fix-sha> is the commit before the one that introduced the fix
git checkout <pre-fix-sha> -- datafusion/spark/src/function/<category>/<function>.rs
cargo test --test sqllogictests -- spark/<category>/$ARGUMENTS   # expect FAIL
git checkout HEAD -- datafusion/spark/src/function/<category>/<function>.rs
cargo test --test sqllogictests -- spark/<category>/$ARGUMENTS   # expect PASS
```

Find `<pre-fix-sha>` with `git log --oneline -- <the implementation file>` and
take the commit before the fix. If the fix also touched other files, list every
one of them in both `git checkout` commands.

Do not reach for `git reset`, `git revert`, or `git rebase` here. None of them
are needed and all of them can leave the branch in a state the audit then has
to repair.

### Capture what cannot be fixed

A deferred divergence becomes a commented-out SLT query carrying the correct
Spark 4.2.0 result and the issue link:

```sql
# Spark 4.2.0 raises ILLEGAL_DAY_OF_WEEK under ANSI mode; DataFusion returns NULL
# https://github.com/apache/datafusion/issues/23889   <- the real number, not a placeholder
# query error
# SELECT next_day('2015-01-14'::DATE, 'NOT_A_DAY'::string);
```

Uncommenting the block is the contract for verifying the eventual fix. Note
that DataFusion `.slt` files use `#` for comments, not `--`.

**File the issue first, then write the block.** The block cannot be written
until its issue number exists, so do the filing described below before typing
the comment. Never commit a placeholder in place of the number. A string like
`<ISSUE-URL-PENDING>`, `NNNNN`, or `TBD` inside a URL ships a dead link, and
because the surrounding block is commented out no test ever catches it.

If the issue cannot be filed at all, for example because `gh` is not
authenticated or the user asked that nothing be filed yet, omit the link line
entirely. A block with no link is honest and self-contained. Then say so
explicitly in the audit report: name the divergence, state that no issue was
filed and why, and ask the user to file it. Do not leave the reader to
discover the gap from a broken URL.

When you do assert an error, remember that the runner normalizes whitespace in
the `query error` regex, so literal leading, trailing, and repeated spaces in
the expected message cannot be written out. An input like `' MO '` produces
`Illegal input for day of week:  MO .` with two spaces, and a regex containing
those two spaces is collapsed to one before matching. Write `\s+` instead,
which carries no literal whitespace and survives normalization:

```sql
query error Illegal input for day of week:\s+MO \.
SELECT next_day('2015-01-14'::DATE, ' MO '::string);
```

That asserts the padding is present without pinning its exact width. Anchor
the right-hand end of the match on a non-whitespace character from Spark's
message, such as the trailing period in Spark 4.x error templates, so the
assertion still fails if the padding runs into the rest of the message. If
even that cannot be expressed, fall back to the stable prefix and say in a
comment why the assertion stops where it does.

Match Spark's message text exactly, including trailing punctuation. Spark 4.x
error templates in `error-conditions.json` generally end in a period, and the
pre-4.0 exception strings they replaced generally did not. Copying the older
string leaves the implementation asserting a version the audit does not claim,
and a `query error` prefix match will not catch it.

Before filing, search for an existing issue:

```bash
gh issue list --repo apache/datafusion --search "$ARGUMENTS <symptom> in:title,body" --state all --limit 5
```

If a candidate comes back, open it with `gh issue view <N> --repo apache/datafusion`
and confirm it actually describes this divergence and is still open. A closed
issue cited as a known divergence is worse than no citation, because the reader
follows the link and finds a fix that already shipped.

If no issue exists, file one with the `bug` label plus `spark`. Title format:
`[Bug] <function> <one-line symptom>`. The body includes the Spark version
range affected, a minimal repro in both Spark SQL and DataFusion SQL, the
divergent result, and the relevant file and line.

### Capture cross-version divergences

A behavior difference between Spark versions cannot be tested today. Capture it
in two places.

A comment at the query site naming the versions:

```sql
# Spark 3.5.8 returns NULL here; Spark 4.0.4 and later throw under ANSI mode.
# Expectation below asserts 4.2.0. Version-specific expectations are tracked by
# https://github.com/apache/datafusion/issues/23887
```

And its own filed issue, so the divergence is scheduled once the version
mechanism lands.

### Record the audit

Add a header comment block to the `.slt` file, directly below the license
header and the existing provenance comment:

```sql
# Audited against Spark 4.2.0 on YYYY-MM-DD.
# Expected values verified with pyspark==4.2.0.
# Known divergences are captured as commented-out queries below.
```

Use today's real date. If PySpark was unavailable, say so on the second line
instead of claiming verification.

### Attribution

Every issue and PR the skill files ends with:

```
Surfaced by the audit-datafusion-spark-expression skill.
```

Do not mention Claude, AI, or automated generation anywhere in an issue or PR.

### Before committing

Per the repository `CLAUDE.md`:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test -p datafusion-spark --lib $ARGUMENTS
cargo test --test sqllogictests -- spark/<category>/$ARGUMENTS
```

The `--lib` run is not optional. `cargo test --test sqllogictests` does not
compile or run the `#[test]` blocks inside `datafusion/spark/src/`, so a
refactor that renames or changes the signature of a helper those tests call
passes the SLT gate and fails CI.

Do not use `cargo build -p datafusion-spark` as a quick compile check. The
crate does not currently build in isolation and the resulting error is
unrelated to your change. Use `cargo test -p datafusion-spark --lib` or the
sqllogictest target instead.

If the audit touched documentation:

```bash
./ci/scripts/doc_prettier_check.sh --write --allow-dirty
```

Follow `.github/pull_request_template.md` for the PR.

## Output Format

Present the audit as:

1. **Function summary.** What `$ARGUMENTS` does, its input and output types,
   and its null behavior.
2. **Spark version differences.** Across 3.5.8, 4.0.4, 4.1.3, and 4.2.0. Say
   explicitly if there are none.
3. **DataFusion implementation notes.** Including signature, coercion, and
   nullability findings.
4. **Gap matrix.** From Step 6, plus implementation gaps.
5. **Findings.** Split into fixed in this PR, deferred with a filed issue, and
   low-risk coverage gaps.
6. **Offer to add the low-risk coverage tests.**

Low-risk coverage gaps are the only category that pauses for user input.
Correctness findings are always either fixed or filed.

The line between the two is the code you touched. Coverage for a path this PR
changed goes into the `.slt` file without asking: it is part of the fix. The
offer is for cases on paths the PR did not touch and that already look correct,
where adding tests is optional scope the user may not want. Phrase the offer
as:

> Spark exercises the following cases that have no DataFusion test, on code
> paths that appear correct. Would you like me to add them?
>
> - [list each case]

## Tone and Style

- Write in clear, concise prose.
- Use backticks around function names, file paths, type names, and config keys.
- Acknowledge what is already well covered before raising gaps.
- Avoid em dashes and semicolons. Use separate sentences.
- Do not overstate confidence. An unverified expectation is labeled as one.
