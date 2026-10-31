<!---
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

# Syntax vs Semantics

DataFusion rejects invalid queries in several different places: the SQL parser,
the SQL planner, `LogicalPlan` constructors, the `Analyzer`, and finally during
execution. This document specifies which kind of check belongs where.

The short version:

- The **parser** checks **syntax** only.
- **Semantic** checks belong on the `LogicalPlan`, so they apply no matter how
  the plan was built.

## Definitions

A **syntax** check answers "is this text a well formed statement in DataFusion's
SQL dialect?". For example, `SELECT FROM t` is not a statement, and
`CREATE EXTERNAL TABLE t STORED AS CSV` is missing its required `LOCATION`
clause. Answering such a question requires nothing but the text itself.

A **semantic** check answers "does this well formed statement describe something
DataFusion can compute?". For example:

- `SELECT nonexistent FROM t` (no such column)
- `SELECT * FROM t WHERE 1` (predicate is not boolean)
- `SELECT * FROM t WHERE sum(a) > 0` (aggregate in `WHERE`)
- `CREATE FUNCTION f(DOUBLE, DOUBLE) RETURNS DOUBLE RETURN $1 + $3` (no third
  argument to reference)

Answering these questions requires more than the text: the catalog, schemas,
types, and the surrounding plan.

## The parser checks syntax only

DataFusion parses SQL with [sqlparser-rs]; [`DFParser`] extends it with
DataFusion specific statements such as `CREATE EXTERNAL TABLE` and `COPY`.
sqlparser-rs is explicitly a syntax-only parser, as described in its
[Syntax vs Semantics] section:

> This crate provides only a syntax parser, and tries to avoid applying any SQL
> semantics, and accepts queries that specific databases would reject, even when
> using that Database's specific `Dialect`. For example,
> `CREATE TABLE(x int, x int)` is accepted by this crate, even though most SQL
> engines will reject this statement due to the repeated column name `x`.

`DFParser` follows the same rule: parsing produces an AST, and says nothing
about whether the statement is meaningful. Semantic checks do not belong in the
parser because:

1. The parser has no access to what it would need to decide: the catalog,
   schemas, registered functions, or session configuration.
2. The AST is also used for purposes other than planning, such as round
   tripping SQL text, and rejecting statements there makes those uses
   impossible.
3. A check in the parser applies only to SQL text that DataFusion parsed
   itself, and is skipped by every other way of building a plan (see below).

## Semantic checks belong on the `LogicalPlan`

SQL is only one of the ways a `LogicalPlan` is created. Plans also come from:

- the [DataFrame API] and [`LogicalPlanBuilder`]
- [Substrait] and the protobuf serialization in `datafusion-proto`
- other SQL dialects and query languages built on DataFusion, which may use
  their own parser and planner

A check in [`SqlToRel`] only covers the first of these. If the rule is a
property of the plan rather than of SQL text, enforce it where every frontend
must pass: the constructor of the `LogicalPlan` node (or of the structure it
carries), in the `datafusion-expr` crate. That is what `Filter::try_new`,
`Aggregate::try_new`, `Union::try_new`, and friends are for. A new statement
type should get a `try_new` that validates its own rules rather than relying on
the SQL planner to do so.

Placing the check there also means the rule is enforced once, is tested once,
and cannot be bypassed by a rewrite rule or an extension that rebuilds the node.

| Question the check answers                                          | Where it belongs                                           |
| ------------------------------------------------------------------- | ---------------------------------------------------------- |
| Is the text a well formed statement?                                | `DFParser` / sqlparser-rs (`datafusion/sql/src/parser.rs`) |
| Can this SQL construct be represented as a plan at all?             | `SqlToRel` (`datafusion/sql/src/`)                         |
| Does this plan node make sense (types, arity, references, options)? | `try_new` on the node in `datafusion/expr/`                |
| Does the rule need the whole plan, or types after coercion?         | An `AnalyzerRule`, or a [plan invariant](invariants.md)    |
| Can it only be known from the data itself?                          | Execution (for example, an overflowing cast)               |

### What stays in the SQL planner

[`SqlToRel`] converts the AST into a `LogicalPlan`, including name and type
resolution ("binding"). Two kinds of errors legitimately originate there:

- **Unsupported SQL**: a clause DataFusion has no plan representation for, such
  as `INSERT ... RETURNING`. There is no plan node to validate, so the SQL
  planner is the only place the error can be raised.
- **SQL specific context**: spans and [`Diagnostic`]s that point at the offending
  text. Note this is context added to an error, not the check itself.

### Checks that need the whole plan

Some rules cannot be decided when a single node is built, for example because
they depend on types after coercion, or on the relationship between a subquery
and its outer plan. Those belong in an `AnalyzerRule` (such as type coercion) or
in the logical plan invariants, described in [Invariants](invariants.md) and
implemented in `datafusion/expr/src/logical_plan/invariants.rs`.

## Testing semantic checks

Prefer an end to end [sqllogictest] (`.slt`) case for the user visible error,
rather than only a unit test of the function that happens to implement the check
today. End to end tests keep passing when a check moves to a different layer,
which is exactly the refactoring this document encourages. When a rule applies
to plans built directly, add a test that builds the plan with the DataFrame API
or `LogicalPlanBuilder` as well.

## Examples

### Checks in the right place

- **Non boolean filter predicate** and **window functions in a filter
  predicate**: both are enforced in `Filter::try_new`
  (`datafusion/expr/src/logical_plan/plan.rs`), so `WHERE` in SQL and
  `DataFrame::filter` report the same error at the same point:

  ```text
  // SQL: SELECT * FROM t WHERE 1
  Error during planning: Cannot create filter with non-boolean predicate 'Int64(1)' returning Int64

  // DataFrame: df.filter(lit(1))
  Error during planning: Cannot create filter with non-boolean predicate 'Int32(1)' returning Int32
  ```

- **Nested aggregate or window calls**: `check_aggregate_and_window_nesting`
  (`datafusion/expr/src/utils.rs`), called from `Aggregate::try_new` and
  `Window::try_new`.
- **Mismatched `UNION` inputs**: `Union::try_new`.
- **Type coercion**: the `TypeCoercion` `AnalyzerRule`, which runs on every plan
  regardless of how it was built.
- **Invalid correlated subqueries**: `assert_valid_semantic_plan`
  (`datafusion/expr/src/logical_plan/invariants.rs`).

### Checks in the wrong place

These are existing violations, kept here as illustrations. They are not
precedents for new code, and PRs moving them are welcome.

- **Aggregates in `WHERE`** are rejected in the SQL planner
  (`datafusion/sql/src/select.rs`), unlike window functions in `WHERE`, which
  are rejected in `Filter::try_new`. The consequence is visible from the
  DataFrame API, which builds the invalid plan and only fails later, during
  physical planning, with a much less helpful message:

  ```text
  // SQL: SELECT * FROM t WHERE sum(a) > 0
  Error during planning: Aggregate functions are not allowed in the WHERE clause. Consider using HAVING instead

  // DataFrame: df.filter(sum(col("a")).gt(lit(0)))
  Error during planning: Aggregate function 'sum(CAST(t.a AS Int64))' is not supported in this position. Aggregate functions are supported in the SELECT list, HAVING and ORDER BY of a query with GROUP BY
  ```

- **`CREATE EXTERNAL TABLE` clause combinations**, such as
  `'IF NOT EXISTS' cannot coexist with 'REPLACE'` and
  `Constraints on Partition Columns are not supported`, are rejected by
  `DFParser` (`datafusion/sql/src/parser.rs`) although both statements parse.
- **Column references in a `DEFAULT` expression** are rejected in `SqlToRel`
  (`datafusion/sql/src/planner.rs`) rather than when the DDL node is built.

[sqlparser-rs]: https://github.com/apache/datafusion-sqlparser-rs
[syntax vs semantics]: https://github.com/apache/datafusion-sqlparser-rs#syntax-vs-semantics
[`dfparser`]: https://docs.rs/datafusion/latest/datafusion/sql/parser/struct.DFParser.html
[`sqltorel`]: https://docs.rs/datafusion/latest/datafusion/sql/planner/struct.SqlToRel.html
[`logicalplanbuilder`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.LogicalPlanBuilder.html
[`diagnostic`]: https://docs.rs/datafusion/latest/datafusion/common/struct.Diagnostic.html
[dataframe api]: ../../user-guide/dataframe.md
[substrait]: https://docs.rs/datafusion-substrait/latest/datafusion_substrait/
[sqllogictest]: ../testing.md#sqllogictests-tests
