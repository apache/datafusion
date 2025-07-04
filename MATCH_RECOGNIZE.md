# MATCH_RECOGNIZE in DataFusion  
_A walk-through of the current draft implementation_

> ⚠️ **WARNING – Experimental Feature**  
> This implementation is **not production ready**. Large portions of the code – especially `pattern_matcher.rs` and `functions-window/src/match_recognize.rs` – were vibe-coded with minimal review.  
>  
> Expect missing edge-case handling, correctness issues, and performance problems.
>  
> **This document itself is AI-generated** and may contain inaccuracies or omissions. Use it only as a starting point; always verify details against the source code before relying on them.

This note explains how a `MATCH_RECOGNIZE` statement is translated from SQL into DataFusion's logical and physical plans.

The supported features are based off the current [Snowflake documentation](https://docs.snowflake.com/en/sql-reference/constructs/match_recognize), and limited by the `sqlparser-rs` crate's support of MATCH_RECOGNIZE (no FINAL/RUNNING for MEASURES, EXCLUDE and PERMUTE of symbols only.)

> ⚠️ **Symbol predicates inside the DEFINE clause are *not yet supported*.**

---

## 1.  High-level flow

1. **SQL parsing** – the SQL module recognises the new grammar (table factor `MATCH_RECOGNIZE (…)`) and produces a `TableFactor::MatchRecognize` AST node.

2. **Logical planning** – the SQL planner turns the AST into a hierarchy of logical plan nodes:
   * normalisation of `DEFINE`, `PATTERN`, `MEASURES`, `ROWS PER MATCH`, `AFTER MATCH`
   * explicit `Projection`, `WindowAgg`, `Filter`
   * a new `LogicalPlan::MatchRecognizePattern` node that carries the compiled pattern.

3. **Physical planning** – the core planner detects the new logical node and produces a `MatchRecognizePatternExec`.  
   All remaining operators (projection, window, filter, repartition, …) are produced exactly the same way as for "ordinary" SQL.

4. **Execution** – `MatchRecognizePatternExec` implements pattern matching at runtime, augments every output record batch with five metadata columns and yields the augmented stream.  
   Upstream projections / filters / windows consume those virtual columns.

The rest of this document focuses on step 2 – how the planner constructs the logical plan.

---

## 2.  SQL planner extensions

### 2.1  New planner context

`PlannerContext` now contains an optional `MatchRecognizeContext`.  
When the planner descends into a `MATCH_RECOGNIZE` clause it enables the context to enforce the special scoping rules for

* **qualified identifiers** – they must be of the form `symbol.column`, e.g. `A.price`;
* **window and aggregate functions** – extra implicit arguments are added (see below).

The context also exposes the `PARTITION BY`, `ORDER BY` and `ROWS PER MATCH` clauses so that helper functions can derive default window frames or adjust partitioning.

### 2.2  Handling `DEFINE`

For every symbol reference found in the *pattern* the planner must be able to supply a predicate expression:

```
DEFINE
    A AS price < 50,
    B AS price > 60
```

* All symbols that appear in the pattern are collected first.
* If a symbol has an explicit definition, that definition is planned into a regular `Expr`.
* If it does **not** have a definition, the planner synthesises the constant expression `TRUE`.
* All predicates are gathered in `defines : Vec<(Expr, String)>`, each carrying the predicate as well as the symbol name.

The planner then inserts a **projection** immediately above the input:

```
… → Projection {
        // unchanged input columns
        company,
        price_date,
        price,
        // one boolean column per symbol
        predicate_for_A AS __mr_symbol_A,
        predicate_for_B AS __mr_symbol_B,
        …
    }
```

Those columns serve one single purpose: they are consumed by the pattern matcher at execution time.

If a `DEFINE` expression contains window functions itself the planner inserts a **window node** underneath this projection first; after rebasing the expressions the overall shape becomes:

```
Projection(add __mr_symbol_…)
  Window
    Input
```

### 2.3  Handling `PATTERN`

`PATTERN` is compiled into a nested value of the enum
`datafusion_expr::match_recognize::Pattern` (symbol, concatenation, alternation, repetition, …).

The planner then creates a dedicated logical node

```
LogicalPlan::MatchRecognizePattern {
    input:      Projection(with __mr_symbol_…)
    partition_by: […]
    order_by: […]
    pattern:    Pattern     // compiled tree
    after_skip: Option<…>
    rows_per_match: Option<…>
    symbols:    Vec<String> // ["A","B",…] in declaration order
}
```

The node itself is *purely declarative* – it only describes the pattern; the projection added earlier already made all predicates available.

### 2.4  Handling `MEASURES`

`MEASURES` is conceptually just another projection applied **after** pattern detection.

1. Each measure expression is individually planned through
   `sql_to_expr_with_match_recognize_measures_context`.
   That function
   * enables the special context so that `A.price` is valid,
   * **implicitly** appends hidden columns expected by specialised functions  
     (`FIRST`, `LAST`, `PREV`, `NEXT`, `CLASSIFIER`, …) and
   * asks every registered `ExprPlanner` to post-process the expression.  
     The default planners turn symbol predicates into the dedicated
     window UDF calls (`mr_first`, `mr_prev`, `classifier`, …) and rewrite
     aggregate functions such as

     ```
     COUNT(A.*)  ->  COUNT( CASE WHEN __mr_classifier = 'A' THEN 1 END )
     SUM(A.price) -> SUM( CASE WHEN __mr_classifier = 'A' THEN A.price END )
     ```

2. If at least one measure contains a window function, another **window node** is pushed below the final projection (including a sort & repartition identical to ordinary SQL).

3. Finally the planner calls `rows_filter` and `rows_projection` helpers to apply the semantics of `ROWS PER MATCH`:

   * default (`ONE ROW`)  →  filter on `__mr_is_last_match_row`
   * `ALL ROWS SHOW`      →  filter on `__mr_is_included_row`
   * `ALL ROWS OMIT EMPTY`→  `__mr_is_included_row` **and** classifier ≠ `'(empty)'`
   * `WITH UNMATCHED`     →  no additional filter

   and to choose the projection list (last-row only or all input columns).

The complete logical plan therefore has the following skeleton (greatly simplified):

```
01)Filter <- ROWS PER MATCH filter
02)--Projection <- MEASURES projections
03)----WindowAggr <- MEASURES Window functions
04)------MatchRecognizePattern <- Runs PATTERN on symbols, emits metadata virtual columns
05)--------Projection <- DEFINE symbols projected to virtual columns `__mr_symbol_<…>`
06)----------WindowAggr <- DEFINE Window functions
07)------------TableScan <- Input
```

### 2.5  Virtual columns

The pattern executor generates five metadata columns:

| Name | Type | Meaning |
|------|------|---------|
| `__mr_classifier`            | `Utf8`   | symbol the current row matched (or `'(empty)'`) |
| `__mr_match_number`          | `UInt64` | running match counter, starts at 1 |
| `__mr_match_sequence_number` | `UInt64` | position inside current match, starts at 1 |
| `__mr_is_last_match_row`     | `Boolean`| true on the final row of every match |
| `__mr_is_included_row`       | `Boolean`| true if row is *not* excluded |

They are appended to the schema in `pattern_schema()` and used directly
by filters, partitioning and measures.

---

## 3.  Physical planning and execution

* The core planner recognises `LogicalPlan::MatchRecognizePattern` and
  instantiates `MatchRecognizePatternExec`.

* `MatchRecognizePatternExec`
  * receives the compiled `Pattern`, `partition_by` and `order_by`
  * exposes ordering/partitioning requirements identical to `WindowAggExec`
  * implements `execute()` by
    1. materialising a completely ordered partition,
    2. running the pattern matcher (`PatternMatcher`) which scans the partition once, emits matches and populates the metadata columns,
    3. honouring `AFTER MATCH SKIP` and `ROWS PER MATCH`.

* All projections / window aggregates / filters produced earlier continue to behave exactly as they do for ordinary queries.

---

## 4.  Summary

1. `MATCH_RECOGNIZE` is implemented entirely as a *normal* combination of projections, filters and window aggregates plus one dedicated pattern-matching node.

2. `DEFINE` ⇒ boolean columns (`__mr_symbol_*`)  
   `PATTERN` ⇒ `MatchRecognizePattern` node  
   `MEASURES` ⇒ projection of window / aggregate functions over metadata

3. Everything above the pattern node reuses DataFusion's existing machinery; physical execution differs only in the single custom executor that performs the row-wise NFA scan.
