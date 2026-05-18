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

## Built-in Optimizer Rules

DataFusion applies a default analyzer, logical optimizer, and physical
optimizer pipeline.

The rule names listed here match the names shown by `EXPLAIN VERBOSE`.

Rule order matters. The default pipeline may change between releases.

### Analyzer Rules

| order | rule                        | summary                                                                                 |
| ----- | --------------------------- | --------------------------------------------------------------------------------------- |
| 1     | `resolve_grouping_function` | Rewrites `GROUPING(...)` calls into expressions over DataFusion's internal grouping id. |
| 2     | `type_coercion`             | Adds implicit casts so operators and functions receive valid input types.               |

### Logical Optimizer Rules

| order | rule                                      | summary                                                                                                                     |
| ----- | ----------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| 1     | `rewrite_set_comparison`                  | Rewrites `ANY` and `ALL` set-comparison subqueries into `EXISTS`-based boolean expressions with correct SQL NULL semantics. |
| 2     | `optimize_unions`                         | Flattens nested unions and removes unions with a single input.                                                              |
| 3     | `simplify_expressions`                    | Constant-folds and simplifies expressions while preserving output names.                                                    |
| 4     | `replace_distinct_aggregate`              | Rewrites `DISTINCT` and `DISTINCT ON` operators into aggregate-based plans that later rules can optimize further.           |
| 5     | `eliminate_join`                          | Replaces keyless inner joins with a literal `false` filter by an empty relation.                                            |
| 6     | `decorrelate_predicate_subquery`          | Converts eligible `IN` and `EXISTS` predicate subqueries into semi or anti joins.                                           |
| 7     | `scalar_subquery_to_join`                 | Rewrites eligible scalar subqueries into joins and adds schema-preserving projections.                                      |
| 8     | `decorrelate_lateral_join`                | Rewrites eligible lateral joins into regular joins.                                                                         |
| 9     | `extract_equijoin_predicate`              | Splits join filters into equijoin keys and residual predicates.                                                             |
| 10    | `eliminate_duplicated_expr`               | Removes duplicate expressions from projections, aggregates, and similar operators.                                          |
| 11    | `eliminate_filter`                        | Drops always-true filters and replaces always-false or NULL filters with empty relations.                                   |
| 12    | `eliminate_cross_join`                    | Uses filter predicates to replace cross joins with inner joins when join keys can be found.                                 |
| 13    | `eliminate_limit`                         | Removes no-op limits and simplifies trivial limit shapes.                                                                   |
| 14    | `propagate_empty_relation`                | Pushes empty-relation knowledge upward so operators fed by no rows collapse early.                                          |
| 15    | `filter_null_join_keys`                   | Adds `IS NOT NULL` filters to nullable equijoin keys that can never match.                                                  |
| 16    | `eliminate_outer_join`                    | Rewrites outer joins to inner joins when later filters reject the NULL-extended rows.                                       |
| 17    | `push_down_limit`                         | Moves literal limits closer to scans and unions and merges adjacent limits.                                                 |
| 18    | `push_down_filter`                        | Moves filters as early as possible through filter-commutative operators.                                                    |
| 19    | `single_distinct_aggregation_to_group_by` | Rewrites single-column `DISTINCT` aggregations into two-stage `GROUP BY` plans.                                             |
| 20    | `eliminate_group_by_constant`             | Removes constant or functionally redundant expressions from `GROUP BY`.                                                     |
| 21    | `common_sub_expression_eliminate`         | Computes repeated subexpressions once and reuses the result.                                                                |
| 22    | `extract_leaf_expressions`                | Pulls cheap leaf expressions closer to data sources so later pruning and filter rules can act earlier.                      |
| 23    | `push_down_leaf_projections`              | Pushes the helper projections created by leaf extraction toward leaf inputs.                                                |
| 24    | `optimize_projections`                    | Prunes unused columns and removes unnecessary logical projections.                                                          |

### Physical Optimizer Rules

The same rule name may appear more than once when the default pipeline runs it
in multiple phases.

| order | rule                           | phase                   | summary                                                                                                      |
| ----- | ------------------------------ | ----------------------- | ------------------------------------------------------------------------------------------------------------ |
| 1     | `OutputRequirements`           | add phase               | Adds helper nodes so output requirements survive later physical rewrites.                                    |
| 2     | `aggregate_statistics`         | -                       | Uses exact source statistics to answer some aggregates without scanning data.                                |
| 3     | `join_selection`               | -                       | Chooses join implementation, build side, and partition mode from statistics and stream properties.           |
| 4     | `LimitedDistinctAggregation`   | -                       | Pushes limit hints into grouped distinct-style aggregations when only a small result is needed.              |
| 5     | `FilterPushdown`               | pre-optimization phase  | Pushes supported physical filters down toward data sources before distribution and sorting are enforced.     |
| 6     | `EnforceDistribution`          | -                       | Adds repartitioning only where needed to satisfy physical distribution requirements.                         |
| 7     | `CombinePartialFinalAggregate` | -                       | Collapses adjacent partial and final aggregates when the distributed shape makes them redundant.             |
| 8     | `EnforceSorting`               | -                       | Adds or removes local sorts to satisfy required input orderings.                                             |
| 9     | `OptimizeAggregateOrder`       | -                       | Updates aggregate expressions to use the best ordering once sort requirements are known.                     |
| 10    | `WindowTopN`                   | -                       | Replaces eligible row-number window and filter patterns with per-partition TopK execution.                   |
| 11    | `ProjectionPushdown`           | early pass              | Pushes projections toward inputs before later physical rewrites add more limit and TopK structure.           |
| 12    | `OutputRequirements`           | remove phase            | Removes the temporary output-requirement helper nodes after requirement-sensitive planning is done.          |
| 13    | `LimitAggregation`             | -                       | Passes a limit hint into eligible aggregations so they can keep fewer accumulator buckets.                   |
| 14    | `LimitPushPastWindows`         | -                       | Pushes fetch limits through bounded window operators when doing so keeps the result correct.                 |
| 15    | `HashJoinBuffering`            | -                       | Adds buffering on the probe side of hash joins so probing can start before build completion.                 |
| 16    | `LimitPushdown`                | -                       | Moves physical limits into child operators or fetch-enabled variants to cut data early.                      |
| 17    | `TopKRepartition`              | -                       | Pushes TopK below hash repartition when the partition key is a prefix of the sort key.                       |
| 18    | `ProjectionPushdown`           | late pass               | Runs projection pushdown again after limit and TopK rewrites expose new pruning opportunities.               |
| 19    | `PushdownSort`                 | -                       | Pushes sort requirements into data sources that can already return sorted output.                            |
| 20    | `EnsureCooperative`            | -                       | Wraps non-cooperative plan parts so long-running tasks yield fairly.                                         |
| 21    | `FilterPushdown(Post)`         | post-optimization phase | Pushes dynamic filters at the end of optimization, after plan references stop moving.                        |
| 22    | `SanityCheckPlan`              | -                       | Validates that the final physical plan meets ordering, distribution, and infinite-input safety requirements. |
