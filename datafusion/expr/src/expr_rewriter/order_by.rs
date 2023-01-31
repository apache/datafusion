// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Rewrite for order by expressions

use crate::expr::Sort;
use crate::expr_rewriter::{normalize_col, ExprRewritable, ExprRewriter};
use crate::logical_plan::Aggregate;
use crate::utils::grouping_set_to_exprlist;
use crate::{Expr, ExprSchemable, LogicalPlan};
use datafusion_common::Result;

/// Rewrite sort on aggregate expressions to sort on the column of aggregate output
/// For example, `max(x)` is written to `col("MAX(x)")`
pub fn rewrite_sort_cols_by_aggs(
    exprs: impl IntoIterator<Item = impl Into<Expr>>,
    plan: &LogicalPlan,
) -> Result<Vec<Expr>> {
    exprs
        .into_iter()
        .map(|e| {
            let expr = e.into();
            match expr {
                Expr::Sort(Sort {
                    expr,
                    asc,
                    nulls_first,
                }) => {
                    let sort = Expr::Sort(Sort::new(
                        Box::new(rewrite_sort_col_by_aggs(*expr, plan)?),
                        asc,
                        nulls_first,
                    ));
                    Ok(sort)
                }
                expr => Ok(expr),
            }
        })
        .collect()
}

fn rewrite_sort_col_by_aggs(expr: Expr, plan: &LogicalPlan) -> Result<Expr> {
    match plan {
        LogicalPlan::Aggregate(Aggregate {
            input,
            aggr_expr,
            group_expr,
            ..
        }) => {
            struct Rewriter<'a> {
                plan: &'a LogicalPlan,
                input: &'a LogicalPlan,
                aggr_expr: &'a Vec<Expr>,
                distinct_group_exprs: &'a Vec<Expr>,
            }

            impl<'a> ExprRewriter for Rewriter<'a> {
                fn mutate(&mut self, expr: Expr) -> Result<Expr> {
                    let normalized_expr = normalize_col(expr.clone(), self.plan);
                    if normalized_expr.is_err() {
                        // The expr is not based on Aggregate plan output. Skip it.
                        return Ok(expr);
                    }
                    let normalized_expr = normalized_expr?;
                    if let Some(found_agg) = self
                        .aggr_expr
                        .iter()
                        .chain(self.distinct_group_exprs)
                        .find(|a| (**a) == normalized_expr)
                    {
                        let agg = normalize_col(found_agg.clone(), self.plan)?;
                        let col = Expr::Column(
                            agg.to_field(self.input.schema())
                                .map(|f| f.qualified_column())?,
                        );
                        Ok(col)
                    } else {
                        Ok(expr)
                    }
                }
            }

            let distinct_group_exprs = grouping_set_to_exprlist(group_expr.as_slice())?;
            expr.rewrite(&mut Rewriter {
                plan,
                input,
                aggr_expr,
                distinct_group_exprs: &distinct_group_exprs,
            })
        }
        LogicalPlan::Projection(_) => rewrite_sort_col_by_aggs(expr, plan.inputs()[0]),
        _ => Ok(expr),
    }
}

#[cfg(test)]
mod test {
    use std::ops::Add;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use crate::{
        col, lit, logical_plan::builder::LogicalTableSource, min, LogicalPlanBuilder,
    };

    use super::*;

    #[test]
    fn rewrite_sort_cols_by_agg() {
        //  gby c1, agg: min(c2)
        let agg = make_input()
            .aggregate(
                // gby: c1
                vec![col("c1")],
                // agg: min(c2)
                vec![min(col("c2"))],
            )
            .unwrap()
            .build()
            .unwrap();

        let cases = vec![
            TestCase {
                desc: "c1 --> t.c1",
                input: sort(col("c1")),
                expected: sort(col("t.c1")),
            },
            TestCase {
                desc: "c1 + c2 --> t.c1 + t.c2a",
                input: sort(col("c1") + col("c1")),
                expected: sort(col("t.c1") + col("t.c1")),
            },
            TestCase {
                desc: r#"min(c2) --> "MIN(t.c2)" (column *named* "min(t.c2)"!)"#,
                input: sort(min(col("c2"))),
                expected: sort(col("MIN(t.c2)")),
            },
            TestCase {
                desc: r#"c1 + min(c2) --> "t.c1 + MIN(t.c2)" (column *named* "min(t.c2)"!)"#,
                input: sort(col("c1") + min(col("c2"))),
                expected: sort(col("t.c1") + col("MIN(t.c2)")),
            },
        ];

        for case in cases {
            case.run(&agg)
        }
    }

    #[test]
    fn rewrite_sort_cols_by_agg_alias() {
        let agg = make_input()
            .aggregate(
                // gby c1
                vec![col("c1")],
                // agg: min(2)
                vec![min(col("c2"))],
            )
            .unwrap()
            //  projects out an expression "c1" that is different than the column "c1"
            .project(vec![
                // c1 + 1 as c1,
                col("c1").add(lit(1)).alias("c1"),
                // min(c2)
                min(col("c2")),
            ])
            .unwrap()
            .build()
            .unwrap();

        let cases = vec![
            TestCase {
                desc: "c1 --> c1  -- column *named* c1 that came out of the projection, (not t.c1)",
                input: sort(col("c1")),
                // Incorrect due to https://github.com/apache/arrow-datafusion/issues/4854
                // should be "c1" not t.c1
                expected: sort(col("t.c1")),
            },
            TestCase {
                desc: r#"min(c2) --> "MIN(c2)" -- (column *named* "min(t.c2)"!)"#,
                input: sort(min(col("c2"))),
                expected: sort(col("MIN(t.c2)")),
            },
            TestCase {
                desc: r#"c1 + min(c2) --> "c1 + MIN(c2)" -- (column *named* "min(t.c2)"!)"#,
                input: sort(col("c1") + min(col("c2"))),
                // Incorrect due to https://github.com/apache/arrow-datafusion/issues/4854
                // should be "c1" not t.c1
                expected: sort(col("t.c1") + col("MIN(t.c2)")),
            }
        ];

        for case in cases {
            case.run(&agg)
        }
    }

    struct TestCase {
        desc: &'static str,
        input: Expr,
        expected: Expr,
    }

    impl TestCase {
        /// calls rewrite_sort_cols_by_aggs for expr and compares it to expected_expr
        fn run(self, input_plan: &LogicalPlan) {
            let Self {
                desc,
                input,
                expected,
            } = self;

            println!("running: '{desc}'");
            let mut exprs =
                rewrite_sort_cols_by_aggs(vec![input.clone()], input_plan).unwrap();

            assert_eq!(exprs.len(), 1);
            let rewritten = exprs.pop().unwrap();

            assert_eq!(
                rewritten, expected,
                "\n\ninput:{input:?}\nrewritten:{rewritten:?}\nexpected:{expected:?}\n"
            );
        }
    }

    /// Scan of a table: t(c1 int, c2 varchar, c3 float)
    fn make_input() -> LogicalPlanBuilder {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Utf8, true),
            Field::new("c3", DataType::Float64, true),
        ]));
        let projection = None;
        LogicalPlanBuilder::scan(
            "t",
            Arc::new(LogicalTableSource::new(schema)),
            projection,
        )
        .unwrap()
    }

    fn sort(expr: Expr) -> Expr {
        let asc = true;
        let nulls_first = true;
        expr.sort(asc, nulls_first)
    }
}
