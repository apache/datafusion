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

//! [`CommonSubexprEliminate`] to avoid redundant computation of common sub-expressions

use std::collections::{BTreeSet, HashMap};
use std::hash::{BuildHasher, Hash, Hasher, RandomState};
use std::sync::Arc;

use crate::{OptimizerConfig, OptimizerRule};

use crate::optimizer::ApplyOrder;
use crate::utils::NamePreserver;
use datafusion_common::alias::AliasGenerator;
use datafusion_common::hash_utils::combine_hashes;
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
    TreeNodeVisitor,
};
use datafusion_common::{qualified_name, Column, DFSchema, DFSchemaRef, Result};
use datafusion_expr::expr::{Alias, ScalarFunction};
use datafusion_expr::logical_plan::{
    Aggregate, Filter, LogicalPlan, Projection, Sort, Window,
};
use datafusion_expr::tree_node::replace_sort_expressions;
use datafusion_expr::{col, BinaryExpr, Case, Expr, Operator};
use indexmap::IndexMap;

const CSE_PREFIX: &str = "__common_expr";

/// Identifier that represents a subexpression tree.
///
/// This identifier is designed to be efficient and  "hash", "accumulate", "equal" and
/// "have no collision (as low as possible)"
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Identifier<'n> {
    // Hash of `expr` built up incrementally during the first, visiting traversal, but its
    // value is not necessarily equal to `expr.hash()`.
    hash: u64,
    expr: &'n Expr,
}

impl<'n> Identifier<'n> {
    fn new(expr: &'n Expr, random_state: &RandomState) -> Self {
        let mut hasher = random_state.build_hasher();
        expr.hash_node(&mut hasher);
        let hash = hasher.finish();
        Self { hash, expr }
    }

    fn combine(mut self, other: Option<Self>) -> Self {
        other.map_or(self, |other_id| {
            self.hash = combine_hashes(self.hash, other_id.hash);
            self
        })
    }
}

impl Hash for Identifier<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

/// A cache that contains the postorder index and the identifier of expression tree nodes
/// by the preorder index of the nodes.
///
/// This cache is filled by `ExprIdentifierVisitor` during the first traversal and is used
/// by `CommonSubexprRewriter` during the second traversal.
///
/// The purpose of this cache is to quickly find the identifier of a node during the
/// second traversal.
///
/// Elements in this array are added during `f_down` so the indexes represent the preorder
/// index of expression nodes and thus element 0 belongs to the root of the expression
/// tree.
/// The elements of the array are tuples that contain:
/// - Postorder index that belongs to the preorder index. Assigned during `f_up`, start
///   from 0.
/// - Identifier of the expression. If empty (`""`), expr should not be considered for
///   CSE.
///
/// # Example
/// An expression like `(a + b)` would have the following `IdArray`:
/// ```text
/// [
///   (2, "a + b"),
///   (1, "a"),
///   (0, "b")
/// ]
/// ```
type IdArray<'n> = Vec<(usize, Option<Identifier<'n>>)>;

/// A map that contains the number of normal and conditional occurrences of expressions by
/// their identifiers.
type ExprStats<'n> = HashMap<Identifier<'n>, (usize, usize)>;

/// A map that contains the common expressions and their alias extracted during the
/// second, rewriting traversal.
type CommonExprs<'n> = IndexMap<Identifier<'n>, (Expr, String)>;

/// Performs Common Sub-expression Elimination optimization.
///
/// This optimization improves query performance by computing expressions that
/// appear more than once and reusing those results rather than re-computing the
/// same value
///
/// Currently only common sub-expressions within a single `LogicalPlan` are
/// eliminated.
///
/// # Example
///
/// Given a projection that computes the same expensive expression
/// multiple times such as parsing as string as a date with `to_date` twice:
///
/// ```text
/// ProjectionExec(expr=[extract (day from to_date(c1)), extract (year from to_date(c1))])
/// ```
///
/// This optimization will rewrite the plan to compute the common expression once
/// using a new `ProjectionExec` and then rewrite the original expressions to
/// refer to that new column.
///
/// ```text
/// ProjectionExec(exprs=[extract (day from new_col), extract (year from new_col)]) <-- reuse here
///   ProjectionExec(exprs=[to_date(c1) as new_col]) <-- compute to_date once
/// ```
#[derive(Debug)]
pub struct CommonSubexprEliminate {
    random_state: RandomState,
}

/// The result of potentially rewriting a list of expressions to eliminate common
/// subexpressions.
#[derive(Debug)]
enum FoundCommonExprs {
    /// No common expressions were found
    No { original_exprs_list: Vec<Vec<Expr>> },
    /// Common expressions were found
    Yes {
        /// extracted common expressions
        common_exprs: Vec<(Expr, String)>,
        /// new expressions with common subexpressions replaced
        new_exprs_list: Vec<Vec<Expr>>,
        /// original expressions
        original_exprs_list: Vec<Vec<Expr>>,
    },
}

impl CommonSubexprEliminate {
    pub fn new() -> Self {
        Self {
            random_state: RandomState::new(),
        }
    }

    /// Returns the identifier list for each element in `exprs` and a flag to indicate if
    /// rewrite phase of CSE make sense.
    ///
    /// Returns and array with 1 element for each input expr in `exprs`
    ///
    /// Each element is itself the result of [`CommonSubexprEliminate::expr_to_identifier`] for that expr
    /// (e.g. the identifiers for each node in the tree)
    fn to_arrays<'n>(
        &self,
        exprs: &'n [Expr],
        expr_stats: &mut ExprStats<'n>,
        expr_mask: ExprMask,
    ) -> Result<(bool, Vec<IdArray<'n>>)> {
        let mut found_common = false;
        exprs
            .iter()
            .map(|e| {
                let mut id_array = vec![];
                self.expr_to_identifier(e, expr_stats, &mut id_array, expr_mask)
                    .map(|fc| {
                        found_common |= fc;

                        id_array
                    })
            })
            .collect::<Result<Vec<_>>>()
            .map(|id_arrays| (found_common, id_arrays))
    }

    /// Add an identifier to `id_array` for every subexpression in this tree.
    fn expr_to_identifier<'n>(
        &self,
        expr: &'n Expr,
        expr_stats: &mut ExprStats<'n>,
        id_array: &mut IdArray<'n>,
        expr_mask: ExprMask,
    ) -> Result<bool> {
        let mut visitor = ExprIdentifierVisitor {
            expr_stats,
            id_array,
            visit_stack: vec![],
            down_index: 0,
            up_index: 0,
            expr_mask,
            random_state: &self.random_state,
            found_common: false,
            conditional: false,
        };
        expr.visit(&mut visitor)?;

        Ok(visitor.found_common)
    }

    /// Rewrites `exprs_list` with common sub-expressions replaced with a new
    /// column.
    ///
    /// `common_exprs` is updated with any sub expressions that were replaced.
    ///
    /// Returns the rewritten expressions
    fn rewrite_exprs_list<'n>(
        &self,
        exprs_list: Vec<Vec<Expr>>,
        arrays_list: &[Vec<IdArray<'n>>],
        expr_stats: &ExprStats<'n>,
        common_exprs: &mut CommonExprs<'n>,
        alias_generator: &AliasGenerator,
    ) -> Result<Vec<Vec<Expr>>> {
        exprs_list
            .into_iter()
            .zip(arrays_list.iter())
            .map(|(exprs, arrays)| {
                exprs
                    .into_iter()
                    .zip(arrays.iter())
                    .map(|(expr, id_array)| {
                        replace_common_expr(
                            expr,
                            id_array,
                            expr_stats,
                            common_exprs,
                            alias_generator,
                        )
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Extracts common sub-expressions and rewrites `exprs_list`.
    ///
    /// Returns `FoundCommonExprs` recording the result of the extraction
    fn find_common_exprs(
        &self,
        exprs_list: Vec<Vec<Expr>>,
        config: &dyn OptimizerConfig,
        expr_mask: ExprMask,
    ) -> Result<Transformed<FoundCommonExprs>> {
        let mut found_common = false;
        let mut expr_stats = ExprStats::new();
        let id_arrays_list = exprs_list
            .iter()
            .map(|exprs| {
                self.to_arrays(exprs, &mut expr_stats, expr_mask).map(
                    |(fc, id_arrays)| {
                        found_common |= fc;

                        id_arrays
                    },
                )
            })
            .collect::<Result<Vec<_>>>()?;
        if found_common {
            let mut common_exprs = CommonExprs::new();
            let new_exprs_list = self.rewrite_exprs_list(
                // Must clone as Identifiers use references to original expressions so we have
                // to keep the original expressions intact.
                exprs_list.clone(),
                &id_arrays_list,
                &expr_stats,
                &mut common_exprs,
                config.alias_generator().as_ref(),
            )?;
            assert!(!common_exprs.is_empty());

            Ok(Transformed::yes(FoundCommonExprs::Yes {
                common_exprs: common_exprs.into_values().collect(),
                new_exprs_list,
                original_exprs_list: exprs_list,
            }))
        } else {
            Ok(Transformed::no(FoundCommonExprs::No {
                original_exprs_list: exprs_list,
            }))
        }
    }

    fn try_optimize_proj(
        &self,
        projection: Projection,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let Projection {
            expr,
            input,
            schema,
            ..
        } = projection;
        let input = Arc::unwrap_or_clone(input);
        self.try_unary_plan(expr, input, config)?
            .map_data(|(new_expr, new_input)| {
                Projection::try_new_with_schema(new_expr, Arc::new(new_input), schema)
                    .map(LogicalPlan::Projection)
            })
    }
    fn try_optimize_sort(
        &self,
        sort: Sort,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let Sort { expr, input, fetch } = sort;
        let input = Arc::unwrap_or_clone(input);
        let sort_expressions = expr.iter().map(|sort| sort.expr.clone()).collect();
        let new_sort = self
            .try_unary_plan(sort_expressions, input, config)?
            .update_data(|(new_expr, new_input)| {
                LogicalPlan::Sort(Sort {
                    expr: replace_sort_expressions(expr, new_expr),
                    input: Arc::new(new_input),
                    fetch,
                })
            });
        Ok(new_sort)
    }

    fn try_optimize_filter(
        &self,
        filter: Filter,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let Filter {
            predicate, input, ..
        } = filter;
        let input = Arc::unwrap_or_clone(input);
        let expr = vec![predicate];
        self.try_unary_plan(expr, input, config)?
            .map_data(|(mut new_expr, new_input)| {
                assert_eq!(new_expr.len(), 1); // passed in vec![predicate]
                let new_predicate = new_expr.pop().unwrap();
                Filter::try_new(new_predicate, Arc::new(new_input))
                    .map(LogicalPlan::Filter)
            })
    }

    fn try_optimize_window(
        &self,
        window: Window,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // Collects window expressions from consecutive `LogicalPlan::Window` nodes into
        // a list.
        let (window_expr_list, window_schemas, input) =
            get_consecutive_window_exprs(window);

        // Extract common sub-expressions from the list.
        self.find_common_exprs(window_expr_list, config, ExprMask::Normal)?
            .map_data(|common| match common {
                // If there are common sub-expressions, then the insert a projection node
                // with the common expressions between the new window nodes and the
                // original input.
                FoundCommonExprs::Yes {
                    common_exprs,
                    new_exprs_list,
                    original_exprs_list,
                } => {
                    build_common_expr_project_plan(input, common_exprs).map(|new_input| {
                        (new_exprs_list, new_input, Some(original_exprs_list))
                    })
                }
                FoundCommonExprs::No {
                    original_exprs_list,
                } => Ok((original_exprs_list, input, None)),
            })?
            // Recurse into the new input.
            // (This is similar to what a `ApplyOrder::TopDown` optimizer rule would do.)
            .transform_data(|(new_window_expr_list, new_input, window_expr_list)| {
                self.rewrite(new_input, config)?.map_data(|new_input| {
                    Ok((new_window_expr_list, new_input, window_expr_list))
                })
            })?
            // Rebuild the consecutive window nodes.
            .map_data(|(new_window_expr_list, new_input, window_expr_list)| {
                // If there were common expressions extracted, then we need to make sure
                // we restore the original column names.
                // TODO: Although `find_common_exprs()` inserts aliases around extracted
                //  common expressions this doesn't mean that the original column names
                //  (schema) are preserved due to the inserted aliases are not always at
                //  the top of the expression.
                //  Let's consider improving `find_common_exprs()` to always keep column
                //  names and get rid of additional name preserving logic here.
                if let Some(window_expr_list) = window_expr_list {
                    let name_preserver = NamePreserver::new_for_projection();
                    let saved_names = window_expr_list
                        .iter()
                        .map(|exprs| {
                            exprs
                                .iter()
                                .map(|expr| name_preserver.save(expr))
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>();
                    new_window_expr_list.into_iter().zip(saved_names).try_rfold(
                        new_input,
                        |plan, (new_window_expr, saved_names)| {
                            let new_window_expr = new_window_expr
                                .into_iter()
                                .zip(saved_names)
                                .map(|(new_window_expr, saved_name)| {
                                    saved_name.restore(new_window_expr)
                                })
                                .collect::<Vec<_>>();
                            Window::try_new(new_window_expr, Arc::new(plan))
                                .map(LogicalPlan::Window)
                        },
                    )
                } else {
                    new_window_expr_list
                        .into_iter()
                        .zip(window_schemas)
                        .try_rfold(new_input, |plan, (new_window_expr, schema)| {
                            Window::try_new_with_schema(
                                new_window_expr,
                                Arc::new(plan),
                                schema,
                            )
                            .map(LogicalPlan::Window)
                        })
                }
            })
    }

    fn try_optimize_aggregate(
        &self,
        aggregate: Aggregate,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let Aggregate {
            group_expr,
            aggr_expr,
            input,
            schema,
            ..
        } = aggregate;
        let input = Arc::unwrap_or_clone(input);
        // Extract common sub-expressions from the aggregate and grouping expressions.
        self.find_common_exprs(vec![group_expr, aggr_expr], config, ExprMask::Normal)?
            .map_data(|common| {
                match common {
                    // If there are common sub-expressions, then insert a projection node
                    // with the common expressions between the new aggregate node and the
                    // original input.
                    FoundCommonExprs::Yes {
                        common_exprs,
                        mut new_exprs_list,
                        mut original_exprs_list,
                    } => {
                        let new_aggr_expr = new_exprs_list.pop().unwrap();
                        let new_group_expr = new_exprs_list.pop().unwrap();

                        build_common_expr_project_plan(input, common_exprs).map(
                            |new_input| {
                                let aggr_expr = original_exprs_list.pop().unwrap();
                                (
                                    new_aggr_expr,
                                    new_group_expr,
                                    new_input,
                                    Some(aggr_expr),
                                )
                            },
                        )
                    }

                    FoundCommonExprs::No {
                        mut original_exprs_list,
                    } => {
                        let new_aggr_expr = original_exprs_list.pop().unwrap();
                        let new_group_expr = original_exprs_list.pop().unwrap();

                        Ok((new_aggr_expr, new_group_expr, input, None))
                    }
                }
            })?
            // Recurse into the new input.
            // (This is similar to what a `ApplyOrder::TopDown` optimizer rule would do.)
            .transform_data(|(new_aggr_expr, new_group_expr, new_input, aggr_expr)| {
                self.rewrite(new_input, config)?.map_data(|new_input| {
                    Ok((
                        new_aggr_expr,
                        new_group_expr,
                        aggr_expr,
                        Arc::new(new_input),
                    ))
                })
            })?
            // Try extracting common aggregate expressions and rebuild the aggregate node.
            .transform_data(|(new_aggr_expr, new_group_expr, aggr_expr, new_input)| {
                // Extract common aggregate sub-expressions from the aggregate expressions.
                self.find_common_exprs(
                    vec![new_aggr_expr],
                    config,
                    ExprMask::NormalAndAggregates,
                )?
                .map_data(|common| {
                    match common {
                        FoundCommonExprs::Yes {
                            common_exprs,
                            mut new_exprs_list,
                            mut original_exprs_list,
                        } => {
                            let rewritten_aggr_expr = new_exprs_list.pop().unwrap();
                            let new_aggr_expr = original_exprs_list.pop().unwrap();

                            let mut agg_exprs = common_exprs
                                .into_iter()
                                .map(|(expr, expr_alias)| expr.alias(expr_alias))
                                .collect::<Vec<_>>();

                            let mut proj_exprs = vec![];
                            for expr in &new_group_expr {
                                extract_expressions(expr, &mut proj_exprs)
                            }
                            for (expr_rewritten, expr_orig) in
                                rewritten_aggr_expr.into_iter().zip(new_aggr_expr)
                            {
                                if expr_rewritten == expr_orig {
                                    if let Expr::Alias(Alias { expr, name, .. }) =
                                        expr_rewritten
                                    {
                                        agg_exprs.push(expr.alias(&name));
                                        proj_exprs
                                            .push(Expr::Column(Column::from_name(name)));
                                    } else {
                                        let expr_alias =
                                            config.alias_generator().next(CSE_PREFIX);
                                        let (qualifier, field_name) =
                                            expr_rewritten.qualified_name();
                                        let out_name = qualified_name(
                                            qualifier.as_ref(),
                                            &field_name,
                                        );

                                        agg_exprs.push(expr_rewritten.alias(&expr_alias));
                                        proj_exprs.push(
                                            Expr::Column(Column::from_name(expr_alias))
                                                .alias(out_name),
                                        );
                                    }
                                } else {
                                    proj_exprs.push(expr_rewritten);
                                }
                            }

                            let agg = LogicalPlan::Aggregate(Aggregate::try_new(
                                new_input,
                                new_group_expr,
                                agg_exprs,
                            )?);
                            Projection::try_new(proj_exprs, Arc::new(agg))
                                .map(LogicalPlan::Projection)
                        }

                        // If there aren't any common aggregate sub-expressions, then just
                        // rebuild the aggregate node.
                        FoundCommonExprs::No {
                            mut original_exprs_list,
                        } => {
                            let rewritten_aggr_expr = original_exprs_list.pop().unwrap();

                            // If there were common expressions extracted, then we need to
                            // make sure we restore the original column names.
                            // TODO: Although `find_common_exprs()` inserts aliases around
                            //  extracted common expressions this doesn't mean that the
                            //  original column names (schema) are preserved due to the
                            //  inserted aliases are not always at the top of the
                            //  expression.
                            //  Let's consider improving `find_common_exprs()` to always
                            //  keep column names and get rid of additional name
                            //  preserving logic here.
                            if let Some(aggr_expr) = aggr_expr {
                                let name_perserver = NamePreserver::new_for_projection();
                                let saved_names = aggr_expr
                                    .iter()
                                    .map(|expr| name_perserver.save(expr))
                                    .collect::<Vec<_>>();
                                let new_aggr_expr = rewritten_aggr_expr
                                    .into_iter()
                                    .zip(saved_names)
                                    .map(|(new_expr, saved_name)| {
                                        saved_name.restore(new_expr)
                                    })
                                    .collect::<Vec<Expr>>();

                                // Since `group_expr` may have changed, schema may also.
                                // Use `try_new()` method.
                                Aggregate::try_new(
                                    new_input,
                                    new_group_expr,
                                    new_aggr_expr,
                                )
                                .map(LogicalPlan::Aggregate)
                            } else {
                                Aggregate::try_new_with_schema(
                                    new_input,
                                    new_group_expr,
                                    rewritten_aggr_expr,
                                    schema,
                                )
                                .map(LogicalPlan::Aggregate)
                            }
                        }
                    }
                })
            })
    }

    /// Rewrites the expr list and input to remove common subexpressions
    ///
    /// # Parameters
    ///
    /// * `exprs`: List of expressions in the node
    /// * `input`: input plan (that produces the columns referred to in `exprs`)
    ///
    /// # Return value
    ///
    ///  Returns `(rewritten_exprs, new_input)`. `new_input` is either:
    ///
    /// 1. The original `input` of no common subexpressions were extracted
    /// 2. A newly added projection on top of the original input
    ///    that computes the common subexpressions
    fn try_unary_plan(
        &self,
        exprs: Vec<Expr>,
        input: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<(Vec<Expr>, LogicalPlan)>> {
        // Extract common sub-expressions from the expressions.
        self.find_common_exprs(vec![exprs], config, ExprMask::Normal)?
            .map_data(|common| match common {
                FoundCommonExprs::Yes {
                    common_exprs,
                    mut new_exprs_list,
                    original_exprs_list: _,
                } => {
                    let new_exprs = new_exprs_list.pop().unwrap();
                    build_common_expr_project_plan(input, common_exprs)
                        .map(|new_input| (new_exprs, new_input))
                }
                FoundCommonExprs::No {
                    mut original_exprs_list,
                } => {
                    let new_exprs = original_exprs_list.pop().unwrap();
                    Ok((new_exprs, input))
                }
            })?
            // Recurse into the new input.
            // (This is similar to what a `ApplyOrder::TopDown` optimizer rule would do.)
            .transform_data(|(new_exprs, new_input)| {
                self.rewrite(new_input, config)?
                    .map_data(|new_input| Ok((new_exprs, new_input)))
            })
    }
}

/// Get all window expressions inside the consecutive window operators.
///
/// Returns the window expressions, and the input to the deepest child
/// LogicalPlan.
///
/// For example, if the input window looks like
///
/// ```text
///   LogicalPlan::Window(exprs=[a, b, c])
///     LogicalPlan::Window(exprs=[d])
///       InputPlan
/// ```
///
/// Returns:
/// *  `window_exprs`: `[[a, b, c], [d]]`
/// * InputPlan
///
/// Consecutive window expressions may refer to same complex expression.
///
/// If same complex expression is referred more than once by subsequent
/// `WindowAggr`s, we can cache complex expression by evaluating it with a
/// projection before the first WindowAggr.
///
/// This enables us to cache complex expression "c3+c4" for following plan:
///
/// ```text
/// WindowAggr: windowExpr=[[sum(c9) ORDER BY [c3 + c4] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
/// --WindowAggr: windowExpr=[[sum(c9) ORDER BY [c3 + c4] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
/// ```
///
/// where, it is referred once by each `WindowAggr` (total of 2) in the plan.
fn get_consecutive_window_exprs(
    window: Window,
) -> (Vec<Vec<Expr>>, Vec<DFSchemaRef>, LogicalPlan) {
    let mut window_expr_list = vec![];
    let mut window_schemas = vec![];
    let mut plan = LogicalPlan::Window(window);
    while let LogicalPlan::Window(Window {
        input,
        window_expr,
        schema,
    }) = plan
    {
        window_expr_list.push(window_expr);
        window_schemas.push(schema);

        plan = Arc::unwrap_or_clone(input);
    }
    (window_expr_list, window_schemas, plan)
}

impl OptimizerRule for CommonSubexprEliminate {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        // This rule handles recursion itself in a `ApplyOrder::TopDown` like manner.
        // This is because in some cases adjacent nodes are collected (e.g. `Window`) and
        // CSEd as a group, which can't be done in a simple `ApplyOrder::TopDown` rule.
        None
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let original_schema = Arc::clone(plan.schema());

        let optimized_plan = match plan {
            LogicalPlan::Projection(proj) => self.try_optimize_proj(proj, config)?,
            LogicalPlan::Sort(sort) => self.try_optimize_sort(sort, config)?,
            LogicalPlan::Filter(filter) => self.try_optimize_filter(filter, config)?,
            LogicalPlan::Window(window) => self.try_optimize_window(window, config)?,
            LogicalPlan::Aggregate(agg) => self.try_optimize_aggregate(agg, config)?,
            LogicalPlan::Join(_)
            | LogicalPlan::CrossJoin(_)
            | LogicalPlan::Repartition(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::TableScan(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Subquery(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::DescribeTable(_)
            | LogicalPlan::Distinct(_)
            | LogicalPlan::Extension(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::Unnest(_)
            | LogicalPlan::RecursiveQuery(_)
            | LogicalPlan::Prepare(_) => {
                // This rule handles recursion itself in a `ApplyOrder::TopDown` like
                // manner.
                plan.map_children(|c| self.rewrite(c, config))?
            }
        };

        // If we rewrote the plan, ensure the schema stays the same
        if optimized_plan.transformed && optimized_plan.data.schema() != &original_schema
        {
            optimized_plan.map_data(|optimized_plan| {
                build_recover_project_plan(&original_schema, optimized_plan)
            })
        } else {
            Ok(optimized_plan)
        }
    }

    fn name(&self) -> &str {
        "common_sub_expression_eliminate"
    }
}

impl Default for CommonSubexprEliminate {
    fn default() -> Self {
        Self::new()
    }
}

/// Build the "intermediate" projection plan that evaluates the extracted common
/// expressions.
///
/// # Arguments
/// input: the input plan
///
/// common_exprs: which common subexpressions were used (and thus are added to
/// intermediate projection)
///
/// expr_stats: the set of common subexpressions
fn build_common_expr_project_plan(
    input: LogicalPlan,
    common_exprs: Vec<(Expr, String)>,
) -> Result<LogicalPlan> {
    let mut fields_set = BTreeSet::new();
    let mut project_exprs = common_exprs
        .into_iter()
        .map(|(expr, expr_alias)| {
            fields_set.insert(expr_alias.clone());
            Ok(expr.alias(expr_alias))
        })
        .collect::<Result<Vec<_>>>()?;

    for (qualifier, field) in input.schema().iter() {
        if fields_set.insert(qualified_name(qualifier, field.name())) {
            project_exprs.push(Expr::from((qualifier, field)));
        }
    }

    Projection::try_new(project_exprs, Arc::new(input)).map(LogicalPlan::Projection)
}

/// Build the projection plan to eliminate unnecessary columns produced by
/// the "intermediate" projection plan built in [build_common_expr_project_plan].
///
/// This is required to keep the schema the same for plans that pass the input
/// on to the output, such as `Filter` or `Sort`.
fn build_recover_project_plan(
    schema: &DFSchema,
    input: LogicalPlan,
) -> Result<LogicalPlan> {
    let col_exprs = schema.iter().map(Expr::from).collect();
    Projection::try_new(col_exprs, Arc::new(input)).map(LogicalPlan::Projection)
}

fn extract_expressions(expr: &Expr, result: &mut Vec<Expr>) {
    if let Expr::GroupingSet(groupings) = expr {
        for e in groupings.distinct_expr() {
            let (qualifier, field_name) = e.qualified_name();
            let col = Column::new(qualifier, field_name);
            result.push(Expr::Column(col))
        }
    } else {
        let (qualifier, field_name) = expr.qualified_name();
        let col = Column::new(qualifier, field_name);
        result.push(Expr::Column(col));
    }
}

/// Which type of [expressions](Expr) should be considered for rewriting?
#[derive(Debug, Clone, Copy)]
enum ExprMask {
    /// Ignores:
    ///
    /// - [`Literal`](Expr::Literal)
    /// - [`Columns`](Expr::Column)
    /// - [`ScalarVariable`](Expr::ScalarVariable)
    /// - [`Alias`](Expr::Alias)
    /// - [`Wildcard`](Expr::Wildcard)
    /// - [`AggregateFunction`](Expr::AggregateFunction)
    Normal,

    /// Like [`Normal`](Self::Normal), but includes [`AggregateFunction`](Expr::AggregateFunction).
    NormalAndAggregates,
}

impl ExprMask {
    fn ignores(&self, expr: &Expr) -> bool {
        let is_normal_minus_aggregates = matches!(
            expr,
            Expr::Literal(..)
                | Expr::Column(..)
                | Expr::ScalarVariable(..)
                | Expr::Alias(..)
                | Expr::Wildcard { .. }
        );

        let is_aggr = matches!(expr, Expr::AggregateFunction(..));

        match self {
            Self::Normal => is_normal_minus_aggregates || is_aggr,
            Self::NormalAndAggregates => is_normal_minus_aggregates,
        }
    }
}

/// Go through an expression tree and generate identifiers for each subexpression.
///
/// An identifier contains information of the expression itself and its sub-expression.
/// This visitor implementation use a stack `visit_stack` to track traversal, which
/// lets us know when a sub-tree's visiting is finished. When `pre_visit` is called
/// (traversing to a new node), an `EnterMark` and an `ExprItem` will be pushed into stack.
/// And try to pop out a `EnterMark` on leaving a node (`f_up()`). All `ExprItem`
/// before the first `EnterMark` is considered to be sub-tree of the leaving node.
///
/// This visitor also records identifier in `id_array`. Makes the following traverse
/// pass can get the identifier of a node without recalculate it. We assign each node
/// in the expr tree a series number, start from 1, maintained by `series_number`.
/// Series number represents the order we left (`f_up()`) a node. Has the property
/// that child node's series number always smaller than parent's. While `id_array` is
/// organized in the order we enter (`f_down()`) a node. `node_count` helps us to
/// get the index of `id_array` for each node.
///
/// `Expr` without sub-expr (column, literal etc.) will not have identifier
/// because they should not be recognized as common sub-expr.
struct ExprIdentifierVisitor<'a, 'n> {
    // statistics of expressions
    expr_stats: &'a mut ExprStats<'n>,
    // cache to speed up second traversal
    id_array: &'a mut IdArray<'n>,
    // inner states
    visit_stack: Vec<VisitRecord<'n>>,
    // preorder index, start from 0.
    down_index: usize,
    // postorder index, start from 0.
    up_index: usize,
    // which expression should be skipped?
    expr_mask: ExprMask,
    // a `RandomState` to generate hashes during the first traversal
    random_state: &'a RandomState,
    // a flag to indicate that common expression found
    found_common: bool,
    // if we are in a conditional branch. A conditional branch means that the expression
    // might not be executed depending on the runtime values of other expressions, and
    // thus can not be extracted as a common expression.
    conditional: bool,
}

/// Record item that used when traversing an expression tree.
enum VisitRecord<'n> {
    /// Marks the beginning of expression. It contains:
    /// - The post-order index assigned during the first, visiting traversal.
    EnterMark(usize),

    /// Marks an accumulated subexpression tree. It contains:
    /// - The accumulated identifier of a subexpression.
    /// - A boolean flag if the expression is valid for subexpression elimination.
    ///   The flag is propagated up from children to parent. (E.g. volatile expressions
    ///   are not valid and can't be extracted, but non-volatile children of volatile
    ///   expressions can be extracted.)
    ExprItem(Identifier<'n>, bool),
}

impl<'n> ExprIdentifierVisitor<'_, 'n> {
    /// Find the first `EnterMark` in the stack, and accumulates every `ExprItem` before
    /// it. Returns a tuple that contains:
    /// - The pre-order index of the expression we marked.
    /// - The accumulated identifier of the children of the marked expression.
    /// - An accumulated boolean flag from the children of the marked expression if all
    ///   children are valid for subexpression elimination (i.e. it is safe to extract the
    ///   expression as a common expression from its children POV).
    ///   (E.g. if any of the children of the marked expression is not valid (e.g. is
    ///   volatile) then the expression is also not valid, so we can propagate this
    ///   information up from children to parents via `visit_stack` during the first,
    ///   visiting traversal and no need to test the expression's validity beforehand with
    ///   an extra traversal).
    fn pop_enter_mark(&mut self) -> (usize, Option<Identifier<'n>>, bool) {
        let mut expr_id = None;
        let mut is_valid = true;

        while let Some(item) = self.visit_stack.pop() {
            match item {
                VisitRecord::EnterMark(down_index) => {
                    return (down_index, expr_id, is_valid);
                }
                VisitRecord::ExprItem(sub_expr_id, sub_expr_is_valid) => {
                    expr_id = Some(sub_expr_id.combine(expr_id));
                    is_valid &= sub_expr_is_valid;
                }
            }
        }
        unreachable!("Enter mark should paired with node number");
    }

    /// Save the current `conditional` status and run `f` with `conditional` set to true.
    fn conditionally<F: FnMut(&mut Self) -> Result<()>>(
        &mut self,
        mut f: F,
    ) -> Result<()> {
        let conditional = self.conditional;
        self.conditional = true;
        f(self)?;
        self.conditional = conditional;

        Ok(())
    }
}

impl<'n> TreeNodeVisitor<'n> for ExprIdentifierVisitor<'_, 'n> {
    type Node = Expr;

    fn f_down(&mut self, expr: &'n Expr) -> Result<TreeNodeRecursion> {
        self.id_array.push((0, None));
        self.visit_stack
            .push(VisitRecord::EnterMark(self.down_index));
        self.down_index += 1;

        // If an expression can short-circuit then some of its children might not be
        // executed so count the occurrence of subexpressions as conditional in all
        // children.
        Ok(match expr {
            // If we are already in a conditionally evaluated subtree then continue
            // traversal.
            _ if self.conditional => TreeNodeRecursion::Continue,

            // In case of `ScalarFunction`s we don't know which children are surely
            // executed so start visiting all children conditionally and stop the
            // recursion with `TreeNodeRecursion::Jump`.
            Expr::ScalarFunction(ScalarFunction { func, args })
                if func.short_circuits() =>
            {
                self.conditionally(|visitor| {
                    args.iter().try_for_each(|e| e.visit(visitor).map(|_| ()))
                })?;

                TreeNodeRecursion::Jump
            }

            // In case of `And` and `Or` the first child is surely executed, but we
            // account subexpressions as conditional in the second.
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::And | Operator::Or,
                right,
            }) => {
                left.visit(self)?;
                self.conditionally(|visitor| right.visit(visitor).map(|_| ()))?;

                TreeNodeRecursion::Jump
            }

            // In case of `Case` the optional base expression and the first when
            // expressions are surely executed, but we account subexpressions as
            // conditional in the others.
            Expr::Case(Case {
                expr,
                when_then_expr,
                else_expr,
            }) => {
                expr.iter().try_for_each(|e| e.visit(self).map(|_| ()))?;
                when_then_expr.iter().take(1).try_for_each(|(when, then)| {
                    when.visit(self)?;
                    self.conditionally(|visitor| then.visit(visitor).map(|_| ()))
                })?;
                self.conditionally(|visitor| {
                    when_then_expr.iter().skip(1).try_for_each(|(when, then)| {
                        when.visit(visitor)?;
                        then.visit(visitor).map(|_| ())
                    })?;
                    else_expr
                        .iter()
                        .try_for_each(|e| e.visit(visitor).map(|_| ()))
                })?;

                TreeNodeRecursion::Jump
            }

            // In case of non-short-circuit expressions continue the traversal.
            _ => TreeNodeRecursion::Continue,
        })
    }

    fn f_up(&mut self, expr: &'n Expr) -> Result<TreeNodeRecursion> {
        let (down_index, sub_expr_id, sub_expr_is_valid) = self.pop_enter_mark();

        let expr_id = Identifier::new(expr, self.random_state).combine(sub_expr_id);
        let is_valid = !expr.is_volatile_node() && sub_expr_is_valid;

        self.id_array[down_index].0 = self.up_index;
        if is_valid && !self.expr_mask.ignores(expr) {
            self.id_array[down_index].1 = Some(expr_id);
            let (count, conditional_count) =
                self.expr_stats.entry(expr_id).or_insert((0, 0));
            if self.conditional {
                *conditional_count += 1;
            } else {
                *count += 1;
            }
            if *count > 1 || (*count == 1 && *conditional_count > 0) {
                self.found_common = true;
            }
        }
        self.visit_stack
            .push(VisitRecord::ExprItem(expr_id, is_valid));
        self.up_index += 1;

        Ok(TreeNodeRecursion::Continue)
    }
}

/// Rewrite expression by replacing detected common sub-expression with
/// the corresponding temporary column name. That column contains the
/// evaluate result of replaced expression.
struct CommonSubexprRewriter<'a, 'n> {
    // statistics of expressions
    expr_stats: &'a ExprStats<'n>,
    // cache to speed up second traversal
    id_array: &'a IdArray<'n>,
    // common expression, that are replaced during the second traversal, are collected to
    // this map
    common_exprs: &'a mut CommonExprs<'n>,
    // preorder index, starts from 0.
    down_index: usize,
    // how many aliases have we seen so far
    alias_counter: usize,
    // alias generator for extracted common expressions
    alias_generator: &'a AliasGenerator,
}

impl TreeNodeRewriter for CommonSubexprRewriter<'_, '_> {
    type Node = Expr;

    fn f_down(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        if matches!(expr, Expr::Alias(_)) {
            self.alias_counter += 1;
        }

        let (up_index, expr_id) = self.id_array[self.down_index];
        self.down_index += 1;

        // Handle `Expr`s with identifiers only
        if let Some(expr_id) = expr_id {
            let (count, conditional_count) = self.expr_stats.get(&expr_id).unwrap();
            if *count > 1 || *count == 1 && *conditional_count > 0 {
                // step index to skip all sub-node (which has smaller series number).
                while self.down_index < self.id_array.len()
                    && self.id_array[self.down_index].0 < up_index
                {
                    self.down_index += 1;
                }

                let expr_name = expr.schema_name().to_string();
                let (_, expr_alias) =
                    self.common_exprs.entry(expr_id).or_insert_with(|| {
                        let expr_alias = self.alias_generator.next(CSE_PREFIX);
                        (expr, expr_alias)
                    });

                // alias the expressions without an `Alias` ancestor node
                let rewritten = if self.alias_counter > 0 {
                    col(expr_alias.clone())
                } else {
                    self.alias_counter += 1;
                    col(expr_alias.clone()).alias(expr_name)
                };

                return Ok(Transformed::new(rewritten, true, TreeNodeRecursion::Jump));
            }
        }

        Ok(Transformed::no(expr))
    }

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Self::Node>> {
        if matches!(expr, Expr::Alias(_)) {
            self.alias_counter -= 1
        }

        Ok(Transformed::no(expr))
    }
}

/// Replace common sub-expression in `expr` with the corresponding temporary
/// column name, updating `common_exprs` with any replaced expressions
fn replace_common_expr<'n>(
    expr: Expr,
    id_array: &IdArray<'n>,
    expr_stats: &ExprStats<'n>,
    common_exprs: &mut CommonExprs<'n>,
    alias_generator: &AliasGenerator,
) -> Result<Expr> {
    if id_array.is_empty() {
        Ok(Transformed::no(expr))
    } else {
        expr.rewrite(&mut CommonSubexprRewriter {
            expr_stats,
            id_array,
            common_exprs,
            down_index: 0,
            alias_counter: 0,
            alias_generator,
        })
    }
    .data()
}

#[cfg(test)]
mod test {
    use std::any::Any;
    use std::collections::HashSet;
    use std::iter;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_expr::expr::AggregateFunction;
    use datafusion_expr::logical_plan::{table_scan, JoinType};
    use datafusion_expr::{
        grouping_set, AccumulatorFactoryFunction, AggregateUDF, BinaryExpr,
        ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, SimpleAggregateUDF,
        Volatility,
    };
    use datafusion_expr::{lit, logical_plan::builder::LogicalPlanBuilder};

    use super::*;
    use crate::optimizer::OptimizerContext;
    use crate::test::*;
    use crate::Optimizer;
    use datafusion_expr::test::function_stub::{avg, sum};

    fn assert_optimized_plan_eq(
        expected: &str,
        plan: LogicalPlan,
        config: Option<&dyn OptimizerConfig>,
    ) {
        let optimizer =
            Optimizer::with_rules(vec![Arc::new(CommonSubexprEliminate::new())]);
        let default_config = OptimizerContext::new();
        let config = config.unwrap_or(&default_config);
        let optimized_plan = optimizer.optimize(plan, config, |_, _| ()).unwrap();
        let formatted_plan = format!("{optimized_plan}");
        assert_eq!(expected, formatted_plan);
    }

    #[test]
    fn id_array_visitor() -> Result<()> {
        let optimizer = CommonSubexprEliminate::new();

        let a_plus_1 = col("a") + lit(1);
        let avg_c = avg(col("c"));
        let sum_a_plus_1 = sum(a_plus_1);
        let sum_a_plus_1_minus_avg_c = sum_a_plus_1 - avg_c;
        let expr = sum_a_plus_1_minus_avg_c * lit(2);

        let Expr::BinaryExpr(BinaryExpr {
            left: sum_a_plus_1_minus_avg_c,
            ..
        }) = &expr
        else {
            panic!("Cannot extract subexpression reference")
        };
        let Expr::BinaryExpr(BinaryExpr {
            left: sum_a_plus_1,
            right: avg_c,
            ..
        }) = sum_a_plus_1_minus_avg_c.as_ref()
        else {
            panic!("Cannot extract subexpression reference")
        };
        let Expr::AggregateFunction(AggregateFunction {
            args: a_plus_1_vec, ..
        }) = sum_a_plus_1.as_ref()
        else {
            panic!("Cannot extract subexpression reference")
        };
        let a_plus_1 = &a_plus_1_vec.as_slice()[0];

        // skip aggregates
        let mut id_array = vec![];
        optimizer.expr_to_identifier(
            &expr,
            &mut ExprStats::new(),
            &mut id_array,
            ExprMask::Normal,
        )?;

        // Collect distinct hashes and set them to 0 in `id_array`
        fn collect_hashes(id_array: &mut IdArray) -> HashSet<u64> {
            id_array
                .iter_mut()
                .flat_map(|(_, expr_id_option)| {
                    expr_id_option.as_mut().map(|expr_id| {
                        let hash = expr_id.hash;
                        expr_id.hash = 0;
                        hash
                    })
                })
                .collect::<HashSet<_>>()
        }

        let hashes = collect_hashes(&mut id_array);
        assert_eq!(hashes.len(), 3);

        let expected = vec![
            (
                8,
                Some(Identifier {
                    hash: 0,
                    expr: &expr,
                }),
            ),
            (
                6,
                Some(Identifier {
                    hash: 0,
                    expr: sum_a_plus_1_minus_avg_c,
                }),
            ),
            (3, None),
            (
                2,
                Some(Identifier {
                    hash: 0,
                    expr: a_plus_1,
                }),
            ),
            (0, None),
            (1, None),
            (5, None),
            (4, None),
            (7, None),
        ];
        assert_eq!(expected, id_array);

        // include aggregates
        let mut id_array = vec![];
        optimizer.expr_to_identifier(
            &expr,
            &mut ExprStats::new(),
            &mut id_array,
            ExprMask::NormalAndAggregates,
        )?;

        let hashes = collect_hashes(&mut id_array);
        assert_eq!(hashes.len(), 5);

        let expected = vec![
            (
                8,
                Some(Identifier {
                    hash: 0,
                    expr: &expr,
                }),
            ),
            (
                6,
                Some(Identifier {
                    hash: 0,
                    expr: sum_a_plus_1_minus_avg_c,
                }),
            ),
            (
                3,
                Some(Identifier {
                    hash: 0,
                    expr: sum_a_plus_1,
                }),
            ),
            (
                2,
                Some(Identifier {
                    hash: 0,
                    expr: a_plus_1,
                }),
            ),
            (0, None),
            (1, None),
            (
                5,
                Some(Identifier {
                    hash: 0,
                    expr: avg_c,
                }),
            ),
            (4, None),
            (7, None),
        ];
        assert_eq!(expected, id_array);

        Ok(())
    }

    #[test]
    fn tpch_q1_simplified() -> Result<()> {
        // SQL:
        //  select
        //      sum(a * (1 - b)),
        //      sum(a * (1 - b) * (1 + c))
        //  from T;
        //
        // The manual assembled logical plan don't contains the outermost `Projection`.

        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                iter::empty::<Expr>(),
                vec![
                    sum(col("a") * (lit(1) - col("b"))),
                    sum((col("a") * (lit(1) - col("b"))) * (lit(1) + col("c"))),
                ],
            )?
            .build()?;

        let expected = "Aggregate: groupBy=[[]], aggr=[[sum(__common_expr_1 AS test.a * Int32(1) - test.b), sum(__common_expr_1 AS test.a * Int32(1) - test.b * (Int32(1) + test.c))]]\
        \n  Projection: test.a * (Int32(1) - test.b) AS __common_expr_1, test.a, test.b, test.c\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, plan, None);

        Ok(())
    }

    #[test]
    fn nested_aliases() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                (col("a") + col("b") - col("c")).alias("alias1") * (col("a") + col("b")),
                col("a") + col("b"),
            ])?
            .build()?;

        let expected = "Projection: __common_expr_1 - test.c AS alias1 * __common_expr_1 AS test.a + test.b, __common_expr_1 AS test.a + test.b\
        \n  Projection: test.a + test.b AS __common_expr_1, test.a, test.b, test.c\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, plan, None);

        Ok(())
    }

    #[test]
    fn aggregate() -> Result<()> {
        let table_scan = test_table_scan()?;

        let return_type = DataType::UInt32;
        let accumulator: AccumulatorFactoryFunction = Arc::new(|_| unimplemented!());
        let udf_agg = |inner: Expr| {
            Expr::AggregateFunction(datafusion_expr::expr::AggregateFunction::new_udf(
                Arc::new(AggregateUDF::from(SimpleAggregateUDF::new_with_signature(
                    "my_agg",
                    Signature::exact(vec![DataType::UInt32], Volatility::Stable),
                    return_type.clone(),
                    Arc::clone(&accumulator),
                    vec![Field::new("value", DataType::UInt32, true)],
                ))),
                vec![inner],
                false,
                None,
                None,
                None,
            ))
        };

        // test: common aggregates
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .aggregate(
                iter::empty::<Expr>(),
                vec![
                    // common: avg(col("a"))
                    avg(col("a")).alias("col1"),
                    avg(col("a")).alias("col2"),
                    // no common
                    avg(col("b")).alias("col3"),
                    avg(col("c")),
                    // common: udf_agg(col("a"))
                    udf_agg(col("a")).alias("col4"),
                    udf_agg(col("a")).alias("col5"),
                    // no common
                    udf_agg(col("b")).alias("col6"),
                    udf_agg(col("c")),
                ],
            )?
            .build()?;

        let expected = "Projection: __common_expr_1 AS col1, __common_expr_1 AS col2, col3, __common_expr_3 AS avg(test.c), __common_expr_2 AS col4, __common_expr_2 AS col5, col6, __common_expr_4 AS my_agg(test.c)\
        \n  Aggregate: groupBy=[[]], aggr=[[avg(test.a) AS __common_expr_1, my_agg(test.a) AS __common_expr_2, avg(test.b) AS col3, avg(test.c) AS __common_expr_3, my_agg(test.b) AS col6, my_agg(test.c) AS __common_expr_4]]\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, plan, None);

        // test: trafo after aggregate
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .aggregate(
                iter::empty::<Expr>(),
                vec![
                    lit(1) + avg(col("a")),
                    lit(1) - avg(col("a")),
                    lit(1) + udf_agg(col("a")),
                    lit(1) - udf_agg(col("a")),
                ],
            )?
            .build()?;

        let expected = "Projection: Int32(1) + __common_expr_1 AS avg(test.a), Int32(1) - __common_expr_1 AS avg(test.a), Int32(1) + __common_expr_2 AS my_agg(test.a), Int32(1) - __common_expr_2 AS my_agg(test.a)\
        \n  Aggregate: groupBy=[[]], aggr=[[avg(test.a) AS __common_expr_1, my_agg(test.a) AS __common_expr_2]]\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, plan, None);

        // test: transformation before aggregate
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .aggregate(
                iter::empty::<Expr>(),
                vec![
                    avg(lit(1u32) + col("a")).alias("col1"),
                    udf_agg(lit(1u32) + col("a")).alias("col2"),
                ],
            )?
            .build()?;

        let expected ="Aggregate: groupBy=[[]], aggr=[[avg(__common_expr_1) AS col1, my_agg(__common_expr_1) AS col2]]\
        \n  Projection: UInt32(1) + test.a AS __common_expr_1, test.a, test.b, test.c\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, plan, None);

        // test: common between agg and group
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .aggregate(
                vec![lit(1u32) + col("a")],
                vec![
                    avg(lit(1u32) + col("a")).alias("col1"),
                    udf_agg(lit(1u32) + col("a")).alias("col2"),
                ],
            )?
            .build()?;

        let expected = "Aggregate: groupBy=[[__common_expr_1 AS UInt32(1) + test.a]], aggr=[[avg(__common_expr_1) AS col1, my_agg(__common_expr_1) AS col2]]\
        \n  Projection: UInt32(1) + test.a AS __common_expr_1, test.a, test.b, test.c\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, plan, None);

        // test: all mixed
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![lit(1u32) + col("a")],
                vec![
                    (lit(1u32) + avg(lit(1u32) + col("a"))).alias("col1"),
                    (lit(1u32) - avg(lit(1u32) + col("a"))).alias("col2"),
                    avg(lit(1u32) + col("a")),
                    (lit(1u32) + udf_agg(lit(1u32) + col("a"))).alias("col3"),
                    (lit(1u32) - udf_agg(lit(1u32) + col("a"))).alias("col4"),
                    udf_agg(lit(1u32) + col("a")),
                ],
            )?
            .build()?;

        let expected = "Projection: UInt32(1) + test.a, UInt32(1) + __common_expr_2 AS col1, UInt32(1) - __common_expr_2 AS col2, __common_expr_4 AS avg(UInt32(1) + test.a), UInt32(1) + __common_expr_3 AS col3, UInt32(1) - __common_expr_3 AS col4, __common_expr_5 AS my_agg(UInt32(1) + test.a)\
        \n  Aggregate: groupBy=[[__common_expr_1 AS UInt32(1) + test.a]], aggr=[[avg(__common_expr_1) AS __common_expr_2, my_agg(__common_expr_1) AS __common_expr_3, avg(__common_expr_1 AS UInt32(1) + test.a) AS __common_expr_4, my_agg(__common_expr_1 AS UInt32(1) + test.a) AS __common_expr_5]]\
        \n    Projection: UInt32(1) + test.a AS __common_expr_1, test.a, test.b, test.c\
        \n      TableScan: test";

        assert_optimized_plan_eq(expected, plan, None);

        Ok(())
    }

    #[test]
    fn aggregate_with_relations_and_dots() -> Result<()> {
        let schema = Schema::new(vec![Field::new("col.a", DataType::UInt32, false)]);
        let table_scan = table_scan(Some("table.test"), &schema, None)?.build()?;

        let col_a = Expr::Column(Column::new(Some("table.test"), "col.a"));

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col_a.clone()],
                vec![
                    (lit(1u32) + avg(lit(1u32) + col_a.clone())),
                    avg(lit(1u32) + col_a),
                ],
            )?
            .build()?;

        let expected = "Projection: table.test.col.a, UInt32(1) + __common_expr_2 AS avg(UInt32(1) + table.test.col.a), __common_expr_2 AS avg(UInt32(1) + table.test.col.a)\
        \n  Aggregate: groupBy=[[table.test.col.a]], aggr=[[avg(__common_expr_1 AS UInt32(1) + table.test.col.a) AS __common_expr_2]]\
        \n    Projection: UInt32(1) + table.test.col.a AS __common_expr_1, table.test.col.a\
        \n      TableScan: table.test";

        assert_optimized_plan_eq(expected, plan, None);

        Ok(())
    }

    #[test]
    fn subexpr_in_same_order() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                (lit(1) + col("a")).alias("first"),
                (lit(1) + col("a")).alias("second"),
            ])?
            .build()?;

        let expected = "Projection: __common_expr_1 AS first, __common_expr_1 AS second\
        \n  Projection: Int32(1) + test.a AS __common_expr_1, test.a, test.b, test.c\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, plan, None);

        Ok(())
    }

    #[test]
    fn subexpr_in_different_order() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![lit(1) + col("a"), col("a") + lit(1)])?
            .build()?;

        let expected = "Projection: Int32(1) + test.a, test.a + Int32(1)\
        \n  TableScan: test";

        assert_optimized_plan_eq(expected, plan, None);

        Ok(())
    }

    #[test]
    fn cross_plans_subexpr() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![lit(1) + col("a"), col("a")])?
            .project(vec![lit(1) + col("a")])?
            .build()?;

        let expected = "Projection: Int32(1) + test.a\
        \n  Projection: Int32(1) + test.a, test.a\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, plan, None);
        Ok(())
    }

    #[test]
    fn redundant_project_fields() {
        let table_scan = test_table_scan().unwrap();
        let c_plus_a = col("c") + col("a");
        let b_plus_a = col("b") + col("a");
        let common_exprs_1 = vec![
            (c_plus_a, format!("{CSE_PREFIX}_1")),
            (b_plus_a, format!("{CSE_PREFIX}_2")),
        ];
        let c_plus_a_2 = col(format!("{CSE_PREFIX}_1"));
        let b_plus_a_2 = col(format!("{CSE_PREFIX}_2"));
        let common_exprs_2 = vec![
            (c_plus_a_2, format!("{CSE_PREFIX}_3")),
            (b_plus_a_2, format!("{CSE_PREFIX}_4")),
        ];
        let project = build_common_expr_project_plan(table_scan, common_exprs_1).unwrap();
        let project_2 = build_common_expr_project_plan(project, common_exprs_2).unwrap();

        let mut field_set = BTreeSet::new();
        for name in project_2.schema().field_names() {
            assert!(field_set.insert(name));
        }
    }

    #[test]
    fn redundant_project_fields_join_input() {
        let table_scan_1 = test_table_scan_with_name("test1").unwrap();
        let table_scan_2 = test_table_scan_with_name("test2").unwrap();
        let join = LogicalPlanBuilder::from(table_scan_1)
            .join(table_scan_2, JoinType::Inner, (vec!["a"], vec!["a"]), None)
            .unwrap()
            .build()
            .unwrap();
        let c_plus_a = col("test1.c") + col("test1.a");
        let b_plus_a = col("test1.b") + col("test1.a");
        let common_exprs_1 = vec![
            (c_plus_a, format!("{CSE_PREFIX}_1")),
            (b_plus_a, format!("{CSE_PREFIX}_2")),
        ];
        let c_plus_a_2 = col(format!("{CSE_PREFIX}_1"));
        let b_plus_a_2 = col(format!("{CSE_PREFIX}_2"));
        let common_exprs_2 = vec![
            (c_plus_a_2, format!("{CSE_PREFIX}_3")),
            (b_plus_a_2, format!("{CSE_PREFIX}_4")),
        ];
        let project = build_common_expr_project_plan(join, common_exprs_1).unwrap();
        let project_2 = build_common_expr_project_plan(project, common_exprs_2).unwrap();

        let mut field_set = BTreeSet::new();
        for name in project_2.schema().field_names() {
            assert!(field_set.insert(name));
        }
    }

    #[test]
    fn eliminated_subexpr_datatype() {
        use datafusion_expr::cast;

        let schema = Schema::new(vec![
            Field::new("a", DataType::UInt64, false),
            Field::new("b", DataType::UInt64, false),
            Field::new("c", DataType::UInt64, false),
        ]);

        let plan = table_scan(Some("table"), &schema, None)
            .unwrap()
            .filter(
                cast(col("a"), DataType::Int64)
                    .lt(lit(1_i64))
                    .and(cast(col("a"), DataType::Int64).not_eq(lit(1_i64))),
            )
            .unwrap()
            .build()
            .unwrap();
        let rule = CommonSubexprEliminate::new();
        let optimized_plan = rule.rewrite(plan, &OptimizerContext::new()).unwrap();
        assert!(optimized_plan.transformed);
        let optimized_plan = optimized_plan.data;

        let schema = optimized_plan.schema();
        let fields_with_datatypes: Vec<_> = schema
            .fields()
            .iter()
            .map(|field| (field.name(), field.data_type()))
            .collect();
        let formatted_fields_with_datatype = format!("{fields_with_datatypes:#?}");
        let expected = r#"[
    (
        "a",
        UInt64,
    ),
    (
        "b",
        UInt64,
    ),
    (
        "c",
        UInt64,
    ),
]"#;
        assert_eq!(expected, formatted_fields_with_datatype);
    }

    #[test]
    fn filter_schema_changed() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter((lit(1) + col("a") - lit(10)).gt(lit(1) + col("a")))?
            .build()?;

        let expected = "Projection: test.a, test.b, test.c\
        \n  Filter: __common_expr_1 - Int32(10) > __common_expr_1\
        \n    Projection: Int32(1) + test.a AS __common_expr_1, test.a, test.b, test.c\
        \n      TableScan: test";

        assert_optimized_plan_eq(expected, plan, None);

        Ok(())
    }

    #[test]
    fn test_extract_expressions_from_grouping_set() -> Result<()> {
        let mut result = Vec::with_capacity(3);
        let grouping = grouping_set(vec![vec![col("a"), col("b")], vec![col("c")]]);
        extract_expressions(&grouping, &mut result);

        assert!(result.len() == 3);
        Ok(())
    }

    #[test]
    fn test_extract_expressions_from_grouping_set_with_identical_expr() -> Result<()> {
        let mut result = Vec::with_capacity(2);
        let grouping = grouping_set(vec![vec![col("a"), col("b")], vec![col("a")]]);
        extract_expressions(&grouping, &mut result);
        assert!(result.len() == 2);
        Ok(())
    }

    #[test]
    fn test_alias_collision() -> Result<()> {
        let table_scan = test_table_scan()?;

        let config = &OptimizerContext::new();
        let common_expr_1 = config.alias_generator().next(CSE_PREFIX);
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .project(vec![
                (col("a") + col("b")).alias(common_expr_1.clone()),
                col("c"),
            ])?
            .project(vec![
                col(common_expr_1.clone()).alias("c1"),
                col(common_expr_1).alias("c2"),
                (col("c") + lit(2)).alias("c3"),
                (col("c") + lit(2)).alias("c4"),
            ])?
            .build()?;

        let expected = "Projection: __common_expr_1 AS c1, __common_expr_1 AS c2, __common_expr_2 AS c3, __common_expr_2 AS c4\
        \n  Projection: test.c + Int32(2) AS __common_expr_2, __common_expr_1, test.c\
        \n    Projection: test.a + test.b AS __common_expr_1, test.c\
        \n      TableScan: test";

        assert_optimized_plan_eq(expected, plan, Some(config));

        let config = &OptimizerContext::new();
        let _common_expr_1 = config.alias_generator().next(CSE_PREFIX);
        let common_expr_2 = config.alias_generator().next(CSE_PREFIX);
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                (col("a") + col("b")).alias(common_expr_2.clone()),
                col("c"),
            ])?
            .project(vec![
                col(common_expr_2.clone()).alias("c1"),
                col(common_expr_2).alias("c2"),
                (col("c") + lit(2)).alias("c3"),
                (col("c") + lit(2)).alias("c4"),
            ])?
            .build()?;

        let expected = "Projection: __common_expr_2 AS c1, __common_expr_2 AS c2, __common_expr_3 AS c3, __common_expr_3 AS c4\
        \n  Projection: test.c + Int32(2) AS __common_expr_3, __common_expr_2, test.c\
        \n    Projection: test.a + test.b AS __common_expr_2, test.c\
        \n      TableScan: test";

        assert_optimized_plan_eq(expected, plan, Some(config));

        Ok(())
    }

    #[test]
    fn test_extract_expressions_from_col() -> Result<()> {
        let mut result = Vec::with_capacity(1);
        extract_expressions(&col("a"), &mut result);
        assert!(result.len() == 1);
        Ok(())
    }

    #[test]
    fn test_short_circuits() -> Result<()> {
        let table_scan = test_table_scan()?;

        let extracted_short_circuit = col("a").eq(lit(0)).or(col("b").eq(lit(0)));
        let extracted_short_circuit_leg_1 = (col("a") + col("b")).eq(lit(0));
        let not_extracted_short_circuit_leg_2 = (col("a") - col("b")).eq(lit(0));
        let extracted_short_circuit_leg_3 = (col("a") * col("b")).eq(lit(0));
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                extracted_short_circuit.clone().alias("c1"),
                extracted_short_circuit.alias("c2"),
                extracted_short_circuit_leg_1
                    .clone()
                    .or(not_extracted_short_circuit_leg_2.clone())
                    .alias("c3"),
                extracted_short_circuit_leg_1
                    .and(not_extracted_short_circuit_leg_2)
                    .alias("c4"),
                extracted_short_circuit_leg_3
                    .clone()
                    .or(extracted_short_circuit_leg_3)
                    .alias("c5"),
            ])?
            .build()?;

        let expected = "Projection: __common_expr_1 AS c1, __common_expr_1 AS c2, __common_expr_2 OR test.a - test.b = Int32(0) AS c3, __common_expr_2 AND test.a - test.b = Int32(0) AS c4, __common_expr_3 OR __common_expr_3 AS c5\
        \n  Projection: test.a = Int32(0) OR test.b = Int32(0) AS __common_expr_1, test.a + test.b = Int32(0) AS __common_expr_2, test.a * test.b = Int32(0) AS __common_expr_3, test.a, test.b, test.c\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, plan, None);

        Ok(())
    }

    #[test]
    fn test_volatile() -> Result<()> {
        let table_scan = test_table_scan()?;

        let extracted_child = col("a") + col("b");
        let rand = rand_func().call(vec![]);
        let not_extracted_volatile = extracted_child + rand;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                not_extracted_volatile.clone().alias("c1"),
                not_extracted_volatile.alias("c2"),
            ])?
            .build()?;

        let expected = "Projection: __common_expr_1 + random() AS c1, __common_expr_1 + random() AS c2\
        \n  Projection: test.a + test.b AS __common_expr_1, test.a, test.b, test.c\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, plan, None);

        Ok(())
    }

    #[test]
    fn test_volatile_short_circuits() -> Result<()> {
        let table_scan = test_table_scan()?;

        let rand = rand_func().call(vec![]);
        let extracted_short_circuit_leg_1 = col("a").eq(lit(0));
        let not_extracted_volatile_short_circuit_1 =
            extracted_short_circuit_leg_1.or(rand.clone().eq(lit(0)));
        let not_extracted_short_circuit_leg_2 = col("b").eq(lit(0));
        let not_extracted_volatile_short_circuit_2 =
            rand.eq(lit(0)).or(not_extracted_short_circuit_leg_2);
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                not_extracted_volatile_short_circuit_1.clone().alias("c1"),
                not_extracted_volatile_short_circuit_1.alias("c2"),
                not_extracted_volatile_short_circuit_2.clone().alias("c3"),
                not_extracted_volatile_short_circuit_2.alias("c4"),
            ])?
            .build()?;

        let expected = "Projection: __common_expr_1 OR random() = Int32(0) AS c1, __common_expr_1 OR random() = Int32(0) AS c2, random() = Int32(0) OR test.b = Int32(0) AS c3, random() = Int32(0) OR test.b = Int32(0) AS c4\
        \n  Projection: test.a = Int32(0) AS __common_expr_1, test.a, test.b, test.c\
        \n    TableScan: test";

        assert_optimized_plan_eq(expected, plan, None);

        Ok(())
    }

    #[test]
    fn test_non_top_level_common_expression() -> Result<()> {
        let table_scan = test_table_scan()?;

        let common_expr = col("a") + col("b");
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                common_expr.clone().alias("c1"),
                common_expr.alias("c2"),
            ])?
            .project(vec![col("c1"), col("c2")])?
            .build()?;

        let expected = "Projection: c1, c2\
        \n  Projection: __common_expr_1 AS c1, __common_expr_1 AS c2\
        \n    Projection: test.a + test.b AS __common_expr_1, test.a, test.b, test.c\
        \n      TableScan: test";

        assert_optimized_plan_eq(expected, plan, None);

        Ok(())
    }

    #[test]
    fn test_nested_common_expression() -> Result<()> {
        let table_scan = test_table_scan()?;

        let nested_common_expr = col("a") + col("b");
        let common_expr = nested_common_expr.clone() * nested_common_expr;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                common_expr.clone().alias("c1"),
                common_expr.alias("c2"),
            ])?
            .build()?;

        let expected = "Projection: __common_expr_1 AS c1, __common_expr_1 AS c2\
        \n  Projection: __common_expr_2 * __common_expr_2 AS __common_expr_1, test.a, test.b, test.c\
        \n    Projection: test.a + test.b AS __common_expr_2, test.a, test.b, test.c\
        \n      TableScan: test";

        assert_optimized_plan_eq(expected, plan, None);

        Ok(())
    }

    /// returns a "random" function that is marked volatile (aka each invocation
    /// returns a different value)
    ///
    /// Does not use datafusion_functions::rand to avoid introducing a
    /// dependency on that crate.
    fn rand_func() -> ScalarUDF {
        ScalarUDF::new_from_impl(RandomStub::new())
    }

    #[derive(Debug)]
    struct RandomStub {
        signature: Signature,
    }

    impl RandomStub {
        fn new() -> Self {
            Self {
                signature: Signature::exact(vec![], Volatility::Volatile),
            }
        }
    }
    impl ScalarUDFImpl for RandomStub {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn name(&self) -> &str {
            "random"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Float64)
        }

        fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
            unimplemented!()
        }
    }
}
