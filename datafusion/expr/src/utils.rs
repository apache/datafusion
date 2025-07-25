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

//! Expression utilities

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashSet};
use std::hash::Hasher;
use std::sync::Arc;

use crate::expr::{Alias, Sort, WildcardOptions, WindowFunctionParams};
use crate::expr_rewriter::strip_outer_reference;
use crate::{
    and, BinaryExpr, Expr, ExprSchemable, Filter, GroupingSet, LogicalPlan, Operator,
};
use datafusion_expr_common::signature::{Signature, TypeSignature};

use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion_common::utils::get_at_indices;
use datafusion_common::{
    internal_err, plan_datafusion_err, plan_err, Column, DFSchema, DFSchemaRef, HashMap,
    Result, TableReference,
};

use indexmap::IndexSet;
use sqlparser::ast::{ExceptSelectItem, ExcludeSelectItem};

pub use datafusion_functions_aggregate_common::order::AggregateOrderSensitivity;

///  The value to which `COUNT(*)` is expanded to in
///  `COUNT(<constant>)` expressions
pub use datafusion_common::utils::expr::COUNT_STAR_EXPANSION;

/// Count the number of distinct exprs in a list of group by expressions. If the
/// first element is a `GroupingSet` expression then it must be the only expr.
pub fn grouping_set_expr_count(group_expr: &[Expr]) -> Result<usize> {
    if let Some(Expr::GroupingSet(grouping_set)) = group_expr.first() {
        if group_expr.len() > 1 {
            return plan_err!(
                "Invalid group by expressions, GroupingSet must be the only expression"
            );
        }
        // Groupings sets have an additional integral column for the grouping id
        Ok(grouping_set.distinct_expr().len() + 1)
    } else {
        grouping_set_to_exprlist(group_expr).map(|exprs| exprs.len())
    }
}

/// The [power set] (or powerset) of a set S is the set of all subsets of S, \
/// including the empty set and S itself.
///
/// Example:
///
/// If S is the set {x, y, z}, then all the subsets of S are \
///  {} \
///  {x} \
///  {y} \
///  {z} \
///  {x, y} \
///  {x, z} \
///  {y, z} \
///  {x, y, z} \
///  and hence the power set of S is {{}, {x}, {y}, {z}, {x, y}, {x, z}, {y, z}, {x, y, z}}.
///
/// [power set]: https://en.wikipedia.org/wiki/Power_set
fn powerset<T>(slice: &[T]) -> Result<Vec<Vec<&T>>, String> {
    if slice.len() >= 64 {
        return Err("The size of the set must be less than 64.".into());
    }

    let mut v = Vec::new();
    for mask in 0..(1 << slice.len()) {
        let mut ss = vec![];
        let mut bitset = mask;
        while bitset > 0 {
            let rightmost: u64 = bitset & !(bitset - 1);
            let idx = rightmost.trailing_zeros();
            let item = slice.get(idx as usize).unwrap();
            ss.push(item);
            // zero the trailing bit
            bitset &= bitset - 1;
        }
        v.push(ss);
    }
    Ok(v)
}

/// check the number of expressions contained in the grouping_set
fn check_grouping_set_size_limit(size: usize) -> Result<()> {
    let max_grouping_set_size = 65535;
    if size > max_grouping_set_size {
        return plan_err!("The number of group_expression in grouping_set exceeds the maximum limit {max_grouping_set_size}, found {size}");
    }

    Ok(())
}

/// check the number of grouping_set contained in the grouping sets
fn check_grouping_sets_size_limit(size: usize) -> Result<()> {
    let max_grouping_sets_size = 4096;
    if size > max_grouping_sets_size {
        return plan_err!("The number of grouping_set in grouping_sets exceeds the maximum limit {max_grouping_sets_size}, found {size}");
    }

    Ok(())
}

/// Merge two grouping_set
///
/// # Example
/// ```text
/// (A, B), (C, D) -> (A, B, C, D)
/// ```
///
/// # Error
/// - [`DataFusionError`]: The number of group_expression in grouping_set exceeds the maximum limit
///
/// [`DataFusionError`]: datafusion_common::DataFusionError
fn merge_grouping_set<T: Clone>(left: &[T], right: &[T]) -> Result<Vec<T>> {
    check_grouping_set_size_limit(left.len() + right.len())?;
    Ok(left.iter().chain(right.iter()).cloned().collect())
}

/// Compute the cross product of two grouping_sets
///
/// # Example
/// ```text
/// [(A, B), (C, D)], [(E), (F)] -> [(A, B, E), (A, B, F), (C, D, E), (C, D, F)]
/// ```
///
/// # Error
/// - [`DataFusionError`]: The number of group_expression in grouping_set exceeds the maximum limit
/// - [`DataFusionError`]: The number of grouping_set in grouping_sets exceeds the maximum limit
///
/// [`DataFusionError`]: datafusion_common::DataFusionError
fn cross_join_grouping_sets<T: Clone>(
    left: &[Vec<T>],
    right: &[Vec<T>],
) -> Result<Vec<Vec<T>>> {
    let grouping_sets_size = left.len() * right.len();

    check_grouping_sets_size_limit(grouping_sets_size)?;

    let mut result = Vec::with_capacity(grouping_sets_size);
    for le in left {
        for re in right {
            result.push(merge_grouping_set(le, re)?);
        }
    }
    Ok(result)
}

/// Convert multiple grouping expressions into one [`GroupingSet::GroupingSets`],\
/// if the grouping expression does not contain [`Expr::GroupingSet`] or only has one expression,\
/// no conversion will be performed.
///
/// e.g.
///
/// person.id,\
/// GROUPING SETS ((person.age, person.salary),(person.age)),\
/// ROLLUP(person.state, person.birth_date)
///
/// =>
///
/// GROUPING SETS (\
///   (person.id, person.age, person.salary),\
///   (person.id, person.age, person.salary, person.state),\
///   (person.id, person.age, person.salary, person.state, person.birth_date),\
///   (person.id, person.age),\
///   (person.id, person.age, person.state),\
///   (person.id, person.age, person.state, person.birth_date)\
/// )
pub fn enumerate_grouping_sets(group_expr: Vec<Expr>) -> Result<Vec<Expr>> {
    let has_grouping_set = group_expr
        .iter()
        .any(|expr| matches!(expr, Expr::GroupingSet(_)));
    if !has_grouping_set || group_expr.len() == 1 {
        return Ok(group_expr);
    }
    // Only process mix grouping sets
    let partial_sets = group_expr
        .iter()
        .map(|expr| {
            let exprs = match expr {
                Expr::GroupingSet(GroupingSet::GroupingSets(grouping_sets)) => {
                    check_grouping_sets_size_limit(grouping_sets.len())?;
                    grouping_sets.iter().map(|e| e.iter().collect()).collect()
                }
                Expr::GroupingSet(GroupingSet::Cube(group_exprs)) => {
                    let grouping_sets = powerset(group_exprs)
                        .map_err(|e| plan_datafusion_err!("{}", e))?;
                    check_grouping_sets_size_limit(grouping_sets.len())?;
                    grouping_sets
                }
                Expr::GroupingSet(GroupingSet::Rollup(group_exprs)) => {
                    let size = group_exprs.len();
                    let slice = group_exprs.as_slice();
                    check_grouping_sets_size_limit(size * (size + 1) / 2 + 1)?;
                    (0..(size + 1))
                        .map(|i| slice[0..i].iter().collect())
                        .collect()
                }
                expr => vec![vec![expr]],
            };
            Ok(exprs)
        })
        .collect::<Result<Vec<_>>>()?;

    // Cross Join
    let grouping_sets = partial_sets
        .into_iter()
        .map(Ok)
        .reduce(|l, r| cross_join_grouping_sets(&l?, &r?))
        .transpose()?
        .map(|e| {
            e.into_iter()
                .map(|e| e.into_iter().cloned().collect())
                .collect()
        })
        .unwrap_or_default();

    Ok(vec![Expr::GroupingSet(GroupingSet::GroupingSets(
        grouping_sets,
    ))])
}

/// Find all distinct exprs in a list of group by expressions. If the
/// first element is a `GroupingSet` expression then it must be the only expr.
pub fn grouping_set_to_exprlist(group_expr: &[Expr]) -> Result<Vec<&Expr>> {
    if let Some(Expr::GroupingSet(grouping_set)) = group_expr.first() {
        if group_expr.len() > 1 {
            return plan_err!(
                "Invalid group by expressions, GroupingSet must be the only expression"
            );
        }
        Ok(grouping_set.distinct_expr())
    } else {
        Ok(group_expr
            .iter()
            .collect::<IndexSet<_>>()
            .into_iter()
            .collect())
    }
}

/// Recursively walk an expression tree, collecting the unique set of columns
/// referenced in the expression
pub fn expr_to_columns(expr: &Expr, accum: &mut HashSet<Column>) -> Result<()> {
    expr.apply(|expr| {
        match expr {
            Expr::Column(qc) => {
                accum.insert(qc.clone());
            }
            // Use explicit pattern match instead of a default
            // implementation, so that in the future if someone adds
            // new Expr types, they will check here as well
            // TODO: remove the next line after `Expr::Wildcard` is removed
            #[expect(deprecated)]
            Expr::Unnest(_)
            | Expr::ScalarVariable(_, _)
            | Expr::Alias(_)
            | Expr::Literal(_, _)
            | Expr::BinaryExpr { .. }
            | Expr::Like { .. }
            | Expr::SimilarTo { .. }
            | Expr::Not(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotUnknown(_)
            | Expr::Negative(_)
            | Expr::Between { .. }
            | Expr::Case { .. }
            | Expr::Cast { .. }
            | Expr::TryCast { .. }
            | Expr::ScalarFunction(..)
            | Expr::WindowFunction { .. }
            | Expr::AggregateFunction { .. }
            | Expr::GroupingSet(_)
            | Expr::InList { .. }
            | Expr::Exists { .. }
            | Expr::InSubquery(_)
            | Expr::ScalarSubquery(_)
            | Expr::Wildcard { .. }
            | Expr::Placeholder(_)
            | Expr::OuterReferenceColumn { .. } => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .map(|_| ())
}

/// Find excluded columns in the schema, if any
/// SELECT * EXCLUDE(col1, col2), would return `vec![col1, col2]`
fn get_excluded_columns(
    opt_exclude: Option<&ExcludeSelectItem>,
    opt_except: Option<&ExceptSelectItem>,
    schema: &DFSchema,
    qualifier: Option<&TableReference>,
) -> Result<Vec<Column>> {
    let mut idents = vec![];
    if let Some(excepts) = opt_except {
        idents.push(&excepts.first_element);
        idents.extend(&excepts.additional_elements);
    }
    if let Some(exclude) = opt_exclude {
        match exclude {
            ExcludeSelectItem::Single(ident) => idents.push(ident),
            ExcludeSelectItem::Multiple(idents_inner) => idents.extend(idents_inner),
        }
    }
    // Excluded columns should be unique
    let n_elem = idents.len();
    let unique_idents = idents.into_iter().collect::<HashSet<_>>();
    // If HashSet size, and vector length are different, this means that some of the excluded columns
    // are not unique. In this case return error.
    if n_elem != unique_idents.len() {
        return plan_err!("EXCLUDE or EXCEPT contains duplicate column names");
    }

    let mut result = vec![];
    for ident in unique_idents.into_iter() {
        let col_name = ident.value.as_str();
        let (qualifier, field) = schema.qualified_field_with_name(qualifier, col_name)?;
        result.push(Column::from((qualifier, field)));
    }
    Ok(result)
}

/// Returns all `Expr`s in the schema, except the `Column`s in the `columns_to_skip`
fn get_exprs_except_skipped(
    schema: &DFSchema,
    columns_to_skip: HashSet<Column>,
) -> Vec<Expr> {
    if columns_to_skip.is_empty() {
        schema.iter().map(Expr::from).collect::<Vec<Expr>>()
    } else {
        schema
            .columns()
            .iter()
            .filter_map(|c| {
                if !columns_to_skip.contains(c) {
                    Some(Expr::Column(c.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<Expr>>()
    }
}

/// For each column specified in the USING JOIN condition, the JOIN plan outputs it twice
/// (once for each join side), but an unqualified wildcard should include it only once.
/// This function returns the columns that should be excluded.
fn exclude_using_columns(plan: &LogicalPlan) -> Result<HashSet<Column>> {
    let using_columns = plan.using_columns()?;
    let excluded = using_columns
        .into_iter()
        // For each USING JOIN condition, only expand to one of each join column in projection
        .flat_map(|cols| {
            let mut cols = cols.into_iter().collect::<Vec<_>>();
            // sort join columns to make sure we consistently keep the same
            // qualified column
            cols.sort();
            let mut out_column_names: HashSet<String> = HashSet::new();
            cols.into_iter().filter_map(move |c| {
                if out_column_names.contains(&c.name) {
                    Some(c)
                } else {
                    out_column_names.insert(c.name);
                    None
                }
            })
        })
        .collect::<HashSet<_>>();
    Ok(excluded)
}

/// Resolves an `Expr::Wildcard` to a collection of `Expr::Column`'s.
pub fn expand_wildcard(
    schema: &DFSchema,
    plan: &LogicalPlan,
    wildcard_options: Option<&WildcardOptions>,
) -> Result<Vec<Expr>> {
    let mut columns_to_skip = exclude_using_columns(plan)?;
    let excluded_columns = if let Some(WildcardOptions {
        exclude: opt_exclude,
        except: opt_except,
        ..
    }) = wildcard_options
    {
        get_excluded_columns(opt_exclude.as_ref(), opt_except.as_ref(), schema, None)?
    } else {
        vec![]
    };
    // Add each excluded `Column` to columns_to_skip
    columns_to_skip.extend(excluded_columns);
    Ok(get_exprs_except_skipped(schema, columns_to_skip))
}

/// Resolves an `Expr::Wildcard` to a collection of qualified `Expr::Column`'s.
pub fn expand_qualified_wildcard(
    qualifier: &TableReference,
    schema: &DFSchema,
    wildcard_options: Option<&WildcardOptions>,
) -> Result<Vec<Expr>> {
    let qualified_indices = schema.fields_indices_with_qualified(qualifier);
    let projected_func_dependencies = schema
        .functional_dependencies()
        .project_functional_dependencies(&qualified_indices, qualified_indices.len());
    let fields_with_qualified = get_at_indices(schema.fields(), &qualified_indices)?;
    if fields_with_qualified.is_empty() {
        return plan_err!("Invalid qualifier {qualifier}");
    }

    let qualified_schema = Arc::new(Schema::new_with_metadata(
        fields_with_qualified,
        schema.metadata().clone(),
    ));
    let qualified_dfschema =
        DFSchema::try_from_qualified_schema(qualifier.clone(), &qualified_schema)?
            .with_functional_dependencies(projected_func_dependencies)?;
    let excluded_columns = if let Some(WildcardOptions {
        exclude: opt_exclude,
        except: opt_except,
        ..
    }) = wildcard_options
    {
        get_excluded_columns(
            opt_exclude.as_ref(),
            opt_except.as_ref(),
            schema,
            Some(qualifier),
        )?
    } else {
        vec![]
    };
    // Add each excluded `Column` to columns_to_skip
    let mut columns_to_skip = HashSet::new();
    columns_to_skip.extend(excluded_columns);
    Ok(get_exprs_except_skipped(
        &qualified_dfschema,
        columns_to_skip,
    ))
}

/// (expr, "is the SortExpr for window (either comes from PARTITION BY or ORDER BY columns)")
/// If bool is true SortExpr comes from `PARTITION BY` column, if false comes from `ORDER BY` column
type WindowSortKey = Vec<(Sort, bool)>;

/// Generate a sort key for a given window expr's partition_by and order_by expr
pub fn generate_sort_key(
    partition_by: &[Expr],
    order_by: &[Sort],
) -> Result<WindowSortKey> {
    let normalized_order_by_keys = order_by
        .iter()
        .map(|e| {
            let Sort { expr, .. } = e;
            Sort::new(expr.clone(), true, false)
        })
        .collect::<Vec<_>>();

    let mut final_sort_keys = vec![];
    let mut is_partition_flag = vec![];
    partition_by.iter().for_each(|e| {
        // By default, create sort key with ASC is true and NULLS LAST to be consistent with
        // PostgreSQL's rule: https://www.postgresql.org/docs/current/queries-order.html
        let e = e.clone().sort(true, false);
        if let Some(pos) = normalized_order_by_keys.iter().position(|key| key.eq(&e)) {
            let order_by_key = &order_by[pos];
            if !final_sort_keys.contains(order_by_key) {
                final_sort_keys.push(order_by_key.clone());
                is_partition_flag.push(true);
            }
        } else if !final_sort_keys.contains(&e) {
            final_sort_keys.push(e);
            is_partition_flag.push(true);
        }
    });

    order_by.iter().for_each(|e| {
        if !final_sort_keys.contains(e) {
            final_sort_keys.push(e.clone());
            is_partition_flag.push(false);
        }
    });
    let res = final_sort_keys
        .into_iter()
        .zip(is_partition_flag)
        .collect::<Vec<_>>();
    Ok(res)
}

/// Compare the sort expr as PostgreSQL's common_prefix_cmp():
/// <https://github.com/postgres/postgres/blob/master/src/backend/optimizer/plan/planner.c>
pub fn compare_sort_expr(
    sort_expr_a: &Sort,
    sort_expr_b: &Sort,
    schema: &DFSchemaRef,
) -> Ordering {
    let Sort {
        expr: expr_a,
        asc: asc_a,
        nulls_first: nulls_first_a,
    } = sort_expr_a;

    let Sort {
        expr: expr_b,
        asc: asc_b,
        nulls_first: nulls_first_b,
    } = sort_expr_b;

    let ref_indexes_a = find_column_indexes_referenced_by_expr(expr_a, schema);
    let ref_indexes_b = find_column_indexes_referenced_by_expr(expr_b, schema);
    for (idx_a, idx_b) in ref_indexes_a.iter().zip(ref_indexes_b.iter()) {
        match idx_a.cmp(idx_b) {
            Ordering::Less => {
                return Ordering::Less;
            }
            Ordering::Greater => {
                return Ordering::Greater;
            }
            Ordering::Equal => {}
        }
    }
    match ref_indexes_a.len().cmp(&ref_indexes_b.len()) {
        Ordering::Less => return Ordering::Greater,
        Ordering::Greater => {
            return Ordering::Less;
        }
        Ordering::Equal => {}
    }
    match (asc_a, asc_b) {
        (true, false) => {
            return Ordering::Greater;
        }
        (false, true) => {
            return Ordering::Less;
        }
        _ => {}
    }
    match (nulls_first_a, nulls_first_b) {
        (true, false) => {
            return Ordering::Less;
        }
        (false, true) => {
            return Ordering::Greater;
        }
        _ => {}
    }
    Ordering::Equal
}

/// Group a slice of window expression expr by their order by expressions
pub fn group_window_expr_by_sort_keys(
    window_expr: impl IntoIterator<Item = Expr>,
) -> Result<Vec<(WindowSortKey, Vec<Expr>)>> {
    let mut result = vec![];
    window_expr.into_iter().try_for_each(|expr| match &expr {
        Expr::WindowFunction(window_fun) => {
            let WindowFunctionParams{ partition_by, order_by, ..} = &window_fun.as_ref().params;
            let sort_key = generate_sort_key(partition_by, order_by)?;
            if let Some((_, values)) = result.iter_mut().find(
                |group: &&mut (WindowSortKey, Vec<Expr>)| matches!(group, (key, _) if *key == sort_key),
            ) {
                values.push(expr);
            } else {
                result.push((sort_key, vec![expr]))
            }
            Ok(())
        }
        other => internal_err!(
            "Impossibly got non-window expr {other:?}"
        ),
    })?;
    Ok(result)
}

/// Collect all deeply nested `Expr::AggregateFunction`.
/// They are returned in order of occurrence (depth
/// first), with duplicates omitted.
pub fn find_aggregate_exprs<'a>(exprs: impl IntoIterator<Item = &'a Expr>) -> Vec<Expr> {
    find_exprs_in_exprs(exprs, &|nested_expr| {
        matches!(nested_expr, Expr::AggregateFunction { .. })
    })
}

/// Collect all deeply nested `Expr::WindowFunction`. They are returned in order of occurrence
/// (depth first), with duplicates omitted.
pub fn find_window_exprs<'a>(exprs: impl IntoIterator<Item = &'a Expr>) -> Vec<Expr> {
    find_exprs_in_exprs(exprs, &|nested_expr| {
        matches!(nested_expr, Expr::WindowFunction { .. })
    })
}

/// Collect all deeply nested `Expr::OuterReferenceColumn`. They are returned in order of occurrence
/// (depth first), with duplicates omitted.
pub fn find_out_reference_exprs(expr: &Expr) -> Vec<Expr> {
    find_exprs_in_expr(expr, &|nested_expr| {
        matches!(nested_expr, Expr::OuterReferenceColumn { .. })
    })
}

/// Search the provided `Expr`'s, and all of their nested `Expr`, for any that
/// pass the provided test. The returned `Expr`'s are deduplicated and returned
/// in order of appearance (depth first).
fn find_exprs_in_exprs<'a, F>(
    exprs: impl IntoIterator<Item = &'a Expr>,
    test_fn: &F,
) -> Vec<Expr>
where
    F: Fn(&Expr) -> bool,
{
    exprs
        .into_iter()
        .flat_map(|expr| find_exprs_in_expr(expr, test_fn))
        .fold(vec![], |mut acc, expr| {
            if !acc.contains(&expr) {
                acc.push(expr)
            }
            acc
        })
}

/// Search an `Expr`, and all of its nested `Expr`'s, for any that pass the
/// provided test. The returned `Expr`'s are deduplicated and returned in order
/// of appearance (depth first).
fn find_exprs_in_expr<F>(expr: &Expr, test_fn: &F) -> Vec<Expr>
where
    F: Fn(&Expr) -> bool,
{
    let mut exprs = vec![];
    expr.apply(|expr| {
        if test_fn(expr) {
            if !(exprs.contains(expr)) {
                exprs.push(expr.clone())
            }
            // Stop recursing down this expr once we find a match
            return Ok(TreeNodeRecursion::Jump);
        }

        Ok(TreeNodeRecursion::Continue)
    })
    // pre_visit always returns OK, so this will always too
    .expect("no way to return error during recursion");
    exprs
}

/// Recursively inspect an [`Expr`] and all its children.
pub fn inspect_expr_pre<F, E>(expr: &Expr, mut f: F) -> Result<(), E>
where
    F: FnMut(&Expr) -> Result<(), E>,
{
    let mut err = Ok(());
    expr.apply(|expr| {
        if let Err(e) = f(expr) {
            // Save the error for later (it may not be a DataFusionError)
            err = Err(e);
            Ok(TreeNodeRecursion::Stop)
        } else {
            // keep going
            Ok(TreeNodeRecursion::Continue)
        }
    })
    // The closure always returns OK, so this will always too
    .expect("no way to return error during recursion");

    err
}

/// Create field meta-data from an expression, for use in a result set schema
pub fn exprlist_to_fields<'a>(
    exprs: impl IntoIterator<Item = &'a Expr>,
    plan: &LogicalPlan,
) -> Result<Vec<(Option<TableReference>, Arc<Field>)>> {
    // Look for exact match in plan's output schema
    let input_schema = plan.schema();
    exprs
        .into_iter()
        .map(|e| e.to_field(input_schema))
        .collect()
}

/// Convert an expression into Column expression if it's already provided as input plan.
///
/// For example, it rewrites:
///
/// ```text
/// .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
/// .project(vec![col("c1"), sum(col("c2"))?
/// ```
///
/// Into:
///
/// ```text
/// .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
/// .project(vec![col("c1"), col("SUM(c2)")?
/// ```
pub fn columnize_expr(e: Expr, input: &LogicalPlan) -> Result<Expr> {
    let output_exprs = match input.columnized_output_exprs() {
        Ok(exprs) if !exprs.is_empty() => exprs,
        _ => return Ok(e),
    };
    let exprs_map: HashMap<&Expr, Column> = output_exprs.into_iter().collect();
    e.transform_down(|node: Expr| match exprs_map.get(&node) {
        Some(column) => Ok(Transformed::new(
            Expr::Column(column.clone()),
            true,
            TreeNodeRecursion::Jump,
        )),
        None => Ok(Transformed::no(node)),
    })
    .data()
}

/// Collect all deeply nested `Expr::Column`'s. They are returned in order of
/// appearance (depth first), and may contain duplicates.
pub fn find_column_exprs(exprs: &[Expr]) -> Vec<Expr> {
    exprs
        .iter()
        .flat_map(find_columns_referenced_by_expr)
        .map(Expr::Column)
        .collect()
}

pub(crate) fn find_columns_referenced_by_expr(e: &Expr) -> Vec<Column> {
    let mut exprs = vec![];
    e.apply(|expr| {
        if let Expr::Column(c) = expr {
            exprs.push(c.clone())
        }
        Ok(TreeNodeRecursion::Continue)
    })
    // As the closure always returns Ok, this "can't" error
    .expect("Unexpected error");
    exprs
}

/// Convert any `Expr` to an `Expr::Column`.
pub fn expr_as_column_expr(expr: &Expr, plan: &LogicalPlan) -> Result<Expr> {
    match expr {
        Expr::Column(col) => {
            let (qualifier, field) = plan.schema().qualified_field_from_column(col)?;
            Ok(Expr::from(Column::from((qualifier, field))))
        }
        _ => Ok(Expr::Column(Column::from_name(
            expr.schema_name().to_string(),
        ))),
    }
}

/// Recursively walk an expression tree, collecting the column indexes
/// referenced in the expression
pub(crate) fn find_column_indexes_referenced_by_expr(
    e: &Expr,
    schema: &DFSchemaRef,
) -> Vec<usize> {
    let mut indexes = vec![];
    e.apply(|expr| {
        match expr {
            Expr::Column(qc) => {
                if let Ok(idx) = schema.index_of_column(qc) {
                    indexes.push(idx);
                }
            }
            Expr::Literal(_, _) => {
                indexes.push(usize::MAX);
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    indexes
}

/// Can this data type be used in hash join equal conditions??
/// Data types here come from function 'equal_rows', if more data types are supported
/// in create_hashes, add those data types here to generate join logical plan.
pub fn can_hash(data_type: &DataType) -> bool {
    match data_type {
        DataType::Null => true,
        DataType::Boolean => true,
        DataType::Int8 => true,
        DataType::Int16 => true,
        DataType::Int32 => true,
        DataType::Int64 => true,
        DataType::UInt8 => true,
        DataType::UInt16 => true,
        DataType::UInt32 => true,
        DataType::UInt64 => true,
        DataType::Float16 => true,
        DataType::Float32 => true,
        DataType::Float64 => true,
        DataType::Decimal128(_, _) => true,
        DataType::Decimal256(_, _) => true,
        DataType::Timestamp(_, _) => true,
        DataType::Utf8 => true,
        DataType::LargeUtf8 => true,
        DataType::Utf8View => true,
        DataType::Binary => true,
        DataType::LargeBinary => true,
        DataType::BinaryView => true,
        DataType::Date32 => true,
        DataType::Date64 => true,
        DataType::Time32(_) => true,
        DataType::Time64(_) => true,
        DataType::Duration(_) => true,
        DataType::Interval(_) => true,
        DataType::FixedSizeBinary(_) => true,
        DataType::Dictionary(key_type, value_type) => {
            DataType::is_dictionary_key_type(key_type) && can_hash(value_type)
        }
        DataType::List(value_type) => can_hash(value_type.data_type()),
        DataType::LargeList(value_type) => can_hash(value_type.data_type()),
        DataType::FixedSizeList(value_type, _) => can_hash(value_type.data_type()),
        DataType::Map(map_struct, true | false) => can_hash(map_struct.data_type()),
        DataType::Struct(fields) => fields.iter().all(|f| can_hash(f.data_type())),

        DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::Union(_, _)
        | DataType::RunEndEncoded(_, _) => false,
    }
}

/// Check whether all columns are from the schema.
pub fn check_all_columns_from_schema(
    columns: &HashSet<&Column>,
    schema: &DFSchema,
) -> Result<bool> {
    for col in columns.iter() {
        let exist = schema.is_column_from_schema(col);
        if !exist {
            return Ok(false);
        }
    }

    Ok(true)
}

/// Give two sides of the equijoin predicate, return a valid join key pair.
/// If there is no valid join key pair, return None.
///
/// A valid join means:
/// 1. All referenced column of the left side is from the left schema, and
///    all referenced column of the right side is from the right schema.
/// 2. Or opposite. All referenced column of the left side is from the right schema,
///    and the right side is from the left schema.
///
pub fn find_valid_equijoin_key_pair(
    left_key: &Expr,
    right_key: &Expr,
    left_schema: &DFSchema,
    right_schema: &DFSchema,
) -> Result<Option<(Expr, Expr)>> {
    let left_using_columns = left_key.column_refs();
    let right_using_columns = right_key.column_refs();

    // Conditions like a = 10, will be added to non-equijoin.
    if left_using_columns.is_empty() || right_using_columns.is_empty() {
        return Ok(None);
    }

    if check_all_columns_from_schema(&left_using_columns, left_schema)?
        && check_all_columns_from_schema(&right_using_columns, right_schema)?
    {
        return Ok(Some((left_key.clone(), right_key.clone())));
    } else if check_all_columns_from_schema(&right_using_columns, left_schema)?
        && check_all_columns_from_schema(&left_using_columns, right_schema)?
    {
        return Ok(Some((right_key.clone(), left_key.clone())));
    }

    Ok(None)
}

/// Creates a detailed error message for a function with wrong signature.
///
/// For example, a query like `select round(3.14, 1.1);` would yield:
/// ```text
/// Error during planning: No function matches 'round(Float64, Float64)'. You might need to add explicit type casts.
///     Candidate functions:
///     round(Float64, Int64)
///     round(Float32, Int64)
///     round(Float64)
///     round(Float32)
/// ```
pub fn generate_signature_error_msg(
    func_name: &str,
    func_signature: Signature,
    input_expr_types: &[DataType],
) -> String {
    let candidate_signatures = func_signature
        .type_signature
        .to_string_repr()
        .iter()
        .map(|args_str| format!("\t{func_name}({args_str})"))
        .collect::<Vec<String>>()
        .join("\n");

    format!(
            "No function matches the given name and argument types '{}({})'. You might need to add explicit type casts.\n\tCandidate functions:\n{}",
            func_name, TypeSignature::join_types(input_expr_types, ", "), candidate_signatures
        )
}

/// Splits a conjunctive [`Expr`] such as `A AND B AND C` => `[A, B, C]`
///
/// See [`split_conjunction_owned`] for more details and an example.
pub fn split_conjunction(expr: &Expr) -> Vec<&Expr> {
    split_conjunction_impl(expr, vec![])
}

fn split_conjunction_impl<'a>(expr: &'a Expr, mut exprs: Vec<&'a Expr>) -> Vec<&'a Expr> {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            right,
            op: Operator::And,
            left,
        }) => {
            let exprs = split_conjunction_impl(left, exprs);
            split_conjunction_impl(right, exprs)
        }
        Expr::Alias(Alias { expr, .. }) => split_conjunction_impl(expr, exprs),
        other => {
            exprs.push(other);
            exprs
        }
    }
}

/// Iterate parts in a conjunctive [`Expr`] such as `A AND B AND C` => `[A, B, C]`
///
/// See [`split_conjunction_owned`] for more details and an example.
pub fn iter_conjunction(expr: &Expr) -> impl Iterator<Item = &Expr> {
    let mut stack = vec![expr];
    std::iter::from_fn(move || {
        while let Some(expr) = stack.pop() {
            match expr {
                Expr::BinaryExpr(BinaryExpr {
                    right,
                    op: Operator::And,
                    left,
                }) => {
                    stack.push(right);
                    stack.push(left);
                }
                Expr::Alias(Alias { expr, .. }) => stack.push(expr),
                other => return Some(other),
            }
        }
        None
    })
}

/// Iterate parts in a conjunctive [`Expr`] such as `A AND B AND C` => `[A, B, C]`
///
/// See [`split_conjunction_owned`] for more details and an example.
pub fn iter_conjunction_owned(expr: Expr) -> impl Iterator<Item = Expr> {
    let mut stack = vec![expr];
    std::iter::from_fn(move || {
        while let Some(expr) = stack.pop() {
            match expr {
                Expr::BinaryExpr(BinaryExpr {
                    right,
                    op: Operator::And,
                    left,
                }) => {
                    stack.push(*right);
                    stack.push(*left);
                }
                Expr::Alias(Alias { expr, .. }) => stack.push(*expr),
                other => return Some(other),
            }
        }
        None
    })
}

/// Splits an owned conjunctive [`Expr`] such as `A AND B AND C` => `[A, B, C]`
///
/// This is often used to "split" filter expressions such as `col1 = 5
/// AND col2 = 10` into [`col1 = 5`, `col2 = 10`];
///
/// # Example
/// ```
/// # use datafusion_expr::{col, lit};
/// # use datafusion_expr::utils::split_conjunction_owned;
/// // a=1 AND b=2
/// let expr = col("a").eq(lit(1)).and(col("b").eq(lit(2)));
///
/// // [a=1, b=2]
/// let split = vec![
///   col("a").eq(lit(1)),
///   col("b").eq(lit(2)),
/// ];
///
/// // use split_conjunction_owned to split them
/// assert_eq!(split_conjunction_owned(expr), split);
/// ```
pub fn split_conjunction_owned(expr: Expr) -> Vec<Expr> {
    split_binary_owned(expr, Operator::And)
}

/// Splits an owned binary operator tree [`Expr`] such as `A <OP> B <OP> C` => `[A, B, C]`
///
/// This is often used to "split" expressions such as `col1 = 5
/// AND col2 = 10` into [`col1 = 5`, `col2 = 10`];
///
/// # Example
/// ```
/// # use datafusion_expr::{col, lit, Operator};
/// # use datafusion_expr::utils::split_binary_owned;
/// # use std::ops::Add;
/// // a=1 + b=2
/// let expr = col("a").eq(lit(1)).add(col("b").eq(lit(2)));
///
/// // [a=1, b=2]
/// let split = vec![
///   col("a").eq(lit(1)),
///   col("b").eq(lit(2)),
/// ];
///
/// // use split_binary_owned to split them
/// assert_eq!(split_binary_owned(expr, Operator::Plus), split);
/// ```
pub fn split_binary_owned(expr: Expr, op: Operator) -> Vec<Expr> {
    split_binary_owned_impl(expr, op, vec![])
}

fn split_binary_owned_impl(
    expr: Expr,
    operator: Operator,
    mut exprs: Vec<Expr>,
) -> Vec<Expr> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { right, op, left }) if op == operator => {
            let exprs = split_binary_owned_impl(*left, operator, exprs);
            split_binary_owned_impl(*right, operator, exprs)
        }
        Expr::Alias(Alias { expr, .. }) => {
            split_binary_owned_impl(*expr, operator, exprs)
        }
        other => {
            exprs.push(other);
            exprs
        }
    }
}

/// Splits an binary operator tree [`Expr`] such as `A <OP> B <OP> C` => `[A, B, C]`
///
/// See [`split_binary_owned`] for more details and an example.
pub fn split_binary(expr: &Expr, op: Operator) -> Vec<&Expr> {
    split_binary_impl(expr, op, vec![])
}

fn split_binary_impl<'a>(
    expr: &'a Expr,
    operator: Operator,
    mut exprs: Vec<&'a Expr>,
) -> Vec<&'a Expr> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { right, op, left }) if *op == operator => {
            let exprs = split_binary_impl(left, operator, exprs);
            split_binary_impl(right, operator, exprs)
        }
        Expr::Alias(Alias { expr, .. }) => split_binary_impl(expr, operator, exprs),
        other => {
            exprs.push(other);
            exprs
        }
    }
}

/// Combines an array of filter expressions into a single filter
/// expression consisting of the input filter expressions joined with
/// logical AND.
///
/// Returns None if the filters array is empty.
///
/// # Example
/// ```
/// # use datafusion_expr::{col, lit};
/// # use datafusion_expr::utils::conjunction;
/// // a=1 AND b=2
/// let expr = col("a").eq(lit(1)).and(col("b").eq(lit(2)));
///
/// // [a=1, b=2]
/// let split = vec![
///   col("a").eq(lit(1)),
///   col("b").eq(lit(2)),
/// ];
///
/// // use conjunction to join them together with `AND`
/// assert_eq!(conjunction(split), Some(expr));
/// ```
pub fn conjunction(filters: impl IntoIterator<Item = Expr>) -> Option<Expr> {
    filters.into_iter().reduce(Expr::and)
}

/// Combines an array of filter expressions into a single filter
/// expression consisting of the input filter expressions joined with
/// logical OR.
///
/// Returns None if the filters array is empty.
///
/// # Example
/// ```
/// # use datafusion_expr::{col, lit};
/// # use datafusion_expr::utils::disjunction;
/// // a=1 OR b=2
/// let expr = col("a").eq(lit(1)).or(col("b").eq(lit(2)));
///
/// // [a=1, b=2]
/// let split = vec![
///   col("a").eq(lit(1)),
///   col("b").eq(lit(2)),
/// ];
///
/// // use disjunction to join them together with `OR`
/// assert_eq!(disjunction(split), Some(expr));
/// ```
pub fn disjunction(filters: impl IntoIterator<Item = Expr>) -> Option<Expr> {
    filters.into_iter().reduce(Expr::or)
}

/// Returns a new [LogicalPlan] that filters the output of  `plan` with a
/// [LogicalPlan::Filter] with all `predicates` ANDed.
///
/// # Example
/// Before:
/// ```text
/// plan
/// ```
///
/// After:
/// ```text
/// Filter(predicate)
///   plan
/// ```
pub fn add_filter(plan: LogicalPlan, predicates: &[&Expr]) -> Result<LogicalPlan> {
    // reduce filters to a single filter with an AND
    let predicate = predicates
        .iter()
        .skip(1)
        .fold(predicates[0].clone(), |acc, predicate| {
            and(acc, (*predicate).to_owned())
        });

    Ok(LogicalPlan::Filter(Filter::try_new(
        predicate,
        Arc::new(plan),
    )?))
}

/// Looks for correlating expressions: for example, a binary expression with one field from the subquery, and
/// one not in the subquery (closed upon from outer scope)
///
/// # Arguments
///
/// * `exprs` - List of expressions that may or may not be joins
///
/// # Return value
///
/// Tuple of (expressions containing joins, remaining non-join expressions)
pub fn find_join_exprs(exprs: Vec<&Expr>) -> Result<(Vec<Expr>, Vec<Expr>)> {
    let mut joins = vec![];
    let mut others = vec![];
    for filter in exprs.into_iter() {
        // If the expression contains correlated predicates, add it to join filters
        if filter.contains_outer() {
            if !matches!(filter, Expr::BinaryExpr(BinaryExpr{ left, op: Operator::Eq, right }) if left.eq(right))
            {
                joins.push(strip_outer_reference((*filter).clone()));
            }
        } else {
            others.push((*filter).clone());
        }
    }

    Ok((joins, others))
}

/// Returns the first (and only) element in a slice, or an error
///
/// # Arguments
///
/// * `slice` - The slice to extract from
///
/// # Return value
///
/// The first element, or an error
pub fn only_or_err<T>(slice: &[T]) -> Result<&T> {
    match slice {
        [it] => Ok(it),
        [] => plan_err!("No items found!"),
        _ => plan_err!("More than one item found!"),
    }
}

/// merge inputs schema into a single schema.
pub fn merge_schema(inputs: &[&LogicalPlan]) -> DFSchema {
    if inputs.len() == 1 {
        inputs[0].schema().as_ref().clone()
    } else {
        inputs.iter().map(|input| input.schema()).fold(
            DFSchema::empty(),
            |mut lhs, rhs| {
                lhs.merge(rhs);
                lhs
            },
        )
    }
}

/// Build state name. State is the intermediate state of the aggregate function.
pub fn format_state_name(name: &str, state_name: &str) -> String {
    format!("{name}[{state_name}]")
}

/// Determine the set of [`Column`]s produced by the subquery.
pub fn collect_subquery_cols(
    exprs: &[Expr],
    subquery_schema: &DFSchema,
) -> Result<BTreeSet<Column>> {
    exprs.iter().try_fold(BTreeSet::new(), |mut cols, expr| {
        let mut using_cols: Vec<Column> = vec![];
        for col in expr.column_refs().into_iter() {
            if subquery_schema.has_column(col) {
                using_cols.push(col.clone());
            }
        }

        cols.extend(using_cols);
        Result::<_>::Ok(cols)
    })
}

/// Generates implementation of `equals` and `hash_value` methods for a trait, delegating
/// to [`PartialEq`] and [`Hash`] implementations on Self.
/// Meant to be used with traits representing user-defined functions (UDFs).
///
/// Example showing generation of [`ScalarUDFImpl::equals`] and [`ScalarUDFImpl::hash_value`]
/// implementations.
///
/// ```
/// # use arrow::datatypes::DataType;
/// # use datafusion_expr::{udf_equals_hash, ScalarFunctionArgs, ScalarUDFImpl};
/// # use datafusion_expr_common::columnar_value::ColumnarValue;
/// # use datafusion_expr_common::signature::Signature;
/// # use std::any::Any;
///
/// // Implementing PartialEq & Hash is a prerequisite for using this macro,
/// // but the implementation can be derived.
/// #[derive(Debug, PartialEq, Hash)]
/// struct VarcharToTimestampTz {
///     safe: bool,
/// }
///
/// impl ScalarUDFImpl for VarcharToTimestampTz {
///     /* other methods omitted for brevity */
/// #    fn as_any(&self) -> &dyn Any {
/// #        self
/// #    }
/// #
/// #    fn name(&self) -> &str {
/// #        "varchar_to_timestamp_tz"
/// #    }
/// #
/// #    fn signature(&self) -> &Signature {
/// #        todo!()
/// #    }
/// #
/// #    fn return_type(
/// #        &self,
/// #        _arg_types: &[DataType],
/// #    ) -> datafusion_common::Result<DataType> {
/// #        todo!()
/// #    }
/// #
/// #    fn invoke_with_args(
/// #        &self,
/// #        args: ScalarFunctionArgs,
/// #    ) -> datafusion_common::Result<ColumnarValue> {
/// #        todo!()
/// #    }
/// #
///     udf_equals_hash!(ScalarUDFImpl);
/// }
/// ```
///
/// [`ScalarUDFImpl::equals`]: crate::ScalarUDFImpl::equals
/// [`ScalarUDFImpl::hash_value`]: crate::ScalarUDFImpl::hash_value
#[macro_export]
macro_rules! udf_equals_hash {
    ($udf_type:tt) => {
        fn equals(&self, other: &dyn $udf_type) -> bool {
            use ::core::any::Any;
            use ::core::cmp::PartialEq;
            let Some(other) = <dyn Any + 'static>::downcast_ref::<Self>(other.as_any())
            else {
                return false;
            };
            PartialEq::eq(self, other)
        }

        fn hash_value(&self) -> u64 {
            use ::std::any::type_name;
            use ::std::hash::{DefaultHasher, Hash, Hasher};
            let hasher = &mut DefaultHasher::new();
            type_name::<Self>().hash(hasher);
            Hash::hash(self, hasher);
            Hasher::finish(hasher)
        }
    };
}

pub fn arc_ptr_eq<T: ?Sized>(a: &Arc<T>, b: &Arc<T>) -> bool {
    // Not necessarily equivalent to `Arc::ptr_eq` for fat pointers.
    std::ptr::eq(Arc::as_ptr(a), Arc::as_ptr(b))
}

pub fn arc_ptr_hash<T: ?Sized>(a: &Arc<T>, hasher: &mut impl Hasher) {
    std::ptr::hash(Arc::as_ptr(a), hasher)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        col, cube,
        expr::WindowFunction,
        expr_vec_fmt, grouping_set, lit, rollup,
        test::function_stub::{max_udaf, min_udaf, sum_udaf},
        Cast, ExprFunctionExt, ScalarFunctionArgs, ScalarUDFImpl,
        WindowFunctionDefinition,
    };
    use arrow::datatypes::{UnionFields, UnionMode};
    use datafusion_expr_common::columnar_value::ColumnarValue;
    use datafusion_expr_common::signature::Volatility;
    use std::any::Any;

    #[test]
    fn test_group_window_expr_by_sort_keys_empty_case() -> Result<()> {
        let result = group_window_expr_by_sort_keys(vec![])?;
        let expected: Vec<(WindowSortKey, Vec<Expr>)> = vec![];
        assert_eq!(expected, result);
        Ok(())
    }

    #[test]
    fn test_group_window_expr_by_sort_keys_empty_window() -> Result<()> {
        let max1 = Expr::from(WindowFunction::new(
            WindowFunctionDefinition::AggregateUDF(max_udaf()),
            vec![col("name")],
        ));
        let max2 = Expr::from(WindowFunction::new(
            WindowFunctionDefinition::AggregateUDF(max_udaf()),
            vec![col("name")],
        ));
        let min3 = Expr::from(WindowFunction::new(
            WindowFunctionDefinition::AggregateUDF(min_udaf()),
            vec![col("name")],
        ));
        let sum4 = Expr::from(WindowFunction::new(
            WindowFunctionDefinition::AggregateUDF(sum_udaf()),
            vec![col("age")],
        ));
        let exprs = &[max1.clone(), max2.clone(), min3.clone(), sum4.clone()];
        let result = group_window_expr_by_sort_keys(exprs.to_vec())?;
        let key = vec![];
        let expected: Vec<(WindowSortKey, Vec<Expr>)> =
            vec![(key, vec![max1, max2, min3, sum4])];
        assert_eq!(expected, result);
        Ok(())
    }

    #[test]
    fn test_group_window_expr_by_sort_keys() -> Result<()> {
        let age_asc = Sort::new(col("age"), true, true);
        let name_desc = Sort::new(col("name"), false, true);
        let created_at_desc = Sort::new(col("created_at"), false, true);
        let max1 = Expr::from(WindowFunction::new(
            WindowFunctionDefinition::AggregateUDF(max_udaf()),
            vec![col("name")],
        ))
        .order_by(vec![age_asc.clone(), name_desc.clone()])
        .build()
        .unwrap();
        let max2 = Expr::from(WindowFunction::new(
            WindowFunctionDefinition::AggregateUDF(max_udaf()),
            vec![col("name")],
        ));
        let min3 = Expr::from(WindowFunction::new(
            WindowFunctionDefinition::AggregateUDF(min_udaf()),
            vec![col("name")],
        ))
        .order_by(vec![age_asc.clone(), name_desc.clone()])
        .build()
        .unwrap();
        let sum4 = Expr::from(WindowFunction::new(
            WindowFunctionDefinition::AggregateUDF(sum_udaf()),
            vec![col("age")],
        ))
        .order_by(vec![
            name_desc.clone(),
            age_asc.clone(),
            created_at_desc.clone(),
        ])
        .build()
        .unwrap();
        // FIXME use as_ref
        let exprs = &[max1.clone(), max2.clone(), min3.clone(), sum4.clone()];
        let result = group_window_expr_by_sort_keys(exprs.to_vec())?;

        let key1 = vec![(age_asc.clone(), false), (name_desc.clone(), false)];
        let key2 = vec![];
        let key3 = vec![
            (name_desc, false),
            (age_asc, false),
            (created_at_desc, false),
        ];

        let expected: Vec<(WindowSortKey, Vec<Expr>)> = vec![
            (key1, vec![max1, min3]),
            (key2, vec![max2]),
            (key3, vec![sum4]),
        ];
        assert_eq!(expected, result);
        Ok(())
    }

    #[test]
    fn avoid_generate_duplicate_sort_keys() -> Result<()> {
        let asc_or_desc = [true, false];
        let nulls_first_or_last = [true, false];
        let partition_by = &[col("age"), col("name"), col("created_at")];
        for asc_ in asc_or_desc {
            for nulls_first_ in nulls_first_or_last {
                let order_by = &[
                    Sort {
                        expr: col("age"),
                        asc: asc_,
                        nulls_first: nulls_first_,
                    },
                    Sort {
                        expr: col("name"),
                        asc: asc_,
                        nulls_first: nulls_first_,
                    },
                ];

                let expected = vec![
                    (
                        Sort {
                            expr: col("age"),
                            asc: asc_,
                            nulls_first: nulls_first_,
                        },
                        true,
                    ),
                    (
                        Sort {
                            expr: col("name"),
                            asc: asc_,
                            nulls_first: nulls_first_,
                        },
                        true,
                    ),
                    (
                        Sort {
                            expr: col("created_at"),
                            asc: true,
                            nulls_first: false,
                        },
                        true,
                    ),
                ];
                let result = generate_sort_key(partition_by, order_by)?;
                assert_eq!(expected, result);
            }
        }
        Ok(())
    }

    #[test]
    fn test_enumerate_grouping_sets() -> Result<()> {
        let multi_cols = vec![col("col1"), col("col2"), col("col3")];
        let simple_col = col("simple_col");
        let cube = cube(multi_cols.clone());
        let rollup = rollup(multi_cols.clone());
        let grouping_set = grouping_set(vec![multi_cols]);

        // 1. col
        let sets = enumerate_grouping_sets(vec![simple_col.clone()])?;
        let result = format!("[{}]", expr_vec_fmt!(sets));
        assert_eq!("[simple_col]", &result);

        // 2. cube
        let sets = enumerate_grouping_sets(vec![cube.clone()])?;
        let result = format!("[{}]", expr_vec_fmt!(sets));
        assert_eq!("[CUBE (col1, col2, col3)]", &result);

        // 3. rollup
        let sets = enumerate_grouping_sets(vec![rollup.clone()])?;
        let result = format!("[{}]", expr_vec_fmt!(sets));
        assert_eq!("[ROLLUP (col1, col2, col3)]", &result);

        // 4. col + cube
        let sets = enumerate_grouping_sets(vec![simple_col.clone(), cube.clone()])?;
        let result = format!("[{}]", expr_vec_fmt!(sets));
        assert_eq!(
            "[GROUPING SETS (\
            (simple_col), \
            (simple_col, col1), \
            (simple_col, col2), \
            (simple_col, col1, col2), \
            (simple_col, col3), \
            (simple_col, col1, col3), \
            (simple_col, col2, col3), \
            (simple_col, col1, col2, col3))]",
            &result
        );

        // 5. col + rollup
        let sets = enumerate_grouping_sets(vec![simple_col.clone(), rollup.clone()])?;
        let result = format!("[{}]", expr_vec_fmt!(sets));
        assert_eq!(
            "[GROUPING SETS (\
            (simple_col), \
            (simple_col, col1), \
            (simple_col, col1, col2), \
            (simple_col, col1, col2, col3))]",
            &result
        );

        // 6. col + grouping_set
        let sets =
            enumerate_grouping_sets(vec![simple_col.clone(), grouping_set.clone()])?;
        let result = format!("[{}]", expr_vec_fmt!(sets));
        assert_eq!(
            "[GROUPING SETS (\
            (simple_col, col1, col2, col3))]",
            &result
        );

        // 7. col + grouping_set + rollup
        let sets = enumerate_grouping_sets(vec![
            simple_col.clone(),
            grouping_set,
            rollup.clone(),
        ])?;
        let result = format!("[{}]", expr_vec_fmt!(sets));
        assert_eq!(
            "[GROUPING SETS (\
            (simple_col, col1, col2, col3), \
            (simple_col, col1, col2, col3, col1), \
            (simple_col, col1, col2, col3, col1, col2), \
            (simple_col, col1, col2, col3, col1, col2, col3))]",
            &result
        );

        // 8. col + cube + rollup
        let sets = enumerate_grouping_sets(vec![simple_col, cube, rollup])?;
        let result = format!("[{}]", expr_vec_fmt!(sets));
        assert_eq!(
            "[GROUPING SETS (\
            (simple_col), \
            (simple_col, col1), \
            (simple_col, col1, col2), \
            (simple_col, col1, col2, col3), \
            (simple_col, col1), \
            (simple_col, col1, col1), \
            (simple_col, col1, col1, col2), \
            (simple_col, col1, col1, col2, col3), \
            (simple_col, col2), \
            (simple_col, col2, col1), \
            (simple_col, col2, col1, col2), \
            (simple_col, col2, col1, col2, col3), \
            (simple_col, col1, col2), \
            (simple_col, col1, col2, col1), \
            (simple_col, col1, col2, col1, col2), \
            (simple_col, col1, col2, col1, col2, col3), \
            (simple_col, col3), \
            (simple_col, col3, col1), \
            (simple_col, col3, col1, col2), \
            (simple_col, col3, col1, col2, col3), \
            (simple_col, col1, col3), \
            (simple_col, col1, col3, col1), \
            (simple_col, col1, col3, col1, col2), \
            (simple_col, col1, col3, col1, col2, col3), \
            (simple_col, col2, col3), \
            (simple_col, col2, col3, col1), \
            (simple_col, col2, col3, col1, col2), \
            (simple_col, col2, col3, col1, col2, col3), \
            (simple_col, col1, col2, col3), \
            (simple_col, col1, col2, col3, col1), \
            (simple_col, col1, col2, col3, col1, col2), \
            (simple_col, col1, col2, col3, col1, col2, col3))]",
            &result
        );

        Ok(())
    }
    #[test]
    fn test_split_conjunction() {
        let expr = col("a");
        let result = split_conjunction(&expr);
        assert_eq!(result, vec![&expr]);
    }

    #[test]
    fn test_split_conjunction_two() {
        let expr = col("a").eq(lit(5)).and(col("b"));
        let expr1 = col("a").eq(lit(5));
        let expr2 = col("b");

        let result = split_conjunction(&expr);
        assert_eq!(result, vec![&expr1, &expr2]);
    }

    #[test]
    fn test_split_conjunction_alias() {
        let expr = col("a").eq(lit(5)).and(col("b").alias("the_alias"));
        let expr1 = col("a").eq(lit(5));
        let expr2 = col("b"); // has no alias

        let result = split_conjunction(&expr);
        assert_eq!(result, vec![&expr1, &expr2]);
    }

    #[test]
    fn test_split_conjunction_or() {
        let expr = col("a").eq(lit(5)).or(col("b"));
        let result = split_conjunction(&expr);
        assert_eq!(result, vec![&expr]);
    }

    #[test]
    fn test_split_binary_owned() {
        let expr = col("a");
        assert_eq!(split_binary_owned(expr.clone(), Operator::And), vec![expr]);
    }

    #[test]
    fn test_split_binary_owned_two() {
        assert_eq!(
            split_binary_owned(col("a").eq(lit(5)).and(col("b")), Operator::And),
            vec![col("a").eq(lit(5)), col("b")]
        );
    }

    #[test]
    fn test_split_binary_owned_different_op() {
        let expr = col("a").eq(lit(5)).or(col("b"));
        assert_eq!(
            // expr is connected by OR, but pass in AND
            split_binary_owned(expr.clone(), Operator::And),
            vec![expr]
        );
    }

    #[test]
    fn test_split_conjunction_owned() {
        let expr = col("a");
        assert_eq!(split_conjunction_owned(expr.clone()), vec![expr]);
    }

    #[test]
    fn test_split_conjunction_owned_two() {
        assert_eq!(
            split_conjunction_owned(col("a").eq(lit(5)).and(col("b"))),
            vec![col("a").eq(lit(5)), col("b")]
        );
    }

    #[test]
    fn test_split_conjunction_owned_alias() {
        assert_eq!(
            split_conjunction_owned(col("a").eq(lit(5)).and(col("b").alias("the_alias"))),
            vec![
                col("a").eq(lit(5)),
                // no alias on b
                col("b"),
            ]
        );
    }

    #[test]
    fn test_conjunction_empty() {
        assert_eq!(conjunction(vec![]), None);
    }

    #[test]
    fn test_conjunction() {
        // `[A, B, C]`
        let expr = conjunction(vec![col("a"), col("b"), col("c")]);

        // --> `(A AND B) AND C`
        assert_eq!(expr, Some(col("a").and(col("b")).and(col("c"))));

        // which is different than `A AND (B AND C)`
        assert_ne!(expr, Some(col("a").and(col("b").and(col("c")))));
    }

    #[test]
    fn test_disjunction_empty() {
        assert_eq!(disjunction(vec![]), None);
    }

    #[test]
    fn test_disjunction() {
        // `[A, B, C]`
        let expr = disjunction(vec![col("a"), col("b"), col("c")]);

        // --> `(A OR B) OR C`
        assert_eq!(expr, Some(col("a").or(col("b")).or(col("c"))));

        // which is different than `A OR (B OR C)`
        assert_ne!(expr, Some(col("a").or(col("b").or(col("c")))));
    }

    #[test]
    fn test_split_conjunction_owned_or() {
        let expr = col("a").eq(lit(5)).or(col("b"));
        assert_eq!(split_conjunction_owned(expr.clone()), vec![expr]);
    }

    #[test]
    fn test_collect_expr() -> Result<()> {
        let mut accum: HashSet<Column> = HashSet::new();
        expr_to_columns(
            &Expr::Cast(Cast::new(Box::new(col("a")), DataType::Float64)),
            &mut accum,
        )?;
        expr_to_columns(
            &Expr::Cast(Cast::new(Box::new(col("a")), DataType::Float64)),
            &mut accum,
        )?;
        assert_eq!(1, accum.len());
        assert!(accum.contains(&Column::from_name("a")));
        Ok(())
    }

    #[test]
    fn test_can_hash() {
        let union_fields: UnionFields = [
            (0, Arc::new(Field::new("A", DataType::Int32, true))),
            (1, Arc::new(Field::new("B", DataType::Float64, true))),
        ]
        .into_iter()
        .collect();

        let union_type = DataType::Union(union_fields, UnionMode::Sparse);
        assert!(!can_hash(&union_type));

        let list_union_type =
            DataType::List(Arc::new(Field::new("my_union", union_type, true)));
        assert!(!can_hash(&list_union_type));
    }

    #[test]
    fn test_udf_equals_hash() {
        #[derive(Debug, PartialEq, Hash)]
        struct StatefulFunctionWithEqHash {
            signature: Signature,
            state: bool,
        }
        impl ScalarUDFImpl for StatefulFunctionWithEqHash {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn name(&self) -> &str {
                "StatefulFunctionWithEqHash"
            }
            fn signature(&self) -> &Signature {
                &self.signature
            }
            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                todo!()
            }
            fn invoke_with_args(
                &self,
                _args: ScalarFunctionArgs,
            ) -> Result<ColumnarValue> {
                todo!()
            }
        }

        #[derive(Debug, PartialEq, Hash)]
        struct StatefulFunctionWithEqHashWithUdfEqualsHash {
            signature: Signature,
            state: bool,
        }
        impl ScalarUDFImpl for StatefulFunctionWithEqHashWithUdfEqualsHash {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn name(&self) -> &str {
                "StatefulFunctionWithEqHashWithUdfEqualsHash"
            }
            fn signature(&self) -> &Signature {
                &self.signature
            }
            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                todo!()
            }
            fn invoke_with_args(
                &self,
                _args: ScalarFunctionArgs,
            ) -> Result<ColumnarValue> {
                todo!()
            }
            udf_equals_hash!(ScalarUDFImpl);
        }

        let signature = Signature::exact(vec![DataType::Utf8], Volatility::Immutable);

        // Sadly, without `udf_equals_hash!` macro, the equals and hash_value ignore state fields,
        // even though the struct implements `PartialEq` and `Hash`.
        let a: Box<dyn ScalarUDFImpl> = Box::new(StatefulFunctionWithEqHash {
            signature: signature.clone(),
            state: true,
        });
        let b: Box<dyn ScalarUDFImpl> = Box::new(StatefulFunctionWithEqHash {
            signature: signature.clone(),
            state: false,
        });
        assert!(a.equals(b.as_ref()));
        assert_eq!(a.hash_value(), b.hash_value());

        // With udf_equals_hash! macro, the equals and hash_value compare the state.
        // even though the struct implements `PartialEq` and `Hash`.
        let a: Box<dyn ScalarUDFImpl> =
            Box::new(StatefulFunctionWithEqHashWithUdfEqualsHash {
                signature: signature.clone(),
                state: true,
            });
        let b: Box<dyn ScalarUDFImpl> =
            Box::new(StatefulFunctionWithEqHashWithUdfEqualsHash {
                signature: signature.clone(),
                state: false,
            });
        assert!(!a.equals(b.as_ref()));
        // This could be true, but it's very unlikely that boolean true and false hash the same
        assert_ne!(a.hash_value(), b.hash_value());
    }
}
