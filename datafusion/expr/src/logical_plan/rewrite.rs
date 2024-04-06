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

//! Methods for rewriting logical plans

use crate::{
    Aggregate, CrossJoin, Distinct, DistinctOn, EmptyRelation, Filter, Join, Limit,
    LogicalPlan, Prepare, Projection, RecursiveQuery, Repartition, Sort, Subquery,
    SubqueryAlias, Union, Unnest, UserDefinedLogicalNode, Window,
};
use datafusion_common::tree_node::{Transformed, TreeNodeIterator};
use datafusion_common::{DFSchema, DFSchemaRef, Result};
use std::sync::{Arc, OnceLock};

/// A temporary node that is left in place while rewriting the children of a
/// [`LogicalPlan`]. This is necessary to ensure that the `LogicalPlan` is
/// always in a valid state (from the Rust perspective)
static PLACEHOLDER: OnceLock<Arc<LogicalPlan>> = OnceLock::new();

/// its inputs, so this code would not be needed. However, for now we try and
/// unwrap the `Arc` which avoids `clone`ing in most cases.
///
/// On error, node be left with a placeholder logical plan
fn rewrite_arc<F>(
    node: &mut Arc<LogicalPlan>,
    mut f: F,
) -> datafusion_common::Result<Transformed<&mut Arc<LogicalPlan>>>
where
    F: FnMut(LogicalPlan) -> Result<Transformed<LogicalPlan>>,
{
    // We need to leave a valid node in the Arc, while we rewrite the existing
    // one, so use a single global static placeholder node
    let mut new_node = PLACEHOLDER
        .get_or_init(|| {
            Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: DFSchemaRef::new(DFSchema::empty()),
            }))
        })
        .clone();

    // take the old value out of the Arc
    std::mem::swap(node, &mut new_node);

    // try to update existing node, if it isn't shared with others
    let new_node = Arc::try_unwrap(new_node)
        // if None is returned, there is another reference to this
        // LogicalPlan, so we must clone instead
        .unwrap_or_else(|node| node.as_ref().clone());

    // apply the actual transform
    let result = f(new_node)?;

    // put the new value back into the Arc
    let mut new_node = Arc::new(result.data);
    std::mem::swap(node, &mut new_node);

    // return the `node` back
    Ok(Transformed::new(node, result.transformed, result.tnr))
}

/// Rewrite the arc and discard the contents of Transformed
fn rewrite_arc_no_data<F>(
    node: &mut Arc<LogicalPlan>,
    f: F,
) -> datafusion_common::Result<Transformed<()>>
where
    F: FnMut(LogicalPlan) -> Result<Transformed<LogicalPlan>>,
{
    rewrite_arc(node, f).map(|res| res.discard_data())
}

/// Rewrites all inputs for an Extension node "in place"
/// (it currently has to copy values because there are no APIs for in place modification)
///
/// Should be removed when we have an API for in place modifications of the
/// extension to avoid these copies
fn rewrite_extension_inputs<F>(
    node: &mut Arc<dyn UserDefinedLogicalNode>,
    f: F,
) -> datafusion_common::Result<Transformed<()>>
where
    F: FnMut(LogicalPlan) -> Result<Transformed<LogicalPlan>>,
{
    let Transformed {
        data: new_inputs,
        transformed,
        tnr,
    } = node
        .inputs()
        .into_iter()
        .cloned()
        .map_until_stop_and_collect(f)?;

    let exprs = node.expressions();
    let mut new_node = node.from_template(&exprs, &new_inputs);
    std::mem::swap(node, &mut new_node);
    Ok(Transformed {
        data: (),
        transformed,
        tnr,
    })
}

impl LogicalPlan {
    /// Applies `f` to each child (input) of this plan node, rewriting them *in place.*
    ///
    /// Note that this function returns `Transformed<()>` because it does not
    /// consume `self`, but instead modifies it in place. However, `F` transforms
    /// the children by ownership
    ///
    /// # Notes
    ///
    /// Inputs include ONLY direct children, not embedded subquery
    /// `LogicalPlan`s, for example such as are in [`Expr::Exists`].
    ///
    /// [`Expr::Exists`]: crate::expr::Expr::Exists
    pub(crate) fn rewrite_children<F>(&mut self, mut f: F) -> Result<Transformed<()>>
    where
        F: FnMut(Self) -> Result<Transformed<Self>>,
    {
        let children_result = match self {
            LogicalPlan::Projection(Projection { input, .. }) => {
                rewrite_arc_no_data(input, &mut f)
            }
            LogicalPlan::Filter(Filter { input, .. }) => {
                rewrite_arc_no_data(input, &mut f)
            }
            LogicalPlan::Repartition(Repartition { input, .. }) => {
                rewrite_arc_no_data(input, &mut f)
            }
            LogicalPlan::Window(Window { input, .. }) => {
                rewrite_arc_no_data(input, &mut f)
            }
            LogicalPlan::Aggregate(Aggregate { input, .. }) => {
                rewrite_arc_no_data(input, &mut f)
            }
            LogicalPlan::Sort(Sort { input, .. }) => rewrite_arc_no_data(input, &mut f),
            LogicalPlan::Join(Join { left, right, .. }) => {
                let results = [left, right]
                    .into_iter()
                    .map_until_stop_and_collect(|input| rewrite_arc(input, &mut f))?;
                Ok(results.discard_data())
            }
            LogicalPlan::CrossJoin(CrossJoin { left, right, .. }) => {
                let results = [left, right]
                    .into_iter()
                    .map_until_stop_and_collect(|input| rewrite_arc(input, &mut f))?;
                Ok(results.discard_data())
            }
            LogicalPlan::Limit(Limit { input, .. }) => rewrite_arc_no_data(input, &mut f),
            LogicalPlan::Subquery(Subquery { subquery, .. }) => {
                rewrite_arc_no_data(subquery, &mut f)
            }
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) => {
                rewrite_arc_no_data(input, &mut f)
            }
            LogicalPlan::Extension(extension) => {
                rewrite_extension_inputs(&mut extension.node, &mut f)
            }
            LogicalPlan::Union(Union { inputs, .. }) => {
                let results = inputs
                    .iter_mut()
                    .map_until_stop_and_collect(|input| rewrite_arc(input, &mut f))?;
                Ok(results.discard_data())
            }
            LogicalPlan::Distinct(
                Distinct::All(input) | Distinct::On(DistinctOn { input, .. }),
            ) => rewrite_arc_no_data(input, &mut f),
            LogicalPlan::Explain(explain) => {
                rewrite_arc_no_data(&mut explain.plan, &mut f)
            }
            LogicalPlan::Analyze(analyze) => {
                rewrite_arc_no_data(&mut analyze.input, &mut f)
            }
            LogicalPlan::Dml(write) => rewrite_arc_no_data(&mut write.input, &mut f),
            LogicalPlan::Copy(copy) => rewrite_arc_no_data(&mut copy.input, &mut f),
            LogicalPlan::Ddl(ddl) => {
                if let Some(input) = ddl.input_mut() {
                    rewrite_arc_no_data(input, &mut f)
                } else {
                    Ok(Transformed::no(()))
                }
            }
            LogicalPlan::Unnest(Unnest { input, .. }) => {
                rewrite_arc_no_data(input, &mut f)
            }
            LogicalPlan::Prepare(Prepare { input, .. }) => {
                rewrite_arc_no_data(input, &mut f)
            }
            LogicalPlan::RecursiveQuery(RecursiveQuery {
                static_term,
                recursive_term,
                ..
            }) => {
                let results = [static_term, recursive_term]
                    .into_iter()
                    .map_until_stop_and_collect(|input| rewrite_arc(input, &mut f))?;
                Ok(results.discard_data())
            }
            // plans without inputs
            LogicalPlan::TableScan { .. }
            | LogicalPlan::Statement { .. }
            | LogicalPlan::EmptyRelation { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::DescribeTable(_) => Ok(Transformed::no(())),
        }?;

        // after visiting the actual children we we need to visit any subqueries
        // that are inside the expressions
        // TODO use pattern introduced in https://github.com/apache/arrow-datafusion/pull/9913
        Ok(children_result)
    }
}
