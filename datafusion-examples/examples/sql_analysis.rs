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

//! This example shows how to use the structures that DataFusion provides to perform
//! Analysis on SQL queries and their plans.
//!
//! As a motivating example, we show how to count the number of JOINs in a query
//! as well as how many join tree's there are with their respective join count

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::{
    datasource::MemTable,
    execution::context::{SessionConfig, SessionContext},
};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_expr::LogicalPlan;
use test_utils::tpcds::tpcds_schemas;

/// Counts the total number of joins in a plan
fn total_join_count(plan: &LogicalPlan) -> usize {
    let mut total = 0;

    // We can use the TreeNode API to walk over a LogicalPlan.
    plan.apply(|node| {
        // if we encounter a join we update the running count
        if matches!(node, LogicalPlan::Join(_)) {
            total += 1;
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();

    total
}

/// Counts the total number of joins in a plan and collects every join tree in
/// the plan with their respective join count.
///
/// Join Tree Definition: the largest subtree consisting entirely of joins
///
/// For example, this plan:
///
/// ```text
///         JOIN
///         /  \
///       A   JOIN
///            /  \
///           B    C
/// ```
///
/// has a single join tree `(A-B-C)` which will result in `(2, [2])`
///
/// This plan:
///
/// ```text
///         JOIN
///         /  \
///       A   GROUP
///              |
///             JOIN
///             /  \
///            B    C
/// ```
///
/// Has two join trees `(A-, B-C)` which will result in `(2, [1, 1])`
fn count_trees(plan: &LogicalPlan) -> (usize, Vec<usize>) {
    // this works the same way as `total_count`, but now when we encounter a Join
    // we try to collect it's entire tree
    let mut to_visit = vec![plan];
    let mut total = 0;
    let mut groups = vec![];

    while let Some(node) = to_visit.pop() {
        // if we encounter a join, we know were at the root of the tree
        // count this tree and recurse on it's inputs
        if matches!(node, LogicalPlan::Join(_)) {
            let (group_count, inputs) = count_tree(node);
            total += group_count;
            groups.push(group_count);
            to_visit.extend(inputs);
        } else {
            to_visit.extend(node.inputs());
        }
    }

    (total, groups)
}

/// Count the entire join tree and return its inputs using TreeNode API
///
/// For example, if this function receives following plan:
///
/// ```text
///         JOIN
///         /  \
///       A   GROUP
///              |
///             JOIN
///             /  \
///            B    C
/// ```
///
/// It will return `(1, [A, GROUP])`
fn count_tree(join: &LogicalPlan) -> (usize, Vec<&LogicalPlan>) {
    let mut inputs = Vec::new();
    let mut total = 0;

    join.apply(|node| {
        // Some extra knowledge:
        //
        // optimized plans have their projections pushed down as far as
        // possible, which sometimes results in a projection going in between 2
        // subsequent joins giving the illusion these joins are not "related",
        // when in fact they are.
        //
        // This plan:
        //   JOIN
        //   /  \
        // A   PROJECTION
        //        |
        //       JOIN
        //       /  \
        //      B    C
        //
        // is the same as:
        //
        //   JOIN
        //   /  \
        // A   JOIN
        //     /  \
        //    B    C
        // we can continue the recursion in this case
        if let LogicalPlan::Projection(_) = node {
            return Ok(TreeNodeRecursion::Continue);
        }

        // any join we count
        if matches!(node, LogicalPlan::Join(_)) {
            total += 1;
            Ok(TreeNodeRecursion::Continue)
        } else {
            inputs.push(node);
            // skip children of input node
            Ok(TreeNodeRecursion::Jump)
        }
    })
    .unwrap();

    (total, inputs)
}

#[tokio::main]
async fn main() -> Result<()> {
    // To show how we can count the joins in a sql query we'll be using query 88
    // from the TPC-DS benchmark.
    //
    // q8 has many joins, cross-joins and multiple join-trees, perfect for our
    // example:

    let tpcds_query_88 = "
select  *
from
 (select count(*) h8_30_to_9
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk   
     and ss_hdemo_sk = household_demographics.hd_demo_sk 
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 8
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2) or
          (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
          (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2)) 
     and store.s_store_name = 'ese') s1,
 (select count(*) h9_to_9_30 
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk 
     and time_dim.t_hour = 9 
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2) or
          (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
          (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2))
     and store.s_store_name = 'ese') s2,
 (select count(*) h9_30_to_10 
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 9
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2) or
          (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
          (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2))
     and store.s_store_name = 'ese') s3,
 (select count(*) h10_to_10_30
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 10 
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2) or
          (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
          (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2))
     and store.s_store_name = 'ese') s4,
 (select count(*) h10_30_to_11
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 10 
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2) or
          (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
          (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2))
     and store.s_store_name = 'ese') s5,
 (select count(*) h11_to_11_30
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk 
     and time_dim.t_hour = 11
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2) or
          (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
          (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2))
     and store.s_store_name = 'ese') s6,
 (select count(*) h11_30_to_12
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 11
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2) or
          (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
          (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2))
     and store.s_store_name = 'ese') s7,
 (select count(*) h12_to_12_30
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 12
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2) or
          (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
          (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2))
     and store.s_store_name = 'ese') s8;";

    // first set up the config
    let config = SessionConfig::default();
    let ctx = SessionContext::new_with_config(config);

    // register the tables of the TPC-DS query
    let tables = tpcds_schemas();
    for table in tables {
        ctx.register_table(
            table.name,
            Arc::new(MemTable::try_new(Arc::new(table.schema.clone()), vec![])?),
        )?;
    }
    // We can create a LogicalPlan from a SQL query like this
    let logical_plan = ctx.sql(tpcds_query_88).await?.into_optimized_plan()?;

    println!(
        "Optimized Logical Plan:\n\n{}\n",
        logical_plan.display_indent()
    );
    // we can get the total count (query 88 has 31 joins: 7 CROSS joins and 24 INNER joins => 40 input relations)
    let total_join_count = total_join_count(&logical_plan);
    assert_eq!(31, total_join_count);

    println!("The plan has {total_join_count} joins.");

    // Furthermore the 24 inner joins are 8 groups of 3 joins with the 7
    // cross-joins combining them we can get these groups using the
    // `count_trees` method
    let (total_join_count, trees) = count_trees(&logical_plan);
    assert_eq!(
        (total_join_count, &trees),
        // query 88 is very straightforward, we know the cross-join group is at
        // the top of the plan followed by the INNER joins
        (31, &vec![7, 3, 3, 3, 3, 3, 3, 3, 3])
    );

    println!(
        "And following join-trees (number represents join amount in tree): {trees:?}"
    );

    Ok(())
}
