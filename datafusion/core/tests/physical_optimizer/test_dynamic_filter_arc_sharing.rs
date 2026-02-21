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

//! Test to debug Arc sharing for dynamic filters

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use datafusion_common::config::ConfigOptions;
use datafusion_common::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::filter_pushdown::FilterPushdown;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use std::sync::Arc;

fn print_arc_addresses(plan: &Arc<dyn ExecutionPlan>, depth: usize, label: &str) {
    let indent = "  ".repeat(depth);
    println!("{}{} - {}", indent, plan.name(), label);

    if let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() {
        if let Some(filter) = hash_join.dynamic_filter_for_test() {
            let ptr = Arc::as_ptr(filter) as *const () as u64;
            let strong_count = Arc::strong_count(filter);
            println!("{}  -> HashJoin dynamic_filter: 0x{:x} (strong_count={})", indent, ptr, strong_count);
        }
    }

    if let Some(data_source) = plan.as_any().downcast_ref::<DataSourceExec>() {
        if let Some(filter) = data_source.filter_for_test() {
            let ptr = Arc::as_ptr(&filter) as *const () as u64;
            let strong_count = Arc::strong_count(&filter);

            if filter.as_any().is::<DynamicFilterPhysicalExpr>() {
                println!("{}  -> DataSource filter (DynamicFilter): 0x{:x} (strong_count={})", indent, ptr, strong_count);
            } else {
                println!("{}  -> DataSource filter (other): 0x{:x} (strong_count={})", indent, ptr, strong_count);
            }
        }
    }

    for child in plan.children() {
        print_arc_addresses(child, depth + 1, label);
    }
}

#[tokio::test]
async fn test_dynamic_filter_arc_sharing() -> Result<()> {
    // Create context with dynamic filter pushdown enabled
    let mut config = ConfigOptions::default();
    config.optimizer.enable_dynamic_filter_pushdown = true;

    let ctx = SessionContext::new_with_config(config.into());

    // Create build_table: (id, value)
    let build_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![100, 200])),
        ],
    )?;
    ctx.register_batch("build_table", build_batch)?;

    // Create probe_table: (id, data)
    let probe_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("data", DataType::Int32, false),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
        ],
    )?;
    ctx.register_batch("probe_table", probe_batch)?;

    // Create join query
    let df = ctx.sql("SELECT * FROM build_table b JOIN probe_table p ON b.id = p.id").await?;
    let (state, logical_plan) = df.into_parts();

    // Create physical plan
    let physical_plan = state.create_physical_plan(&logical_plan).await?;

    println!("\n=== BEFORE FILTER PUSHDOWN ===");
    print_arc_addresses(&physical_plan, 0, "before");

    // Apply FilterPushdown optimizer
    let filter_pushdown = FilterPushdown::new_post_optimization();
    let optimized_plan = filter_pushdown.optimize(physical_plan, state.config().options())?;

    println!("\n=== AFTER FILTER PUSHDOWN ===");
    print_arc_addresses(&optimized_plan, 0, "after");

    // Check if Arc addresses match
    let mut hash_join_filter_addr: Option<u64> = None;
    let mut data_source_filter_addrs: Vec<u64> = Vec::new();

    fn collect_addresses(
        plan: &Arc<dyn ExecutionPlan>,
        hash_join_addr: &mut Option<u64>,
        data_source_addrs: &mut Vec<u64>,
    ) {
        if let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() {
            if let Some(filter) = hash_join.dynamic_filter_for_test() {
                *hash_join_addr = Some(Arc::as_ptr(filter) as *const () as u64);
            }
        }

        if let Some(data_source) = plan.as_any().downcast_ref::<DataSourceExec>() {
            if let Some(filter) = data_source.filter_for_test() {
                if filter.as_any().is::<DynamicFilterPhysicalExpr>() {
                    data_source_addrs.push(Arc::as_ptr(&filter) as *const () as u64);
                }
            }
        }

        for child in plan.children() {
            collect_addresses(child, hash_join_addr, data_source_addrs);
        }
    }

    collect_addresses(&optimized_plan, &mut hash_join_filter_addr, &mut data_source_filter_addrs);

    println!("\n=== ARC ADDRESS COMPARISON ===");
    if let Some(hj_addr) = hash_join_filter_addr {
        println!("HashJoinExec filter: 0x{:x}", hj_addr);

        for (i, ds_addr) in data_source_filter_addrs.iter().enumerate() {
            println!("DataSourceExec[{}] filter: 0x{:x}", i, ds_addr);

            if hj_addr == *ds_addr {
                println!("  ✓ SHARED - Same Arc instance!");
            } else {
                println!("  ✗ NOT SHARED - Different Arc instances!");
            }
        }
    }

    if hash_join_filter_addr.is_some() && !data_source_filter_addrs.is_empty() {
        assert_eq!(
            hash_join_filter_addr.unwrap(),
            data_source_filter_addrs[0],
            "Dynamic filter Arc should be shared between HashJoinExec and DataSourceExec!"
        );
    }

    Ok(())
}
