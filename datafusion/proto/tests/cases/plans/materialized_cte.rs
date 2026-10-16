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

//! `MaterializedCteExec` and the `MaterializedCteScanExec`s that share its
//! buffer.

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::physical_plan::materialized_cte::{
    MaterializedCteExec, MaterializedCteScanExec,
};
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, collect, displayable,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::Result;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_proto::bytes::{physical_plan_from_bytes, physical_plan_to_bytes};
use std::sync::Arc;

/// The body returns 100 random values, so a query that compares the rows of
/// two scans returns 100 only if the body runs one time.
const BODY: &str = "SELECT value + random() AS r FROM generate_series(1, 100)";

fn context() -> SessionContext {
    // sqlparser only parses `MATERIALIZED` for the PostgreSQL dialect.
    SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(4)
            .set_str("datafusion.sql_parser.dialect", "PostgreSQL"),
    )
}

/// The ids of the `MaterializedCteExec`s and of the scans in `plan`.
fn cte_ids(plan: &Arc<dyn ExecutionPlan>) -> Result<(Vec<u64>, Vec<u64>)> {
    let mut cte_ids = vec![];
    let mut scan_ids = vec![];
    plan.apply(|node| {
        if let Some(cte) = node.downcast_ref::<MaterializedCteExec>() {
            cte_ids.push(cte.buffer().id());
        } else if let Some(scan) = node.downcast_ref::<MaterializedCteScanExec>() {
            assert!(scan.is_bound());
            scan_ids.push(scan.id());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok((cte_ids, scan_ids))
}

fn buffered_rows(plan: &Arc<dyn ExecutionPlan>) -> Result<Vec<usize>> {
    let mut rows = vec![];
    plan.apply(|node| {
        if node.is::<MaterializedCteExec>() {
            let metrics = node.metrics().unwrap();
            let buffered = metrics.sum_by_name("buffered_rows").unwrap();
            rows.push(buffered.as_usize());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(rows)
}

/// Plan `sql`, which has one materialized CTE and returns one count. Then do
/// a serde round trip, run the decoded plan and return it with the count.
async fn roundtrip_and_count(sql: &str) -> Result<(Arc<dyn ExecutionPlan>, i64)> {
    let ctx = context();
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let bytes = physical_plan_to_bytes(Arc::clone(&plan))?;

    // Decode in a new session, as another process does.
    let ctx = context();
    let decoded = physical_plan_from_bytes(&bytes, ctx.task_ctx().as_ref())?;
    assert_eq!(
        displayable(plan.as_ref()).indent(true).to_string(),
        displayable(decoded.as_ref()).indent(true).to_string()
    );

    // The decoded plan has new ids, and all its scans read the decoded CTE.
    let (cte_ids, scan_ids) = cte_ids(&decoded)?;
    assert_eq!(cte_ids.len(), 1);
    assert_ne!(cte_ids, self::cte_ids(&plan)?.0);
    assert!(scan_ids.iter().all(|id| *id == cte_ids[0]), "{scan_ids:?}");
    // A second decode of the same bytes gives a separate CTE.
    let again = physical_plan_from_bytes(&bytes, ctx.task_ctx().as_ref())?;
    assert_ne!(cte_ids, self::cte_ids(&again)?.0);

    let batches = collect(Arc::clone(&decoded), ctx.task_ctx()).await?;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    let batch = batches.iter().find(|b| b.num_rows() == 1).unwrap();
    let count = batch.column(0).as_any().downcast_ref::<Int64Array>();
    let count = count.unwrap().value(0);
    // The body ran one time.
    assert_eq!(buffered_rows(&decoded)?, vec![100]);
    Ok((decoded, count))
}

#[tokio::test]
async fn roundtrip_materialized_cte_with_two_scans() -> Result<()> {
    let sql = format!(
        "WITH c AS MATERIALIZED ({BODY}) \
         SELECT count(*) FROM c AS a JOIN c AS b ON a.r = b.r"
    );
    let (decoded, count) = roundtrip_and_count(&sql).await?;
    assert_eq!(cte_ids(&decoded)?.1.len(), 2);
    assert_eq!(count, 100);
    Ok(())
}

/// The scan in the scalar subquery is not below the `MaterializedCteExec`.
#[tokio::test]
async fn roundtrip_materialized_cte_scan_in_scalar_subquery() -> Result<()> {
    let sql = format!(
        "WITH c AS MATERIALIZED ({BODY}) \
         SELECT count(*) FROM c WHERE r <= (SELECT max(r) FROM c)"
    );
    let (decoded, count) = roundtrip_and_count(&sql).await?;
    let plan = displayable(decoded.as_ref()).indent(true).to_string();
    assert!(plan.starts_with("ScalarSubqueryExec"), "{plan}");
    assert_eq!(cte_ids(&decoded)?.1.len(), 2);
    assert_eq!(count, 100);
    Ok(())
}

/// A plan fragment that has a scan but not its `MaterializedCteExec` can be
/// decoded. It fails when the scan is executed.
#[tokio::test]
async fn materialized_cte_scan_without_its_cte_fails_at_execution() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("r", DataType::Float64, true)]));
    let scan = Arc::new(MaterializedCteScanExec::new(7, "c", schema, 2));
    let bytes = physical_plan_to_bytes(scan)?;

    let ctx = SessionContext::new();
    let decoded = physical_plan_from_bytes(&bytes, ctx.task_ctx().as_ref())?;
    assert_eq!(decoded.output_partitioning().partition_count(), 2);
    let err = decoded.execute(0, ctx.task_ctx()).err().unwrap();
    assert_eq!(
        err.strip_backtrace(),
        "Execution error: The materialized CTE c has a MaterializedCteScanExec but no MaterializedCteExec in this plan. The body and the scans of a materialized CTE must be in one plan and run in one process"
    );
    Ok(())
}
