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

//! Downstream feature-contract test, run by check_object_store_features.py.

use async_trait::async_trait;
use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{Result, assert_batches_eq, exec_err};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::prelude::SessionContext;
use datafusion_catalog::{Session, TableProvider};
use datafusion_execution::TaskContext;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::memory::MemoryStream;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use std::fmt;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Debug)]
struct NativeTable {
    schema: SchemaRef,
    files: Vec<PathBuf>,
    reads: Arc<AtomicUsize>,
}

#[async_trait]
impl TableProvider for NativeTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&[usize]>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert!(
            filters.is_empty(),
            "filters must stay in DataFusion unless pushdown is supported"
        );
        let schema = match projection {
            Some(indices) => Arc::new(self.schema.project(indices)?),
            None => Arc::clone(&self.schema),
        };
        Ok(Arc::new(NativeScan {
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(self.files.len()),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
            source_schema: Arc::clone(&self.schema),
            projection: projection.map(<[usize]>::to_vec),
            files: self.files.clone(),
            reads: Arc::clone(&self.reads),
        }))
    }
}

#[derive(Debug)]
struct NativeScan {
    properties: Arc<PlanProperties>,
    source_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    files: Vec<PathBuf>,
    reads: Arc<AtomicUsize>,
}
impl DisplayAs for NativeScan {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "NativeScan: partitions={}", self.files.len())
    }
}
impl ExecutionPlan for NativeScan {
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        )
            -> Result<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }
    fn name(&self) -> &str {
        "NativeScan"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return exec_err!("NativeScan is a leaf");
        }
        Ok(self)
    }
    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // The downstream owns file access and decoding. No DataFusion file APIs.
        let bytes = std::fs::read(
            self.files
                .get(partition)
                .ok_or_else(|| std::io::Error::other("invalid partition"))?,
        )?;
        if bytes.len() % 24 != 0 {
            return exec_err!("invalid native record length");
        }
        let mut columns = [vec![], vec![], vec![]];
        for record in bytes.chunks_exact(24) {
            for (column, field) in columns.iter_mut().zip(record.chunks_exact(8)) {
                column.push(i64::from_le_bytes(field.try_into().unwrap()));
            }
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&self.source_schema),
            columns
                .into_iter()
                .map(|c| Arc::new(Int64Array::from(c)) as _)
                .collect(),
        )?;
        let batch = match &self.projection {
            Some(p) => batch.project(p)?,
            None => batch,
        };
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(MemoryStream::try_new(
            vec![batch],
            self.schema(),
            None,
        )?))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let temp = temp_dir.path();
    let rows = [
        vec![[1_i64, 10, 5], [2, 10, 20], [3, 20, 30]],
        vec![[4, 10, 40], [5, 20, 50], [6, 30, 60]],
    ];
    let files: Vec<_> = rows
        .iter()
        .enumerate()
        .map(|(i, rows)| {
            let path = temp.join(format!("{i}.native"));
            let bytes: Vec<_> = rows
                .iter()
                .flatten()
                .flat_map(|v| v.to_le_bytes())
                .collect();
            std::fs::write(&path, bytes).unwrap();
            path
        })
        .collect();
    let empty_schema = Schema::empty();
    datafusion::test_util::scan_empty(Some("empty"), &empty_schema, None)?;
    let reads = Arc::new(AtomicUsize::new(0));
    let ctx = SessionContext::new();
    ctx.register_table(
        "native",
        Arc::new(NativeTable {
            schema: Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("grp", DataType::Int64, false),
                Field::new("value", DataType::Int64, false),
            ])),
            files,
            reads: Arc::clone(&reads),
        }),
    )?;
    let df = ctx.sql("SELECT n.grp, SUM(n.value * w.factor) AS total FROM native n JOIN (VALUES (10, 2), (20, 3)) AS w(grp, factor) ON n.grp = w.grp WHERE n.value >= 20 GROUP BY n.grp ORDER BY total DESC LIMIT 2").await?;
    let plan = df.create_physical_plan().await?;
    let display = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(
        display.contains("NativeScan"),
        "custom execution plan must be used: {display}"
    );
    println!("{display}");
    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
    assert_batches_eq!(
        [
            "+-----+-------+",
            "| grp | total |",
            "+-----+-------+",
            "| 20  | 240   |",
            "| 10  | 120   |",
            "+-----+-------+"
        ],
        &batches
    );
    assert_eq!(reads.load(Ordering::SeqCst), 2);
    let projection = ctx
        .sql("SELECT id FROM native WHERE value = 50")
        .await?
        .collect()
        .await?;
    assert_batches_eq!(
        ["+----+", "| id |", "+----+", "| 5  |", "+----+"],
        &projection
    );
    assert_eq!(reads.load(Ordering::SeqCst), 4);
    #[cfg(not(feature = "storage"))]
    {
        let err = ctx
            .sql("COPY native TO 'disabled.csv' STORED AS CSV")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("object_store feature"), "{err}");
        assert!(
            ctx.runtime_env()
                .config_entries()
                .iter()
                .all(|entry| !entry.key.contains("cache"))
        );
        let err = ctx
            .sql("SET datafusion.runtime.metadata_cache_limit = '1M'")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("metadata_cache_limit"));
        println!(
            "PASS: file operations fail explicitly; unavailable cache settings are not advertised"
        );
    }
    #[cfg(feature = "storage")]
    {
        use datafusion::prelude::CsvReadOptions;
        let csv = temp.join("builtin.csv");
        std::fs::write(&csv, "id,value\n1,20\n2,40\n")?;
        let df = ctx
            .read_csv(csv.to_str().unwrap(), CsvReadOptions::new())
            .await?;
        let batches = df.collect().await?;
        assert_batches_eq!(
            [
                "+----+-------+",
                "| id | value |",
                "+----+-------+",
                "| 1  | 20    |",
                "| 2  | 40    |",
                "+----+-------+"
            ],
            &batches
        );
        let url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
        assert!(ctx.runtime_env().object_store(url).is_ok());
        assert!(
            ctx.runtime_env()
                .config_entries()
                .iter()
                .any(|entry| entry.key.contains("metadata_cache_limit"))
        );
        println!("PASS: CSV read, local store registry, cache settings");
    }
    ctx.deregister_table("native")?;
    println!(
        "PASS: native file IO, custom TableProvider and ExecutionPlan, 2 partitions, filter, projection, join, aggregation, sort, limit, deregistration"
    );
    Ok(())
}
