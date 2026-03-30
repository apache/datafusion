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

//! See `main.rs` for how to run it.
//!
//! Example demonstrating filter pushdown using [`PruningPredicate`] to skip
//! entire partitions based on min/max statistics.
//!
//! A [`TableProvider`] with multiple in-memory partitions uses
//! [`PruningPredicate::prune`] to determine which partitions could contain
//! matching rows and only scans those.

use std::any::Any;
use std::collections::HashSet;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, UInt8Builder, UInt64Array, UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::pruning::PruningStatistics;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::planner::logical2physical;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use datafusion_expr::TableProviderFilterPushDown;
use datafusion_expr::utils::conjunction;

pub async fn pruning_predicate_filter_pushdown() -> Result<()> {
    let db = CustomDataSource::new(vec![
        Partition::new(
            vec![
                User {
                    id: 1,
                    bank_account: 100,
                },
                User {
                    id: 2,
                    bank_account: 500,
                },
            ],
            100,
            500,
        ),
        Partition::new(
            vec![
                User {
                    id: 3,
                    bank_account: 1_000,
                },
                User {
                    id: 4,
                    bank_account: 4_000,
                },
            ],
            1_000,
            4_000,
        ),
        Partition::new(
            vec![
                User {
                    id: 5,
                    bank_account: 5_000,
                },
                User {
                    id: 6,
                    bank_account: 9_000,
                },
            ],
            5_000,
            9_000,
        ),
    ]);

    let ctx = SessionContext::new();
    ctx.register_table("accounts", Arc::new(db))?;

    // No filter: all 3 partitions scanned, 6 rows returned.
    let all = ctx.sql("SELECT * FROM accounts").await?.collect().await?;
    assert_eq!(all.iter().map(|b| b.num_rows()).sum::<usize>(), 6);
    println!("All rows:");
    for batch in &all {
        println!("{batch:?}");
    }

    // Equality filter: pruning skips partitions 0 and 1 because 9000 is
    // outside their ranges. Partition 2 (5000–9000) is scanned, but it also
    // contains bank_account=5000 which doesn't match. Because our pushdown
    // is Inexact, DataFusion re-checks the filter and removes that row.
    let one = ctx
        .sql("SELECT * FROM accounts WHERE bank_account = 9000")
        .await?
        .collect()
        .await?;
    assert_eq!(one.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    println!("\nbank_account = 9000:");
    for batch in &one {
        println!("{batch:?}");
    }

    // Range filter: only partition 1 (1000–4000) needs scanning.
    let range = ctx
        .sql("SELECT * FROM accounts WHERE bank_account >= 1000 AND bank_account <= 4000")
        .await?
        .collect()
        .await?;
    assert_eq!(range.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    println!("\nbank_account >= 1000 AND bank_account <= 4000:");
    for batch in &range {
        println!("{batch:?}");
    }

    // Inexact pushdown: pruning keeps partitions 1 and 2 because
    // both overlap with the range > 500, skipping only partition 0 (100–500).
    // But within the kept partitions, DataFusion re-checks and removes rows
    // that don't satisfy bank_account > 4000 (e.g. bank_account=1000 from
    // partition 1).
    let inexact = ctx
        .sql("SELECT * FROM accounts WHERE bank_account > 4000")
        .await?
        .collect()
        .await?;
    assert_eq!(inexact.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    println!("\nbank_account > 4000 (inexact: partition 1 scanned but no rows match):");
    for batch in &inexact {
        println!("{batch:?}");
    }

    Ok(())
}

/// A User, with an id and a bank account
#[derive(Clone, Debug)]
struct User {
    id: u8,
    bank_account: u64,
}

/// A partition of users with known min/max bank_account statistics
#[derive(Clone, Debug)]
struct Partition {
    users: Vec<User>,
    min_account: u64,
    max_account: u64,
}

impl Partition {
    fn new(users: Vec<User>, min_account: u64, max_account: u64) -> Self {
        Self {
            users,
            min_account,
            max_account,
        }
    }
}

/// A custom datasource with partitions that have known min/max statistics
#[derive(Clone, Debug)]
struct CustomDataSource {
    partitions: Vec<Partition>,
}

impl CustomDataSource {
    fn new(partitions: Vec<Partition>) -> Self {
        Self { partitions }
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt8, false),
        Field::new("bank_account", DataType::UInt64, false),
    ]))
}

/// Provides min/max statistics for each partition so [`PruningPredicate`]
/// can decide which partitions to skip.
struct PartitionStats<'a> {
    partitions: &'a [Partition],
}

impl PruningStatistics for PartitionStats<'_> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        if column.name == "bank_account" {
            let mins: Vec<u64> = self.partitions.iter().map(|p| p.min_account).collect();
            Some(Arc::new(UInt64Array::from(mins)))
        } else {
            None
        }
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        if column.name == "bank_account" {
            let maxes: Vec<u64> = self.partitions.iter().map(|p| p.max_account).collect();
            Some(Arc::new(UInt64Array::from(maxes)))
        } else {
            None
        }
    }

    fn num_containers(&self) -> usize {
        self.partitions.len()
    }

    fn null_counts(&self, _column: &Column) -> Option<ArrayRef> {
        None
    }

    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        None
    }

    fn contained(
        &self,
        _column: &Column,
        _values: &HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        None
    }
}

#[async_trait]
impl TableProvider for CustomDataSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // Pruning narrows which partitions to scan but doesn't guarantee
        // every row in a scanned partition matches, so Inexact is correct.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Use PruningPredicate to determine which partitions to scan.
        let partitions_to_scan = if let Some(expr) = conjunction(filters.to_vec()) {
            let physical_expr = logical2physical(&expr, self.schema().as_ref());
            let predicate = PruningPredicate::try_new(physical_expr, self.schema())?;
            let keep = predicate.prune(&PartitionStats {
                partitions: &self.partitions,
            })?;
            self.partitions
                .iter()
                .zip(keep)
                .filter(|(_, keep)| *keep)
                .map(|(p, _)| p.clone())
                .collect()
        } else {
            self.partitions.clone()
        };

        Ok(Arc::new(CustomExec::new(partitions_to_scan, projection)))
    }
}

#[derive(Debug, Clone)]
struct CustomExec {
    partitions: Vec<Partition>,
    projected_schema: SchemaRef,
    cache: Arc<PlanProperties>,
}

impl CustomExec {
    fn new(partitions: Vec<Partition>, projections: Option<&Vec<usize>>) -> Self {
        let projected_schema =
            datafusion::physical_plan::project_schema(&schema(), projections).unwrap();
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projected_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            partitions,
            projected_schema,
            cache,
        }
    }
}

impl DisplayAs for CustomExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "CustomExec(partitions={})", self.partitions.len())
    }
}

impl ExecutionPlan for CustomExec {
    fn name(&self) -> &'static str {
        "CustomExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &dyn datafusion::physical_plan::PhysicalExpr,
        ) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let users: Vec<&User> = self.partitions.iter().flat_map(|p| &p.users).collect();

        let mut id_array = UInt8Builder::with_capacity(users.len());
        let mut account_array = UInt64Builder::with_capacity(users.len());
        for user in &users {
            id_array.append_value(user.id);
            account_array.append_value(user.bank_account);
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&self.projected_schema),
            vec![
                Arc::new(id_array.finish()),
                Arc::new(account_array.finish()),
            ],
        )?;

        Ok(Box::pin(MemoryStream::try_new(
            vec![batch],
            self.schema(),
            None,
        )?))
    }
}
