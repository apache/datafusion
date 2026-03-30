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
//! Example demonstrating filter pushdown using [`LiteralGuarantee`] with a
//! custom index.
//!
//! A [`TableProvider`] with a `BTreeMap` index on `bank_account` uses
//! [`LiteralGuarantee::analyze`] to extract equality / IN-list constraints
//! from pushed-down filter predicates and resolves them via the index,
//! returning only the matching rows.

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Debug, Formatter};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::{UInt8Builder, UInt64Builder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::planner::logical2physical;
use datafusion::physical_expr::utils::Guarantee;
use datafusion::physical_expr::utils::LiteralGuarantee;
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

pub async fn literal_guarantee_filter_pushdown() -> Result<()> {
    let db = CustomDataSource::default();
    db.populate_users();

    let ctx = SessionContext::new();
    ctx.register_table("accounts", Arc::new(db))?;

    // No filter — returns all 3 rows.
    let all = ctx.sql("SELECT * FROM accounts").await?.collect().await?;
    assert_eq!(all.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    println!("All rows:");
    for batch in &all {
        println!("{batch:?}");
    }

    // Equality filter — the index resolves exactly one row.
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

    // IN-list filter — the index resolves two rows.
    let two = ctx
        .sql("SELECT * FROM accounts WHERE bank_account IN (100, 1000)")
        .await?
        .collect()
        .await?;
    assert_eq!(two.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    println!("\nbank_account IN (100, 1000):");
    for batch in &two {
        println!("{batch:?}");
    }

    // NOT-IN filter — the index excludes one row, returning the other two.
    let not_in = ctx
        .sql("SELECT * FROM accounts WHERE bank_account != 9000")
        .await?
        .collect()
        .await?;
    assert_eq!(not_in.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    println!("\nbank_account != 9000:");
    for batch in &not_in {
        println!("{batch:?}");
    }

    // Inexact pushdown — the filter references bank_account (indexed) AND id
    // (not indexed). The index narrows the scan to bank_account IN (100, 1000),
    // but DataFusion must re-check "id > 2" since we can't handle that part.
    let inexact = ctx
        .sql("SELECT * FROM accounts WHERE bank_account IN (100, 1000) AND id > 2")
        .await?
        .collect()
        .await?;
    assert_eq!(inexact.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    println!("\nbank_account IN (100, 1000) AND id > 2 (inexact pushdown):");
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

/// A custom datasource with a `BTreeMap` index on `bank_account`
#[derive(Clone)]
struct CustomDataSource {
    inner: Arc<Mutex<CustomDataSourceInner>>,
}

struct CustomDataSourceInner {
    data: HashMap<u8, User>,
    bank_account_index: BTreeMap<u64, u8>,
}

impl Debug for CustomDataSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("custom_db")
    }
}

impl Default for CustomDataSource {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(CustomDataSourceInner {
                data: Default::default(),
                bank_account_index: Default::default(),
            })),
        }
    }
}

impl CustomDataSource {
    fn populate_users(&self) {
        for user in [
            User {
                id: 1,
                bank_account: 9_000,
            },
            User {
                id: 2,
                bank_account: 100,
            },
            User {
                id: 3,
                bank_account: 1_000,
            },
        ] {
            let mut inner = self.inner.lock().unwrap();
            inner.bank_account_index.insert(user.bank_account, user.id);
            inner.data.insert(user.id, user);
        }
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt8, false),
        Field::new("bank_account", DataType::UInt64, false),
    ]))
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
        Ok(filters
            .iter()
            .map(|f| {
                let columns = f.column_refs();
                let has_bank_account = columns.iter().any(|c| c.name == "bank_account");
                let all_bank_account =
                    has_bank_account && columns.iter().all(|c| c.name == "bank_account");
                if all_bank_account {
                    // Only bank_account — fully handled via the index.
                    TableProviderFilterPushDown::Exact
                } else if has_bank_account {
                    // Mixed columns — index narrows the scan, but DataFusion
                    // must re-check the parts we can't handle.
                    TableProviderFilterPushDown::Inexact
                } else {
                    // No indexed columns — we can't help with this filter.
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Convert the logical filter expressions into a single physical
        // expression so we can feed it to LiteralGuarantee::analyze.
        let filter = conjunction(filters.to_vec()).and_then(|expr| {
            let physical_expr = logical2physical(&expr, self.schema().as_ref());
            let guarantees = LiteralGuarantee::analyze(&physical_expr);

            // Look for a guarantee on the `bank_account` column.
            let guarantee = guarantees
                .iter()
                .find(|g| g.column.name() == "bank_account")?;

            // Resolve the literal values to user ids via the index.
            let inner = self.inner.lock().unwrap();
            let ids: Vec<u8> = guarantee
                .literals
                .iter()
                .filter_map(|v| {
                    if let ScalarValue::UInt64(Some(val)) = v {
                        inner.bank_account_index.get(val).copied()
                    } else {
                        None
                    }
                })
                .collect();
            Some(IndexFilter {
                ids,
                guarantee: guarantee.guarantee,
            })
        });

        Ok(Arc::new(CustomExec::new(self.clone(), projection, filter)))
    }
}

/// Result of resolving a `LiteralGuarantee` against the index.
#[derive(Debug, Clone)]
struct IndexFilter {
    ids: Vec<u8>,
    guarantee: Guarantee,
}

#[derive(Debug, Clone)]
struct CustomExec {
    db: CustomDataSource,
    projected_schema: SchemaRef,
    /// When Some, filters users by including (In) or excluding (NotIn) IDs.
    filter: Option<IndexFilter>,
    cache: Arc<PlanProperties>,
}

impl CustomExec {
    fn new(
        db: CustomDataSource,
        projections: Option<&Vec<usize>>,
        filter: Option<IndexFilter>,
    ) -> Self {
        let projected_schema =
            datafusion::physical_plan::project_schema(&schema(), projections).unwrap();
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projected_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            db,
            projected_schema,
            filter,
            cache,
        }
    }
}

impl DisplayAs for CustomExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "CustomExec(filter={:?})", self.filter)
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
        let inner = self.db.inner.lock().unwrap();

        // Collect the users to return based on the index filter.
        let users: Vec<&User> = match &self.filter {
            Some(IndexFilter {
                ids,
                guarantee: Guarantee::In,
            }) => ids.iter().filter_map(|id| inner.data.get(id)).collect(),
            Some(IndexFilter {
                ids,
                guarantee: Guarantee::NotIn,
            }) => inner
                .data
                .values()
                .filter(|u| !ids.contains(&u.id))
                .collect(),
            None => inner.data.values().collect(),
        };

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
