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

use std::{any::Any, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    error::Result,
    prelude::{SessionConfig, SessionContext},
};
use datafusion_catalog::{Session, TableProvider};
use datafusion_expr::{dml::InsertOp, Expr, TableType};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_plan::{
    execution_plan::{Boundedness, EmissionType},
    DisplayAs, ExecutionPlan, PlanProperties,
};

#[tokio::test]
async fn insert_operation_is_passed_correctly_to_table_provider() {
    // Use the SQLite syntax so we can test the "INSERT OR REPLACE INTO" syntax
    let ctx = session_ctx_with_dialect("SQLite");
    let table_provider = Arc::new(TestInsertTableProvider::new());
    ctx.register_table("testing", table_provider.clone())
        .unwrap();

    let sql = "INSERT INTO testing (column) VALUES (1)";
    assert_insert_op(&ctx, sql, InsertOp::Append).await;

    let sql = "INSERT OVERWRITE testing (column) VALUES (1)";
    assert_insert_op(&ctx, sql, InsertOp::Overwrite).await;

    let sql = "REPLACE INTO testing (column) VALUES (1)";
    assert_insert_op(&ctx, sql, InsertOp::Replace).await;

    let sql = "INSERT OR REPLACE INTO testing (column) VALUES (1)";
    assert_insert_op(&ctx, sql, InsertOp::Replace).await;
}

async fn assert_insert_op(ctx: &SessionContext, sql: &str, insert_op: InsertOp) {
    let df = ctx.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let exec = plan.as_any().downcast_ref::<TestInsertExec>().unwrap();
    assert_eq!(exec.op, insert_op);
}

fn session_ctx_with_dialect(dialect: impl Into<String>) -> SessionContext {
    let mut config = SessionConfig::new();
    let options = config.options_mut();
    options.sql_parser.dialect = dialect.into();
    SessionContext::new_with_config(config)
}

#[derive(Debug)]
struct TestInsertTableProvider {
    schema: SchemaRef,
}

impl TestInsertTableProvider {
    fn new() -> Self {
        Self {
            schema: SchemaRef::new(Schema::new(vec![Field::new(
                "column",
                DataType::Int64,
                false,
            )])),
        }
    }
}

#[async_trait]
impl TableProvider for TestInsertTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!("TestInsertTableProvider is a stub for testing.")
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(TestInsertExec::new(insert_op)))
    }
}

#[derive(Debug)]
struct TestInsertExec {
    op: InsertOp,
    plan_properties: PlanProperties,
}

impl TestInsertExec {
    fn new(op: InsertOp) -> Self {
        Self {
            op,
            plan_properties: PlanProperties::new(
                EquivalenceProperties::new(make_count_schema()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
        }
    }
}

impl DisplayAs for TestInsertExec {
    fn fmt_as(
        &self,
        _t: datafusion_physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "TestInsertExec")
    }
}

impl ExecutionPlan for TestInsertExec {
    fn name(&self) -> &str {
        "TestInsertExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert!(children.is_empty());
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion_execution::TaskContext>,
    ) -> Result<datafusion_execution::SendableRecordBatchStream> {
        unimplemented!("TestInsertExec is a stub for testing.")
    }
}

fn make_count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}
