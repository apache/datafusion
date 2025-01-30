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

use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{ArrayRef, StringArray, UInt64Array};
use async_trait::async_trait;
use datafusion::arrow::array::{UInt64Builder, UInt8Builder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};

use datafusion::prelude::*;

use datafusion::catalog::Session;
use datafusion_common::{extract_field_index, FieldIndex};
use itertools::Itertools;
use tokio::time::timeout;

/// This example demonstrates executing a simple query against a custom datasource
#[tokio::main]
async fn main() -> Result<()> {
    // create our custom datasource and adding some users
    let db = CustomDataSource::default();
    db.populate_users();

    search_accounts(db.clone(), "select * from accounts", 3).await?;
    search_accounts(
        db.clone(),
        "select _rowid, _file, * from accounts where _rowid > 1",
        1,
    )
    .await?;
    search_accounts(
        db.clone(),
        "select _rowid, _file, * from accounts where _file = 'file-0'",
        1,
    )
    .await?;

    Ok(())
}

async fn search_accounts(
    db: CustomDataSource,
    sql: &str,
    expected_result_length: usize,
) -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();
    ctx.register_table("accounts", Arc::new(db)).unwrap();
    let options = SQLOptions::new().with_allow_ddl(false);

    timeout(Duration::from_secs(10), async move {
        let dataframe = ctx.sql_with_options(sql, options).await.unwrap();
        let result = dataframe.collect().await.unwrap();
        let record_batch = result.first().unwrap();

        assert_eq!(expected_result_length, record_batch.column(1).len());
        dbg!(record_batch.columns());
    })
    .await
    .unwrap();

    Ok(())
}

/// A User, with an id and a bank account
#[derive(Clone, Debug)]
struct User {
    id: u8,
    bank_account: u64,
}

/// A custom datasource, used to represent a datastore with a single index
#[derive(Clone)]
pub struct CustomDataSource {
    inner: Arc<Mutex<CustomDataSourceInner>>,
    metadata_columns: SchemaRef,
}

struct CustomDataSourceInner {
    data: Vec<User>,
}

impl Debug for CustomDataSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("custom_db")
    }
}

impl CustomDataSource {
    pub(crate) async fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CustomExec::new(projections, self.clone())))
    }

    pub(crate) fn populate_users(&self) {
        self.add_user(User {
            id: 1,
            bank_account: 9_000,
        });
        self.add_user(User {
            id: 2,
            bank_account: 100,
        });
        self.add_user(User {
            id: 3,
            bank_account: 1_000,
        });
    }

    fn add_user(&self, user: User) {
        let mut inner = self.inner.lock().unwrap();
        inner.data.push(user);
    }
}

impl Default for CustomDataSource {
    fn default() -> Self {
        CustomDataSource {
            inner: Arc::new(Mutex::new(CustomDataSourceInner {
                data: Default::default(),
            })),
            metadata_columns: Arc::new(Schema::new(vec![
                Field::new("_rowid", DataType::UInt64, false),
                Field::new("_file", DataType::Utf8, false),
            ])),
        }
    }
}

#[async_trait]
impl TableProvider for CustomDataSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("id", DataType::UInt8, false),
            Field::new("bank_account", DataType::UInt64, true),
        ]))
    }

    fn metadata_columns(&self) -> Option<SchemaRef> {
        Some(self.metadata_columns.clone())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        return self.create_physical_plan(projection).await;
    }
}

#[derive(Debug, Clone)]
struct CustomExec {
    db: CustomDataSource,
    projected_schema: SchemaRef,
    cache: PlanProperties,
}

impl CustomExec {
    fn new(projections: Option<&Vec<usize>>, db: CustomDataSource) -> Self {
        let schema = db.schema();
        let metadata_schema = db.metadata_columns();
        let projected_schema = match projections {
            Some(projection) => {
                let projection = projection
                    .iter()
                    .map(|idx| match extract_field_index(*idx) {
                        FieldIndex::NormalIndex(i) => Arc::new(schema.field(i).clone()),
                        FieldIndex::MetadataIndex(i) => {
                            Arc::new(metadata_schema.as_ref().unwrap().field(i).clone())
                        }
                    })
                    .collect_vec();
                Arc::new(Schema::new(projection))
            }
            None => schema,
        };
        let cache = Self::compute_properties(projected_schema.clone());
        Self {
            db,
            projected_schema,
            cache,
        }
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);
        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl DisplayAs for CustomExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "CustomExec")
    }
}

impl ExecutionPlan for CustomExec {
    fn name(&self) -> &'static str {
        "CustomExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
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

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let users: Vec<User> = {
            let db = self.db.inner.lock().unwrap();
            db.data.clone()
        };

        let mut id_array = UInt8Builder::with_capacity(users.len());
        let mut account_array = UInt64Builder::with_capacity(users.len());
        let len = users.len() as u64;

        for user in users {
            id_array.append_value(user.id);
            account_array.append_value(user.bank_account);
        }

        let id_array = id_array.finish();
        let account_array = account_array.finish();
        let rowid_array = UInt64Array::from_iter_values(0_u64..len);
        let file_array =
            StringArray::from_iter_values((0_u64..len).map(|i| format!("file-{}", i)));

        let arrays = self
            .projected_schema
            .fields
            .iter()
            .map(|f| match f.name().as_str() {
                "_rowid" => Arc::new(rowid_array.clone()) as ArrayRef,
                "id" => Arc::new(id_array.clone()) as ArrayRef,
                "bank_account" => Arc::new(account_array.clone()) as ArrayRef,
                "_file" => Arc::new(file_array.clone()) as ArrayRef,
                _ => panic!("cannot reach here"),
            })
            .collect();

        Ok(Box::pin(MemoryStream::try_new(
            vec![RecordBatch::try_new(self.projected_schema.clone(), arrays)?],
            self.schema(),
            None,
        )?))
    }
}
