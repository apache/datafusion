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

use arrow::compute::concat_batches;
use arrow_array::{ArrayRef, UInt64Array};
use arrow_schema::SchemaBuilder;
use async_trait::async_trait;
use datafusion::arrow::array::{UInt64Builder, UInt8Builder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::csv::CsvSerializer;
use datafusion::datasource::file_format::write::BatchSerializer;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    project_schema, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion::prelude::*;

use datafusion::catalog::Session;

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
        schema: SchemaRef,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CustomExec::new(projections, schema, self.clone())))
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
            metadata_columns: Arc::new(Schema::new(vec![Field::new(
                "_rowid",
                DataType::UInt64,
                false,
            )])),
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
        let mut schema = self.schema();
        if let Some(metadata) = self.metadata_columns() {
            let mut builder = SchemaBuilder::from(schema.as_ref());
            for f in metadata.fields.iter() {
                builder.try_merge(f)?;
            }
            schema = Arc::new(builder.finish());
        }
        return self.create_physical_plan(projection, schema).await;
    }
}

#[derive(Debug, Clone)]
struct CustomExec {
    db: CustomDataSource,
    projected_schema: SchemaRef,
    cache: PlanProperties,
}

impl CustomExec {
    fn new(
        projections: Option<&Vec<usize>>,
        schema: SchemaRef,
        db: CustomDataSource,
    ) -> Self {
        let projected_schema = project_schema(&schema, projections).unwrap();
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

        let arrays = self
            .projected_schema
            .fields
            .iter()
            .map(|f| match f.name().as_str() {
                "_rowid" => Arc::new(rowid_array.clone()) as ArrayRef,
                "id" => Arc::new(id_array.clone()) as ArrayRef,
                "bank_account" => Arc::new(account_array.clone()) as ArrayRef,
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

#[tokio::test]
async fn select_metadata_column() {
    // Verify SessionContext::with_sql_options errors appropriately
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().with_information_schema(true),
    );
    let db = CustomDataSource::default();
    db.populate_users();
    // ctx.sql("CREATE TABLE test (x int)").await.unwrap();
    ctx.register_table("test", Arc::new(db)).unwrap();
    // disallow ddl
    let options = SQLOptions::new().with_allow_ddl(false);

    let show_columns = "show columns from test;";
    let df_columns = ctx.sql_with_options(show_columns, options).await.unwrap();
    let all_batchs = df_columns
        .select(vec![col("column_name"), col("data_type")])
        .unwrap()
        .collect()
        .await
        .unwrap();
    let batch = concat_batches(&all_batchs[0].schema(), &all_batchs).unwrap();
    assert_eq!(batch.num_rows(), 2);
    let serializer = CsvSerializer::new().with_header(false);
    let bytes = serializer.serialize(batch, true).unwrap();
    assert_eq!(bytes, "id,UInt8\nbank_account,UInt64\n");
    let select0 = "SELECT * FROM test order by id";
    let df0 = ctx.sql_with_options(select0, options).await.unwrap();
    assert!(!df0.schema().has_column_with_unqualified_name(&"_rowid"));

    let all_batchs = df0.collect().await.unwrap();
    let batch = concat_batches(&all_batchs[0].schema(), &all_batchs).unwrap();
    let bytes = serializer.serialize(batch, true).unwrap();
    assert_eq!(bytes, "1,9000\n2,100\n3,1000\n");

    let select1 = "SELECT _rowid FROM test order by _rowid";
    let df1 = ctx.sql_with_options(select1, options).await.unwrap();
    assert_eq!(df1.schema().field_names(), vec!["test._rowid"]);

    let all_batchs = df1.collect().await.unwrap();
    let batch = concat_batches(&all_batchs[0].schema(), &all_batchs).unwrap();
    let bytes = serializer.serialize(batch, true).unwrap();
    assert_eq!(bytes, "0\n1\n2\n");

    let select2 = "SELECT _rowid, id FROM test order by _rowid";
    let df2 = ctx.sql_with_options(select2, options).await.unwrap();
    assert_eq!(df2.schema().field_names(), vec!["test._rowid", "test.id"]);

    let all_batchs = df2.collect().await.unwrap();
    let batch = concat_batches(&all_batchs[0].schema(), &all_batchs).unwrap();
    let bytes = serializer.serialize(batch, true).unwrap();
    assert_eq!(bytes, "0,1\n1,2\n2,3\n");

    let select3 = "SELECT _rowid, id FROM test WHERE _rowid = 0";
    let df3 = ctx.sql_with_options(select3, options).await.unwrap();
    assert_eq!(df3.schema().field_names(), vec!["test._rowid", "test.id"]);

    let all_batchs = df3.collect().await.unwrap();
    let batch = concat_batches(&all_batchs[0].schema(), &all_batchs).unwrap();
    let bytes = serializer.serialize(batch, true).unwrap();
    assert_eq!(bytes, "0,1\n");

    let select4 = "SELECT _rowid FROM test LIMIT 1";
    let df4 = ctx.sql_with_options(select4, options).await.unwrap();
    assert_eq!(df4.schema().field_names(), vec!["test._rowid"]);

    let all_batchs = df4.collect().await.unwrap();
    let batch = concat_batches(&all_batchs[0].schema(), &all_batchs).unwrap();
    let bytes = serializer.serialize(batch, true).unwrap();
    assert_eq!(bytes, "0\n");

    let select5 = "SELECT _rowid, id FROM test WHERE _rowid % 2 = 1";
    let df5 = ctx.sql_with_options(select5, options).await.unwrap();
    assert_eq!(df5.schema().field_names(), vec!["test._rowid", "test.id"]);

    let all_batchs = df5.collect().await.unwrap();
    let batch = concat_batches(&all_batchs[0].schema(), &all_batchs).unwrap();
    let bytes = serializer.serialize(batch, true).unwrap();
    assert_eq!(bytes, "1,2\n");
}
