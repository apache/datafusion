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

//! [`MemTable`] for querying `Vec<RecordBatch>` by DataFusion.

use futures::{StreamExt, TryStreamExt};
use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion_expr::LogicalPlan;
use tokio::sync::RwLock;
use tokio::task;

use crate::datasource::{TableProvider, TableType};
use crate::error::{DataFusionError, Result};
use crate::execution::context::SessionState;
use crate::logical_expr::Expr;
use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use crate::physical_plan::common;
use crate::physical_plan::common::AbortOnDropSingle;
use crate::physical_plan::memory::MemoryExec;
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::{repartition::RepartitionExec, Partitioning};

/// In-memory data source for presenting a `Vec<RecordBatch>` as a
/// data source that can be queried by DataFusion. This allows data to
/// be pre-loaded into memory and then repeatedly queried without
/// incurring additional file I/O overhead.
#[derive(Debug)]
pub struct MemTable {
    schema: SchemaRef,
    batches: Arc<RwLock<Vec<Vec<RecordBatch>>>>,
}

impl MemTable {
    /// Create a new in-memory table from the provided schema and record batches
    pub fn try_new(schema: SchemaRef, partitions: Vec<Vec<RecordBatch>>) -> Result<Self> {
        if partitions
            .iter()
            .flatten()
            .all(|batches| schema.contains(&batches.schema()))
        {
            Ok(Self {
                schema,
                batches: Arc::new(RwLock::new(partitions)),
            })
        } else {
            Err(DataFusionError::Plan(
                "Mismatch between schema and batches".to_string(),
            ))
        }
    }

    /// Create a mem table by reading from another data source
    pub async fn load(
        t: Arc<dyn TableProvider>,
        output_partitions: Option<usize>,
        state: &SessionState,
    ) -> Result<Self> {
        let schema = t.schema();
        let exec = t.scan(state, None, &[], None).await?;
        let partition_count = exec.output_partitioning().partition_count();

        let tasks = (0..partition_count)
            .map(|part_i| {
                let task = state.task_ctx();
                let exec = exec.clone();
                let task = tokio::spawn(async move {
                    let stream = exec.execute(part_i, task)?;
                    common::collect(stream).await
                });

                AbortOnDropSingle::new(task)
            })
            // this collect *is needed* so that the join below can
            // switch between tasks
            .collect::<Vec<_>>();

        let mut data: Vec<Vec<RecordBatch>> =
            Vec::with_capacity(exec.output_partitioning().partition_count());

        for result in futures::future::join_all(tasks).await {
            data.push(result.map_err(|e| DataFusionError::External(Box::new(e)))??)
        }

        let exec = MemoryExec::try_new(&data, schema.clone(), None)?;

        if let Some(num_partitions) = output_partitions {
            let exec = RepartitionExec::try_new(
                Arc::new(exec),
                Partitioning::RoundRobinBatch(num_partitions),
            )?;

            // execute and collect results
            let mut output_partitions = vec![];
            for i in 0..exec.output_partitioning().partition_count() {
                // execute this *output* partition and collect all batches
                let task_ctx = state.task_ctx();
                let mut stream = exec.execute(i, task_ctx)?;
                let mut batches = vec![];
                while let Some(result) = stream.next().await {
                    batches.push(result?);
                }
                output_partitions.push(batches);
            }

            return MemTable::try_new(schema.clone(), output_partitions);
        }
        MemTable::try_new(schema.clone(), data)
    }
}

#[async_trait]
impl TableProvider for MemTable {
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
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let batches = &self.batches.read().await;
        Ok(Arc::new(MemoryExec::try_new(
            batches,
            self.schema(),
            projection.cloned(),
        )?))
    }

    /// Inserts the execution results of a given [LogicalPlan] into this [MemTable].
    /// The `LogicalPlan` must have the same schema as this `MemTable`.
    ///
    /// # Arguments
    ///
    /// * `state` - The [SessionState] containing the context for executing the plan.
    /// * `input` - The [LogicalPlan] to execute and insert.
    ///
    /// # Returns
    ///
    /// * A `Result` indicating success or failure.
    async fn insert_into(&self, state: &SessionState, input: &LogicalPlan) -> Result<()> {
        // Create a physical plan from the logical plan.
        let plan = state.create_physical_plan(input).await?;

        // Check that the schema of the plan matches the schema of this table.
        if !plan.schema().eq(&self.schema) {
            return Err(DataFusionError::Plan(
                "Inserting query must have the same schema with the table.".to_string(),
            ));
        }

        // Get the number of partitions in the plan and the table.
        let plan_partition_count = plan.output_partitioning().partition_count();
        let table_partition_count = self.batches.read().await.len();

        // Adjust the plan as necessary to match the number of partitions in the table.
        let plan: Arc<dyn ExecutionPlan> = if plan_partition_count
            == table_partition_count
            || table_partition_count == 0
        {
            plan
        } else if table_partition_count == 1 {
            // If the table has only one partition, coalesce the partitions in the plan.
            Arc::new(CoalescePartitionsExec::new(plan))
        } else {
            // Otherwise, repartition the plan using a round-robin partitioning scheme.
            Arc::new(RepartitionExec::try_new(
                plan,
                Partitioning::RoundRobinBatch(table_partition_count),
            )?)
        };

        // Get the task context from the session state.
        let task_ctx = state.task_ctx();

        // Execute the plan and collect the results into batches.
        let mut tasks = vec![];
        for idx in 0..plan.output_partitioning().partition_count() {
            let stream = plan.execute(idx, task_ctx.clone())?;
            let handle = task::spawn(async move {
                stream.try_collect().await.map_err(DataFusionError::from)
            });
            tasks.push(AbortOnDropSingle::new(handle));
        }
        let results = futures::future::join_all(tasks)
            .await
            .into_iter()
            .map(|result| {
                result.map_err(|e| DataFusionError::Execution(format!("{e}")))?
            })
            .collect::<Result<Vec<Vec<RecordBatch>>>>()?;

        // Write the results into the table.
        let mut all_batches = self.batches.write().await;

        if all_batches.is_empty() {
            *all_batches = results
        } else {
            for (batches, result) in all_batches.iter_mut().zip(results.into_iter()) {
                batches.extend(result);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::provider_as_source;
    use crate::from_slice::FromSlice;
    use crate::prelude::SessionContext;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::error::ArrowError;
    use datafusion_expr::LogicalPlanBuilder;
    use futures::StreamExt;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_with_projection() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
            Field::new("d", DataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_slice([1, 2, 3])),
                Arc::new(Int32Array::from_slice([4, 5, 6])),
                Arc::new(Int32Array::from_slice([7, 8, 9])),
                Arc::new(Int32Array::from(vec![None, None, Some(9)])),
            ],
        )?;

        let provider = MemTable::try_new(schema, vec![vec![batch]])?;

        // scan with projection
        let exec = provider
            .scan(&session_ctx.state(), Some(&vec![2, 1]), &[], None)
            .await?;

        let mut it = exec.execute(0, task_ctx)?;
        let batch2 = it.next().await.unwrap()?;
        assert_eq!(2, batch2.schema().fields().len());
        assert_eq!("c", batch2.schema().field(0).name());
        assert_eq!("b", batch2.schema().field(1).name());
        assert_eq!(2, batch2.num_columns());

        Ok(())
    }

    #[tokio::test]
    async fn test_without_projection() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_slice([1, 2, 3])),
                Arc::new(Int32Array::from_slice([4, 5, 6])),
                Arc::new(Int32Array::from_slice([7, 8, 9])),
            ],
        )?;

        let provider = MemTable::try_new(schema, vec![vec![batch]])?;

        let exec = provider.scan(&session_ctx.state(), None, &[], None).await?;
        let mut it = exec.execute(0, task_ctx)?;
        let batch1 = it.next().await.unwrap()?;
        assert_eq!(3, batch1.schema().fields().len());
        assert_eq!(3, batch1.num_columns());

        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_projection() -> Result<()> {
        let session_ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_slice([1, 2, 3])),
                Arc::new(Int32Array::from_slice([4, 5, 6])),
                Arc::new(Int32Array::from_slice([7, 8, 9])),
            ],
        )?;

        let provider = MemTable::try_new(schema, vec![vec![batch]])?;

        let projection: Vec<usize> = vec![0, 4];

        match provider
            .scan(&session_ctx.state(), Some(&projection), &[], None)
            .await
        {
            Err(DataFusionError::ArrowError(ArrowError::SchemaError(e))) => {
                assert_eq!(
                    "\"project index 4 out of bounds, max field 3\"",
                    format!("{e:?}")
                )
            }
            res => panic!("Scan should failed on invalid projection, got {res:?}"),
        };

        Ok(())
    }

    #[test]
    fn test_schema_validation_incompatible_column() -> Result<()> {
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let schema2 = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Float64, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema1,
            vec![
                Arc::new(Int32Array::from_slice([1, 2, 3])),
                Arc::new(Int32Array::from_slice([4, 5, 6])),
                Arc::new(Int32Array::from_slice([7, 8, 9])),
            ],
        )?;

        match MemTable::try_new(schema2, vec![vec![batch]]) {
            Err(DataFusionError::Plan(e)) => {
                assert_eq!("\"Mismatch between schema and batches\"", format!("{e:?}"))
            }
            _ => panic!("MemTable::new should have failed due to schema mismatch"),
        }

        Ok(())
    }

    #[test]
    fn test_schema_validation_different_column_count() -> Result<()> {
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let schema2 = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema1,
            vec![
                Arc::new(Int32Array::from_slice([1, 2, 3])),
                Arc::new(Int32Array::from_slice([7, 5, 9])),
            ],
        )?;

        match MemTable::try_new(schema2, vec![vec![batch]]) {
            Err(DataFusionError::Plan(e)) => {
                assert_eq!("\"Mismatch between schema and batches\"", format!("{e:?}"))
            }
            _ => panic!("MemTable::new should have failed due to schema mismatch"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_merged_schema() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let mut metadata = HashMap::new();
        metadata.insert("foo".to_string(), "bar".to_string());

        let schema1 = Schema::new_with_metadata(
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
                Field::new("c", DataType::Int32, false),
            ],
            // test for comparing metadata
            metadata,
        );

        let schema2 = Schema::new(vec![
            // test for comparing nullability
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);

        let merged_schema = Schema::try_merge(vec![schema1.clone(), schema2.clone()])?;

        let batch1 = RecordBatch::try_new(
            Arc::new(schema1),
            vec![
                Arc::new(Int32Array::from_slice([1, 2, 3])),
                Arc::new(Int32Array::from_slice([4, 5, 6])),
                Arc::new(Int32Array::from_slice([7, 8, 9])),
            ],
        )?;

        let batch2 = RecordBatch::try_new(
            Arc::new(schema2),
            vec![
                Arc::new(Int32Array::from_slice([1, 2, 3])),
                Arc::new(Int32Array::from_slice([4, 5, 6])),
                Arc::new(Int32Array::from_slice([7, 8, 9])),
            ],
        )?;

        let provider =
            MemTable::try_new(Arc::new(merged_schema), vec![vec![batch1, batch2]])?;

        let exec = provider.scan(&session_ctx.state(), None, &[], None).await?;
        let mut it = exec.execute(0, task_ctx)?;
        let batch1 = it.next().await.unwrap()?;
        assert_eq!(3, batch1.schema().fields().len());
        assert_eq!(3, batch1.num_columns());

        Ok(())
    }

    fn create_mem_table_scan(
        schema: SchemaRef,
        data: Vec<Vec<RecordBatch>>,
    ) -> Result<Arc<LogicalPlan>> {
        // Convert the table into a provider so that it can be used in a query
        let provider = provider_as_source(Arc::new(MemTable::try_new(schema, data)?));
        // Create a table scan logical plan to read from the table
        Ok(Arc::new(
            LogicalPlanBuilder::scan("source", provider, None)?.build()?,
        ))
    }

    fn create_initial_ctx() -> Result<(SessionContext, SchemaRef, RecordBatch)> {
        // Create a new session context
        let session_ctx = SessionContext::new();
        // Create a new schema with one field called "a" of type Int32
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

        // Create a new batch of data to insert into the table
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_slice([1, 2, 3]))],
        )?;
        Ok((session_ctx, schema, batch))
    }

    #[tokio::test]
    async fn test_insert_into_single_partition() -> Result<()> {
        let (session_ctx, schema, batch) = create_initial_ctx()?;
        let initial_table = Arc::new(MemTable::try_new(
            schema.clone(),
            vec![vec![batch.clone()]],
        )?);
        // Create a table scan logical plan to read from the table
        let single_partition_table_scan =
            create_mem_table_scan(schema.clone(), vec![vec![batch.clone()]])?;
        // Insert the data from the provider into the table
        initial_table
            .insert_into(&session_ctx.state(), &single_partition_table_scan)
            .await?;
        // Ensure that the table now contains two batches of data in the same partition
        assert_eq!(initial_table.batches.read().await.get(0).unwrap().len(), 2);

        // Create a new provider with 2 partitions
        let multi_partition_table_scan = create_mem_table_scan(
            schema.clone(),
            vec![vec![batch.clone()], vec![batch]],
        )?;

        // Insert the data from the provider into the table. We expect coalescing partitions.
        initial_table
            .insert_into(&session_ctx.state(), &multi_partition_table_scan)
            .await?;
        // Ensure that the table now contains 4 batches of data with only 1 partition
        assert_eq!(initial_table.batches.read().await.get(0).unwrap().len(), 4);
        assert_eq!(initial_table.batches.read().await.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_multiple_partition() -> Result<()> {
        let (session_ctx, schema, batch) = create_initial_ctx()?;
        // create a memory table with two partitions, each having one batch with the same data
        let initial_table = Arc::new(MemTable::try_new(
            schema.clone(),
            vec![vec![batch.clone()], vec![batch.clone()]],
        )?);

        // scan a data source provider from a memory table with a single partition
        let single_partition_table_scan = create_mem_table_scan(
            schema.clone(),
            vec![vec![batch.clone(), batch.clone()]],
        )?;

        // insert the data from the 1 partition data source provider into the initial table
        initial_table
            .insert_into(&session_ctx.state(), &single_partition_table_scan)
            .await?;

        // We expect round robin repartition here, each partition gets 1 batch.
        assert_eq!(initial_table.batches.read().await.get(0).unwrap().len(), 2);
        assert_eq!(initial_table.batches.read().await.get(1).unwrap().len(), 2);

        // scan a data source provider from a memory table with 2 partition
        let multi_partition_table_scan = create_mem_table_scan(
            schema.clone(),
            vec![vec![batch.clone()], vec![batch]],
        )?;
        // We expect one-to-one partition mapping.
        initial_table
            .insert_into(&session_ctx.state(), &multi_partition_table_scan)
            .await?;
        // Ensure that the table now contains 3 batches of data with 2 partitions.
        assert_eq!(initial_table.batches.read().await.get(0).unwrap().len(), 3);
        assert_eq!(initial_table.batches.read().await.get(1).unwrap().len(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_empty_table() -> Result<()> {
        let (session_ctx, schema, batch) = create_initial_ctx()?;
        // create empty memory table
        let initial_table = Arc::new(MemTable::try_new(schema.clone(), vec![])?);

        // scan a data source provider from a memory table with a single partition
        let single_partition_table_scan = create_mem_table_scan(
            schema.clone(),
            vec![vec![batch.clone(), batch.clone()]],
        )?;

        // insert the data from the 1 partition data source provider into the initial table
        initial_table
            .insert_into(&session_ctx.state(), &single_partition_table_scan)
            .await?;

        assert_eq!(initial_table.batches.read().await.get(0).unwrap().len(), 2);

        // scan a data source provider from a memory table with 2 partition
        let single_partition_table_scan = create_mem_table_scan(
            schema.clone(),
            vec![vec![batch.clone()], vec![batch]],
        )?;
        // We expect coalesce partitions here.
        initial_table
            .insert_into(&session_ctx.state(), &single_partition_table_scan)
            .await?;
        // Ensure that the table now contains 3 batches of data with 2 partitions.
        assert_eq!(initial_table.batches.read().await.get(0).unwrap().len(), 4);
        Ok(())
    }
}
