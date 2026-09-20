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

#[cfg(test)]
mod tests {

    use crate::datasource::MemTable;
    use crate::datasource::{DefaultTableSource, provider_as_source};
    use crate::physical_plan::{ExecutionPlan, collect};
    use crate::prelude::SessionContext;
    use arrow::array::{AsArray, Int32Array};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema, UInt64Type};
    use arrow::error::ArrowError;
    use arrow::record_batch::RecordBatch;
    use arrow_schema::SchemaRef;
    use datafusion_catalog::TableProvider;
    use datafusion_common::tree_node::TreeNodeRecursion;
    use datafusion_common::{
        Constraint, Constraints, DataFusionError, Result, assert_contains,
    };
    use datafusion_expr::dml::InsertOp;
    use datafusion_expr::{Expr, LogicalPlanBuilder, col, lit};
    use datafusion_physical_expr::utils::collect_columns;
    use futures::StreamExt;
    use std::collections::HashMap;
    use std::sync::Arc;

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
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
                Arc::new(Int32Array::from(vec![None, None, Some(9)])),
            ],
        )?;

        let provider = MemTable::try_new(schema, vec![vec![batch]])?;

        // scan with projection
        let exec = provider
            .scan(&session_ctx.state(), Some(&[2, 1]), &[], None)
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
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
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

    /// Builds a single-batch [`MemTable`] over an `(a, b)` schema, optionally
    /// attaching the given constraints.
    fn source_table(constraints: Option<Constraints>) -> Result<MemTable> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
            ],
        )?;
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        Ok(match constraints {
            Some(constraints) => table.with_constraints(constraints),
            None => table,
        })
    }

    #[tokio::test]
    async fn test_load_preserves_constraints() -> Result<()> {
        let session_ctx = SessionContext::new();
        let constraints =
            Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])]);

        // Single partition
        let source = Arc::new(source_table(Some(constraints.clone()))?);
        let loaded = MemTable::load(source, None, &session_ctx.state()).await?;
        assert_eq!(loaded.constraints(), Some(&constraints));

        // Multiple partitions
        let source = Arc::new(source_table(Some(constraints.clone()))?);
        let loaded = MemTable::load(source, Some(2), &session_ctx.state()).await?;
        assert_eq!(loaded.constraints(), Some(&constraints));

        Ok(())
    }

    #[tokio::test]
    async fn test_load_without_constraints() -> Result<()> {
        let session_ctx = SessionContext::new();

        let source = Arc::new(source_table(None)?);
        let loaded = MemTable::load(source, None, &session_ctx.state()).await?;
        assert_eq!(loaded.constraints(), Some(&Constraints::default()));

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
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )?;

        let provider = MemTable::try_new(schema, vec![vec![batch]])?;

        let projection: Vec<usize> = vec![0, 4];

        match provider
            .scan(&session_ctx.state(), Some(&projection), &[], None)
            .await
        {
            Err(DataFusionError::ArrowError(err, _)) => match err.as_ref() {
                ArrowError::SchemaError(e) => {
                    assert_eq!(
                        "\"project index 4 out of bounds, max field 3\"",
                        format!("{e:?}")
                    )
                }
                _ => panic!("unexpected error"),
            },
            res => panic!("Scan should failed on invalid projection, got {res:?}"),
        }

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
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )?;

        let e = MemTable::try_new(schema2, vec![vec![batch]]).unwrap_err();
        assert_eq!(
            "Error during planning: Mismatch between schema and batches",
            e.strip_backtrace()
        );

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
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![7, 5, 9])),
            ],
        )?;

        let e = MemTable::try_new(schema2, vec![vec![batch]]).unwrap_err();
        assert_eq!(
            "Error during planning: Mismatch between schema and batches",
            e.strip_backtrace()
        );

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
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )?;

        let batch2 = RecordBatch::try_new(
            Arc::new(schema2),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
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

    async fn experiment(
        schema: SchemaRef,
        initial_data: Vec<Vec<RecordBatch>>,
        inserted_data: Vec<Vec<RecordBatch>>,
    ) -> Result<Vec<Vec<RecordBatch>>> {
        let expected_count: u64 = inserted_data
            .iter()
            .flat_map(|batches| batches.iter().map(|batch| batch.num_rows() as u64))
            .sum();

        // Create a new session context
        let session_ctx = SessionContext::new();
        // Create and register the initial table with the provided schema and data
        let initial_table = Arc::new(MemTable::try_new(schema.clone(), initial_data)?);
        session_ctx.register_table("t", initial_table.clone())?;
        let target = Arc::new(DefaultTableSource::new(initial_table.clone()));
        // Create and register the source table with the provided schema and inserted data
        let source_table = Arc::new(MemTable::try_new(schema.clone(), inserted_data)?);
        session_ctx.register_table("source", source_table.clone())?;
        // Convert the source table into a provider so that it can be used in a query
        let source = provider_as_source(source_table);
        // Create a table scan logical plan to read from the source table
        let scan_plan = LogicalPlanBuilder::scan("source", source, None)?.build()?;
        // Create an insert plan to insert the source data into the initial table
        let insert_into_table =
            LogicalPlanBuilder::insert_into(scan_plan, "t", target, InsertOp::Append)?
                .build()?;
        // Create a physical plan from the insert plan
        let plan = session_ctx
            .state()
            .create_physical_plan(&insert_into_table)
            .await?;

        // Execute the physical plan and collect the results
        let res = collect(plan, session_ctx.task_ctx()).await?;
        assert_eq!(extract_count(res), expected_count);

        // Read the data from the initial table and store it in a vector of partitions
        let mut partitions = vec![];
        for partition in initial_table.batches.iter() {
            let part = partition.read().await.clone();
            partitions.push(part);
        }
        Ok(partitions)
    }

    /// Returns the value of results. For example, returns 6 given the following
    ///
    /// ```text
    /// +-------+,
    /// | count |,
    /// +-------+,
    /// | 6     |,
    /// +-------+,
    /// ```
    fn extract_count(res: Vec<RecordBatch>) -> u64 {
        assert_eq!(res.len(), 1, "expected one batch, got {}", res.len());
        let batch = &res[0];
        assert_eq!(
            batch.num_columns(),
            1,
            "expected 1 column, got {}",
            batch.num_columns()
        );
        let col = batch.column(0).as_primitive::<UInt64Type>();
        assert_eq!(col.len(), 1, "expected 1 row, got {}", col.len());

        col.iter()
            .next()
            .expect("had value")
            .expect("expected non null")
    }

    // Test inserting a single batch of data into a single partition
    #[tokio::test]
    async fn test_insert_into_single_partition() -> Result<()> {
        // Create a new schema with one field called "a" of type Int32
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

        // Create a new batch of data to insert into the table
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        // Run the experiment and obtain the resulting data in the table
        let resulting_data_in_table =
            experiment(schema, vec![vec![batch.clone()]], vec![vec![batch.clone()]])
                .await?;
        // Ensure that the table now contains two batches of data in the same partition
        assert_eq!(resulting_data_in_table[0].len(), 2);
        Ok(())
    }

    // Test inserting multiple batches of data into a single partition
    #[tokio::test]
    async fn test_insert_into_single_partition_with_multi_partition() -> Result<()> {
        // Create a new schema with one field called "a" of type Int32
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

        // Create a new batch of data to insert into the table
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        // Run the experiment and obtain the resulting data in the table
        let resulting_data_in_table = experiment(
            schema,
            vec![vec![batch.clone()]],
            vec![vec![batch.clone()], vec![batch]],
        )
        .await?;
        // Ensure that the table now contains three batches of data in the same partition
        assert_eq!(resulting_data_in_table[0].len(), 3);
        Ok(())
    }

    // Test inserting multiple batches of data into multiple partitions
    #[tokio::test]
    async fn test_insert_into_multi_partition_with_multi_partition() -> Result<()> {
        // Create a new schema with one field called "a" of type Int32
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

        // Create a new batch of data to insert into the table
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        // Run the experiment and obtain the resulting data in the table
        let resulting_data_in_table = experiment(
            schema,
            vec![vec![batch.clone()], vec![batch.clone()]],
            vec![
                vec![batch.clone(), batch.clone()],
                vec![batch.clone(), batch],
            ],
        )
        .await?;
        // Ensure that each partition in the table now contains three batches of data
        assert_eq!(resulting_data_in_table[0].len(), 3);
        assert_eq!(resulting_data_in_table[1].len(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_from_empty_table() -> Result<()> {
        // Create a new schema with one field called "a" of type Int32
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

        // Create a new batch of data to insert into the table
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        // Run the experiment and obtain the resulting data in the table
        let resulting_data_in_table = experiment(
            schema,
            vec![vec![batch.clone(), batch.clone()]],
            vec![vec![]],
        )
        .await?;
        // Ensure that the table now contains two batches of data in the same partition
        assert_eq!(resulting_data_in_table[0].len(), 2);
        Ok(())
    }

    // Test inserting a batch into a MemTable without any partitions
    #[tokio::test]
    async fn test_insert_into_zero_partition() -> Result<()> {
        // Create a new schema with one field called "a" of type Int32
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

        // Create a new batch of data to insert into the table
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        // Run the experiment and expect an error
        let experiment_result = experiment(schema, vec![], vec![vec![batch.clone()]])
            .await
            .unwrap_err();
        // Ensure that there is a descriptive error message
        assert_eq!(
            "Error during planning: No partitions provided, expected at least one partition",
            experiment_result.strip_backtrace()
        );
        Ok(())
    }

    /// A schema of one non-nullable Int32 column called "a".
    fn one_column_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    /// A batch of the rows 1, 2 and 3 in the column "a".
    fn one_column_batch(schema: &SchemaRef) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?)
    }

    /// A `MemTable` of one partition holding the rows 1, 2 and 3.
    fn one_column_table() -> Result<MemTable> {
        let schema = one_column_schema();
        MemTable::try_new(Arc::clone(&schema), vec![vec![one_column_batch(&schema)?]])
    }

    /// A `MemTable` that holds no partition. `MemTable::try_new` rejects an
    /// empty partition list, so a caller reaches this state through the public
    /// `batches` field.
    fn zero_partition_table(schema: SchemaRef) -> Result<MemTable> {
        let mut table = MemTable::try_new(schema, vec![vec![]])?;
        table.batches.clear();
        Ok(table)
    }

    /// Run a DELETE or an UPDATE plan and return the count that it emits.
    async fn run_dml(
        plan: Arc<dyn ExecutionPlan>,
        session_ctx: &SessionContext,
    ) -> Result<u64> {
        Ok(extract_count(collect(plan, session_ctx.task_ctx()).await?))
    }

    /// Read one partition of a `MemTable` as a plain vector of batches.
    async fn read_partition(table: &MemTable, partition: usize) -> Vec<RecordBatch> {
        table.batches[partition].read().await.clone()
    }

    /// The values of the first column of `batch`, which must hold no null.
    fn column_values(batch: &RecordBatch) -> Vec<i32> {
        batch
            .column(0)
            .as_primitive::<Int32Type>()
            .iter()
            .map(|value| value.expect("expected non null"))
            .collect()
    }

    // SQL cannot hold an unpolled stream or execute the same physical plan twice.
    #[tokio::test]
    async fn test_dml_execution_lifecycle() -> Result<()> {
        for update in [false, true] {
            let session_ctx = SessionContext::new();
            let schema = one_column_schema();
            let batch = one_column_batch(&schema)?;
            let ordering = vec![vec![col("a").sort(true, false)]];
            let table = MemTable::try_new(schema, vec![vec![batch.clone()], vec![]])?
                .with_sort_order(ordering.clone());
            let filters = vec![col("a").gt(lit(1))];
            let plan = if update {
                table
                    .update(
                        &session_ctx.state(),
                        vec![("a".to_string(), col("a") + lit(10))],
                        filters,
                    )
                    .await?
            } else {
                table.delete_from(&session_ctx.state(), filters).await?
            };

            let stream = plan.execute(0, session_ctx.task_ctx())?;
            assert_eq!(stream.schema(), plan.schema());
            drop(stream);
            assert_eq!(
                column_values(&read_partition(&table, 0).await[0]),
                vec![1, 2, 3]
            );
            assert_eq!(*table.sort_order.lock(), ordering);

            // Rows added after planning must participate, including duplicates
            // in separate batches and an initially empty partition.
            table.batches[1]
                .write()
                .await
                .extend([batch.clone(), batch]);
            assert_eq!(run_dml(Arc::clone(&plan), &session_ctx).await?, 6);
            assert!(table.sort_order.lock().is_empty());
            let expected = if update { vec![1, 12, 13] } else { vec![1] };
            let mut actual = Vec::new();
            for partition in &table.batches {
                for batch in partition.read().await.iter() {
                    actual.extend(column_values(batch));
                }
            }
            actual.sort_unstable();
            let mut expected = expected.repeat(3);
            expected.sort_unstable();
            assert_eq!(actual, expected);

            // Reusing the plan reevaluates current rows and emits a fresh count.
            assert_eq!(
                run_dml(plan, &session_ctx).await?,
                if update { 6 } else { 0 }
            );
            let expected = if update { vec![1, 22, 23] } else { vec![1] };
            let mut actual = Vec::new();
            for partition in &table.batches {
                for batch in partition.read().await.iter() {
                    actual.extend(column_values(batch));
                }
            }
            actual.sort_unstable();
            let mut expected = expected.repeat(3);
            expected.sort_unstable();
            assert_eq!(actual, expected);
        }
        Ok(())
    }

    // Expression visitors must see both assignments and predicates, while
    // respecting early termination across those two groups.
    #[tokio::test]
    async fn test_dml_expression_visitors() -> Result<()> {
        let session_ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("untouched", DataType::Int32, false),
        ]));
        let table = MemTable::try_new(schema, vec![vec![]])?;
        let state = session_ctx.state();
        let delete = table.delete_from(&state, vec![col("a").gt(lit(1))]).await?;
        let update = table
            .update(
                &state,
                vec![("b".to_string(), col("b") + lit(10))],
                vec![col("a").gt(lit(1))],
            )
            .await?;

        for (plan, expected) in [(delete, vec!["a"]), (update, vec!["a", "b"])] {
            for recursion in [TreeNodeRecursion::Continue, TreeNodeRecursion::Jump] {
                let mut columns = Vec::new();
                plan.apply_expressions(&mut |expr| {
                    columns.extend(
                        collect_columns(expr)
                            .iter()
                            .map(|col| col.name().to_string()),
                    );
                    Ok(recursion)
                })?;
                columns.sort();
                assert_eq!(columns, expected);
            }

            let mut visits = 0;
            let result = plan.apply_expressions(&mut |_| {
                visits += 1;
                Ok(TreeNodeRecursion::Stop)
            })?;
            assert_eq!(result, TreeNodeRecursion::Stop);
            assert_eq!(visits, 1);

            let err = plan
                .apply_expressions(&mut |_| {
                    Err(DataFusionError::Execution("visitor failed".to_string()))
                })
                .unwrap_err();
            assert_contains!(err.to_string(), "visitor failed");
        }
        Ok(())
    }

    // A DELETE on a table without a partition affects no row
    #[tokio::test]
    async fn test_delete_from_zero_partition() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let table = zero_partition_table(one_column_schema())?;

        let plan = table.delete_from(&state, vec![col("a").gt(lit(1))]).await?;
        assert_eq!(run_dml(plan, &session_ctx).await?, 0);
        Ok(())
    }

    // An UPDATE on a table without a partition affects no row
    #[tokio::test]
    async fn test_update_zero_partition() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let table = zero_partition_table(one_column_schema())?;

        let plan = table
            .update(&state, vec![("a".to_string(), lit(7))], vec![])
            .await?;
        assert_eq!(run_dml(plan, &session_ctx).await?, 0);
        Ok(())
    }

    // A DELETE skips a batch of no row and drops it from the partition
    #[tokio::test]
    async fn test_delete_from_empty_batch() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let schema = one_column_schema();
        let table = MemTable::try_new(
            Arc::clone(&schema),
            vec![vec![
                RecordBatch::new_empty(Arc::clone(&schema)),
                one_column_batch(&schema)?,
            ]],
        )?;

        let plan = table.delete_from(&state, vec![col("a").gt(lit(1))]).await?;
        assert_eq!(run_dml(plan, &session_ctx).await?, 2);

        // The empty batch is gone and the row 1 remains
        let partition = read_partition(&table, 0).await;
        assert_eq!(partition.len(), 1);
        assert_eq!(column_values(&partition[0]), vec![1]);
        Ok(())
    }

    // An UPDATE skips a batch of no row and drops it from the partition
    #[tokio::test]
    async fn test_update_empty_batch() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let schema = one_column_schema();
        let table = MemTable::try_new(
            Arc::clone(&schema),
            vec![vec![
                RecordBatch::new_empty(Arc::clone(&schema)),
                one_column_batch(&schema)?,
            ]],
        )?;

        let plan = table
            .update(
                &state,
                vec![("a".to_string(), lit(7))],
                vec![col("a").gt(lit(1))],
            )
            .await?;
        assert_eq!(run_dml(plan, &session_ctx).await?, 2);

        // The empty batch is gone and the rows 2 and 3 now hold 7
        let partition = read_partition(&table, 0).await;
        assert_eq!(partition.len(), 1);
        assert_eq!(column_values(&partition[0]), vec![1, 7, 7]);
        Ok(())
    }

    // The DELETE plan has one partition and rejects a request for another
    #[tokio::test]
    async fn test_delete_exec_rejects_other_partition() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let table = one_column_table()?;

        let plan = table.delete_from(&state, vec![]).await?;
        let Err(err) = plan.execute(1, session_ctx.task_ctx()) else {
            panic!("expected an error for partition 1");
        };
        assert_contains!(
            err.strip_backtrace(),
            "MemDeleteExec has one partition, but partition 1 was requested"
        );

        // The failed request leaves the rows alone
        assert_eq!(read_partition(&table, 0).await[0].num_rows(), 3);
        Ok(())
    }

    // The UPDATE plan has one partition and rejects a request for another
    #[tokio::test]
    async fn test_update_exec_rejects_other_partition() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let table = one_column_table()?;

        let plan = table
            .update(&state, vec![("a".to_string(), lit(7))], vec![])
            .await?;
        let Err(err) = plan.execute(1, session_ctx.task_ctx()) else {
            panic!("expected an error for partition 1");
        };
        assert_contains!(
            err.strip_backtrace(),
            "MemUpdateExec has one partition, but partition 1 was requested"
        );

        // The failed request leaves the rows alone
        assert_eq!(
            column_values(&read_partition(&table, 0).await[0]),
            vec![1, 2, 3]
        );
        Ok(())
    }

    // An UPDATE whose `SET` clause names an unknown column fails while the plan
    // is built, so `EXPLAIN` reports it
    #[tokio::test]
    async fn test_update_unknown_set_column() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let table = one_column_table()?;

        let err = table
            .update(&state, vec![("nonexistent".to_string(), lit(7))], vec![])
            .await
            .unwrap_err();
        assert_contains!(
            err.strip_backtrace(),
            "UPDATE failed: column 'nonexistent' does not exist. Available columns: a"
        );
        Ok(())
    }

    // A DELETE whose `WHERE` clause is not a predicate fails when the plan runs
    #[tokio::test]
    async fn test_delete_from_non_boolean_filter() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let table = one_column_table()?;

        let plan = table.delete_from(&state, vec![lit(1)]).await?;
        let err = run_dml(plan, &session_ctx).await.unwrap_err();
        assert_contains!(err.strip_backtrace(), "Filter did not evaluate to boolean");

        // The failed run leaves the rows alone
        assert_eq!(read_partition(&table, 0).await[0].num_rows(), 3);
        Ok(())
    }

    // An UPDATE whose `WHERE` clause is not a predicate fails when the plan runs
    #[tokio::test]
    async fn test_update_non_boolean_filter() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let table = one_column_table()?;

        let plan = table
            .update(&state, vec![("a".to_string(), lit(7))], vec![lit(1)])
            .await?;
        let err = run_dml(plan, &session_ctx).await.unwrap_err();
        assert_contains!(err.strip_backtrace(), "Filter did not evaluate to boolean");
        Ok(())
    }

    // An UPDATE reports the column that a batch of the table does not hold
    #[tokio::test]
    async fn test_update_column_missing_from_batch() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, true),
        ]));
        let table = MemTable::try_new(
            Arc::clone(&table_schema),
            vec![vec![RecordBatch::try_new(
                table_schema,
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(Int32Array::from(vec![4, 5, 6])),
                ],
            )?]],
        )?;
        // `try_new` rejects a batch that misses a column of the table, so the
        // test writes one straight into the partition. The UPDATE reports the
        // missing column instead of a panic.
        let batch = one_column_batch(&one_column_schema())?;
        *table.batches[0].write().await = vec![batch];

        let plan = table
            .update(&state, vec![("a".to_string(), lit(7))], vec![])
            .await?;
        let err = run_dml(plan, &session_ctx).await.unwrap_err();
        assert_contains!(err.strip_backtrace(), "Column 'b' not found in batch");
        Ok(())
    }

    // An UPDATE reports an assignment of a value of the wrong type
    #[tokio::test]
    async fn test_update_assignment_type_mismatch() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let table = one_column_table()?;

        let assignment: (String, Expr) = ("a".to_string(), lit("seven"));
        let plan = table
            .update(&state, vec![assignment], vec![col("a").gt(lit(1))])
            .await?;
        let err = run_dml(plan, &session_ctx).await.unwrap_err();
        assert_contains!(
            err.strip_backtrace(),
            "arguments need to have the same data type"
        );
        Ok(())
    }
}
