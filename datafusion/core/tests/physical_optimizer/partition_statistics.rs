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
mod test {
    use insta::assert_snapshot;
    use std::sync::Arc;

    use arrow::array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema, SortOptions};
    use datafusion::datasource::listing::ListingTable;
    use datafusion::prelude::SessionContext;
    use datafusion_catalog::TableProvider;
    use datafusion_common::stats::Precision;
    use datafusion_common::Result;
    use datafusion_common::{ColumnStatistics, ScalarValue, Statistics};
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::TaskContext;
    use datafusion_expr_common::operator::Operator;
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::{binary, col, lit, Column};
    use datafusion_physical_expr::Partitioning;
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
    use datafusion_physical_plan::aggregates::{
        AggregateExec, AggregateMode, PhysicalGroupBy,
    };
    use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
    use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion_physical_plan::common::compute_record_batch_statistics;
    use datafusion_physical_plan::empty::EmptyExec;
    use datafusion_physical_plan::filter::FilterExec;
    use datafusion_physical_plan::joins::CrossJoinExec;
    use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
    use datafusion_physical_plan::placeholder_row::PlaceholderRowExec;
    use datafusion_physical_plan::projection::{ProjectionExec, ProjectionExpr};
    use datafusion_physical_plan::repartition::RepartitionExec;
    use datafusion_physical_plan::sorts::sort::SortExec;
    use datafusion_physical_plan::union::{InterleaveExec, UnionExec};
    use datafusion_physical_plan::{
        execute_stream_partitioned, get_plan_string, ExecutionPlan,
        ExecutionPlanProperties,
    };

    use futures::TryStreamExt;

    /// Creates a test table with statistics from the test data directory.
    ///
    /// This function:
    /// - Creates an external table from './tests/data/test_statistics_per_partition'
    /// - If we set the `target_partition` to 2, the data contains 2 partitions, each with 2 rows
    /// - Each partition has an "id" column (INT) with the following values:
    ///   - First partition: [3, 4]
    ///   - Second partition: [1, 2]
    /// - Each row is 110 bytes in size
    ///
    /// @param create_table_sql Optional parameter to set the create table SQL
    /// @param target_partition Optional parameter to set the target partitions
    /// @return ExecutionPlan representing the scan of the table with statistics
    async fn create_scan_exec_with_statistics(
        create_table_sql: Option<&str>,
        target_partition: Option<usize>,
    ) -> Arc<dyn ExecutionPlan> {
        let mut session_config = SessionConfig::new().with_collect_statistics(true);
        if let Some(partition) = target_partition {
            session_config = session_config.with_target_partitions(partition);
        }
        let ctx = SessionContext::new_with_config(session_config);
        // Create table with partition
        let create_table_sql = create_table_sql.unwrap_or(
            "CREATE EXTERNAL TABLE t1 (id INT NOT NULL, date DATE) \
             STORED AS PARQUET LOCATION './tests/data/test_statistics_per_partition'\
             PARTITIONED BY (date) \
             WITH ORDER (id ASC);",
        );
        // Get table name from `create_table_sql`
        let table_name = create_table_sql
            .split_whitespace()
            .nth(3)
            .unwrap_or("t1")
            .to_string();
        ctx.sql(create_table_sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let table = ctx.table_provider(table_name.as_str()).await.unwrap();
        let listing_table = table
            .as_any()
            .downcast_ref::<ListingTable>()
            .unwrap()
            .clone();
        listing_table
            .scan(&ctx.state(), None, &[], None)
            .await
            .unwrap()
    }

    /// Helper function to create expected statistics for a partition with Int32 column
    fn create_partition_statistics(
        num_rows: usize,
        total_byte_size: usize,
        min_value: i32,
        max_value: i32,
        include_date_column: bool,
    ) -> Statistics {
        let mut column_stats = vec![ColumnStatistics {
            null_count: Precision::Exact(0),
            max_value: Precision::Exact(ScalarValue::Int32(Some(max_value))),
            min_value: Precision::Exact(ScalarValue::Int32(Some(min_value))),
            sum_value: Precision::Absent,
            distinct_count: Precision::Absent,
        }];

        if include_date_column {
            column_stats.push(ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Absent,
                min_value: Precision::Absent,
                sum_value: Precision::Absent,
                distinct_count: Precision::Absent,
            });
        }

        Statistics {
            num_rows: Precision::Exact(num_rows),
            total_byte_size: Precision::Exact(total_byte_size),
            column_statistics: column_stats,
        }
    }

    #[derive(PartialEq, Eq, Debug)]
    enum ExpectedStatistics {
        Empty,                     // row_count == 0
        NonEmpty(i32, i32, usize), // (min_id, max_id, row_count)
    }

    /// Helper function to validate that statistics from statistics_by_partition match the actual data
    async fn validate_statistics_with_data(
        plan: Arc<dyn ExecutionPlan>,
        expected_stats: Vec<ExpectedStatistics>,
        id_column_index: usize,
    ) -> Result<()> {
        let ctx = TaskContext::default();
        let partitions = execute_stream_partitioned(plan, Arc::new(ctx))?;

        let mut actual_stats = Vec::new();
        for partition_stream in partitions.into_iter() {
            let result: Vec<RecordBatch> = partition_stream.try_collect().await?;

            let mut min_id = i32::MAX;
            let mut max_id = i32::MIN;
            let mut row_count = 0;

            for batch in result {
                if batch.num_columns() > id_column_index {
                    let id_array = batch
                        .column(id_column_index)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    for i in 0..batch.num_rows() {
                        let id_value = id_array.value(i);
                        min_id = min_id.min(id_value);
                        max_id = max_id.max(id_value);
                        row_count += 1;
                    }
                }
            }

            if row_count == 0 {
                actual_stats.push(ExpectedStatistics::Empty);
            } else {
                actual_stats
                    .push(ExpectedStatistics::NonEmpty(min_id, max_id, row_count));
            }
        }

        // Compare actual data with expected statistics
        assert_eq!(
            actual_stats.len(),
            expected_stats.len(),
            "Number of partitions with data doesn't match expected"
        );
        for i in 0..actual_stats.len() {
            assert_eq!(
                actual_stats[i], expected_stats[i],
                "Partition {i} data doesn't match statistics"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_statistics_by_partition_of_data_source() -> Result<()> {
        let scan = create_scan_exec_with_statistics(None, Some(2)).await;
        let statistics = (0..scan.output_partitioning().partition_count())
            .map(|idx| scan.partition_statistics(Some(idx)))
            .collect::<Result<Vec<_>>>()?;
        let expected_statistic_partition_1 =
            create_partition_statistics(2, 110, 3, 4, true);
        let expected_statistic_partition_2 =
            create_partition_statistics(2, 110, 1, 2, true);
        // Check the statistics of each partition
        assert_eq!(statistics.len(), 2);
        assert_eq!(statistics[0], expected_statistic_partition_1);
        assert_eq!(statistics[1], expected_statistic_partition_2);

        // Check the statistics_by_partition with real results
        let expected_stats = vec![
            ExpectedStatistics::NonEmpty(3, 4, 2), // (min_id, max_id, row_count) for first partition
            ExpectedStatistics::NonEmpty(1, 2, 2), // (min_id, max_id, row_count) for second partition
        ];
        validate_statistics_with_data(scan, expected_stats, 0).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_statistics_by_partition_of_projection() -> Result<()> {
        let scan = create_scan_exec_with_statistics(None, Some(2)).await;
        // Add projection execution plan
        let exprs = vec![ProjectionExpr {
            expr: Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>,
            alias: "id".to_string(),
        }];
        let projection: Arc<dyn ExecutionPlan> =
            Arc::new(ProjectionExec::try_new(exprs, scan)?);
        let statistics = (0..projection.output_partitioning().partition_count())
            .map(|idx| projection.partition_statistics(Some(idx)))
            .collect::<Result<Vec<_>>>()?;
        let expected_statistic_partition_1 =
            create_partition_statistics(2, 8, 3, 4, false);
        let expected_statistic_partition_2 =
            create_partition_statistics(2, 8, 1, 2, false);
        // Check the statistics of each partition
        assert_eq!(statistics.len(), 2);
        assert_eq!(statistics[0], expected_statistic_partition_1);
        assert_eq!(statistics[1], expected_statistic_partition_2);

        // Check the statistics_by_partition with real results
        let expected_stats = vec![
            ExpectedStatistics::NonEmpty(3, 4, 2),
            ExpectedStatistics::NonEmpty(1, 2, 2),
        ];
        validate_statistics_with_data(projection, expected_stats, 0).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_statistics_by_partition_of_sort() -> Result<()> {
        let scan_1 = create_scan_exec_with_statistics(None, Some(1)).await;
        // Add sort execution plan
        let ordering = [PhysicalSortExpr::new(
            Arc::new(Column::new("id", 0)),
            SortOptions::new(false, false),
        )];
        let sort = SortExec::new(ordering.clone().into(), scan_1);
        let sort_exec: Arc<dyn ExecutionPlan> = Arc::new(sort);
        let statistics = (0..sort_exec.output_partitioning().partition_count())
            .map(|idx| sort_exec.partition_statistics(Some(idx)))
            .collect::<Result<Vec<_>>>()?;
        let expected_statistic_partition =
            create_partition_statistics(4, 220, 1, 4, true);
        assert_eq!(statistics.len(), 1);
        assert_eq!(statistics[0], expected_statistic_partition);
        // Check the statistics_by_partition with real results
        let expected_stats = vec![ExpectedStatistics::NonEmpty(1, 4, 4)];
        validate_statistics_with_data(sort_exec.clone(), expected_stats, 0).await?;

        // Sort with preserve_partitioning
        let scan_2 = create_scan_exec_with_statistics(None, Some(2)).await;
        // Add sort execution plan
        let sort_exec: Arc<dyn ExecutionPlan> = Arc::new(
            SortExec::new(ordering.into(), scan_2).with_preserve_partitioning(true),
        );
        let expected_statistic_partition_1 =
            create_partition_statistics(2, 110, 3, 4, true);
        let expected_statistic_partition_2 =
            create_partition_statistics(2, 110, 1, 2, true);
        let statistics = (0..sort_exec.output_partitioning().partition_count())
            .map(|idx| sort_exec.partition_statistics(Some(idx)))
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(statistics.len(), 2);
        assert_eq!(statistics[0], expected_statistic_partition_1);
        assert_eq!(statistics[1], expected_statistic_partition_2);

        // Check the statistics_by_partition with real results
        let expected_stats = vec![
            ExpectedStatistics::NonEmpty(3, 4, 2),
            ExpectedStatistics::NonEmpty(1, 2, 2),
        ];
        validate_statistics_with_data(sort_exec, expected_stats, 0).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_statistics_by_partition_of_filter() -> Result<()> {
        let scan = create_scan_exec_with_statistics(None, Some(2)).await;
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let predicate = binary(
            Arc::new(Column::new("id", 0)),
            Operator::Lt,
            lit(1i32),
            &schema,
        )?;
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, scan)?);
        let full_statistics = filter.partition_statistics(None)?;
        let expected_full_statistic = Statistics {
            num_rows: Precision::Inexact(0),
            total_byte_size: Precision::Inexact(0),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(0),
                    max_value: Precision::Exact(ScalarValue::Null),
                    min_value: Precision::Exact(ScalarValue::Null),
                    sum_value: Precision::Exact(ScalarValue::Null),
                    distinct_count: Precision::Exact(0),
                },
                ColumnStatistics {
                    null_count: Precision::Exact(0),
                    max_value: Precision::Exact(ScalarValue::Null),
                    min_value: Precision::Exact(ScalarValue::Null),
                    sum_value: Precision::Exact(ScalarValue::Null),
                    distinct_count: Precision::Exact(0),
                },
            ],
        };
        assert_eq!(full_statistics, expected_full_statistic);

        let statistics = (0..filter.output_partitioning().partition_count())
            .map(|idx| filter.partition_statistics(Some(idx)))
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(statistics.len(), 2);
        assert_eq!(statistics[0], expected_full_statistic);
        assert_eq!(statistics[1], expected_full_statistic);
        Ok(())
    }

    #[tokio::test]
    async fn test_statistic_by_partition_of_union() -> Result<()> {
        let scan = create_scan_exec_with_statistics(None, Some(2)).await;
        let union_exec: Arc<dyn ExecutionPlan> =
            UnionExec::try_new(vec![scan.clone(), scan])?;
        let statistics = (0..union_exec.output_partitioning().partition_count())
            .map(|idx| union_exec.partition_statistics(Some(idx)))
            .collect::<Result<Vec<_>>>()?;
        // Check that we have 4 partitions (2 from each scan)
        assert_eq!(statistics.len(), 4);
        let expected_statistic_partition_1 =
            create_partition_statistics(2, 110, 3, 4, true);
        let expected_statistic_partition_2 =
            create_partition_statistics(2, 110, 1, 2, true);
        // Verify first partition (from first scan)
        assert_eq!(statistics[0], expected_statistic_partition_1);
        // Verify second partition (from first scan)
        assert_eq!(statistics[1], expected_statistic_partition_2);
        // Verify third partition (from second scan - same as first partition)
        assert_eq!(statistics[2], expected_statistic_partition_1);
        // Verify fourth partition (from second scan - same as second partition)
        assert_eq!(statistics[3], expected_statistic_partition_2);

        // Check the statistics_by_partition with real results
        let expected_stats = vec![
            ExpectedStatistics::NonEmpty(3, 4, 2),
            ExpectedStatistics::NonEmpty(1, 2, 2),
            ExpectedStatistics::NonEmpty(3, 4, 2),
            ExpectedStatistics::NonEmpty(1, 2, 2),
        ];
        validate_statistics_with_data(union_exec, expected_stats, 0).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_statistics_by_partition_of_interleave() -> Result<()> {
        let scan1 = create_scan_exec_with_statistics(None, Some(1)).await;
        let scan2 = create_scan_exec_with_statistics(None, Some(1)).await;

        // Create same hash partitioning on the 'id' column as InterleaveExec
        // requires all children have a consistent hash partitioning
        let hash_expr1 = vec![col("id", &scan1.schema())?];
        let repartition1 = Arc::new(RepartitionExec::try_new(
            scan1,
            Partitioning::Hash(hash_expr1, 2),
        )?);
        let hash_expr2 = vec![col("id", &scan2.schema())?];
        let repartition2 = Arc::new(RepartitionExec::try_new(
            scan2,
            Partitioning::Hash(hash_expr2, 2),
        )?);

        let interleave: Arc<dyn ExecutionPlan> =
            Arc::new(InterleaveExec::try_new(vec![repartition1, repartition2])?);

        // Verify the result of partition statistics
        let stats = (0..interleave.output_partitioning().partition_count())
            .map(|idx| interleave.partition_statistics(Some(idx)))
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(stats.len(), 2);

        let expected_stats = Statistics {
            num_rows: Precision::Inexact(4),
            total_byte_size: Precision::Inexact(220),
            column_statistics: vec![
                ColumnStatistics::new_unknown(),
                ColumnStatistics::new_unknown(),
            ],
        };
        assert_eq!(stats[0], expected_stats);
        assert_eq!(stats[1], expected_stats);

        // Verify the execution results
        let partitions = execute_stream_partitioned(
            interleave.clone(),
            Arc::new(TaskContext::default()),
        )?;
        assert_eq!(partitions.len(), 2);

        let mut partition_row_counts = Vec::new();
        for partition_stream in partitions.into_iter() {
            let results: Vec<RecordBatch> = partition_stream.try_collect().await?;
            let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
            partition_row_counts.push(total_rows);
        }
        assert_eq!(partition_row_counts.len(), 2);
        assert_eq!(partition_row_counts.iter().sum::<usize>(), 8);

        Ok(())
    }

    #[tokio::test]
    async fn test_statistic_by_partition_of_cross_join() -> Result<()> {
        let left_scan = create_scan_exec_with_statistics(None, Some(1)).await;
        let right_create_table_sql = "CREATE EXTERNAL TABLE t2 (id INT NOT NULL) \
                                                STORED AS PARQUET LOCATION './tests/data/test_statistics_per_partition'\
                                                WITH ORDER (id ASC);";
        let right_scan =
            create_scan_exec_with_statistics(Some(right_create_table_sql), Some(2)).await;
        let cross_join: Arc<dyn ExecutionPlan> =
            Arc::new(CrossJoinExec::new(left_scan, right_scan));
        let statistics = (0..cross_join.output_partitioning().partition_count())
            .map(|idx| cross_join.partition_statistics(Some(idx)))
            .collect::<Result<Vec<_>>>()?;
        // Check that we have 2 partitions
        assert_eq!(statistics.len(), 2);
        let mut expected_statistic_partition_1 =
            create_partition_statistics(8, 48400, 1, 4, true);
        expected_statistic_partition_1
            .column_statistics
            .push(ColumnStatistics {
                null_count: Precision::Exact(0),
                max_value: Precision::Exact(ScalarValue::Int32(Some(4))),
                min_value: Precision::Exact(ScalarValue::Int32(Some(3))),
                sum_value: Precision::Absent,
                distinct_count: Precision::Absent,
            });
        let mut expected_statistic_partition_2 =
            create_partition_statistics(8, 48400, 1, 4, true);
        expected_statistic_partition_2
            .column_statistics
            .push(ColumnStatistics {
                null_count: Precision::Exact(0),
                max_value: Precision::Exact(ScalarValue::Int32(Some(2))),
                min_value: Precision::Exact(ScalarValue::Int32(Some(1))),
                sum_value: Precision::Absent,
                distinct_count: Precision::Absent,
            });
        assert_eq!(statistics[0], expected_statistic_partition_1);
        assert_eq!(statistics[1], expected_statistic_partition_2);

        // Check the statistics_by_partition with real results
        let expected_stats = vec![
            ExpectedStatistics::NonEmpty(1, 4, 8),
            ExpectedStatistics::NonEmpty(1, 4, 8),
        ];
        validate_statistics_with_data(cross_join, expected_stats, 0).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_statistic_by_partition_of_coalesce_batches() -> Result<()> {
        let scan = create_scan_exec_with_statistics(None, Some(2)).await;
        let coalesce_batches: Arc<dyn ExecutionPlan> =
            Arc::new(CoalesceBatchesExec::new(scan, 2));
        let expected_statistic_partition_1 =
            create_partition_statistics(2, 110, 3, 4, true);
        let expected_statistic_partition_2 =
            create_partition_statistics(2, 110, 1, 2, true);
        let statistics = (0..coalesce_batches.output_partitioning().partition_count())
            .map(|idx| coalesce_batches.partition_statistics(Some(idx)))
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(statistics.len(), 2);
        assert_eq!(statistics[0], expected_statistic_partition_1);
        assert_eq!(statistics[1], expected_statistic_partition_2);

        // Check the statistics_by_partition with real results
        let expected_stats = vec![
            ExpectedStatistics::NonEmpty(3, 4, 2),
            ExpectedStatistics::NonEmpty(1, 2, 2),
        ];
        validate_statistics_with_data(coalesce_batches, expected_stats, 0).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_statistic_by_partition_of_coalesce_partitions() -> Result<()> {
        let scan = create_scan_exec_with_statistics(None, Some(2)).await;
        let coalesce_partitions: Arc<dyn ExecutionPlan> =
            Arc::new(CoalescePartitionsExec::new(scan));
        let expected_statistic_partition =
            create_partition_statistics(4, 220, 1, 4, true);
        let statistics = (0..coalesce_partitions.output_partitioning().partition_count())
            .map(|idx| coalesce_partitions.partition_statistics(Some(idx)))
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(statistics.len(), 1);
        assert_eq!(statistics[0], expected_statistic_partition);

        // Check the statistics_by_partition with real results
        let expected_stats = vec![ExpectedStatistics::NonEmpty(1, 4, 4)];
        validate_statistics_with_data(coalesce_partitions, expected_stats, 0).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_statistic_by_partition_of_local_limit() -> Result<()> {
        let scan = create_scan_exec_with_statistics(None, Some(2)).await;
        let local_limit: Arc<dyn ExecutionPlan> =
            Arc::new(LocalLimitExec::new(scan.clone(), 1));
        let statistics = (0..local_limit.output_partitioning().partition_count())
            .map(|idx| local_limit.partition_statistics(Some(idx)))
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(statistics.len(), 2);
        let mut expected_0 = statistics[0].clone();
        expected_0.column_statistics = expected_0
            .column_statistics
            .into_iter()
            .map(|c| c.to_inexact())
            .collect();
        let mut expected_1 = statistics[1].clone();
        expected_1.column_statistics = expected_1
            .column_statistics
            .into_iter()
            .map(|c| c.to_inexact())
            .collect();
        assert_eq!(statistics[0], expected_0);
        assert_eq!(statistics[1], expected_1);
        Ok(())
    }

    #[tokio::test]
    async fn test_statistic_by_partition_of_global_limit_partitions() -> Result<()> {
        let scan = create_scan_exec_with_statistics(None, Some(2)).await;
        // Skip 2 rows
        let global_limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(scan.clone(), 0, Some(2)));
        let statistics = (0..global_limit.output_partitioning().partition_count())
            .map(|idx| global_limit.partition_statistics(Some(idx)))
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(statistics.len(), 1);
        let expected_statistic_partition =
            create_partition_statistics(2, 110, 3, 4, true);
        assert_eq!(statistics[0], expected_statistic_partition);
        Ok(())
    }

    #[tokio::test]
    async fn test_statistic_by_partition_of_agg() -> Result<()> {
        let scan = create_scan_exec_with_statistics(None, Some(2)).await;

        let scan_schema = scan.schema();

        // select id, 1+id, count(*) from t group by id, 1+id
        let group_by = PhysicalGroupBy::new_single(vec![
            (col("id", &scan_schema)?, "id".to_string()),
            (
                binary(
                    lit(1),
                    Operator::Plus,
                    col("id", &scan_schema)?,
                    &scan_schema,
                )?,
                "expr".to_string(),
            ),
        ]);

        let aggr_expr = vec![AggregateExprBuilder::new(count_udaf(), vec![lit(1)])
            .schema(Arc::clone(&scan_schema))
            .alias(String::from("COUNT(c)"))
            .build()
            .map(Arc::new)?];

        let aggregate_exec_partial: Arc<dyn ExecutionPlan> =
            Arc::new(AggregateExec::try_new(
                AggregateMode::Partial,
                group_by.clone(),
                aggr_expr.clone(),
                vec![None],
                Arc::clone(&scan),
                scan_schema.clone(),
            )?) as _;

        let plan_string = get_plan_string(&aggregate_exec_partial).swap_remove(0);
        assert_snapshot!(
            plan_string,
            @"AggregateExec: mode=Partial, gby=[id@0 as id, 1 + id@0 as expr], aggr=[COUNT(c)], ordering_mode=Sorted"
        );

        let p0_statistics = aggregate_exec_partial.partition_statistics(Some(0))?;

        let expected_p0_statistics = Statistics {
            num_rows: Precision::Inexact(2),
            total_byte_size: Precision::Inexact(110),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Absent,
                    max_value: Precision::Exact(ScalarValue::Int32(Some(4))),
                    min_value: Precision::Exact(ScalarValue::Int32(Some(3))),
                    sum_value: Precision::Absent,
                    distinct_count: Precision::Absent,
                },
                ColumnStatistics::new_unknown(),
                ColumnStatistics::new_unknown(),
            ],
        };

        assert_eq!(&p0_statistics, &expected_p0_statistics);

        let expected_p1_statistics = Statistics {
            num_rows: Precision::Inexact(2),
            total_byte_size: Precision::Inexact(110),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Absent,
                    max_value: Precision::Exact(ScalarValue::Int32(Some(2))),
                    min_value: Precision::Exact(ScalarValue::Int32(Some(1))),
                    sum_value: Precision::Absent,
                    distinct_count: Precision::Absent,
                },
                ColumnStatistics::new_unknown(),
                ColumnStatistics::new_unknown(),
            ],
        };

        let p1_statistics = aggregate_exec_partial.partition_statistics(Some(1))?;
        assert_eq!(&p1_statistics, &expected_p1_statistics);

        validate_statistics_with_data(
            aggregate_exec_partial.clone(),
            vec![
                ExpectedStatistics::NonEmpty(3, 4, 2),
                ExpectedStatistics::NonEmpty(1, 2, 2),
            ],
            0,
        )
        .await?;

        let agg_final = Arc::new(AggregateExec::try_new(
            AggregateMode::FinalPartitioned,
            group_by.clone(),
            aggr_expr.clone(),
            vec![None],
            aggregate_exec_partial.clone(),
            aggregate_exec_partial.schema(),
        )?);

        let p0_statistics = agg_final.partition_statistics(Some(0))?;
        assert_eq!(&p0_statistics, &expected_p0_statistics);

        let p1_statistics = agg_final.partition_statistics(Some(1))?;
        assert_eq!(&p1_statistics, &expected_p1_statistics);

        validate_statistics_with_data(
            agg_final.clone(),
            vec![
                ExpectedStatistics::NonEmpty(3, 4, 2),
                ExpectedStatistics::NonEmpty(1, 2, 2),
            ],
            0,
        )
        .await?;

        // select id, 1+id, count(*) from empty_table group by id, 1+id
        let empty_table =
            Arc::new(EmptyExec::new(scan_schema.clone()).with_partitions(2));

        let agg_partial = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            group_by.clone(),
            aggr_expr.clone(),
            vec![None],
            empty_table.clone(),
            scan_schema.clone(),
        )?) as _;

        let agg_plan = get_plan_string(&agg_partial).remove(0);
        assert_snapshot!(
            agg_plan,
            @"AggregateExec: mode=Partial, gby=[id@0 as id, 1 + id@0 as expr], aggr=[COUNT(c)]"
        );

        let empty_stat = Statistics {
            num_rows: Precision::Exact(0),
            total_byte_size: Precision::Absent,
            column_statistics: vec![
                ColumnStatistics::new_unknown(),
                ColumnStatistics::new_unknown(),
                ColumnStatistics::new_unknown(),
            ],
        };

        assert_eq!(&empty_stat, &agg_partial.partition_statistics(Some(0))?);
        assert_eq!(&empty_stat, &agg_partial.partition_statistics(Some(1))?);
        validate_statistics_with_data(
            agg_partial.clone(),
            vec![ExpectedStatistics::Empty, ExpectedStatistics::Empty],
            0,
        )
        .await?;

        let agg_partial = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            group_by.clone(),
            aggr_expr.clone(),
            vec![None],
            empty_table.clone(),
            scan_schema.clone(),
        )?);

        let agg_final = Arc::new(AggregateExec::try_new(
            AggregateMode::FinalPartitioned,
            group_by.clone(),
            aggr_expr.clone(),
            vec![None],
            agg_partial.clone(),
            agg_partial.schema(),
        )?);

        assert_eq!(&empty_stat, &agg_final.partition_statistics(Some(0))?);
        assert_eq!(&empty_stat, &agg_final.partition_statistics(Some(1))?);

        validate_statistics_with_data(
            agg_final,
            vec![ExpectedStatistics::Empty, ExpectedStatistics::Empty],
            0,
        )
        .await?;

        // select count(*) from empty_table
        let agg_partial = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::default(),
            aggr_expr.clone(),
            vec![None],
            empty_table.clone(),
            scan_schema.clone(),
        )?);

        let coalesce = Arc::new(CoalescePartitionsExec::new(agg_partial.clone()));

        let agg_final = Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::default(),
            aggr_expr.clone(),
            vec![None],
            coalesce.clone(),
            coalesce.schema(),
        )?);

        let expect_stat = Statistics {
            num_rows: Precision::Exact(1),
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics::new_unknown()],
        };

        assert_eq!(&expect_stat, &agg_final.partition_statistics(Some(0))?);

        // Verify that the aggregate final result has exactly one partition with one row
        let mut partitions = execute_stream_partitioned(
            agg_final.clone(),
            Arc::new(TaskContext::default()),
        )?;
        assert_eq!(1, partitions.len());
        let result: Vec<RecordBatch> = partitions.remove(0).try_collect().await?;
        assert_eq!(1, result[0].num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn test_statistic_by_partition_of_placeholder_rows() -> Result<()> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let plan = Arc::new(PlaceholderRowExec::new(schema).with_partitions(2))
            as Arc<dyn ExecutionPlan>;
        let schema = plan.schema();

        let ctx = TaskContext::default();
        let partitions = execute_stream_partitioned(Arc::clone(&plan), Arc::new(ctx))?;

        let mut all_batches = vec![];
        for (i, partition_stream) in partitions.into_iter().enumerate() {
            let batches: Vec<RecordBatch> = partition_stream.try_collect().await?;
            let actual = plan.partition_statistics(Some(i))?;
            let expected = compute_record_batch_statistics(
                std::slice::from_ref(&batches),
                &schema,
                None,
            );
            assert_eq!(actual, expected);
            all_batches.push(batches);
        }

        let actual = plan.partition_statistics(None)?;
        let expected = compute_record_batch_statistics(&all_batches, &schema, None);
        assert_eq!(actual, expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_statistic_by_partition_of_repartition() -> Result<()> {
        let scan = create_scan_exec_with_statistics(None, Some(2)).await;

        let repartition = Arc::new(RepartitionExec::try_new(
            scan.clone(),
            Partitioning::RoundRobinBatch(3),
        )?);

        let statistics = (0..repartition.partitioning().partition_count())
            .map(|idx| repartition.partition_statistics(Some(idx)))
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(statistics.len(), 3);

        let expected_stats = Statistics {
            num_rows: Precision::Inexact(1),
            total_byte_size: Precision::Inexact(73),
            column_statistics: vec![
                ColumnStatistics::new_unknown(),
                ColumnStatistics::new_unknown(),
            ],
        };

        // All partitions should have the same statistics
        for stat in statistics.iter() {
            assert_eq!(stat, &expected_stats);
        }

        // Verify that the result has exactly 3 partitions
        let partitions = execute_stream_partitioned(
            repartition.clone(),
            Arc::new(TaskContext::default()),
        )?;
        assert_eq!(partitions.len(), 3);

        // Collect row counts from each partition
        let mut partition_row_counts = Vec::new();
        for partition_stream in partitions.into_iter() {
            let results: Vec<RecordBatch> = partition_stream.try_collect().await?;
            let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
            partition_row_counts.push(total_rows);
        }
        assert_eq!(partition_row_counts.len(), 3);
        assert_eq!(partition_row_counts[0], 1);
        assert_eq!(partition_row_counts[1], 2);
        assert_eq!(partition_row_counts[2], 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_statistic_by_partition_of_repartition_invalid_partition() -> Result<()>
    {
        let scan = create_scan_exec_with_statistics(None, Some(2)).await;

        let repartition = Arc::new(RepartitionExec::try_new(
            scan.clone(),
            Partitioning::RoundRobinBatch(2),
        )?);

        let result = repartition.partition_statistics(Some(2));
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error
            .to_string()
            .contains("RepartitionExec invalid partition 2 (expected less than 2)"));

        let partitions = execute_stream_partitioned(
            repartition.clone(),
            Arc::new(TaskContext::default()),
        )?;
        assert_eq!(partitions.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_statistic_by_partition_of_repartition_zero_partitions() -> Result<()> {
        let scan = create_scan_exec_with_statistics(None, Some(2)).await;
        let scan_schema = scan.schema();

        // Create a repartition with 0 partitions
        let repartition = Arc::new(RepartitionExec::try_new(
            Arc::new(EmptyExec::new(scan_schema.clone())),
            Partitioning::RoundRobinBatch(0),
        )?);

        let result = repartition.partition_statistics(Some(0))?;
        assert_eq!(result, Statistics::new_unknown(&scan_schema));

        // Verify that the result has exactly 0 partitions
        let partitions = execute_stream_partitioned(
            repartition.clone(),
            Arc::new(TaskContext::default()),
        )?;
        assert_eq!(partitions.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_statistic_by_partition_of_repartition_hash_partitioning() -> Result<()>
    {
        let scan = create_scan_exec_with_statistics(None, Some(1)).await;

        // Create hash partitioning on the 'id' column
        let hash_expr = vec![col("id", &scan.schema())?];
        let repartition = Arc::new(RepartitionExec::try_new(
            scan,
            Partitioning::Hash(hash_expr, 2),
        )?);

        // Verify the result of partition statistics of repartition
        let stats = (0..repartition.partitioning().partition_count())
            .map(|idx| repartition.partition_statistics(Some(idx)))
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(stats.len(), 2);

        let expected_stats = Statistics {
            num_rows: Precision::Inexact(2),
            total_byte_size: Precision::Inexact(110),
            column_statistics: vec![
                ColumnStatistics::new_unknown(),
                ColumnStatistics::new_unknown(),
            ],
        };
        assert_eq!(stats[0], expected_stats);
        assert_eq!(stats[1], expected_stats);

        // Verify the repartition execution results
        let partitions =
            execute_stream_partitioned(repartition, Arc::new(TaskContext::default()))?;
        assert_eq!(partitions.len(), 2);

        let mut partition_row_counts = Vec::new();
        for partition_stream in partitions.into_iter() {
            let results: Vec<RecordBatch> = partition_stream.try_collect().await?;
            let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
            partition_row_counts.push(total_rows);
        }
        assert_eq!(partition_row_counts.len(), 2);
        assert_eq!(partition_row_counts.iter().sum::<usize>(), 4);

        Ok(())
    }
}
