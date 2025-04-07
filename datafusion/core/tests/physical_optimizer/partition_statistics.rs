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
    use arrow_schema::{DataType, Field, Schema, SortOptions};
    use datafusion::datasource::listing::ListingTable;
    use datafusion::prelude::SessionContext;
    use datafusion_catalog::TableProvider;
    use datafusion_common::stats::Precision;
    use datafusion_common::{ColumnStatistics, ScalarValue, Statistics};
    use datafusion_execution::config::SessionConfig;
    use datafusion_expr_common::operator::Operator;
    use datafusion_physical_expr::expressions::{binary, lit, Column};
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion_physical_plan::filter::FilterExec;
    use datafusion_physical_plan::joins::CrossJoinExec;
    use datafusion_physical_plan::projection::ProjectionExec;
    use datafusion_physical_plan::sorts::sort::SortExec;
    use datafusion_physical_plan::union::UnionExec;
    use datafusion_physical_plan::ExecutionPlan;
    use std::sync::Arc;

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

    #[tokio::test]
    async fn test_statistics_by_partition_of_data_source() -> datafusion_common::Result<()>
    {
        let scan = create_scan_exec_with_statistics(None, Some(2)).await;
        let statistics = scan.statistics_by_partition()?;
        let expected_statistic_partition_1 =
            create_partition_statistics(2, 110, 3, 4, true);
        let expected_statistic_partition_2 =
            create_partition_statistics(2, 110, 1, 2, true);
        // Check the statistics of each partition
        assert_eq!(statistics.len(), 2);
        assert_eq!(statistics[0], expected_statistic_partition_1);
        assert_eq!(statistics[1], expected_statistic_partition_2);
        Ok(())
    }

    #[tokio::test]
    async fn test_statistics_by_partition_of_projection() -> datafusion_common::Result<()>
    {
        let scan = create_scan_exec_with_statistics(None, Some(2)).await;
        // Add projection execution plan
        let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
            vec![(Arc::new(Column::new("id", 0)), "id".to_string())];
        let projection = ProjectionExec::try_new(exprs, scan)?;
        let statistics = projection.statistics_by_partition()?;
        let expected_statistic_partition_1 =
            create_partition_statistics(2, 8, 3, 4, false);
        let expected_statistic_partition_2 =
            create_partition_statistics(2, 8, 1, 2, false);
        // Check the statistics of each partition
        assert_eq!(statistics.len(), 2);
        assert_eq!(statistics[0], expected_statistic_partition_1);
        assert_eq!(statistics[1], expected_statistic_partition_2);
        Ok(())
    }

    #[tokio::test]
    async fn test_statistics_by_partition_of_sort() -> datafusion_common::Result<()> {
        let scan = create_scan_exec_with_statistics(None, Some(2)).await;
        // Add sort execution plan
        let sort = SortExec::new(
            LexOrdering::new(vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("id", 0)),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            }]),
            scan,
        );
        let mut sort_exec = Arc::new(sort.clone());
        let statistics = sort_exec.statistics_by_partition()?;
        let expected_statistic_partition =
            create_partition_statistics(4, 220, 1, 4, true);
        assert_eq!(statistics.len(), 1);
        assert_eq!(statistics[0], expected_statistic_partition);

        sort_exec = Arc::new(sort.with_preserve_partitioning(true));
        let expected_statistic_partition_1 =
            create_partition_statistics(2, 110, 3, 4, true);
        let expected_statistic_partition_2 =
            create_partition_statistics(2, 110, 1, 2, true);
        let statistics = sort_exec.statistics_by_partition()?;
        assert_eq!(statistics.len(), 2);
        assert_eq!(statistics[0], expected_statistic_partition_1);
        assert_eq!(statistics[1], expected_statistic_partition_2);
        Ok(())
    }

    #[tokio::test]
    async fn test_statistics_by_partition_of_filter() -> datafusion_common::Result<()> {
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
        let full_statistics = filter.statistics()?;
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

        let statistics = filter.statistics_by_partition()?;
        assert_eq!(statistics.len(), 2);
        assert_eq!(statistics[0], expected_full_statistic);
        assert_eq!(statistics[1], expected_full_statistic);
        Ok(())
    }

    #[tokio::test]
    async fn test_statistic_by_partition_of_union() -> datafusion_common::Result<()> {
        let scan = create_scan_exec_with_statistics(None, Some(2)).await;
        let union_exec = Arc::new(UnionExec::new(vec![scan.clone(), scan]));
        let statistics = union_exec.statistics_by_partition()?;
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

        Ok(())
    }

    #[tokio::test]
    async fn test_statistic_by_partition_of_cross_join() -> datafusion_common::Result<()>
    {
        let left_scan = create_scan_exec_with_statistics(None, Some(2)).await;
        let right_create_table_sql = "CREATE EXTERNAL TABLE t2 (id INT NOT NULL) \
                                                STORED AS PARQUET LOCATION './tests/data/test_statistics_per_partition'\
                                                WITH ORDER (id ASC);";
        let right_scan =
            create_scan_exec_with_statistics(Some(right_create_table_sql), Some(2)).await;
        let cross_join = CrossJoinExec::new(left_scan, right_scan);
        let statistics = cross_join.statistics_by_partition()?;
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
        Ok(())
    }
}
