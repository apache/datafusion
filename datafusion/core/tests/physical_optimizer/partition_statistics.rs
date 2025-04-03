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
    use datafusion_common::{ScalarValue, Statistics};
    use datafusion_execution::config::SessionConfig;
    use datafusion_expr_common::operator::Operator;
    use datafusion_physical_expr::expressions::{binary, lit, Column};
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion_physical_plan::filter::FilterExec;
    use datafusion_physical_plan::projection::ProjectionExec;
    use datafusion_physical_plan::sorts::sort::SortExec;
    use datafusion_physical_plan::union::UnionExec;
    use datafusion_physical_plan::ExecutionPlan;
    use std::sync::Arc;

    async fn generate_listing_table_with_statistics(
        target_partition: Option<usize>,
    ) -> Arc<dyn ExecutionPlan> {
        let mut session_config = SessionConfig::new().with_collect_statistics(true);
        if let Some(partition) = target_partition {
            session_config = session_config.with_target_partitions(partition);
        }
        let ctx = SessionContext::new_with_config(session_config);
        // Create table with partition
        let create_table_sql = "CREATE EXTERNAL TABLE t1 (id INT not null, date DATE) STORED AS PARQUET LOCATION './tests/data/test_statistics_per_partition' PARTITIONED BY (date) WITH ORDER (id ASC);";
        ctx.sql(create_table_sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let table = ctx.table_provider("t1").await.unwrap();
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

    fn check_unchanged_statistics(statistics: Vec<Statistics>) {
        // Check the statistics of each partition
        for stat in &statistics {
            assert_eq!(stat.num_rows, Precision::Exact(1));
            // First column (id) should have non-null values
            assert_eq!(stat.column_statistics[0].null_count, Precision::Exact(0));
        }

        // Verify specific id values for each partition
        assert_eq!(
            statistics[0].column_statistics[0].max_value,
            Precision::Exact(ScalarValue::Int32(Some(4)))
        );
        assert_eq!(
            statistics[1].column_statistics[0].max_value,
            Precision::Exact(ScalarValue::Int32(Some(3)))
        );
        assert_eq!(
            statistics[2].column_statistics[0].max_value,
            Precision::Exact(ScalarValue::Int32(Some(2)))
        );
        assert_eq!(
            statistics[3].column_statistics[0].max_value,
            Precision::Exact(ScalarValue::Int32(Some(1)))
        );
    }

    #[tokio::test]
    async fn test_statistics_by_partition_of_data_source() -> datafusion_common::Result<()>
    {
        let scan = generate_listing_table_with_statistics(None).await;
        let statistics = scan.statistics_by_partition()?;
        // Check the statistics of each partition
        assert_eq!(statistics.len(), 4);
        for stat in &statistics {
            assert_eq!(stat.column_statistics.len(), 2);
            assert_eq!(stat.total_byte_size, Precision::Exact(55));
        }
        check_unchanged_statistics(statistics);
        Ok(())
    }

    #[tokio::test]
    async fn test_statistics_by_partition_of_projection() -> datafusion_common::Result<()>
    {
        let scan = generate_listing_table_with_statistics(None).await;
        // Add projection execution plan
        let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
            vec![(Arc::new(Column::new("id", 0)), "id".to_string())];
        let projection = ProjectionExec::try_new(exprs, scan)?;
        let statistics = projection.statistics_by_partition()?;
        for stat in &statistics {
            assert_eq!(stat.column_statistics.len(), 1);
            assert_eq!(stat.total_byte_size, Precision::Exact(4));
        }
        check_unchanged_statistics(statistics);
        Ok(())
    }

    #[tokio::test]
    async fn test_statistics_by_partition_of_sort() -> datafusion_common::Result<()> {
        let scan = generate_listing_table_with_statistics(Some(2)).await;
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
        assert_eq!(statistics.len(), 1);
        assert_eq!(statistics[0].num_rows, Precision::Exact(4));
        assert_eq!(statistics[0].column_statistics.len(), 2);
        assert_eq!(statistics[0].total_byte_size, Precision::Exact(220));
        assert_eq!(
            statistics[0].column_statistics[0].null_count,
            Precision::Exact(0)
        );
        assert_eq!(
            statistics[0].column_statistics[0].max_value,
            Precision::Exact(ScalarValue::Int32(Some(4)))
        );
        assert_eq!(
            statistics[0].column_statistics[0].min_value,
            Precision::Exact(ScalarValue::Int32(Some(1)))
        );
        sort_exec = Arc::new(sort.with_preserve_partitioning(true));
        let statistics = sort_exec.statistics_by_partition()?;
        assert_eq!(statistics.len(), 2);
        assert_eq!(statistics[0].num_rows, Precision::Exact(2));
        assert_eq!(statistics[1].num_rows, Precision::Exact(2));
        assert_eq!(statistics[0].column_statistics.len(), 2);
        assert_eq!(statistics[1].column_statistics.len(), 2);
        assert_eq!(statistics[0].total_byte_size, Precision::Exact(110));
        assert_eq!(statistics[1].total_byte_size, Precision::Exact(110));
        assert_eq!(
            statistics[0].column_statistics[0].null_count,
            Precision::Exact(0)
        );
        assert_eq!(
            statistics[0].column_statistics[0].max_value,
            Precision::Exact(ScalarValue::Int32(Some(4)))
        );
        assert_eq!(
            statistics[0].column_statistics[0].min_value,
            Precision::Exact(ScalarValue::Int32(Some(3)))
        );
        assert_eq!(
            statistics[1].column_statistics[0].null_count,
            Precision::Exact(0)
        );
        assert_eq!(
            statistics[1].column_statistics[0].max_value,
            Precision::Exact(ScalarValue::Int32(Some(2)))
        );
        assert_eq!(
            statistics[1].column_statistics[0].min_value,
            Precision::Exact(ScalarValue::Int32(Some(1)))
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_statistics_by_partition_of_filter() -> datafusion_common::Result<()> {
        let scan = generate_listing_table_with_statistics(None).await;
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let predicate = binary(
            Arc::new(Column::new("id", 0)),
            Operator::Lt,
            lit(1i32),
            &schema,
        )?;
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, scan)?);
        let _full_statistics = filter.statistics()?;
        // The full statistics is invalid, at least, we can improve the selectivity estimation of the filter
        /*
        Statistics {
           num_rows: Inexact(0),
           total_byte_size: Inexact(0),
           column_statistics: [
               ColumnStatistics {
                   null_count: Exact(0),
                   max_value: Exact(NULL),
                   min_value: Exact(NULL),
                   sum_value: Exact(NULL),
                   distinct_count: Exact(0),
               },
               ColumnStatistics {
                   null_count: Exact(0),
                   max_value: Exact(NULL),
                   min_value: Exact(NULL),
                   sum_value: Exact(NULL),
                   distinct_count: Exact(0),
               },
           ],
        }
        */
        let statistics = filter.statistics_by_partition()?;
        // Also the statistics of each partition is also invalid due to above
        // But we can ensure the current behavior by tests
        assert_eq!(statistics.len(), 4);
        for stat in &statistics {
            assert_eq!(stat.column_statistics.len(), 2);
            assert_eq!(stat.total_byte_size, Precision::Inexact(0));
            assert_eq!(stat.num_rows, Precision::Inexact(0));
            assert_eq!(stat.column_statistics[0].null_count, Precision::Exact(0));
            assert_eq!(
                stat.column_statistics[0].max_value,
                Precision::Exact(ScalarValue::Null)
            );
            assert_eq!(
                stat.column_statistics[0].min_value,
                Precision::Exact(ScalarValue::Null)
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_statistic_by_partition_of_union() -> datafusion_common::Result<()> {
        let scan = generate_listing_table_with_statistics(Some(2)).await;
        let union_exec = Arc::new(UnionExec::new(vec![scan.clone(), scan]));
        let statistics = union_exec.statistics_by_partition()?;
        // Check that we have 4 partitions (2 from each scan)
        assert_eq!(statistics.len(), 4);

        // Verify first partition (from first scan)
        assert_eq!(statistics[0].num_rows, Precision::Exact(2));
        assert_eq!(statistics[0].total_byte_size, Precision::Exact(110));
        assert_eq!(statistics[0].column_statistics.len(), 2);
        assert_eq!(
            statistics[0].column_statistics[0].null_count,
            Precision::Exact(0)
        );
        assert_eq!(
            statistics[0].column_statistics[0].max_value,
            Precision::Exact(ScalarValue::Int32(Some(4)))
        );
        assert_eq!(
            statistics[0].column_statistics[0].min_value,
            Precision::Exact(ScalarValue::Int32(Some(3)))
        );

        // Verify second partition (from first scan)
        assert_eq!(statistics[1].num_rows, Precision::Exact(2));
        assert_eq!(statistics[1].total_byte_size, Precision::Exact(110));
        assert_eq!(
            statistics[1].column_statistics[0].null_count,
            Precision::Exact(0)
        );
        assert_eq!(
            statistics[1].column_statistics[0].max_value,
            Precision::Exact(ScalarValue::Int32(Some(2)))
        );
        assert_eq!(
            statistics[1].column_statistics[0].min_value,
            Precision::Exact(ScalarValue::Int32(Some(1)))
        );

        // Verify third partition (from second scan - same as first partition)
        assert_eq!(statistics[2].num_rows, Precision::Exact(2));
        assert_eq!(statistics[2].total_byte_size, Precision::Exact(110));
        assert_eq!(
            statistics[2].column_statistics[0].max_value,
            Precision::Exact(ScalarValue::Int32(Some(4)))
        );
        assert_eq!(
            statistics[2].column_statistics[0].min_value,
            Precision::Exact(ScalarValue::Int32(Some(3)))
        );

        // Verify fourth partition (from second scan - same as second partition)
        assert_eq!(statistics[3].num_rows, Precision::Exact(2));
        assert_eq!(statistics[3].total_byte_size, Precision::Exact(110));
        assert_eq!(
            statistics[3].column_statistics[0].max_value,
            Precision::Exact(ScalarValue::Int32(Some(2)))
        );
        assert_eq!(
            statistics[3].column_statistics[0].min_value,
            Precision::Exact(ScalarValue::Int32(Some(1)))
        );

        Ok(())
    }
}
