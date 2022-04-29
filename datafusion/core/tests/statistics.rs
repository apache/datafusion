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

//! This module contains end to end tests of statistics propagation

use std::{any::Any, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::{
    datasource::TableProvider,
    error::Result,
    logical_plan::Expr,
    physical_plan::{
        expressions::PhysicalSortExpr, project_schema, ColumnStatistics,
        DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
        Statistics,
    },
    prelude::SessionContext,
    scalar::ScalarValue,
};

use async_trait::async_trait;
use datafusion::execution::context::TaskContext;

/// This is a testing structure for statistics
/// It will act both as a table provider and execution plan
#[derive(Debug, Clone)]
struct StatisticsValidation {
    stats: Statistics,
    schema: Arc<Schema>,
}

impl StatisticsValidation {
    fn new(stats: Statistics, schema: SchemaRef) -> Self {
        assert!(
            stats
                .column_statistics
                .as_ref()
                .map(|cols| cols.len() == schema.fields().len())
                .unwrap_or(true),
            "if defined, the column statistics vector length should be the number of fields"
        );
        Self { stats, schema }
    }
}

#[async_trait]
impl TableProvider for StatisticsValidation {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        // limit is ignored because it is not mandatory for a `TableProvider` to honor it
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Filters should not be pushed down as they are marked as unsupported by default.
        assert_eq!(
            0,
            filters.len(),
            "Unsupported expressions should not be pushed down"
        );
        let projection = match projection.clone() {
            Some(p) => p,
            None => (0..self.schema.fields().len()).collect(),
        };
        let projected_schema = project_schema(&self.schema, Some(&projection))?;

        let current_stat = self.stats.clone();

        let proj_col_stats = current_stat
            .column_statistics
            .map(|col_stat| projection.iter().map(|i| col_stat[*i].clone()).collect());

        Ok(Arc::new(Self::new(
            Statistics {
                is_exact: current_stat.is_exact,
                num_rows: current_stat.num_rows,
                column_statistics: proj_col_stats,
                // TODO stats: knowing the type of the new columns we can guess the output size
                total_byte_size: None,
            },
            projected_schema,
        )))
    }
}

#[async_trait]
impl ExecutionPlan for StatisticsValidation {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(2)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    async fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unimplemented!("This plan only serves for testing statistics")
    }

    fn statistics(&self) -> Statistics {
        self.stats.clone()
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "StatisticsValidation: col_count={}, row_count={:?}",
                    self.schema.fields().len(),
                    self.stats.num_rows,
                )
            }
        }
    }
}

fn init_ctx(stats: Statistics, schema: Schema) -> Result<SessionContext> {
    let ctx = SessionContext::new();
    let provider: Arc<dyn TableProvider> =
        Arc::new(StatisticsValidation::new(stats, Arc::new(schema)));
    ctx.register_table("stats_table", provider)?;
    Ok(ctx)
}

fn fully_defined() -> (Statistics, Schema) {
    (
        Statistics {
            num_rows: Some(13),
            is_exact: true,
            total_byte_size: None, // ignore byte size for now
            column_statistics: Some(vec![
                ColumnStatistics {
                    distinct_count: Some(2),
                    max_value: Some(ScalarValue::Int32(Some(1023))),
                    min_value: Some(ScalarValue::Int32(Some(-24))),
                    null_count: Some(0),
                },
                ColumnStatistics {
                    distinct_count: Some(13),
                    max_value: Some(ScalarValue::Int64(Some(5486))),
                    min_value: Some(ScalarValue::Int64(Some(-6783))),
                    null_count: Some(5),
                },
            ]),
        },
        Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int64, false),
        ]),
    )
}

#[tokio::test]
async fn sql_basic() -> Result<()> {
    let (stats, schema) = fully_defined();
    let ctx = init_ctx(stats.clone(), schema)?;

    let df = ctx.sql("SELECT * from stats_table").await.unwrap();

    let physical_plan = ctx
        .create_physical_plan(&df.to_logical_plan()?)
        .await
        .unwrap();

    // the statistics should be those of the source
    assert_eq!(stats, physical_plan.statistics());

    Ok(())
}

#[tokio::test]
async fn sql_filter() -> Result<()> {
    let (stats, schema) = fully_defined();
    let ctx = init_ctx(stats, schema)?;

    let df = ctx
        .sql("SELECT * FROM stats_table WHERE c1 = 5")
        .await
        .unwrap();

    let physical_plan = ctx
        .create_physical_plan(&df.to_logical_plan()?)
        .await
        .unwrap();

    // with a filtering condition we loose all knowledge about the statistics
    assert_eq!(Statistics::default(), physical_plan.statistics());

    Ok(())
}

#[tokio::test]
async fn sql_limit() -> Result<()> {
    let (stats, schema) = fully_defined();
    let ctx = init_ctx(stats.clone(), schema)?;

    let df = ctx.sql("SELECT * FROM stats_table LIMIT 5").await.unwrap();
    let physical_plan = ctx
        .create_physical_plan(&df.to_logical_plan()?)
        .await
        .unwrap();
    // when the limit is smaller than the original number of lines
    // we loose all statistics except the for number of rows which becomes the limit
    assert_eq!(
        Statistics {
            num_rows: Some(5),
            is_exact: true,
            ..Default::default()
        },
        physical_plan.statistics()
    );

    let df = ctx
        .sql("SELECT * FROM stats_table LIMIT 100")
        .await
        .unwrap();
    let physical_plan = ctx
        .create_physical_plan(&df.to_logical_plan()?)
        .await
        .unwrap();
    // when the limit is larger than the original number of lines, statistics remain unchanged
    assert_eq!(stats, physical_plan.statistics());

    Ok(())
}

#[tokio::test]
async fn sql_window() -> Result<()> {
    let (stats, schema) = fully_defined();
    let ctx = init_ctx(stats.clone(), schema)?;

    let df = ctx
        .sql("SELECT c2, sum(c1) over (partition by c2) FROM stats_table")
        .await
        .unwrap();

    let physical_plan = ctx
        .create_physical_plan(&df.to_logical_plan()?)
        .await
        .unwrap();

    let result = physical_plan.statistics();

    assert_eq!(stats.num_rows, result.num_rows);
    assert!(result.column_statistics.is_some());
    let col_stats = result.column_statistics.unwrap();
    assert_eq!(2, col_stats.len());
    assert_eq!(stats.column_statistics.unwrap()[1], col_stats[0]);

    Ok(())
}
