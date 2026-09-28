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

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::context::TaskContext;
use datafusion::{
    datasource::{TableProvider, TableType},
    error::Result,
    logical_expr::Expr,
    physical_plan::{
        ColumnStatistics, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
        PlanProperties, SendableRecordBatchStream, Statistics,
    },
    prelude::SessionContext,
    scalar::ScalarValue,
};
use datafusion_catalog::Session;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{project_schema, stats::Precision};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::{
    ChildrenPropertiesMode, ReplaceChildrenOptions, StatisticsArgs, StatisticsContext,
};

use async_trait::async_trait;

/// This is a testing structure for statistics
/// It will act both as a table provider and execution plan
#[derive(Debug, Clone)]
struct StatisticsValidation {
    stats: Statistics,
    schema: Arc<Schema>,
    cache: Arc<PlanProperties>,
}

impl StatisticsValidation {
    fn new(stats: Statistics, schema: SchemaRef) -> Self {
        assert_eq!(
            stats.column_statistics.len(),
            schema.fields().len(),
            "the column statistics vector length should be the number of fields"
        );
        let cache = Self::compute_properties(schema.clone());
        Self {
            stats,
            schema,
            cache: Arc::new(cache),
        }
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(2),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

#[async_trait]
impl TableProvider for StatisticsValidation {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&[usize]>,
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
        let projection = match projection.map(|p| p.to_vec()) {
            Some(p) => p,
            None => (0..self.schema.fields().len()).collect(),
        };
        let projected_schema = project_schema(&self.schema, Some(&projection))?;

        let current_stat = self.stats.clone();

        let proj_col_stats = projection
            .iter()
            .map(|i| current_stat.column_statistics[*i].clone())
            .collect();
        Ok(Arc::new(Self::new(
            Statistics {
                num_rows: current_stat.num_rows,
                column_statistics: proj_col_stats,
                // TODO stats: knowing the type of the new columns we can guess the output size
                total_byte_size: Precision::Absent,
            },
            projected_schema,
        )))
    }
}

impl DisplayAs for StatisticsValidation {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "StatisticsValidation: col_count={}, row_count={:?}",
                    self.schema.fields().len(),
                    self.stats.num_rows,
                )
            }
            DisplayFormatType::TreeRender => {
                // TODO: collect info
                write!(f, "")
            }
        }
    }
}

impl ExecutionPlan for StatisticsValidation {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn replace_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
        _: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unimplemented!("This plan only serves for testing statistics")
    }

    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        if args.partition().is_some() {
            Ok(Arc::new(Statistics::new_unknown(&self.schema)))
        } else {
            Ok(Arc::new(self.stats.clone()))
        }
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        ) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
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
            num_rows: Precision::Exact(13),
            total_byte_size: Precision::Absent, // ignore byte size for now
            column_statistics: vec![
                ColumnStatistics {
                    distinct_count: Precision::Exact(2),
                    max_value: Precision::Exact(ScalarValue::Int32(Some(1023))),
                    min_value: Precision::Exact(ScalarValue::Int32(Some(-24))),
                    sum_value: Precision::Exact(ScalarValue::Int64(Some(10))),
                    null_count: Precision::Exact(0),
                    byte_size: Precision::Absent,
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(13),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(5486))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(-6783))),
                    sum_value: Precision::Exact(ScalarValue::Int64(Some(10))),
                    null_count: Precision::Exact(5),
                    byte_size: Precision::Absent,
                },
            ],
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
    let physical_plan = df.create_physical_plan().await.unwrap();

    // the statistics should be those of the source
    assert_eq!(
        stats,
        *StatisticsContext::new()
            .compute(physical_plan.as_ref(), &StatisticsArgs::new())?
    );

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

    let physical_plan = df.create_physical_plan().await.unwrap();
    let stats = StatisticsContext::new()
        .compute(physical_plan.as_ref(), &StatisticsArgs::new())?;
    assert_eq!(stats.num_rows, Precision::Inexact(7));

    Ok(())
}

fn string_filter_ctx(
    data_type: DataType,
    distinct_count: Precision<usize>,
) -> Result<SessionContext> {
    init_ctx(
        Statistics {
            num_rows: Precision::Exact(1000),
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Exact(200),
                distinct_count,
                ..ColumnStatistics::new_unknown()
            }],
        },
        Schema::new(vec![Field::new("c1", data_type, true)]),
    )
}

async fn string_filter_rows(
    ctx: &SessionContext,
    predicate: &str,
) -> Result<Precision<usize>> {
    let plan = ctx
        .sql(&format!("SELECT * FROM stats_table WHERE {predicate}"))
        .await?
        .create_physical_plan()
        .await?;
    Ok(StatisticsContext::new()
        .compute(plan.as_ref(), &StatisticsArgs::new())?
        .num_rows)
}

#[tokio::test]
async fn sql_string_filter_selectivity() -> Result<()> {
    // There are 800 non-null rows and 20 distinct strings. Equality estimates
    // 40 matches, and negated predicates exclude nulls as well as matches.
    // LIKE estimates distinguish unanchored patterns from patterns anchored
    // at one or both ends; escaped wildcards are literal characters.
    let cases = [
        ("c1 = 'foo'", 40),
        ("'foo' = c1", 40),
        ("c1 != 'foo'", 760),
        ("'foo' != c1", 760),
        ("c1 LIKE 'foo'", 40),
        ("c1 NOT LIKE 'foo'", 760),
        ("c1 LIKE '%foo%'", 160),
        ("c1 NOT LIKE '%foo%'", 640),
        ("c1 LIKE 'foo%'", 80),
        ("c1 NOT LIKE 'foo%'", 720),
        ("c1 LIKE '%foo'", 80),
        ("c1 NOT LIKE '%foo'", 720),
        ("c1 LIKE 'f_o'", 40),
        ("c1 NOT LIKE 'f_o'", 760),
        (r"c1 LIKE 'foo\%'", 40),
        (r"c1 NOT LIKE 'foo\%'", 760),
        (r"c1 LIKE 'foo\%%'", 80),
        (r"c1 NOT LIKE 'foo\%%'", 720),
        (r"c1 LIKE 'foo\'", 40),
        (r"c1 NOT LIKE 'foo\'", 760),
        (r"c1 LIKE '%foo\'", 80),
        (r"c1 NOT LIKE '%foo\'", 720),
        // Case-sensitive NDV cannot estimate a case-insensitive equality.
        ("c1 ILIKE 'foo'", 160),
        ("c1 NOT ILIKE 'foo'", 640),
    ];
    for data_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        let ctx = string_filter_ctx(data_type.clone(), Precision::Exact(20))?;
        for (predicate, expected_rows) in cases {
            assert_eq!(
                string_filter_rows(&ctx, predicate).await?,
                Precision::Inexact(expected_rows),
                "{data_type:?}: {predicate}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn sql_string_filter_without_distinct_count() -> Result<()> {
    let ctx = string_filter_ctx(DataType::Utf8View, Precision::Absent)?;
    // Without NDV, use the configured default (20%) over non-null rows.
    for (predicate, expected_rows) in [
        ("c1 = 'foo'", 160),
        ("c1 != 'foo'", 640),
        ("c1 LIKE 'foo'", 160),
        ("c1 NOT LIKE 'foo'", 640),
    ] {
        assert_eq!(
            string_filter_rows(&ctx, predicate).await?,
            Precision::Inexact(expected_rows),
            "{predicate}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn sql_string_filter_custom_selectivity() -> Result<()> {
    let ctx = string_filter_ctx(DataType::Utf8View, Precision::Absent)?;
    ctx.sql("SET datafusion.optimizer.default_filter_selectivity = 40")
        .await?
        .collect()
        .await?;

    for (predicate, expected_rows) in [
        ("c1 = 'foo'", 320),
        ("c1 != 'foo'", 480),
        ("c1 LIKE '%foo%'", 320),
        ("c1 NOT LIKE '%foo%'", 480),
        ("c1 LIKE 'foo%'", 160),
        ("c1 NOT LIKE 'foo%'", 640),
    ] {
        assert_eq!(
            string_filter_rows(&ctx, predicate).await?,
            Precision::Inexact(expected_rows),
            "{predicate}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn sql_limit() -> Result<()> {
    let (stats, schema) = fully_defined();
    let ctx = init_ctx(stats.clone(), schema)?;

    let df = ctx.sql("SELECT * FROM stats_table LIMIT 5").await.unwrap();
    let physical_plan = df.create_physical_plan().await.unwrap();
    // when the limit is smaller than the original number of lines we mark the statistics as inexact
    // and cap NDV at the new row count
    let limit_stats = StatisticsContext::new()
        .compute(physical_plan.as_ref(), &StatisticsArgs::new())?;
    assert_eq!(limit_stats.num_rows, Precision::Exact(5));
    // c1: NDV=2 stays at 2 (already below limit of 5)
    assert_eq!(
        limit_stats.column_statistics[0].distinct_count,
        Precision::Inexact(2)
    );
    // c2: NDV=13 capped to 5 (the limit row count)
    assert_eq!(
        limit_stats.column_statistics[1].distinct_count,
        Precision::Inexact(5)
    );

    let df = ctx
        .sql("SELECT * FROM stats_table LIMIT 100")
        .await
        .unwrap();
    let physical_plan = df.create_physical_plan().await.unwrap();
    // when the limit is larger than the original number of lines, statistics remain unchanged
    assert_eq!(
        stats,
        *StatisticsContext::new()
            .compute(physical_plan.as_ref(), &StatisticsArgs::new())?
    );

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

    let physical_plan = df.create_physical_plan().await.unwrap();

    let result = StatisticsContext::new()
        .compute(physical_plan.as_ref(), &StatisticsArgs::new())?;

    assert_eq!(stats.num_rows, result.num_rows);
    let col_stats = &result.column_statistics;
    assert_eq!(2, col_stats.len());
    assert_eq!(stats.column_statistics[1], col_stats[0]);

    Ok(())
}
