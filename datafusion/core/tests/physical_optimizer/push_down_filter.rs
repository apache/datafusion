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

use std::sync::{Arc, OnceLock};
use std::{
    any::Any,
    fmt::{Display, Formatter},
};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::{
    datasource::object_store::ObjectStoreUrl,
    logical_expr::Operator,
    physical_plan::{
        expressions::{BinaryExpr, Column, Literal},
        PhysicalExpr,
    },
    scalar::ScalarValue,
};
use datafusion_common::{config::ConfigOptions, Statistics};
use datafusion_common::{internal_err, Result};
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::{
    file::FileSource, file_scan_config::FileScanConfig, file_stream::FileOpener,
};
use datafusion_expr::test::function_stub::count_udaf;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr::{
    aggregate::AggregateExprBuilder, conjunction, Partitioning,
};
use datafusion_physical_expr_common::physical_expr::fmt_sql;
use datafusion_physical_optimizer::push_down_filter::PushdownFilter;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::filter_pushdown::{
    filter_pushdown_not_supported, FilterDescription, FilterPushdownResult,
    FilterPushdownSupport,
};
use datafusion_physical_plan::{
    aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy},
    coalesce_batches::CoalesceBatchesExec,
    filter::FilterExec,
    repartition::RepartitionExec,
};
use datafusion_physical_plan::{
    displayable, metrics::ExecutionPlanMetricsSet, DisplayFormatType, ExecutionPlan,
};

use object_store::ObjectStore;

/// A placeholder data source that accepts filter pushdown
#[derive(Clone, Default)]
struct TestSource {
    support: bool,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    statistics: Option<Statistics>,
}

impl TestSource {
    fn new(support: bool) -> Self {
        Self {
            support,
            predicate: None,
            statistics: None,
        }
    }
}

impl FileSource for TestSource {
    fn create_file_opener(
        &self,
        _object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> Arc<dyn FileOpener> {
        todo!("should not be called")
    }

    fn as_any(&self) -> &dyn Any {
        todo!("should not be called")
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        todo!("should not be called")
    }

    fn with_schema(&self, _schema: SchemaRef) -> Arc<dyn FileSource> {
        todo!("should not be called")
    }

    fn with_projection(&self, _config: &FileScanConfig) -> Arc<dyn FileSource> {
        todo!("should not be called")
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        Arc::new(TestSource {
            statistics: Some(statistics),
            ..self.clone()
        })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        todo!("should not be called")
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self
            .statistics
            .as_ref()
            .expect("statistics not set")
            .clone())
    }

    fn file_type(&self) -> &str {
        "test"
    }

    fn fmt_extra(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let support = format!(", pushdown_supported={}", self.support);

                let predicate_string = self
                    .predicate
                    .as_ref()
                    .map(|p| format!(", predicate={p}"))
                    .unwrap_or_default();

                write!(f, "{}{}", support, predicate_string)
            }
            DisplayFormatType::TreeRender => {
                if let Some(predicate) = &self.predicate {
                    writeln!(f, "pushdown_supported={}", fmt_sql(predicate.as_ref()))?;
                    writeln!(f, "predicate={}", fmt_sql(predicate.as_ref()))?;
                }
                Ok(())
            }
        }
    }

    fn try_pushdown_filters(
        &self,
        mut fd: FilterDescription,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownResult<Arc<dyn FileSource>>> {
        if self.support && config.execution.parquet.pushdown_filters {
            if let Some(internal) = self.predicate.as_ref() {
                fd.filters.push(Arc::clone(internal));
            }
            let all_filters = fd.take_description();

            Ok(FilterPushdownResult {
                support: FilterPushdownSupport::Supported {
                    child_descriptions: vec![],
                    op: Arc::new(TestSource {
                        support: true,
                        predicate: Some(conjunction(all_filters)),
                        statistics: self.statistics.clone(), // should be updated in reality
                    }),
                    revisit: false,
                },
                remaining_description: FilterDescription::empty(),
            })
        } else {
            Ok(filter_pushdown_not_supported(fd))
        }
    }
}

fn test_scan(support: bool) -> Arc<dyn ExecutionPlan> {
    let schema = schema();
    let source = Arc::new(TestSource::new(support));
    let base_config = FileScanConfigBuilder::new(
        ObjectStoreUrl::parse("test://").unwrap(),
        Arc::clone(schema),
        source,
    )
    .build();
    DataSourceExec::from_data_source(base_config)
}

#[test]
fn test_pushdown_into_scan() {
    let scan = test_scan(true);
    let predicate = col_lit_predicate("a", "foo", schema());
    let plan = Arc::new(FilterExec::try_new(predicate, scan).unwrap());

    // expect the predicate to be pushed down into the DataSource
    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownFilter{}, true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo
        -   DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo
    "
    );
}

/// Show that we can use config options to determine how to do pushdown.
#[test]
fn test_pushdown_into_scan_with_config_options() {
    let scan = test_scan(true);
    let predicate = col_lit_predicate("a", "foo", schema());
    let plan = Arc::new(FilterExec::try_new(predicate, scan).unwrap()) as _;

    let mut cfg = ConfigOptions::default();
    insta::assert_snapshot!(
        OptimizationTest::new(
            Arc::clone(&plan),
            PushdownFilter {},
            false
        ),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo
        -   DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - FilterExec: a@0 = foo
          -   DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true
    "
    );

    cfg.execution.parquet.pushdown_filters = true;
    insta::assert_snapshot!(
        OptimizationTest::new(
            plan,
            PushdownFilter {},
            true
        ),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo
        -   DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo
    "
    );
}

#[test]
fn test_filter_collapse() {
    // filter should be pushed down into the parquet scan with two filters
    let scan = test_scan(true);
    let predicate1 = col_lit_predicate("a", "foo", schema());
    let filter1 = Arc::new(FilterExec::try_new(predicate1, scan).unwrap());
    let predicate2 = col_lit_predicate("b", "bar", schema());
    let plan = Arc::new(FilterExec::try_new(predicate2, filter1).unwrap());

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownFilter{}, true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: b@1 = bar
        -   FilterExec: a@0 = foo
        -     DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=b@1 = bar AND a@0 = foo
    "
    );
}

#[test]
fn test_filter_with_projection() {
    let scan = test_scan(true);
    let projection = vec![1, 0];
    let predicate = col_lit_predicate("a", "foo", schema());
    let plan = Arc::new(
        FilterExec::try_new(predicate, Arc::clone(&scan))
            .unwrap()
            .with_projection(Some(projection))
            .unwrap(),
    );

    // expect the predicate to be pushed down into the DataSource but the FilterExec to be converted to ProjectionExec
    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownFilter{}, true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo, projection=[b@1, a@0]
        -   DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - ProjectionExec: expr=[b@1 as b, a@0 as a]
          -   DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo
    ",
    );

    // add a test where the filter is on a column that isn't included in the output
    let projection = vec![1];
    let predicate = col_lit_predicate("a", "foo", schema());
    let plan = Arc::new(
        FilterExec::try_new(predicate, scan)
            .unwrap()
            .with_projection(Some(projection))
            .unwrap(),
    );
    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownFilter{},true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo, projection=[b@1]
        -   DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - ProjectionExec: expr=[b@1 as b]
          -   DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo
    "
    );
}

#[test]
fn test_push_down_through_transparent_nodes() {
    // expect the predicate to be pushed down into the DataSource
    let scan = test_scan(true);
    let coalesce = Arc::new(CoalesceBatchesExec::new(scan, 1));
    let predicate = col_lit_predicate("a", "foo", schema());
    let filter = Arc::new(FilterExec::try_new(predicate, coalesce).unwrap());
    let repartition = Arc::new(
        RepartitionExec::try_new(filter, Partitioning::RoundRobinBatch(1)).unwrap(),
    );
    let predicate = col_lit_predicate("b", "bar", schema());
    let plan = Arc::new(FilterExec::try_new(predicate, repartition).unwrap());

    // expect the predicate to be pushed down into the DataSource
    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownFilter{},true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: b@1 = bar
        -   RepartitionExec: partitioning=RoundRobinBatch(1), input_partitions=0
        -     FilterExec: a@0 = foo
        -       CoalesceBatchesExec: target_batch_size=1
        -         DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - RepartitionExec: partitioning=RoundRobinBatch(1), input_partitions=0
          -   CoalesceBatchesExec: target_batch_size=1
          -     DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=b@1 = bar AND a@0 = foo
    "
    );
}

#[test]
fn test_no_pushdown_through_aggregates() {
    // There are 2 important points here:
    // 1. The outer filter **is not** pushed down at all because we haven't implemented pushdown support
    //    yet for AggregateExec.
    // 2. The inner filter **is** pushed down into the DataSource.
    let scan = test_scan(true);

    let coalesce = Arc::new(CoalesceBatchesExec::new(scan, 10));

    let filter = Arc::new(
        FilterExec::try_new(col_lit_predicate("a", "foo", schema()), coalesce).unwrap(),
    );

    let aggregate_expr =
        vec![
            AggregateExprBuilder::new(count_udaf(), vec![col("a", schema()).unwrap()])
                .schema(Arc::clone(schema()))
                .alias("cnt")
                .build()
                .map(Arc::new)
                .unwrap(),
        ];
    let group_by = PhysicalGroupBy::new_single(vec![
        (col("a", schema()).unwrap(), "a".to_string()),
        (col("b", schema()).unwrap(), "b".to_string()),
    ]);
    let aggregate = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Final,
            group_by,
            aggregate_expr.clone(),
            vec![None],
            filter,
            Arc::clone(schema()),
        )
        .unwrap(),
    );

    let coalesce = Arc::new(CoalesceBatchesExec::new(aggregate, 100));

    let predicate = col_lit_predicate("b", "bar", schema());
    let plan = Arc::new(FilterExec::try_new(predicate, coalesce).unwrap());

    // expect the predicate to be pushed down into the DataSource
    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownFilter{}, true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: b@1 = bar
        -   CoalesceBatchesExec: target_batch_size=100
        -     AggregateExec: mode=Final, gby=[a@0 as a, b@1 as b], aggr=[cnt], ordering_mode=PartiallySorted([0])
        -       FilterExec: a@0 = foo
        -         CoalesceBatchesExec: target_batch_size=10
        -           DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - FilterExec: b@1 = bar
          -   CoalesceBatchesExec: target_batch_size=100
          -     AggregateExec: mode=Final, gby=[a@0 as a, b@1 as b], aggr=[cnt]
          -       CoalesceBatchesExec: target_batch_size=10
          -         DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo
    "
    );
}

/// Schema:
/// a: String
/// b: String
/// c: f64
static TEST_SCHEMA: OnceLock<SchemaRef> = OnceLock::new();

fn schema() -> &'static SchemaRef {
    TEST_SCHEMA.get_or_init(|| {
        let fields = vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Float64, false),
        ];
        Arc::new(Schema::new(fields))
    })
}

/// Returns a predicate that is a binary expression col = lit
fn col_lit_predicate(
    column_name: &str,
    scalar_value: impl Into<ScalarValue>,
    schema: &Schema,
) -> Arc<dyn PhysicalExpr> {
    let scalar_value = scalar_value.into();
    Arc::new(BinaryExpr::new(
        Arc::new(Column::new_with_schema(column_name, schema).unwrap()),
        Operator::Eq,
        Arc::new(Literal::new(scalar_value)),
    ))
}

/// A harness for testing physical optimizers.
///
/// You can use this to test the output of a physical optimizer rule using insta snapshots
#[derive(Debug)]
pub struct OptimizationTest {
    input: Vec<String>,
    output: Result<Vec<String>, String>,
}

impl OptimizationTest {
    pub fn new<O>(
        input_plan: Arc<dyn ExecutionPlan>,
        opt: O,
        allow_pushdown_filters: bool,
    ) -> Self
    where
        O: PhysicalOptimizerRule,
    {
        let mut parquet_pushdown_config = ConfigOptions::default();
        parquet_pushdown_config.execution.parquet.pushdown_filters =
            allow_pushdown_filters;

        let input = format_execution_plan(&input_plan);
        let input_schema = input_plan.schema();

        let output_result = opt.optimize(input_plan, &parquet_pushdown_config);
        let output = output_result
            .and_then(|plan| {
                if opt.schema_check() && (plan.schema() != input_schema) {
                    internal_err!(
                        "Schema mismatch:\n\nBefore:\n{:?}\n\nAfter:\n{:?}",
                        input_schema,
                        plan.schema()
                    )
                } else {
                    Ok(plan)
                }
            })
            .map(|plan| format_execution_plan(&plan))
            .map_err(|e| e.to_string());

        Self { input, output }
    }
}

impl Display for OptimizationTest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "OptimizationTest:")?;
        writeln!(f, "  input:")?;
        for line in &self.input {
            writeln!(f, "    - {line}")?;
        }
        writeln!(f, "  output:")?;
        match &self.output {
            Ok(output) => {
                writeln!(f, "    Ok:")?;
                for line in output {
                    writeln!(f, "      - {line}")?;
                }
            }
            Err(err) => {
                writeln!(f, "    Err: {err}")?;
            }
        }
        Ok(())
    }
}

pub fn format_execution_plan(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    format_lines(&displayable(plan.as_ref()).indent(false).to_string())
}

fn format_lines(s: &str) -> Vec<String> {
    s.trim().split('\n').map(|s| s.to_string()).collect()
}
