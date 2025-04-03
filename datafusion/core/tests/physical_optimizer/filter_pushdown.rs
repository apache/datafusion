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
use datafusion_common::internal_err;
use datafusion_common::{config::ConfigOptions, Statistics};
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::{
    file::{FileSource, FileSourceFilterPushdownResult},
    file_scan_config::FileScanConfig,
    file_stream::FileOpener,
};
use datafusion_physical_expr::{conjunction, PhysicalExprRef};
use datafusion_physical_expr_common::physical_expr::fmt_sql;
use datafusion_physical_optimizer::filter_pushdown::PushdownFilter;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::{
    displayable, execution_plan::FilterSupport, metrics::ExecutionPlanMetricsSet,
    DisplayFormatType, ExecutionPlan,
};
use object_store::ObjectStore;
use std::sync::{Arc, OnceLock};
use std::{
    any::Any,
    fmt::{Display, Formatter},
};

/// A placeholder data source that accepts filter pushdown
#[derive(Clone)]
struct TestSource {
    support: FilterSupport,
    predicate: Option<PhysicalExprRef>,
    statistics: Option<Statistics>,
}

impl TestSource {
    fn new(support: FilterSupport) -> Self {
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

    fn statistics(&self) -> datafusion_common::Result<Statistics> {
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
                let predicate_string = self
                    .predicate
                    .as_ref()
                    .map(|p| format!(", predicate={p}"))
                    .unwrap_or_default();

                write!(f, "{}", predicate_string)
            }
            DisplayFormatType::TreeRender => {
                if let Some(predicate) = &self.predicate {
                    writeln!(f, "predicate={}", fmt_sql(predicate.as_ref()))?;
                }
                Ok(())
            }
        }
    }

    fn push_down_filters(
        &self,
        filters: &[PhysicalExprRef],
    ) -> datafusion_common::Result<Option<FileSourceFilterPushdownResult>> {
        let new = Arc::new(TestSource {
            support: self.support,
            predicate: Some(conjunction(filters.iter().map(Arc::clone))),
            statistics: self.statistics.clone(),
        });
        Ok(Some(FileSourceFilterPushdownResult::new(
            new,
            vec![self.support; filters.len()],
        )))
    }
}

fn test_scan(support: FilterSupport) -> Arc<dyn ExecutionPlan> {
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
    let scan = test_scan(FilterSupport::HandledExact);
    let predicate = col_lit_predicate("a", "foo", schema());
    let plan = Arc::new(FilterExec::try_new(predicate, scan).unwrap());

    // expect the predicate to be pushed down into the DataSource
    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownFilter{}),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo
        -   DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test
      output:
        Ok:
          - DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, predicate=a@0 = foo
    "
    );
}

#[test]
fn test_parquet_pushdown() {
    // filter should be pushed down into the parquet scan with two filters
    let scan = test_scan(FilterSupport::HandledExact);
    let predicate1 = col_lit_predicate("a", "foo", schema());
    let filter1 = Arc::new(FilterExec::try_new(predicate1, scan).unwrap());
    let predicate2 = col_lit_predicate("b", "bar", schema());
    let plan = Arc::new(FilterExec::try_new(predicate2, filter1).unwrap());

    insta::assert_snapshot!(
        OptimizationTest::new(plan, PushdownFilter{}),
        @r"
    OptimizationTest:
      input:
        - FilterExec: b@1 = bar
        -   FilterExec: a@0 = foo
        -     DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test
      output:
        Ok:
          - DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=test, predicate=a@0 = foo AND b@1 = bar
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
    pub fn new<O>(input_plan: Arc<dyn ExecutionPlan>, opt: O) -> Self
    where
        O: PhysicalOptimizerRule,
    {
        Self::new_with_config(input_plan, opt, &ConfigOptions::default())
    }

    pub fn new_with_config<O>(
        input_plan: Arc<dyn ExecutionPlan>,
        opt: O,
        config: &ConfigOptions,
    ) -> Self
    where
        O: PhysicalOptimizerRule,
    {
        let input = format_execution_plan(&input_plan);

        let input_schema = input_plan.schema();

        let output_result = opt.optimize(input_plan, config);
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
