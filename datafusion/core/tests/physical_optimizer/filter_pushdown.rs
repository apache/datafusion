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
use datafusion_common::config::{ConfigOptions, TableParquetOptions};
use datafusion_common::internal_err;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_physical_optimizer::filter_pushdown::FilterPushdown;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::{displayable, ExecutionPlan};
use insta;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, OnceLock};

#[test]
fn test_pushdown_into_scan() {
    let scan = parquet_scan();
    let predicate = col_lit_predicate("a", "foo", schema());
    let plan = Arc::new(FilterExec::try_new(predicate, scan).unwrap());

    // expect the predicate to be pushed down into the DataSource
    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown{}),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo
        -   DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=parquet
      output:
        Ok:
          - FilterExec: a@0 = foo
          -   DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=parquet, predicate=a@0 = foo
    "
    );
}

#[test]
fn test_parquet_pushdown() {
    // filter should be pushed down into the parquet scan with two filters
    let scan = parquet_scan();
    let predicate1 = col_lit_predicate("a", "foo", schema());
    let filter1 = Arc::new(FilterExec::try_new(predicate1, scan).unwrap());
    let predicate2 = col_lit_predicate("b", "bar", schema());
    let plan = Arc::new(FilterExec::try_new(predicate2, filter1).unwrap());

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown{}),
        @r"
    OptimizationTest:
      input:
        - FilterExec: b@1 = bar
        -   FilterExec: a@0 = foo
        -     DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=parquet
      output:
        Ok:
          - FilterExec: b@1 = bar
          -   FilterExec: a@0 = foo AND b@1 = bar
          -     DataSourceExec: file_groups={0 groups: []}, projection=[a, b, c], file_type=parquet, predicate=b@1 = bar AND a@0 = foo
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

/// Return a execution plan that reads from a parquet file
fn parquet_scan() -> Arc<dyn ExecutionPlan> {
    let schema = schema();
    let source = ParquetSource::new(TableParquetOptions::default())
        .with_schema(Arc::clone(schema));
    let base_config = FileScanConfigBuilder::new(
        ObjectStoreUrl::parse("test://").unwrap(),
        Arc::clone(schema),
        source,
    )
    .build();
    DataSourceExec::from_data_source(base_config)
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
