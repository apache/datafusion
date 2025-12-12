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

use arrow::datatypes::SchemaRef;
use arrow::{array::RecordBatch, compute::concat_batches};
use datafusion::{datasource::object_store::ObjectStoreUrl, physical_plan::PhysicalExpr};
use datafusion_common::{config::ConfigOptions, internal_err, Result};
use datafusion_datasource::{
    file::FileSource, file_scan_config::FileScanConfig,
    file_scan_config::FileScanConfigBuilder, file_stream::FileOpenFuture,
    file_stream::FileOpener, schema_adapter::DefaultSchemaAdapterFactory,
    schema_adapter::SchemaAdapterFactory, source::DataSourceExec, PartitionedFile,
};
use datafusion_execution::config::SessionConfig;
use datafusion_physical_expr_common::physical_expr::fmt_sql;
use datafusion_physical_optimizer::{OptimizerContext, PhysicalOptimizerRule};
use datafusion_physical_plan::filter::batch_filter;
use datafusion_physical_plan::filter_pushdown::{FilterPushdownPhase, PushedDown};
use datafusion_physical_plan::{
    displayable,
    filter::FilterExec,
    filter_pushdown::{
        ChildFilterDescription, ChildPushdownResult, FilterDescription,
        FilterPushdownPropagation,
    },
    metrics::ExecutionPlanMetricsSet,
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use futures::StreamExt;
use futures::{FutureExt, Stream};
use object_store::ObjectStore;
use std::{
    any::Any,
    fmt::{Display, Formatter},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
pub struct TestOpener {
    batches: Vec<RecordBatch>,
    batch_size: Option<usize>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    predicate: Option<Arc<dyn PhysicalExpr>>,
}

impl FileOpener for TestOpener {
    fn open(&self, _partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let mut batches = self.batches.clone();
        if self.batches.is_empty() {
            return Ok((async { Ok(TestStream::new(vec![]).boxed()) }).boxed());
        }
        if let Some(batch_size) = self.batch_size {
            let batch = concat_batches(&batches[0].schema(), &batches)?;
            let mut new_batches = Vec::new();
            for i in (0..batch.num_rows()).step_by(batch_size) {
                let end = std::cmp::min(i + batch_size, batch.num_rows());
                let batch = batch.slice(i, end - i);
                new_batches.push(batch);
            }
            batches = new_batches.into_iter().collect();
        }

        let factory = DefaultSchemaAdapterFactory::from_schema(Arc::clone(&self.schema));
        let (mapper, projection) = factory.map_schema(&batches[0].schema()).unwrap();
        let mut new_batches = Vec::new();
        for batch in batches {
            let batch = if let Some(predicate) = &self.predicate {
                batch_filter(&batch, predicate)?
            } else {
                batch
            };

            let batch = batch.project(&projection).unwrap();
            let batch = mapper.map_batch(batch).unwrap();
            new_batches.push(batch);
        }
        batches = new_batches;

        if let Some(projection) = &self.projection {
            batches = batches
                .into_iter()
                .map(|batch| batch.project(projection).unwrap())
                .collect();
        }

        let stream = TestStream::new(batches);

        Ok((async { Ok(stream.boxed()) }).boxed())
    }
}

/// A placeholder data source that accepts filter pushdown
#[derive(Clone)]
pub struct TestSource {
    support: bool,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    batch_size: Option<usize>,
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    projection: Option<Vec<usize>>,
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    table_schema: datafusion_datasource::TableSchema,
}

impl TestSource {
    pub fn new(schema: SchemaRef, support: bool, batches: Vec<RecordBatch>) -> Self {
        let table_schema =
            datafusion_datasource::TableSchema::new(Arc::clone(&schema), vec![]);
        Self {
            schema,
            support,
            metrics: ExecutionPlanMetricsSet::new(),
            batches,
            predicate: None,
            batch_size: None,
            projection: None,
            schema_adapter_factory: None,
            table_schema,
        }
    }
}

impl FileSource for TestSource {
    fn create_file_opener(
        &self,
        _object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        Ok(Arc::new(TestOpener {
            batches: self.batches.clone(),
            batch_size: self.batch_size,
            schema: Arc::clone(&self.schema),
            projection: self.projection.clone(),
            predicate: self.predicate.clone(),
        }))
    }

    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.predicate.clone()
    }

    fn as_any(&self) -> &dyn Any {
        todo!("should not be called")
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(TestSource {
            batch_size: Some(batch_size),
            ..self.clone()
        })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
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

                write!(f, "{support}{predicate_string}")
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
        mut filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        if self.support && config.execution.parquet.pushdown_filters {
            if let Some(internal) = self.predicate.as_ref() {
                filters.push(Arc::clone(internal));
            }
            let new_node = Arc::new(TestSource {
                predicate: datafusion_physical_expr::utils::conjunction_opt(
                    filters.clone(),
                ),
                ..self.clone()
            });
            Ok(FilterPushdownPropagation::with_parent_pushdown_result(
                vec![PushedDown::Yes; filters.len()],
            )
            .with_updated_node(new_node))
        } else {
            Ok(FilterPushdownPropagation::with_parent_pushdown_result(
                vec![PushedDown::No; filters.len()],
            ))
        }
    }

    fn with_schema_adapter_factory(
        &self,
        schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Result<Arc<dyn FileSource>> {
        Ok(Arc::new(Self {
            schema_adapter_factory: Some(schema_adapter_factory),
            ..self.clone()
        }))
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
    }

    fn table_schema(&self) -> &datafusion_datasource::TableSchema {
        &self.table_schema
    }
}

#[derive(Debug, Clone)]
pub struct TestScanBuilder {
    support: bool,
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
}

impl TestScanBuilder {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            support: false,
            batches: vec![],
            schema,
        }
    }

    pub fn with_support(mut self, support: bool) -> Self {
        self.support = support;
        self
    }

    pub fn with_batches(mut self, batches: Vec<RecordBatch>) -> Self {
        self.batches = batches;
        self
    }

    pub fn build(self) -> Arc<dyn ExecutionPlan> {
        let source = Arc::new(TestSource::new(
            Arc::clone(&self.schema),
            self.support,
            self.batches,
        ));
        let base_config =
            FileScanConfigBuilder::new(ObjectStoreUrl::parse("test://").unwrap(), source)
                .with_file(PartitionedFile::new("test.parquet", 123))
                .build();
        DataSourceExec::from_data_source(base_config)
    }
}

/// Index into the data that has been returned so far
#[derive(Debug, Default, Clone)]
pub struct BatchIndex {
    inner: Arc<std::sync::Mutex<usize>>,
}

impl BatchIndex {
    /// Return the current index
    pub fn value(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        *inner
    }

    // increment the current index by one
    pub fn incr(&self) {
        let mut inner = self.inner.lock().unwrap();
        *inner += 1;
    }
}

/// Iterator over batches
#[derive(Debug, Default)]
pub struct TestStream {
    /// Vector of record batches
    data: Vec<RecordBatch>,
    /// Index into the data that has been returned so far
    index: BatchIndex,
}

impl TestStream {
    /// Create an iterator for a vector of record batches. Assumes at
    /// least one entry in data (for the schema)
    pub fn new(data: Vec<RecordBatch>) -> Self {
        // check that there is at least one entry in data and that all batches have the same schema
        if let Some(first) = data.first() {
            assert!(
                data.iter().all(|batch| batch.schema() == first.schema()),
                "all batches must have the same schema"
            );
        }
        Self {
            data,
            ..Default::default()
        }
    }
}

impl Stream for TestStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let next_batch = self.index.value();

        Poll::Ready(if next_batch < self.data.len() {
            let next_batch = self.index.value();
            self.index.incr();
            Some(Ok(self.data[next_batch].clone()))
        } else {
            None
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.data.len(), Some(self.data.len()))
    }
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

        let session_config = SessionConfig::from(parquet_pushdown_config);
        let optimizer_context = OptimizerContext::new(session_config.clone());
        let output_result = opt.optimize_plan(input_plan, &optimizer_context);
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

pub fn format_plan_for_test(plan: &Arc<dyn ExecutionPlan>) -> String {
    let mut out = String::new();
    for line in format_execution_plan(plan) {
        out.push_str(&format!("  - {line}\n"));
    }
    out.push('\n');
    out
}

#[derive(Debug)]
pub(crate) struct TestNode {
    inject_filter: bool,
    input: Arc<dyn ExecutionPlan>,
    predicate: Arc<dyn PhysicalExpr>,
}

impl TestNode {
    pub fn new(
        inject_filter: bool,
        input: Arc<dyn ExecutionPlan>,
        predicate: Arc<dyn PhysicalExpr>,
    ) -> Self {
        Self {
            inject_filter,
            input,
            predicate,
        }
    }
}

impl DisplayAs for TestNode {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "TestInsertExec {{ inject_filter: {} }}",
            self.inject_filter
        )
    }
}

impl ExecutionPlan for TestNode {
    fn name(&self) -> &str {
        "TestInsertExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert!(children.len() == 1);
        Ok(Arc::new(TestNode::new(
            self.inject_filter,
            children[0].clone(),
            self.predicate.clone(),
        )))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion_execution::TaskContext>,
    ) -> Result<datafusion_execution::SendableRecordBatchStream> {
        unimplemented!("TestInsertExec is a stub for testing.")
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        // Since TestNode marks all parent filters as supported and adds its own filter,
        // we use from_child to create a description with all parent filters supported
        let child = &self.input;
        let child_desc = ChildFilterDescription::from_child(&parent_filters, child)?
            .with_self_filter(Arc::clone(&self.predicate));
        Ok(FilterDescription::new().with_child(child_desc))
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        if self.inject_filter {
            // Add a FilterExec if our own filter was not handled by the child

            // We have 1 child
            assert_eq!(child_pushdown_result.self_filters.len(), 1);
            let self_pushdown_result = child_pushdown_result.self_filters[0].clone();
            // And pushed down 1 filter
            assert_eq!(self_pushdown_result.len(), 1);
            let self_pushdown_result: Vec<_> = self_pushdown_result.into_iter().collect();

            let first_pushdown_result = self_pushdown_result[0].clone();

            match &first_pushdown_result.discriminant {
                PushedDown::No => {
                    // We have a filter to push down
                    let new_child = FilterExec::try_new(
                        Arc::clone(&first_pushdown_result.predicate),
                        Arc::clone(&self.input),
                    )?;
                    let new_self =
                        TestNode::new(false, Arc::new(new_child), self.predicate.clone());
                    let mut res =
                        FilterPushdownPropagation::if_all(child_pushdown_result);
                    res.updated_node = Some(Arc::new(new_self) as Arc<dyn ExecutionPlan>);
                    Ok(res)
                }
                PushedDown::Yes => {
                    let res = FilterPushdownPropagation::if_all(child_pushdown_result);
                    Ok(res)
                }
            }
        } else {
            let res = FilterPushdownPropagation::if_all(child_pushdown_result);
            Ok(res)
        }
    }
}
