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

use arrow::array::UInt64Array;
use arrow::datatypes::SchemaRef;
use arrow::{array::RecordBatch, compute::concat_batches};
use arrow_schema::DataType;
use datafusion::{datasource::object_store::ObjectStoreUrl, physical_plan::PhysicalExpr};
use datafusion_common::{config::ConfigOptions, internal_err, Result, Statistics};
use datafusion_datasource::{
    file::FileSource, file_meta::FileMeta, file_scan_config::FileScanConfig,
    file_scan_config::FileScanConfigBuilder, file_stream::FileOpenFuture,
    file_stream::FileOpener, schema_adapter::DefaultSchemaAdapterFactory,
    schema_adapter::SchemaAdapterFactory, source::DataSourceExec, PartitionedFile,
};
use datafusion_execution::RecordBatchStream;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl};
use datafusion_physical_expr_common::physical_expr::fmt_sql;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
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
use std::future::Future;
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
    schema: Option<SchemaRef>,
    projection: Option<Vec<usize>>,
    predicate: Option<Arc<dyn PhysicalExpr>>,
}

impl FileOpener for TestOpener {
    fn open(
        &self,
        _file_meta: FileMeta,
        _file: PartitionedFile,
    ) -> Result<FileOpenFuture> {
        let mut batches = self.batches.clone();
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
        if let Some(schema) = &self.schema {
            let factory = DefaultSchemaAdapterFactory::from_schema(Arc::clone(schema));
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
        }
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
#[derive(Clone, Default)]
pub struct TestSource {
    support: bool,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    statistics: Option<Statistics>,
    batch_size: Option<usize>,
    batches: Vec<RecordBatch>,
    schema: Option<SchemaRef>,
    metrics: ExecutionPlanMetricsSet,
    projection: Option<Vec<usize>>,
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
}

impl TestSource {
    fn new(support: bool, batches: Vec<RecordBatch>) -> Self {
        Self {
            support,
            metrics: ExecutionPlanMetricsSet::new(),
            batches,
            ..Default::default()
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
        Arc::new(TestOpener {
            batches: self.batches.clone(),
            batch_size: self.batch_size,
            schema: self.schema.clone(),
            projection: self.projection.clone(),
            predicate: self.predicate.clone(),
        })
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

    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
        Arc::new(TestSource {
            schema: Some(schema),
            ..self.clone()
        })
    }

    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(TestSource {
            projection: config.projection.clone(),
            ..self.clone()
        })
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        Arc::new(TestSource {
            statistics: Some(statistics),
            ..self.clone()
        })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
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
        let source = Arc::new(TestSource::new(self.support, self.batches));
        let base_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("test://").unwrap(),
            Arc::clone(&self.schema),
            source,
        )
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
        assert!(!data.is_empty(), "data must not be empty");
        assert!(
            data.iter().all(|batch| batch.schema() == data[0].schema()),
            "all batches must have the same schema"
        );
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

pub struct Flag {
    sender: tokio::sync::watch::Sender<bool>,
}

impl Flag {
    /// Creates a new flag object.
    pub fn new() -> Self {
        Self {
            sender: tokio::sync::watch::channel(false).0,
        }
    }

    /// Enables the flag.
    pub fn enable(&self) {
        self.sender.send_if_modified(|value| {
            if *value {
                false
            } else {
                *value = true;

                true
            }
        });
    }

    /// Waits the flag to become enabled.
    pub async fn wait_enabled(&self) {
        if !*self.sender.borrow() {
            let mut receiver = self.sender.subscribe();

            if !*receiver.borrow() {
                receiver.changed().await.ok();
            }
        }
    }
}

/// An execution plan node that waits for a notification before yielding any data from designated partitions.
pub struct SlowPartitionNode {
    input: Arc<dyn ExecutionPlan>,
    flag: Arc<Flag>,
    slow_partitions: Vec<usize>,
}

impl SlowPartitionNode {
    pub fn new(input: Arc<dyn ExecutionPlan>, slow_partitions: Vec<usize>) -> Self {
        Self {
            input,
            flag: Arc::new(Flag::new()),
            slow_partitions,
        }
    }

    pub fn unblock(&self) {
        self.flag.enable();
    }
}

impl std::fmt::Debug for SlowPartitionNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlowPartitionNode")
    }
}

impl DisplayAs for SlowPartitionNode {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SlowPartitionNode")
    }
}

impl ExecutionPlan for SlowPartitionNode {
    fn name(&self) -> &str {
        "SlowPartitionNode"
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
        Ok(Arc::new(SlowPartitionNode {
            input: children[0].clone(),
            flag: Arc::clone(&self.flag),
            slow_partitions: self.slow_partitions.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion_execution::TaskContext>,
    ) -> Result<datafusion_execution::SendableRecordBatchStream> {
        if self.slow_partitions.contains(&partition) {
            let stream = self.input.execute(partition, context)?;
            let waiter_stream = WaiterStream {
                inner: stream,
                flag: Arc::clone(&self.flag),
                flag_checked: false,
            };
            Ok(Box::pin(waiter_stream)
                as datafusion_execution::SendableRecordBatchStream)
        } else {
            self.input.execute(partition, context)
        }
    }
}

/// Stream that waits for a notification before yielding the first batch
struct WaiterStream {
    inner: datafusion_execution::SendableRecordBatchStream,
    flag: Arc<Flag>,
    flag_checked: bool,
}

impl Stream for WaiterStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // If we haven't checked the flag yet, wait for it to be enabled
        if !this.flag_checked {
            let flag = Arc::clone(&this.flag);
            let wait_future = flag.wait_enabled();
            futures::pin_mut!(wait_future);

            match wait_future.poll(cx) {
                Poll::Ready(()) => {
                    // Flag is now enabled, mark as checked and continue
                    this.flag_checked = true;
                }
                Poll::Pending => {
                    // Still waiting for flag to be enabled
                    return Poll::Pending;
                }
            }
        }

        // Flag has been checked and is enabled, delegate to inner stream
        Pin::new(&mut this.inner).poll_next(cx)
    }
}

impl RecordBatchStream for WaiterStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

/// A hash repartition implementation that only accepts integers and hashes them to themselves.
#[derive(Debug)]
pub struct TestRepartitionHash {
    signature: datafusion_expr::Signature,
}

impl TestRepartitionHash {
    pub fn new() -> Self {
        Self {
            signature: datafusion_expr::Signature::one_of(
                vec![datafusion_expr::TypeSignature::VariadicAny],
                datafusion_expr::Volatility::Immutable,
            ),
        }
    }
}

impl PartialEq for TestRepartitionHash {
    fn eq(&self, other: &Self) -> bool {
        // RandomState doesn't implement PartialEq, so we just compare signatures
        self.signature == other.signature
    }
}

impl Eq for TestRepartitionHash {}

impl std::hash::Hash for TestRepartitionHash {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Only hash the signature since RandomState doesn't implement Hash
        self.signature.hash(state);
    }
}

impl ScalarUDFImpl for TestRepartitionHash {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "test_repartition_hash"
    }

    fn signature(&self) -> &datafusion_expr::Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // Always return UInt64Array regardless of input types
        Ok(DataType::UInt64)
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        // All inputs must be arrays of UInt64
        let arrays: Vec<UInt64Array> = args
            .args
            .iter()
            .map(|cv| {
                let ColumnarValue::Array(array) = cv else {
                    panic!("Expected array input");
                };
                let Some(array) = array.as_any().downcast_ref::<UInt64Array>() else {
                    panic!("Expected UInt64Array input");
                };
                array.clone()
            })
            .collect();
        // We accept only 1 array
        if arrays.is_empty() {
            return Err(datafusion_common::DataFusionError::Internal(
                "Expected at least one argument".to_string(),
            ));
        }

        let num_rows = arrays[0].len();
        let mut result_values = Vec::with_capacity(num_rows);

        // Add together all the integer values from all input arrays
        for row_idx in 0..num_rows {
            let mut sum = 0u64;
            for array in &arrays {
                let value = array.value(row_idx);
                sum = sum.wrapping_add(value);
            }
            result_values.push(sum);
        }
        // Return the summed values as a UInt64Array
        Ok(ColumnarValue::Array(Arc::new(UInt64Array::from(
            result_values,
        ))))
    }

    fn documentation(&self) -> Option<&datafusion_expr::Documentation> {
        None
    }
}
