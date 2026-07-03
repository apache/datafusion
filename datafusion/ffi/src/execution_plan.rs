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

use std::ffi::c_void;
use std::pin::Pin;
use std::sync::Arc;

use datafusion_common::config::ConfigOptions;
use datafusion_common::{DataFusionError, Result, Statistics};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr_common::metrics::MetricsSet;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, StatisticsArgs,
};
use stabby::string::String as SString;
use stabby::vec::Vec as SVec;
use tokio::runtime::Handle;

use crate::config::FFI_ConfigOptions;
use crate::execution::FFI_TaskContext;
use crate::physical_expr::metrics::FFI_MetricsSet;
use crate::plan_properties::FFI_PlanProperties;
use crate::record_batch_stream::FFI_RecordBatchStream;
use crate::statistics::{deserialize_statistics, serialize_statistics};
use crate::util::{FFI_Option, FFI_Result};
use crate::{df_result, sresult, sresult_return};

/// A stable struct for sharing a [`ExecutionPlan`] across FFI boundaries.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_ExecutionPlan {
    /// Return the plan properties
    pub properties: unsafe extern "C" fn(plan: &Self) -> FFI_PlanProperties,

    /// Return a vector of children plans
    pub children: unsafe extern "C" fn(plan: &Self) -> SVec<FFI_ExecutionPlan>,

    pub with_new_children:
        unsafe extern "C" fn(plan: &Self, children: SVec<Self>) -> FFI_Result<Self>,

    /// Return the plan name.
    pub name: unsafe extern "C" fn(plan: &Self) -> SString,

    /// Execute the plan and return a record batch stream. Errors
    /// will be returned as a string.
    pub execute: unsafe extern "C" fn(
        plan: &Self,
        partition: usize,
        context: FFI_TaskContext,
    ) -> FFI_Result<FFI_RecordBatchStream>,

    pub repartitioned: unsafe extern "C" fn(
        plan: &Self,
        target_partitions: usize,
        config: FFI_ConfigOptions,
    )
        -> FFI_Result<FFI_Option<FFI_ExecutionPlan>>,

    /// Snapshot the plan's execution metrics. Returns `None` when the
    /// underlying [`ExecutionPlan::metrics`] returned `None`.
    pub metrics: unsafe extern "C" fn(plan: &Self) -> FFI_Option<FFI_MetricsSet>,

    /// Snapshot partition statistics. `partition == None` corresponds to
    /// statistics over all partitions; `Some(idx)` corresponds to a specific
    /// partition. The returned bytes are a prost-encoded
    /// `datafusion_proto_common::Statistics`.
    pub partition_statistics: unsafe extern "C" fn(
        plan: &Self,
        partition: FFI_Option<usize>,
    ) -> FFI_Result<SVec<u8>>,

    /// Used to create a clone on the provider of the execution plan. This should
    /// only need to be called by the receiver of the plan.
    pub clone: unsafe extern "C" fn(plan: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the plan.
    /// A [`ForeignExecutionPlan`] should never attempt to access this data.
    pub private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface. See [`crate::get_library_marker_id`] and
    /// the crate's `README.md` for more information.
    pub library_marker_id: extern "C" fn() -> usize,
}

unsafe impl Send for FFI_ExecutionPlan {}
unsafe impl Sync for FFI_ExecutionPlan {}

pub struct ExecutionPlanPrivateData {
    pub plan: Arc<dyn ExecutionPlan>,
    pub runtime: Option<Handle>,
}

impl FFI_ExecutionPlan {
    fn inner(&self) -> &Arc<dyn ExecutionPlan> {
        let private_data = self.private_data as *const ExecutionPlanPrivateData;
        unsafe { &(*private_data).plan }
    }

    fn runtime(&self) -> Option<Handle> {
        let private_data = self.private_data as *const ExecutionPlanPrivateData;
        unsafe { (*private_data).runtime.clone() }
    }
}

unsafe extern "C" fn properties_fn_wrapper(
    plan: &FFI_ExecutionPlan,
) -> FFI_PlanProperties {
    plan.inner().properties().as_ref().into()
}

unsafe extern "C" fn children_fn_wrapper(
    plan: &FFI_ExecutionPlan,
) -> SVec<FFI_ExecutionPlan> {
    let runtime = plan.runtime();
    plan.inner()
        .children()
        .into_iter()
        .map(|child| FFI_ExecutionPlan::new(Arc::clone(child), runtime.clone()))
        .collect()
}

unsafe extern "C" fn with_new_children_fn_wrapper(
    plan: &FFI_ExecutionPlan,
    children: SVec<FFI_ExecutionPlan>,
) -> FFI_Result<FFI_ExecutionPlan> {
    let runtime = plan.runtime();
    let inner_plan = Arc::clone(plan.inner());

    let children: Result<Vec<Arc<dyn ExecutionPlan>>> = children
        .iter()
        .map(<Arc<dyn ExecutionPlan>>::try_from)
        .collect();

    let children = sresult_return!(children);
    let new_plan = sresult_return!(inner_plan.with_new_children(children));

    FFI_Result::Ok(FFI_ExecutionPlan::new(new_plan, runtime))
}

unsafe extern "C" fn execute_fn_wrapper(
    plan: &FFI_ExecutionPlan,
    partition: usize,
    context: FFI_TaskContext,
) -> FFI_Result<FFI_RecordBatchStream> {
    let ctx = context.into();
    let runtime = plan.runtime();
    let plan = plan.inner();

    let _runtime_guard = runtime.as_ref().map(|rt| rt.enter());

    sresult!(
        plan.execute(partition, ctx)
            .map(|rbs| FFI_RecordBatchStream::new(rbs, runtime))
    )
}

unsafe extern "C" fn repartitioned_fn_wrapper(
    plan: &FFI_ExecutionPlan,
    target_partitions: usize,
    config: FFI_ConfigOptions,
) -> FFI_Result<FFI_Option<FFI_ExecutionPlan>> {
    let maybe_config: Result<ConfigOptions, DataFusionError> = config.try_into();
    let config = sresult_return!(maybe_config);
    let runtime = plan.runtime();
    let plan = plan.inner();

    sresult!(
        plan.repartitioned(target_partitions, &config)
            .map(|maybe_plan| maybe_plan
                .map(|plan| FFI_ExecutionPlan::new(plan, runtime))
                .into())
    )
}

unsafe extern "C" fn name_fn_wrapper(plan: &FFI_ExecutionPlan) -> SString {
    plan.inner().name().into()
}

unsafe extern "C" fn metrics_fn_wrapper(
    plan: &FFI_ExecutionPlan,
) -> FFI_Option<FFI_MetricsSet> {
    plan.inner()
        .metrics()
        .as_ref()
        .map(FFI_MetricsSet::from)
        .into()
}

unsafe extern "C" fn partition_statistics_fn_wrapper(
    plan: &FFI_ExecutionPlan,
    partition: FFI_Option<usize>,
) -> FFI_Result<SVec<u8>> {
    let partition: Option<usize> = partition.into();
    plan.inner()
        .statistics_with_args(&StatisticsArgs::new().with_partition(partition))
        .map(|stats| SVec::from(serialize_statistics(stats.as_ref()).as_slice()))
        .into()
}

unsafe extern "C" fn release_fn_wrapper(plan: &mut FFI_ExecutionPlan) {
    unsafe {
        debug_assert!(!plan.private_data.is_null());
        let private_data =
            Box::from_raw(plan.private_data as *mut ExecutionPlanPrivateData);
        drop(private_data);
        plan.private_data = std::ptr::null_mut();
    }
}

unsafe extern "C" fn clone_fn_wrapper(plan: &FFI_ExecutionPlan) -> FFI_ExecutionPlan {
    let runtime = plan.runtime();
    let plan = plan.inner();

    FFI_ExecutionPlan::new(Arc::clone(plan), runtime)
}

impl Clone for FFI_ExecutionPlan {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

/// Helper function to recursively identify any children that do not
/// have a runtime set but should because they are local to this same
/// library. This does imply a restriction that all execution plans
/// in this chain that are within the same library use the same runtime.
fn pass_runtime_to_children(
    plan: &Arc<dyn ExecutionPlan>,
    runtime: &Handle,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let mut updated_children = false;
    let plan_is_foreign = plan.is::<ForeignExecutionPlan>();

    let children = plan
        .children()
        .into_iter()
        .map(|child| {
            let child = match pass_runtime_to_children(child, runtime)? {
                Some(child) => {
                    updated_children = true;
                    child
                }
                None => Arc::clone(child),
            };

            // If the parent is foreign and the child is local to this library, then when
            // we called `children()` above we will get something other than a
            // `ForeignExecutionPlan`. In this case wrap the plan in a `ForeignExecutionPlan`
            // because when we call `with_new_children` below it will extract the
            // FFI plan that does contain the runtime.
            if plan_is_foreign && !child.is::<ForeignExecutionPlan>() {
                updated_children = true;
                let ffi_child = FFI_ExecutionPlan::new(child, Some(runtime.clone()));
                let foreign_child = ForeignExecutionPlan::try_from(ffi_child);
                foreign_child.map(|c| Arc::new(c) as Arc<dyn ExecutionPlan>)
            } else {
                Ok(child)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    if updated_children {
        Arc::clone(plan).with_new_children(children).map(Some)
    } else {
        Ok(None)
    }
}

impl FFI_ExecutionPlan {
    /// This function is called on the provider's side.
    pub fn new(mut plan: Arc<dyn ExecutionPlan>, runtime: Option<Handle>) -> Self {
        // Note to developers: `pass_runtime_to_children` relies on the logic here to
        // get the underlying FFI plan during calls to `new_with_children`.
        if let Some(plan) = plan.downcast_ref::<ForeignExecutionPlan>() {
            return plan.plan.clone();
        }

        if let Some(rt) = &runtime
            && let Ok(Some(p)) = pass_runtime_to_children(&plan, rt)
        {
            plan = p;
        }

        let private_data = Box::new(ExecutionPlanPrivateData { plan, runtime });
        Self {
            properties: properties_fn_wrapper,
            children: children_fn_wrapper,
            with_new_children: with_new_children_fn_wrapper,
            name: name_fn_wrapper,
            execute: execute_fn_wrapper,
            repartitioned: repartitioned_fn_wrapper,
            metrics: metrics_fn_wrapper,
            partition_statistics: partition_statistics_fn_wrapper,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            private_data: Box::into_raw(private_data) as *mut c_void,
            library_marker_id: crate::get_library_marker_id,
        }
    }
}

impl Drop for FFI_ExecutionPlan {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

/// This struct is used to access an execution plan provided by a foreign
/// library across a FFI boundary.
///
/// The ForeignExecutionPlan is to be used by the caller of the plan, so it has
/// no knowledge or access to the private data. All interaction with the plan
/// must occur through the functions defined in FFI_ExecutionPlan.
#[derive(Debug)]
pub struct ForeignExecutionPlan {
    name: String,
    plan: FFI_ExecutionPlan,
    properties: Arc<PlanProperties>,
    children: Vec<Arc<dyn ExecutionPlan>>,
}

unsafe impl Send for ForeignExecutionPlan {}
unsafe impl Sync for ForeignExecutionPlan {}

impl DisplayAs for ForeignExecutionPlan {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "FFI_ExecutionPlan: {}, number_of_children={}",
                    self.name,
                    self.children.len(),
                )
            }
            DisplayFormatType::TreeRender => {
                // TODO: collect info
                write!(f, "")
            }
        }
    }
}

impl TryFrom<&FFI_ExecutionPlan> for Arc<dyn ExecutionPlan> {
    type Error = DataFusionError;

    fn try_from(plan: &FFI_ExecutionPlan) -> Result<Self, Self::Error> {
        if (plan.library_marker_id)() == crate::get_library_marker_id() {
            Ok(Arc::clone(plan.inner()))
        } else {
            let plan = ForeignExecutionPlan::try_from(plan.clone())?;
            Ok(Arc::new(plan))
        }
    }
}

impl TryFrom<FFI_ExecutionPlan> for ForeignExecutionPlan {
    type Error = DataFusionError;
    fn try_from(plan: FFI_ExecutionPlan) -> Result<Self, Self::Error> {
        unsafe {
            let name = (plan.name)(&plan).into();

            let properties: PlanProperties = (plan.properties)(&plan).try_into()?;

            let children_rvec = (plan.children)(&plan);
            let children = children_rvec
                .iter()
                .map(<Arc<dyn ExecutionPlan>>::try_from)
                .collect::<Result<Vec<_>>>()?;

            Ok(ForeignExecutionPlan {
                name,
                plan,
                properties: Arc::new(properties),
                children,
            })
        }
    }
}

impl ExecutionPlan for ForeignExecutionPlan {
    fn name(&self) -> &str {
        &self.name
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.children.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let children = children
            .into_iter()
            .map(|child| FFI_ExecutionPlan::new(child, None))
            .collect::<SVec<_>>();
        let new_plan =
            unsafe { df_result!((self.plan.with_new_children)(&self.plan, children))? };

        (&new_plan).try_into()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let context = FFI_TaskContext::from(context);
        unsafe {
            df_result!((self.plan.execute)(&self.plan, partition, context))
                .map(|stream| Pin::new(Box::new(stream)) as SendableRecordBatchStream)
        }
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let config = config.into();
        let maybe_plan: Option<FFI_ExecutionPlan> = df_result!(unsafe {
            (self.plan.repartitioned)(&self.plan, target_partitions, config)
        })?
        .into();

        maybe_plan
            .map(|plan| <Arc<dyn ExecutionPlan>>::try_from(&plan))
            .transpose()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        let ffi: Option<FFI_MetricsSet> =
            unsafe { (self.plan.metrics)(&self.plan) }.into();
        ffi.map(MetricsSet::from)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        let bytes = df_result!(unsafe {
            (self.plan.partition_statistics)(&self.plan, partition.into())
        })?;
        Ok(Arc::new(deserialize_statistics(bytes.as_slice())?))
    }
}

#[cfg(any(test, feature = "integration-tests"))]
pub mod tests {
    use datafusion_physical_plan::Partitioning;
    use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};

    use super::*;

    #[derive(Debug)]
    pub struct EmptyExec {
        props: Arc<PlanProperties>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        metrics: Option<MetricsSet>,
        statistics: Option<Statistics>,
    }

    impl EmptyExec {
        pub fn new(schema: arrow::datatypes::SchemaRef) -> Self {
            Self {
                props: Arc::new(PlanProperties::new(
                    datafusion_physical_expr::EquivalenceProperties::new(schema),
                    Partitioning::UnknownPartitioning(3),
                    EmissionType::Incremental,
                    Boundedness::Bounded,
                )),
                children: Vec::default(),
                metrics: None,
                statistics: None,
            }
        }

        pub fn with_metrics(mut self, metrics: MetricsSet) -> Self {
            self.metrics = Some(metrics);
            self
        }

        pub fn with_statistics(mut self, statistics: Statistics) -> Self {
            self.statistics = Some(statistics);
            self
        }
    }

    impl DisplayAs for EmptyExec {
        fn fmt_as(
            &self,
            _t: DisplayFormatType,
            _f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            unimplemented!()
        }
    }

    impl ExecutionPlan for EmptyExec {
        fn name(&self) -> &'static str {
            "empty-exec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.props
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            self.children.iter().collect()
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(EmptyExec {
                props: Arc::clone(&self.props),
                children,
                metrics: self.metrics.clone(),
                statistics: self.statistics.clone(),
            }))
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            unimplemented!()
        }

        fn metrics(&self) -> Option<MetricsSet> {
            self.metrics.clone()
        }

        fn statistics_with_args(
            &self,
            _args: &StatisticsArgs,
        ) -> Result<Arc<Statistics>> {
            Ok(Arc::new(self.statistics.clone().unwrap_or_else(|| {
                Statistics::new_unknown(self.props.eq_properties.schema())
            })))
        }
    }

    #[test]
    fn test_round_trip_ffi_execution_plan() -> Result<()> {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Float32, false),
        ]));

        let original_plan = Arc::new(EmptyExec::new(schema));
        let original_name = original_plan.name().to_string();

        let mut local_plan = FFI_ExecutionPlan::new(original_plan, None);
        local_plan.library_marker_id = crate::mock_foreign_marker_id;

        let foreign_plan: Arc<dyn ExecutionPlan> = (&local_plan).try_into()?;

        assert_eq!(original_name, foreign_plan.name());

        let display = datafusion_physical_plan::display::DisplayableExecutionPlan::new(
            foreign_plan.as_ref(),
        );

        let buf = display.one_line().to_string();
        assert_eq!(
            buf.trim(),
            "FFI_ExecutionPlan: empty-exec, number_of_children=0"
        );

        Ok(())
    }

    #[test]
    fn test_ffi_execution_plan_children() -> Result<()> {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Float32, false),
        ]));

        // Version 1: Adding child to the foreign plan
        let child_plan = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let mut child_local = FFI_ExecutionPlan::new(child_plan, None);
        child_local.library_marker_id = crate::mock_foreign_marker_id;
        let child_foreign = <Arc<dyn ExecutionPlan>>::try_from(&child_local)?;

        let parent_plan = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let mut parent_local = FFI_ExecutionPlan::new(parent_plan, None);
        parent_local.library_marker_id = crate::mock_foreign_marker_id;
        let parent_foreign = <Arc<dyn ExecutionPlan>>::try_from(&parent_local)?;

        assert_eq!(parent_foreign.children().len(), 0);
        assert_eq!(child_foreign.children().len(), 0);

        let parent_foreign = parent_foreign.with_new_children(vec![child_foreign])?;
        assert_eq!(parent_foreign.children().len(), 1);

        // Version 2: Adding child to the local plan
        let child_plan = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let mut child_local = FFI_ExecutionPlan::new(child_plan, None);
        child_local.library_marker_id = crate::mock_foreign_marker_id;
        let child_foreign = <Arc<dyn ExecutionPlan>>::try_from(&child_local)?;

        let parent_plan = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let parent_plan = parent_plan.with_new_children(vec![child_foreign])?;
        let mut parent_local = FFI_ExecutionPlan::new(parent_plan, None);
        parent_local.library_marker_id = crate::mock_foreign_marker_id;
        let parent_foreign = <Arc<dyn ExecutionPlan>>::try_from(&parent_local)?;

        assert_eq!(parent_foreign.children().len(), 1);

        Ok(())
    }

    #[test]
    fn test_ffi_execution_plan_metrics_round_trip() -> Result<()> {
        use datafusion_physical_expr_common::metrics::{Count, Metric, MetricValue};

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Float32, false),
        ]));

        // Plans without metrics still return None across the boundary.
        let bare_plan = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let mut bare_local = FFI_ExecutionPlan::new(bare_plan, None);
        bare_local.library_marker_id = crate::mock_foreign_marker_id;
        let bare_foreign: Arc<dyn ExecutionPlan> = (&bare_local).try_into()?;
        assert!(bare_foreign.metrics().is_none());

        // Plans with metrics produce equivalent MetricsSets after a round trip.
        let mut original_metrics = MetricsSet::new();
        let c0 = Count::new();
        c0.add(11);
        original_metrics
            .push(Arc::new(Metric::new(MetricValue::OutputRows(c0), Some(0))));
        let c1 = Count::new();
        c1.add(31);
        original_metrics
            .push(Arc::new(Metric::new(MetricValue::OutputRows(c1), Some(1))));

        let metric_plan = Arc::new(EmptyExec::new(schema).with_metrics(original_metrics));
        let mut metric_local = FFI_ExecutionPlan::new(metric_plan, None);
        metric_local.library_marker_id = crate::mock_foreign_marker_id;
        let metric_foreign: Arc<dyn ExecutionPlan> = (&metric_local).try_into()?;

        let observed = metric_foreign.metrics().expect("metrics should be present");
        assert_eq!(observed.output_rows(), Some(42));

        Ok(())
    }

    /// Build an `EmptyExec` carrying `statistics`, then export it across the
    /// (mock) FFI boundary and return the resulting foreign plan.
    #[cfg(test)]
    fn export_empty_exec_over_ffi(
        schema: &arrow::datatypes::SchemaRef,
        statistics: Option<Statistics>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut plan = EmptyExec::new(Arc::clone(schema));
        if let Some(statistics) = statistics {
            plan = plan.with_statistics(statistics);
        }
        let mut local = FFI_ExecutionPlan::new(Arc::new(plan), None);
        local.library_marker_id = crate::mock_foreign_marker_id;
        let foreign: Arc<dyn ExecutionPlan> = (&local).try_into()?;
        Ok(foreign)
    }

    /// Schema and a fully-populated `Statistics` (including `ScalarValue`-typed
    /// min/max) shared by the FFI statistics round-trip tests.
    #[cfg(test)]
    fn stats_round_trip_fixture() -> (arrow::datatypes::SchemaRef, Statistics) {
        use datafusion_common::stats::Precision;
        use datafusion_common::{ColumnStatistics, ScalarValue};

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int32, true),
        ]));
        let statistics = Statistics {
            num_rows: Precision::Exact(7),
            total_byte_size: Precision::Inexact(128),
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Exact(1),
                max_value: Precision::Exact(ScalarValue::Int32(Some(10))),
                min_value: Precision::Exact(ScalarValue::Int32(Some(-3))),
                sum_value: Precision::Absent,
                distinct_count: Precision::Inexact(6),
                byte_size: Precision::Exact(28),
            }],
        };
        (schema, statistics)
    }

    /// Statistics survive an FFI round trip when queried through the
    /// **deprecated** `partition_statistics` entry point on the foreign plan.
    #[test]
    #[expect(deprecated)]
    fn test_ffi_execution_plan_partition_statistics_round_trip() -> Result<()> {
        let (schema, original_stats) = stats_round_trip_fixture();

        // A plan without explicit statistics reports new_unknown.
        let bare = export_empty_exec_over_ffi(&schema, None)?;
        assert_eq!(
            bare.partition_statistics(None)?.as_ref(),
            &Statistics::new_unknown(&schema)
        );

        // A plan with statistics round-trips them for overall and per-partition queries.
        let with_stats =
            export_empty_exec_over_ffi(&schema, Some(original_stats.clone()))?;
        assert_eq!(
            with_stats.partition_statistics(None)?.as_ref(),
            &original_stats
        );
        assert_eq!(
            with_stats.partition_statistics(Some(1))?.as_ref(),
            &original_stats
        );

        Ok(())
    }

    /// Same round trip as
    /// [`test_ffi_execution_plan_partition_statistics_round_trip`], but queried
    /// through the **new** `statistics_with_args` entry point.
    #[test]
    fn test_ffi_execution_plan_statistics_with_args_round_trip() -> Result<()> {
        let (schema, original_stats) = stats_round_trip_fixture();

        // A plan without explicit statistics reports new_unknown.
        let bare = export_empty_exec_over_ffi(&schema, None)?;
        assert_eq!(
            bare.statistics_with_args(&StatisticsArgs::new())?.as_ref(),
            &Statistics::new_unknown(&schema)
        );

        // A plan with statistics round-trips them for overall and per-partition queries.
        let with_stats =
            export_empty_exec_over_ffi(&schema, Some(original_stats.clone()))?;
        assert_eq!(
            with_stats
                .statistics_with_args(&StatisticsArgs::new())?
                .as_ref(),
            &original_stats
        );
        assert_eq!(
            with_stats
                .statistics_with_args(&StatisticsArgs::new().with_partition(Some(1)))?
                .as_ref(),
            &original_stats
        );

        Ok(())
    }

    #[test]
    fn test_ffi_execution_plan_local_bypass() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Float32, false),
        ]));

        let plan = Arc::new(EmptyExec::new(schema));

        let mut ffi_plan = FFI_ExecutionPlan::new(plan, None);

        // Verify local libraries can be downcast to their original
        let foreign_plan: Arc<dyn ExecutionPlan> = (&ffi_plan).try_into().unwrap();
        assert!(foreign_plan.is::<EmptyExec>());

        // Verify different library markers generate foreign providers
        ffi_plan.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_plan: Arc<dyn ExecutionPlan> = (&ffi_plan).try_into().unwrap();
        assert!(foreign_plan.is::<ForeignExecutionPlan>());
    }
}
