use arrow_schema::SchemaRef;
use datafusion_common::DataFusionError;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

/// Execution plan that return the stream on the call to `execute`.
pub struct StreamExec {
    /// the results to send back
    stream: Mutex<Option<SendableRecordBatchStream>>,
    cache: PlanProperties,
}

impl Debug for StreamExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "StreamExec")
    }
}

impl StreamExec {
    /// Create a new `MockExec` with a single partition that returns
    /// the specified `Results`s.
    ///
    /// By default, the batches are not produced immediately (the
    /// caller has to actually yield and another task must run) to
    /// ensure any poll loops are correct. This behavior can be
    /// changed with `with_use_task`
    pub fn new(stream: SendableRecordBatchStream) -> Self {
        let cache = Self::compute_properties(stream.schema());
        Self {
            stream: Mutex::new(Some(stream)),
            cache,
        }
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl DisplayAs for StreamExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "StreamExec:")
            }
            DisplayFormatType::TreeRender => {
                write!(f, "")
            }
        }
    }
}

impl ExecutionPlan for StreamExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    /// Returns a stream which yields data
    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        assert_eq!(partition, 0);

        let stream = self.stream.lock().unwrap().take();

        stream.ok_or(DataFusionError::Internal(
            "Stream already consumed".to_string(),
        ))
    }
}
