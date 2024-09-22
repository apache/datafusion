use std::any::Any;
use std::fmt::Formatter;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Poll;

use crate::coalesce_partitions::CoalescePartitionsExec;
use crate::joins::utils::{
    adjust_indices_by_join_type, apply_join_filter_to_indices, build_batch_from_indices,
    build_join_schema, check_join_is_valid, estimate_join_statistics,
    get_final_indices_from_bit_map, BuildProbeJoinMetrics, ColumnIndex, JoinFilter,
    OnceAsync, OnceFut,
};
use crate::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::{
    execution_mode_from_children, DisplayAs, DisplayFormatType, Distribution,
    ExecutionMode, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
};

use arrow::array::{BooleanBufferBuilder, UInt32Array, UInt64Array};
use arrow::compute::concat_batches;
use arrow::datatypes::{Schema, SchemaRef, UInt64Type};
use arrow::record_batch::RecordBatch;
use arrow::util::bit_util;
use arrow_array::PrimitiveArray;
use arrow_ord::partition;
use datafusion_common::{exec_datafusion_err, plan_err, JoinSide, Result, Statistics};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::TaskContext;
use datafusion_expr::JoinType;
use datafusion_physical_expr::equivalence::join_equivalence_properties;

use datafusion_physical_expr::{Partitioning, PhysicalExpr, PhysicalExprRef};
use futures::{ready, Stream, StreamExt, TryStreamExt};
use parking_lot::Mutex;

use super::utils::check_inequality_conditions;

/// IEJoinExec is optimized join without any equijoin conditions in `ON` clause but with two or more inequality conditions.
/// For more detail algorithm, see https://vldb.org/pvldb/vol8/p2074-khayyat.pdf
///
/// Take this query q as an example:
///
/// SELECT t1.t id, t2.t id
/// FROM west t1, west t2
/// WHERE t1.time > t2.time AND t1.cost < t2.cost
///
/// There is no equijoin condition in the `ON` clause, but there are two inequality conditions.
/// Currently, left table is t1, right table is t2.
///
/// The berif idea of this algorithm is converting it to inversion of permutation problem. For a permutation of a[0..n-1], for a pairs (i, j) such that i < j and a[i] > a[j], we call it an inversion of permutation.
/// For example, for a[0..4] = [2, 3, 1, 4], there are 2 inversions: (0, 2), (1, 2)
///
/// To convert query q to inversion of permutation problem. We will do the following steps:
/// 1. Sort t1 union t2 by time in ascending order, mark the sorted table as l1.
/// 2. Sort t1 union t2 by cost in ascending order, mark the sorted table as l2.
/// 3. For each element e_i in l2, find the index j in l1 such that l1[j] = e_i, mark the computed index as permutation array p.
/// 4. Compute the inversion of permutation array p. For a pair (i, j) in l2, if i < j then e_i.cost < e_j.cost because l2 is sorted by cost in ascending order. And if p[i] > p[j], then e_i.time > e_j.time because l1 is sorted by time in ascending order.
/// 5. The result of query q is the pairs (i, j) in l2 such that i < j and p[i] > p[j] and e_i is from right table and e_j is from left table.
///
/// To get the final result, we need to get all the pairs (i, j) in l2 such that i < j and p[i] > p[j] and e_i is from right table and e_j is from left table. We can do this by the following steps:
/// 1. Traverse l2 from left to right, at offset j, we can maintain BtreeSet or bitmap to record all the p[i] that i < j, then find all the pairs (i, j) in l2 such that p[i] > p[j].
///
/// To parallel the above algorithm, we can sort t1 and t2 by time (condition 1) firstly, and repartition the data into N partitions, then join t1[i] and t2[j] respectively. And if the max time of t1[i] is smaller than the min time of t2[j], we can skip the join of t1[i] and t2[j] because there is no join result between them according to condition 1.
#[derive(Debug)]
pub struct IEJoinExec {
    /// left side
    pub(crate) left: Arc<dyn ExecutionPlan>,
    /// right side
    pub(crate) right: Arc<dyn ExecutionPlan>,
    /// inequality conditions for iejoin, for example, t1.time > t2.time and t1.cost < t2.cost, only support two inequality conditions, other conditions will be stored in `filter`
    pub(crate) inequality_conditions: Vec<PhysicalExprRef>,
    /// filters which are applied while finding matching rows
    pub(crate) filter: Option<JoinFilter>,
    /// how the join is performed
    pub(crate) join_type: JoinType,
    /// the schema once the join is applied
    schema: SchemaRef,
    /// left table data after sort by condition 1
    left_data: OnceAsync<Arc<Vec<RecordBatch>>>,
    /// right table data after sort by condition 1
    right_data: OnceAsync<Arc<Vec<RecordBatch>>>,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    /// execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
}

impl IEJoinExec {
    /// Try to create a new [`IEJoinExec`]
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        inequality_conditions: Vec<PhysicalExprRef>,
        filter: Option<JoinFilter>,
        join_type: &JoinType,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        check_join_is_valid(&left_schema, &right_schema, &[])?;
        let (schema, column_indices) =
            build_join_schema(&left_schema, &right_schema, join_type);
        check_inequality_conditions(&left_schema, &right_schema, &inequality_conditions)?;
        let schema = Arc::new(schema);
        if !matches!(join_type, JoinType::Inner) {
            return plan_err!(
                "IEJoinExec only supports inner join currently, got {}",
                join_type
            );
        }
        let cache =
            Self::compute_properties(&left, &right, Arc::clone(&schema), *join_type);
        Ok(IEJoinExec {
            left,
            right,
            inequality_conditions,
            filter,
            join_type: *join_type,
            schema,
            left_data: Default::default(),
            right_data: Default::default(),
            column_indices,
            metrics: Default::default(),
            cache,
        })
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        left: &Arc<dyn ExecutionPlan>,
        right: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        join_type: JoinType,
    ) -> PlanProperties {
        // Calculate equivalence properties:
        let eq_properties = join_equivalence_properties(
            left.equivalence_properties().clone(),
            right.equivalence_properties().clone(),
            &join_type,
            schema,
            &[false, false],
            None,
            // No on columns in nested loop join
            &[],
        );

        let output_partitioning = Partitioning::UnknownPartitioning(
            right.output_partitioning().partition_count(),
        );

        // Determine execution mode:
        let mut mode = execution_mode_from_children([left, right]);
        if mode.is_unbounded() {
            mode = ExecutionMode::PipelineBreaking;
        }

        PlanProperties::new(eq_properties, output_partitioning, mode)
    }
}

impl DisplayAs for IEJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let display_filter = self.filter.as_ref().map_or_else(
                    || "".to_string(),
                    |f| format!(", filter={}", f.expression()),
                );
                let display_inequality_conditions = self
                    .inequality_conditions
                    .iter()
                    .map(|c| format!("({})", c))
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(
                    f,
                    "IEJoinExec: mode={:?}, join_type={:?}, inequality_conditions=[{}], {}",
                    self.cache.execution_mode,
                    self.join_type,
                    display_inequality_conditions,
                    display_filter,
                )
            }
        }
    }
}

impl ExecutionPlan for IEJoinExec {
    fn name(&self) -> &'static str {
        "IEJoinExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition, Distribution::SinglePartition]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(IEJoinExec::try_new(
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
            self.inequality_conditions.clone(),
            self.filter.clone(),
            &self.join_type,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        todo!()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        estimate_join_statistics(
            Arc::clone(&self.left),
            Arc::clone(&self.right),
            vec![],
            &self.join_type,
            &self.schema,
        )
    }
}
