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

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt::Formatter;
use std::ops::Range;
use std::sync::Arc;
use std::task::Poll;

use crate::joins::utils::{
    apply_join_filter_to_indices, build_batch_from_indices, build_join_schema,
    check_inequality_condition, check_join_is_valid, estimate_join_statistics,
    inequality_conditions_to_sort_exprs, is_loose_inequality_operator, ColumnIndex,
    JoinFilter, OnceAsync, OnceFut,
};
use crate::metrics::{self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use crate::sorts::sort::sort_batch;
use crate::{
    collect, execution_mode_from_children, DisplayAs, DisplayFormatType, Distribution,
    ExecutionMode, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
};
use arrow::array::{make_comparator, AsArray, UInt64Builder};

use arrow::compute::concat;
use arrow::compute::kernels::sort::SortOptions;
use arrow::datatypes::{Int64Type, Schema, SchemaRef, UInt64Type};
use arrow::record_batch::RecordBatch;
use arrow_array::{ArrayRef, Int64Array, UInt64Array};
use datafusion_common::{plan_err, JoinSide, Result, Statistics};
use datafusion_execution::TaskContext;
use datafusion_expr::{JoinType, Operator};
use datafusion_physical_expr::equivalence::join_equivalence_properties;

use datafusion_physical_expr::{Partitioning, PhysicalSortExpr, PhysicalSortRequirement};
use futures::{ready, Stream};
use parking_lot::Mutex;

/// IEJoinExec is optimized join without any equijoin conditions in `ON` clause but with two or more inequality conditions.
/// For more detail algorithm, see <https://vldb.org/pvldb/vol8/p2074-khayyat.pdf>
///
/// Take this query q as an example:
///
/// SELECT t1.t id, t2.t id
/// FROM west t1, west t2
/// WHERE t1.time < t2.time AND t1.cost < t2.cost
///
/// There is no equijoin condition in the `ON` clause, but there are two inequality conditions.
/// Currently, left table is t1, right table is t2.
///
/// The berif idea of this algorithm is converting it to ordered pair/inversion pair of permutation problem. For a permutation of a[0..n-1], for a pairs (i, j) such that i < j and a\[i\] < a\[j\], we call it an ordered pair of permutation.
///
/// For example, for a[0..4] = [2, 1, 3, 0], there are 2 ordered pairs: (2, 3), (1, 3)
///
/// To convert query q to ordered pair of permutation problem. We will do the following steps:
/// 1. Sort t1 union t2 by time in ascending order, mark the sorted table as l1.
/// 2. Sort t1 union t2 by cost in ascending order, mark the sorted table as l2.
/// 3. For each element e_i in l2, find the index j in l1 such that l1\[j\] = e_i, mark the computed index as permutation array p. If p\[i\] = j, it means that the ith element in l2 is the jth element in l1.
/// 4. Compute the ordered pair of permutation array p. For a pair (i, j) in l2, if i < j then e_i.cost < e_j.cost because l2 is sorted by cost in ascending order. And if p\[i\] < p\[j\], then e_i.time < e_j.time because l1 is sorted by time in ascending order.
/// 5. The result of query q is the pairs (i, j) in l2 such that i < j and p\[i\] < p\[j\] and e_i is from right table and e_j is from left table.
///
/// To get the final result, we need to get all the pairs (i, j) in l2 such that i < j and p\[i\] < p\[j\] and e_i is from right table and e_j is from left table. We can do this by the following steps:
/// 1. Traverse l2 from left to right, at offset j, we can maintain BtreeSet or bitmap to record all the p\[i\] that i < j, then find all the pairs (i, j) in l2 such that p\[i\] < p\[j\].
///    See more detailed example in `compute_permutation` and `build_join_indices` function.
///
/// To parallel the above algorithm, we can sort t1 and t2 by time (condition 1) firstly, and repartition the data into N partitions, then join t1\[i\] and t2\[j\] respectively. And if the minimum time of t1\[i\] is greater than the maximum time of t2\[j\], we can skip the join of t1\[i\] and t2\[j\] because there is no join result between them according to condition 1.
#[derive(Debug)]
pub struct IEJoinExec {
    /// left side, which have been sorted by condition 1
    pub(crate) left: Arc<dyn ExecutionPlan>,
    /// right side, which have been sorted by condition 1
    pub(crate) right: Arc<dyn ExecutionPlan>,
    /// inequality conditions for iejoin, for example, t1.time > t2.time and t1.cost < t2.cost, only support two inequality conditions, other conditions will be stored in `filter`
    pub(crate) inequality_conditions: Vec<JoinFilter>,
    /// filters which are applied while finding matching rows
    pub(crate) filter: Option<JoinFilter>,
    /// how the join is performed
    pub(crate) join_type: JoinType,
    /// the schema once the join is applied
    schema: SchemaRef,
    /// data for iejoin
    iejoin_data: OnceAsync<IEJoinData>,
    /// left condition, it represents `t1.time asc` and `t1.cost asc` in above example
    left_conditions: Arc<[PhysicalSortExpr; 2]>,
    /// right condition, it represents `t2.time asc` and `t2.cost asc` in above example
    right_conditions: Arc<[PhysicalSortExpr; 2]>,
    /// operator of the inequality condition
    operators: Arc<[Operator; 2]>,
    /// sort options of the inequality conditions, it represents `asc` and `asc` in above example
    sort_options: Arc<[SortOptions; 2]>,
    /// partition pairs, used to get the next pair of left and right blocks, IEJoinStream handles one pair of blocks each time
    pairs: Arc<Mutex<u64>>,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    // TODO: add memory reservation?
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
        inequality_conditions: Vec<JoinFilter>,
        filter: Option<JoinFilter>,
        join_type: &JoinType,
        target_partitions: usize,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        check_join_is_valid(&left_schema, &right_schema, &[])?;
        let (schema, column_indices) =
            build_join_schema(&left_schema, &right_schema, join_type);
        if inequality_conditions.len() != 2 {
            return plan_err!(
                "IEJoinExec only supports two inequality conditions, got {}",
                inequality_conditions.len()
            );
        }
        for condition in &inequality_conditions {
            check_inequality_condition(condition)?;
        }
        let schema = Arc::new(schema);
        if !matches!(join_type, JoinType::Inner) {
            return plan_err!(
                "IEJoinExec only supports inner join currently, got {}",
                join_type
            );
        }
        let cache = Self::compute_properties(
            &left,
            &right,
            Arc::clone(&schema),
            *join_type,
            target_partitions,
        );
        let condition_parts =
            inequality_conditions_to_sort_exprs(&inequality_conditions)?;
        let left_conditions =
            Arc::new([condition_parts[0].0.clone(), condition_parts[1].0.clone()]);
        let right_conditions =
            Arc::new([condition_parts[0].1.clone(), condition_parts[1].1.clone()]);
        let operators = Arc::new([condition_parts[0].2, condition_parts[1].2]);
        let sort_options = Arc::new([
            operator_to_sort_option(operators[0]),
            operator_to_sort_option(operators[1]),
        ]);

        Ok(IEJoinExec {
            left,
            right,
            inequality_conditions,
            filter,
            join_type: *join_type,
            schema,
            iejoin_data: Default::default(),
            left_conditions,
            right_conditions,
            operators,
            sort_options,
            pairs: Arc::new(Mutex::new(0)),
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
        target_partitions: usize,
    ) -> PlanProperties {
        // Calculate equivalence properties:
        let eq_properties = join_equivalence_properties(
            left.equivalence_properties().clone(),
            right.equivalence_properties().clone(),
            &join_type,
            schema,
            &[false, false],
            None,
            // No on columns in iejoin
            &[],
        );

        let output_partitioning = Partitioning::UnknownPartitioning(target_partitions);

        // Determine execution mode
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
                    .map(|c| format!("({})", c.expression()))
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(
                    f,
                    "IEJoinExec: mode={:?}, join_type={:?}, inequality_conditions=[{}]{}",
                    self.cache.execution_mode,
                    self.join_type,
                    display_inequality_conditions,
                    display_filter,
                )
            }
        }
    }
}

/// convert operator to sort option for iejoin
/// for left.a <= right.b, the sort option is ascending order
/// for left.a >= right.b, the sort option is descending order
pub fn operator_to_sort_option(op: Operator) -> SortOptions {
    match op {
        Operator::Lt | Operator::LtEq => SortOptions {
            descending: false,
            nulls_first: false,
        },
        Operator::Gt | Operator::GtEq => SortOptions {
            descending: true,
            nulls_first: false,
        },
        _ => panic!("Unsupported operator"),
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

    fn required_input_ordering(
        &self,
    ) -> Vec<Option<datafusion_physical_expr::LexRequirement>> {
        // sort left and right data by condition 1 to prune not intersected RecordBatch pairs
        vec![
            Some(PhysicalSortRequirement::from_sort_exprs(vec![
                &self.left_conditions[0],
            ])),
            Some(PhysicalSortRequirement::from_sort_exprs(vec![
                &self.right_conditions[0],
            ])),
        ]
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
            self.cache.output_partitioning().partition_count(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let join_metrics = IEJoinMetrics::new(partition, &self.metrics);
        let iejoin_data = self.iejoin_data.once(|| {
            collect_iejoin_data(
                Arc::clone(&self.left),
                Arc::clone(&self.right),
                Arc::clone(&self.left_conditions),
                Arc::clone(&self.right_conditions),
                join_metrics.clone(),
                Arc::clone(&context),
            )
        });
        Ok(Box::pin(IEJoinStream {
            schema: Arc::clone(&self.schema),
            filter: self.filter.clone(),
            _join_type: self.join_type,
            operators: Arc::clone(&self.operators),
            sort_options: Arc::clone(&self.sort_options),
            iejoin_data,
            column_indices: self.column_indices.clone(),
            pairs: Arc::clone(&self.pairs),
            finished: false,
            join_metrics,
        }))
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

/// Metrics for iejoin
#[derive(Debug, Clone)]
struct IEJoinMetrics {
    /// Total time for collecting init data of both sides
    pub(crate) load_time: metrics::Time,
    /// Number of batches of left side
    pub(crate) left_input_batches: metrics::Count,
    /// Number of batches of right side
    pub(crate) right_input_batches: metrics::Count,
    /// Number of rows of left side
    pub(crate) left_input_rows: metrics::Count,
    /// Number of rows of right side
    pub(crate) right_input_rows: metrics::Count,
    /// Memory used by collecting init data
    pub(crate) load_mem_used: metrics::Gauge,
    /// Total time for joining intersection blocks of input table
    pub(crate) join_time: metrics::Time,
    /// Number of batches produced by this operator
    pub(crate) output_batches: metrics::Count,
    /// Number of rows produced by this operator
    pub(crate) output_rows: metrics::Count,
    /// Number of pairs of left and right blocks are skipped because of no intersection
    pub(crate) skipped_pairs: metrics::Count,
}

impl IEJoinMetrics {
    pub fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        let load_time = MetricBuilder::new(metrics).subset_time("load_time", partition);
        let left_input_batches =
            MetricBuilder::new(metrics).counter("left_input_batches", partition);
        let right_input_batches =
            MetricBuilder::new(metrics).counter("right_input_batches", partition);
        let left_input_rows =
            MetricBuilder::new(metrics).counter("left_input_rows", partition);
        let right_input_rows =
            MetricBuilder::new(metrics).counter("right_input_rows", partition);
        let load_mem_used = MetricBuilder::new(metrics).gauge("load_mem_used", partition);
        let join_time = MetricBuilder::new(metrics).subset_time("join_time", partition);
        let output_batches =
            MetricBuilder::new(metrics).counter("output_batches", partition);
        let output_rows = MetricBuilder::new(metrics).counter("output_rows", partition);
        let skipped_pairs =
            MetricBuilder::new(metrics).counter("skipped_pairs", partition);
        Self {
            load_time,
            left_input_batches,
            right_input_batches,
            left_input_rows,
            right_input_rows,
            load_mem_used,
            join_time,
            output_batches,
            output_rows,
            skipped_pairs,
        }
    }
}

#[derive(Debug, Clone)]
/// SortedBlock contains arrays that are sorted by specified columns
// TODO: use struct support spill?
pub struct SortedBlock {
    pub data: RecordBatch,
    pub sort_options: Vec<(usize, SortOptions)>,
}

impl SortedBlock {
    pub fn new(array: Vec<ArrayRef>, sort_options: Vec<(usize, SortOptions)>) -> Self {
        let schema = Arc::new(Schema::new({
            array
                .iter()
                .enumerate()
                .map(|(i, array)| {
                    arrow_schema::Field::new(
                        format!("col{}", i),
                        array.data_type().clone(),
                        true,
                    )
                })
                .collect::<Vec<_>>()
        }));
        let data = RecordBatch::try_new(schema, array).unwrap();
        Self { data, sort_options }
    }

    /// sort the block by the specified columns
    pub fn sort_by_columns(&mut self) -> Result<()> {
        let sort_exprs = self
            .sort_options
            .iter()
            .map(|(i, sort_options)| PhysicalSortExpr {
                expr: Arc::new(datafusion_physical_expr::expressions::Column::new(
                    &format!("col{}", *i),
                    *i,
                )),
                options: *sort_options,
            })
            .collect::<Vec<_>>();
        self.data = sort_batch(&self.data, &sort_exprs, None)?;
        Ok(())
    }

    pub fn arrays(&self) -> &[ArrayRef] {
        self.data.columns()
    }

    pub fn data(&self) -> &RecordBatch {
        &self.data
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        let data = self.data.slice(range.start, range.len());
        SortedBlock {
            data,
            sort_options: self.sort_options.clone(),
        }
    }
}

/// IEJoinData contains all data blocks from left and right side, and the data evaluated by condition 1 and condition 2 from left and right side
#[derive(Debug)]
pub struct IEJoinData {
    /// collected left data after sort by condition 1
    pub left_data: Vec<RecordBatch>,
    /// collected right data after sort by condition 1
    pub right_data: Vec<RecordBatch>,
    /// sorted blocks of left data, contains the evaluated result of condition 1 and condition 2
    pub left_blocks: Vec<SortedBlock>,
    /// sorted blocks of right data, contains the evaluated result of condition 1 and condition 2
    pub right_blocks: Vec<SortedBlock>,
}

async fn collect_iejoin_data(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    left_conditions: Arc<[PhysicalSortExpr; 2]>,
    right_conditions: Arc<[PhysicalSortExpr; 2]>,
    join_metrics: IEJoinMetrics,
    context: Arc<TaskContext>,
) -> Result<IEJoinData> {
    // the left and right data are sort by condition 1 already (the `try_iejoin` rewrite rule has done this), collect it directly
    let left_data = collect(left, Arc::clone(&context)).await?;
    let right_data = collect(right, Arc::clone(&context)).await?;
    let left_blocks = left_data
        .iter()
        .map(|batch| {
            join_metrics.left_input_batches.add(1);
            join_metrics.left_input_rows.add(batch.num_rows());
            join_metrics
                .load_mem_used
                .add(batch.get_array_memory_size());
            let columns = left_conditions
                .iter()
                .map(|expr| expr.expr.evaluate(batch)?.into_array(batch.num_rows()))
                .collect::<Result<Vec<_>>>()?;
            Ok(SortedBlock::new(columns, vec![]))
        })
        .collect::<Result<Vec<_>>>()?;
    left_blocks.iter().for_each(|block| {
        join_metrics
            .load_mem_used
            .add(block.data().get_array_memory_size())
    });
    let right_blocks = right_data
        .iter()
        .map(|batch| {
            join_metrics.right_input_batches.add(1);
            join_metrics.right_input_rows.add(batch.num_rows());
            join_metrics
                .load_mem_used
                .add(batch.get_array_memory_size());
            let columns = right_conditions
                .iter()
                .map(|expr| expr.expr.evaluate(batch)?.into_array(batch.num_rows()))
                .collect::<Result<Vec<_>>>()?;
            Ok(SortedBlock::new(columns, vec![]))
        })
        .collect::<Result<Vec<_>>>()?;
    right_blocks.iter().for_each(|block| {
        join_metrics
            .load_mem_used
            .add(block.data().get_array_memory_size())
    });
    Ok(IEJoinData {
        left_data,
        right_data,
        left_blocks,
        right_blocks,
    })
}

struct IEJoinStream {
    /// input schema
    schema: Arc<Schema>,
    /// join filter
    filter: Option<JoinFilter>,
    /// type of the join
    /// Only support inner join currently
    _join_type: JoinType,
    /// operator of the inequality condition
    operators: Arc<[Operator; 2]>,
    /// sort options of the inequality condition
    sort_options: Arc<[SortOptions; 2]>,
    /// iejoin data
    iejoin_data: OnceFut<IEJoinData>,
    /// column indices
    column_indices: Vec<ColumnIndex>,
    /// partition pair
    pairs: Arc<Mutex<u64>>,
    /// finished
    finished: bool,
    /// join metrics
    join_metrics: IEJoinMetrics,
}

impl IEJoinStream {
    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if self.finished {
            return Poll::Ready(None);
        }

        let load_timer = self.join_metrics.load_time.timer();
        let iejoin_data = match ready!(self.iejoin_data.get_shared(cx)) {
            Ok(data) => data,
            Err(e) => return Poll::Ready(Some(Err(e))),
        };
        load_timer.done();

        // get the size of left and right blocks
        let (n, m) = (iejoin_data.left_data.len(), iejoin_data.right_data.len());

        // get pair of left and right blocks, add 1 to the pair
        let pair = {
            let mut pair = self.pairs.lock();
            let p = *pair;
            *pair += 1;
            p
        };

        // no more block pair to join
        if pair >= (n * m) as u64 {
            self.finished = true;
            return Poll::Ready(None);
        }
        // get the index of left and right block
        let (left_block_idx, right_block_idx) =
            ((pair / m as u64) as usize, (pair % m as u64) as usize);

        // get the left and right block
        let left_block = &(iejoin_data.left_blocks[left_block_idx]);
        let right_block = &(iejoin_data.right_blocks[right_block_idx]);

        // no intersection between two blocks
        if !IEJoinStream::check_intersection(
            left_block,
            right_block,
            &self.sort_options[0],
        ) {
            self.join_metrics.skipped_pairs.add(1);
            return Poll::Ready(Some(Ok(RecordBatch::new_empty(Arc::clone(
                &self.schema,
            )))));
        }

        let join_timer = self.join_metrics.join_time.timer();
        // compute the join result
        // TODO: should return batches one by one if the result size larger than the batch size in config?
        let batch = IEJoinStream::compute(
            left_block,
            right_block,
            &self.sort_options,
            &self.operators,
            &iejoin_data.left_data[left_block_idx],
            &iejoin_data.right_data[right_block_idx],
            &self.filter,
            &self.schema,
            &self.column_indices,
        )?;
        join_timer.done();
        self.join_metrics.output_batches.add(1);
        self.join_metrics.output_rows.add(batch.num_rows());
        Poll::Ready(Some(Ok(batch)))
    }

    #[allow(clippy::too_many_arguments)]
    fn compute(
        left_block: &SortedBlock,
        right_block: &SortedBlock,
        sort_options: &[SortOptions; 2],
        operators: &[Operator; 2],
        left_data: &RecordBatch,
        right_data: &RecordBatch,
        filter: &Option<JoinFilter>,
        schema: &Arc<Schema>,
        column_indices: &[ColumnIndex],
    ) -> Result<RecordBatch> {
        let (l1_indexes, permutation) = IEJoinStream::compute_permutation(
            left_block,
            right_block,
            sort_options,
            operators,
        )?;

        // compute the join indices statify the inequality conditions
        let (left_indices, right_indices) =
            IEJoinStream::build_join_indices(&l1_indexes, &permutation)?;

        // apply the filter to the join result
        let (left_indices, right_indices) = if let Some(filter) = filter {
            apply_join_filter_to_indices(
                left_data,
                right_data,
                left_indices,
                right_indices,
                filter,
                JoinSide::Left,
            )?
        } else {
            (left_indices, right_indices)
        };

        build_batch_from_indices(
            schema,
            left_data,
            right_data,
            &left_indices,
            &right_indices,
            column_indices,
            JoinSide::Left,
        )
    }

    /// check if there is an intersection between two sorted blocks
    fn check_intersection(
        left_block: &SortedBlock,
        right_block: &SortedBlock,
        sort_options: &SortOptions,
    ) -> bool {
        // filter all null result
        if left_block.arrays()[0].null_count() == left_block.arrays()[0].len()
            || right_block.arrays()[0].null_count() == right_block.arrays()[0].len()
        {
            return false;
        }
        let comparator = make_comparator(
            &left_block.arrays()[0],
            &right_block.arrays()[0],
            *sort_options,
        )
        .unwrap();
        // get the valid count of right block
        let m = right_block.arrays()[0].len() - right_block.arrays()[0].null_count();
        // if the max valid element of right block is smaller than the min valid element of left block, there is no intersection
        // for example, if left.a <= right.b, the left block is \[7, 8, 9\], the right block is \[2, 4, 6\], left\[0\] greater than right\[2\] so there is no intersection between left block and right block
        // if left.a >= right.b, the left block is \[1, 0, 0\], the right block is \[6, 4, 2\], left\[0\] lesser than right\[2\] (because the sort options used in `make_comparator` is desc, so the compare result will be greater) so there is no intersection between left block and right block
        if comparator(0, m - 1) == std::cmp::Ordering::Greater {
            return false;
        }
        true
    }

    /// this function computes l1_indexes array and the permutation array of condition 2 on condition 1
    /// for example, if condition 1 is left.a <= right.b, condition 2 is left.x <= right.y
    /// for left table, we have:
    /// | id    | a  | x |
    /// |-------|----|---|
    /// | left1 | 1  | 7 |
    /// | left2 | 3  | 4 |
    /// for right table, we have:
    /// | id | b | y |
    /// |----|---|---|
    /// | right1 | 2 | 5 |
    /// | right2 | 4 | 6 |
    /// Sort by condition 1, we get l1:
    /// | value | 1 | 2 | 3 | 4 |
    /// |-------|---|---|---|---|
    /// | id    | left1 | right1 | left2 | right2 |
    /// The l1_indexes array is [-1, 1, -2, 2], the negative value means it is the index of left table, the positive value means it is the index of right table, the absolute value is the index of original recordbatch
    /// Sort by condition 2, we get l2:
    /// | value | 4 | 5 | 6 | 7 |
    /// |-------|---|---|---|---|
    /// | id    | left2 | right1 | right2 | left1 |
    /// Then the permutation array is [2, 1, 3, 0]
    /// The first element of l2 is left2, which is the 3rd element(index 2) of l1. The second element of l2 is right1, which is the 2nd element(index 1) of l1. And so on.
    fn compute_permutation(
        left_block: &SortedBlock,
        right_block: &SortedBlock,
        sort_options: &[SortOptions; 2],
        operators: &[Operator; 2],
    ) -> Result<(Int64Array, UInt64Array)> {
        // step1. sort the union block l1
        let n = left_block.arrays()[0].len() as i64;
        let m = right_block.arrays()[0].len() as i64;
        // concat the left block and right block
        let cond1 = concat(&[
            &Arc::clone(&left_block.arrays()[0]),
            &Arc::clone(&right_block.arrays()[0]),
        ])?;
        let cond2 = concat(&[
            &Arc::clone(&left_block.arrays()[1]),
            &Arc::clone(&right_block.arrays()[1]),
        ])?;
        // store index of left table and right table
        // -i in (-n..-1) means it is index i in left table, j in (1..m) means it is index j in right table
        let indexes = concat(&[
            &Int64Array::from((1..=n).map(|i| -i).collect::<Vec<_>>()),
            &Int64Array::from((1..=m).collect::<Vec<_>>()),
        ])?;
        let mut l1 = SortedBlock::new(
            vec![cond1, indexes, cond2],
            vec![
                // order by condition 1
                (0, sort_options[0]),
                (
                    1,
                    SortOptions {
                        // if the operator is loose inequality, let the right index (> 0) in backward of left index (< 0)
                        // otherwise, let the right index (> 0) in forward of left index (< 0)
                        // for example, t1.time <= t2.time
                        // | value| 1      | 1      | 1     | 1     | 2     |
                        // |------|--------|--------|-------|-------|-------|
                        // | index| -2(l2) | -1(l2) | 1(r1) | 2(r2) | 3(r3) |
                        // if t1.time < t2.time
                        // |value| 1      | 1      | 1     | 1     | 2     |
                        // |-----|--------|--------|-------|-------|-------|
                        // |index| 2(r2) | 1(r1) | -1(l2) | -2(l1) | 3(r3) |
                        // according to this order request, if i < j then value\[i\](from left table) and value\[j\](from right table) match the condition(t1.time <= t2.time or t1.time < t2.time)
                        descending: !is_loose_inequality_operator(&operators[0]),
                        nulls_first: false,
                    },
                ),
            ],
        );
        l1.sort_by_columns()?;
        // ignore the null values of the first condition
        let valid = (l1.arrays()[0].len() - l1.arrays()[0].null_count()) as i64;
        let l1 = l1.slice(0..valid as usize);

        // l1_indexes\[i\] = j means the ith element of l1 is the jth element of original recordbatch
        let l1_indexes = Arc::clone(&l1.arrays()[1])
            .as_primitive::<Int64Type>()
            .clone();

        // mark the order of l1, the index i means this element is the ith element of l1(sorted by condition 1)
        let permutation = UInt64Array::from((0..valid as u64).collect::<Vec<_>>());

        let mut l2 = SortedBlock::new(
            vec![
                // condition 2
                Arc::clone(&l1.arrays()[2]),
                // index of original recordbatch
                Arc::clone(&l1.arrays()[1]),
                // index of l1
                Arc::new(permutation),
            ],
            vec![
                // order by condition 2
                (0, sort_options[1]),
                (
                    1,
                    SortOptions {
                        // same as above
                        descending: !is_loose_inequality_operator(&operators[1]),
                        nulls_first: false,
                    },
                ),
            ],
        );
        l2.sort_by_columns()?;
        let valid = (l2.arrays()[0].len() - l2.arrays()[0].null_count()) as usize;
        let l2 = l2.slice(0..valid);

        Ok((
            l1_indexes,
            Arc::clone(&l2.arrays()[2])
                .as_primitive::<UInt64Type>()
                .clone(),
        ))
    }

    /// compute the join indices statify the inequality conditions
    /// following the example in `compute_permutation`, the l1_indexes is \[1, -1, 2, -2\], the permutation is \[2, 1, 3, 0\]
    /// range_map is empty at first
    /// 1、 p\[0\] = 2, range_map is empty, l1_indexes\[2\] is greater than 0, it means 2nd element in l1 is from left table, insert(2) into range_map, range_map {(2, 3)}
    /// 2、 p\[1\] = 1, no value less than p\[1\] in range_map, l1_indexes\[1\] is less than 0, it means 1st element in l1 is from right table, no need to insert(1) into range_map, range_map {(2, 3)}
    /// 3、 p\[2\] = 3, found 2 less than p\[2\] in range_map, append all pairs (l1_indexes\[2\], l1_indexes\[3\]) to the indeices array, l1_indexes\[3\] is less than 0, it means 3rd element in l1 is from right table, no need to insert(3) into range_map, range_map {(1, 4)}
    /// 4、 p\[3\] = 0, no value less than p\[3\] in range_map, insert(0) into range_map, range_map {(0, 1), (2, 3)}
    /// The indices array is \[(2), (2)\]
    fn build_join_indices(
        l1_indexes: &Int64Array,
        permutation: &UInt64Array,
    ) -> Result<(UInt64Array, UInt64Array)> {
        let mut left_builder = UInt64Builder::new();
        let mut right_builder = UInt64Builder::new();
        // left_order\[i\] = l means there are l elements from left table in l1\[0..=i\], also means element i is the l-th smallest element in left recordbatch.
        let mut left_order = UInt64Array::builder(l1_indexes.len());
        let mut l_pos = 0;
        for ind in l1_indexes.values().iter() {
            if *ind < 0 {
                l_pos += 1;
            }
            left_order.append_value(l_pos);
        }
        let left_order = left_order.finish();
        // use btree map to maintain all p\[i\], for i in 0..j, map\[s\]=t means range \[s, t\) is valid
        // our target is to find all pair(i, j) that i<j and p\[i\] < p\[j\] and i from left table and j from right table here
        // range_map use key as end index and value as start index to represent a interval [start, end)
        let mut range_map = BTreeMap::<u64, u64>::new();
        for p in permutation.values().iter() {
            // get the index of original recordbatch
            let l1_index = unsafe { l1_indexes.value_unchecked(*p as usize) };
            if l1_index < 0 {
                // index from left table
                // insert p in to range_map
                IEJoinStream::insert_range_map(&mut range_map, unsafe {
                    left_order.value_unchecked(*p as usize)
                });
                continue;
            }
            // index from right table, remap to 0..m
            let right_index = (l1_index - 1) as u64;
            // r\[right_index] in right table and l\[0..=rp\] in left table statisfy comparsion requirement of condition1
            let rp = unsafe { left_order.value_unchecked(*p as usize) };
            for range in range_map.iter() {
                let (end, start) = range;
                if *start > rp {
                    break;
                }
                let (start, end) = (*start, std::cmp::min(*end, rp + 1));
                for left_index in start..end {
                    left_builder.append_value(left_index - 1);
                    // append right index
                    right_builder.append_value(right_index);
                }
            }
        }
        Ok((left_builder.finish(), right_builder.finish()))
    }

    #[inline]
    fn insert_range_map(range_map: &mut BTreeMap<u64, u64>, p: u64) {
        let mut range = (p, p + 1);
        let mut need_insert = true;
        let mut need_remove = false;
        // merge it with prev consecutive range
        // for example, if range_map is [(1, 2), (3, 4), (5, 6)], then insert(2) will make it [(1, 3), (3, 4), (5, 6)]
        let mut iter = range_map.range_mut(p..);
        let mut interval = iter.next();
        let mut move_next = false;
        if let Some(ref interval) = interval {
            if interval.0 == &p {
                // merge prev range, update current range.start
                range = (*interval.1, p + 1);
                // remove prev range
                need_remove = true;
                // move to next range
                move_next = true;
            }
        }
        if move_next {
            interval = iter.next();
        }
        // if previous range is consecutive, merge them
        // follow the example, [(1, 3), (3, 4), (5, 6)] will be merged into [(1, 4), (5, 6)]
        if let Some(ref mut interval) = interval {
            if *interval.1 == range.1 {
                // merge into next range, update next range.start
                *interval.1 = range.0;
                // already merge into next range, no need to insert current range
                need_insert = false;
            }
        }
        if need_remove {
            range_map.remove(&p);
        }
        // if this range is not consecutive with previous one, insert it
        if need_insert {
            range_map.insert(range.1, range.0);
        }
    }
}

impl Stream for IEJoinStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

impl RecordBatchStream for IEJoinStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{common, memory::MemoryExec, test::build_table_i32_with_nulls};

    use arrow::datatypes::{DataType, Field};
    use datafusion_common::{assert_batches_sorted_eq, ScalarValue};
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal};
    use datafusion_physical_expr::PhysicalExpr;

    use itertools::Itertools;

    #[test]
    fn test_insert_range_map() {
        let mut range_map = BTreeMap::new();
        // shuffle 0..8 and insert it into range_map
        let values = (0..8).collect::<Vec<_>>();
        // test for all permutation of 0..8
        for permutaion in values.iter().permutations(values.len()) {
            range_map.clear();
            for v in permutaion.iter() {
                IEJoinStream::insert_range_map(&mut range_map, **v as u64);
            }
            assert_eq!(range_map.len(), 1);
        }
    }

    fn build_table(
        a: (&str, &Vec<Option<i32>>),
        b: (&str, &Vec<Option<i32>>),
        c: (&str, &Vec<Option<i32>>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32_with_nulls(a, b, c);
        let schema = batch.schema();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    /// Returns the column names on the schema
    fn columns(schema: &Schema) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }

    async fn multi_partitioned_join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_type: &JoinType,
        ie_join_filter: Vec<JoinFilter>,
        join_filter: Option<JoinFilter>,
        context: Arc<TaskContext>,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let partition_count = 4;

        let ie_join = IEJoinExec::try_new(
            left,
            right,
            ie_join_filter,
            join_filter,
            join_type,
            partition_count,
        )?;
        let columns = columns(&ie_join.schema());
        let mut batches = vec![];
        for i in 0..partition_count {
            let stream = ie_join.execute(i, Arc::clone(&context))?;
            let more_batches = common::collect(stream).await?;
            batches.extend(
                more_batches
                    .into_iter()
                    .filter(|b| b.num_rows() > 0)
                    .collect::<Vec<_>>(),
            );
        }
        Ok((columns, batches))
    }

    #[tokio::test]
    async fn test_ie_join() -> Result<()> {
        let column_indices = vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 1,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
            ColumnIndex {
                index: 1,
                side: JoinSide::Right,
            },
            ColumnIndex {
                index: 2,
                side: JoinSide::Right,
            },
        ];
        let intermediate_schema = Schema::new(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("y", DataType::Int32, true),
            Field::new("x", DataType::Int32, true),
            Field::new("y", DataType::Int32, true),
            Field::new("z", DataType::Int32, true),
        ]);
        // test left.x < right.x and left.y >= right.y
        let filter1 = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::Lt,
            Arc::new(Column::new("x", 2)),
        )) as Arc<dyn PhysicalExpr>;
        let filter2 = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("y", 1)),
            Operator::GtEq,
            Arc::new(Column::new("y", 3)),
        )) as Arc<dyn PhysicalExpr>;
        let ie_filter = vec![
            JoinFilter::new(filter1, column_indices.clone(), intermediate_schema.clone()),
            JoinFilter::new(filter2, column_indices.clone(), intermediate_schema.clone()),
        ];
        let join_filter = Some(JoinFilter::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("z", 4)),
                Operator::NotEq,
                Arc::new(Literal::new(ScalarValue::Int32(Some(8)))),
            )),
            column_indices.clone(),
            intermediate_schema.clone(),
        ));
        //
        let left = build_table(
            ("x", &vec![Some(5), Some(9), None]),
            ("y", &vec![Some(6), Some(10), Some(10)]),
            ("z", &vec![Some(3), Some(5), Some(10)]),
        );
        let right = build_table(
            (
                "x",
                &vec![
                    Some(10),
                    Some(6),
                    Some(5),
                    Some(6),
                    Some(6),
                    Some(6),
                    Some(6),
                    Some(6),
                ],
            ),
            (
                "y",
                &vec![
                    Some(9),
                    Some(6),
                    Some(5),
                    Some(5),
                    Some(6),
                    Some(7),
                    Some(6),
                    None,
                ],
            ),
            (
                "z",
                &vec![
                    Some(7),
                    Some(3),
                    Some(5),
                    Some(5),
                    Some(7),
                    Some(7),
                    Some(8),
                    Some(9),
                ],
            ),
        );
        let task_ctx = Arc::new(TaskContext::default());
        let (columns, batches) = multi_partitioned_join_collect(
            Arc::clone(&left),
            Arc::clone(&right),
            &JoinType::Inner,
            ie_filter,
            join_filter,
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["x", "y", "z", "x", "y", "z"]);
        let expected = [
            "+---+----+---+----+---+---+",
            "| x | y  | z | x  | y | z |",
            "+---+----+---+----+---+---+",
            "| 5 | 6  | 3 | 6  | 5 | 5 |",
            "| 5 | 6  | 3 | 6  | 6 | 3 |",
            "| 5 | 6  | 3 | 6  | 6 | 7 |",
            "| 9 | 10 | 5 | 10 | 9 | 7 |",
            "+---+----+---+----+---+---+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }
}
