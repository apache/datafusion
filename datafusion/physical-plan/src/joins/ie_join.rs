use dashmap::DashMap;
use std::any::Any;
use std::collections::BTreeMap;
use std::fmt::Formatter;
use std::ops::Range;
use std::sync::Arc;
use std::task::Poll;

use crate::joins::utils::{
    apply_join_filter_to_indices, build_batch_from_indices, build_join_schema,
    check_inequality_conditions, check_join_is_valid, estimate_join_statistics,
    inequality_conditions_to_sort_exprs, is_loose_inequality_operator, ColumnIndex,
    JoinFilter, OnceAsync, OnceFut,
};
use crate::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::sorts::sort::SortExec;
use crate::{
    collect, execution_mode_from_children, DisplayAs, DisplayFormatType, Distribution,
    ExecutionMode, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
};
use arrow::array::{make_comparator, AsArray, UInt64Builder};

use arrow::compute::kernels::sort::SortOptions;
use arrow::compute::kernels::take::take;
use arrow::compute::{concat, lexsort_to_indices, SortColumn};
use arrow::datatypes::{Int64Type, Schema, SchemaRef, UInt64Type};
use arrow::record_batch::RecordBatch;
use arrow_array::{ArrayRef, Int64Array, UInt64Array};
use datafusion_common::{plan_err, JoinSide, Result, Statistics};
use datafusion_execution::TaskContext;
use datafusion_expr::{JoinType, Operator};
use datafusion_physical_expr::equivalence::join_equivalence_properties;

use datafusion_physical_expr::{Partitioning, PhysicalExprRef, PhysicalSortExpr};
use futures::{ready, Stream};
use parking_lot::RwLock;

/// IEJoinExec is optimized join without any equijoin conditions in `ON` clause but with two or more inequality conditions.
/// For more detail algorithm, see https://vldb.org/pvldb/vol8/p2074-khayyat.pdf
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
/// The berif idea of this algorithm is converting it to ordered pair/inversion pair of permutation problem. For a permutation of a[0..n-1], for a pairs (i, j) such that i < j and a[i] < a[j], we call it an ordered pair of permutation.
///
/// For example, for a[0..4] = [2, 1, 3, 0], there are 2 ordered pairs: (2, 3), (1, 3)
///
/// To convert query q to ordered pair of permutation problem. We will do the following steps:
/// 1. Sort t1 union t2 by time in ascending order, mark the sorted table as l1.
/// 2. Sort t1 union t2 by cost in ascending order, mark the sorted table as l2.
/// 3. For each element e_i in l2, find the index j in l1 such that l1[j] = e_i, mark the computed index as permutation array p.
/// 4. Compute the inversion of permutation array p. For a pair (i, j) in l2, if i < j then e_i.cost < e_j.cost because l2 is sorted by cost in ascending order. And if p[i] < p[j], then e_i.time < e_j.time because l1 is sorted by time in ascending order.
/// 5. The result of query q is the pairs (i, j) in l2 such that i < j and p[i] < p[j] and e_i is from right table and e_j is from left table.
///
/// To get the final result, we need to get all the pairs (i, j) in l2 such that i < j and p[i] < p[j] and e_i is from right table and e_j is from left table. We can do this by the following steps:
/// 1. Traverse l2 from left to right, at offset j, we can maintain BtreeSet or bitmap to record all the p[i] that i < j, then find all the pairs (i, j) in l2 such that p[i] < p[j].
///
/// To parallel the above algorithm, we can sort t1 and t2 by time (condition 1) firstly, and repartition the data into N partitions, then join t1[i] and t2[j] respectively. And if the minimum time of t1[i] is greater than the maximum time of t2[j], we can skip the join of t1[i] and t2[j] because there is no join result between them according to condition 1.
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
    /// left condition
    left_conditions: Arc<[PhysicalSortExpr; 2]>,
    /// right condition
    right_conditions: Arc<[PhysicalSortExpr; 2]>,
    /// operator of the inequality condition
    operators: Arc<[Operator; 2]>,
    /// sort options of the inequality condition
    sort_options: Arc<[SortOptions; 2]>,
    /// data blocks from left table, store evaluated result of left expr for each record batch
    /// TODO: use OnceAsync to store the data blocks asynchronously
    left_blocks: DashMap<usize, Arc<SortedBlock>>,
    /// data blocks from right table, store evaluated result of right expr for each record batch
    right_blocks: DashMap<usize, Arc<SortedBlock>>,
    /// partition pairs
    /// TODO: we can use a channel to store the pairs
    pairs: RwLock<Option<(usize, usize)>>,
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
        if inequality_conditions.len() != 2 {
            return plan_err!(
                "IEJoinExec only supports two inequality conditions, got {}",
                inequality_conditions.len()
            );
        }
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
        let condition_parts =
            inequality_conditions_to_sort_exprs(&inequality_conditions)?;
        let left_conditions =
            Arc::new([condition_parts[0].0.clone(), condition_parts[1].0.clone()]);
        let right_conditions =
            Arc::new([condition_parts[0].1.clone(), condition_parts[1].1.clone()]);
        let operators =
            Arc::new([condition_parts[0].2.clone(), condition_parts[1].2.clone()]);
        let sort_options = Arc::new([
            operator_to_sort_option(operators[0], false),
            operator_to_sort_option(operators[1], false),
        ]);

        Ok(IEJoinExec {
            left,
            right,
            inequality_conditions,
            filter,
            join_type: *join_type,
            schema,
            left_data: Default::default(),
            right_data: Default::default(),
            left_conditions,
            right_conditions,
            operators,
            left_blocks: DashMap::new(),
            right_blocks: DashMap::new(),
            sort_options,
            pairs: RwLock::new(Some((0, 0))),
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

/// generate the next pair of block indices
pub fn get_next_pair(n: usize, m: usize, pair: (usize, usize)) -> Option<(usize, usize)> {
    let (i, j) = pair;
    if j < m - 1 {
        Some((i, j + 1))
    } else if i < n - 1 {
        Some((i + 1, 0))
    } else {
        None
    }
}

/// convert operator to sort option for iejoin
/// for left.a <= right.b, the sort option is ascending order
/// for left.a >= right.b, the sort option is descending order
/// negated is true if need to negate the sort direction
pub fn operator_to_sort_option(op: Operator, negated: bool) -> SortOptions {
    match op {
        Operator::Lt | Operator::LtEq => SortOptions {
            descending: negated,
            nulls_first: false,
        },
        Operator::Gt | Operator::GtEq => SortOptions {
            descending: !negated,
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
        vec![
            Distribution::UnspecifiedDistribution,
            Distribution::UnspecifiedDistribution,
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

#[derive(Debug)]
/// SortedBlock contains arrays that are sorted by specified columns
pub struct SortedBlock {
    pub array: Vec<ArrayRef>,
    pub sort_options: Vec<(usize, SortOptions)>,
}

impl SortedBlock {
    pub fn new(array: Vec<ArrayRef>, sort_options: Vec<(usize, SortOptions)>) -> Self {
        Self {
            array,
            sort_options,
        }
    }

    /// sort the block by the specified columns
    pub fn sort_by_columns(&mut self) -> Result<()> {
        let sort_columns = self
            .sort_options
            .iter()
            .map(|(i, opt)| SortColumn {
                values: self.array[*i].clone(),
                options: Some(*opt),
            })
            .collect::<Vec<_>>();
        let indices = lexsort_to_indices(&sort_columns, None)?;
        self.array = self
            .array
            .iter()
            .map(|array| take(array, &indices, None))
            .collect::<Result<_, _>>()?;
        Ok(())
    }

    pub fn arrays(&self) -> &[ArrayRef] {
        &self.array
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        let array = self
            .array
            .iter()
            .map(|array| array.slice(range.start, range.end - range.start))
            .collect();
        SortedBlock::new(array, self.sort_options.clone())
    }
}

/// sort the input plan by the first inequality condition, and collect all the data into sorted blocks
async fn collect_by_condition(
    input: Arc<dyn ExecutionPlan>,
    sort_expr: PhysicalSortExpr,
    context: Arc<TaskContext>,
) -> Result<Vec<RecordBatch>> {
    // let sort_options = sort_expr.options.clone();
    let sort_plan = Arc::new(SortExec::new(vec![sort_expr], input));
    let record_batches = collect(sort_plan, context).await?;
    // let sorted_blocks = record_batches
    //     .into_iter()
    //     .map(|batch| SortedBlock::new(batch.columns().to_vec(), sort_options))
    //     .collect();
    Ok(record_batches)
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
    /// left table data
    left_data: OnceFut<Arc<[RecordBatch]>>,
    /// right table data
    right_data: OnceFut<Arc<[RecordBatch]>>,
    /// column indices
    column_indices: Vec<ColumnIndex>,
    /// partition pair
    pair: (usize, usize),
    /// left block
    left_block: OnceFut<SortedBlock>,
    /// right block
    right_block: OnceFut<SortedBlock>,
    /// finished
    finished: bool,
}

impl IEJoinStream {
    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if self.finished {
            return Poll::Ready(None);
        }
        let left_block = match ready!(self.left_block.get_shared(cx)) {
            Ok(block) => block,
            Err(e) => return Poll::Ready(Some(Err(e))),
        };
        let right_block = match ready!(self.right_block.get_shared(cx)) {
            Ok(block) => block,
            Err(e) => return Poll::Ready(Some(Err(e))),
        };

        if !IEJoinStream::check_intersection(
            &left_block,
            &right_block,
            &self.sort_options[0],
        ) {
            return Poll::Ready(None);
        }

        let (l1_indexes, permutation) = IEJoinStream::compute_permutation(
            &left_block,
            &right_block,
            &self.sort_options,
            &self.operators,
        )?;

        let (left_indices, right_indices) =
            IEJoinStream::build_join_indices(&l1_indexes, &permutation)?;

        let left_batch = match ready!(self.left_data.get_shared(cx)) {
            Ok(batches) => batches[self.pair.0].clone(),
            Err(e) => return Poll::Ready(Some(Err(e))),
        };

        let right_batch = match ready!(self.right_data.get_shared(cx)) {
            Ok(batches) => batches[self.pair.1].clone(),
            Err(e) => return Poll::Ready(Some(Err(e))),
        };

        let (left_indices, right_indices) = if let Some(filter) = &self.filter {
            apply_join_filter_to_indices(
                &left_batch,
                &right_batch,
                left_indices,
                right_indices,
                &filter,
                JoinSide::Left,
            )?
        } else {
            (left_indices, right_indices)
        };

        let batch = build_batch_from_indices(
            &self.schema,
            &left_batch,
            &right_batch,
            &left_indices,
            &right_indices,
            &self.column_indices,
            JoinSide::Left,
        );

        self.finished = true;
        Poll::Ready(Some(batch))
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
        if comparator(0, m - 1) == std::cmp::Ordering::Greater {
            return false;
        }
        true
    }

    /// this function computes the permutation array of condition 2 on condition 1
    /// for example, if condition 1 is left.a <= right.b, condition 2 is left.x <= right.y
    /// for left table, we have:
    /// | id | a | x |
    /// | left1 | 1  | 7 |
    /// | left2 | 3  | 4 |
    /// for right table, we have:
    /// | id | b | y |
    /// | right1 | 2 | 5 |
    /// | right2 | 4 | 6 |
    /// Sort by condition 1, we get l1:
    /// | value | 1 | 2 | 3 | 4 |
    /// | id    | left1 | right1 | left2 | right2 |
    /// Sort by condition 2, we get l2:
    /// | value | 4 | 5 | 6 | 7 |
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
        let n = left_block.array[0].len() as i64;
        let m = right_block.array[0].len() as i64;
        // concat the left block and right block
        let cond1 =
            concat(&[&left_block.array[0].clone(), &right_block.array[0].clone()])?;
        let cond2 =
            concat(&[&left_block.array[1].clone(), &right_block.array[1].clone()])?;
        // store index of left table and right table
        // -i in (-n..-1) means it is index i in left table, j in (1..m) means it is index j in right table
        let indexes = concat(&[
            &Int64Array::from(
                std::iter::successors(
                    Some(-1),
                    |&x| if x > -n { Some(x - 1) } else { None },
                )
                .collect::<Vec<_>>(),
            ),
            &Int64Array::from(
                std::iter::successors(
                    Some(1),
                    |&x| if x < m { Some(x + 1) } else { None },
                )
                .collect::<Vec<_>>(),
            ),
        ])?;
        let mut l1 = SortedBlock::new(
            vec![cond1, indexes, cond2],
            vec![
                (0, sort_options[0]),
                (
                    1,
                    SortOptions {
                        // if the operator is loose inequality,
                        descending: !is_loose_inequality_operator(&operators[0]),
                        nulls_first: false,
                    },
                ),
            ],
        );
        // TODO: use more sort column to handle loose order
        l1.sort_by_columns()?;
        // ignore the null values of the first condition
        // TODO: test all null result.
        let valid = (l1.arrays()[0].len() - l1.arrays()[0].null_count()) as i64;
        let l1 = l1.slice(0..valid as usize);

        // l1_indexes[i] = j means the ith element of l1 is the jth element of original recordbatch
        let l1_indexes = l1.arrays()[1].clone().as_primitive::<Int64Type>().clone();

        let permutation = UInt64Array::from(
            std::iter::successors(Some(0 as u64), |&x| {
                if x < (valid as u64) {
                    Some(x + 1)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>(),
        );

        let mut l2 = SortedBlock::new(
            vec![
                l1.arrays()[2].clone(),
                l1.arrays()[1].clone(),
                Arc::new(permutation),
            ],
            vec![
                (0, sort_options[1]),
                (
                    1,
                    SortOptions {
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
            l2.arrays()[2].clone().as_primitive::<UInt64Type>().clone(),
        ))
    }

    fn build_join_indices(
        l1_indexes: &Int64Array,
        permutation: &UInt64Array,
    ) -> Result<(UInt64Array, UInt64Array)> {
        let mut left_builder = UInt64Builder::new();
        let mut right_builder = UInt64Builder::new();
        let mut range_map = BTreeMap::<u64, u64>::new();
        for p in permutation.values().iter() {
            let l1_index = unsafe { l1_indexes.value_unchecked(*p as usize) };
            if l1_index < 0 {
                // index from left table
                IEJoinStream::insert_range_map(&mut range_map, *p as u64);
                continue;
            }
            // index from right table, remap to 0..m
            let right_index = (l1_index - 1) as u64;
            for range in range_map.range(0..(*p as u64)) {
                let (start, end) = range;
                let (start, end) = (*start, std::cmp::min(*end, *p as u64));
                for left_index in start..end {
                    left_builder.append_value(
                        -(unsafe { l1_indexes.value_unchecked(left_index as usize) } + 1)
                            as u64,
                    );
                    right_builder.append_value(right_index);
                }
            }
        }
        Ok((left_builder.finish(), right_builder.finish()))
    }

    fn insert_range_map(range_map: &mut BTreeMap<u64, u64>, p: u64) {
        let mut range = (p, p + 1);
        // merge it with next consecutive range
        // for example, if range_map is [(1, 2), (3, 4), (5, 6)], then insert(2) will make it [(1, 2), (2, 4), (5, 6)]
        if let Some(end) = range_map.get(&(p + 1)) {
            range = (p, *end);
            range_map.remove(&(p + 1));
        }
        let mut need_insert = true;
        let up_range = range_map.range_mut(0..p);
        // if previous range is consecutive, merge them
        // follow the example, [(1, 2), (2, 4), (5, 6)] will be merged into [(1, 4), (5, 6)]
        if let Some(head) = up_range.last() {
            if head.1 == &p {
                *head.1 = range.1;
                need_insert = false;
            }
        }
        // if this range is not consecutive with previous one, insert it
        if need_insert {
            range_map.insert(range.0, range.1);
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
    use std::cmp::Ordering;

    use arrow::array::make_comparator;
    use arrow_array::Int32Array;
    use arrow_schema::SortOptions;

    #[test]
    fn test_compactor() {
        let array1 = Int32Array::from(vec![Some(1), None]);
        let array2 = Int32Array::from(vec![None, Some(2)]);
        let cmp = make_comparator(&array1, &array2, SortOptions::default()).unwrap();

        assert_eq!(cmp(0, 1), Ordering::Less); // Some(1) vs Some(2)
        assert_eq!(cmp(1, 1), Ordering::Less); // None vs Some(2)
        assert_eq!(cmp(1, 0), Ordering::Equal); // None vs None
        assert_eq!(cmp(0, 0), Ordering::Greater); // Some(1) vs None
    }

    #[test]
    fn test_successor() {
        let iter =
            std::iter::successors(Some(-1), |&x| if x > -4 { Some(x - 1) } else { None });
        let vec = iter.collect::<Vec<_>>();
        assert_eq!(vec, vec![-1, -2, -3, -4]);
    }
}
