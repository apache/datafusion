use crate::datasource::physical_plan::{CsvExec, ParquetExec};
use crate::physical_optimizer::sort_enforcement::ExecTree;
use crate::physical_optimizer::utils::is_repartition;
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::union::{can_interleave, InterleaveExec, UnionExec};
use crate::physical_plan::{
    with_new_children_if_necessary, Distribution, ExecutionPlan, Partitioning,
};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, VisitRecursion};
use datafusion_common::{DataFusionError, Result};
use datafusion_physical_expr::PhysicalExpr;
use itertools::izip;
use std::sync::Arc;

#[derive(Default)]
pub struct EnforceDistributionV2 {}

impl EnforceDistributionV2 {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for EnforceDistributionV2 {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let target_partitions = config.execution.target_partitions;
        let enable_roundrobin = config.optimizer.enable_round_robin_repartition;
        let repartition_file_scans = config.optimizer.repartition_file_scans;
        let repartition_file_min_size = config.optimizer.repartition_file_min_size;
        let repartition_context = RepartitionContext::new(plan);
        // let res = repartition_context.transform_up(&|plan_with_pipeline_fixer| {ensure_distribution(repartition_context, target_partitions)})?;

        // Distribution enforcement needs to be applied bottom-up.
        let updated_plan = repartition_context.transform_up(&|repartition_context| {
            ensure_distribution(
                repartition_context,
                target_partitions,
                enable_roundrobin,
                repartition_file_scans,
                repartition_file_min_size,
            )
        })?;

        Ok(updated_plan.plan)
    }

    fn name(&self) -> &str {
        "EnforceDistributionV2"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn are_exprs_equal(lhs: &[Arc<dyn PhysicalExpr>], rhs: &[Arc<dyn PhysicalExpr>]) -> bool {
    if lhs.len() != rhs.len() {
        false
    } else {
        izip!(lhs.iter(), rhs.iter()).all(|(lhs, rhs)| lhs.eq(rhs))
    }
}

fn add_roundrobin_on_top(
    input: Arc<dyn ExecutionPlan>,
    n_target: usize,
    repartition_onward: &mut Option<ExecTree>,
) -> Result<(Arc<dyn ExecutionPlan>, bool)> {
    Ok(
        if input.output_partitioning().partition_count() < n_target {
            let new_plan = Arc::new(RepartitionExec::try_new(
                input,
                Partitioning::RoundRobinBatch(n_target),
            )?) as Arc<dyn ExecutionPlan>;
            if let Some(exec_tree) = repartition_onward {
                panic!(
                    "exectree should have been empty, exec tree:{:?} ",
                    exec_tree
                );
            }
            // Initialize new onward
            *repartition_onward = Some(ExecTree::new(new_plan.clone(), 0, vec![]));
            (new_plan, true)
        } else {
            (input, false)
        },
    )
}

fn add_hash_on_top(
    input: Arc<dyn ExecutionPlan>,
    hash_exprs: Vec<Arc<dyn PhysicalExpr>>,
    n_target: usize,
    repartition_onward: &mut Option<ExecTree>,
) -> Result<(Arc<dyn ExecutionPlan>, bool)> {
    if n_target == 1 {
        return Ok((input, false));
    }
    if let Partitioning::Hash(exprs, n_partition) = input.output_partitioning() {
        if are_exprs_equal(&exprs, &hash_exprs) && n_partition == n_target {
            return Ok((input, false));
        }
    }
    let new_plan = Arc::new(RepartitionExec::try_new(
        input,
        Partitioning::Hash(hash_exprs, n_target),
    )?) as Arc<dyn ExecutionPlan>;
    if let Some(exec_tree) = repartition_onward {
        *exec_tree = ExecTree::new(new_plan.clone(), 0, vec![exec_tree.clone()]);
    } else {
        // Initialize new onward
        *repartition_onward = Some(ExecTree::new(new_plan.clone(), 0, vec![]));
    }
    Ok((new_plan, true))
}

fn update_repartition_from_context(
    repartition_context: &RepartitionContext,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut new_children = repartition_context.plan.children();
    let benefits_from_partitioning =
        repartition_context.plan.benefits_from_input_partitioning();
    let required_input_orderings = repartition_context.plan.required_input_ordering();
    for (child, repartition_onwards, benefits, required_ordering) in izip!(
        new_children.iter_mut(),
        repartition_context.repartition_onwards.iter(),
        benefits_from_partitioning.into_iter(),
        required_input_orderings.into_iter(),
    ) {
        // if repartition masses with ordering requirement
        if required_ordering.is_some() {
            if let Some(exec_tree) = repartition_onwards {
                *child = remove_parallelization(exec_tree)?;
            }
        }
    }
    repartition_context
        .plan
        .clone()
        .with_new_children(new_children)
}

fn remove_parallelization(exec_tree: &ExecTree) -> Result<Arc<dyn ExecutionPlan>> {
    let mut updated_children = exec_tree.plan.children();
    for child in &exec_tree.children {
        let child_idx = child.idx;
        let new_child = remove_parallelization(&child)?;
        updated_children[child_idx] = new_child;
    }
    if let Some(repartition) = exec_tree.plan.as_any().downcast_ref::<RepartitionExec>() {
        // Leaf node
        if let Partitioning::RoundRobinBatch(_n_target) = repartition.partitioning() {
            // If repartition input have an ordering, remove repartition (masses with ordering)
            // otherwise keep it as is. Because repartition doesn't affect ordering.
            if repartition.input().output_ordering().is_some() {
                return Ok(repartition.input().clone());
            }
        }
    }
    exec_tree.plan.clone().with_new_children(updated_children)
}

fn print_plan(plan: &Arc<dyn ExecutionPlan>) -> () {
    let formatted = crate::physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    println!("{:#?}", actual);
}

fn ensure_distribution(
    repartition_context: RepartitionContext,
    target_partitions: usize,
    enable_round_robin: bool,
    repartition_file_scans: bool,
    repartition_file_min_size: usize,
) -> Result<Transformed<RepartitionContext>> {
    // let plan = repartition_context.plan;
    // let repartition_onwards = repartition_context.repartition_onwards;

    if repartition_context.plan.children().is_empty() {
        return Ok(Transformed::No(repartition_context));
    }
    // // Don't need to apply when the returned row count is not greater than 1
    // let stats = repartition_context.plan.statistics();
    // if stats.is_exact {
    //     let should_repartition =
    //         stats.num_rows.map(|num_rows| num_rows > 1).unwrap_or(true);
    //     if !should_repartition {
    //         return Ok(Transformed::No(repartition_context));
    //     }
    // }

    let mut is_updated = false;

    // println!("start");
    // print_plan(&repartition_context.plan);
    // println!("WOULD BENEFIT:{:?}", repartition_context.plan.benefits_from_input_partitioning());
    // println!("plan required dist: {:?}", repartition_context.plan.required_input_distribution());
    // println!("plan required ordering: {:?}", repartition_context.plan.required_input_ordering());

    let (plan, mut updated_repartition_onwards) = if repartition_context
        .plan
        .required_input_ordering()
        .iter()
        .any(|item| item.is_some())
    {
        // println!("----------start--------------");
        // print_plan(&repartition_context.plan);

        let new_plan = update_repartition_from_context(&repartition_context)?;

        // print_plan(&new_plan);
        // println!("----------end------------");
        let n_child = new_plan.children().len();
        is_updated = true;
        (new_plan, vec![None; n_child])
    } else {
        (
            repartition_context.plan.clone(),
            repartition_context.repartition_onwards.clone(),
        )
    };
    let new_children_with_flags = izip!(
        plan.children().iter(),
        plan.required_input_distribution().iter(),
        plan.required_input_ordering().iter(),
        plan.maintains_input_order().iter(),
        updated_repartition_onwards.iter_mut(),
        plan.benefits_from_input_partitioning()
    )
    .map(
        |(
            child,
            requirement,
            required_input_ordering,
            maintains,
            repartition_onward,
            would_benefit,
        )| {
            let mut new_child = child.clone();
            let mut is_changed = false;

            // // We can reorder a child if:
            // //   - It has no ordering to preserve, or
            // //   - Its parent has no required input ordering and does not
            // //     maintain input ordering.
            // //   - input partition is 1, in this case repartition preservers ordering.
            // // Check if this condition holds:
            // let can_reorder = child.output_ordering().is_none()
            //     || (!required_input_ordering.is_some() && !maintains);
            //     // || child.output_partitioning().partition_count() == 1;

            if enable_round_robin
                && would_benefit
                && child.output_partitioning().partition_count() < target_partitions
                && (required_input_ordering.is_none()
                    || child.output_ordering().is_none())
            {
                // For ParquetExec return internally repartitioned version of the plan in case `repartition_file_scans` is set
                if let Some(parquet_exec) =
                    new_child.as_any().downcast_ref::<ParquetExec>()
                {
                    if repartition_file_scans {
                        new_child = Arc::new(parquet_exec.get_repartitioned(
                            target_partitions,
                            repartition_file_min_size,
                        )) as _;
                        is_changed = true;
                    }
                }

                if let Some(csv_exec) = new_child.as_any().downcast_ref::<CsvExec>() {
                    if repartition_file_scans {
                        if let Some(csv_exec) = csv_exec.get_repartitioned(
                            target_partitions,
                            repartition_file_min_size,
                        ) {
                            new_child = Arc::new(csv_exec) as _;
                            is_changed = true;
                            // return Ok((plan, true));
                        }
                    }
                }
                let mut is_changed_new = false;
                (new_child, is_changed_new) = add_roundrobin_on_top(
                    new_child,
                    target_partitions,
                    repartition_onward,
                )?;
                is_changed = is_changed || is_changed_new;
            }

            match requirement {
                Distribution::SinglePartition => {
                    if child.output_partitioning().partition_count() > 1 {
                        new_child = Arc::new(CoalescePartitionsExec::new(new_child));
                        is_changed = true;
                    }
                }
                Distribution::HashPartitioned(exprs) => {
                    let mut is_changed_new = false;
                    (new_child, is_changed_new) = add_hash_on_top(
                        new_child,
                        exprs.to_vec(),
                        target_partitions,
                        repartition_onward,
                    )?;
                    is_changed = is_changed || is_changed_new;
                }
                Distribution::UnspecifiedDistribution => {}
            };
            Ok((new_child, is_changed))
        },
    )
    .collect::<Result<Vec<_>>>()?;
    let (new_children, changed_flags): (Vec<_>, Vec<_>) =
        new_children_with_flags.into_iter().unzip();

    // special case for UnionExec: We want to "bubble up" hash-partitioned data. So instead of:
    //
    // Agg:
    //   Repartition (hash):
    //     Union:
    //       - Agg:
    //           Repartition (hash):
    //             Data
    //       - Agg:
    //           Repartition (hash):
    //             Data
    //
    // We can use:
    //
    // Agg:
    //   Interleave:
    //     - Agg:
    //         Repartition (hash):
    //           Data
    //     - Agg:
    //         Repartition (hash):
    //           Data
    if plan.as_any().is::<UnionExec>() {
        if can_interleave(&new_children) {
            let plan = Arc::new(InterleaveExec::try_new(new_children)?) as _;
            let new_repartition_context = RepartitionContext {
                plan,
                repartition_onwards: updated_repartition_onwards,
            };
            return Ok(Transformed::Yes(new_repartition_context));
        }
    }

    if is_updated || changed_flags.iter().any(|item| *item) {
        let new_plan = plan.clone().with_new_children(new_children)?;
        let new_repartition_context = RepartitionContext {
            plan: new_plan,
            repartition_onwards: updated_repartition_onwards,
        };
        Ok(Transformed::Yes(new_repartition_context))
    } else {
        Ok(Transformed::No(repartition_context))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RepartitionContext {
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    repartition_onwards: Vec<Option<ExecTree>>,
}

impl RepartitionContext {
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        let length = plan.children().len();
        RepartitionContext {
            plan,
            repartition_onwards: vec![None; length],
        }
    }

    pub fn new_from_children_nodes(
        children_nodes: Vec<RepartitionContext>,
        parent_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let children_plans = children_nodes
            .iter()
            .map(|item| item.plan.clone())
            .collect();
        let repartition_onwards = children_nodes
            .into_iter()
            .enumerate()
            .map(|(idx, item)| {
                // `ordering_onwards` tree keeps track of executors that maintain
                // ordering, (or that can maintain ordering with the replacement of
                // its variant)
                let plan = item.plan;
                let repartition_onwards = item.repartition_onwards;
                if plan.children().is_empty() {
                    // Plan has no children, there is nothing to propagate.
                    None
                } else if is_repartition(&plan) && repartition_onwards[0].is_none() {
                    Some(ExecTree::new(plan, idx, vec![]))
                } else {
                    let mut new_repartition_onwards = vec![];
                    for (required_dist, repartition_onwards, would_benefit) in izip!(
                        plan.required_input_distribution().iter(),
                        repartition_onwards.into_iter(),
                        plan.benefits_from_input_partitioning().into_iter()
                    ) {
                        if let Some(repartition_onwards) = repartition_onwards {
                            if let Distribution::UnspecifiedDistribution = required_dist {
                                if would_benefit {
                                    new_repartition_onwards.push(repartition_onwards);
                                }
                            }
                        }
                    }
                    if new_repartition_onwards.is_empty() {
                        None
                    } else {
                        Some(ExecTree::new(plan, idx, new_repartition_onwards))
                    }
                }
            })
            .collect();
        let plan = with_new_children_if_necessary(parent_plan, children_plans)?.into();
        Ok(RepartitionContext {
            plan,
            repartition_onwards,
        })
    }

    /// Computes order-preservation contexts for every child of the plan.
    pub fn children(&self) -> Vec<RepartitionContext> {
        self.plan
            .children()
            .into_iter()
            .map(|child| RepartitionContext::new(child))
            .collect()
    }
}

impl TreeNode for RepartitionContext {
    fn apply_children<F>(&self, op: &mut F) -> Result<VisitRecursion>
    where
        F: FnMut(&Self) -> Result<VisitRecursion>,
    {
        for child in self.children() {
            match op(&child)? {
                VisitRecursion::Continue => {}
                VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
                VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
            }
        }
        Ok(VisitRecursion::Continue)
    }

    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let children = self.children();
        if children.is_empty() {
            Ok(self)
        } else {
            let children_nodes = children
                .into_iter()
                .map(transform)
                .collect::<Result<Vec<_>>>()?;
            RepartitionContext::new_from_children_nodes(children_nodes, self.plan)
        }
    }
}

#[cfg(test)]
#[ctor::ctor]
fn init() {
    let _ = env_logger::try_init();
}

#[cfg(test)]
mod tests {
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

    use super::*;
    use crate::datasource::file_format::file_type::FileCompressionType;
    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::datasource::physical_plan::{FileScanConfig, ParquetExec};
    use crate::physical_optimizer::dist_enforcement::EnforceDistribution;
    use crate::physical_optimizer::sort_enforcement::EnforceSorting;
    use crate::physical_plan::aggregates::{
        AggregateExec, AggregateMode, PhysicalGroupBy,
    };
    use crate::physical_plan::expressions::{col, PhysicalSortExpr};
    use crate::physical_plan::filter::FilterExec;
    use crate::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
    use crate::physical_plan::projection::ProjectionExec;
    use crate::physical_plan::sorts::sort::SortExec;
    use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
    use crate::physical_plan::union::UnionExec;
    use crate::physical_plan::{displayable, DisplayAs, DisplayFormatType, Statistics};
    use datafusion_physical_expr::{LexOrderingReq, PhysicalSortRequirement};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("c1", DataType::Boolean, true)]))
    }

    /// Generate FileScanConfig for file scan executors like 'ParquetExec'
    fn scan_config(sorted: bool, single_file: bool) -> FileScanConfig {
        let sort_exprs = vec![PhysicalSortExpr {
            expr: col("c1", &schema()).unwrap(),
            options: SortOptions::default(),
        }];

        let file_groups = if single_file {
            vec![vec![PartitionedFile::new("x".to_string(), 100)]]
        } else {
            vec![
                vec![PartitionedFile::new("x".to_string(), 100)],
                vec![PartitionedFile::new("y".to_string(), 200)],
            ]
        };

        FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
            file_schema: schema(),
            file_groups,
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: if sorted { vec![sort_exprs] } else { vec![] },
            infinite_source: false,
        }
    }

    /// Create a non sorted parquet exec
    fn parquet_exec() -> Arc<ParquetExec> {
        Arc::new(ParquetExec::new(scan_config(false, true), None, None))
    }

    /// Create a non sorted CSV exec
    fn csv_exec() -> Arc<CsvExec> {
        Arc::new(CsvExec::new(
            scan_config(false, true),
            false,
            b',',
            b'"',
            None,
            FileCompressionType::UNCOMPRESSED,
        ))
    }

    /// Create a non sorted parquet exec over two files / partitions
    fn parquet_exec_two_partitions() -> Arc<ParquetExec> {
        Arc::new(ParquetExec::new(scan_config(false, false), None, None))
    }

    /// Create a non sorted csv exec over two files / partitions
    fn csv_exec_two_partitions() -> Arc<CsvExec> {
        Arc::new(CsvExec::new(
            scan_config(false, false),
            false,
            b',',
            b'"',
            None,
            FileCompressionType::UNCOMPRESSED,
        ))
    }

    // Created a sorted parquet exec
    fn parquet_exec_sorted() -> Arc<ParquetExec> {
        Arc::new(ParquetExec::new(scan_config(true, true), None, None))
    }

    // Created a sorted csv exec
    fn csv_exec_sorted() -> Arc<CsvExec> {
        Arc::new(CsvExec::new(
            scan_config(true, true),
            false,
            b',',
            b'"',
            None,
            FileCompressionType::UNCOMPRESSED,
        ))
    }

    // Created a sorted parquet exec with multiple files
    fn parquet_exec_multiple_sorted() -> Arc<ParquetExec> {
        Arc::new(ParquetExec::new(scan_config(true, false), None, None))
    }

    fn sort_preserving_merge_exec(
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        let expr = vec![PhysicalSortExpr {
            expr: col("c1", &schema()).unwrap(),
            options: arrow::compute::SortOptions::default(),
        }];

        Arc::new(SortPreservingMergeExec::new(expr, input))
    }

    fn filter_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(FilterExec::try_new(col("c1", &schema()).unwrap(), input).unwrap())
    }

    fn sort_exec(
        input: Arc<dyn ExecutionPlan>,
        preserve_partitioning: bool,
    ) -> Arc<dyn ExecutionPlan> {
        let sort_exprs = vec![PhysicalSortExpr {
            expr: col("c1", &schema()).unwrap(),
            options: SortOptions::default(),
        }];
        let new_sort = SortExec::new(sort_exprs, input)
            .with_preserve_partitioning(preserve_partitioning);
        Arc::new(new_sort)
    }

    fn projection_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let exprs = vec![(col("c1", &schema()).unwrap(), "c1".to_string())];
        Arc::new(ProjectionExec::try_new(exprs, input).unwrap())
    }

    fn aggregate(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let schema = schema();
        Arc::new(
            AggregateExec::try_new(
                AggregateMode::Final,
                PhysicalGroupBy::default(),
                vec![],
                vec![],
                vec![],
                Arc::new(
                    AggregateExec::try_new(
                        AggregateMode::Partial,
                        PhysicalGroupBy::default(),
                        vec![],
                        vec![],
                        vec![],
                        input,
                        schema.clone(),
                    )
                    .unwrap(),
                ),
                schema,
            )
            .unwrap(),
        )
    }

    fn limit_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(GlobalLimitExec::new(
            Arc::new(LocalLimitExec::new(input, 100)),
            0,
            Some(100),
        ))
    }

    fn limit_exec_with_skip(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(GlobalLimitExec::new(
            Arc::new(LocalLimitExec::new(input, 100)),
            5,
            Some(100),
        ))
    }

    fn union_exec(input: Vec<Arc<dyn ExecutionPlan>>) -> Arc<dyn ExecutionPlan> {
        Arc::new(UnionExec::new(input))
    }

    fn sort_required_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(SortRequiredExec::new(input))
    }

    fn trim_plan_display(plan: &str) -> Vec<&str> {
        plan.split('\n')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect()
    }

    /// Runs the repartition optimizer and asserts the plan against the expected
    macro_rules! assert_optimized {
        ($EXPECTED_LINES: expr, $PLAN: expr) => {
            assert_optimized!($EXPECTED_LINES, $PLAN, 10, false, 1024);
        };

        ($EXPECTED_LINES: expr, $PLAN: expr, $TARGET_PARTITIONS: expr, $REPARTITION_FILE_SCANS: expr, $REPARTITION_FILE_MIN_SIZE: expr) => {
            let expected_lines: Vec<&str> = $EXPECTED_LINES.iter().map(|s| *s).collect();

            let mut config = ConfigOptions::new();
            config.execution.target_partitions = $TARGET_PARTITIONS;
            config.optimizer.repartition_file_scans = $REPARTITION_FILE_SCANS;
            config.optimizer.repartition_file_min_size = $REPARTITION_FILE_MIN_SIZE;

            // run optimizer
            let optimizers: Vec<Arc<dyn PhysicalOptimizerRule + Sync + Send>> = vec![
                // Run enforce distribution rule
                Arc::new(EnforceDistributionV2::new()),
                // EnforceSorting is an essential rule to be applied.
                // Otherwise, the correctness of the generated optimized plan cannot be guaranteed
                Arc::new(EnforceSorting::new()),
            ];
            let optimized = optimizers.into_iter().fold($PLAN, |plan, optimizer| {
                optimizer.optimize(plan, &config).unwrap()
            });

            // Now format correctly
            let plan = displayable(optimized.as_ref()).indent(true).to_string();
            let actual_lines = trim_plan_display(&plan);

            assert_eq!(
                &expected_lines, &actual_lines,
                "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
                expected_lines, actual_lines
            );
        };
    }

    #[test]
    fn added_repartition_to_single_partition() -> Result<()> {
        let plan = aggregate(parquet_exec());

        let expected = [
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_deepest_node() -> Result<()> {
        let plan = aggregate(filter_exec(parquet_exec()));

        let expected = &[
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            "FilterExec: c1@0",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_unsorted_limit() -> Result<()> {
        let plan = limit_exec(filter_exec(parquet_exec()));

        let expected = &[
            "GlobalLimitExec: skip=0, fetch=100",
            "CoalescePartitionsExec",
            "LocalLimitExec: fetch=100",
            "FilterExec: c1@0",
            // nothing sorts the data, so the local limit doesn't require sorted data either
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_unsorted_limit_with_skip() -> Result<()> {
        let plan = limit_exec_with_skip(filter_exec(parquet_exec()));

        let expected = &[
            "GlobalLimitExec: skip=5, fetch=100",
            "CoalescePartitionsExec",
            "LocalLimitExec: fetch=100",
            "FilterExec: c1@0",
            // nothing sorts the data, so the local limit doesn't require sorted data either
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_sorted_limit() -> Result<()> {
        let plan = limit_exec(sort_exec(parquet_exec(), false));

        let expected = &[
            "GlobalLimitExec: skip=0, fetch=100",
            "LocalLimitExec: fetch=100",
            // data is sorted so can't repartition here
            "SortExec: expr=[c1@0 ASC]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_sorted_limit_with_filter() -> Result<()> {
        let plan = sort_required_exec(filter_exec(sort_exec(parquet_exec(), false)));

        let expected = &[
            "SortRequiredExec",
            "FilterExec: c1@0",
            // Do not add repartition, even though filter benefits from it
            // because subsequent executor requires this ordering.
            "SortExec: expr=[c1@0 ASC]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_ignores_limit() -> Result<()> {
        let plan = aggregate(limit_exec(filter_exec(limit_exec(parquet_exec()))));

        let expected = &[
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "GlobalLimitExec: skip=0, fetch=100",
            "CoalescePartitionsExec",
            "LocalLimitExec: fetch=100",
            "FilterExec: c1@0",
            // repartition should happen prior to the filter to maximize parallelism
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "GlobalLimitExec: skip=0, fetch=100",
            "LocalLimitExec: fetch=100",
            // Expect no repartition to happen for local limit
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_ignores_limit_with_skip() -> Result<()> {
        let plan = aggregate(limit_exec_with_skip(filter_exec(limit_exec(
            parquet_exec(),
        ))));

        let expected = &[
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "GlobalLimitExec: skip=5, fetch=100",
            "CoalescePartitionsExec",
            "LocalLimitExec: fetch=100",
            "FilterExec: c1@0",
            // repartition should happen prior to the filter to maximize parallelism
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "GlobalLimitExec: skip=0, fetch=100",
            "LocalLimitExec: fetch=100",
            // Expect no repartition to happen for local limit
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    // repartition works differently for limit when there is a sort below it

    #[test]
    fn repartition_ignores_union() -> Result<()> {
        let plan = union_exec(vec![parquet_exec(); 5]);

        let expected = &[
            "UnionExec",
            // Expect no repartition of ParquetExec
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_through_sort_preserving_merge() -> Result<()> {
        // sort preserving merge with non-sorted input
        let plan = sort_preserving_merge_exec(parquet_exec());

        // need repartiton and resort as the data was not sorted correctly
        let expected = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            "SortExec: expr=[c1@0 ASC]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_ignores_sort_preserving_merge() -> Result<()> {
        // sort preserving merge already sorted input,
        let plan = sort_preserving_merge_exec(parquet_exec_multiple_sorted());

        // should not repartition / sort (as the data was already sorted)
        let expected = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            "ParquetExec: file_groups={2 groups: [[x], [y]]}, projection=[c1], output_ordering=[c1@0 ASC]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_ignores_sort_preserving_merge_with_union() -> Result<()> {
        // 2 sorted parquet files unioned (partitions are concatenated, sort is preserved)
        let input = union_exec(vec![parquet_exec_sorted(); 2]);
        let plan = sort_preserving_merge_exec(input);

        // should not repartition / sort (as the data was already sorted)
        let expected = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            "UnionExec",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_does_not_destroy_sort() -> Result<()> {
        //  SortRequired
        //    Parquet(sorted)

        let plan = sort_required_exec(parquet_exec_sorted());

        // should not repartition as doing so destroys the necessary sort order
        let expected = &[
            "SortRequiredExec",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_does_not_destroy_sort_more_complex() -> Result<()> {
        // model a more complicated scenario where one child of a union can be repartitioned for performance
        // but the other can not be
        //
        // Union
        //  SortRequired
        //    Parquet(sorted)
        //  Filter
        //    Parquet(unsorted)

        let input1 = sort_required_exec(parquet_exec_sorted());
        let input2 = filter_exec(parquet_exec());
        let plan = union_exec(vec![input1, input2]);

        // should not repartition below the SortRequired as that
        // destroys the sort order but should still repartition for
        // FilterExec
        let expected = &[
            "UnionExec",
            // union input 1: no repartitioning
            "SortRequiredExec",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC]",
            // union input 2: should repartition
            "FilterExec: c1@0",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_transitively_with_projection() -> Result<()> {
        // non sorted input
        let plan = sort_preserving_merge_exec(projection_exec(parquet_exec()));

        // needs to repartition / sort as the data was not sorted correctly
        let expected = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            "SortExec: expr=[c1@0 ASC]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ProjectionExec: expr=[c1@0 as c1]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_ignores_transitively_with_projection() -> Result<()> {
        // sorted input
        let plan =
            sort_preserving_merge_exec(projection_exec(parquet_exec_multiple_sorted()));

        // data should not be repartitioned / resorted
        let expected = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            "ProjectionExec: expr=[c1@0 as c1]",
            "ParquetExec: file_groups={2 groups: [[x], [y]]}, projection=[c1], output_ordering=[c1@0 ASC]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_transitively_past_sort_with_projection() -> Result<()> {
        let plan =
            sort_preserving_merge_exec(sort_exec(projection_exec(parquet_exec()), true));

        let expected = &[
            "SortExec: expr=[c1@0 ASC]",
            "ProjectionExec: expr=[c1@0 as c1]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_transitively_past_sort_with_filter() -> Result<()> {
        let plan =
            sort_preserving_merge_exec(sort_exec(filter_exec(parquet_exec()), true));

        let expected = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            // Expect repartition on the input to the sort (as it can benefit from additional parallelism)
            "SortExec: expr=[c1@0 ASC]",
            "FilterExec: c1@0",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_transitively_past_sort_with_projection_and_filter() -> Result<()> {
        let plan = sort_preserving_merge_exec(sort_exec(
            projection_exec(filter_exec(parquet_exec())),
            true,
        ));

        let expected = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            // Expect repartition on the input to the sort (as it can benefit from additional parallelism)
            "SortExec: expr=[c1@0 ASC]",
            "ProjectionExec: expr=[c1@0 as c1]",
            "FilterExec: c1@0",
            // repartition is lowest down
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn parallelization_single_partition() -> Result<()> {
        let plan_parquet = aggregate(parquet_exec());
        let plan_csv = aggregate(csv_exec());

        let expected_parquet = [
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            "ParquetExec: file_groups={2 groups: [[x:0..50], [x:50..100]]}, projection=[c1]",
        ];
        let expected_csv = [
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            "CsvExec: file_groups={2 groups: [[x:0..50], [x:50..100]]}, projection=[c1], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, 2, true, 10);
        assert_optimized!(expected_csv, plan_csv, 2, true, 10);
        Ok(())
    }

    #[test]
    /// CsvExec on compressed csv file will not be partitioned
    /// (Not able to decompress chunked csv file)
    fn parallelization_compressed_csv() -> Result<()> {
        let compression_types = [
            FileCompressionType::GZIP,
            FileCompressionType::BZIP2,
            FileCompressionType::XZ,
            FileCompressionType::ZSTD,
            FileCompressionType::UNCOMPRESSED,
        ];

        let expected_not_partitioned = [
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], has_header=false",
        ];

        let expected_partitioned = [
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            "CsvExec: file_groups={2 groups: [[x:0..50], [x:50..100]]}, projection=[c1], has_header=false",
        ];

        for compression_type in compression_types {
            let expected = if compression_type.is_compressed() {
                &expected_not_partitioned[..]
            } else {
                &expected_partitioned[..]
            };

            let plan = aggregate(Arc::new(CsvExec::new(
                scan_config(false, true),
                false,
                b',',
                b'"',
                None,
                compression_type,
            )));

            assert_optimized!(expected, plan, 2, true, 10);
        }
        Ok(())
    }

    #[test]
    fn parallelization_two_partitions() -> Result<()> {
        let plan_parquet = aggregate(parquet_exec_two_partitions());
        let plan_csv = aggregate(csv_exec_two_partitions());

        let expected_parquet = [
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            // Plan already has two partitions
            "ParquetExec: file_groups={2 groups: [[x], [y]]}, projection=[c1]",
        ];
        let expected_csv = [
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            // Plan already has two partitions
            "CsvExec: file_groups={2 groups: [[x], [y]]}, projection=[c1], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, 2, true, 10);
        assert_optimized!(expected_csv, plan_csv, 2, true, 10);
        Ok(())
    }

    #[test]
    fn parallelization_two_partitions_into_four() -> Result<()> {
        let plan_parquet = aggregate(parquet_exec_two_partitions());
        let plan_csv = aggregate(csv_exec_two_partitions());

        let expected_parquet = [
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            // Multiple source files splitted across partitions
            "ParquetExec: file_groups={4 groups: [[x:0..75], [x:75..100, y:0..50], [y:50..125], [y:125..200]]}, projection=[c1]",
        ];
        let expected_csv = [
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            // Multiple source files splitted across partitions
            "CsvExec: file_groups={4 groups: [[x:0..75], [x:75..100, y:0..50], [y:50..125], [y:125..200]]}, projection=[c1], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, 4, true, 10);
        assert_optimized!(expected_csv, plan_csv, 4, true, 10);
        Ok(())
    }

    #[test]
    fn parallelization_sorted_limit() -> Result<()> {
        let plan_parquet = limit_exec(sort_exec(parquet_exec(), false));
        let plan_csv = limit_exec(sort_exec(csv_exec(), false));

        let expected_parquet = &[
            "GlobalLimitExec: skip=0, fetch=100",
            "LocalLimitExec: fetch=100",
            // data is sorted so can't repartition here
            "SortExec: expr=[c1@0 ASC]",
            // Doesn't parallelize for SortExec without preserve_partitioning
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];
        let expected_csv = &[
            "GlobalLimitExec: skip=0, fetch=100",
            "LocalLimitExec: fetch=100",
            // data is sorted so can't repartition here
            "SortExec: expr=[c1@0 ASC]",
            // Doesn't parallelize for SortExec without preserve_partitioning
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, 2, true, 10);
        assert_optimized!(expected_csv, plan_csv, 2, true, 10);
        Ok(())
    }

    #[test]
    fn parallelization_limit_with_filter_no_required_ordering() -> Result<()> {
        let plan_parquet = limit_exec(filter_exec(sort_exec(parquet_exec(), false)));
        let plan_csv = limit_exec(filter_exec(sort_exec(csv_exec(), false)));

        let expected_parquet = &[
            "GlobalLimitExec: skip=0, fetch=100",
            "CoalescePartitionsExec",
            "LocalLimitExec: fetch=100",
            "FilterExec: c1@0",
            // even though data is sorted, we can use repartition here. Since
            // ordering is not used in subsequent stages anyway.
            "RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
            "SortExec: expr=[c1@0 ASC]",
            // SortExec doesn't benefit from input partitioning
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];
        let expected_csv = &[
            "GlobalLimitExec: skip=0, fetch=100",
            "CoalescePartitionsExec",
            "LocalLimitExec: fetch=100",
            "FilterExec: c1@0",
            // even though data is sorted, we can use repartition here. Since
            // ordering is not used in subsequent stages anyway.
            "RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
            "SortExec: expr=[c1@0 ASC]",
            // SortExec doesn't benefit from input partitioning
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, 2, true, 10);
        assert_optimized!(expected_csv, plan_csv, 2, true, 10);
        Ok(())
    }

    #[test]
    fn parallelization_ignores_limit() -> Result<()> {
        let plan_parquet = aggregate(limit_exec(filter_exec(limit_exec(parquet_exec()))));
        let plan_csv = aggregate(limit_exec(filter_exec(limit_exec(csv_exec()))));

        let expected_parquet = &[
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
            "GlobalLimitExec: skip=0, fetch=100",
            "CoalescePartitionsExec",
            "LocalLimitExec: fetch=100",
            "FilterExec: c1@0",
            // repartition should happen prior to the filter to maximize parallelism
            "RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
            "GlobalLimitExec: skip=0, fetch=100",
            // Limit doesn't benefit from input partitionins - no parallelism
            "LocalLimitExec: fetch=100",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];
        let expected_csv = &[
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
            "GlobalLimitExec: skip=0, fetch=100",
            "CoalescePartitionsExec",
            "LocalLimitExec: fetch=100",
            "FilterExec: c1@0",
            // repartition should happen prior to the filter to maximize parallelism
            "RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
            "GlobalLimitExec: skip=0, fetch=100",
            // Limit doesn't benefit from input partitionins - no parallelism
            "LocalLimitExec: fetch=100",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, 2, true, 10);
        assert_optimized!(expected_csv, plan_csv, 2, true, 10);
        Ok(())
    }

    #[test]
    fn parallelization_union_inputs() -> Result<()> {
        let plan_parquet = union_exec(vec![parquet_exec(); 5]);
        let plan_csv = union_exec(vec![csv_exec(); 5]);

        let expected_parquet = &[
            "UnionExec",
            // Union doesn't benefit from input partitioning - no parallelism
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];
        let expected_csv = &[
            "UnionExec",
            // Union doesn't benefit from input partitioning - no parallelism
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], has_header=false",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], has_header=false",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], has_header=false",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], has_header=false",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, 2, true, 10);
        assert_optimized!(expected_csv, plan_csv, 2, true, 10);
        Ok(())
    }

    #[test]
    fn parallelization_prior_to_sort_preserving_merge() -> Result<()> {
        // sort preserving merge already sorted input,
        let plan_parquet = sort_preserving_merge_exec(parquet_exec_sorted());
        let plan_csv = sort_preserving_merge_exec(csv_exec_sorted());

        // parallelization potentially could break sort order
        let expected_parquet = &[
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC]",
        ];
        let expected_csv = &[
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, 2, true, 10);
        assert_optimized!(expected_csv, plan_csv, 2, true, 10);
        Ok(())
    }

    #[test]
    fn parallelization_sort_preserving_merge_with_union() -> Result<()> {
        // 2 sorted parquet files unioned (partitions are concatenated, sort is preserved)
        let input_parquet = union_exec(vec![parquet_exec_sorted(); 2]);
        let input_csv = union_exec(vec![csv_exec_sorted(); 2]);
        let plan_parquet = sort_preserving_merge_exec(input_parquet);
        let plan_csv = sort_preserving_merge_exec(input_csv);

        // should not repartition / sort (as the data was already sorted)
        let expected_parquet = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            "UnionExec",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC]",
        ];
        let expected_csv = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            "UnionExec",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC], has_header=false",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, 2, true, 10);
        assert_optimized!(expected_csv, plan_csv, 2, true, 10);
        Ok(())
    }

    #[test]
    fn parallelization_does_not_destroy_sort() -> Result<()> {
        //  SortRequired
        //    Parquet(sorted)
        let plan_parquet = sort_required_exec(parquet_exec_sorted());
        let plan_csv = sort_required_exec(csv_exec_sorted());

        // no parallelization to preserve sort order
        let expected_parquet = &[
            "SortRequiredExec",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC]",
        ];
        let expected_csv = &[
            "SortRequiredExec",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, 2, true, 10);
        assert_optimized!(expected_csv, plan_csv, 2, true, 10);
        Ok(())
    }

    #[test]
    fn parallelization_ignores_transitively_with_projection() -> Result<()> {
        // sorted input
        let plan_parquet =
            sort_preserving_merge_exec(projection_exec(parquet_exec_sorted()));
        let plan_csv = sort_preserving_merge_exec(projection_exec(csv_exec_sorted()));

        // data should not be repartitioned / resorted
        let expected_parquet = &[
            "ProjectionExec: expr=[c1@0 as c1]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC]",
        ];
        let expected_csv = &[
            "ProjectionExec: expr=[c1@0 as c1]",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, 2, true, 10);
        assert_optimized!(expected_csv, plan_csv, 2, true, 10);
        Ok(())
    }

    /// Models operators like BoundedWindowExec that require an input
    /// ordering but is easy to construct
    #[derive(Debug)]
    struct SortRequiredExec {
        input: Arc<dyn ExecutionPlan>,
        sort_req: LexOrderingReq,
    }

    impl SortRequiredExec {
        fn new(input: Arc<dyn ExecutionPlan>) -> Self {
            let sort_exprs = vec![PhysicalSortExpr {
                expr: col("c1", &schema()).unwrap(),
                options: SortOptions::default(),
            }];
            let sort_req = PhysicalSortRequirement::from_sort_exprs(&sort_exprs);
            Self { input, sort_req }
        }
    }

    impl DisplayAs for SortRequiredExec {
        fn fmt_as(
            &self,
            _t: DisplayFormatType,
            f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            write!(f, "SortRequiredExec")
        }
    }

    impl ExecutionPlan for SortRequiredExec {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.input.schema()
        }

        fn output_partitioning(&self) -> crate::physical_plan::Partitioning {
            self.input.output_partitioning()
        }

        fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
            self.input.output_ordering()
        }

        fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
            vec![self.input.clone()]
        }

        // model that it requires the output ordering of its input
        fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
            vec![Some(self.sort_req.clone())]
        }

        fn with_new_children(
            self: Arc<Self>,
            mut children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            assert_eq!(children.len(), 1);
            let child = children.pop().unwrap();
            Ok(Arc::new(Self::new(child)))
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<crate::execution::context::TaskContext>,
        ) -> Result<crate::physical_plan::SendableRecordBatchStream> {
            unreachable!();
        }

        fn statistics(&self) -> Statistics {
            self.input.statistics()
        }
    }
}
