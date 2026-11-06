// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`Optimizer`] and [`OptimizerRule`]

use std::fmt::Debug;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{InvariantLevel, assert_expected_schema};
use log::{debug, warn};

use datafusion_common::alias::AliasGenerator;
use datafusion_common::config::ConfigOptions;
use datafusion_common::instant::Instant;
use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
};
use datafusion_common::{
    ColumnIdGenerator, DFSchema, DataFusionError, HashSet, Result, TableReference,
    assert_or_internal_err, internal_err,
};
use datafusion_expr::dml::CopyTo;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::{
    Aggregate, Analyze, CreateMemoryTable, CreateView, DdlStatement, Distinct,
    DistinctOn, DmlStatement, Explain, Expr, Extension, Filter, Join, Limit,
    MaterializedCte, MaterializedCtePlan, NamedExpr, Projection, RecursiveQuery,
    Repartition, Sort, Statement, Subquery, SubqueryAlias, Union, Unnest, Window,
};

use crate::column_id_check::ColumnIdCheck;
use crate::common_subexpr_eliminate::CommonSubexprEliminate;
use crate::decorrelate_lateral_join::DecorrelateLateralJoin;
use crate::decorrelate_predicate_subquery::DecorrelatePredicateSubquery;
use crate::eliminate_aggregate_distinct::EliminateAggregateDistinct;
use crate::eliminate_cross_join::EliminateCrossJoin;
use crate::eliminate_duplicated_expr::EliminateDuplicatedExpr;
use crate::eliminate_filter::EliminateFilter;
use crate::eliminate_group_by_constant::EliminateGroupByConstant;
use crate::eliminate_join::EliminateJoin;
use crate::eliminate_limit::EliminateLimit;
use crate::eliminate_outer_join::EliminateOuterJoin;
use crate::extract_equijoin_predicate::ExtractEquijoinPredicate;
use crate::extract_leaf_expressions::{ExtractLeafExpressions, PushDownLeafProjections};
use crate::filter_null_join_keys::FilterNullJoinKeys;
use crate::optimize_projections::OptimizeProjections;
use crate::optimize_unions::OptimizeUnions;
use crate::plan_signature::LogicalPlanSignature;
use crate::propagate_empty_relation::PropagateEmptyRelation;
use crate::push_down_filter::PushDownFilter;
use crate::push_down_limit::PushDownLimit;
use crate::replace_distinct_aggregate::ReplaceDistinctWithAggregate;
use crate::replace_filter_top1::ReplaceFilterTop1;
use crate::rewrite_set_comparison::RewriteSetComparison;
use crate::scalar_subquery_to_join::ScalarSubqueryToJoin;
use crate::simplify_expressions::SimplifyExpressions;
use crate::single_distinct_to_groupby::SingleDistinctToGroupBy;
use crate::unions_to_filter::UnionsToFilter;
use crate::utils::log_plan;

/// Transforms one [`LogicalPlan`] into another which computes the same results,
/// but in a potentially more efficient way.
///
/// See notes on [`Self::rewrite`] for details on how to implement an `OptimizerRule`.
///
/// To change the semantics of a `LogicalPlan`, see [`AnalyzerRule`].
///
/// Use [`SessionState::add_optimizer_rule`] to register additional
/// `OptimizerRule`s.
///
/// [`AnalyzerRule`]: crate::analyzer::AnalyzerRule
/// [`SessionState::add_optimizer_rule`]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html#method.add_optimizer_rule
pub trait OptimizerRule: Debug {
    /// A human readable name for this optimizer rule
    fn name(&self) -> &str;

    /// How should the rule be applied by the optimizer? See comments on
    /// [`ApplyOrder`] for details.
    ///
    /// If returns `None`, the default, the rule must handle recursion itself
    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }

    /// Try to rewrite `plan` to an optimized form, returning [`Transformed::yes`]
    /// if the plan was rewritten and [`Transformed::no`] if it was not.
    ///
    /// # Notes for implementations:
    ///
    /// ## Return the same plan if no changes were made
    ///
    /// If there are no suitable transformations for the input plan,
    /// the optimizer should simply return it unmodified.
    ///
    /// The optimizer will call `rewrite` several times until a fixed point is
    /// reached, so it is important that `rewrite` return [`Transformed::no`] if
    /// the output is the same.
    ///
    /// ## Matching on functions
    ///
    /// The rule should avoid function-specific transformations, and instead use
    /// methods on [`ScalarUDFImpl`] and [`AggregateUDFImpl`]. Specifically, the
    /// rule should not check function names as functions can be overridden, and
    /// may not have the same semantics as the functions provided with
    /// DataFusion.
    ///
    /// For example, if a rule rewrites a function based on the check
    /// `func.name() == "sum"`, it may rewrite the plan incorrectly if the
    /// registered `sum` function has different semantics (for example, the
    /// `sum` function from the `datafusion-spark` crate).
    ///
    /// There are still several cases that rely on function name checking in
    /// the rules included with DataFusion. Please see [#18643] for more details
    /// and to help remove these cases.
    ///
    /// [`ScalarUDFImpl`]: datafusion_expr::ScalarUDFImpl
    /// [`AggregateUDFImpl`]: datafusion_expr::ScalarUDFImpl
    /// [#18643]: https://github.com/apache/datafusion/issues/18643
    fn rewrite(
        &self,
        _plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        internal_err!("rewrite is not implemented for {}", self.name())
    }
}

/// Options to control the DataFusion Optimizer.
pub trait OptimizerConfig {
    /// Return the time at which the query execution started. This
    /// time is used as the value for `now()`. If `None`, time-dependent
    /// functions like `now()` will not be simplified during optimization.
    fn query_execution_start_time(&self) -> Option<DateTime<Utc>>;

    /// Return alias generator used to generate unique aliases for subqueries
    fn alias_generator(&self) -> &Arc<AliasGenerator>;

    /// Return the generator of the [`ColumnId`]s that rules allocate (for
    /// example, the generator of the session that built the plan, or a new
    /// one, as [`OptimizerContext`] does by default).
    ///
    /// The optimizer moves it past the highest id of the plan before running
    /// the rules (see [`LogicalPlan::max_column_id`]), so that the ids it
    /// allocates do not collide with the ids of the plan.
    ///
    /// [`ColumnId`]: datafusion_common::ColumnId
    fn column_id_generator(&self) -> &Arc<ColumnIdGenerator>;

    fn options(&self) -> Arc<ConfigOptions>;

    fn function_registry(&self) -> Option<&dyn FunctionRegistry> {
        None
    }
}

/// A standalone [`OptimizerConfig`] that can be used independently
/// of DataFusion's config management
#[derive(Debug)]
pub struct OptimizerContext {
    /// Query execution start time that can be used to rewrite
    /// expressions such as `now()` to use a literal value instead.
    /// If `None`, time-dependent functions will not be simplified.
    query_execution_start_time: Option<DateTime<Utc>>,

    /// Alias generator used to generate unique aliases for subqueries
    alias_generator: Arc<AliasGenerator>,

    /// Generator of the column ids that rules allocate
    column_id_generator: Arc<ColumnIdGenerator>,

    options: Arc<ConfigOptions>,
}

impl OptimizerContext {
    /// Create optimizer config
    pub fn new() -> Self {
        let mut options = ConfigOptions::default();
        options.optimizer.filter_null_join_keys = true;

        Self::new_with_config_options(Arc::new(options))
    }

    /// Create a optimizer config with provided [ConfigOptions].
    pub fn new_with_config_options(options: Arc<ConfigOptions>) -> Self {
        Self {
            query_execution_start_time: Some(Utc::now()),
            alias_generator: Arc::new(AliasGenerator::new()),
            column_id_generator: Arc::new(ColumnIdGenerator::new()),
            options,
        }
    }

    /// Use the given column id generator, for example the one of the session
    /// that built the plan (by default, a new one)
    pub fn with_column_id_generator(
        mut self,
        column_id_generator: Arc<ColumnIdGenerator>,
    ) -> Self {
        self.column_id_generator = column_id_generator;
        self
    }

    /// Specify whether to enable the filter_null_keys rule
    pub fn filter_null_keys(mut self, filter_null_keys: bool) -> Self {
        Arc::make_mut(&mut self.options)
            .optimizer
            .filter_null_join_keys = filter_null_keys;
        self
    }

    /// Set the query execution start time
    pub fn with_query_execution_start_time(
        mut self,
        query_execution_start_time: DateTime<Utc>,
    ) -> Self {
        self.query_execution_start_time = Some(query_execution_start_time);
        self
    }

    /// Clear the query execution start time. When `None`, time-dependent
    /// functions like `now()` will not be simplified during optimization.
    pub fn without_query_execution_start_time(mut self) -> Self {
        self.query_execution_start_time = None;
        self
    }

    /// Specify whether the optimizer should skip rules that produce
    /// errors, or fail the query
    pub fn with_skip_failing_rules(mut self, b: bool) -> Self {
        Arc::make_mut(&mut self.options).optimizer.skip_failed_rules = b;
        self
    }

    /// Specify how many times to attempt to optimize the plan
    pub fn with_max_passes(mut self, v: u8) -> Self {
        Arc::make_mut(&mut self.options).optimizer.max_passes = v as usize;
        self
    }
}

impl Default for OptimizerContext {
    /// Create optimizer config
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizerConfig for OptimizerContext {
    fn query_execution_start_time(&self) -> Option<DateTime<Utc>> {
        self.query_execution_start_time
    }

    fn alias_generator(&self) -> &Arc<AliasGenerator> {
        &self.alias_generator
    }

    fn column_id_generator(&self) -> &Arc<ColumnIdGenerator> {
        &self.column_id_generator
    }

    fn options(&self) -> Arc<ConfigOptions> {
        Arc::clone(&self.options)
    }
}

/// A rule-based logical optimizer.
///
/// Applies rules in order for up to `datafusion.optimizer.max_passes` passes,
/// stopping early if the plan repeats.
#[derive(Clone, Debug)]
pub struct Optimizer {
    /// All optimizer rules to apply
    pub rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
}

/// Specifies how recursion for an `OptimizerRule` should be handled.
///
/// * `Some(apply_order)`: The Optimizer will recursively apply the rule to the plan.
/// * `None`: the rule must handle any required recursion itself.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ApplyOrder {
    /// Apply the rule to the node before its inputs
    TopDown,
    /// Apply the rule to the node after its inputs
    BottomUp,
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl Optimizer {
    /// Create a new optimizer using the recommended list of rules
    pub fn new() -> Self {
        // NOTEs:
        // - The order of rules in this list is important, as it determines the
        //   order in which they are applied.
        // - Adding a new rule here is expensive as it will be applied to all
        //   queries, and will likely increase the optimization time. Please extend
        //   existing rules when possible, rather than adding a new rule.
        //   If you do add a new rule considering having aggressive no-op paths
        //   (e.g. if the plan doesn't contain any of the nodes you are looking for
        //    return `Transformed::no`; only works if you control the traversal).
        let rules: Vec<Arc<dyn OptimizerRule + Sync + Send>> = vec![
            Arc::new(RewriteSetComparison::new()),
            Arc::new(OptimizeUnions::new()),
            Arc::new(UnionsToFilter::new()),
            Arc::new(SimplifyExpressions::new()),
            Arc::new(ReplaceDistinctWithAggregate::new()),
            Arc::new(EliminateJoin::new()),
            Arc::new(DecorrelatePredicateSubquery::new()),
            Arc::new(ScalarSubqueryToJoin::new()),
            Arc::new(DecorrelateLateralJoin::new()),
            Arc::new(ExtractEquijoinPredicate::new()),
            Arc::new(EliminateDuplicatedExpr::new()),
            Arc::new(EliminateFilter::new()),
            Arc::new(EliminateCrossJoin::new()),
            Arc::new(EliminateLimit::new()),
            Arc::new(PropagateEmptyRelation::new()),
            Arc::new(FilterNullJoinKeys::default()),
            Arc::new(EliminateOuterJoin::new()),
            // Filters can't be pushed down past Limits, we should do PushDownFilter after PushDownLimit
            Arc::new(PushDownLimit::new()),
            Arc::new(PushDownFilter::new()),
            Arc::new(EliminateAggregateDistinct::new()),
            Arc::new(SingleDistinctToGroupBy::new()),
            // The previous optimizations added expressions and projections,
            // that might benefit from the following rules
            Arc::new(EliminateGroupByConstant::new()),
            Arc::new(CommonSubexprEliminate::new()),
            Arc::new(ExtractLeafExpressions::new()),
            Arc::new(PushDownLeafProjections::new()),
            Arc::new(OptimizeProjections::new()),
            Arc::new(ReplaceFilterTop1::new()),
        ];

        Self::with_rules(rules)
    }

    /// Create a new optimizer with the given rules
    pub fn with_rules(rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>) -> Self {
        Self { rules }
    }
}

/// Recursively rewrites LogicalPlans
struct Rewriter<'a> {
    apply_order: ApplyOrder,
    rule: &'a dyn OptimizerRule,
    config: &'a dyn OptimizerConfig,
}

impl<'a> Rewriter<'a> {
    fn new(
        apply_order: ApplyOrder,
        rule: &'a dyn OptimizerRule,
        config: &'a dyn OptimizerConfig,
    ) -> Self {
        Self {
            apply_order,
            rule,
            config,
        }
    }
}

impl TreeNodeRewriter for Rewriter<'_> {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        if self.apply_order == ApplyOrder::TopDown {
            self.rule.rewrite(node, self.config)
        } else {
            Ok(Transformed::no(node))
        }
    }

    fn f_up(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        if self.apply_order == ApplyOrder::BottomUp {
            self.rule.rewrite(node, self.config)
        } else {
            Ok(Transformed::no(node))
        }
    }
}

/// Applies `f` to each child (input) of `plan` in place, using
/// [`Arc::make_mut`] for copy-on-write semantics on `Arc<LogicalPlan>`
/// children. When the `Arc` refcount is 1 (the common case here)
/// `Arc::make_mut` hands out a `&mut` without cloning; when it is >1 the
/// inner value is cloned first.
///
/// Returns `Ok(true)` if any child was modified by `f`.
///
/// This is deliberately private to the optimizer rather than a method on
/// [`LogicalPlan`]: it is an implementation detail of in-place rewriting, and
/// the `Arc::make_mut` approach does not generalize to the other tree types
/// (`Expr` children are `Box`ed; `PhysicalExpr`/`ExecutionPlan` children are
/// `Arc<dyn _>`, which `Arc::make_mut` cannot handle). If `TreeNode` ever
/// grows an in-place traversal this logic can move there.
///
/// # Error semantics
///
/// If `f` returns `Err` for a child, that error is returned immediately;
/// children visited earlier keep whatever modifications `f` already applied
/// to them — they are **not** rolled back.
fn map_children_mut<F: FnMut(&mut LogicalPlan) -> Result<bool>>(
    plan: &mut LogicalPlan,
    mut f: F,
) -> Result<bool> {
    Ok(match plan {
        LogicalPlan::Projection(Projection { input, .. })
        | LogicalPlan::Filter(Filter { input, .. })
        | LogicalPlan::Repartition(Repartition { input, .. })
        | LogicalPlan::Window(Window { input, .. })
        | LogicalPlan::Aggregate(Aggregate { input, .. })
        | LogicalPlan::Sort(Sort { input, .. })
        | LogicalPlan::Limit(Limit { input, .. })
        | LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. })
        | LogicalPlan::Analyze(Analyze { input, .. })
        | LogicalPlan::Dml(DmlStatement { input, .. })
        | LogicalPlan::Copy(CopyTo { input, .. })
        | LogicalPlan::Unnest(Unnest { input, .. }) => f(Arc::make_mut(input))?,
        LogicalPlan::Subquery(Subquery { subquery, .. }) => f(Arc::make_mut(subquery))?,
        LogicalPlan::Join(Join { left, right, .. }) => {
            let l = f(Arc::make_mut(left))?;
            let r = f(Arc::make_mut(right))?;
            l || r
        }
        LogicalPlan::AsOfJoin(join) => {
            let l = f(Arc::make_mut(&mut join.left))?;
            let r = f(Arc::make_mut(&mut join.right))?;
            l || r
        }
        LogicalPlan::Union(Union { inputs, .. }) => {
            let mut changed = false;
            for input in inputs {
                changed |= f(Arc::make_mut(input))?;
            }
            changed
        }
        LogicalPlan::Distinct(Distinct::All(input)) => f(Arc::make_mut(input))?,
        LogicalPlan::Distinct(Distinct::On(DistinctOn { input, .. })) => {
            f(Arc::make_mut(input))?
        }
        LogicalPlan::Explain(Explain { plan, .. }) => f(Arc::make_mut(plan))?,
        LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(CreateMemoryTable {
            input,
            ..
        }))
        | LogicalPlan::Ddl(DdlStatement::CreateView(CreateView { input, .. })) => {
            f(Arc::make_mut(input))?
        }
        LogicalPlan::RecursiveQuery(RecursiveQuery {
            static_term,
            recursive_term,
            ..
        }) => {
            let s = f(Arc::make_mut(static_term))?;
            let r = f(Arc::make_mut(recursive_term))?;
            s || r
        }
        LogicalPlan::MaterializedCte(MaterializedCtePlan::Cte(MaterializedCte {
            cte,
            continuation,
            ..
        })) => {
            let b = f(Arc::make_mut(cte))?;
            let c = f(Arc::make_mut(continuation))?;
            b || c
        }
        LogicalPlan::Statement(Statement::Prepare(p)) => f(Arc::make_mut(&mut p.input))?,
        LogicalPlan::Extension(Extension { node }) => {
            let inputs = node.inputs();
            if inputs.is_empty() {
                false
            } else {
                // Extension nodes don't expose mutable children,
                // fall back to the ownership-based API
                let mut changed = false;
                let exprs = node.expressions();
                let new_inputs: Vec<LogicalPlan> = inputs
                    .into_iter()
                    .map(|input| {
                        let mut plan = input.clone();
                        if f(&mut plan)? {
                            changed = true;
                        }
                        Ok(plan)
                    })
                    .collect::<Result<Vec<_>>>()?;
                *node = node.with_exprs_and_inputs(exprs, new_inputs)?;
                changed
            }
        }
        // plans without inputs
        LogicalPlan::TableScan { .. }
        | LogicalPlan::EmptyRelation { .. }
        | LogicalPlan::Values { .. }
        | LogicalPlan::MaterializedCte(MaterializedCtePlan::Scan(_))
        | LogicalPlan::Show(_)
        | LogicalPlan::Ddl(DdlStatement::CreateExternalTable(_))
        | LogicalPlan::Ddl(DdlStatement::CreateCatalogSchema(_))
        | LogicalPlan::Ddl(DdlStatement::CreateCatalog(_))
        | LogicalPlan::Ddl(DdlStatement::CreateExternalCatalog(_))
        | LogicalPlan::Ddl(DdlStatement::CreateIndex(_))
        | LogicalPlan::Ddl(DdlStatement::DropTable(_))
        | LogicalPlan::Ddl(DdlStatement::DropView(_))
        | LogicalPlan::Ddl(DdlStatement::DropCatalogSchema(_))
        | LogicalPlan::Ddl(DdlStatement::DropCatalog(_))
        | LogicalPlan::Ddl(DdlStatement::CreateFunction(_))
        | LogicalPlan::Ddl(DdlStatement::DropFunction(_))
        | LogicalPlan::Statement(_) => false,
    })
}

/// Rewrites a plan tree in place using `Arc::make_mut` for
/// copy-on-write semantics on `Arc<LogicalPlan>` children.
///
/// This avoids the `Arc::unwrap_or_clone` + `Arc::new` cycle that the
/// ownership-based `TreeNode::rewrite` performs at every child node.
///
/// # Error semantics
///
/// On `Err`, `*plan` is left in an **unspecified** state and must not be used.
/// Note this is different than consuming APIs such as [`TreeNode::rewrite`]
/// where the original plan is freed and no longer available on error
#[cfg_attr(feature = "recursive_protection", recursive::recursive)]
fn rewrite_plan_in_place(
    plan: &mut LogicalPlan,
    apply_order: ApplyOrder,
    rule: &dyn OptimizerRule,
    config: &dyn OptimizerConfig,
) -> Result<bool> {
    // f_down phase
    let mut changed = false;
    if apply_order == ApplyOrder::TopDown {
        // `rule.rewrite()` takes the plan by value, so bridge the `&mut` to an
        // owned value with `std::mem::take`. `LogicalPlan::default()` is a cheap
        // empty placeholder (shared empty schema, no allocation) and is
        // overwritten with the rule's output on the next line.
        let owned = std::mem::take(plan);
        let result = rule.rewrite(owned, config)?;
        *plan = result.data;
        changed |= result.transformed;
        // Respect TreeNodeRecursion::Stop/Jump from the rule
        if result.tnr == TreeNodeRecursion::Stop {
            return Ok(changed);
        }
    }

    let mut child_schema_changed = false;
    let children_changed = map_children_mut(plan, |child| {
        let old_schema = Arc::clone(child.schema());
        let child_changed = rewrite_plan_in_place(child, apply_order, rule, config)?;
        if child_changed && old_schema.as_ref() != child.schema().as_ref() {
            child_schema_changed = true;
        }
        Ok(child_changed)
    })?;
    changed |= children_changed;

    if child_schema_changed {
        // Child rewrites can change their output schemas. Recompute the current
        // node before later rules use positional requirements from that schema.
        let owned = std::mem::take(plan);
        *plan = owned.recompute_schema()?;
    }

    // f_up phase
    if apply_order == ApplyOrder::BottomUp {
        let owned = std::mem::take(plan);
        let result = rule.rewrite(owned, config)?;
        *plan = result.data;
        changed |= result.transformed;
    }

    Ok(changed)
}

/// Returns true if the plan contains any subquery expressions
/// (EXISTS, IN subquery, scalar subquery, set comparison).
///
/// Used to determine whether the more expensive `rewrite_with_subqueries`
/// traversal is needed. When the plan has no subqueries, the cheaper
/// `rewrite` traversal is sufficient since all plan nodes are reachable
/// via direct children.
fn plan_has_subqueries(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| {
        if found {
            return Ok(TreeNodeRecursion::Stop);
        }
        node.apply_expressions(|expr| {
            if found {
                return Ok(TreeNodeRecursion::Stop);
            }
            expr.apply(|e| {
                if matches!(
                    e,
                    Expr::Exists(_)
                        | Expr::InSubquery(_)
                        | Expr::SetComparison(_)
                        | Expr::ScalarSubquery(_)
                ) {
                    found = true;
                    Ok(TreeNodeRecursion::Stop)
                } else {
                    Ok(TreeNodeRecursion::Continue)
                }
            })
        })?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    found
}

impl Optimizer {
    /// Optimizes the logical plan by applying optimizer rules, and
    /// invoking observer function after each call
    pub fn optimize<F>(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
        mut observer: F,
    ) -> Result<LogicalPlan>
    where
        F: FnMut(&LogicalPlan, &dyn OptimizerRule),
    {
        // verify LP is valid, before the first LP optimizer pass.
        plan.check_invariants(InvariantLevel::Executable)
            .map_err(|e| e.context("Invalid input plan before LP Optimizers"))?;

        let start_time = Instant::now();
        let options = config.options();
        let mut new_plan = plan;

        // Ids that rules allocate must not collide with the ids of the plan,
        // which may have been built elsewhere (decoded, or with another
        // generator)
        if let Some(max_id) = new_plan.max_column_id() {
            config.column_id_generator().advance_past(max_id);
        }

        let mut column_id_check = ColumnIdCheck::new(&new_plan);

        let mut previous_plans = HashSet::with_capacity(16);
        previous_plans.insert(LogicalPlanSignature::new(&new_plan));

        let starting_schema = Arc::clone(new_plan.schema());
        // Rules may rename the outputs of the root (names come from the
        // definitions, and a rewrite renames what it rewrites); they are
        // restored once, at the end
        let output_names = RootNames::of(&new_plan);

        let mut i = 0;
        while i < options.optimizer.max_passes {
            log_plan(&format!("Optimizer input (pass {i})"), &new_plan);

            // Track subquery presence across the pass. Refresh after changed
            // rules so decorrelation can move later rules onto the in-place
            // path; that path refreshes parent schemas after child schemas
            // change.
            let mut has_subqueries = plan_has_subqueries(&new_plan);

            for rule in &self.rules {
                // If skipping failed rules, copy plan before attempting to rewrite
                // as rewriting is destructive
                let prev_plan = options
                    .optimizer
                    .skip_failed_rules
                    .then(|| new_plan.clone());

                let starting_schema = Arc::clone(new_plan.schema());

                let result = match rule.apply_order() {
                    // optimizer handles recursion
                    Some(apply_order) => {
                        if has_subqueries {
                            // Plans with subqueries need the full
                            // rewrite_with_subqueries traversal to
                            // recurse into subquery plans.
                            new_plan.rewrite_with_subqueries(
                                &mut Rewriter::new(
                                    apply_order,
                                    rule.as_ref(),
                                    config,
                                ),
                            )
                        } else {
                            // No subqueries: use in-place rewriting
                            // with Arc::make_mut for zero-cost CoW on
                            // children, avoiding Arc unwrap/rewrap.
                            //
                            // On error `new_plan` is left in an unspecified
                            // state (see `rewrite_plan_in_place`); the result
                            // handling below discards it, restoring `prev_plan`
                            // when `skip_failed_rules` is set or propagating
                            // the error otherwise.
                            rewrite_plan_in_place(
                                &mut new_plan,
                                apply_order,
                                rule.as_ref(),
                                config,
                            )
                            .map(|transformed| {
                                Transformed::new_transformed(
                                    std::mem::take(&mut new_plan),
                                    transformed,
                                )
                            })
                        }
                    }
                    // rule handles recursion itself
                    None => {
                        rule.rewrite(new_plan, config)
                    },
                }
                .and_then(|tnr| {
                    // run checks optimizer invariant checks, per optimizer rule
                    // applied (names are restored at the end)
                    assert_valid_optimization_up_to_names(&tnr.data, &starting_schema)
                        .map_err(|e| e.context(format!("Check optimizer-specific invariants after optimizer rule: {}", rule.name())))?;

                    // run LP invariant checks only in debug mode for performance reasons
                    #[cfg(debug_assertions)]
                    tnr.data.check_invariants(InvariantLevel::Executable)
                        .map_err(|e| e.context(format!("Invalid (non-executable) plan after Optimizer rule: {}", rule.name())))?;

                    Ok(tnr)
                });

                // Handle results
                match (result, prev_plan) {
                    // OptimizerRule was successful
                    (
                        Ok(Transformed {
                            data, transformed, ..
                        }),
                        _,
                    ) => {
                        new_plan = data;
                        if transformed {
                            has_subqueries = plan_has_subqueries(&new_plan);
                        }
                        observer(&new_plan, rule.as_ref());
                        if transformed {
                            // a rule that breaks column ids fails the query
                            // when verification is on, even if failed rules
                            // are skipped
                            column_id_check.after_rule(rule.name(), &new_plan)?;
                            log_plan(rule.name(), &new_plan);
                        } else {
                            debug!(
                                "Plan unchanged by optimizer rule '{}' (pass {})",
                                rule.name(),
                                i
                            );
                        }
                    }
                    // OptimizerRule was unsuccessful, but skipped failed rules is on
                    // so use the previous plan
                    (Err(e), Some(orig_plan)) => {
                        // Note to future readers: if you see this warning it signals a
                        // bug in the DataFusion optimizer. Please consider filing a ticket
                        // https://github.com/apache/datafusion
                        warn!(
                            "Skipping optimizer rule '{}' due to unexpected error: {}",
                            rule.name(),
                            e
                        );
                        new_plan = orig_plan;
                    }
                    // OptimizerRule was unsuccessful, but skipped failed rules is off, return error
                    (Err(e), None) => {
                        return Err(e.context(format!(
                            "Optimizer rule '{}' failed",
                            rule.name()
                        )));
                    }
                }
            }
            log_plan(&format!("Optimized plan (pass {i})"), &new_plan);

            // HashSet::insert returns, whether the value was newly inserted.
            let plan_is_fresh =
                previous_plans.insert(LogicalPlanSignature::new(&new_plan));
            if !plan_is_fresh {
                // plan did not change, so no need to continue trying to optimize
                debug!("optimizer pass {i} did not make changes");
                break;
            }
            i += 1;
        }

        // References read by id, and rules leave their labels out of date:
        // label each with the column it reads, so that the plan displays what
        // it reads (outputs named after an expression follow)
        let relabeled = new_plan
            .with_relabeled_columns()
            .map_err(|e| e.context("Relabeling the columns after all passes"))?;
        let mut new_plan = relabeled.data;
        if relabeled.transformed {
            column_id_check.after_rule("relabel columns", &new_plan)?;
        }

        // the root keeps the names it started with
        let restored = output_names
            .restore(new_plan, config.column_id_generator())
            .map_err(|e| e.context("Restoring the output names after all passes"))?;
        new_plan = restored.data;
        if restored.transformed {
            // the references to renamed outputs (the expressions of a MERGE
            // INTO read the input of the write) follow
            new_plan = new_plan.with_relabeled_columns()?.data;
            column_id_check.after_rule("restore output names", &new_plan)?;
        }

        // verify that the optimizer passes only mutated what was permitted.
        assert_valid_optimization(&new_plan, &starting_schema).map_err(|e| {
            e.context("Check optimizer-specific invariants after all passes")
        })?;

        // verify LP is valid, after the last optimizer pass.
        new_plan
            .check_invariants(InvariantLevel::Executable)
            .map_err(|e| {
                e.context("Invalid (non-executable) plan after LP Optimizers")
            })?;

        log_plan("Final optimized plan", &new_plan);
        debug!("Optimizer took {} ms", start_time.elapsed().as_millis());
        Ok(new_plan)
    }
}

/// These are invariants which should hold true before and after [`LogicalPlan`] optimization.
///
/// This differs from [`LogicalPlan::check_invariants`], which addresses if a singular
/// LogicalPlan is valid. Instead, this address if the optimization was valid based upon permitted changes.
fn assert_valid_optimization(
    plan: &LogicalPlan,
    prev_schema: &Arc<DFSchema>,
) -> Result<()> {
    // verify invariant: optimizer passes should not change the schema if the schema can't be cast from the previous schema.
    // Refer to <https://datafusion.apache.org/contributor-guide/specification/invariants.html#logical-schema-is-invariant-under-logical-optimization>
    assert_expected_schema(prev_schema, plan)?;

    Ok(())
}

/// [`assert_valid_optimization`] for the plan after one rule: the outputs
/// keep their number and types, but may be renamed (see [`OutputNames`])
fn assert_valid_optimization_up_to_names(
    plan: &LogicalPlan,
    prev_schema: &Arc<DFSchema>,
) -> Result<()> {
    let schema = plan.schema();
    let compatible = schema.fields().len() == prev_schema.fields().len()
        && schema
            .fields()
            .iter()
            .zip(prev_schema.fields())
            .all(|(f1, f2)| {
                DFSchema::datatype_is_logically_equal(f1.data_type(), f2.data_type())
            });
    assert_or_internal_err!(
        compatible,
        "Failed due to a difference in schemas: original schema: {:?}, new schema: {:?}",
        prev_schema,
        schema
    );
    Ok(())
}

/// The names of the outputs of the root of the plan being optimized, and of
/// the input of a root that writes it (`Dml`, `Copy`): the input of a write
/// is a second root, whose names the table provider (`insert_into`, `update`,
/// `merge_into`) or the file writer reads. See [`OutputNames`].
///
/// The analyzer records and restores them too: its rules (type coercion,
/// function rewrites) rename the outputs they rewrite.
pub(crate) struct RootNames {
    root: OutputNames,
    written: Option<OutputNames>,
}

impl RootNames {
    pub(crate) fn of(plan: &LogicalPlan) -> Self {
        let written = match plan {
            LogicalPlan::Dml(DmlStatement { input, .. })
            | LogicalPlan::Copy(CopyTo { input, .. }) => Some(OutputNames::of(input)),
            _ => None,
        };
        Self {
            root: OutputNames::of(plan),
            written,
        }
    }

    /// `plan` with the outputs of its root, and of the input of a write,
    /// renamed back to the recorded names
    pub(crate) fn restore(
        &self,
        plan: LogicalPlan,
        generator: &ColumnIdGenerator,
    ) -> Result<Transformed<LogicalPlan>> {
        let plan = match (&self.written, plan) {
            (Some(names), LogicalPlan::Dml(mut dml)) => {
                let input = names.restore(Arc::unwrap_or_clone(dml.input), generator)?;
                dml.input = Arc::new(input.data);
                Transformed::new_transformed(LogicalPlan::Dml(dml), input.transformed)
            }
            (Some(names), LogicalPlan::Copy(mut copy)) => {
                let input = names.restore(Arc::unwrap_or_clone(copy.input), generator)?;
                copy.input = Arc::new(input.data);
                Transformed::new_transformed(LogicalPlan::Copy(copy), input.transformed)
            }
            (_, plan) => Transformed::no(plan),
        };
        plan.transform_data(|plan| self.root.restore(plan, generator))
    }
}

/// The names (qualifier and name) of the outputs of a plan being optimized.
///
/// Names come from the definitions: a rule that rewrites an output may rename
/// it, which breaks nothing inside the plan (references resolve by id), but
/// the outputs of the root are user visible. The optimizer records them when
/// it starts and restores them once when it ends, renaming the outputs of a
/// root projection in place, or adding a projection only if another root
/// renamed them.
struct OutputNames(Vec<(Option<TableReference>, String)>);

impl OutputNames {
    fn of(plan: &LogicalPlan) -> Self {
        Self(
            plan.schema()
                .iter()
                .map(|(qualifier, field)| (qualifier.cloned(), field.name().clone()))
                .collect(),
        )
    }

    /// The indexes of the outputs of `plan` that were renamed
    fn renamed(&self, plan: &LogicalPlan) -> Vec<usize> {
        let schema = plan.schema();
        if schema.fields().len() != self.0.len() {
            // the per-rule check reports this
            return vec![];
        }
        schema
            .iter()
            .zip(&self.0)
            .enumerate()
            .filter(|(_, ((qualifier, field), (old_qualifier, old_name)))| {
                *qualifier != old_qualifier.as_ref() || field.name() != old_name
            })
            .map(|(i, _)| i)
            .collect()
    }

    /// `expr` renamed to the output name `i`
    fn rename(&self, expr: Expr, i: usize) -> Expr {
        let (qualifier, name) = &self.0[i];
        match expr {
            // keep the alias's metadata
            Expr::Alias(mut alias) => {
                alias.relation.clone_from(qualifier);
                alias.name.clone_from(name);
                Expr::Alias(alias)
            }
            expr => expr.alias_qualified(qualifier.clone(), name),
        }
    }

    /// `plan` with the outputs renamed back to the recorded names
    fn restore(
        &self,
        plan: LogicalPlan,
        generator: &ColumnIdGenerator,
    ) -> Result<Transformed<LogicalPlan>> {
        let renamed = self.renamed(&plan);
        if renamed.is_empty() {
            return Ok(Transformed::no(plan));
        }
        // a node that passes its input's columns through (a sort, a limit, a
        // filter) over a projection: the projection is renamed in place
        if matches!(
            plan,
            LogicalPlan::Sort(_) | LogicalPlan::Limit(_) | LogicalPlan::Filter(_)
        ) && reaches_projection(&plan)
        {
            let input = plan.inputs()[0].clone();
            let input = self.restore(input, generator)?.data;
            let plan = plan.with_new_exprs(plan.expressions(), vec![input])?;
            return Ok(Transformed::yes(plan));
        }
        // an aggregate without grouping sets whose renamed outputs are its
        // aggregate expressions: they are renamed in place
        if let LogicalPlan::Aggregate(aggregate) = &plan
            && aggregate.grouping_sets.is_none()
            && let group_len = aggregate.group_expr_len()?
            && renamed.iter().all(|i| *i >= group_len)
        {
            let LogicalPlan::Aggregate(aggregate) = plan else {
                unreachable!()
            };
            let aggr_expr = aggregate
                .aggr_expr
                .into_iter()
                .enumerate()
                .map(|(i, entry)| {
                    if renamed.contains(&(group_len + i)) {
                        let expr = self.rename(entry.expr.clone(), group_len + i);
                        entry.with_expr(expr)
                    } else {
                        entry
                    }
                })
                .collect();
            let aggregate = Aggregate::try_new(
                aggregate.input,
                aggregate.group_expr,
                aggregate.grouping_sets,
                aggr_expr,
            )?;
            return Ok(Transformed::yes(LogicalPlan::Aggregate(aggregate)));
        }
        let plan = match plan {
            LogicalPlan::Projection(Projection { expr, input, .. }) => {
                let expr = expr
                    .into_iter()
                    .enumerate()
                    .map(|(i, entry)| {
                        if renamed.contains(&i) {
                            let expr = self.rename(entry.expr.clone(), i);
                            // a renamed pass-through still passes its
                            // column through, with its id
                            entry.with_rewritten_expr(expr, generator)
                        } else {
                            entry
                        }
                    })
                    .collect();
                Projection::try_new(expr, input)?
            }
            plan => {
                let schema = Arc::clone(plan.schema());
                let expr = schema
                    .columns()
                    .into_iter()
                    .enumerate()
                    .map(|(i, column)| {
                        let entry =
                            NamedExpr::allocate(Expr::Column(column), &schema, generator);
                        if renamed.contains(&i) {
                            let expr = self.rename(entry.expr.clone(), i);
                            entry.with_rewritten_expr(expr, generator)
                        } else {
                            entry
                        }
                    })
                    .collect();
                Projection::try_new(expr, Arc::new(plan))?
            }
        };
        Ok(Transformed::yes(LogicalPlan::Projection(plan)))
    }
}

/// Whether `plan` is a projection, or passes the columns of one through
/// sorts, limits and filters
fn reaches_projection(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Projection(_) => true,
        LogicalPlan::Sort(_) | LogicalPlan::Limit(_) | LogicalPlan::Filter(_) => {
            reaches_projection(plan.inputs()[0])
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use arrow::datatypes::Metadata;

    use arrow::datatypes::{DataType, Field};
    use datafusion_common::tree_node::Transformed;
    use datafusion_common::{
        Column, ColumnId, DFSchema, DFSchemaRef, DataFusionError, Result,
        assert_contains, plan_err,
    };
    use datafusion_expr::dml::InsertOp;
    use datafusion_expr::logical_plan::EmptyRelation;
    use datafusion_expr::logical_plan::builder::LogicalTableSource;
    use datafusion_expr::{
        Expr, JoinType, LogicalPlan, LogicalPlanBuilder, NamedExpr, Projection, col, lit,
    };
    use insta::assert_snapshot;

    use crate::optimizer::Optimizer;
    use crate::test::{test_table_scan, test_table_scan_with_name};
    use crate::{OptimizerConfig, OptimizerContext, OptimizerRule};

    use super::ApplyOrder;

    #[test]
    fn optimizer_advances_column_id_generator_past_the_plan() -> Result<()> {
        let id = |n| ColumnId::new(std::num::NonZeroU64::new(n).unwrap());
        // a plan with a field id built with another generator
        let schema = DFSchema::from_unqualified_fields(
            vec![Field::new("a", DataType::Int32, true)].into(),
            std::collections::HashMap::new(),
        )?
        .with_field_ids(vec![Some(id(41))])?;
        let plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(schema),
        });

        let config = OptimizerContext::new();
        let opt = Optimizer::with_rules(vec![]);
        opt.optimize(plan, &config, observe)?;
        assert_eq!(config.column_id_generator().next(), id(42));
        Ok(())
    }

    #[test]
    fn restores_the_renamed_outputs_of_a_root_projection() -> Result<()> {
        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .project(vec![col("a").alias("x"), col("b")])?
            .build()?;
        let ids = plan.schema().field_ids().to_vec();
        let config = OptimizerContext::new();
        let opt = Optimizer::with_rules(vec![Arc::new(RenameXToYRule {})]);
        let optimized = opt.optimize(plan, &config, observe)?;
        // renamed in place, keeping the ids of the outputs
        assert_snapshot!(optimized, @r"
        Projection: test.a AS x, test.b
          TableScan: test
        ");
        assert_eq!(optimized.schema().field_ids(), ids.as_slice());
        Ok(())
    }

    #[test]
    fn restores_the_renamed_outputs_below_the_root() -> Result<()> {
        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .project(vec![col("a").alias("x"), col("b")])?
            .limit(0, Some(1))?
            .build()?;
        let config = OptimizerContext::new();
        let opt = Optimizer::with_rules(vec![Arc::new(RenameXToYRule {})]);
        let optimized = opt.optimize(plan, &config, observe)?;
        // a projection over the root renames them back
        assert_snapshot!(optimized, @"
        Limit: skip=0, fetch=1
          Projection: test.a AS x, test.b
            TableScan: test
        ");
        assert_eq!(
            optimized.schema().field_names(),
            vec!["x".to_string(), "test.b".to_string()]
        );
        Ok(())
    }

    #[test]
    fn restores_the_renamed_outputs_of_the_input_of_a_write() -> Result<()> {
        let input = LogicalPlanBuilder::from(test_table_scan()?)
            .project(vec![col("a").alias("x"), col("b")])?
            .limit(0, Some(1))?
            .build()?;
        let target = Arc::new(LogicalTableSource::new(Arc::new(
            input.schema().as_arrow().clone(),
        )));
        let plan = LogicalPlanBuilder::insert_into(input, "t", target, InsertOp::Append)?
            .build()?;
        let config = OptimizerContext::new();
        let opt = Optimizer::with_rules(vec![Arc::new(RenameXToYRule {})]);
        let optimized = opt.optimize(plan, &config, observe)?;
        // the table provider reads the names of the input of an INSERT
        assert_snapshot!(optimized, @"
        Dml: op=[Insert Into] table=[t]
          Limit: skip=0, fetch=1
            Projection: test.a AS x, test.b
              TableScan: test
        ");
        Ok(())
    }

    #[test]
    fn skip_failing_rule() {
        let opt = Optimizer::with_rules(vec![Arc::new(BadRule {})]);
        let config = OptimizerContext::new().with_skip_failing_rules(true);
        let plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });
        opt.optimize(plan, &config, &observe).unwrap();
    }

    #[test]
    fn no_skip_failing_rule() {
        let opt = Optimizer::with_rules(vec![Arc::new(BadRule {})]);
        let config = OptimizerContext::new().with_skip_failing_rules(false);
        let plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });
        let err = opt.optimize(plan, &config, &observe).unwrap_err();
        assert_eq!(
            "Optimizer rule 'bad rule' failed\ncaused by\n\
            Error during planning: rule failed",
            err.strip_backtrace()
        );
    }

    #[test]
    fn generate_different_schema() {
        let opt = Optimizer::with_rules(vec![Arc::new(GetTableScanRule {})]);
        let config = OptimizerContext::new().with_skip_failing_rules(false);
        let plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });
        let err = opt.optimize(plan, &config, &observe).unwrap_err();

        // Simplify assert to check the error message contains the expected message
        assert_contains!(
            err.strip_backtrace(),
            "Failed due to a difference in schemas: original schema: DFSchema"
        );
    }

    #[test]
    fn skip_generate_different_schema() {
        let opt = Optimizer::with_rules(vec![Arc::new(GetTableScanRule {})]);
        let config = OptimizerContext::new().with_skip_failing_rules(true);
        let plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });
        opt.optimize(plan, &config, &observe).unwrap();
    }

    #[test]
    fn generate_same_schema_different_metadata() -> Result<()> {
        // if the plan creates more metadata than previously (because
        // some wrapping functions are removed, etc) do not error
        let opt = Optimizer::with_rules(vec![Arc::new(GetTableScanRule {})]);
        let config = OptimizerContext::new().with_skip_failing_rules(false);

        let input = Arc::new(test_table_scan()?);
        let input_schema = Arc::clone(input.schema());

        let plan = LogicalPlan::Projection(Projection::try_new_with_schema(
            crate::test::named_exprs(vec![col("a"), col("b"), col("c")], &input),
            input,
            add_metadata_to_fields(input_schema.as_ref()),
        )?);

        // optimizing should be ok, but the schema will have changed  (no metadata)
        assert_ne!(plan.schema().as_ref(), input_schema.as_ref());
        let optimized_plan = opt.optimize(plan, &config, &observe)?;
        // metadata was removed
        assert_eq!(optimized_plan.schema().as_ref(), input_schema.as_ref());
        Ok(())
    }

    #[test]
    fn in_place_rewrite_recomputes_parent_schema_when_child_schema_changes() -> Result<()>
    {
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("left")?)
            .project(vec![col("left.a"), col("left.b"), col("left.c")])?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("right")?)
            .project(vec![col("right.a"), col("right.b"), col("right.c")])?
            .build()?;
        let mut plan = LogicalPlanBuilder::from(left)
            .join_on(right, JoinType::Inner, [col("left.a").eq(col("right.a"))])?
            .build()?;

        assert_eq!(plan.schema().fields().len(), 6);

        let changed = super::rewrite_plan_in_place(
            &mut plan,
            ApplyOrder::TopDown,
            &KeepOnlyAProjectionRule {},
            &OptimizerContext::new(),
        )?;

        assert!(changed);
        assert_eq!(plan.schema().fields().len(), 2);
        assert!(plan.schema().has_column_with_unqualified_name("a"));
        Ok(())
    }

    #[test]
    fn optimizer_detects_plan_equal_to_the_initial() -> Result<()> {
        // Run a goofy optimizer, which rotates projection columns
        // [1, 2, 3] -> [2, 3, 1] -> [3, 1, 2] -> [1, 2, 3]

        let opt = Optimizer::with_rules(vec![Arc::new(RotateProjectionRule::new(false))]);
        let config = OptimizerContext::new().with_max_passes(16);

        let initial_plan = LogicalPlanBuilder::empty(false)
            .project([lit(1), lit(2), lit(3)])?
            .project([lit(100)])? // to not trigger changed schema error
            .build()?;

        let mut plans: Vec<LogicalPlan> = Vec::new();
        let final_plan =
            opt.optimize(initial_plan.clone(), &config, |p, _| plans.push(p.clone()))?;

        // initial_plan is not observed, so we have 3 plans
        assert_eq!(3, plans.len());

        // we got again the initial_plan with [1, 2, 3]
        assert_eq!(initial_plan, final_plan);

        Ok(())
    }

    #[test]
    fn optimizer_detects_plan_equal_to_a_non_initial() -> Result<()> {
        // Run a goofy optimizer, which reverses and rotates projection columns
        // [1, 2, 3] -> [3, 2, 1] -> [2, 1, 3] -> [1, 3, 2] -> [3, 2, 1]

        let opt = Optimizer::with_rules(vec![Arc::new(RotateProjectionRule::new(true))]);
        let config = OptimizerContext::new().with_max_passes(16);

        let initial_plan = LogicalPlanBuilder::empty(false)
            .project([lit(1), lit(2), lit(3)])?
            .project([lit(100)])? // to not trigger changed schema error
            .build()?;

        let mut plans: Vec<LogicalPlan> = Vec::new();
        let final_plan =
            opt.optimize(initial_plan, &config, |p, _| plans.push(p.clone()))?;

        // initial_plan is not observed, so we have 4 plans
        assert_eq!(4, plans.len());

        // we got again the plan with [3, 2, 1]
        assert_eq!(plans[0], final_plan);

        Ok(())
    }

    fn add_metadata_to_fields(schema: &DFSchema) -> DFSchemaRef {
        let new_fields = schema
            .iter()
            .enumerate()
            .map(|(i, (qualifier, field))| {
                let metadata = Metadata::new().with("key", format!("value {i}"));

                let new_arrow_field = field.as_ref().clone().with_metadata(metadata);
                (qualifier.cloned(), Arc::new(new_arrow_field))
            })
            .collect::<Vec<_>>();

        let new_metadata = schema.metadata().clone();
        Arc::new(DFSchema::new_with_metadata(new_fields, new_metadata).unwrap())
    }

    fn observe(_plan: &LogicalPlan, _rule: &dyn OptimizerRule) {}

    #[derive(Default, Debug)]
    struct BadRule {}

    impl OptimizerRule for BadRule {
        fn name(&self) -> &str {
            "bad rule"
        }

        fn rewrite(
            &self,
            _plan: LogicalPlan,
            _config: &dyn OptimizerConfig,
        ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
            plan_err!("rule failed")
        }
    }

    /// Replaces whatever plan with a single table scan
    #[derive(Default, Debug)]
    struct GetTableScanRule {}

    impl OptimizerRule for GetTableScanRule {
        fn name(&self) -> &str {
            "get table_scan rule"
        }

        fn rewrite(
            &self,
            _plan: LogicalPlan,
            _config: &dyn OptimizerConfig,
        ) -> Result<Transformed<LogicalPlan>> {
            let table_scan = test_table_scan()?;
            Ok(Transformed::yes(
                LogicalPlanBuilder::from(table_scan).build()?,
            ))
        }
    }

    /// Renames the projection outputs aliased `x` to `y`, keeping their ids,
    /// as a rewrite that does not preserve names does
    #[derive(Default, Debug)]
    struct RenameXToYRule {}

    impl OptimizerRule for RenameXToYRule {
        fn name(&self) -> &str {
            "rename_x_to_y"
        }

        fn apply_order(&self) -> Option<ApplyOrder> {
            Some(ApplyOrder::TopDown)
        }

        fn rewrite(
            &self,
            plan: LogicalPlan,
            _config: &dyn OptimizerConfig,
        ) -> Result<Transformed<LogicalPlan>> {
            let LogicalPlan::Projection(projection) = plan else {
                return Ok(Transformed::no(plan));
            };
            let mut transformed = false;
            let expr = projection
                .expr
                .into_iter()
                .map(|entry| match entry.expr {
                    Expr::Alias(mut alias) if alias.name == "x" => {
                        transformed = true;
                        alias.name = "y".to_string();
                        NamedExpr::new(Expr::Alias(alias), entry.id)
                    }
                    _ => entry,
                })
                .collect();
            let plan =
                LogicalPlan::Projection(Projection::try_new(expr, projection.input)?);
            Ok(Transformed::new_transformed(plan, transformed))
        }
    }

    #[derive(Default, Debug)]
    struct KeepOnlyAProjectionRule {}

    impl OptimizerRule for KeepOnlyAProjectionRule {
        fn name(&self) -> &str {
            "keep_only_a_projection"
        }

        fn apply_order(&self) -> Option<ApplyOrder> {
            Some(ApplyOrder::TopDown)
        }

        fn rewrite(
            &self,
            plan: LogicalPlan,
            _config: &dyn OptimizerConfig,
        ) -> Result<Transformed<LogicalPlan>> {
            let LogicalPlan::Projection(projection) = plan else {
                return Ok(Transformed::no(plan));
            };

            let expr = Expr::from(Column::from(projection.schema.qualified_field(0)));

            Ok(Transformed::yes(LogicalPlan::Projection(
                crate::test::new_projection(vec![expr], Arc::clone(&projection.input))?,
            )))
        }
    }

    /// A goofy rule doing rotation of columns in all projections.
    ///
    /// Useful to test cycle detection.
    #[derive(Default, Debug)]
    struct RotateProjectionRule {
        // reverse exprs instead of rotating on the first pass
        reverse_on_first_pass: Mutex<bool>,
    }

    impl RotateProjectionRule {
        fn new(reverse_on_first_pass: bool) -> Self {
            Self {
                reverse_on_first_pass: Mutex::new(reverse_on_first_pass),
            }
        }
    }

    impl OptimizerRule for RotateProjectionRule {
        fn name(&self) -> &str {
            "rotate_projection"
        }

        fn apply_order(&self) -> Option<ApplyOrder> {
            Some(ApplyOrder::TopDown)
        }

        fn rewrite(
            &self,
            plan: LogicalPlan,
            _config: &dyn OptimizerConfig,
        ) -> Result<Transformed<LogicalPlan>> {
            let projection = match plan {
                LogicalPlan::Projection(p) if p.expr.len() >= 2 => p,
                _ => return Ok(Transformed::no(plan)),
            };

            let mut exprs = projection.expr.clone();

            let mut reverse = self.reverse_on_first_pass.lock().unwrap();
            if *reverse {
                exprs.reverse();
                *reverse = false;
            } else {
                exprs.rotate_left(1);
            }

            Ok(Transformed::yes(LogicalPlan::Projection(
                Projection::try_new(exprs, Arc::clone(&projection.input))?,
            )))
        }
    }
}
