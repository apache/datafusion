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

//! [`SessionState`]: information required to run queries in a session

use std::any::Any;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;

use crate::catalog::{CatalogProviderList, SchemaProvider, TableProviderFactory};
use crate::datasource::cte_worktable::CteWorkTable;
use crate::datasource::file_format::{format_as_file_type, FileFormatFactory};
use crate::datasource::provider_as_source;
use crate::execution::context::{EmptySerializerRegistry, FunctionFactory, QueryPlanner};
use crate::execution::SessionStateDefaults;
use crate::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion_catalog::information_schema::{
    InformationSchemaProvider, INFORMATION_SCHEMA,
};
use datafusion_catalog::MemoryCatalogProviderList;

use arrow::datatypes::{DataType, SchemaRef};
use datafusion_catalog::{Session, TableFunction, TableFunctionImpl};
use datafusion_common::alias::AliasGenerator;
use datafusion_common::config::{ConfigExtension, ConfigOptions, TableOptions};
use datafusion_common::display::{PlanType, StringifiedPlan, ToStringifiedPlan};
use datafusion_common::file_options::file_type::FileType;
use datafusion_common::tree_node::TreeNode;
use datafusion_common::{
    config_err, exec_err, not_impl_err, plan_datafusion_err, DFSchema, DataFusionError,
    ResolvedTableReference, TableReference,
};
use datafusion_execution::config::SessionConfig;
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_execution::TaskContext;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::expr_rewriter::FunctionRewrite;
use datafusion_expr::planner::{ExprPlanner, TypePlanner};
use datafusion_expr::registry::{FunctionRegistry, SerializerRegistry};
use datafusion_expr::simplify::SimplifyInfo;
use datafusion_expr::var_provider::{is_system_variables, VarType};
use datafusion_expr::{
    AggregateUDF, Explain, Expr, ExprSchemable, LogicalPlan, ScalarUDF, TableSource,
    WindowUDF,
};
use datafusion_optimizer::simplify_expressions::ExprSimplifier;
use datafusion_optimizer::{
    Analyzer, AnalyzerRule, Optimizer, OptimizerConfig, OptimizerRule,
};
use datafusion_physical_expr::create_physical_expr;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_sql::parser::{DFParser, Statement};
use datafusion_sql::planner::{ContextProvider, ParserOptions, PlannerContext, SqlToRel};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use log::{debug, info};
use object_store::ObjectStore;
use sqlparser::ast::{Expr as SQLExpr, ExprWithAlias as SQLExprWithAlias};
use sqlparser::dialect::dialect_from_str;
use url::Url;
use uuid::Uuid;

/// `SessionState` contains all the necessary state to plan and execute queries,
/// such as configuration, functions, and runtime environment. Please see the
/// documentation on [`SessionContext`] for more information.
///
///
/// # Example: `SessionState` from a [`SessionContext`]
///
/// ```
/// use datafusion::prelude::*;
/// let ctx = SessionContext::new();
/// let state = ctx.state();
/// ```
///
/// # Example: `SessionState` via [`SessionStateBuilder`]
///
/// You can also use [`SessionStateBuilder`] to build a `SessionState` object
/// directly:
///
/// ```
/// use datafusion::prelude::*;
/// # use datafusion::{error::Result, assert_batches_eq};
/// # use datafusion::execution::session_state::SessionStateBuilder;
/// # use datafusion_execution::runtime_env::RuntimeEnv;
/// # use std::sync::Arc;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
///     let state = SessionStateBuilder::new()
///         .with_config(SessionConfig::new())  
///         .with_runtime_env(Arc::new(RuntimeEnv::default()))
///         .with_default_features()
///         .build();
///     Ok(())  
/// # }
/// ```
///
/// Note that there is no `Default` or `new()` for SessionState,
/// to avoid accidentally running queries or other operations without passing through
/// the [`SessionConfig`] or [`RuntimeEnv`]. See [`SessionStateBuilder`] and
/// [`SessionContext`].
///
/// [`SessionContext`]: crate::execution::context::SessionContext
#[derive(Clone)]
pub struct SessionState {
    /// A unique UUID that identifies the session
    session_id: String,
    /// Responsible for analyzing and rewrite a logical plan before optimization
    analyzer: Analyzer,
    /// Provides support for customizing the SQL planner, e.g. to add support for custom operators like `->>` or `?`
    expr_planners: Vec<Arc<dyn ExprPlanner>>,
    /// Provides support for customizing the SQL type planning
    type_planner: Option<Arc<dyn TypePlanner>>,
    /// Responsible for optimizing a logical plan
    optimizer: Optimizer,
    /// Responsible for optimizing a physical execution plan
    physical_optimizers: PhysicalOptimizer,
    /// Responsible for planning `LogicalPlan`s, and `ExecutionPlan`
    query_planner: Arc<dyn QueryPlanner + Send + Sync>,
    /// Collection of catalogs containing schemas and ultimately TableProviders
    catalog_list: Arc<dyn CatalogProviderList>,
    /// Table Functions
    table_functions: HashMap<String, Arc<TableFunction>>,
    /// Scalar functions that are registered with the context
    scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    /// Aggregate functions registered in the context
    aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    /// Window functions registered in the context
    window_functions: HashMap<String, Arc<WindowUDF>>,
    /// Deserializer registry for extensions.
    serializer_registry: Arc<dyn SerializerRegistry>,
    /// Holds registered external FileFormat implementations
    file_formats: HashMap<String, Arc<dyn FileFormatFactory>>,
    /// Session configuration
    config: SessionConfig,
    /// Table options
    table_options: TableOptions,
    /// Execution properties
    execution_props: ExecutionProps,
    /// TableProviderFactories for different file formats.
    ///
    /// Maps strings like "JSON" to an instance of  [`TableProviderFactory`]
    ///
    /// This is used to create [`TableProvider`] instances for the
    /// `CREATE EXTERNAL TABLE ... STORED AS <FORMAT>` for custom file
    /// formats other than those built into DataFusion
    ///
    /// [`TableProvider`]: crate::catalog::TableProvider
    table_factories: HashMap<String, Arc<dyn TableProviderFactory>>,
    /// Runtime environment
    runtime_env: Arc<RuntimeEnv>,
    /// [FunctionFactory] to support pluggable user defined function handler.
    ///
    /// It will be invoked on `CREATE FUNCTION` statements.
    /// thus, changing dialect o PostgreSql is required
    function_factory: Option<Arc<dyn FunctionFactory>>,
    /// Cache logical plans of prepared statements for later execution.
    /// Key is the prepared statement name.
    prepared_plans: HashMap<String, Arc<PreparedPlan>>,
}

impl Debug for SessionState {
    /// Prefer having short fields at the top and long vector fields near the end
    /// Group fields by
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionState")
            .field("session_id", &self.session_id)
            .field("config", &self.config)
            .field("runtime_env", &self.runtime_env)
            .field("catalog_list", &self.catalog_list)
            .field("serializer_registry", &self.serializer_registry)
            .field("file_formats", &self.file_formats)
            .field("execution_props", &self.execution_props)
            .field("table_options", &self.table_options)
            .field("table_factories", &self.table_factories)
            .field("function_factory", &self.function_factory)
            .field("expr_planners", &self.expr_planners)
            .field("type_planner", &self.type_planner)
            .field("query_planners", &self.query_planner)
            .field("analyzer", &self.analyzer)
            .field("optimizer", &self.optimizer)
            .field("physical_optimizers", &self.physical_optimizers)
            .field("table_functions", &self.table_functions)
            .field("scalar_functions", &self.scalar_functions)
            .field("aggregate_functions", &self.aggregate_functions)
            .field("window_functions", &self.window_functions)
            .field("prepared_plans", &self.prepared_plans)
            .finish()
    }
}

#[async_trait]
impl Session for SessionState {
    fn session_id(&self) -> &str {
        self.session_id()
    }

    fn config(&self) -> &SessionConfig {
        self.config()
    }

    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        self.create_physical_plan(logical_plan).await
    }

    fn create_physical_expr(
        &self,
        expr: Expr,
        df_schema: &DFSchema,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        self.create_physical_expr(expr, df_schema)
    }

    fn scalar_functions(&self) -> &HashMap<String, Arc<ScalarUDF>> {
        &self.scalar_functions
    }

    fn aggregate_functions(&self) -> &HashMap<String, Arc<AggregateUDF>> {
        &self.aggregate_functions
    }

    fn window_functions(&self) -> &HashMap<String, Arc<WindowUDF>> {
        &self.window_functions
    }

    fn runtime_env(&self) -> &Arc<RuntimeEnv> {
        self.runtime_env()
    }

    fn execution_props(&self) -> &ExecutionProps {
        self.execution_props()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl SessionState {
    /// Returns new [`SessionState`] using the provided
    /// [`SessionConfig`] and [`RuntimeEnv`].
    #[deprecated(since = "41.0.0", note = "Use SessionStateBuilder")]
    pub fn new_with_config_rt(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> Self {
        SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features()
            .build()
    }

    /// Returns new [`SessionState`] using the provided
    /// [`SessionConfig`],  [`RuntimeEnv`], and [`CatalogProviderList`]
    #[deprecated(since = "40.0.0", note = "Use SessionStateBuilder")]
    pub fn new_with_config_rt_and_catalog_list(
        config: SessionConfig,
        runtime: Arc<RuntimeEnv>,
        catalog_list: Arc<dyn CatalogProviderList>,
    ) -> Self {
        SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_catalog_list(catalog_list)
            .with_default_features()
            .build()
    }

    pub(crate) fn resolve_table_ref(
        &self,
        table_ref: impl Into<TableReference>,
    ) -> ResolvedTableReference {
        let catalog = &self.config_options().catalog;
        table_ref
            .into()
            .resolve(&catalog.default_catalog, &catalog.default_schema)
    }

    /// Retrieve the [`SchemaProvider`] for a specific [`TableReference`], if it
    /// exists.
    pub fn schema_for_ref(
        &self,
        table_ref: impl Into<TableReference>,
    ) -> datafusion_common::Result<Arc<dyn SchemaProvider>> {
        let resolved_ref = self.resolve_table_ref(table_ref);
        if self.config.information_schema() && *resolved_ref.schema == *INFORMATION_SCHEMA
        {
            return Ok(Arc::new(InformationSchemaProvider::new(Arc::clone(
                &self.catalog_list,
            ))));
        }

        self.catalog_list
            .catalog(&resolved_ref.catalog)
            .ok_or_else(|| {
                plan_datafusion_err!(
                    "failed to resolve catalog: {}",
                    resolved_ref.catalog
                )
            })?
            .schema(&resolved_ref.schema)
            .ok_or_else(|| {
                plan_datafusion_err!("failed to resolve schema: {}", resolved_ref.schema)
            })
    }

    #[deprecated(since = "40.0.0", note = "Use SessionStateBuilder")]
    /// Replace the random session id.
    pub fn with_session_id(mut self, session_id: String) -> Self {
        self.session_id = session_id;
        self
    }

    #[deprecated(since = "40.0.0", note = "Use SessionStateBuilder")]
    /// override default query planner with `query_planner`
    pub fn with_query_planner(
        mut self,
        query_planner: Arc<dyn QueryPlanner + Send + Sync>,
    ) -> Self {
        self.query_planner = query_planner;
        self
    }

    #[deprecated(since = "40.0.0", note = "Use SessionStateBuilder")]
    /// Override the [`AnalyzerRule`]s optimizer plan rules.
    pub fn with_analyzer_rules(
        mut self,
        rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
    ) -> Self {
        self.analyzer = Analyzer::with_rules(rules);
        self
    }

    #[deprecated(since = "40.0.0", note = "Use SessionStateBuilder")]
    /// Replace the entire list of [`OptimizerRule`]s used to optimize plans
    pub fn with_optimizer_rules(
        mut self,
        rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
    ) -> Self {
        self.optimizer = Optimizer::with_rules(rules);
        self
    }

    #[deprecated(since = "40.0.0", note = "Use SessionStateBuilder")]
    /// Replace the entire list of [`PhysicalOptimizerRule`]s used to optimize plans
    pub fn with_physical_optimizer_rules(
        mut self,
        physical_optimizers: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
    ) -> Self {
        self.physical_optimizers = PhysicalOptimizer::with_rules(physical_optimizers);
        self
    }

    /// Add `analyzer_rule` to the end of the list of
    /// [`AnalyzerRule`]s used to rewrite queries.
    pub fn add_analyzer_rule(
        &mut self,
        analyzer_rule: Arc<dyn AnalyzerRule + Send + Sync>,
    ) -> &Self {
        self.analyzer.rules.push(analyzer_rule);
        self
    }

    #[deprecated(since = "40.0.0", note = "Use SessionStateBuilder")]
    /// Add `optimizer_rule` to the end of the list of
    /// [`OptimizerRule`]s used to rewrite queries.
    pub fn add_optimizer_rule(
        mut self,
        optimizer_rule: Arc<dyn OptimizerRule + Send + Sync>,
    ) -> Self {
        self.optimizer.rules.push(optimizer_rule);
        self
    }

    // the add_optimizer_rule takes an owned reference
    // it should probably be renamed to `with_optimizer_rule` to follow builder style
    // and `add_optimizer_rule` that takes &mut self added instead of this
    pub(crate) fn append_optimizer_rule(
        &mut self,
        optimizer_rule: Arc<dyn OptimizerRule + Send + Sync>,
    ) {
        self.optimizer.rules.push(optimizer_rule);
    }

    #[deprecated(since = "40.0.0", note = "Use SessionStateBuilder")]
    /// Add `physical_optimizer_rule` to the end of the list of
    /// [`PhysicalOptimizerRule`]s used to rewrite queries.
    pub fn add_physical_optimizer_rule(
        mut self,
        physical_optimizer_rule: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
    ) -> Self {
        self.physical_optimizers.rules.push(physical_optimizer_rule);
        self
    }

    #[deprecated(since = "40.0.0", note = "Use SessionStateBuilder")]
    /// Adds a new [`ConfigExtension`] to TableOptions
    pub fn add_table_options_extension<T: ConfigExtension>(
        mut self,
        extension: T,
    ) -> Self {
        self.table_options.extensions.insert(extension);
        self
    }

    #[deprecated(since = "40.0.0", note = "Use SessionStateBuilder")]
    /// Registers a [`FunctionFactory`] to handle `CREATE FUNCTION` statements
    pub fn with_function_factory(
        mut self,
        function_factory: Arc<dyn FunctionFactory>,
    ) -> Self {
        self.function_factory = Some(function_factory);
        self
    }

    /// Registers a [`FunctionFactory`] to handle `CREATE FUNCTION` statements
    pub fn set_function_factory(&mut self, function_factory: Arc<dyn FunctionFactory>) {
        self.function_factory = Some(function_factory);
    }

    #[deprecated(since = "40.0.0", note = "Use SessionStateBuilder")]
    /// Replace the extension [`SerializerRegistry`]
    pub fn with_serializer_registry(
        mut self,
        registry: Arc<dyn SerializerRegistry>,
    ) -> Self {
        self.serializer_registry = registry;
        self
    }

    /// Get the function factory
    pub fn function_factory(&self) -> Option<&Arc<dyn FunctionFactory>> {
        self.function_factory.as_ref()
    }

    /// Get the table factories
    pub fn table_factories(&self) -> &HashMap<String, Arc<dyn TableProviderFactory>> {
        &self.table_factories
    }

    /// Get the table factories
    pub fn table_factories_mut(
        &mut self,
    ) -> &mut HashMap<String, Arc<dyn TableProviderFactory>> {
        &mut self.table_factories
    }

    /// Parse an SQL string into an DataFusion specific AST
    /// [`Statement`]. See [`SessionContext::sql`] for running queries.
    ///
    /// [`SessionContext::sql`]: crate::execution::context::SessionContext::sql
    pub fn sql_to_statement(
        &self,
        sql: &str,
        dialect: &str,
    ) -> datafusion_common::Result<Statement> {
        let dialect = dialect_from_str(dialect).ok_or_else(|| {
            plan_datafusion_err!(
                "Unsupported SQL dialect: {dialect}. Available dialects: \
                     Generic, MySQL, PostgreSQL, Hive, SQLite, Snowflake, Redshift, \
                     MsSQL, ClickHouse, BigQuery, Ansi."
            )
        })?;
        let mut statements = DFParser::parse_sql_with_dialect(sql, dialect.as_ref())?;
        if statements.len() > 1 {
            return not_impl_err!(
                "The context currently only supports a single SQL statement"
            );
        }
        let statement = statements.pop_front().ok_or_else(|| {
            plan_datafusion_err!("No SQL statements were provided in the query string")
        })?;
        Ok(statement)
    }

    /// parse a sql string into a sqlparser-rs AST [`SQLExpr`].
    ///
    /// See [`Self::create_logical_expr`] for parsing sql to [`Expr`].
    pub fn sql_to_expr(
        &self,
        sql: &str,
        dialect: &str,
    ) -> datafusion_common::Result<SQLExpr> {
        self.sql_to_expr_with_alias(sql, dialect).map(|x| x.expr)
    }

    /// parse a sql string into a sqlparser-rs AST [`SQLExprWithAlias`].
    ///
    /// See [`Self::create_logical_expr`] for parsing sql to [`Expr`].
    pub fn sql_to_expr_with_alias(
        &self,
        sql: &str,
        dialect: &str,
    ) -> datafusion_common::Result<SQLExprWithAlias> {
        let dialect = dialect_from_str(dialect).ok_or_else(|| {
            plan_datafusion_err!(
                "Unsupported SQL dialect: {dialect}. Available dialects: \
                         Generic, MySQL, PostgreSQL, Hive, SQLite, Snowflake, Redshift, \
                         MsSQL, ClickHouse, BigQuery, Ansi."
            )
        })?;

        let expr = DFParser::parse_sql_into_expr_with_dialect(sql, dialect.as_ref())?;

        Ok(expr)
    }

    /// Resolve all table references in the SQL statement. Does not include CTE references.
    ///
    /// See [`datafusion_catalog::resolve_table_references`] for more information.
    ///
    /// [`datafusion_catalog::resolve_table_references`]: datafusion_catalog::resolve_table_references
    pub fn resolve_table_references(
        &self,
        statement: &Statement,
    ) -> datafusion_common::Result<Vec<TableReference>> {
        let enable_ident_normalization =
            self.config.options().sql_parser.enable_ident_normalization;
        let (table_refs, _) = datafusion_catalog::resolve_table_references(
            statement,
            enable_ident_normalization,
        )?;
        Ok(table_refs)
    }

    /// Convert an AST Statement into a LogicalPlan
    pub async fn statement_to_plan(
        &self,
        statement: Statement,
    ) -> datafusion_common::Result<LogicalPlan> {
        let references = self.resolve_table_references(&statement)?;

        let mut provider = SessionContextProvider {
            state: self,
            tables: HashMap::with_capacity(references.len()),
        };

        for reference in references {
            let resolved = self.resolve_table_ref(reference);
            if let Entry::Vacant(v) = provider.tables.entry(resolved) {
                let resolved = v.key();
                if let Ok(schema) = self.schema_for_ref(resolved.clone()) {
                    if let Some(table) = schema.table(&resolved.table).await? {
                        v.insert(provider_as_source(table));
                    }
                }
            }
        }

        let query = SqlToRel::new_with_options(&provider, self.get_parser_options());
        query.statement_to_plan(statement)
    }

    fn get_parser_options(&self) -> ParserOptions {
        let sql_parser_options = &self.config.options().sql_parser;

        ParserOptions {
            parse_float_as_decimal: sql_parser_options.parse_float_as_decimal,
            enable_ident_normalization: sql_parser_options.enable_ident_normalization,
            enable_options_value_normalization: sql_parser_options
                .enable_options_value_normalization,
            support_varchar_with_length: sql_parser_options.support_varchar_with_length,
            collect_spans: sql_parser_options.collect_spans,
        }
    }

    /// Creates a [`LogicalPlan`] from the provided SQL string. This
    /// interface will plan any SQL DataFusion supports, including DML
    /// like `CREATE TABLE`, and `COPY` (which can write to local
    /// files.
    ///
    /// See [`SessionContext::sql`] and
    /// [`SessionContext::sql_with_options`] for a higher-level
    /// interface that handles DDL and verification of allowed
    /// statements.
    ///
    /// [`SessionContext::sql`]: crate::execution::context::SessionContext::sql
    /// [`SessionContext::sql_with_options`]: crate::execution::context::SessionContext::sql_with_options
    pub async fn create_logical_plan(
        &self,
        sql: &str,
    ) -> datafusion_common::Result<LogicalPlan> {
        let dialect = self.config.options().sql_parser.dialect.as_str();
        let statement = self.sql_to_statement(sql, dialect)?;
        let plan = self.statement_to_plan(statement).await?;
        Ok(plan)
    }

    /// Creates a datafusion style AST [`Expr`] from a SQL string.
    ///
    /// See example on  [SessionContext::parse_sql_expr](crate::execution::context::SessionContext::parse_sql_expr)
    pub fn create_logical_expr(
        &self,
        sql: &str,
        df_schema: &DFSchema,
    ) -> datafusion_common::Result<Expr> {
        let dialect = self.config.options().sql_parser.dialect.as_str();

        let sql_expr = self.sql_to_expr_with_alias(sql, dialect)?;

        let provider = SessionContextProvider {
            state: self,
            tables: HashMap::new(),
        };

        let query = SqlToRel::new_with_options(&provider, self.get_parser_options());
        query.sql_to_expr_with_alias(sql_expr, df_schema, &mut PlannerContext::new())
    }

    /// Returns the [`Analyzer`] for this session
    pub fn analyzer(&self) -> &Analyzer {
        &self.analyzer
    }

    /// Returns the [`Optimizer`] for this session
    pub fn optimizer(&self) -> &Optimizer {
        &self.optimizer
    }

    /// Returns the [`QueryPlanner`] for this session
    pub fn query_planner(&self) -> &Arc<dyn QueryPlanner + Send + Sync> {
        &self.query_planner
    }

    /// Optimizes the logical plan by applying optimizer rules.
    pub fn optimize(&self, plan: &LogicalPlan) -> datafusion_common::Result<LogicalPlan> {
        if let LogicalPlan::Explain(e) = plan {
            let mut stringified_plans = e.stringified_plans.clone();

            // analyze & capture output of each rule
            let analyzer_result = self.analyzer.execute_and_check(
                e.plan.as_ref().clone(),
                self.options(),
                |analyzed_plan, analyzer| {
                    let analyzer_name = analyzer.name().to_string();
                    let plan_type = PlanType::AnalyzedLogicalPlan { analyzer_name };
                    stringified_plans.push(analyzed_plan.to_stringified(plan_type));
                },
            );
            let analyzed_plan = match analyzer_result {
                Ok(plan) => plan,
                Err(DataFusionError::Context(analyzer_name, err)) => {
                    let plan_type = PlanType::AnalyzedLogicalPlan { analyzer_name };
                    stringified_plans
                        .push(StringifiedPlan::new(plan_type, err.to_string()));

                    return Ok(LogicalPlan::Explain(Explain {
                        verbose: e.verbose,
                        plan: Arc::clone(&e.plan),
                        stringified_plans,
                        schema: Arc::clone(&e.schema),
                        logical_optimization_succeeded: false,
                    }));
                }
                Err(e) => return Err(e),
            };

            // to delineate the analyzer & optimizer phases in explain output
            stringified_plans
                .push(analyzed_plan.to_stringified(PlanType::FinalAnalyzedLogicalPlan));

            // optimize the child plan, capturing the output of each optimizer
            let optimized_plan = self.optimizer.optimize(
                analyzed_plan,
                self,
                |optimized_plan, optimizer| {
                    let optimizer_name = optimizer.name().to_string();
                    let plan_type = PlanType::OptimizedLogicalPlan { optimizer_name };
                    stringified_plans.push(optimized_plan.to_stringified(plan_type));
                },
            );
            let (plan, logical_optimization_succeeded) = match optimized_plan {
                Ok(plan) => (Arc::new(plan), true),
                Err(DataFusionError::Context(optimizer_name, err)) => {
                    let plan_type = PlanType::OptimizedLogicalPlan { optimizer_name };
                    stringified_plans
                        .push(StringifiedPlan::new(plan_type, err.to_string()));
                    (Arc::clone(&e.plan), false)
                }
                Err(e) => return Err(e),
            };

            Ok(LogicalPlan::Explain(Explain {
                verbose: e.verbose,
                plan,
                stringified_plans,
                schema: Arc::clone(&e.schema),
                logical_optimization_succeeded,
            }))
        } else {
            let analyzed_plan = self.analyzer.execute_and_check(
                plan.clone(),
                self.options(),
                |_, _| {},
            )?;
            self.optimizer.optimize(analyzed_plan, self, |_, _| {})
        }
    }

    /// Creates a physical [`ExecutionPlan`] plan from a [`LogicalPlan`].
    ///
    /// Note: this first calls [`Self::optimize`] on the provided
    /// plan.
    ///
    /// This function will error for [`LogicalPlan`]s such as catalog DDL like
    /// `CREATE TABLE`, which do not have corresponding physical plans and must
    /// be handled by another layer, typically [`SessionContext`].
    ///
    /// [`SessionContext`]: crate::execution::context::SessionContext
    pub async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let logical_plan = self.optimize(logical_plan)?;
        self.query_planner
            .create_physical_plan(&logical_plan, self)
            .await
    }

    /// Create a [`PhysicalExpr`] from an [`Expr`] after applying type
    /// coercion, and function rewrites.
    ///
    /// Note: The expression is not [simplified] or otherwise optimized:
    /// `a = 1 + 2` will not be simplified to `a = 3` as this is a more involved process.
    /// See the [expr_api] example for how to simplify expressions.
    ///
    /// # See Also:
    /// * [`SessionContext::create_physical_expr`] for a higher-level API
    /// * [`create_physical_expr`] for a lower-level API
    ///
    /// [simplified]: datafusion_optimizer::simplify_expressions
    /// [expr_api]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/expr_api.rs
    /// [`SessionContext::create_physical_expr`]: crate::execution::context::SessionContext::create_physical_expr
    pub fn create_physical_expr(
        &self,
        expr: Expr,
        df_schema: &DFSchema,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        let simplifier =
            ExprSimplifier::new(SessionSimplifyProvider::new(self, df_schema));
        // apply type coercion here to ensure types match
        let mut expr = simplifier.coerce(expr, df_schema)?;

        // rewrite Exprs to functions if necessary
        let config_options = self.config_options();
        for rewrite in self.analyzer.function_rewrites() {
            expr = expr
                .transform_up(|expr| rewrite.rewrite(expr, df_schema, config_options))?
                .data;
        }
        create_physical_expr(&expr, df_schema, self.execution_props())
    }

    /// Return the session ID
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Return the runtime env
    pub fn runtime_env(&self) -> &Arc<RuntimeEnv> {
        &self.runtime_env
    }

    /// Return the execution properties
    pub fn execution_props(&self) -> &ExecutionProps {
        &self.execution_props
    }

    /// Return mutable execution properties
    pub fn execution_props_mut(&mut self) -> &mut ExecutionProps {
        &mut self.execution_props
    }

    /// Return the [`SessionConfig`]
    pub fn config(&self) -> &SessionConfig {
        &self.config
    }

    /// Return the mutable [`SessionConfig`].
    pub fn config_mut(&mut self) -> &mut SessionConfig {
        &mut self.config
    }

    /// Return the logical optimizers
    pub fn optimizers(&self) -> &[Arc<dyn OptimizerRule + Send + Sync>] {
        &self.optimizer.rules
    }

    /// Return the physical optimizers
    pub fn physical_optimizers(&self) -> &[Arc<dyn PhysicalOptimizerRule + Send + Sync>] {
        &self.physical_optimizers.rules
    }

    /// return the configuration options
    pub fn config_options(&self) -> &ConfigOptions {
        self.config.options()
    }

    /// return the TableOptions options with its extensions
    pub fn default_table_options(&self) -> TableOptions {
        self.table_options
            .combine_with_session_config(self.config_options())
    }

    /// Return the table options
    pub fn table_options(&self) -> &TableOptions {
        &self.table_options
    }

    /// Return mutable table options
    pub fn table_options_mut(&mut self) -> &mut TableOptions {
        &mut self.table_options
    }

    /// Registers a [`ConfigExtension`] as a table option extension that can be
    /// referenced from SQL statements executed against this context.
    pub fn register_table_options_extension<T: ConfigExtension>(&mut self, extension: T) {
        self.table_options.extensions.insert(extension)
    }

    /// Adds or updates a [FileFormatFactory] which can be used with COPY TO or
    /// CREATE EXTERNAL TABLE statements for reading and writing files of custom
    /// formats.
    pub fn register_file_format(
        &mut self,
        file_format: Arc<dyn FileFormatFactory>,
        overwrite: bool,
    ) -> Result<(), DataFusionError> {
        let ext = file_format.get_ext().to_lowercase();
        match (self.file_formats.entry(ext.clone()), overwrite){
            (Entry::Vacant(e), _) => {e.insert(file_format);},
            (Entry::Occupied(mut e), true)  => {e.insert(file_format);},
            (Entry::Occupied(_), false) => return config_err!("File type already registered for extension {ext}. Set overwrite to true to replace this extension."),
        };
        Ok(())
    }

    /// Retrieves a [FileFormatFactory] based on file extension which has been registered
    /// via SessionContext::register_file_format. Extensions are not case sensitive.
    pub fn get_file_format_factory(
        &self,
        ext: &str,
    ) -> Option<Arc<dyn FileFormatFactory>> {
        self.file_formats.get(&ext.to_lowercase()).cloned()
    }

    /// Get a new TaskContext to run in this session
    pub fn task_ctx(&self) -> Arc<TaskContext> {
        Arc::new(TaskContext::from(self))
    }

    /// Return catalog list
    pub fn catalog_list(&self) -> &Arc<dyn CatalogProviderList> {
        &self.catalog_list
    }

    /// set the catalog list
    pub(crate) fn register_catalog_list(
        &mut self,
        catalog_list: Arc<dyn CatalogProviderList>,
    ) {
        self.catalog_list = catalog_list;
    }

    /// Return reference to scalar_functions
    pub fn scalar_functions(&self) -> &HashMap<String, Arc<ScalarUDF>> {
        &self.scalar_functions
    }

    /// Return reference to aggregate_functions
    pub fn aggregate_functions(&self) -> &HashMap<String, Arc<AggregateUDF>> {
        &self.aggregate_functions
    }

    /// Return reference to window functions
    pub fn window_functions(&self) -> &HashMap<String, Arc<WindowUDF>> {
        &self.window_functions
    }

    /// Return reference to table_functions
    pub fn table_functions(&self) -> &HashMap<String, Arc<TableFunction>> {
        &self.table_functions
    }

    /// Return [SerializerRegistry] for extensions
    pub fn serializer_registry(&self) -> &Arc<dyn SerializerRegistry> {
        &self.serializer_registry
    }

    /// Return version of the cargo package that produced this query
    pub fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    /// Register a user defined table function
    pub fn register_udtf(&mut self, name: &str, fun: Arc<dyn TableFunctionImpl>) {
        self.table_functions.insert(
            name.to_owned(),
            Arc::new(TableFunction::new(name.to_owned(), fun)),
        );
    }

    /// Deregister a user defined table function
    pub fn deregister_udtf(
        &mut self,
        name: &str,
    ) -> datafusion_common::Result<Option<Arc<dyn TableFunctionImpl>>> {
        let udtf = self.table_functions.remove(name);
        Ok(udtf.map(|x| Arc::clone(x.function())))
    }

    /// Store the logical plan and the parameter types of a prepared statement.
    pub(crate) fn store_prepared(
        &mut self,
        name: String,
        data_types: Vec<DataType>,
        plan: Arc<LogicalPlan>,
    ) -> datafusion_common::Result<()> {
        match self.prepared_plans.entry(name) {
            Entry::Vacant(e) => {
                e.insert(Arc::new(PreparedPlan { data_types, plan }));
                Ok(())
            }
            Entry::Occupied(e) => {
                exec_err!("Prepared statement '{}' already exists", e.key())
            }
        }
    }

    /// Get the prepared plan with the given name.
    pub(crate) fn get_prepared(&self, name: &str) -> Option<Arc<PreparedPlan>> {
        self.prepared_plans.get(name).map(Arc::clone)
    }

    /// Remove the prepared plan with the given name.
    pub(crate) fn remove_prepared(
        &mut self,
        name: &str,
    ) -> datafusion_common::Result<()> {
        match self.prepared_plans.remove(name) {
            Some(_) => Ok(()),
            None => exec_err!("Prepared statement '{}' does not exist", name),
        }
    }
}

/// A builder to be used for building [`SessionState`]'s. Defaults will
/// be used for all values unless explicitly provided.
///
/// See example on [`SessionState`]
pub struct SessionStateBuilder {
    session_id: Option<String>,
    analyzer: Option<Analyzer>,
    expr_planners: Option<Vec<Arc<dyn ExprPlanner>>>,
    type_planner: Option<Arc<dyn TypePlanner>>,
    optimizer: Option<Optimizer>,
    physical_optimizers: Option<PhysicalOptimizer>,
    query_planner: Option<Arc<dyn QueryPlanner + Send + Sync>>,
    catalog_list: Option<Arc<dyn CatalogProviderList>>,
    table_functions: Option<HashMap<String, Arc<TableFunction>>>,
    scalar_functions: Option<Vec<Arc<ScalarUDF>>>,
    aggregate_functions: Option<Vec<Arc<AggregateUDF>>>,
    window_functions: Option<Vec<Arc<WindowUDF>>>,
    serializer_registry: Option<Arc<dyn SerializerRegistry>>,
    file_formats: Option<Vec<Arc<dyn FileFormatFactory>>>,
    config: Option<SessionConfig>,
    table_options: Option<TableOptions>,
    execution_props: Option<ExecutionProps>,
    table_factories: Option<HashMap<String, Arc<dyn TableProviderFactory>>>,
    runtime_env: Option<Arc<RuntimeEnv>>,
    function_factory: Option<Arc<dyn FunctionFactory>>,
    // fields to support convenience functions
    analyzer_rules: Option<Vec<Arc<dyn AnalyzerRule + Send + Sync>>>,
    optimizer_rules: Option<Vec<Arc<dyn OptimizerRule + Send + Sync>>>,
    physical_optimizer_rules: Option<Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>>,
}

impl SessionStateBuilder {
    /// Returns a new [`SessionStateBuilder`] with no options set.
    pub fn new() -> Self {
        Self {
            session_id: None,
            analyzer: None,
            expr_planners: None,
            type_planner: None,
            optimizer: None,
            physical_optimizers: None,
            query_planner: None,
            catalog_list: None,
            table_functions: None,
            scalar_functions: None,
            aggregate_functions: None,
            window_functions: None,
            serializer_registry: None,
            file_formats: None,
            table_options: None,
            config: None,
            execution_props: None,
            table_factories: None,
            runtime_env: None,
            function_factory: None,
            // fields to support convenience functions
            analyzer_rules: None,
            optimizer_rules: None,
            physical_optimizer_rules: None,
        }
    }

    /// Returns a new [SessionStateBuilder] based on an existing [SessionState]
    /// The session id for the new builder will be unset; all other fields will
    /// be cloned from what is set in the provided session state. If the default
    /// catalog exists in existing session state, the new session state will not
    /// create default catalog and schema.
    pub fn new_from_existing(existing: SessionState) -> Self {
        let default_catalog_exist = existing
            .catalog_list()
            .catalog(&existing.config.options().catalog.default_catalog)
            .is_some();
        // The new `with_create_default_catalog_and_schema` should be false if the default catalog exists
        let create_default_catalog_and_schema = existing
            .config
            .options()
            .catalog
            .create_default_catalog_and_schema
            && !default_catalog_exist;
        let new_config = existing
            .config
            .with_create_default_catalog_and_schema(create_default_catalog_and_schema);
        Self {
            session_id: None,
            analyzer: Some(existing.analyzer),
            expr_planners: Some(existing.expr_planners),
            type_planner: existing.type_planner,
            optimizer: Some(existing.optimizer),
            physical_optimizers: Some(existing.physical_optimizers),
            query_planner: Some(existing.query_planner),
            catalog_list: Some(existing.catalog_list),
            table_functions: Some(existing.table_functions),
            scalar_functions: Some(existing.scalar_functions.into_values().collect_vec()),
            aggregate_functions: Some(
                existing.aggregate_functions.into_values().collect_vec(),
            ),
            window_functions: Some(existing.window_functions.into_values().collect_vec()),
            serializer_registry: Some(existing.serializer_registry),
            file_formats: Some(existing.file_formats.into_values().collect_vec()),
            config: Some(new_config),
            table_options: Some(existing.table_options),
            execution_props: Some(existing.execution_props),
            table_factories: Some(existing.table_factories),
            runtime_env: Some(existing.runtime_env),
            function_factory: existing.function_factory,

            // fields to support convenience functions
            analyzer_rules: None,
            optimizer_rules: None,
            physical_optimizer_rules: None,
        }
    }

    /// Create default builder with defaults for table_factories, file formats, expr_planners and builtin
    /// scalar, aggregate and windows functions.
    pub fn with_default_features(self) -> Self {
        self.with_table_factories(SessionStateDefaults::default_table_factories())
            .with_file_formats(SessionStateDefaults::default_file_formats())
            .with_expr_planners(SessionStateDefaults::default_expr_planners())
            .with_scalar_functions(SessionStateDefaults::default_scalar_functions())
            .with_aggregate_functions(SessionStateDefaults::default_aggregate_functions())
            .with_window_functions(SessionStateDefaults::default_window_functions())
            .with_table_function_list(SessionStateDefaults::default_table_functions())
    }

    /// Set the session id.
    pub fn with_session_id(mut self, session_id: String) -> Self {
        self.session_id = Some(session_id);
        self
    }

    /// Set the [`AnalyzerRule`]s optimizer plan rules.
    pub fn with_analyzer_rules(
        mut self,
        rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
    ) -> Self {
        self.analyzer = Some(Analyzer::with_rules(rules));
        self
    }

    /// Add `analyzer_rule` to the end of the list of
    /// [`AnalyzerRule`]s used to rewrite queries.
    pub fn with_analyzer_rule(
        mut self,
        analyzer_rule: Arc<dyn AnalyzerRule + Send + Sync>,
    ) -> Self {
        let mut rules = self.analyzer_rules.unwrap_or_default();
        rules.push(analyzer_rule);
        self.analyzer_rules = Some(rules);
        self
    }

    /// Set the [`OptimizerRule`]s used to optimize plans.
    pub fn with_optimizer_rules(
        mut self,
        rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
    ) -> Self {
        self.optimizer = Some(Optimizer::with_rules(rules));
        self
    }

    /// Add `optimizer_rule` to the end of the list of
    /// [`OptimizerRule`]s used to rewrite queries.
    pub fn with_optimizer_rule(
        mut self,
        optimizer_rule: Arc<dyn OptimizerRule + Send + Sync>,
    ) -> Self {
        let mut rules = self.optimizer_rules.unwrap_or_default();
        rules.push(optimizer_rule);
        self.optimizer_rules = Some(rules);
        self
    }

    /// Set the [`ExprPlanner`]s used to customize the behavior of the SQL planner.
    pub fn with_expr_planners(
        mut self,
        expr_planners: Vec<Arc<dyn ExprPlanner>>,
    ) -> Self {
        self.expr_planners = Some(expr_planners);
        self
    }

    /// Set the [`TypePlanner`] used to customize the behavior of the SQL planner.
    pub fn with_type_planner(mut self, type_planner: Arc<dyn TypePlanner>) -> Self {
        self.type_planner = Some(type_planner);
        self
    }

    /// Set the [`PhysicalOptimizerRule`]s used to optimize plans.
    pub fn with_physical_optimizer_rules(
        mut self,
        physical_optimizers: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
    ) -> Self {
        self.physical_optimizers =
            Some(PhysicalOptimizer::with_rules(physical_optimizers));
        self
    }

    /// Add `physical_optimizer_rule` to the end of the list of
    /// [`PhysicalOptimizerRule`]s used to rewrite queries.
    pub fn with_physical_optimizer_rule(
        mut self,
        physical_optimizer_rule: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
    ) -> Self {
        let mut rules = self.physical_optimizer_rules.unwrap_or_default();
        rules.push(physical_optimizer_rule);
        self.physical_optimizer_rules = Some(rules);
        self
    }

    /// Set the [`QueryPlanner`]
    pub fn with_query_planner(
        mut self,
        query_planner: Arc<dyn QueryPlanner + Send + Sync>,
    ) -> Self {
        self.query_planner = Some(query_planner);
        self
    }

    /// Set the [`CatalogProviderList`]
    pub fn with_catalog_list(
        mut self,
        catalog_list: Arc<dyn CatalogProviderList>,
    ) -> Self {
        self.catalog_list = Some(catalog_list);
        self
    }

    /// Set the map of [`TableFunction`]s
    pub fn with_table_functions(
        mut self,
        table_functions: HashMap<String, Arc<TableFunction>>,
    ) -> Self {
        self.table_functions = Some(table_functions);
        self
    }

    /// Set the list of [`TableFunction`]s
    pub fn with_table_function_list(
        mut self,
        table_functions: Vec<Arc<TableFunction>>,
    ) -> Self {
        let functions = table_functions
            .into_iter()
            .map(|f| (f.name().to_string(), f))
            .collect();
        self.table_functions = Some(functions);
        self
    }

    /// Set the map of [`ScalarUDF`]s
    pub fn with_scalar_functions(
        mut self,
        scalar_functions: Vec<Arc<ScalarUDF>>,
    ) -> Self {
        self.scalar_functions = Some(scalar_functions);
        self
    }

    /// Set the map of [`AggregateUDF`]s
    pub fn with_aggregate_functions(
        mut self,
        aggregate_functions: Vec<Arc<AggregateUDF>>,
    ) -> Self {
        self.aggregate_functions = Some(aggregate_functions);
        self
    }

    /// Set the map of [`WindowUDF`]s
    pub fn with_window_functions(
        mut self,
        window_functions: Vec<Arc<WindowUDF>>,
    ) -> Self {
        self.window_functions = Some(window_functions);
        self
    }

    /// Set the [`SerializerRegistry`]
    pub fn with_serializer_registry(
        mut self,
        serializer_registry: Arc<dyn SerializerRegistry>,
    ) -> Self {
        self.serializer_registry = Some(serializer_registry);
        self
    }

    /// Set the map of [`FileFormatFactory`]s
    pub fn with_file_formats(
        mut self,
        file_formats: Vec<Arc<dyn FileFormatFactory>>,
    ) -> Self {
        self.file_formats = Some(file_formats);
        self
    }

    /// Set the [`SessionConfig`]
    pub fn with_config(mut self, config: SessionConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Set the [`TableOptions`]
    pub fn with_table_options(mut self, table_options: TableOptions) -> Self {
        self.table_options = Some(table_options);
        self
    }

    /// Set the [`ExecutionProps`]
    pub fn with_execution_props(mut self, execution_props: ExecutionProps) -> Self {
        self.execution_props = Some(execution_props);
        self
    }

    /// Add a [`TableProviderFactory`] to the map of factories
    pub fn with_table_factory(
        mut self,
        key: String,
        table_factory: Arc<dyn TableProviderFactory>,
    ) -> Self {
        let mut table_factories = self.table_factories.unwrap_or_default();
        table_factories.insert(key, table_factory);
        self.table_factories = Some(table_factories);
        self
    }

    /// Set the map of [`TableProviderFactory`]s
    pub fn with_table_factories(
        mut self,
        table_factories: HashMap<String, Arc<dyn TableProviderFactory>>,
    ) -> Self {
        self.table_factories = Some(table_factories);
        self
    }

    /// Set the [`RuntimeEnv`]
    pub fn with_runtime_env(mut self, runtime_env: Arc<RuntimeEnv>) -> Self {
        self.runtime_env = Some(runtime_env);
        self
    }

    /// Set a [`FunctionFactory`] to handle `CREATE FUNCTION` statements
    pub fn with_function_factory(
        mut self,
        function_factory: Option<Arc<dyn FunctionFactory>>,
    ) -> Self {
        self.function_factory = function_factory;
        self
    }

    /// Register an `ObjectStore` to the [`RuntimeEnv`]. See [`RuntimeEnv::register_object_store`]
    /// for more details.
    ///
    /// Note that this creates a default [`RuntimeEnv`] if  there isn't one passed in already.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::execution::session_state::SessionStateBuilder;
    /// # use datafusion_execution::runtime_env::RuntimeEnv;
    /// # use url::Url;
    /// # use std::sync::Arc;
    /// # let http_store = object_store::local::LocalFileSystem::new();
    /// let url = Url::try_from("file://").unwrap();
    /// let object_store = object_store::local::LocalFileSystem::new();
    /// let state = SessionStateBuilder::new()
    ///     .with_config(SessionConfig::new())  
    ///     .with_object_store(&url, Arc::new(object_store))
    ///     .with_default_features()
    ///     .build();
    /// ```
    pub fn with_object_store(
        mut self,
        url: &Url,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        if self.runtime_env.is_none() {
            self.runtime_env = Some(Arc::new(RuntimeEnv::default()));
        }
        self.runtime_env
            .as_ref()
            .unwrap()
            .register_object_store(url, object_store);
        self
    }

    /// Builds a [`SessionState`] with the current configuration.
    ///
    /// Note that there is an explicit option for enabling catalog and schema defaults
    /// in [SessionConfig::create_default_catalog_and_schema] which if enabled
    /// will be built here.
    pub fn build(self) -> SessionState {
        let Self {
            session_id,
            analyzer,
            expr_planners,
            type_planner,
            optimizer,
            physical_optimizers,
            query_planner,
            catalog_list,
            table_functions,
            scalar_functions,
            aggregate_functions,
            window_functions,
            serializer_registry,
            file_formats,
            table_options,
            config,
            execution_props,
            table_factories,
            runtime_env,
            function_factory,
            analyzer_rules,
            optimizer_rules,
            physical_optimizer_rules,
        } = self;

        let config = config.unwrap_or_default();
        let runtime_env = runtime_env.unwrap_or(Arc::new(RuntimeEnv::default()));

        let mut state = SessionState {
            session_id: session_id.unwrap_or(Uuid::new_v4().to_string()),
            analyzer: analyzer.unwrap_or_default(),
            expr_planners: expr_planners.unwrap_or_default(),
            type_planner,
            optimizer: optimizer.unwrap_or_default(),
            physical_optimizers: physical_optimizers.unwrap_or_default(),
            query_planner: query_planner.unwrap_or(Arc::new(DefaultQueryPlanner {})),
            catalog_list: catalog_list
                .unwrap_or(Arc::new(MemoryCatalogProviderList::new())
                    as Arc<dyn CatalogProviderList>),
            table_functions: table_functions.unwrap_or_default(),
            scalar_functions: HashMap::new(),
            aggregate_functions: HashMap::new(),
            window_functions: HashMap::new(),
            serializer_registry: serializer_registry
                .unwrap_or(Arc::new(EmptySerializerRegistry)),
            file_formats: HashMap::new(),
            table_options: table_options
                .unwrap_or(TableOptions::default_from_session_config(config.options())),
            config,
            execution_props: execution_props.unwrap_or_default(),
            table_factories: table_factories.unwrap_or_default(),
            runtime_env,
            function_factory,
            prepared_plans: HashMap::new(),
        };

        if let Some(file_formats) = file_formats {
            for file_format in file_formats {
                if let Err(e) = state.register_file_format(file_format, false) {
                    info!("Unable to register file format: {e}")
                };
            }
        }

        if let Some(scalar_functions) = scalar_functions {
            scalar_functions.into_iter().for_each(|udf| {
                let existing_udf = state.register_udf(udf);
                if let Ok(Some(existing_udf)) = existing_udf {
                    debug!("Overwrote an existing UDF: {}", existing_udf.name());
                }
            });
        }

        if let Some(aggregate_functions) = aggregate_functions {
            aggregate_functions.into_iter().for_each(|udaf| {
                let existing_udf = state.register_udaf(udaf);
                if let Ok(Some(existing_udf)) = existing_udf {
                    debug!("Overwrote an existing UDF: {}", existing_udf.name());
                }
            });
        }

        if let Some(window_functions) = window_functions {
            window_functions.into_iter().for_each(|udwf| {
                let existing_udf = state.register_udwf(udwf);
                if let Ok(Some(existing_udf)) = existing_udf {
                    debug!("Overwrote an existing UDF: {}", existing_udf.name());
                }
            });
        }

        if state.config.create_default_catalog_and_schema() {
            let default_catalog = SessionStateDefaults::default_catalog(
                &state.config,
                &state.table_factories,
                &state.runtime_env,
            );

            state.catalog_list.register_catalog(
                state.config.options().catalog.default_catalog.clone(),
                Arc::new(default_catalog),
            );
        }

        if let Some(analyzer_rules) = analyzer_rules {
            for analyzer_rule in analyzer_rules {
                state.analyzer.rules.push(analyzer_rule);
            }
        }

        if let Some(optimizer_rules) = optimizer_rules {
            for optimizer_rule in optimizer_rules {
                state.optimizer.rules.push(optimizer_rule);
            }
        }

        if let Some(physical_optimizer_rules) = physical_optimizer_rules {
            for physical_optimizer_rule in physical_optimizer_rules {
                state
                    .physical_optimizers
                    .rules
                    .push(physical_optimizer_rule);
            }
        }

        state
    }

    /// Returns the current session_id value
    pub fn session_id(&self) -> &Option<String> {
        &self.session_id
    }

    /// Returns the current analyzer value
    pub fn analyzer(&mut self) -> &mut Option<Analyzer> {
        &mut self.analyzer
    }

    /// Returns the current expr_planners value
    pub fn expr_planners(&mut self) -> &mut Option<Vec<Arc<dyn ExprPlanner>>> {
        &mut self.expr_planners
    }

    /// Returns the current type_planner value
    pub fn type_planner(&mut self) -> &mut Option<Arc<dyn TypePlanner>> {
        &mut self.type_planner
    }

    /// Returns the current optimizer value
    pub fn optimizer(&mut self) -> &mut Option<Optimizer> {
        &mut self.optimizer
    }

    /// Returns the current physical_optimizers value
    pub fn physical_optimizers(&mut self) -> &mut Option<PhysicalOptimizer> {
        &mut self.physical_optimizers
    }

    /// Returns the current query_planner value
    pub fn query_planner(&mut self) -> &mut Option<Arc<dyn QueryPlanner + Send + Sync>> {
        &mut self.query_planner
    }

    /// Returns the current catalog_list value
    pub fn catalog_list(&mut self) -> &mut Option<Arc<dyn CatalogProviderList>> {
        &mut self.catalog_list
    }

    /// Returns the current table_functions value
    pub fn table_functions(
        &mut self,
    ) -> &mut Option<HashMap<String, Arc<TableFunction>>> {
        &mut self.table_functions
    }

    /// Returns the current scalar_functions value
    pub fn scalar_functions(&mut self) -> &mut Option<Vec<Arc<ScalarUDF>>> {
        &mut self.scalar_functions
    }

    /// Returns the current aggregate_functions value
    pub fn aggregate_functions(&mut self) -> &mut Option<Vec<Arc<AggregateUDF>>> {
        &mut self.aggregate_functions
    }

    /// Returns the current window_functions value
    pub fn window_functions(&mut self) -> &mut Option<Vec<Arc<WindowUDF>>> {
        &mut self.window_functions
    }

    /// Returns the current serializer_registry value
    pub fn serializer_registry(&mut self) -> &mut Option<Arc<dyn SerializerRegistry>> {
        &mut self.serializer_registry
    }

    /// Returns the current file_formats value
    pub fn file_formats(&mut self) -> &mut Option<Vec<Arc<dyn FileFormatFactory>>> {
        &mut self.file_formats
    }

    /// Returns the current session_config value
    pub fn config(&mut self) -> &mut Option<SessionConfig> {
        &mut self.config
    }

    /// Returns the current table_options value
    pub fn table_options(&mut self) -> &mut Option<TableOptions> {
        &mut self.table_options
    }

    /// Returns the current execution_props value
    pub fn execution_props(&mut self) -> &mut Option<ExecutionProps> {
        &mut self.execution_props
    }

    /// Returns the current table_factories value
    pub fn table_factories(
        &mut self,
    ) -> &mut Option<HashMap<String, Arc<dyn TableProviderFactory>>> {
        &mut self.table_factories
    }

    /// Returns the current runtime_env value
    pub fn runtime_env(&mut self) -> &mut Option<Arc<RuntimeEnv>> {
        &mut self.runtime_env
    }

    /// Returns the current function_factory value
    pub fn function_factory(&mut self) -> &mut Option<Arc<dyn FunctionFactory>> {
        &mut self.function_factory
    }

    /// Returns the current analyzer_rules value
    pub fn analyzer_rules(
        &mut self,
    ) -> &mut Option<Vec<Arc<dyn AnalyzerRule + Send + Sync>>> {
        &mut self.analyzer_rules
    }

    /// Returns the current optimizer_rules value
    pub fn optimizer_rules(
        &mut self,
    ) -> &mut Option<Vec<Arc<dyn OptimizerRule + Send + Sync>>> {
        &mut self.optimizer_rules
    }

    /// Returns the current physical_optimizer_rules value
    pub fn physical_optimizer_rules(
        &mut self,
    ) -> &mut Option<Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>> {
        &mut self.physical_optimizer_rules
    }
}

impl Debug for SessionStateBuilder {
    /// Prefer having short fields at the top and long vector fields near the end
    /// Group fields by
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionStateBuilder")
            .field("session_id", &self.session_id)
            .field("config", &self.config)
            .field("runtime_env", &self.runtime_env)
            .field("catalog_list", &self.catalog_list)
            .field("serializer_registry", &self.serializer_registry)
            .field("file_formats", &self.file_formats)
            .field("execution_props", &self.execution_props)
            .field("table_options", &self.table_options)
            .field("table_factories", &self.table_factories)
            .field("function_factory", &self.function_factory)
            .field("expr_planners", &self.expr_planners)
            .field("type_planner", &self.type_planner)
            .field("query_planners", &self.query_planner)
            .field("analyzer_rules", &self.analyzer_rules)
            .field("analyzer", &self.analyzer)
            .field("optimizer_rules", &self.optimizer_rules)
            .field("optimizer", &self.optimizer)
            .field("physical_optimizer_rules", &self.physical_optimizer_rules)
            .field("physical_optimizers", &self.physical_optimizers)
            .field("table_functions", &self.table_functions)
            .field("scalar_functions", &self.scalar_functions)
            .field("aggregate_functions", &self.aggregate_functions)
            .field("window_functions", &self.window_functions)
            .finish()
    }
}

impl Default for SessionStateBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl From<SessionState> for SessionStateBuilder {
    fn from(state: SessionState) -> Self {
        SessionStateBuilder::new_from_existing(state)
    }
}

/// Adapter that implements the [`ContextProvider`] trait for a [`SessionState`]
///
/// This is used so the SQL planner can access the state of the session without
/// having a direct dependency on the [`SessionState`] struct (and core crate)
struct SessionContextProvider<'a> {
    state: &'a SessionState,
    tables: HashMap<ResolvedTableReference, Arc<dyn TableSource>>,
}

impl ContextProvider for SessionContextProvider<'_> {
    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &self.state.expr_planners
    }

    fn get_type_planner(&self) -> Option<Arc<dyn TypePlanner>> {
        if let Some(type_planner) = &self.state.type_planner {
            Some(Arc::clone(type_planner))
        } else {
            None
        }
    }

    fn get_table_source(
        &self,
        name: TableReference,
    ) -> datafusion_common::Result<Arc<dyn TableSource>> {
        let name = self.state.resolve_table_ref(name);
        self.tables
            .get(&name)
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("table '{name}' not found"))
    }

    fn get_table_function_source(
        &self,
        name: &str,
        args: Vec<Expr>,
    ) -> datafusion_common::Result<Arc<dyn TableSource>> {
        let tbl_func = self
            .state
            .table_functions
            .get(name)
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("table function '{name}' not found"))?;
        let provider = tbl_func.create_table_provider(&args)?;

        Ok(provider_as_source(provider))
    }

    /// Create a new CTE work table for a recursive CTE logical plan
    /// This table will be used in conjunction with a Worktable physical plan
    /// to read and write each iteration of a recursive CTE
    fn create_cte_work_table(
        &self,
        name: &str,
        schema: SchemaRef,
    ) -> datafusion_common::Result<Arc<dyn TableSource>> {
        let table = Arc::new(CteWorkTable::new(name, schema));
        Ok(provider_as_source(table))
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.state.scalar_functions().get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.state.aggregate_functions().get(name).cloned()
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.state.window_functions().get(name).cloned()
    }

    fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType> {
        if variable_names.is_empty() {
            return None;
        }

        let provider_type = if is_system_variables(variable_names) {
            VarType::System
        } else {
            VarType::UserDefined
        };

        self.state
            .execution_props
            .var_providers
            .as_ref()
            .and_then(|provider| provider.get(&provider_type)?.get_type(variable_names))
    }

    fn options(&self) -> &ConfigOptions {
        self.state.config_options()
    }

    fn udf_names(&self) -> Vec<String> {
        self.state.scalar_functions().keys().cloned().collect()
    }

    fn udaf_names(&self) -> Vec<String> {
        self.state.aggregate_functions().keys().cloned().collect()
    }

    fn udwf_names(&self) -> Vec<String> {
        self.state.window_functions().keys().cloned().collect()
    }

    fn get_file_type(&self, ext: &str) -> datafusion_common::Result<Arc<dyn FileType>> {
        self.state
            .file_formats
            .get(&ext.to_lowercase())
            .ok_or(plan_datafusion_err!(
                "There is no registered file format with ext {ext}"
            ))
            .map(|file_type| format_as_file_type(Arc::clone(file_type)))
    }
}

impl FunctionRegistry for SessionState {
    fn udfs(&self) -> HashSet<String> {
        self.scalar_functions.keys().cloned().collect()
    }

    fn udf(&self, name: &str) -> datafusion_common::Result<Arc<ScalarUDF>> {
        let result = self.scalar_functions.get(name);

        result.cloned().ok_or_else(|| {
            plan_datafusion_err!("There is no UDF named \"{name}\" in the registry")
        })
    }

    fn udaf(&self, name: &str) -> datafusion_common::Result<Arc<AggregateUDF>> {
        let result = self.aggregate_functions.get(name);

        result.cloned().ok_or_else(|| {
            plan_datafusion_err!("There is no UDAF named \"{name}\" in the registry")
        })
    }

    fn udwf(&self, name: &str) -> datafusion_common::Result<Arc<WindowUDF>> {
        let result = self.window_functions.get(name);

        result.cloned().ok_or_else(|| {
            plan_datafusion_err!("There is no UDWF named \"{name}\" in the registry")
        })
    }

    fn register_udf(
        &mut self,
        udf: Arc<ScalarUDF>,
    ) -> datafusion_common::Result<Option<Arc<ScalarUDF>>> {
        udf.aliases().iter().for_each(|alias| {
            self.scalar_functions
                .insert(alias.clone(), Arc::clone(&udf));
        });
        Ok(self.scalar_functions.insert(udf.name().into(), udf))
    }

    fn register_udaf(
        &mut self,
        udaf: Arc<AggregateUDF>,
    ) -> datafusion_common::Result<Option<Arc<AggregateUDF>>> {
        udaf.aliases().iter().for_each(|alias| {
            self.aggregate_functions
                .insert(alias.clone(), Arc::clone(&udaf));
        });
        Ok(self.aggregate_functions.insert(udaf.name().into(), udaf))
    }

    fn register_udwf(
        &mut self,
        udwf: Arc<WindowUDF>,
    ) -> datafusion_common::Result<Option<Arc<WindowUDF>>> {
        udwf.aliases().iter().for_each(|alias| {
            self.window_functions
                .insert(alias.clone(), Arc::clone(&udwf));
        });
        Ok(self.window_functions.insert(udwf.name().into(), udwf))
    }

    fn deregister_udf(
        &mut self,
        name: &str,
    ) -> datafusion_common::Result<Option<Arc<ScalarUDF>>> {
        let udf = self.scalar_functions.remove(name);
        if let Some(udf) = &udf {
            for alias in udf.aliases() {
                self.scalar_functions.remove(alias);
            }
        }
        Ok(udf)
    }

    fn deregister_udaf(
        &mut self,
        name: &str,
    ) -> datafusion_common::Result<Option<Arc<AggregateUDF>>> {
        let udaf = self.aggregate_functions.remove(name);
        if let Some(udaf) = &udaf {
            for alias in udaf.aliases() {
                self.aggregate_functions.remove(alias);
            }
        }
        Ok(udaf)
    }

    fn deregister_udwf(
        &mut self,
        name: &str,
    ) -> datafusion_common::Result<Option<Arc<WindowUDF>>> {
        let udwf = self.window_functions.remove(name);
        if let Some(udwf) = &udwf {
            for alias in udwf.aliases() {
                self.window_functions.remove(alias);
            }
        }
        Ok(udwf)
    }

    fn register_function_rewrite(
        &mut self,
        rewrite: Arc<dyn FunctionRewrite + Send + Sync>,
    ) -> datafusion_common::Result<()> {
        self.analyzer.add_function_rewrite(rewrite);
        Ok(())
    }

    fn expr_planners(&self) -> Vec<Arc<dyn ExprPlanner>> {
        self.expr_planners.clone()
    }

    fn register_expr_planner(
        &mut self,
        expr_planner: Arc<dyn ExprPlanner>,
    ) -> datafusion_common::Result<()> {
        self.expr_planners.push(expr_planner);
        Ok(())
    }
}

impl OptimizerConfig for SessionState {
    fn query_execution_start_time(&self) -> DateTime<Utc> {
        self.execution_props.query_execution_start_time
    }

    fn alias_generator(&self) -> &Arc<AliasGenerator> {
        &self.execution_props.alias_generator
    }

    fn options(&self) -> &ConfigOptions {
        self.config_options()
    }

    fn function_registry(&self) -> Option<&dyn FunctionRegistry> {
        Some(self)
    }
}

/// Create a new task context instance from SessionState
impl From<&SessionState> for TaskContext {
    fn from(state: &SessionState) -> Self {
        let task_id = None;
        TaskContext::new(
            task_id,
            state.session_id.clone(),
            state.config.clone(),
            state.scalar_functions.clone(),
            state.aggregate_functions.clone(),
            state.window_functions.clone(),
            Arc::clone(&state.runtime_env),
        )
    }
}

/// The query planner used if no user defined planner is provided
#[derive(Debug)]
struct DefaultQueryPlanner {}

#[async_trait]
impl QueryPlanner for DefaultQueryPlanner {
    /// Given a `LogicalPlan`, create an [`ExecutionPlan`] suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let planner = DefaultPhysicalPlanner::default();
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

struct SessionSimplifyProvider<'a> {
    state: &'a SessionState,
    df_schema: &'a DFSchema,
}

impl<'a> SessionSimplifyProvider<'a> {
    fn new(state: &'a SessionState, df_schema: &'a DFSchema) -> Self {
        Self { state, df_schema }
    }
}

impl SimplifyInfo for SessionSimplifyProvider<'_> {
    fn is_boolean_type(&self, expr: &Expr) -> datafusion_common::Result<bool> {
        Ok(expr.get_type(self.df_schema)? == DataType::Boolean)
    }

    fn nullable(&self, expr: &Expr) -> datafusion_common::Result<bool> {
        expr.nullable(self.df_schema)
    }

    fn execution_props(&self) -> &ExecutionProps {
        self.state.execution_props()
    }

    fn get_data_type(&self, expr: &Expr) -> datafusion_common::Result<DataType> {
        expr.get_type(self.df_schema)
    }
}

#[derive(Debug)]
pub(crate) struct PreparedPlan {
    /// Data types of the parameters
    pub(crate) data_types: Vec<DataType>,
    /// The prepared logical plan
    pub(crate) plan: Arc<LogicalPlan>,
}

#[cfg(test)]
mod tests {
    use super::{SessionContextProvider, SessionStateBuilder};
    use crate::datasource::MemTable;
    use crate::execution::context::SessionState;
    use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_catalog::MemoryCatalogProviderList;
    use datafusion_common::DFSchema;
    use datafusion_common::Result;
    use datafusion_execution::config::SessionConfig;
    use datafusion_expr::Expr;
    use datafusion_optimizer::optimizer::OptimizerRule;
    use datafusion_optimizer::Optimizer;
    use datafusion_sql::planner::{PlannerContext, SqlToRel};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_session_state_with_default_features() {
        // test array planners with and without builtin planners
        fn sql_to_expr(state: &SessionState) -> Result<Expr> {
            let provider = SessionContextProvider {
                state,
                tables: HashMap::new(),
            };

            let sql = "[1,2,3]";
            let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
            let df_schema = DFSchema::try_from(schema)?;
            let dialect = state.config.options().sql_parser.dialect.as_str();
            let sql_expr = state.sql_to_expr(sql, dialect)?;

            let query = SqlToRel::new_with_options(&provider, state.get_parser_options());
            query.sql_to_expr(sql_expr, &df_schema, &mut PlannerContext::new())
        }

        let state = SessionStateBuilder::new().with_default_features().build();

        assert!(sql_to_expr(&state).is_ok());

        // if no builtin planners exist, you should register your own, otherwise returns error
        let state = SessionStateBuilder::new().build();

        assert!(sql_to_expr(&state).is_err())
    }

    #[test]
    fn test_from_existing() -> Result<()> {
        fn employee_batch() -> RecordBatch {
            let name: ArrayRef =
                Arc::new(StringArray::from_iter_values(["Andy", "Andrew"]));
            let age: ArrayRef = Arc::new(Int32Array::from(vec![11, 22]));
            RecordBatch::try_from_iter(vec![("name", name), ("age", age)]).unwrap()
        }
        let batch = employee_batch();
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;

        let session_state = SessionStateBuilder::new()
            .with_catalog_list(Arc::new(MemoryCatalogProviderList::new()))
            .build();
        let table_ref = session_state.resolve_table_ref("employee").to_string();
        session_state
            .schema_for_ref(&table_ref)?
            .register_table("employee".to_string(), Arc::new(table))?;

        let default_catalog = session_state
            .config
            .options()
            .catalog
            .default_catalog
            .clone();
        let default_schema = session_state
            .config
            .options()
            .catalog
            .default_schema
            .clone();
        let is_exist = session_state
            .catalog_list()
            .catalog(default_catalog.as_str())
            .unwrap()
            .schema(default_schema.as_str())
            .unwrap()
            .table_exist("employee");
        assert!(is_exist);
        let new_state = SessionStateBuilder::new_from_existing(session_state).build();
        assert!(new_state
            .catalog_list()
            .catalog(default_catalog.as_str())
            .unwrap()
            .schema(default_schema.as_str())
            .unwrap()
            .table_exist("employee"));

        // if `with_create_default_catalog_and_schema` is disabled, the new one shouldn't create default catalog and schema
        let disable_create_default =
            SessionConfig::default().with_create_default_catalog_and_schema(false);
        let without_default_state = SessionStateBuilder::new()
            .with_config(disable_create_default)
            .build();
        assert!(without_default_state
            .catalog_list()
            .catalog(&default_catalog)
            .is_none());
        let new_state =
            SessionStateBuilder::new_from_existing(without_default_state).build();
        assert!(new_state.catalog_list().catalog(&default_catalog).is_none());
        Ok(())
    }

    #[test]
    fn test_session_state_with_optimizer_rules() {
        #[derive(Default, Debug)]
        struct DummyRule {}

        impl OptimizerRule for DummyRule {
            fn name(&self) -> &str {
                "dummy_rule"
            }
        }
        // test building sessions with fresh set of rules
        let state = SessionStateBuilder::new()
            .with_optimizer_rules(vec![Arc::new(DummyRule {})])
            .build();

        assert_eq!(state.optimizers().len(), 1);

        // test adding rules to default recommendations
        let state = SessionStateBuilder::new()
            .with_optimizer_rule(Arc::new(DummyRule {}))
            .build();

        assert_eq!(
            state.optimizers().len(),
            Optimizer::default().rules.len() + 1
        );
    }

    #[test]
    fn test_with_table_factories() -> Result<()> {
        use crate::test_util::TestTableFactory;

        let state = SessionStateBuilder::new().build();
        let table_factories = state.table_factories();
        assert!(table_factories.is_empty());

        let table_factory = Arc::new(TestTableFactory {});
        let state = SessionStateBuilder::new()
            .with_table_factory("employee".to_string(), table_factory)
            .build();
        let table_factories = state.table_factories();
        assert_eq!(table_factories.len(), 1);
        assert!(table_factories.contains_key("employee"));
        Ok(())
    }
}
