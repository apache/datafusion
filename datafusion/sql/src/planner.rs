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

//! SQL Query Planner (produces logical plan from SQL AST)
use log::debug;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::{convert::TryInto, vec};

use arrow_schema::*;
use sqlparser::ast::TimezoneInfo;
use sqlparser::ast::{ArrayAgg, ExactNumberInfo, SetQuantifier};
use sqlparser::ast::{
    BinaryOperator, DataType as SQLDataType, DateTimeField, Expr as SQLExpr, FunctionArg,
    FunctionArgExpr, Ident, Join, JoinConstraint, JoinOperator, ObjectName,
    Offset as SQLOffset, Query, Select, SelectItem, SetExpr, SetOperator,
    ShowCreateObject, ShowStatementFilter, TableAlias, TableFactor, TableWithJoins,
    TrimWhereField, UnaryOperator, Value, Values as SQLValues,
};
use sqlparser::ast::{ColumnDef as SQLColumnDef, ColumnOption};
use sqlparser::ast::{ObjectType, OrderByExpr, Statement};
use sqlparser::parser::ParserError::ParserError;

use datafusion_common::parsers::parse_interval;
use datafusion_common::ToDFSchema;
use datafusion_common::{
    field_not_found, Column, DFSchema, DFSchemaRef, DataFusionError, Result, ScalarValue,
};
use datafusion_common::{OwnedTableReference, TableReference};
use datafusion_expr::expr::{Between, BinaryExpr, Case, Cast, GroupingSet, Like};
use datafusion_expr::expr_rewriter::normalize_col;
use datafusion_expr::expr_rewriter::normalize_col_with_schemas;
use datafusion_expr::logical_plan::builder::{
    project, wrap_projection_for_join_if_necessary,
};
use datafusion_expr::logical_plan::Join as HashJoin;
use datafusion_expr::logical_plan::JoinConstraint as HashJoinConstraint;
use datafusion_expr::logical_plan::{
    Analyze, CreateCatalog, CreateCatalogSchema,
    CreateExternalTable as PlanCreateExternalTable, CreateMemoryTable, CreateView,
    DropTable, DropView, Explain, JoinType, LogicalPlan, LogicalPlanBuilder,
    Partitioning, PlanType, SetVariable, ToStringifiedPlan,
};
use datafusion_expr::logical_plan::{Filter, Prepare, Subquery};
use datafusion_expr::utils::{
    can_hash, check_all_column_from_schema, expand_qualified_wildcard, expand_wildcard,
    expr_as_column_expr, expr_to_columns, find_aggregate_exprs, find_column_exprs,
    find_window_exprs, COUNT_STAR_EXPANSION,
};
use datafusion_expr::Expr::Alias;
use datafusion_expr::{
    cast, col, lit, AggregateFunction, AggregateUDF, Expr, ExprSchemable,
    GetIndexedField, Operator, ScalarUDF, SubqueryAlias, WindowFrame, WindowFrameUnits,
};
use datafusion_expr::{
    window_function::WindowFunction, BuiltinScalarFunction, TableSource,
};

use crate::parser::{CreateExternalTable, DescribeTable, Statement as DFStatement};
use crate::utils::{
    make_decimal_type, normalize_ident, normalize_ident_owned, resolve_columns,
};

use super::{
    parser::DFParser,
    utils::{
        check_columns_satisfy_exprs, extract_aliases, rebase_expr,
        resolve_aliases_to_exprs, resolve_positions_to_exprs,
    },
};

/// The ContextProvider trait allows the query planner to obtain meta-data about tables and
/// functions referenced in SQL statements
pub trait ContextProvider {
    /// Getter for a datasource
    fn get_table_provider(&self, name: TableReference) -> Result<Arc<dyn TableSource>>;
    /// Getter for a UDF description
    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>>;
    /// Getter for a UDAF description
    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>>;
    /// Getter for system/user-defined variable type
    fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType>;
    /// Getter for config_options
    fn get_config_option(&self, variable: &str) -> Option<ScalarValue>;
}

/// SQL parser options
#[derive(Debug, Default)]
pub struct ParserOptions {
    parse_float_as_decimal: bool,
}

#[derive(Debug, Clone)]
/// Struct to store Common Table Expression (CTE) provided with WITH clause and
/// Parameter Data Types provided with PREPARE statement
pub struct PlannerContext {
    /// Data type provided with prepare statement
    pub prepare_param_data_types: Vec<DataType>,
    /// Map of CTE name to logical plan of the WITH clause
    pub ctes: HashMap<String, LogicalPlan>,
}

impl Default for PlannerContext {
    fn default() -> Self {
        Self::new()
    }
}

impl PlannerContext {
    /// Create an empty PlannerContext
    pub fn new() -> Self {
        Self {
            prepare_param_data_types: vec![],
            ctes: HashMap::new(),
        }
    }

    /// Create a new PlannerContext with provided prepare_param_data_types
    pub fn new_with_prepare_param_data_types(
        prepare_param_data_types: Vec<DataType>,
    ) -> Self {
        Self {
            prepare_param_data_types,
            ctes: HashMap::new(),
        }
    }
}

/// SQL query planner
pub struct SqlToRel<'a, S: ContextProvider> {
    schema_provider: &'a S,
    options: ParserOptions,
}

fn plan_key(key: SQLExpr) -> Result<ScalarValue> {
    let scalar = match key {
        SQLExpr::Value(Value::Number(s, _)) => ScalarValue::Int64(Some(
            s.parse()
                .map_err(|_| ParserError(format!("Cannot parse {} as i64.", s)))?,
        )),
        SQLExpr::Value(Value::SingleQuotedString(s) | Value::DoubleQuotedString(s)) => {
            ScalarValue::Utf8(Some(s))
        }
        _ => {
            return Err(DataFusionError::SQL(ParserError(format!(
                "Unsuported index key expression: {:?}",
                key
            ))));
        }
    };

    Ok(scalar)
}

fn plan_indexed(expr: Expr, mut keys: Vec<SQLExpr>) -> Result<Expr> {
    let key = keys.pop().ok_or_else(|| {
        ParserError("Internal error: Missing index key expression".to_string())
    })?;

    let expr = if !keys.is_empty() {
        plan_indexed(expr, keys)?
    } else {
        expr
    };

    Ok(Expr::GetIndexedField(GetIndexedField::new(
        Box::new(expr),
        plan_key(key)?,
    )))
}

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    /// Create a new query planner
    pub fn new(schema_provider: &'a S) -> Self {
        Self::new_with_options(schema_provider, ParserOptions::default())
    }

    /// Create a new query planner
    pub fn new_with_options(schema_provider: &'a S, options: ParserOptions) -> Self {
        SqlToRel {
            schema_provider,
            options,
        }
    }

    /// Generate a logical plan from an DataFusion SQL statement
    pub fn statement_to_plan(&self, statement: DFStatement) -> Result<LogicalPlan> {
        match statement {
            DFStatement::CreateExternalTable(s) => self.external_table_to_plan(s),
            DFStatement::Statement(s) => self.sql_statement_to_plan(*s),
            DFStatement::DescribeTable(s) => self.describe_table_to_plan(s),
        }
    }

    /// Generate a logical plan from an SQL statement
    pub fn sql_statement_to_plan(&self, statement: Statement) -> Result<LogicalPlan> {
        self.sql_statement_to_plan_with_context(statement, &mut PlannerContext::new())
    }

    /// Generate a logical plan from an SQL statement
    pub fn sql_statement_to_plan_with_context(
        &self,
        statement: Statement,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let sql = Some(statement.to_string());
        match statement {
            Statement::Explain {
                verbose,
                statement,
                analyze,
                format: _,
                describe_alias: _,
                ..
            } => self.explain_statement_to_plan(verbose, analyze, *statement),
            Statement::Query(query) => self.query_to_plan(*query, planner_context),
            Statement::ShowVariable { variable } => self.show_variable_to_plan(&variable),
            Statement::SetVariable {
                local,
                hivevar,
                variable,
                value,
            } => self.set_variable_to_plan(local, hivevar, &variable, value),

            Statement::CreateTable {
                query: Some(query),
                name,
                columns,
                constraints,
                table_properties,
                with_options,
                if_not_exists,
                or_replace,
                ..
            } if constraints.is_empty()
                && table_properties.is_empty()
                && with_options.is_empty() =>
            {
                let plan = self.query_to_plan(*query, planner_context)?;
                let input_schema = plan.schema();

                let plan = if !columns.is_empty() {
                    let schema = self.build_schema(columns)?.to_dfschema_ref()?;
                    if schema.fields().len() != input_schema.fields().len() {
                        return Err(DataFusionError::Plan(format!(
                            "Mismatch: {} columns specified, but result has {} columns",
                            schema.fields().len(),
                            input_schema.fields().len()
                        )));
                    }
                    let input_fields = input_schema.fields();
                    let project_exprs = schema
                        .fields()
                        .iter()
                        .zip(input_fields)
                        .map(|(field, input_field)| {
                            cast(col(input_field.name()), field.data_type().clone())
                                .alias(field.name())
                        })
                        .collect::<Vec<_>>();
                    LogicalPlanBuilder::from(plan.clone())
                        .project(project_exprs)?
                        .build()?
                } else {
                    plan
                };

                Ok(LogicalPlan::CreateMemoryTable(CreateMemoryTable {
                    name: object_name_to_table_reference(name)?,
                    input: Arc::new(plan),
                    if_not_exists,
                    or_replace,
                }))
            }
            Statement::CreateView {
                or_replace,
                name,
                columns,
                query,
                with_options,
                ..
            } if with_options.is_empty() => {
                let mut plan = self.query_to_plan(*query, &mut PlannerContext::new())?;
                plan = self.apply_expr_alias(plan, &columns)?;

                Ok(LogicalPlan::CreateView(CreateView {
                    name: object_name_to_table_reference(name)?,
                    input: Arc::new(plan),
                    or_replace,
                    definition: sql,
                }))
            }
            Statement::CreateTable { .. } => Err(DataFusionError::NotImplemented(
                "Only `CREATE TABLE table_name AS SELECT ...` statement is supported"
                    .to_string(),
            )),
            Statement::ShowCreate { obj_type, obj_name } => match obj_type {
                ShowCreateObject::Table => self.show_create_table_to_plan(obj_name),
                _ => Err(DataFusionError::NotImplemented(
                    "Only `SHOW CREATE TABLE  ...` statement is supported".to_string(),
                )),
            },
            Statement::CreateSchema {
                schema_name,
                if_not_exists,
            } => Ok(LogicalPlan::CreateCatalogSchema(CreateCatalogSchema {
                schema_name: schema_name.to_string(),
                if_not_exists,
                schema: Arc::new(DFSchema::empty()),
            })),
            Statement::CreateDatabase {
                db_name,
                if_not_exists,
                ..
            } => Ok(LogicalPlan::CreateCatalog(CreateCatalog {
                catalog_name: db_name.to_string(),
                if_not_exists,
                schema: Arc::new(DFSchema::empty()),
            })),
            Statement::Drop {
                object_type,
                if_exists,
                mut names,
                cascade: _,
                restrict: _,
                purge: _,
            } => {
                // We don't support cascade and purge for now.
                // nor do we support multiple object names
                let name = match names.len() {
                    0 => Err(ParserError("Missing table name.".to_string()).into()),
                    1 => object_name_to_table_reference(names.pop().unwrap()),
                    _ => {
                        Err(ParserError("Multiple objects not supported".to_string())
                            .into())
                    }
                }?;

                match object_type {
                    ObjectType::Table => Ok(LogicalPlan::DropTable(DropTable {
                        name,
                        if_exists,
                        schema: DFSchemaRef::new(DFSchema::empty()),
                    })),
                    ObjectType::View => Ok(LogicalPlan::DropView(DropView {
                        name,
                        if_exists,
                        schema: DFSchemaRef::new(DFSchema::empty()),
                    })),
                    _ => Err(DataFusionError::NotImplemented(
                        "Only `DROP TABLE/VIEW  ...` statement is supported currently"
                            .to_string(),
                    )),
                }
            }
            Statement::Prepare {
                name,
                data_types,
                statement,
            } => {
                // Convert parser data types to DataFusion data types
                let data_types: Vec<DataType> = data_types
                    .into_iter()
                    .map(|t| self.convert_data_type(&t))
                    .collect::<Result<_>>()?;

                // Create planner context with parameters
                let mut planner_context =
                    PlannerContext::new_with_prepare_param_data_types(data_types.clone());

                // Build logical plan for inner statement of the prepare statement
                let plan = self.sql_statement_to_plan_with_context(
                    *statement,
                    &mut planner_context,
                )?;
                Ok(LogicalPlan::Prepare(Prepare {
                    name: name.to_string(),
                    data_types,
                    input: Arc::new(plan),
                }))
            }

            Statement::ShowTables {
                extended,
                full,
                db_name,
                filter,
            } => self.show_tables_to_plan(extended, full, db_name, filter),

            Statement::ShowColumns {
                extended,
                full,
                table_name,
                filter,
            } => self.show_columns_to_plan(extended, full, table_name, filter),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported SQL statement: {:?}",
                sql
            ))),
        }
    }

    /// Generate a logical plan from a "SHOW TABLES" query
    fn show_tables_to_plan(
        &self,
        extended: bool,
        full: bool,
        db_name: Option<Ident>,
        filter: Option<ShowStatementFilter>,
    ) -> Result<LogicalPlan> {
        if self.has_table("information_schema", "tables") {
            // we only support the basic "SHOW TABLES"
            // https://github.com/apache/arrow-datafusion/issues/3188
            if db_name.is_some() || filter.is_some() || full || extended {
                Err(DataFusionError::Plan(
                    "Unsupported parameters to SHOW TABLES".to_string(),
                ))
            } else {
                let query = "SELECT * FROM information_schema.tables;";
                let mut rewrite = DFParser::parse_sql(query)?;
                assert_eq!(rewrite.len(), 1);
                self.statement_to_plan(rewrite.pop_front().unwrap()) // length of rewrite is 1
            }
        } else {
            Err(DataFusionError::Plan(
                "SHOW TABLES is not supported unless information_schema is enabled"
                    .to_string(),
            ))
        }
    }

    /// Generate a logical plan from an SQL query
    pub fn query_to_plan(
        &self,
        query: Query,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        self.query_to_plan_with_schema(query, planner_context, None)
    }

    /// Generate a logical plan from a SQL subquery
    pub fn subquery_to_plan(
        &self,
        query: Query,
        planner_context: &mut PlannerContext,
        outer_query_schema: &DFSchema,
    ) -> Result<LogicalPlan> {
        self.query_to_plan_with_schema(query, planner_context, Some(outer_query_schema))
    }

    /// Generate a logic plan from an SQL query.
    /// It's implementation of `subquery_to_plan` and `query_to_plan`.
    /// It shouldn't be invoked directly.
    fn query_to_plan_with_schema(
        &self,
        query: Query,
        planner_context: &mut PlannerContext,
        outer_query_schema: Option<&DFSchema>,
    ) -> Result<LogicalPlan> {
        let set_expr = query.body;
        if let Some(with) = query.with {
            // Process CTEs from top to bottom
            // do not allow self-references
            if with.recursive {
                return Err(DataFusionError::NotImplemented(
                    "Recursive CTEs are not supported".to_string(),
                ));
            }

            for cte in with.cte_tables {
                // A `WITH` block can't use the same name more than once
                let cte_name = normalize_ident(&cte.alias.name);
                if planner_context.ctes.contains_key(&cte_name) {
                    return Err(DataFusionError::SQL(ParserError(format!(
                        "WITH query name {:?} specified more than once",
                        cte_name
                    ))));
                }
                // create logical plan & pass backreferencing CTEs
                // CTE expr don't need extend outer_query_schema
                let logical_plan =
                    self.query_to_plan(*cte.query, &mut planner_context.clone())?;

                // Each `WITH` block can change the column names in the last
                // projection (e.g. "WITH table(t1, t2) AS SELECT 1, 2").
                let logical_plan = self.apply_table_alias(logical_plan, cte.alias)?;

                planner_context.ctes.insert(cte_name, logical_plan);
            }
        }
        let plan =
            self.set_expr_to_plan(*set_expr, planner_context, outer_query_schema)?;
        let plan = self.order_by(plan, query.order_by)?;
        self.limit(plan, query.offset, query.limit)
    }

    fn set_expr_to_plan(
        &self,
        set_expr: SetExpr,
        planner_context: &mut PlannerContext,
        outer_query_schema: Option<&DFSchema>,
    ) -> Result<LogicalPlan> {
        match set_expr {
            SetExpr::Select(s) => {
                self.select_to_plan(*s, planner_context, outer_query_schema)
            }
            SetExpr::Values(v) => {
                self.sql_values_to_plan(v, &planner_context.prepare_param_data_types)
            }
            SetExpr::SetOperation {
                op,
                left,
                right,
                set_quantifier,
            } => {
                let all = match set_quantifier {
                    SetQuantifier::All => true,
                    SetQuantifier::Distinct | SetQuantifier::None => false,
                };

                let left_plan =
                    self.set_expr_to_plan(*left, planner_context, outer_query_schema)?;
                let right_plan =
                    self.set_expr_to_plan(*right, planner_context, outer_query_schema)?;
                match (op, all) {
                    (SetOperator::Union, true) => LogicalPlanBuilder::from(left_plan)
                        .union(right_plan)?
                        .build(),
                    (SetOperator::Union, false) => LogicalPlanBuilder::from(left_plan)
                        .union_distinct(right_plan)?
                        .build(),
                    (SetOperator::Intersect, true) => {
                        LogicalPlanBuilder::intersect(left_plan, right_plan, true)
                    }
                    (SetOperator::Intersect, false) => {
                        LogicalPlanBuilder::intersect(left_plan, right_plan, false)
                    }
                    (SetOperator::Except, true) => {
                        LogicalPlanBuilder::except(left_plan, right_plan, true)
                    }
                    (SetOperator::Except, false) => {
                        LogicalPlanBuilder::except(left_plan, right_plan, false)
                    }
                }
            }
            SetExpr::Query(q) => self.query_to_plan(*q, planner_context),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Query {} not implemented yet",
                set_expr
            ))),
        }
    }

    pub fn describe_table_to_plan(
        &self,
        statement: DescribeTable,
    ) -> Result<LogicalPlan> {
        let DescribeTable { table_name } = statement;

        let where_clause = object_name_to_qualifier(&table_name);
        let table_ref = object_name_to_table_reference(table_name)?;

        // check if table_name exists
        let _ = self
            .schema_provider
            .get_table_provider((&table_ref).into())?;

        if self.has_table("information_schema", "tables") {
            let sql = format!(
                "SELECT column_name, data_type, is_nullable \
                                FROM information_schema.columns WHERE {where_clause};"
            );
            let mut rewrite = DFParser::parse_sql(&sql)?;
            self.statement_to_plan(rewrite.pop_front().unwrap())
        } else {
            Err(DataFusionError::Plan(
                "DESCRIBE TABLE is not supported unless information_schema is enabled"
                    .to_string(),
            ))
        }
    }

    /// Generate a logical plan from a CREATE EXTERNAL TABLE statement
    pub fn external_table_to_plan(
        &self,
        statement: CreateExternalTable,
    ) -> Result<LogicalPlan> {
        let definition = Some(statement.to_string());
        let CreateExternalTable {
            name,
            columns,
            file_type,
            has_header,
            delimiter,
            location,
            table_partition_cols,
            if_not_exists,
            file_compression_type,
            options,
        } = statement;

        // semantic checks
        if file_type == "PARQUET" && !columns.is_empty() {
            Err(DataFusionError::Plan(
                "Column definitions can not be specified for PARQUET files.".into(),
            ))?;
        }

        if file_type != "CSV" && file_type != "JSON" && !file_compression_type.is_empty()
        {
            Err(DataFusionError::Plan(
                "File compression type can be specified for CSV/JSON files.".into(),
            ))?;
        }

        let schema = self.build_schema(columns)?;

        // External tables do not support schemas at the moment, so the name is just a table name
        let name = OwnedTableReference::Bare { table: name };

        Ok(LogicalPlan::CreateExternalTable(PlanCreateExternalTable {
            schema: schema.to_dfschema_ref()?,
            name,
            location,
            file_type,
            has_header,
            delimiter,
            table_partition_cols,
            if_not_exists,
            definition,
            file_compression_type,
            options,
        }))
    }

    /// Generate a plan for EXPLAIN ... that will print out a plan
    ///
    pub fn explain_statement_to_plan(
        &self,
        verbose: bool,
        analyze: bool,
        statement: Statement,
    ) -> Result<LogicalPlan> {
        let plan = self.sql_statement_to_plan(statement)?;
        let plan = Arc::new(plan);
        let schema = LogicalPlan::explain_schema();
        let schema = schema.to_dfschema_ref()?;

        if analyze {
            Ok(LogicalPlan::Analyze(Analyze {
                verbose,
                input: plan,
                schema,
            }))
        } else {
            let stringified_plans =
                vec![plan.to_stringified(PlanType::InitialLogicalPlan)];
            Ok(LogicalPlan::Explain(Explain {
                verbose,
                plan,
                stringified_plans,
                schema,
            }))
        }
    }

    fn build_schema(&self, columns: Vec<SQLColumnDef>) -> Result<Schema> {
        let mut fields = Vec::with_capacity(columns.len());

        for column in columns {
            let data_type = self.convert_simple_data_type(&column.data_type)?;
            let allow_null = column
                .options
                .iter()
                .any(|x| x.option == ColumnOption::Null);
            fields.push(Field::new(
                &normalize_ident(&column.name),
                data_type,
                allow_null,
            ));
        }

        Ok(Schema::new(fields))
    }

    fn plan_from_tables(
        &self,
        mut from: Vec<TableWithJoins>,
        planner_context: &mut PlannerContext,
        outer_query_schema: Option<&DFSchema>,
    ) -> Result<LogicalPlan> {
        match from.len() {
            0 => Ok(LogicalPlanBuilder::empty(true).build()?),
            1 => {
                let from = from.remove(0);
                self.plan_table_with_joins(from, planner_context, outer_query_schema)
            }
            _ => {
                let plans = from
                    .into_iter()
                    .map(|t| {
                        self.plan_table_with_joins(t, planner_context, outer_query_schema)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let mut left = plans[0].clone();
                for right in plans.iter().skip(1) {
                    left = LogicalPlanBuilder::from(left).cross_join(right)?.build()?;
                }
                Ok(left)
            }
        }
    }

    fn plan_table_with_joins(
        &self,
        t: TableWithJoins,
        planner_context: &mut PlannerContext,
        outer_query_schema: Option<&DFSchema>,
    ) -> Result<LogicalPlan> {
        // From clause may exist CTEs, we should separate them from global CTEs.
        // CTEs in from clause are allowed to be duplicated.
        // Such as `select * from (WITH source AS (select 1 as e) SELECT * FROM source) t1, (WITH source AS (select 1 as e) SELECT * FROM source) t2;` which is valid.
        // So always use original global CTEs to plan CTEs in from clause.
        // Btw, don't need to add CTEs in from to global CTEs.
        let origin_planner_context = planner_context.clone();
        let left =
            self.create_relation(t.relation, planner_context, outer_query_schema)?;
        match t.joins.len() {
            0 => {
                *planner_context = origin_planner_context;
                Ok(left)
            }
            _ => {
                let mut joins = t.joins.into_iter();
                *planner_context = origin_planner_context.clone();
                let mut left = self.parse_relation_join(
                    left,
                    joins.next().unwrap(), // length of joins > 0
                    planner_context,
                    outer_query_schema,
                )?;
                for join in joins {
                    *planner_context = origin_planner_context.clone();
                    left = self.parse_relation_join(
                        left,
                        join,
                        planner_context,
                        outer_query_schema,
                    )?;
                }
                *planner_context = origin_planner_context;
                Ok(left)
            }
        }
    }

    fn parse_relation_join(
        &self,
        left: LogicalPlan,
        join: Join,
        planner_context: &mut PlannerContext,
        outer_query_schema: Option<&DFSchema>,
    ) -> Result<LogicalPlan> {
        let right =
            self.create_relation(join.relation, planner_context, outer_query_schema)?;
        match join.join_operator {
            JoinOperator::LeftOuter(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Left, planner_context)
            }
            JoinOperator::RightOuter(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Right, planner_context)
            }
            JoinOperator::Inner(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Inner, planner_context)
            }
            JoinOperator::FullOuter(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Full, planner_context)
            }
            JoinOperator::CrossJoin => self.parse_cross_join(left, &right),
            other => Err(DataFusionError::NotImplemented(format!(
                "Unsupported JOIN operator {:?}",
                other
            ))),
        }
    }

    fn parse_cross_join(
        &self,
        left: LogicalPlan,
        right: &LogicalPlan,
    ) -> Result<LogicalPlan> {
        LogicalPlanBuilder::from(left).cross_join(right)?.build()
    }

    fn parse_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        constraint: JoinConstraint,
        join_type: JoinType,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        match constraint {
            JoinConstraint::On(sql_expr) => {
                let mut keys: Vec<(Expr, Expr)> = vec![];
                let join_schema = left.schema().join(right.schema())?;

                // parse ON expression
                let expr = self.sql_to_rex(sql_expr, &join_schema, planner_context)?;

                // ambiguous check
                ensure_any_column_reference_is_unambiguous(
                    &expr,
                    &[left.schema().clone(), right.schema().clone()],
                )?;

                // normalize all columns in expression
                let using_columns = expr.to_columns()?;
                let normalized_expr = normalize_col_with_schemas(
                    expr,
                    &[left.schema(), right.schema()],
                    &[using_columns],
                )?;

                // expression that didn't match equi-join pattern
                let mut filter = vec![];

                // extract join keys
                extract_join_keys(
                    normalized_expr,
                    &mut keys,
                    &mut filter,
                    left.schema(),
                    right.schema(),
                )?;

                let (left_keys, right_keys): (Vec<Expr>, Vec<Expr>) =
                    keys.into_iter().unzip();

                let join_filter = filter.into_iter().reduce(Expr::and);

                if left_keys.is_empty() {
                    // TODO should not use cross join when the join_filter exists
                    // https://github.com/apache/arrow-datafusion/issues/4363
                    let join = LogicalPlanBuilder::from(left).cross_join(&right)?;
                    join_filter
                        .map(|filter| join.filter(filter))
                        .unwrap_or(Ok(join))?
                        .build()
                } else {
                    // Wrap projection for left input if left join keys contain normal expression.
                    let (left_child, left_join_keys, left_projected) =
                        wrap_projection_for_join_if_necessary(&left_keys, left)?;

                    // Wrap projection for right input if right join keys contains normal expression.
                    let (right_child, right_join_keys, right_projected) =
                        wrap_projection_for_join_if_necessary(&right_keys, right)?;

                    let join_plan_builder = LogicalPlanBuilder::from(left_child).join(
                        &right_child,
                        join_type,
                        (left_join_keys, right_join_keys),
                        join_filter,
                    )?;

                    // Remove temporary projected columns if necessary.
                    if left_projected || right_projected {
                        let final_join_result = join_schema
                            .fields()
                            .iter()
                            .map(|field| Expr::Column(field.qualified_column()))
                            .collect::<Vec<_>>();
                        join_plan_builder.project(final_join_result)?.build()
                    } else {
                        join_plan_builder.build()
                    }
                }
            }
            JoinConstraint::Using(idents) => {
                let keys: Vec<Column> = idents
                    .into_iter()
                    .map(|x| Column::from_name(normalize_ident(&x)))
                    .collect();
                LogicalPlanBuilder::from(left)
                    .join_using(&right, join_type, keys)?
                    .build()
            }
            JoinConstraint::Natural => {
                // https://issues.apache.org/jira/browse/ARROW-10727
                Err(DataFusionError::NotImplemented(
                    "NATURAL JOIN is not supported (https://issues.apache.org/jira/browse/ARROW-10727)".to_string(),
                ))
            }
            JoinConstraint::None => Err(DataFusionError::NotImplemented(
                "NONE constraint is not supported".to_string(),
            )),
        }
    }
    fn create_relation(
        &self,
        relation: TableFactor,
        planner_context: &mut PlannerContext,
        outer_query_schema: Option<&DFSchema>,
    ) -> Result<LogicalPlan> {
        let (plan, alias) = match relation {
            TableFactor::Table { name, alias, .. } => {
                // normalize name and alias
                let table_ref = object_name_to_table_reference(name)?;
                let table_name = table_ref.to_string();
                let cte = planner_context.ctes.get(&table_name);
                (
                    match (
                        cte,
                        self.schema_provider.get_table_provider((&table_ref).into()),
                    ) {
                        (Some(cte_plan), _) => Ok(cte_plan.clone()),
                        (_, Ok(provider)) => {
                            LogicalPlanBuilder::scan(&table_name, provider, None)?.build()
                        }
                        (None, Err(e)) => Err(e),
                    }?,
                    alias,
                )
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let logical_plan = self.query_to_plan(*subquery, planner_context)?;
                (logical_plan, alias)
            }
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => (
                self.plan_table_with_joins(
                    *table_with_joins,
                    planner_context,
                    outer_query_schema,
                )?,
                alias,
            ),
            // @todo Support TableFactory::TableFunction?
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported ast node {:?} in create_relation",
                    relation
                )));
            }
        };
        if let Some(alias) = alias {
            self.apply_table_alias(plan, alias)
        } else {
            Ok(plan)
        }
    }

    /// Apply the given TableAlias to the top-level projection.
    fn apply_table_alias(
        &self,
        plan: LogicalPlan,
        alias: TableAlias,
    ) -> Result<LogicalPlan> {
        let apply_name_plan = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            plan,
            &normalize_ident(&alias.name),
        )?);

        self.apply_expr_alias(apply_name_plan, &alias.columns)
    }

    fn apply_expr_alias(
        &self,
        plan: LogicalPlan,
        idents: &Vec<Ident>,
    ) -> Result<LogicalPlan> {
        if idents.is_empty() {
            Ok(plan)
        } else if idents.len() != plan.schema().fields().len() {
            Err(DataFusionError::Plan(format!(
                "Source table contains {} columns but only {} names given as column alias",
                plan.schema().fields().len(),
                idents.len(),
            )))
        } else {
            let fields = plan.schema().fields().clone();
            LogicalPlanBuilder::from(plan)
                .project(fields.iter().zip(idents.iter()).map(|(field, ident)| {
                    col(field.name()).alias(normalize_ident(ident))
                }))?
                .build()
        }
    }

    /// Generate a logic plan from selection clause, the function contain optimization for cross join to inner join
    /// Related PR: <https://github.com/apache/arrow-datafusion/pull/1566>
    fn plan_selection(
        &self,
        selection: Option<SQLExpr>,
        plan: LogicalPlan,
        outer_query_schema: Option<&DFSchema>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        match selection {
            Some(predicate_expr) => {
                let mut join_schema = (**plan.schema()).clone();
                let mut all_schemas: Vec<DFSchemaRef> = vec![];
                for schema in plan.all_schemas() {
                    all_schemas.push(schema.clone());
                }
                if let Some(outer) = outer_query_schema {
                    all_schemas.push(Arc::new(outer.clone()));
                    join_schema.merge(outer);
                }
                let x: Vec<&DFSchemaRef> = all_schemas.iter().collect();

                let filter_expr =
                    self.sql_to_rex(predicate_expr, &join_schema, planner_context)?;
                let mut using_columns = HashSet::new();
                expr_to_columns(&filter_expr, &mut using_columns)?;
                let filter_expr = normalize_col_with_schemas(
                    filter_expr,
                    x.as_slice(),
                    &[using_columns],
                )?;

                Ok(LogicalPlan::Filter(Filter::try_new(
                    filter_expr,
                    Arc::new(plan),
                )?))
            }
            None => Ok(plan),
        }
    }

    /// build schema for unqualifier column ambiguous check
    fn build_schema_for_ambiguous_check(&self, plan: &LogicalPlan) -> Result<DFSchema> {
        let mut fields = plan.schema().fields().clone();

        let metadata = plan.schema().metadata().clone();
        if let LogicalPlan::Join(HashJoin {
            join_constraint: HashJoinConstraint::Using,
            ref on,
            ref left,
            ..
        }) = plan
        {
            // For query: select id from t1 join t2 using(id), this is legal.
            // We should dedup the fields for cols in using clause.
            for join_cols in on.iter() {
                let left_field = left.schema().field_from_column(&join_cols.0)?;
                fields.retain(|field| {
                    field.unqualified_column().name
                        != left_field.unqualified_column().name
                });
                fields.push(left_field.clone());
            }
        }

        DFSchema::new_with_metadata(fields, metadata)
    }

    /// Generate a logic plan from an SQL select
    fn select_to_plan(
        &self,
        select: Select,
        planner_context: &mut PlannerContext,
        outer_query_schema: Option<&DFSchema>,
    ) -> Result<LogicalPlan> {
        // check for unsupported syntax first
        if !select.cluster_by.is_empty() {
            return Err(DataFusionError::NotImplemented("CLUSTER BY".to_string()));
        }
        if !select.lateral_views.is_empty() {
            return Err(DataFusionError::NotImplemented("LATERAL VIEWS".to_string()));
        }
        if select.qualify.is_some() {
            return Err(DataFusionError::NotImplemented("QUALIFY".to_string()));
        }
        if select.top.is_some() {
            return Err(DataFusionError::NotImplemented("TOP".to_string()));
        }

        // process `from` clause
        let plan =
            self.plan_from_tables(select.from, planner_context, outer_query_schema)?;

        let empty_from = matches!(plan, LogicalPlan::EmptyRelation(_));
        // build from schema for unqualifier column ambiguous check
        // we should get only one field for unqualifier column from schema.
        let from_schema = self.build_schema_for_ambiguous_check(&plan)?;

        // process `where` clause
        let plan = self.plan_selection(
            select.selection,
            plan,
            outer_query_schema,
            planner_context,
        )?;

        // process the SELECT expressions, with wildcards expanded.
        let select_exprs = self.prepare_select_exprs(
            &plan,
            select.projection,
            empty_from,
            planner_context,
            &from_schema,
        )?;

        // having and group by clause may reference aliases defined in select projection
        let projected_plan = self.project(plan.clone(), select_exprs.clone())?;
        let mut combined_schema = (**projected_plan.schema()).clone();
        combined_schema.merge(plan.schema());

        // this alias map is resolved and looked up in both having exprs and group by exprs
        let alias_map = extract_aliases(&select_exprs);

        // Optionally the HAVING expression.
        let having_expr_opt = select
            .having
            .map::<Result<Expr>, _>(|having_expr| {
                let having_expr = self.sql_expr_to_logical_expr(
                    having_expr,
                    &combined_schema,
                    planner_context,
                )?;
                // This step "dereferences" any aliases in the HAVING clause.
                //
                // This is how we support queries with HAVING expressions that
                // refer to aliased columns.
                //
                // For example:
                //
                //   SELECT c1, MAX(c2) AS m FROM t GROUP BY c1 HAVING m > 10;
                //
                // are rewritten as, respectively:
                //
                //   SELECT c1, MAX(c2) AS m FROM t GROUP BY c1 HAVING MAX(c2) > 10;
                //
                let having_expr = resolve_aliases_to_exprs(&having_expr, &alias_map)?;
                normalize_col(having_expr, &projected_plan)
            })
            .transpose()?;

        // The outer expressions we will search through for
        // aggregates. Aggregates may be sourced from the SELECT...
        let mut aggr_expr_haystack = select_exprs.clone();
        // ... or from the HAVING.
        if let Some(having_expr) = &having_expr_opt {
            aggr_expr_haystack.push(having_expr.clone());
        }

        // All of the aggregate expressions (deduplicated).
        let aggr_exprs = find_aggregate_exprs(&aggr_expr_haystack);

        // All of the group by expressions
        let group_by_exprs = select
            .group_by
            .into_iter()
            .map(|e| {
                let group_by_expr =
                    self.sql_expr_to_logical_expr(e, &combined_schema, planner_context)?;
                // aliases from the projection can conflict with same-named expressions in the input
                let mut alias_map = alias_map.clone();
                for f in plan.schema().fields() {
                    alias_map.remove(f.name());
                }
                let group_by_expr = resolve_aliases_to_exprs(&group_by_expr, &alias_map)?;
                let group_by_expr =
                    resolve_positions_to_exprs(&group_by_expr, &select_exprs)
                        .unwrap_or(group_by_expr);
                let group_by_expr = normalize_col(group_by_expr, &projected_plan)?;
                self.validate_schema_satisfies_exprs(
                    plan.schema(),
                    &[group_by_expr.clone()],
                )?;
                Ok(group_by_expr)
            })
            .collect::<Result<Vec<Expr>>>()?;

        // process group by, aggregation or having
        let (plan, mut select_exprs_post_aggr, having_expr_post_aggr) =
            if !group_by_exprs.is_empty() || !aggr_exprs.is_empty() {
                self.aggregate(
                    plan,
                    &select_exprs,
                    having_expr_opt.as_ref(),
                    group_by_exprs,
                    aggr_exprs,
                )?
            } else {
                if let Some(having_expr) = &having_expr_opt {
                    let available_columns = select_exprs
                        .iter()
                        .map(|expr| expr_as_column_expr(expr, &plan))
                        .collect::<Result<Vec<Expr>>>()?;

                    // Ensure the HAVING expression is using only columns
                    // provided by the SELECT.
                    check_columns_satisfy_exprs(
                        &available_columns,
                        &[having_expr.clone()],
                        "HAVING clause references column(s) not provided by the select",
                    )?;
                }

                (plan, select_exprs, having_expr_opt)
            };

        let plan = if let Some(having_expr_post_aggr) = having_expr_post_aggr {
            LogicalPlanBuilder::from(plan)
                .filter(having_expr_post_aggr)?
                .build()?
        } else {
            plan
        };

        // process window function
        let window_func_exprs = find_window_exprs(&select_exprs_post_aggr);

        let plan = if window_func_exprs.is_empty() {
            plan
        } else {
            let plan = LogicalPlanBuilder::window_plan(plan, window_func_exprs.clone())?;

            // re-write the projection
            select_exprs_post_aggr = select_exprs_post_aggr
                .iter()
                .map(|expr| rebase_expr(expr, &window_func_exprs, &plan))
                .collect::<Result<Vec<Expr>>>()?;

            plan
        };

        // final projection
        let plan = project(plan, select_exprs_post_aggr)?;

        // process distinct clause
        let plan = if select.distinct {
            LogicalPlanBuilder::from(plan).distinct()?.build()
        } else {
            Ok(plan)
        }?;

        // DISTRIBUTE BY
        if !select.distribute_by.is_empty() {
            let x = select
                .distribute_by
                .iter()
                .map(|e| {
                    self.sql_expr_to_logical_expr(
                        e.clone(),
                        &combined_schema,
                        planner_context,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            LogicalPlanBuilder::from(plan)
                .repartition(Partitioning::DistributeBy(x))?
                .build()
        } else {
            Ok(plan)
        }
    }

    /// Returns the `Expr`'s corresponding to a SQL query's SELECT expressions.
    ///
    /// Wildcards are expanded into the concrete list of columns.
    fn prepare_select_exprs(
        &self,
        plan: &LogicalPlan,
        projection: Vec<SelectItem>,
        empty_from: bool,
        planner_context: &mut PlannerContext,
        from_schema: &DFSchema,
    ) -> Result<Vec<Expr>> {
        projection
            .into_iter()
            .map(|expr| {
                self.sql_select_to_rex(
                    expr,
                    plan,
                    empty_from,
                    planner_context,
                    from_schema,
                )
            })
            .flat_map(|result| match result {
                Ok(vec) => vec.into_iter().map(Ok).collect(),
                Err(err) => vec![Err(err)],
            })
            .collect::<Result<Vec<Expr>>>()
    }

    /// Wrap a plan in a projection
    fn project(&self, input: LogicalPlan, expr: Vec<Expr>) -> Result<LogicalPlan> {
        self.validate_schema_satisfies_exprs(input.schema(), &expr)?;
        LogicalPlanBuilder::from(input).project(expr)?.build()
    }

    /// Create an aggregate plan.
    ///
    /// An aggregate plan consists of grouping expressions, aggregate expressions, and an
    /// optional HAVING expression (which is a filter on the output of the aggregate).
    ///
    /// # Arguments
    ///
    /// * `input`           - The input plan that will be aggregated. The grouping, aggregate, and
    ///                       "having" expressions must all be resolvable from this plan.
    /// * `select_exprs`    - The projection expressions from the SELECT clause.
    /// * `having_expr_opt` - Optional HAVING clause.
    /// * `group_by_exprs`  - Grouping expressions from the GROUP BY clause. These can be column
    ///                       references or more complex expressions.
    /// * `aggr_exprs`      - Aggregate expressions, such as `SUM(a)` or `COUNT(1)`.
    ///
    /// # Return
    ///
    /// The return value is a triplet of the following items:
    ///
    /// * `plan`                   - A [LogicalPlan::Aggregate] plan for the newly created aggregate.
    /// * `select_exprs_post_aggr` - The projection expressions rewritten to reference columns from
    ///                              the aggregate
    /// * `having_expr_post_aggr`  - The "having" expression rewritten to reference a column from
    ///                              the aggregate
    fn aggregate(
        &self,
        input: LogicalPlan,
        select_exprs: &[Expr],
        having_expr_opt: Option<&Expr>,
        group_by_exprs: Vec<Expr>,
        aggr_exprs: Vec<Expr>,
    ) -> Result<(LogicalPlan, Vec<Expr>, Option<Expr>)> {
        // create the aggregate plan
        let plan = LogicalPlanBuilder::from(input.clone())
            .aggregate(group_by_exprs.clone(), aggr_exprs.clone())?
            .build()?;

        // in this next section of code we are re-writing the projection to refer to columns
        // output by the aggregate plan. For example, if the projection contains the expression
        // `SUM(a)` then we replace that with a reference to a column `SUM(a)` produced by
        // the aggregate plan.

        // combine the original grouping and aggregate expressions into one list (note that
        // we do not add the "having" expression since that is not part of the projection)
        let mut aggr_projection_exprs = vec![];
        for expr in &group_by_exprs {
            match expr {
                Expr::GroupingSet(GroupingSet::Rollup(exprs)) => {
                    aggr_projection_exprs.extend_from_slice(exprs)
                }
                Expr::GroupingSet(GroupingSet::Cube(exprs)) => {
                    aggr_projection_exprs.extend_from_slice(exprs)
                }
                Expr::GroupingSet(GroupingSet::GroupingSets(lists_of_exprs)) => {
                    for exprs in lists_of_exprs {
                        aggr_projection_exprs.extend_from_slice(exprs)
                    }
                }
                _ => aggr_projection_exprs.push(expr.clone()),
            }
        }
        aggr_projection_exprs.extend_from_slice(&aggr_exprs);

        // now attempt to resolve columns and replace with fully-qualified columns
        let aggr_projection_exprs = aggr_projection_exprs
            .iter()
            .map(|expr| resolve_columns(expr, &input))
            .collect::<Result<Vec<Expr>>>()?;

        // next we replace any expressions that are not a column with a column referencing
        // an output column from the aggregate schema
        let column_exprs_post_aggr = aggr_projection_exprs
            .iter()
            .map(|expr| expr_as_column_expr(expr, &input))
            .collect::<Result<Vec<Expr>>>()?;

        // next we re-write the projection
        let select_exprs_post_aggr = select_exprs
            .iter()
            .map(|expr| rebase_expr(expr, &aggr_projection_exprs, &input))
            .collect::<Result<Vec<Expr>>>()?;

        // finally, we have some validation that the re-written projection can be resolved
        // from the aggregate output columns
        check_columns_satisfy_exprs(
            &column_exprs_post_aggr,
            &select_exprs_post_aggr,
            "Projection references non-aggregate values",
        )?;

        // Rewrite the HAVING expression to use the columns produced by the
        // aggregation.
        let having_expr_post_aggr = if let Some(having_expr) = having_expr_opt {
            let having_expr_post_aggr =
                rebase_expr(having_expr, &aggr_projection_exprs, &input)?;

            check_columns_satisfy_exprs(
                &column_exprs_post_aggr,
                &[having_expr_post_aggr.clone()],
                "HAVING clause references non-aggregate values",
            )?;

            Some(having_expr_post_aggr)
        } else {
            None
        };

        Ok((plan, select_exprs_post_aggr, having_expr_post_aggr))
    }

    /// Wrap a plan in a limit
    fn limit(
        &self,
        input: LogicalPlan,
        skip: Option<SQLOffset>,
        fetch: Option<SQLExpr>,
    ) -> Result<LogicalPlan> {
        if skip.is_none() && fetch.is_none() {
            return Ok(input);
        }

        let skip = match skip {
            Some(skip_expr) => match self.sql_to_rex(
                skip_expr.value,
                input.schema(),
                &mut PlannerContext::new(),
            )? {
                Expr::Literal(ScalarValue::Int64(Some(s))) => {
                    if s < 0 {
                        return Err(DataFusionError::Plan(format!(
                            "Offset must be >= 0, '{}' was provided.",
                            s
                        )));
                    }
                    Ok(s as usize)
                }
                _ => Err(DataFusionError::Plan(
                    "Unexpected expression in OFFSET clause".to_string(),
                )),
            }?,
            _ => 0,
        };

        let fetch = match fetch {
            Some(limit_expr) => {
                let n = match self.sql_to_rex(
                    limit_expr,
                    input.schema(),
                    &mut PlannerContext::new(),
                )? {
                    Expr::Literal(ScalarValue::Int64(Some(n))) => Ok(n as usize),
                    _ => Err(DataFusionError::Plan(
                        "Unexpected expression for LIMIT clause".to_string(),
                    )),
                }?;
                Some(n)
            }
            _ => None,
        };

        LogicalPlanBuilder::from(input).limit(skip, fetch)?.build()
    }

    /// Wrap the logical in a sort
    fn order_by(
        &self,
        plan: LogicalPlan,
        order_by: Vec<OrderByExpr>,
    ) -> Result<LogicalPlan> {
        if order_by.is_empty() {
            return Ok(plan);
        }

        let order_by_rex = order_by
            .into_iter()
            .map(|e| self.order_by_to_sort_expr(e, plan.schema()))
            .collect::<Result<Vec<_>>>()?;

        LogicalPlanBuilder::from(plan).sort(order_by_rex)?.build()
    }

    /// convert sql OrderByExpr to Expr::Sort
    fn order_by_to_sort_expr(&self, e: OrderByExpr, schema: &DFSchema) -> Result<Expr> {
        let OrderByExpr {
            asc,
            expr,
            nulls_first,
        } = e;

        let expr = match expr {
            SQLExpr::Value(Value::Number(v, _)) => {
                let field_index = v
                    .parse::<usize>()
                    .map_err(|err| DataFusionError::Plan(err.to_string()))?;

                if field_index == 0 {
                    return Err(DataFusionError::Plan(
                        "Order by index starts at 1 for column indexes".to_string(),
                    ));
                } else if schema.fields().len() < field_index {
                    return Err(DataFusionError::Plan(format!(
                        "Order by column out of bounds, specified: {}, max: {}",
                        field_index,
                        schema.fields().len()
                    )));
                }

                let field = schema.field(field_index - 1);
                Expr::Column(field.qualified_column())
            }
            e => self.sql_expr_to_logical_expr(e, schema, &mut PlannerContext::new())?,
        };
        Ok({
            let asc = asc.unwrap_or(true);
            Expr::Sort {
                expr: Box::new(expr),
                asc,
                // when asc is true, by default nulls last to be consistent with postgres
                // postgres rule: https://www.postgresql.org/docs/current/queries-order.html
                nulls_first: nulls_first.unwrap_or(!asc),
            }
        })
    }

    /// Validate the schema provides all of the columns referenced in the expressions.
    fn validate_schema_satisfies_exprs(
        &self,
        schema: &DFSchema,
        exprs: &[Expr],
    ) -> Result<()> {
        find_column_exprs(exprs)
            .iter()
            .try_for_each(|col| match col {
                Expr::Column(col) => match &col.relation {
                    Some(r) => {
                        schema.field_with_qualified_name(r, &col.name)?;
                        Ok(())
                    }
                    None => {
                        if !schema.fields_with_unqualified_name(&col.name).is_empty() {
                            Ok(())
                        } else {
                            Err(field_not_found(None, col.name.as_str(), schema))
                        }
                    }
                }
                .map_err(|_: DataFusionError| {
                    field_not_found(
                        col.relation.as_ref().map(|s| s.to_owned()),
                        col.name.as_str(),
                        schema,
                    )
                }),
                _ => Err(DataFusionError::Internal("Not a column".to_string())),
            })
    }

    /// ambiguous check for unqualifier column
    fn column_reference_ambiguous_check(
        &self,
        schema: &DFSchema,
        exprs: &[Expr],
    ) -> Result<()> {
        find_column_exprs(exprs)
            .iter()
            .try_for_each(|col| match col {
                Expr::Column(col) => match &col.relation {
                    None => {
                        // should get only one field in from_schema.
                        if schema.fields_with_unqualified_name(&col.name).len() != 1 {
                            Err(DataFusionError::Internal(format!(
                                "column reference {} is ambiguous",
                                col.name
                            )))
                        } else {
                            Ok(())
                        }
                    }
                    _ => Ok(()),
                },
                _ => Ok(()),
            })
    }

    /// Generate a relational expression from a select SQL expression
    fn sql_select_to_rex(
        &self,
        sql: SelectItem,
        plan: &LogicalPlan,
        empty_from: bool,
        planner_context: &mut PlannerContext,
        from_schema: &DFSchema,
    ) -> Result<Vec<Expr>> {
        match sql {
            SelectItem::UnnamedExpr(expr) => {
                let expr = self.sql_to_rex(expr, plan.schema(), planner_context)?;
                self.column_reference_ambiguous_check(from_schema, &[expr.clone()])?;
                Ok(vec![normalize_col(expr, plan)?])
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let select_expr =
                    self.sql_to_rex(expr, plan.schema(), planner_context)?;
                self.column_reference_ambiguous_check(
                    from_schema,
                    &[select_expr.clone()],
                )?;
                let expr = Alias(Box::new(select_expr), normalize_ident(&alias));
                Ok(vec![normalize_col(expr, plan)?])
            }
            SelectItem::Wildcard => {
                if empty_from {
                    return Err(DataFusionError::Plan(
                        "SELECT * with no tables specified is not valid".to_string(),
                    ));
                }
                // do not expand from outer schema
                expand_wildcard(plan.schema().as_ref(), plan)
            }
            SelectItem::QualifiedWildcard(ref object_name) => {
                let qualifier = format!("{}", object_name);
                // do not expand from outer schema
                expand_qualified_wildcard(&qualifier, plan.schema().as_ref(), plan)
            }
        }
    }

    /// Generate a relational expression from a SQL expression
    pub fn sql_to_rex(
        &self,
        sql: SQLExpr,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let mut expr = self.sql_expr_to_logical_expr(sql, schema, planner_context)?;
        expr = self.rewrite_partial_qualifier(expr, schema);
        self.validate_schema_satisfies_exprs(schema, &[expr.clone()])?;
        Ok(expr)
    }

    /// Rewrite aliases which are not-complete (e.g. ones that only include only table qualifier in a schema.table qualified relation)
    fn rewrite_partial_qualifier(&self, expr: Expr, schema: &DFSchema) -> Expr {
        match expr {
            Expr::Column(col) => match &col.relation {
                Some(q) => {
                    match schema
                        .fields()
                        .iter()
                        .find(|field| match field.qualifier() {
                            Some(field_q) => {
                                field.name() == &col.name
                                    && field_q.ends_with(&format!(".{}", q))
                            }
                            _ => false,
                        }) {
                        Some(df_field) => Expr::Column(Column {
                            relation: df_field.qualifier().cloned(),
                            name: df_field.name().clone(),
                        }),
                        None => Expr::Column(col),
                    }
                }
                None => Expr::Column(col),
            },
            _ => expr,
        }
    }

    fn sql_fn_arg_to_logical_expr(
        &self,
        sql: FunctionArg,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        match sql {
            FunctionArg::Named {
                name: _,
                arg: FunctionArgExpr::Expr(arg),
            } => self.sql_expr_to_logical_expr(arg, schema, planner_context),
            FunctionArg::Named {
                name: _,
                arg: FunctionArgExpr::Wildcard,
            } => Ok(Expr::Wildcard),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)) => {
                self.sql_expr_to_logical_expr(arg, schema, planner_context)
            }
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok(Expr::Wildcard),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported qualified wildcard argument: {:?}",
                sql
            ))),
        }
    }

    fn parse_sql_binary_op(
        &self,
        left: SQLExpr,
        op: BinaryOperator,
        right: SQLExpr,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let operator = match op {
            BinaryOperator::Gt => Ok(Operator::Gt),
            BinaryOperator::GtEq => Ok(Operator::GtEq),
            BinaryOperator::Lt => Ok(Operator::Lt),
            BinaryOperator::LtEq => Ok(Operator::LtEq),
            BinaryOperator::Eq => Ok(Operator::Eq),
            BinaryOperator::NotEq => Ok(Operator::NotEq),
            BinaryOperator::Plus => Ok(Operator::Plus),
            BinaryOperator::Minus => Ok(Operator::Minus),
            BinaryOperator::Multiply => Ok(Operator::Multiply),
            BinaryOperator::Divide => Ok(Operator::Divide),
            BinaryOperator::Modulo => Ok(Operator::Modulo),
            BinaryOperator::And => Ok(Operator::And),
            BinaryOperator::Or => Ok(Operator::Or),
            BinaryOperator::PGRegexMatch => Ok(Operator::RegexMatch),
            BinaryOperator::PGRegexIMatch => Ok(Operator::RegexIMatch),
            BinaryOperator::PGRegexNotMatch => Ok(Operator::RegexNotMatch),
            BinaryOperator::PGRegexNotIMatch => Ok(Operator::RegexNotIMatch),
            BinaryOperator::BitwiseAnd => Ok(Operator::BitwiseAnd),
            BinaryOperator::BitwiseOr => Ok(Operator::BitwiseOr),
            BinaryOperator::BitwiseXor => Ok(Operator::BitwiseXor),
            BinaryOperator::PGBitwiseShiftRight => Ok(Operator::BitwiseShiftRight),
            BinaryOperator::PGBitwiseShiftLeft => Ok(Operator::BitwiseShiftLeft),
            BinaryOperator::StringConcat => Ok(Operator::StringConcat),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported SQL binary operator {:?}",
                op
            ))),
        }?;

        Ok(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(self.sql_expr_to_logical_expr(left, schema, planner_context)?),
            operator,
            Box::new(self.sql_expr_to_logical_expr(right, schema, planner_context)?),
        )))
    }

    fn parse_sql_unary_op(
        &self,
        op: UnaryOperator,
        expr: SQLExpr,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        match op {
            UnaryOperator::Not => Ok(Expr::Not(Box::new(
                self.sql_expr_to_logical_expr(expr, schema, planner_context)?,
            ))),
            UnaryOperator::Plus => {
                Ok(self.sql_expr_to_logical_expr(expr, schema, planner_context)?)
            }
            UnaryOperator::Minus => {
                match expr {
                    // optimization: if it's a number literal, we apply the negative operator
                    // here directly to calculate the new literal.
                    SQLExpr::Value(Value::Number(n, _)) => match n.parse::<i64>() {
                        Ok(n) => Ok(lit(-n)),
                        Err(_) => Ok(lit(-n
                            .parse::<f64>()
                            .map_err(|_e| {
                                DataFusionError::Internal(format!(
                                    "negative operator can be only applied to integer and float operands, got: {}",
                                    n))
                            })?)),
                    },
                    // not a literal, apply negative operator on expression
                    _ => Ok(Expr::Negative(Box::new(self.sql_expr_to_logical_expr(expr, schema, planner_context)?))),
                }
            }
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported SQL unary operator {:?}",
                op
            ))),
        }
    }

    fn sql_values_to_plan(
        &self,
        values: SQLValues,
        param_data_types: &[DataType],
    ) -> Result<LogicalPlan> {
        // values should not be based on any other schema
        let schema = DFSchema::empty();
        let values = values
            .0
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|v| match v {
                        SQLExpr::Value(Value::Number(n, _)) => self.parse_sql_number(&n),
                        SQLExpr::Value(
                            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s),
                        ) => Ok(lit(s)),
                        SQLExpr::Value(Value::Null) => {
                            Ok(Expr::Literal(ScalarValue::Null))
                        }
                        SQLExpr::Value(Value::Boolean(n)) => Ok(lit(n)),
                        SQLExpr::Value(Value::Placeholder(param)) => {
                            Self::create_placeholder_expr(param, param_data_types)
                        }
                        SQLExpr::UnaryOp { op, expr } => self.parse_sql_unary_op(
                            op,
                            *expr,
                            &schema,
                            &mut PlannerContext::new(),
                        ),
                        SQLExpr::BinaryOp { left, op, right } => self
                            .parse_sql_binary_op(
                                *left,
                                op,
                                *right,
                                &schema,
                                &mut PlannerContext::new(),
                            ),
                        SQLExpr::TypedString { data_type, value } => {
                            Ok(Expr::Cast(Cast::new(
                                Box::new(lit(value)),
                                self.convert_data_type(&data_type)?,
                            )))
                        }
                        SQLExpr::Cast { expr, data_type } => Ok(Expr::Cast(Cast::new(
                            Box::new(self.sql_expr_to_logical_expr(
                                *expr,
                                &schema,
                                &mut PlannerContext::new(),
                            )?),
                            self.convert_data_type(&data_type)?,
                        ))),
                        other => Err(DataFusionError::NotImplemented(format!(
                            "Unsupported value {:?} in a values list expression",
                            other
                        ))),
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        LogicalPlanBuilder::values(values)?.build()
    }

    /// Create a placeholder expression
    /// This is the same as Postgres's prepare statement syntax in which a placeholder starts with `$` sign and then
    /// number 1, 2, ... etc. For example, `$1` is the first placeholder; $2 is the second one and so on.
    fn create_placeholder_expr(
        param: String,
        param_data_types: &[DataType],
    ) -> Result<Expr> {
        // Parse the placeholder as a number because it is the only support from sqlparser and postgres
        let index = param[1..].parse::<usize>();
        let idx = match index {
            Ok(index) => index - 1,
            Err(_) => {
                return Err(DataFusionError::Internal(format!(
                    "Invalid placeholder, not a number: {}",
                    param
                )))
            }
        };
        // Check if the placeholder is in the parameter list
        if param_data_types.len() <= idx {
            return Err(DataFusionError::Internal(format!(
                "Placehoder {} does not exist in the parameter list: {:?}",
                param, param_data_types
            )));
        }
        // Data type of the parameter
        let param_type = param_data_types[idx].clone();
        debug!(
            "type of param {} param_data_types[idx]: {:?}",
            param, param_type
        );

        Ok(Expr::Placeholder {
            id: param,
            data_type: param_type,
        })
    }

    fn sql_expr_to_logical_expr(
        &self,
        sql: SQLExpr,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        match sql {
            SQLExpr::Value(Value::Number(n, _)) => self.parse_sql_number(&n),
            SQLExpr::Value(Value::SingleQuotedString(ref s) | Value::DoubleQuotedString(ref s)) => Ok(lit(s.clone())),
            SQLExpr::Value(Value::Boolean(n)) => Ok(lit(n)),
            SQLExpr::Value(Value::Null) => Ok(Expr::Literal(ScalarValue::Null)),
            SQLExpr::Value(Value::Placeholder(param)) => Self::create_placeholder_expr(param, &planner_context.prepare_param_data_types),
            SQLExpr::Extract { field, expr } => Ok(Expr::ScalarFunction {
                fun: BuiltinScalarFunction::DatePart,
                args: vec![
                    Expr::Literal(ScalarValue::Utf8(Some(format!("{}", field)))),
                    self.sql_expr_to_logical_expr(*expr, schema, planner_context)?,
                ],
            }),

            SQLExpr::Array(arr) => self.sql_array_literal(arr.elem, schema),
            SQLExpr::Interval {
                value,
                leading_field,
                leading_precision,
                last_field,
                fractional_seconds_precision,
            } => self.sql_interval_to_expr(
                *value,
                leading_field,
                leading_precision,
                last_field,
                fractional_seconds_precision,
            ),
            SQLExpr::Identifier(id) => {
                if id.value.starts_with('@') {
                    // TODO: figure out if ScalarVariables should be insensitive.
                    let var_names = vec![id.value];
                    let ty = self
                        .schema_provider
                        .get_variable_type(&var_names)
                        .ok_or_else(|| {
                            DataFusionError::Execution(format!(
                                "variable {:?} has no type information",
                                var_names
                            ))
                        })?;
                    Ok(Expr::ScalarVariable(ty, var_names))
                } else {
                    // Don't use `col()` here because it will try to
                    // interpret names with '.' as if they were
                    // compound identifiers, but this is not a compound
                    // identifier. (e.g. it is "foo.bar" not foo.bar)

                    Ok(Expr::Column(Column {
                        relation: None,
                        name: normalize_ident(&id),
                    }))
                }
            }

            SQLExpr::MapAccess { ref column, keys } => {
                if let SQLExpr::Identifier(ref id) = column.as_ref() {
                    plan_indexed(col(&normalize_ident(id)), keys)
                } else {
                    Err(DataFusionError::NotImplemented(format!(
                        "map access requires an identifier, found column {} instead",
                        column
                    )))
                }
            }

            SQLExpr::ArrayIndex { obj, indexes } => {
                let expr = self.sql_expr_to_logical_expr(*obj, schema, planner_context)?;
                plan_indexed(expr, indexes)
            }

            SQLExpr::CompoundIdentifier(ids) => {
                if ids[0].value.starts_with('@') {
                    let var_names: Vec<_> = ids.into_iter().map(|s| normalize_ident(&s)).collect();
                    let ty = self
                        .schema_provider
                        .get_variable_type(&var_names)
                        .ok_or_else(|| {
                            DataFusionError::Execution(format!(
                                "variable {:?} has no type information",
                                var_names
                            ))
                        })?;
                    Ok(Expr::ScalarVariable(ty, var_names))
                } else {
                    // only support "schema.table" type identifiers here
                    let (name, relation) = match idents_to_table_reference(ids)? {
                        OwnedTableReference::Partial { schema, table } => (table, schema),
                        r @ OwnedTableReference::Bare { .. } |
                        r @ OwnedTableReference::Full { .. } => {
                            return Err(DataFusionError::Plan(format!(
                            "Unsupported compound identifier '{:?}'", r,
                            )))
                        }
                    };

                    // Try and find the reference in schema
                    match schema.field_with_qualified_name(&relation, &name) {
                        Ok(_) => {
                            // found an exact match on a qualified name so this is a table.column identifier
                            Ok(Expr::Column(Column {
                                relation: Some(relation),
                                name,
                            }))
                        }
                        Err(_) => {
                            if let Some(field) = schema.fields().iter().find(|f| f.name().eq(&relation)) {
                                // Access to a field of a column which is a structure, example: SELECT my_struct.key
                                Ok(Expr::GetIndexedField(GetIndexedField::new(
                                    Box::new(Expr::Column(field.qualified_column())),
                                    ScalarValue::Utf8(Some(name)),
                                )))
                            } else {
                                // table.column identifier
                                Ok(Expr::Column(Column {
                                    relation: Some(relation),
                                    name,
                                }))
                            }
                        }
                    }
                }
            }

            SQLExpr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                let expr = if let Some(e) = operand {
                    Some(Box::new(self.sql_expr_to_logical_expr(*e, schema, planner_context)?))
                } else {
                    None
                };
                let when_expr = conditions
                    .into_iter()
                    .map(|e| self.sql_expr_to_logical_expr(e, schema, planner_context))
                    .collect::<Result<Vec<_>>>()?;
                let then_expr = results
                    .into_iter()
                    .map(|e| self.sql_expr_to_logical_expr(e, schema, planner_context))
                    .collect::<Result<Vec<_>>>()?;
                let else_expr = if let Some(e) = else_result {
                    Some(Box::new(self.sql_expr_to_logical_expr(*e, schema, planner_context)?))
                } else {
                    None
                };

                Ok(Expr::Case(Case::new(
                    expr,
                    when_expr
                        .iter()
                        .zip(then_expr.iter())
                        .map(|(w, t)| (Box::new(w.to_owned()), Box::new(t.to_owned())))
                        .collect(),
                    else_expr,
                )))
            }

            SQLExpr::Cast {
                expr,
                data_type,
            } => Ok(Expr::Cast(Cast::new(
                Box::new(self.sql_expr_to_logical_expr(*expr, schema, planner_context)?),
                self.convert_data_type(&data_type)?,
            ))),

            SQLExpr::TryCast {
                expr,
                data_type,
            } => Ok(Expr::TryCast {
                expr: Box::new(self.sql_expr_to_logical_expr(*expr, schema, planner_context)?),
                data_type: self.convert_data_type(&data_type)?,
            }),

            SQLExpr::TypedString {
                data_type,
                value,
            } => Ok(Expr::Cast(Cast::new(
                Box::new(lit(value)),
                self.convert_data_type(&data_type)?,
            ))),

            SQLExpr::IsNull(expr) => Ok(Expr::IsNull(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)?,
            ))),

            SQLExpr::IsNotNull(expr) => Ok(Expr::IsNotNull(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)?,
            ))),

            SQLExpr::IsDistinctFrom(left, right) => Ok(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(self.sql_expr_to_logical_expr(*left, schema, planner_context)?),
                Operator::IsDistinctFrom,
                Box::new(self.sql_expr_to_logical_expr(*right, schema, planner_context)?),
            ))),

            SQLExpr::IsNotDistinctFrom(left, right) => Ok(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(self.sql_expr_to_logical_expr(*left, schema, planner_context)?),
                Operator::IsNotDistinctFrom,
                Box::new(self.sql_expr_to_logical_expr(*right, schema, planner_context)?),
            ))),

            SQLExpr::IsTrue(expr) => Ok(Expr::IsTrue(Box::new(self.sql_expr_to_logical_expr(*expr, schema, planner_context)?))),

            SQLExpr::IsFalse(expr) => Ok(Expr::IsFalse(Box::new(self.sql_expr_to_logical_expr(*expr, schema, planner_context)?))),

            SQLExpr::IsNotTrue(expr) => Ok(Expr::IsNotTrue(Box::new(self.sql_expr_to_logical_expr(*expr, schema, planner_context)?))),

            SQLExpr::IsNotFalse(expr) => Ok(Expr::IsNotFalse(Box::new(self.sql_expr_to_logical_expr(*expr, schema, planner_context)?))),

            SQLExpr::IsUnknown(expr) => Ok(Expr::IsUnknown(Box::new(self.sql_expr_to_logical_expr(*expr, schema, planner_context)?))),

            SQLExpr::IsNotUnknown(expr) => Ok(Expr::IsNotUnknown(Box::new(self.sql_expr_to_logical_expr(*expr, schema, planner_context)?))),

            SQLExpr::UnaryOp { op, expr } => self.parse_sql_unary_op(op, *expr, schema, planner_context),

            SQLExpr::Between {
                expr,
                negated,
                low,
                high,
            } => Ok(Expr::Between(Between::new(
                Box::new(self.sql_expr_to_logical_expr(*expr, schema, planner_context)?),
                negated,
                Box::new(self.sql_expr_to_logical_expr(*low, schema, planner_context)?),
                Box::new(self.sql_expr_to_logical_expr(*high, schema, planner_context)?),
            ))),

            SQLExpr::InList {
                expr,
                list,
                negated,
            } => {
                let list_expr = list
                    .into_iter()
                    .map(|e| self.sql_expr_to_logical_expr(e, schema, planner_context))
                    .collect::<Result<Vec<_>>>()?;

                Ok(Expr::InList {
                    expr: Box::new(self.sql_expr_to_logical_expr(*expr, schema, planner_context)?),
                    list: list_expr,
                    negated,
                })
            }

            SQLExpr::Like { negated, expr, pattern, escape_char } => {
                let pattern = self.sql_expr_to_logical_expr(*pattern, schema, planner_context)?;
                let pattern_type = pattern.get_type(schema)?;
                if pattern_type != DataType::Utf8 && pattern_type != DataType::Null {
                    return Err(DataFusionError::Plan(
                        "Invalid pattern in LIKE expression".to_string(),
                    ));
                }
                Ok(Expr::Like(Like::new(
                    negated,
                    Box::new(self.sql_expr_to_logical_expr(*expr, schema, planner_context)?),
                    Box::new(pattern),
                    escape_char,
                )))
            }

            SQLExpr::ILike { negated, expr, pattern, escape_char } => {
                let pattern = self.sql_expr_to_logical_expr(*pattern, schema, planner_context)?;
                let pattern_type = pattern.get_type(schema)?;
                if pattern_type != DataType::Utf8 && pattern_type != DataType::Null {
                    return Err(DataFusionError::Plan(
                        "Invalid pattern in ILIKE expression".to_string(),
                    ));
                }
                Ok(Expr::ILike(Like::new(
                    negated,
                    Box::new(self.sql_expr_to_logical_expr(*expr, schema, planner_context)?),
                    Box::new(pattern),
                    escape_char,
                )))
            }

            SQLExpr::SimilarTo { negated, expr, pattern, escape_char } => {
                let pattern = self.sql_expr_to_logical_expr(*pattern, schema, planner_context)?;
                let pattern_type = pattern.get_type(schema)?;
                if pattern_type != DataType::Utf8 && pattern_type != DataType::Null {
                    return Err(DataFusionError::Plan(
                        "Invalid pattern in SIMILAR TO expression".to_string(),
                    ));
                }
                Ok(Expr::SimilarTo(Like::new(
                    negated,
                    Box::new(self.sql_expr_to_logical_expr(*expr, schema, planner_context)?),
                    Box::new(pattern),
                    escape_char
                )))
            }

            SQLExpr::BinaryOp {
                left,
                op,
                right,
            } => self.parse_sql_binary_op(*left, op, *right, schema, planner_context),

            #[cfg(feature = "unicode_expressions")]
            SQLExpr::Substring {
                expr,
                substring_from,
                substring_for,
            } => {
                let args = match (substring_from, substring_for) {
                    (Some(from_expr), Some(for_expr)) => {
                        let arg = self.sql_expr_to_logical_expr(*expr, schema, planner_context)?;
                        let from_logic =
                            self.sql_expr_to_logical_expr(*from_expr, schema, planner_context)?;
                        let for_logic =
                            self.sql_expr_to_logical_expr(*for_expr, schema, planner_context)?;
                        vec![arg, from_logic, for_logic]
                    }
                    (Some(from_expr), None) => {
                        let arg = self.sql_expr_to_logical_expr(*expr, schema, planner_context)?;
                        let from_logic =
                            self.sql_expr_to_logical_expr(*from_expr, schema, planner_context)?;
                        vec![arg, from_logic]
                    }
                    (None, Some(for_expr)) => {
                        let arg = self.sql_expr_to_logical_expr(*expr, schema, planner_context)?;
                        let from_logic = Expr::Literal(ScalarValue::Int64(Some(1)));
                        let for_logic =
                            self.sql_expr_to_logical_expr(*for_expr, schema, planner_context)?;
                        vec![arg, from_logic, for_logic]
                    }
                    (None, None) => {
                        let orig_sql = SQLExpr::Substring {
                            expr,
                            substring_from: None,
                            substring_for: None,
                        };

                        return Err(DataFusionError::Plan(format!(
                            "Substring without for/from is not valid {:?}",
                            orig_sql
                        )));
                    }
                };


                Ok(Expr::ScalarFunction {
                    fun: BuiltinScalarFunction::Substr,
                    args,
                })
            }

            #[cfg(not(feature = "unicode_expressions"))]
            SQLExpr::Substring {
                ..
            } => {
                Err(DataFusionError::Internal(
                    "statement substring requires compilation with feature flag: unicode_expressions.".to_string()
                ))
            }

            SQLExpr::Trim { expr, trim_where, trim_what } => {
                let fun = match trim_where {
                    Some(TrimWhereField::Leading) => {
                        BuiltinScalarFunction::Ltrim
                    }
                    Some(TrimWhereField::Trailing) => {
                        BuiltinScalarFunction::Rtrim
                    }
                    Some(TrimWhereField::Both) => {
                        BuiltinScalarFunction::Btrim
                    }
                    None => BuiltinScalarFunction::Trim
                };
                let arg = self.sql_expr_to_logical_expr(*expr, schema, planner_context)?;
                let args = match trim_what {
                    Some(to_trim) => {
                        let to_trim = self.sql_expr_to_logical_expr(*to_trim, schema, planner_context)?;
                        vec![arg, to_trim]
                    }
                    None => vec![arg],
                };
                Ok(Expr::ScalarFunction { fun, args })
            }

            SQLExpr::AggregateExpressionWithFilter { expr, filter } => {
                match self.sql_expr_to_logical_expr(*expr, schema, planner_context)? {
                    Expr::AggregateFunction {
                        fun, args, distinct, ..
                    } => Ok(Expr::AggregateFunction { fun, args, distinct, filter: Some(Box::new(self.sql_expr_to_logical_expr(*filter, schema, planner_context)?)) }),
                    _ => Err(DataFusionError::Internal("AggregateExpressionWithFilter expression was not an AggregateFunction".to_string()))
                }
            }

            SQLExpr::Function(mut function) => {
                let name = if function.name.0.len() > 1 {
                    // DF doesn't handle compound identifiers
                    // (e.g. "foo.bar") for function names yet
                    function.name.to_string()
                } else {
                    normalize_ident(&function.name.0[0])
                };

                // first, check SQL reserved words
                if name == "rollup" {
                    let args = self.function_args_to_expr(function.args, schema)?;
                    return Ok(Expr::GroupingSet(GroupingSet::Rollup(args)));
                } else if name == "cube" {
                    let args = self.function_args_to_expr(function.args, schema)?;
                    return Ok(Expr::GroupingSet(GroupingSet::Cube(args)));
                }

                // next, scalar built-in
                if let Ok(fun) = BuiltinScalarFunction::from_str(&name) {
                    let args = self.function_args_to_expr(function.args, schema)?;
                    return Ok(Expr::ScalarFunction { fun, args });
                };

                // then, window function
                if let Some(window) = function.over.take() {
                    let partition_by = window
                        .partition_by
                        .into_iter()
                        .map(|e| self.sql_expr_to_logical_expr(e, schema, planner_context))
                        .collect::<Result<Vec<_>>>()?;
                    let order_by = window
                        .order_by
                        .into_iter()
                        .map(|e| self.order_by_to_sort_expr(e, schema))
                        .collect::<Result<Vec<_>>>()?;
                    let window_frame = window
                        .window_frame
                        .as_ref()
                        .map(|window_frame| {
                            let window_frame: WindowFrame = window_frame.clone().try_into()?;
                            if WindowFrameUnits::Range == window_frame.units
                                && order_by.len() != 1
                            {
                                Err(DataFusionError::Plan(format!(
                                    "With window frame of type RANGE, the order by expression must be of length 1, got {}", order_by.len())))
                            } else {
                                Ok(window_frame)
                            }
                        })
                        .transpose()?;
                    let window_frame = if let Some(window_frame) = window_frame {
                        window_frame
                    } else {
                        WindowFrame::new(!order_by.is_empty())
                    };
                    let fun = WindowFunction::from_str(&name)?;
                    match fun {
                        WindowFunction::AggregateFunction(
                            aggregate_fun,
                        ) => {
                            let (aggregate_fun, args) = self.aggregate_fn_to_expr(
                                aggregate_fun,
                                function.args,
                                schema,
                            )?;

                            return Ok(Expr::WindowFunction {
                                fun: WindowFunction::AggregateFunction(
                                    aggregate_fun,
                                ),
                                args,
                                partition_by,
                                order_by,
                                window_frame,
                            });
                        }
                        WindowFunction::BuiltInWindowFunction(
                            window_fun,
                        ) => {
                            return Ok(Expr::WindowFunction {
                                fun: WindowFunction::BuiltInWindowFunction(
                                    window_fun,
                                ),
                                args: self.function_args_to_expr(function.args, schema)?,
                                partition_by,
                                order_by,
                                window_frame,
                            });
                        }
                    }
                }

                // next, aggregate built-ins
                if let Ok(fun) = AggregateFunction::from_str(&name) {
                    let distinct = function.distinct;
                    let (fun, args) = self.aggregate_fn_to_expr(fun, function.args, schema)?;
                    return Ok(Expr::AggregateFunction {
                        fun,
                        distinct,
                        args,
                        filter: None,
                    });
                };

                // finally, user-defined functions (UDF) and UDAF
                match self.schema_provider.get_function_meta(&name) {
                    Some(fm) => {
                        let args = self.function_args_to_expr(function.args, schema)?;

                        Ok(Expr::ScalarUDF { fun: fm, args })
                    }
                    None => match self.schema_provider.get_aggregate_meta(&name) {
                        Some(fm) => {
                            let args = self.function_args_to_expr(function.args, schema)?;
                            Ok(Expr::AggregateUDF { fun: fm, args, filter: None })
                        }
                        _ => Err(DataFusionError::Plan(format!(
                            "Invalid function '{}'",
                            name
                        ))),
                    },
                }
            }

            SQLExpr::Floor{expr, field: _field} => {
                let fun = BuiltinScalarFunction::Floor;
                let args = vec![self.sql_expr_to_logical_expr(*expr, schema, planner_context)?];
                Ok(Expr::ScalarFunction { fun, args })
            }

            SQLExpr::Ceil{expr, field: _field} => {
                let fun = BuiltinScalarFunction::Ceil;
                let args = vec![self.sql_expr_to_logical_expr(*expr, schema, planner_context)?];
                Ok(Expr::ScalarFunction { fun, args })
            }

            SQLExpr::Nested(e) => self.sql_expr_to_logical_expr(*e, schema, planner_context),

            SQLExpr::Exists { subquery, negated } => self.parse_exists_subquery(*subquery, negated, schema, planner_context),

            SQLExpr::InSubquery { expr, subquery, negated } => self.parse_in_subquery(*expr, *subquery, negated, schema, planner_context),

            SQLExpr::Subquery(subquery) => self.parse_scalar_subquery(*subquery, schema, planner_context),

            SQLExpr::ArrayAgg(array_agg) => self.parse_array_agg(array_agg, schema, planner_context),

            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported ast node in sqltorel: {:?}",
                sql
            ))),
        }
    }

    fn parse_exists_subquery(
        &self,
        subquery: Query,
        negated: bool,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        Ok(Expr::Exists {
            subquery: Subquery {
                subquery: Arc::new(self.subquery_to_plan(
                    subquery,
                    planner_context,
                    input_schema,
                )?),
            },
            negated,
        })
    }

    fn parse_in_subquery(
        &self,
        expr: SQLExpr,
        subquery: Query,
        negated: bool,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        Ok(Expr::InSubquery {
            expr: Box::new(self.sql_to_rex(expr, input_schema, planner_context)?),
            subquery: Subquery {
                subquery: Arc::new(self.subquery_to_plan(
                    subquery,
                    planner_context,
                    input_schema,
                )?),
            },
            negated,
        })
    }

    fn parse_scalar_subquery(
        &self,
        subquery: Query,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        Ok(Expr::ScalarSubquery(Subquery {
            subquery: Arc::new(self.subquery_to_plan(
                subquery,
                planner_context,
                input_schema,
            )?),
        }))
    }

    fn parse_array_agg(
        &self,
        array_agg: ArrayAgg,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        // Some dialects have special syntax for array_agg. DataFusion only supports it like a function.
        let ArrayAgg {
            distinct,
            expr,
            order_by,
            limit,
            within_group,
        } = array_agg;

        if let Some(order_by) = order_by {
            return Err(DataFusionError::NotImplemented(format!(
                "ORDER BY not supported in ARRAY_AGG: {}",
                order_by
            )));
        }

        if let Some(limit) = limit {
            return Err(DataFusionError::NotImplemented(format!(
                "LIMIT not supported in ARRAY_AGG: {}",
                limit
            )));
        }

        if within_group {
            return Err(DataFusionError::NotImplemented(
                "WITHIN GROUP not supported in ARRAY_AGG".to_string(),
            ));
        }

        let args =
            vec![self.sql_expr_to_logical_expr(*expr, input_schema, planner_context)?];
        // next, aggregate built-ins
        let fun = AggregateFunction::ArrayAgg;

        Ok(Expr::AggregateFunction {
            fun,
            distinct,
            args,
            filter: None,
        })
    }

    fn function_args_to_expr(
        &self,
        args: Vec<FunctionArg>,
        schema: &DFSchema,
    ) -> Result<Vec<Expr>> {
        args.into_iter()
            .map(|a| {
                self.sql_fn_arg_to_logical_expr(a, schema, &mut PlannerContext::new())
            })
            .collect::<Result<Vec<Expr>>>()
    }

    fn aggregate_fn_to_expr(
        &self,
        fun: AggregateFunction,
        args: Vec<FunctionArg>,
        schema: &DFSchema,
    ) -> Result<(AggregateFunction, Vec<Expr>)> {
        let args = match fun {
            // Special case rewrite COUNT(*) to COUNT(constant)
            AggregateFunction::Count => args
                .into_iter()
                .map(|a| match a {
                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                        Ok(Expr::Literal(COUNT_STAR_EXPANSION.clone()))
                    }
                    _ => self.sql_fn_arg_to_logical_expr(
                        a,
                        schema,
                        &mut PlannerContext::new(),
                    ),
                })
                .collect::<Result<Vec<Expr>>>()?,
            _ => self.function_args_to_expr(args, schema)?,
        };

        Ok((fun, args))
    }

    fn sql_interval_to_expr(
        &self,
        value: SQLExpr,
        leading_field: Option<DateTimeField>,
        leading_precision: Option<u64>,
        last_field: Option<DateTimeField>,
        fractional_seconds_precision: Option<u64>,
    ) -> Result<Expr> {
        if leading_precision.is_some() {
            return Err(DataFusionError::NotImplemented(format!(
                "Unsupported Interval Expression with leading_precision {:?}",
                leading_precision
            )));
        }

        if last_field.is_some() {
            return Err(DataFusionError::NotImplemented(format!(
                "Unsupported Interval Expression with last_field {:?}",
                last_field
            )));
        }

        if fractional_seconds_precision.is_some() {
            return Err(DataFusionError::NotImplemented(format!(
                "Unsupported Interval Expression with fractional_seconds_precision {:?}",
                fractional_seconds_precision
            )));
        }

        // Only handle string exprs for now
        let value = match value {
            SQLExpr::Value(
                Value::SingleQuotedString(s) | Value::DoubleQuotedString(s),
            ) => s,
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported interval argument. Expected string literal, got: {:?}",
                    value
                )));
            }
        };

        let leading_field = leading_field
            .as_ref()
            .map(|dt| dt.to_string())
            .unwrap_or_else(|| "second".to_string());

        Ok(lit(parse_interval(&leading_field, &value)?))
    }

    fn show_variable_to_plan(&self, variable: &[Ident]) -> Result<LogicalPlan> {
        let variable = ObjectName(variable.to_vec()).to_string();

        if !self.has_table("information_schema", "df_settings") {
            return Err(DataFusionError::Plan(
                "SHOW [VARIABLE] is not supported unless information_schema is enabled"
                    .to_string(),
            ));
        }

        let variable_lower = variable.to_lowercase();

        let query = if variable_lower == "all" {
            // Add an ORDER BY so the output comes out in a consistent order
            String::from(
                "SELECT name, setting FROM information_schema.df_settings ORDER BY name",
            )
        } else if variable_lower == "timezone" || variable_lower == "time.zone" {
            // we could introduce alias in OptionDefinition if this string matching thing grows
            String::from("SELECT name, setting FROM information_schema.df_settings WHERE name = 'datafusion.execution.time_zone'")
        } else {
            format!(
                "SELECT name, setting FROM information_schema.df_settings WHERE name = '{}'",
                variable
            )
        };

        let mut rewrite = DFParser::parse_sql(&query)?;
        assert_eq!(rewrite.len(), 1);

        self.statement_to_plan(rewrite.pop_front().unwrap())
    }

    fn set_variable_to_plan(
        &self,
        local: bool,
        hivevar: bool,
        variable: &ObjectName,
        value: Vec<sqlparser::ast::Expr>,
    ) -> Result<LogicalPlan> {
        if local {
            return Err(DataFusionError::NotImplemented(
                "LOCAL is not supported".to_string(),
            ));
        }

        if hivevar {
            return Err(DataFusionError::NotImplemented(
                "HIVEVAR is not supported".to_string(),
            ));
        }

        let variable = variable.to_string();
        let mut variable_lower = variable.to_lowercase();

        if variable_lower == "timezone" || variable_lower == "time.zone" {
            // we could introduce alias in OptionDefinition if this string matching thing grows
            variable_lower = "datafusion.execution.time_zone".to_string();
        }

        // parse value string from Expr
        let value_string = match &value[0] {
            SQLExpr::Identifier(i) => i.to_string(),
            SQLExpr::Value(v) => match v {
                Value::SingleQuotedString(s) => s.to_string(),
                Value::Number(_, _) | Value::Boolean(_) => v.to_string(),
                Value::DoubleQuotedString(_)
                | Value::UnQuotedString(_)
                | Value::EscapedStringLiteral(_)
                | Value::NationalStringLiteral(_)
                | Value::HexStringLiteral(_)
                | Value::Null
                | Value::Placeholder(_) => {
                    return Err(DataFusionError::Plan(format!(
                        "Unsupported Value {}",
                        value[0]
                    )))
                }
            },
            // for capture signed number e.g. +8, -8
            SQLExpr::UnaryOp { op, expr } => match op {
                UnaryOperator::Plus => format!("+{}", expr),
                UnaryOperator::Minus => format!("-{}", expr),
                _ => {
                    return Err(DataFusionError::Plan(format!(
                        "Unsupported Value {}",
                        value[0]
                    )))
                }
            },
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "Unsupported Value {}",
                    value[0]
                )))
            }
        };

        Ok(LogicalPlan::SetVariable(SetVariable {
            variable: variable_lower,
            value: value_string,
            schema: DFSchemaRef::new(DFSchema::empty()),
        }))
    }

    fn show_columns_to_plan(
        &self,
        extended: bool,
        full: bool,
        sql_table_name: ObjectName,
        filter: Option<ShowStatementFilter>,
    ) -> Result<LogicalPlan> {
        if filter.is_some() {
            return Err(DataFusionError::Plan(
                "SHOW COLUMNS with WHERE or LIKE is not supported".to_string(),
            ));
        }

        if !self.has_table("information_schema", "columns") {
            return Err(DataFusionError::Plan(
                "SHOW COLUMNS is not supported unless information_schema is enabled"
                    .to_string(),
            ));
        }
        // Figure out the where clause
        let where_clause = object_name_to_qualifier(&sql_table_name);

        // Do a table lookup to verify the table exists
        let table_ref = object_name_to_table_reference(sql_table_name)?;
        let _ = self
            .schema_provider
            .get_table_provider((&table_ref).into())?;

        // treat both FULL and EXTENDED as the same
        let select_list = if full || extended {
            "*"
        } else {
            "table_catalog, table_schema, table_name, column_name, data_type, is_nullable"
        };

        let query = format!(
            "SELECT {} FROM information_schema.columns WHERE {}",
            select_list, where_clause
        );

        let mut rewrite = DFParser::parse_sql(&query)?;
        assert_eq!(rewrite.len(), 1);
        self.statement_to_plan(rewrite.pop_front().unwrap()) // length of rewrite is 1
    }

    fn show_create_table_to_plan(
        &self,
        sql_table_name: ObjectName,
    ) -> Result<LogicalPlan> {
        if !self.has_table("information_schema", "tables") {
            return Err(DataFusionError::Plan(
                "SHOW CREATE TABLE is not supported unless information_schema is enabled"
                    .to_string(),
            ));
        }
        // Figure out the where clause
        let where_clause = object_name_to_qualifier(&sql_table_name);

        // Do a table lookup to verify the table exists
        let table_ref = object_name_to_table_reference(sql_table_name)?;
        let _ = self
            .schema_provider
            .get_table_provider((&table_ref).into())?;

        let query = format!(
            "SELECT table_catalog, table_schema, table_name, definition FROM information_schema.views WHERE {}",
            where_clause
        );

        let mut rewrite = DFParser::parse_sql(&query)?;
        assert_eq!(rewrite.len(), 1);
        self.statement_to_plan(rewrite.pop_front().unwrap()) // length of rewrite is 1
    }

    /// Return true if there is a table provider available for "schema.table"
    fn has_table(&self, schema: &str, table: &str) -> bool {
        let tables_reference = TableReference::Partial { schema, table };
        self.schema_provider
            .get_table_provider(tables_reference)
            .is_ok()
    }

    fn sql_array_literal(
        &self,
        elements: Vec<SQLExpr>,
        schema: &DFSchema,
    ) -> Result<Expr> {
        let mut values = Vec::with_capacity(elements.len());

        for element in elements {
            let value = self.sql_expr_to_logical_expr(
                element,
                schema,
                &mut PlannerContext::new(),
            )?;
            match value {
                Expr::Literal(scalar) => {
                    values.push(scalar);
                }
                _ => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Arrays with elements other than literal are not supported: {}",
                        value
                    )));
                }
            }
        }

        let data_types: HashSet<DataType> =
            values.iter().map(|e| e.get_datatype()).collect();

        if data_types.is_empty() {
            Ok(lit(ScalarValue::new_list(None, DataType::Utf8)))
        } else if data_types.len() > 1 {
            Err(DataFusionError::NotImplemented(format!(
                "Arrays with different types are not supported: {:?}",
                data_types,
            )))
        } else {
            let data_type = values[0].get_datatype();

            Ok(lit(ScalarValue::new_list(Some(values), data_type)))
        }
    }

    /// Parse number in sql string, convert to Expr::Literal
    fn parse_sql_number(&self, n: &str) -> Result<Expr> {
        if n.find('E').is_some() {
            // not implemented yet
            // https://github.com/apache/arrow-datafusion/issues/3448
            Err(DataFusionError::NotImplemented(
                "sql numeric literals in scientific notation are not supported"
                    .to_string(),
            ))
        } else if let Ok(n) = n.parse::<i64>() {
            Ok(lit(n))
        } else if self.options.parse_float_as_decimal {
            // remove leading zeroes
            let str = n.trim_start_matches('0');
            if let Some(i) = str.find('.') {
                let p = str.len() - 1;
                let s = str.len() - i - 1;
                let str = str.replace('.', "");
                let n = str.parse::<i128>().map_err(|_| {
                    DataFusionError::from(ParserError(format!(
                        "Cannot parse {} as i128 when building decimal",
                        str
                    )))
                })?;
                Ok(Expr::Literal(ScalarValue::Decimal128(
                    Some(n),
                    p as u8,
                    s as i8,
                )))
            } else {
                let number = n.parse::<i128>().map_err(|_| {
                    DataFusionError::from(ParserError(format!(
                        "Cannot parse {} as i128 when building decimal",
                        n
                    )))
                })?;
                Ok(Expr::Literal(ScalarValue::Decimal128(Some(number), 38, 0)))
            }
        } else {
            n.parse::<f64>().map(lit).map_err(|_| {
                DataFusionError::from(ParserError(format!("Cannot parse {} as f64", n)))
            })
        }
    }

    fn convert_data_type(&self, sql_type: &SQLDataType) -> Result<DataType> {
        match sql_type {
            SQLDataType::Array(Some(inner_sql_type)) => {
                let data_type = self.convert_simple_data_type(inner_sql_type)?;

                Ok(DataType::List(Box::new(Field::new(
                    "field", data_type, true,
                ))))
            }
            SQLDataType::Array(None) => Err(DataFusionError::NotImplemented(
                "Arrays with unspecified type is not supported".to_string(),
            )),
            other => self.convert_simple_data_type(other),
        }
    }
    fn convert_simple_data_type(&self, sql_type: &SQLDataType) -> Result<DataType> {
        match sql_type {
            SQLDataType::Boolean => Ok(DataType::Boolean),
            SQLDataType::TinyInt(_) => Ok(DataType::Int8),
            SQLDataType::SmallInt(_) => Ok(DataType::Int16),
            SQLDataType::Int(_) | SQLDataType::Integer(_) => Ok(DataType::Int32),
            SQLDataType::BigInt(_) => Ok(DataType::Int64),
            SQLDataType::UnsignedTinyInt(_) => Ok(DataType::UInt8),
            SQLDataType::UnsignedSmallInt(_) => Ok(DataType::UInt16),
            SQLDataType::UnsignedInt(_) | SQLDataType::UnsignedInteger(_) => {
                Ok(DataType::UInt32)
            }
            SQLDataType::UnsignedBigInt(_) => Ok(DataType::UInt64),
            SQLDataType::Float(_) => Ok(DataType::Float32),
            SQLDataType::Real => Ok(DataType::Float32),
            SQLDataType::Double | SQLDataType::DoublePrecision => Ok(DataType::Float64),
            SQLDataType::Char(_)
            | SQLDataType::Varchar(_)
            | SQLDataType::Text
            | SQLDataType::String => Ok(DataType::Utf8),
            SQLDataType::Timestamp(None, tz_info) => {
                let tz = if matches!(tz_info, TimezoneInfo::Tz)
                    || matches!(tz_info, TimezoneInfo::WithTimeZone)
                {
                    // Timestamp With Time Zone
                    // INPUT : [SQLDataType]   TimestampTz + [RuntimeConfig] Time Zone
                    // OUTPUT: [ArrowDataType] Timestamp<TimeUnit, Some(Time Zone)>
                    match self
                        .schema_provider
                        .get_config_option("datafusion.execution.time_zone")
                    {
                        Some(ScalarValue::Utf8(s)) => s,
                        Some(v) => {
                            return Err(DataFusionError::Internal(format!(
                                "Incorrect data type for time_zone: {}",
                                v.get_datatype(),
                            )))
                        }
                        None => return Err(DataFusionError::Internal(
                            "Config Option datafusion.execution.time_zone doesn't exist"
                                .to_string(),
                        )),
                    }
                } else {
                    // Timestamp Without Time zone
                    None
                };
                Ok(DataType::Timestamp(TimeUnit::Nanosecond, tz))
            }
            SQLDataType::Date => Ok(DataType::Date32),
            SQLDataType::Time(None, tz_info) => {
                if matches!(tz_info, TimezoneInfo::None)
                    || matches!(tz_info, TimezoneInfo::WithoutTimeZone)
                {
                    Ok(DataType::Time64(TimeUnit::Nanosecond))
                } else {
                    // We dont support TIMETZ and TIME WITH TIME ZONE for now
                    Err(DataFusionError::NotImplemented(format!(
                        "Unsupported SQL type {:?}",
                        sql_type
                    )))
                }
            }
            SQLDataType::Numeric(exact_number_info)
            |SQLDataType::Decimal(exact_number_info) => {
                let (precision, scale) = match *exact_number_info {
                    ExactNumberInfo::None => (None, None),
                    ExactNumberInfo::Precision(precision) => (Some(precision), None),
                    ExactNumberInfo::PrecisionAndScale(precision, scale) => {
                        (Some(precision), Some(scale))
                    }
                };
                make_decimal_type(precision, scale)
            }
            SQLDataType::Bytea => Ok(DataType::Binary),
            // Explicitly list all other types so that if sqlparser
            // adds/changes the `SQLDataType` the compiler will tell us on upgrade
            // and avoid bugs like https://github.com/apache/arrow-datafusion/issues/3059
            SQLDataType::Nvarchar(_)
            | SQLDataType::Uuid
            | SQLDataType::Binary(_)
            | SQLDataType::Varbinary(_)
            | SQLDataType::Blob(_)
            | SQLDataType::Datetime(_)
            | SQLDataType::Interval
            | SQLDataType::Regclass
            | SQLDataType::Custom(_, _)
            | SQLDataType::Array(_)
            | SQLDataType::Enum(_)
            | SQLDataType::Set(_)
            | SQLDataType::MediumInt(_)
            | SQLDataType::UnsignedMediumInt(_)
            | SQLDataType::Character(_)
            | SQLDataType::CharacterVarying(_)
            | SQLDataType::CharVarying(_)
            | SQLDataType::CharacterLargeObject(_)
                | SQLDataType::CharLargeObject(_)
            // precision is not supported
                | SQLDataType::Timestamp(Some(_), _)
            // precision is not supported
                | SQLDataType::Time(Some(_), _)
                | SQLDataType::Dec(_)
            | SQLDataType::Clob(_) => Err(DataFusionError::NotImplemented(format!(
                "Unsupported SQL type {:?}",
                sql_type
            ))),
        }
    }
}

/// Create a [`OwnedTableReference`] after normalizing the specified ObjectName
///
/// Examples
/// ```text
/// ['foo']          -> Bare { table: "foo" }
/// ['"foo.bar"]]    -> Bare { table: "foo.bar" }
/// ['foo', 'Bar']   -> Partial { schema: "foo", table: "bar" } <-- note lower case "bar"
/// ['foo', 'bar']   -> Partial { schema: "foo", table: "bar" }
/// ['foo', '"Bar"'] -> Partial { schema: "foo", table: "Bar" }
/// ```
pub fn object_name_to_table_reference(
    object_name: ObjectName,
) -> Result<OwnedTableReference> {
    // use destructure to make it clear no fields on ObjectName are ignored
    let ObjectName(idents) = object_name;
    idents_to_table_reference(idents)
}

/// Create a [`OwnedTableReference`] after normalizing the specified identifier
fn idents_to_table_reference(idents: Vec<Ident>) -> Result<OwnedTableReference> {
    struct IdentTaker(Vec<Ident>);
    /// take the next identifier from the back of idents, panic'ing if
    /// there are none left
    impl IdentTaker {
        fn take(&mut self) -> String {
            let ident = self.0.pop().expect("no more identifiers");
            normalize_ident_owned(ident)
        }
    }

    let mut taker = IdentTaker(idents);

    match taker.0.len() {
        1 => {
            let table = taker.take();
            Ok(OwnedTableReference::Bare { table })
        }
        2 => {
            let table = taker.take();
            let schema = taker.take();
            Ok(OwnedTableReference::Partial { schema, table })
        }
        3 => {
            let table = taker.take();
            let schema = taker.take();
            let catalog = taker.take();
            Ok(OwnedTableReference::Full {
                catalog,
                schema,
                table,
            })
        }
        _ => Err(DataFusionError::Plan(format!(
            "Unsupported compound identifier '{:?}'",
            taker.0,
        ))),
    }
}

/// Construct a WHERE qualifier suitable for e.g. information_schema filtering
/// from the provided object identifiers (catalog, schema and table names).
pub fn object_name_to_qualifier(sql_table_name: &ObjectName) -> String {
    let columns = vec!["table_name", "table_schema", "table_catalog"].into_iter();
    sql_table_name
        .0
        .iter()
        .rev()
        .zip(columns)
        .map(|(ident, column_name)| {
            format!(r#"{} = '{}'"#, column_name, normalize_ident(ident))
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// Extracts equijoin ON condition be a single Eq or multiple conjunctive Eqs
/// Filters matching this pattern are added to `accum`
/// Filters that don't match this pattern are added to `accum_filter`
/// Examples:
/// ```text
/// foo = bar => accum=[(foo, bar)] accum_filter=[]
/// foo = bar AND bar = baz => accum=[(foo, bar), (bar, baz)] accum_filter=[]
/// foo = bar AND baz > 1 => accum=[(foo, bar)] accum_filter=[baz > 1]
///
/// For equijoin join key, assume we have tables -- a(c0, c1 c2) and b(c0, c1, c2):
/// (a.c0 = 10) => accum=[], accum_filter=[a.c0 = 10]
/// (a.c0 + 1 = b.c0 * 2) => accum=[(a.c0 + 1, b.c0 * 2)],  accum_filter=[]
/// (a.c0 + b.c0 = 10) =>  accum=[], accum_filter=[a.c0 + b.c0 = 10]
/// ```
fn extract_join_keys(
    expr: Expr,
    accum: &mut Vec<(Expr, Expr)>,
    accum_filter: &mut Vec<Expr>,
    left_schema: &Arc<DFSchema>,
    right_schema: &Arc<DFSchema>,
) -> Result<()> {
    match &expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::Eq => {
                let left = *left.clone();
                let right = *right.clone();
                let left_using_columns = left.to_columns()?;
                let right_using_columns = right.to_columns()?;

                // When one side key does not contain columns, we need move this expression to filter.
                // For example: a = 1, a = now() + 10.
                if left_using_columns.is_empty() || right_using_columns.is_empty() {
                    accum_filter.push(expr);
                    return Ok(());
                }

                // Checking left join key is from left schema, right join key is from right schema, or the opposite.
                let l_is_left = check_all_column_from_schema(
                    &left_using_columns,
                    left_schema.clone(),
                )?;
                let r_is_right = check_all_column_from_schema(
                    &right_using_columns,
                    right_schema.clone(),
                )?;

                let r_is_left_and_l_is_right = || {
                    let result = check_all_column_from_schema(
                        &right_using_columns,
                        left_schema.clone(),
                    )? && check_all_column_from_schema(
                        &left_using_columns,
                        right_schema.clone(),
                    )?;

                    Result::Ok(result)
                };

                let join_key_pair = match (l_is_left, r_is_right) {
                    (true, true) => Some((left, right)),
                    (_, _) if r_is_left_and_l_is_right()? => Some((right, left)),
                    _ => None,
                };

                if let Some((left_expr, right_expr)) = join_key_pair {
                    let left_expr_type = left_expr.get_type(left_schema)?;
                    let right_expr_type = right_expr.get_type(right_schema)?;

                    if can_hash(&left_expr_type) && can_hash(&right_expr_type) {
                        accum.push((left_expr, right_expr));
                    } else {
                        accum_filter.push(expr);
                    }
                } else {
                    accum_filter.push(expr);
                }
            }
            Operator::And => {
                if let Expr::BinaryExpr(BinaryExpr { left, op: _, right }) = expr {
                    extract_join_keys(
                        *left,
                        accum,
                        accum_filter,
                        left_schema,
                        right_schema,
                    )?;
                    extract_join_keys(
                        *right,
                        accum,
                        accum_filter,
                        left_schema,
                        right_schema,
                    )?;
                }
            }
            _other => {
                accum_filter.push(expr);
            }
        },
        _other => {
            accum_filter.push(expr);
        }
    }

    Ok(())
}

/// Ensure any column reference of the expression is unambiguous.
/// Assume we have two schema:
/// schema1: a, b ,c
/// schema2: a, d, e
///
/// `schema1.a + schema2.a` is unambiguous.
/// `a + d` is ambiguous, because `a` may come from schema1 or schema2.
fn ensure_any_column_reference_is_unambiguous(
    expr: &Expr,
    schemas: &[DFSchemaRef],
) -> Result<()> {
    if schemas.len() == 1 {
        return Ok(());
    }

    // extract columns both in one more schemas.
    let mut column_count_map: HashMap<&str, usize> = HashMap::new();
    schemas
        .iter()
        .flat_map(|schema| schema.fields())
        .for_each(|field| {
            column_count_map
                .entry(field.name())
                .and_modify(|v| *v += 1)
                .or_insert(1usize);
        });

    let duplicated_column_set = column_count_map
        .into_iter()
        .filter_map(|(column, count)| if count > 1 { Some(column) } else { None })
        .collect::<HashSet<&str>>();

    // check if there is ambiguous column.
    let using_columns = expr.to_columns()?;
    let ambiguous_column = using_columns.iter().find(|column| {
        column.relation.is_none() && duplicated_column_set.contains(column.name.as_str())
    });

    if let Some(column) = ambiguous_column {
        let maybe_field = schemas
            .iter()
            .flat_map(|schema| {
                schema
                    .field_with_unqualified_name(&column.name)
                    .map(|f| f.qualified_name())
                    .ok()
            })
            .collect::<Vec<_>>();
        Err(DataFusionError::Plan(format!(
            "reference \'{}\' is ambiguous, could be {};",
            column.name,
            maybe_field.join(","),
        )))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use sqlparser::dialect::{Dialect, GenericDialect, HiveDialect, MySqlDialect};

    use datafusion_common::assert_contains;

    use super::*;

    #[test]
    fn parse_decimals() {
        let test_data = [
            ("1", "Int64(1)"),
            ("001", "Int64(1)"),
            ("0.1", "Decimal128(Some(1),1,1)"),
            ("0.01", "Decimal128(Some(1),2,2)"),
            ("1.0", "Decimal128(Some(10),2,1)"),
            ("10.01", "Decimal128(Some(1001),4,2)"),
            (
                "10000000000000000000.00",
                "Decimal128(Some(1000000000000000000000),22,2)",
            ),
        ];
        for (a, b) in test_data {
            let sql = format!("SELECT {}", a);
            let expected = format!("Projection: {}\n  EmptyRelation", b);
            quick_test_with_options(
                &sql,
                &expected,
                ParserOptions {
                    parse_float_as_decimal: true,
                },
            );
        }
    }

    #[test]
    fn select_no_relation() {
        quick_test(
            "SELECT 1",
            "Projection: Int64(1)\
             \n  EmptyRelation",
        );
    }

    #[test]
    fn test_real_f32() {
        quick_test(
            "SELECT CAST(1.1 AS REAL)",
            "Projection: CAST(Float64(1.1) AS Float32)\
             \n  EmptyRelation",
        );
    }

    #[test]
    fn test_int_decimal_default() {
        quick_test(
            "SELECT CAST(10 AS DECIMAL)",
            "Projection: CAST(Int64(10) AS Decimal128(38, 10))\
             \n  EmptyRelation",
        );
    }

    #[test]
    fn test_int_decimal_no_scale() {
        quick_test(
            "SELECT CAST(10 AS DECIMAL(5))",
            "Projection: CAST(Int64(10) AS Decimal128(5, 0))\
             \n  EmptyRelation",
        );
    }

    #[test]
    fn test_tinyint() {
        quick_test(
            "SELECT CAST(6 AS TINYINT)",
            "Projection: CAST(Int64(6) AS Int8)\
             \n  EmptyRelation",
        );
    }

    #[test]
    fn cast_from_subquery() {
        quick_test(
            "SELECT CAST (a AS FLOAT) FROM (SELECT 1 AS a)",
            "Projection: CAST(a AS Float32)\
            \n  Projection: Int64(1) AS a\
            \n    EmptyRelation",
        );
    }

    #[test]
    fn cast_to_invalid_decimal_type() {
        // precision == 0
        {
            let sql = "SELECT CAST(10 AS DECIMAL(0))";
            let err = logical_plan(sql).expect_err("query should have failed");
            assert_eq!(
                r##"Internal("Decimal(precision = 0, scale = 0) should satisfy `0 < precision <= 38`, and `scale <= precision`.")"##,
                format!("{:?}", err)
            );
        }
        // precision > 38
        {
            let sql = "SELECT CAST(10 AS DECIMAL(39))";
            let err = logical_plan(sql).expect_err("query should have failed");
            assert_eq!(
                r##"Internal("Decimal(precision = 39, scale = 0) should satisfy `0 < precision <= 38`, and `scale <= precision`.")"##,
                format!("{:?}", err)
            );
        }
        // precision < scale
        {
            let sql = "SELECT CAST(10 AS DECIMAL(5, 10))";
            let err = logical_plan(sql).expect_err("query should have failed");
            assert_eq!(
                r##"Internal("Decimal(precision = 5, scale = 10) should satisfy `0 < precision <= 38`, and `scale <= precision`.")"##,
                format!("{:?}", err)
            );
        }
    }

    #[test]
    fn select_column_does_not_exist() {
        let sql = "SELECT doesnotexist FROM person";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_field_not_found(err, "doesnotexist");
    }

    #[test]
    fn select_repeated_column() {
        let sql = "SELECT age, age FROM person";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            r##"Plan("Projections require unique expression names but the expression \"person.age\" at position 0 and \"person.age\" at position 1 have the same name. Consider aliasing (\"AS\") one of them.")"##,
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_wildcard_with_repeated_column() {
        let sql = "SELECT *, age FROM person";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            r##"Plan("Projections require unique expression names but the expression \"person.age\" at position 3 and \"person.age\" at position 8 have the same name. Consider aliasing (\"AS\") one of them.")"##,
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_wildcard_with_repeated_column_but_is_aliased() {
        quick_test(
            "SELECT *, first_name AS fn from person",
            "Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀, person.first_name AS fn\
            \n  TableScan: person",
        );
    }

    #[test]
    fn select_scalar_func_with_literal_no_relation() {
        quick_test(
            "SELECT sqrt(9)",
            "Projection: sqrt(Int64(9))\
             \n  EmptyRelation",
        );
    }

    #[test]
    fn select_simple_filter() {
        let sql = "SELECT id, first_name, last_name \
                   FROM person WHERE state = 'CO'";
        let expected = "Projection: person.id, person.first_name, person.last_name\
                        \n  Filter: person.state = Utf8(\"CO\")\
                        \n    TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_filter_column_does_not_exist() {
        let sql = "SELECT first_name FROM person WHERE doesnotexist = 'A'";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_field_not_found(err, "doesnotexist");
    }

    #[test]
    fn select_filter_cannot_use_alias() {
        let sql = "SELECT first_name AS x FROM person WHERE x = 'A'";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_field_not_found(err, "x");
    }

    #[test]
    fn select_neg_filter() {
        let sql = "SELECT id, first_name, last_name \
                   FROM person WHERE NOT state";
        let expected = "Projection: person.id, person.first_name, person.last_name\
                        \n  Filter: NOT person.state\
                        \n    TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_compound_filter() {
        let sql = "SELECT id, first_name, last_name \
                   FROM person WHERE state = 'CO' AND age >= 21 AND age <= 65";
        let expected = "Projection: person.id, person.first_name, person.last_name\
            \n  Filter: person.state = Utf8(\"CO\") AND person.age >= Int64(21) AND person.age <= Int64(65)\
            \n    TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn test_timestamp_filter() {
        let sql =
            "SELECT state FROM person WHERE birth_date < CAST (158412331400600000 as timestamp)";

        let expected = "Projection: person.state\
            \n  Filter: person.birth_date < CAST(Int64(158412331400600000) AS Timestamp(Nanosecond, None))\
            \n    TableScan: person";

        quick_test(sql, expected);
    }

    #[test]
    fn test_date_filter() {
        let sql =
            "SELECT state FROM person WHERE birth_date < CAST ('2020-01-01' as date)";

        let expected = "Projection: person.state\
            \n  Filter: person.birth_date < CAST(Utf8(\"2020-01-01\") AS Date32)\
            \n    TableScan: person";

        quick_test(sql, expected);
    }

    #[test]
    fn select_all_boolean_operators() {
        let sql = "SELECT age, first_name, last_name \
                   FROM person \
                   WHERE age = 21 \
                   AND age != 21 \
                   AND age > 21 \
                   AND age >= 21 \
                   AND age < 65 \
                   AND age <= 65";
        let expected = "Projection: person.age, person.first_name, person.last_name\
                        \n  Filter: person.age = Int64(21) \
                        AND person.age != Int64(21) \
                        AND person.age > Int64(21) \
                        AND person.age >= Int64(21) \
                        AND person.age < Int64(65) \
                        AND person.age <= Int64(65)\
                        \n    TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_between() {
        let sql = "SELECT state FROM person WHERE age BETWEEN 21 AND 65";
        let expected = "Projection: person.state\
            \n  Filter: person.age BETWEEN Int64(21) AND Int64(65)\
            \n    TableScan: person";

        quick_test(sql, expected);
    }

    #[test]
    fn select_between_negated() {
        let sql = "SELECT state FROM person WHERE age NOT BETWEEN 21 AND 65";
        let expected = "Projection: person.state\
            \n  Filter: person.age NOT BETWEEN Int64(21) AND Int64(65)\
            \n    TableScan: person";

        quick_test(sql, expected);
    }

    #[test]
    fn select_nested() {
        let sql = "SELECT fn2, last_name
                   FROM (
                     SELECT fn1 as fn2, last_name, birth_date
                     FROM (
                       SELECT first_name AS fn1, last_name, birth_date, age
                       FROM person
                     ) AS a
                   ) AS b";
        let expected = "Projection: b.fn2, b.last_name\
        \n  SubqueryAlias: b\
        \n    Projection: a.fn1 AS fn2, a.last_name, a.birth_date\
        \n      SubqueryAlias: a\
        \n        Projection: person.first_name AS fn1, person.last_name, person.birth_date, person.age\
        \n          TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_nested_with_filters() {
        let sql = "SELECT fn1, age
                   FROM (
                     SELECT first_name AS fn1, age
                     FROM person
                     WHERE age > 20
                   ) AS a
                   WHERE fn1 = 'X' AND age < 30";

        let expected = "Projection: a.fn1, a.age\
        \n  Filter: a.fn1 = Utf8(\"X\") AND a.age < Int64(30)\
        \n    SubqueryAlias: a\
        \n      Projection: person.first_name AS fn1, person.age\
        \n        Filter: person.age > Int64(20)\
        \n          TableScan: person";

        quick_test(sql, expected);
    }

    #[test]
    fn table_with_column_alias() {
        let sql = "SELECT a, b, c
                   FROM lineitem l (a, b, c)";
        let expected = "Projection: a, b, c\
        \n  Projection: l.l_item_id AS a, l.l_description AS b, l.price AS c\
        \n    SubqueryAlias: l\
        \n      TableScan: lineitem";

        quick_test(sql, expected);
    }

    #[test]
    fn table_with_column_alias_number_cols() {
        let sql = "SELECT a, b, c
                   FROM lineitem l (a, b)";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Source table contains 3 columns but only 2 names given as column alias\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_with_ambiguous_column() {
        let sql = "SELECT id FROM person a, person b";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Internal(\"column reference id is ambiguous\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn join_with_ambiguous_column() {
        // This is legal.
        let sql = "SELECT id FROM person a join person b using(id)";
        let expected = "Projection: a.id\
                        \n  Inner Join: Using a.id = b.id\
                        \n    SubqueryAlias: a\
                        \n      TableScan: person\
                        \n    SubqueryAlias: b\
                        \n      TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_with_having() {
        let sql = "SELECT id, age
                   FROM person
                   HAVING age > 100 AND age < 200";
        let expected = "Projection: person.id, person.age\
                        \n  Filter: person.age > Int64(100) AND person.age < Int64(200)\
                        \n    TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_with_having_referencing_column_not_in_select() {
        let sql = "SELECT id, age
                   FROM person
                   HAVING first_name = 'M'";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"HAVING clause references column(s) not provided by the select: Expression person.first_name could not be resolved from available columns: person.id, person.age\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_with_having_referencing_column_nested_in_select_expression() {
        let sql = "SELECT id, age + 1
                   FROM person
                   HAVING age > 100";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"HAVING clause references column(s) not provided by the select: \
            Expression person.age could not be resolved from available columns: \
            person.id, person.age + Int64(1)\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_with_having_with_aggregate_not_in_select() {
        let sql = "SELECT first_name
                   FROM person
                   HAVING MAX(age) > 100";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Projection references non-aggregate values: Expression person.first_name could not be resolved from available columns: MAX(person.age)\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_aggregate_with_having_that_reuses_aggregate() {
        let sql = "SELECT MAX(age)
                   FROM person
                   HAVING MAX(age) < 30";
        let expected = "Projection: MAX(person.age)\
                        \n  Filter: MAX(person.age) < Int64(30)\
                        \n    Aggregate: groupBy=[[]], aggr=[[MAX(person.age)]]\
                        \n      TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_having_with_aggregate_not_in_select() {
        let sql = "SELECT MAX(age)
                   FROM person
                   HAVING MAX(first_name) > 'M'";
        let expected = "Projection: MAX(person.age)\
                        \n  Filter: MAX(person.first_name) > Utf8(\"M\")\
                        \n    Aggregate: groupBy=[[]], aggr=[[MAX(person.age), MAX(person.first_name)]]\
                        \n      TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_having_referencing_column_not_in_select() {
        let sql = "SELECT COUNT(*)
                   FROM person
                   HAVING first_name = 'M'";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"HAVING clause references non-aggregate values: \
            Expression person.first_name could not be resolved from available columns: \
            COUNT(UInt8(1))\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_aggregate_aliased_with_having_referencing_aggregate_by_its_alias() {
        let sql = "SELECT MAX(age) as max_age
                   FROM person
                   HAVING max_age < 30";
        // FIXME: add test for having in execution
        let expected = "Projection: MAX(person.age) AS max_age\
                        \n  Filter: MAX(person.age) < Int64(30)\
                        \n    Aggregate: groupBy=[[]], aggr=[[MAX(person.age)]]\
                        \n      TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_aliased_with_having_that_reuses_aggregate_but_not_by_its_alias() {
        let sql = "SELECT MAX(age) as max_age
                   FROM person
                   HAVING MAX(age) < 30";
        let expected = "Projection: MAX(person.age) AS max_age\
                        \n  Filter: MAX(person.age) < Int64(30)\
                        \n    Aggregate: groupBy=[[]], aggr=[[MAX(person.age)]]\
                        \n      TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having() {
        let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING first_name = 'M'";
        let expected = "Projection: person.first_name, MAX(person.age)\
                        \n  Filter: person.first_name = Utf8(\"M\")\
                        \n    Aggregate: groupBy=[[person.first_name]], aggr=[[MAX(person.age)]]\
                        \n      TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_and_where() {
        let sql = "SELECT first_name, MAX(age)
                   FROM person
                   WHERE id > 5
                   GROUP BY first_name
                   HAVING MAX(age) < 100";
        let expected = "Projection: person.first_name, MAX(person.age)\
                        \n  Filter: MAX(person.age) < Int64(100)\
                        \n    Aggregate: groupBy=[[person.first_name]], aggr=[[MAX(person.age)]]\
                        \n      Filter: person.id > Int64(5)\
                        \n        TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_and_where_filtering_on_aggregate_column(
    ) {
        let sql = "SELECT first_name, MAX(age)
                   FROM person
                   WHERE id > 5 AND age > 18
                   GROUP BY first_name
                   HAVING MAX(age) < 100";
        let expected = "Projection: person.first_name, MAX(person.age)\
                        \n  Filter: MAX(person.age) < Int64(100)\
                        \n    Aggregate: groupBy=[[person.first_name]], aggr=[[MAX(person.age)]]\
                        \n      Filter: person.id > Int64(5) AND person.age > Int64(18)\
                        \n        TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_using_column_by_alias() {
        let sql = "SELECT first_name AS fn, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 2 AND fn = 'M'";
        let expected = "Projection: person.first_name AS fn, MAX(person.age)\
                        \n  Filter: MAX(person.age) > Int64(2) AND person.first_name = Utf8(\"M\")\
                        \n    Aggregate: groupBy=[[person.first_name]], aggr=[[MAX(person.age)]]\
                        \n      TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_using_columns_with_and_without_their_aliases(
    ) {
        let sql = "SELECT first_name AS fn, MAX(age) AS max_age
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 2 AND max_age < 5 AND first_name = 'M' AND fn = 'N'";
        let expected = "Projection: person.first_name AS fn, MAX(person.age) AS max_age\
                        \n  Filter: MAX(person.age) > Int64(2) AND MAX(person.age) < Int64(5) AND person.first_name = Utf8(\"M\") AND person.first_name = Utf8(\"N\")\
                        \n    Aggregate: groupBy=[[person.first_name]], aggr=[[MAX(person.age)]]\
                        \n      TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_that_reuses_aggregate() {
        let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 100";
        let expected = "Projection: person.first_name, MAX(person.age)\
                        \n  Filter: MAX(person.age) > Int64(100)\
                        \n    Aggregate: groupBy=[[person.first_name]], aggr=[[MAX(person.age)]]\
                        \n      TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_referencing_column_not_in_group_by() {
        let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 10 AND last_name = 'M'";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"HAVING clause references non-aggregate values: \
            Expression person.last_name could not be resolved from available columns: \
            person.first_name, MAX(person.age)\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_that_reuses_aggregate_multiple_times() {
        let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 100 AND MAX(age) < 200";
        let expected = "Projection: person.first_name, MAX(person.age)\
                        \n  Filter: MAX(person.age) > Int64(100) AND MAX(person.age) < Int64(200)\
                        \n    Aggregate: groupBy=[[person.first_name]], aggr=[[MAX(person.age)]]\
                        \n      TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_using_aggreagate_not_in_select() {
        let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 100 AND MIN(id) < 50";
        let expected = "Projection: person.first_name, MAX(person.age)\
                        \n  Filter: MAX(person.age) > Int64(100) AND MIN(person.id) < Int64(50)\
                        \n    Aggregate: groupBy=[[person.first_name]], aggr=[[MAX(person.age), MIN(person.id)]]\
                        \n      TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_aliased_with_group_by_with_having_referencing_aggregate_by_its_alias(
    ) {
        let sql = "SELECT first_name, MAX(age) AS max_age
                   FROM person
                   GROUP BY first_name
                   HAVING max_age > 100";
        let expected = "Projection: person.first_name, MAX(person.age) AS max_age\
                        \n  Filter: MAX(person.age) > Int64(100)\
                        \n    Aggregate: groupBy=[[person.first_name]], aggr=[[MAX(person.age)]]\
                        \n      TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_compound_aliased_with_group_by_with_having_referencing_compound_aggregate_by_its_alias(
    ) {
        let sql = "SELECT first_name, MAX(age) + 1 AS max_age_plus_one
                   FROM person
                   GROUP BY first_name
                   HAVING max_age_plus_one > 100";
        let expected =
            "Projection: person.first_name, MAX(person.age) + Int64(1) AS max_age_plus_one\
                        \n  Filter: MAX(person.age) + Int64(1) > Int64(100)\
                        \n    Aggregate: groupBy=[[person.first_name]], aggr=[[MAX(person.age)]]\
                        \n      TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_using_derived_column_aggreagate_not_in_select(
    ) {
        let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 100 AND MIN(id - 2) < 50";
        let expected = "Projection: person.first_name, MAX(person.age)\
                        \n  Filter: MAX(person.age) > Int64(100) AND MIN(person.id - Int64(2)) < Int64(50)\
                        \n    Aggregate: groupBy=[[person.first_name]], aggr=[[MAX(person.age), MIN(person.id - Int64(2))]]\
                        \n      TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_using_count_star_not_in_select() {
        let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 100 AND COUNT(*) < 50";
        let expected = "Projection: person.first_name, MAX(person.age)\
                        \n  Filter: MAX(person.age) > Int64(100) AND COUNT(UInt8(1)) < Int64(50)\
                        \n    Aggregate: groupBy=[[person.first_name]], aggr=[[MAX(person.age), COUNT(UInt8(1))]]\
                        \n      TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_binary_expr() {
        let sql = "SELECT age + salary from person";
        let expected = "Projection: person.age + person.salary\
                        \n  TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_binary_expr_nested() {
        let sql = "SELECT (age + salary)/2 from person";
        let expected = "Projection: (person.age + person.salary) / Int64(2)\
                        \n  TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_wildcard_with_groupby() {
        quick_test(
            r#"SELECT * FROM person GROUP BY id, first_name, last_name, age, state, salary, birth_date, "😀""#,
            "Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀\
             \n  Aggregate: groupBy=[[person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀]], aggr=[[]]\
             \n    TableScan: person",
        );
        quick_test(
            "SELECT * FROM (SELECT first_name, last_name FROM person) AS a GROUP BY first_name, last_name",
            "Projection: a.first_name, a.last_name\
            \n  Aggregate: groupBy=[[a.first_name, a.last_name]], aggr=[[]]\
            \n    SubqueryAlias: a\
            \n      Projection: person.first_name, person.last_name\
            \n        TableScan: person",
        );
    }

    #[test]
    fn select_simple_aggregate() {
        quick_test(
            "SELECT MIN(age) FROM person",
            "Projection: MIN(person.age)\
            \n  Aggregate: groupBy=[[]], aggr=[[MIN(person.age)]]\
            \n    TableScan: person",
        );
    }

    #[test]
    fn test_sum_aggregate() {
        quick_test(
            "SELECT SUM(age) from person",
            "Projection: SUM(person.age)\
            \n  Aggregate: groupBy=[[]], aggr=[[SUM(person.age)]]\
            \n    TableScan: person",
        );
    }

    #[test]
    fn select_simple_aggregate_column_does_not_exist() {
        let sql = "SELECT MIN(doesnotexist) FROM person";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_field_not_found(err, "doesnotexist");
    }

    #[test]
    fn select_simple_aggregate_repeated_aggregate() {
        let sql = "SELECT MIN(age), MIN(age) FROM person";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            r##"Plan("Projections require unique expression names but the expression \"MIN(person.age)\" at position 0 and \"MIN(person.age)\" at position 1 have the same name. Consider aliasing (\"AS\") one of them.")"##,
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_simple_aggregate_repeated_aggregate_with_single_alias() {
        quick_test(
            "SELECT MIN(age), MIN(age) AS a FROM person",
            "Projection: MIN(person.age), MIN(person.age) AS a\
             \n  Aggregate: groupBy=[[]], aggr=[[MIN(person.age)]]\
             \n    TableScan: person",
        );
    }

    #[test]
    fn select_simple_aggregate_repeated_aggregate_with_unique_aliases() {
        quick_test(
            "SELECT MIN(age) AS a, MIN(age) AS b FROM person",
            "Projection: MIN(person.age) AS a, MIN(person.age) AS b\
             \n  Aggregate: groupBy=[[]], aggr=[[MIN(person.age)]]\
             \n    TableScan: person",
        );
    }

    #[test]
    fn select_from_typed_string_values() {
        quick_test(
            "SELECT col1, col2 FROM (VALUES (TIMESTAMP '2021-06-10 17:01:00Z', DATE '2004-04-09')) as t (col1, col2)",
            "Projection: col1, col2\
            \n  Projection: t.column1 AS col1, t.column2 AS col2\
            \n    SubqueryAlias: t\
            \n      Values: (CAST(Utf8(\"2021-06-10 17:01:00Z\") AS Timestamp(Nanosecond, None)), CAST(Utf8(\"2004-04-09\") AS Date32))"
        );
    }

    #[test]
    fn select_simple_aggregate_repeated_aggregate_with_repeated_aliases() {
        let sql = "SELECT MIN(age) AS a, MIN(age) AS a FROM person";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            r##"Plan("Projections require unique expression names but the expression \"MIN(person.age) AS a\" at position 0 and \"MIN(person.age) AS a\" at position 1 have the same name. Consider aliasing (\"AS\") one of them.")"##,
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby() {
        quick_test(
            "SELECT state, MIN(age), MAX(age) FROM person GROUP BY state",
            "Projection: person.state, MIN(person.age), MAX(person.age)\
            \n  Aggregate: groupBy=[[person.state]], aggr=[[MIN(person.age), MAX(person.age)]]\
            \n    TableScan: person",
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_with_aliases() {
        quick_test(
            "SELECT state AS a, MIN(age) AS b FROM person GROUP BY state",
            "Projection: person.state AS a, MIN(person.age) AS b\
             \n  Aggregate: groupBy=[[person.state]], aggr=[[MIN(person.age)]]\
             \n    TableScan: person",
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_with_aliases_repeated() {
        let sql = "SELECT state AS a, MIN(age) AS a FROM person GROUP BY state";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            r##"Plan("Projections require unique expression names but the expression \"person.state AS a\" at position 0 and \"MIN(person.age) AS a\" at position 1 have the same name. Consider aliasing (\"AS\") one of them.")"##,
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_column_unselected() {
        quick_test(
            "SELECT MIN(age), MAX(age) FROM person GROUP BY state",
            "Projection: MIN(person.age), MAX(person.age)\
             \n  Aggregate: groupBy=[[person.state]], aggr=[[MIN(person.age), MAX(person.age)]]\
             \n    TableScan: person",
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_and_column_in_group_by_does_not_exist() {
        let sql = "SELECT SUM(age) FROM person GROUP BY doesnotexist";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!("Schema error: No field named 'doesnotexist'. Valid fields are 'SUM(person.age)', \
        'person'.'id', 'person'.'first_name', 'person'.'last_name', 'person'.'age', 'person'.'state', \
        'person'.'salary', 'person'.'birth_date', 'person'.'😀'.", format!("{}", err));
    }

    #[test]
    fn select_simple_aggregate_with_groupby_and_column_in_aggregate_does_not_exist() {
        let sql = "SELECT SUM(doesnotexist) FROM person GROUP BY first_name";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_field_not_found(err, "doesnotexist");
    }

    #[test]
    fn select_interval_out_of_range() {
        let sql = "SELECT INTERVAL '100000000000000000 day'";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            r#"NotImplemented("Interval field value out of range: \"100000000000000000 day\"")"#,
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_array_no_common_type() {
        let sql = "SELECT [1, true, null]";
        let err = logical_plan(sql).expect_err("query should have failed");

        // HashSet doesn't guarantee order
        assert_contains!(
            err.to_string(),
            r#"Arrays with different types are not supported: "#
        );
    }

    #[test]
    fn recursive_ctes() {
        let sql = "
        WITH RECURSIVE numbers AS (
              select 1 as n
            UNION ALL
              select n + 1 FROM numbers WHERE N < 10
        )
        select * from numbers;";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            r#"NotImplemented("Recursive CTEs are not supported")"#,
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_array_non_literal_type() {
        let sql = "SELECT [now()]";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            r#"NotImplemented("Arrays with elements other than literal are not supported: now()")"#,
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_and_column_is_in_aggregate_and_groupby() {
        quick_test(
            "SELECT MAX(first_name) FROM person GROUP BY first_name",
            "Projection: MAX(person.first_name)\
             \n  Aggregate: groupBy=[[person.first_name]], aggr=[[MAX(person.first_name)]]\
             \n    TableScan: person",
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_can_use_positions() {
        quick_test(
            "SELECT state, age AS b, COUNT(1) FROM person GROUP BY 1, 2",
            "Projection: person.state, person.age AS b, COUNT(Int64(1))\
             \n  Aggregate: groupBy=[[person.state, person.age]], aggr=[[COUNT(Int64(1))]]\
             \n    TableScan: person",
        );
        quick_test(
            "SELECT state, age AS b, COUNT(1) FROM person GROUP BY 2, 1",
            "Projection: person.state, person.age AS b, COUNT(Int64(1))\
             \n  Aggregate: groupBy=[[person.age, person.state]], aggr=[[COUNT(Int64(1))]]\
             \n    TableScan: person",
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_position_out_of_range() {
        let sql = "SELECT state, MIN(age) FROM person GROUP BY 0";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Projection references non-aggregate values: Expression person.state could not be resolved from available columns: Int64(0), MIN(person.age)\")",
            format!("{:?}", err)
        );

        let sql2 = "SELECT state, MIN(age) FROM person GROUP BY 5";
        let err2 = logical_plan(sql2).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Projection references non-aggregate values: Expression person.state could not be resolved from available columns: Int64(5), MIN(person.age)\")",
            format!("{:?}", err2)
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_can_use_alias() {
        quick_test(
            "SELECT state AS a, MIN(age) AS b FROM person GROUP BY a",
            "Projection: person.state AS a, MIN(person.age) AS b\
             \n  Aggregate: groupBy=[[person.state]], aggr=[[MIN(person.age)]]\
             \n    TableScan: person",
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_aggregate_repeated() {
        let sql = "SELECT state, MIN(age), MIN(age) FROM person GROUP BY state";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            r##"Plan("Projections require unique expression names but the expression \"MIN(person.age)\" at position 1 and \"MIN(person.age)\" at position 2 have the same name. Consider aliasing (\"AS\") one of them.")"##,
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_aggregate_repeated_and_one_has_alias() {
        quick_test(
            "SELECT state, MIN(age), MIN(age) AS ma FROM person GROUP BY state",
            "Projection: person.state, MIN(person.age), MIN(person.age) AS ma\
             \n  Aggregate: groupBy=[[person.state]], aggr=[[MIN(person.age)]]\
             \n    TableScan: person",
        )
    }

    #[test]
    fn select_simple_aggregate_with_groupby_non_column_expression_unselected() {
        quick_test(
            "SELECT MIN(first_name) FROM person GROUP BY age + 1",
            "Projection: MIN(person.first_name)\
             \n  Aggregate: groupBy=[[person.age + Int64(1)]], aggr=[[MIN(person.first_name)]]\
             \n    TableScan: person",
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_non_column_expression_selected_and_resolvable(
    ) {
        quick_test(
            "SELECT age + 1, MIN(first_name) FROM person GROUP BY age + 1",
            "Projection: person.age + Int64(1), MIN(person.first_name)\
             \n  Aggregate: groupBy=[[person.age + Int64(1)]], aggr=[[MIN(person.first_name)]]\
             \n    TableScan: person",
        );
        quick_test(
            "SELECT MIN(first_name), age + 1 FROM person GROUP BY age + 1",
            "Projection: MIN(person.first_name), person.age + Int64(1)\
             \n  Aggregate: groupBy=[[person.age + Int64(1)]], aggr=[[MIN(person.first_name)]]\
             \n    TableScan: person",
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_non_column_expression_nested_and_resolvable()
    {
        quick_test(
            "SELECT ((age + 1) / 2) * (age + 1), MIN(first_name) FROM person GROUP BY age + 1",
            "Projection: person.age + Int64(1) / Int64(2) * person.age + Int64(1), MIN(person.first_name)\
             \n  Aggregate: groupBy=[[person.age + Int64(1)]], aggr=[[MIN(person.first_name)]]\
             \n    TableScan: person",
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_non_column_expression_nested_and_not_resolvable(
    ) {
        // The query should fail, because age + 9 is not in the group by.
        let sql =
            "SELECT ((age + 1) / 2) * (age + 9), MIN(first_name) FROM person GROUP BY age + 1";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Projection references non-aggregate values: Expression person.age could not be resolved from available columns: person.age + Int64(1), MIN(person.first_name)\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_non_column_expression_and_its_column_selected(
    ) {
        let sql = "SELECT age, MIN(first_name) FROM person GROUP BY age + 1";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!("Plan(\"Projection references non-aggregate values: Expression person.age could not be resolved from available columns: person.age + Int64(1), MIN(person.first_name)\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_simple_aggregate_nested_in_binary_expr_with_groupby() {
        quick_test(
            "SELECT state, MIN(age) < 10 FROM person GROUP BY state",
            "Projection: person.state, MIN(person.age) < Int64(10)\
             \n  Aggregate: groupBy=[[person.state]], aggr=[[MIN(person.age)]]\
             \n    TableScan: person",
        );
    }

    #[test]
    fn select_simple_aggregate_and_nested_groupby_column() {
        quick_test(
            "SELECT age + 1, MAX(first_name) FROM person GROUP BY age",
            "Projection: person.age + Int64(1), MAX(person.first_name)\
             \n  Aggregate: groupBy=[[person.age]], aggr=[[MAX(person.first_name)]]\
             \n    TableScan: person",
        );
    }

    #[test]
    fn select_aggregate_compounded_with_groupby_column() {
        quick_test(
            "SELECT age + MIN(salary) FROM person GROUP BY age",
            "Projection: person.age + MIN(person.salary)\
             \n  Aggregate: groupBy=[[person.age]], aggr=[[MIN(person.salary)]]\
             \n    TableScan: person",
        );
    }

    #[test]
    fn select_aggregate_with_non_column_inner_expression_with_groupby() {
        quick_test(
            "SELECT state, MIN(age + 1) FROM person GROUP BY state",
            "Projection: person.state, MIN(person.age + Int64(1))\
            \n  Aggregate: groupBy=[[person.state]], aggr=[[MIN(person.age + Int64(1))]]\
            \n    TableScan: person",
        );
    }

    #[test]
    fn test_wildcard() {
        quick_test(
            "SELECT * from person",
            "Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀\
            \n  TableScan: person",
        );
    }

    #[test]
    fn select_count_one() {
        let sql = "SELECT COUNT(1) FROM person";
        let expected = "Projection: COUNT(Int64(1))\
                        \n  Aggregate: groupBy=[[]], aggr=[[COUNT(Int64(1))]]\
                        \n    TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_count_column() {
        let sql = "SELECT COUNT(id) FROM person";
        let expected = "Projection: COUNT(person.id)\
                        \n  Aggregate: groupBy=[[]], aggr=[[COUNT(person.id)]]\
                        \n    TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_approx_median() {
        let sql = "SELECT approx_median(age) FROM person";
        let expected = "Projection: APPROXMEDIAN(person.age)\
                        \n  Aggregate: groupBy=[[]], aggr=[[APPROXMEDIAN(person.age)]]\
                        \n    TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_scalar_func() {
        let sql = "SELECT sqrt(age) FROM person";
        let expected = "Projection: sqrt(person.age)\
                        \n  TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aliased_scalar_func() {
        let sql = "SELECT sqrt(person.age) AS square_people FROM person";
        let expected = "Projection: sqrt(person.age) AS square_people\
                        \n  TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_where_nullif_division() {
        let sql = "SELECT c3/(c4+c5) \
                   FROM aggregate_test_100 WHERE c3/nullif(c4+c5, 0) > 0.1";
        let expected = "Projection: aggregate_test_100.c3 / (aggregate_test_100.c4 + aggregate_test_100.c5)\
            \n  Filter: aggregate_test_100.c3 / nullif(aggregate_test_100.c4 + aggregate_test_100.c5, Int64(0)) > Float64(0.1)\
            \n    TableScan: aggregate_test_100";
        quick_test(sql, expected);
    }

    #[test]
    fn select_where_with_negative_operator() {
        let sql = "SELECT c3 FROM aggregate_test_100 WHERE c3 > -0.1 AND -c4 > 0";
        let expected = "Projection: aggregate_test_100.c3\
            \n  Filter: aggregate_test_100.c3 > Float64(-0.1) AND (- aggregate_test_100.c4) > Int64(0)\
            \n    TableScan: aggregate_test_100";
        quick_test(sql, expected);
    }

    #[test]
    fn select_where_with_positive_operator() {
        let sql = "SELECT c3 FROM aggregate_test_100 WHERE c3 > +0.1 AND +c4 > 0";
        let expected = "Projection: aggregate_test_100.c3\
            \n  Filter: aggregate_test_100.c3 > Float64(0.1) AND aggregate_test_100.c4 > Int64(0)\
            \n    TableScan: aggregate_test_100";
        quick_test(sql, expected);
    }

    #[test]
    fn select_order_by_index() {
        let sql = "SELECT id FROM person ORDER BY 1";
        let expected = "Sort: person.id ASC NULLS LAST\
                        \n  Projection: person.id\
                        \n    TableScan: person";

        quick_test(sql, expected);
    }

    #[test]
    fn select_order_by_multiple_index() {
        let sql = "SELECT id, state, age FROM person ORDER BY 1, 3";
        let expected = "Sort: person.id ASC NULLS LAST, person.age ASC NULLS LAST\
                        \n  Projection: person.id, person.state, person.age\
                        \n    TableScan: person";

        quick_test(sql, expected);
    }

    #[test]
    fn select_order_by_index_of_0() {
        let sql = "SELECT id FROM person ORDER BY 0";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Order by index starts at 1 for column indexes\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_order_by_index_oob() {
        let sql = "SELECT id FROM person ORDER BY 2";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Order by column out of bounds, specified: 2, max: 1\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_order_by() {
        let sql = "SELECT id FROM person ORDER BY id";
        let expected = "Sort: person.id ASC NULLS LAST\
                        \n  Projection: person.id\
                        \n    TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_order_by_desc() {
        let sql = "SELECT id FROM person ORDER BY id DESC";
        let expected = "Sort: person.id DESC NULLS FIRST\
                        \n  Projection: person.id\
                        \n    TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn select_order_by_nulls_last() {
        quick_test(
            "SELECT id FROM person ORDER BY id DESC NULLS LAST",
            "Sort: person.id DESC NULLS LAST\
            \n  Projection: person.id\
            \n    TableScan: person",
        );

        quick_test(
            "SELECT id FROM person ORDER BY id NULLS LAST",
            "Sort: person.id ASC NULLS LAST\
            \n  Projection: person.id\
            \n    TableScan: person",
        );
    }

    #[test]
    fn select_group_by() {
        let sql = "SELECT state FROM person GROUP BY state";
        let expected = "Projection: person.state\
                        \n  Aggregate: groupBy=[[person.state]], aggr=[[]]\
                        \n    TableScan: person";

        quick_test(sql, expected);
    }

    #[test]
    fn select_group_by_columns_not_in_select() {
        let sql = "SELECT MAX(age) FROM person GROUP BY state";
        let expected = "Projection: MAX(person.age)\
                        \n  Aggregate: groupBy=[[person.state]], aggr=[[MAX(person.age)]]\
                        \n    TableScan: person";

        quick_test(sql, expected);
    }

    #[test]
    fn select_group_by_count_star() {
        let sql = "SELECT state, COUNT(*) FROM person GROUP BY state";
        let expected = "Projection: person.state, COUNT(UInt8(1))\
                        \n  Aggregate: groupBy=[[person.state]], aggr=[[COUNT(UInt8(1))]]\
                        \n    TableScan: person";

        quick_test(sql, expected);
    }

    #[test]
    fn select_group_by_needs_projection() {
        let sql = "SELECT COUNT(state), state FROM person GROUP BY state";
        let expected = "\
        Projection: COUNT(person.state), person.state\
        \n  Aggregate: groupBy=[[person.state]], aggr=[[COUNT(person.state)]]\
        \n    TableScan: person";

        quick_test(sql, expected);
    }

    #[test]
    fn select_7480_1() {
        let sql = "SELECT c1, MIN(c12) FROM aggregate_test_100 GROUP BY c1, c13";
        let expected = "Projection: aggregate_test_100.c1, MIN(aggregate_test_100.c12)\
                       \n  Aggregate: groupBy=[[aggregate_test_100.c1, aggregate_test_100.c13]], aggr=[[MIN(aggregate_test_100.c12)]]\
                       \n    TableScan: aggregate_test_100";
        quick_test(sql, expected);
    }

    #[test]
    fn select_7480_2() {
        let sql = "SELECT c1, c13, MIN(c12) FROM aggregate_test_100 GROUP BY c1";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Projection references non-aggregate values: \
            Expression aggregate_test_100.c13 could not be resolved from available columns: \
            aggregate_test_100.c1, MIN(aggregate_test_100.c12)\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn create_external_table_csv() {
        let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV LOCATION 'foo.csv'";
        let expected = "CreateExternalTable: Bare { table: \"t\" }";
        quick_test(sql, expected);
    }

    #[test]
    fn create_external_table_custom() {
        let sql = "CREATE EXTERNAL TABLE dt STORED AS DELTATABLE LOCATION 's3://bucket/schema/table';";
        let expected = r#"CreateExternalTable: Bare { table: "dt" }"#;
        quick_test(sql, expected);
    }

    #[test]
    fn create_external_table_csv_no_schema() {
        let sql = "CREATE EXTERNAL TABLE t STORED AS CSV LOCATION 'foo.csv'";
        let expected = "CreateExternalTable: Bare { table: \"t\" }";
        quick_test(sql, expected);
    }

    #[test]
    fn create_external_table_with_compression_type() {
        // positive case
        let sqls = vec![
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV COMPRESSION TYPE GZIP LOCATION 'foo.csv.gz'",
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV COMPRESSION TYPE BZIP2 LOCATION 'foo.csv.bz2'",
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS JSON COMPRESSION TYPE GZIP LOCATION 'foo.json.gz'",
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS JSON COMPRESSION TYPE BZIP2 LOCATION 'foo.json.bz2'",
        ];
        for sql in sqls {
            let expected = "CreateExternalTable: Bare { table: \"t\" }";
            quick_test(sql, expected);
        }

        // negative case
        let sqls = vec![
            "CREATE EXTERNAL TABLE t STORED AS AVRO COMPRESSION TYPE GZIP LOCATION 'foo.avro'",
            "CREATE EXTERNAL TABLE t STORED AS AVRO COMPRESSION TYPE BZIP2 LOCATION 'foo.avro'",
            "CREATE EXTERNAL TABLE t STORED AS PARQUET COMPRESSION TYPE GZIP LOCATION 'foo.parquet'",
            "CREATE EXTERNAL TABLE t STORED AS PARQUET COMPRESSION TYPE BZIP2 LOCATION 'foo.parquet'",
        ];
        for sql in sqls {
            let err = logical_plan(sql).expect_err("query should have failed");
            assert_eq!(
                "Plan(\"File compression type can be specified for CSV/JSON files.\")",
                format!("{:?}", err)
            );
        }
    }

    #[test]
    fn create_external_table_parquet() {
        let sql =
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS PARQUET LOCATION 'foo.parquet'";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Column definitions can not be specified for PARQUET files.\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn create_external_table_parquet_no_schema() {
        let sql = "CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION 'foo.parquet'";
        let expected = "CreateExternalTable: Bare { table: \"t\" }";
        quick_test(sql, expected);
    }

    #[test]
    fn equijoin_explicit_syntax() {
        let sql = "SELECT id, order_id \
            FROM person \
            JOIN orders \
            ON id = customer_id";
        let expected = "Projection: person.id, orders.order_id\
        \n  Inner Join: person.id = orders.customer_id\
        \n    TableScan: person\
        \n    TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn equijoin_with_condition() {
        let sql = "SELECT id, order_id \
            FROM person \
            JOIN orders \
            ON id = customer_id AND order_id > 1 ";
        let expected = "Projection: person.id, orders.order_id\
            \n  Inner Join: person.id = orders.customer_id Filter: orders.order_id > Int64(1)\
            \n    TableScan: person\
            \n    TableScan: orders";

        quick_test(sql, expected);
    }

    #[test]
    fn left_equijoin_with_conditions() {
        let sql = "SELECT id, order_id \
            FROM person \
            LEFT JOIN orders \
            ON id = customer_id AND order_id > 1 AND age < 30";
        let expected = "Projection: person.id, orders.order_id\
            \n  Left Join: person.id = orders.customer_id Filter: orders.order_id > Int64(1) AND person.age < Int64(30)\
            \n    TableScan: person\
            \n    TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn right_equijoin_with_conditions() {
        let sql = "SELECT id, order_id \
            FROM person \
            RIGHT JOIN orders \
            ON id = customer_id AND id > 1 AND order_id < 100";
        let expected = "Projection: person.id, orders.order_id\
            \n  Right Join: person.id = orders.customer_id Filter: person.id > Int64(1) AND orders.order_id < Int64(100)\
            \n    TableScan: person\
            \n    TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn full_equijoin_with_conditions() {
        let sql = "SELECT id, order_id \
            FROM person \
            FULL JOIN orders \
            ON id = customer_id AND id > 1 AND order_id < 100";
        let expected = "Projection: person.id, orders.order_id\
        \n  Full Join: person.id = orders.customer_id Filter: person.id > Int64(1) AND orders.order_id < Int64(100)\
        \n    TableScan: person\
        \n    TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn join_with_table_name() {
        let sql = "SELECT id, order_id \
            FROM person \
            JOIN orders \
            ON person.id = orders.customer_id";
        let expected = "Projection: person.id, orders.order_id\
        \n  Inner Join: person.id = orders.customer_id\
        \n    TableScan: person\
        \n    TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn join_with_using() {
        let sql = "SELECT person.first_name, id \
            FROM person \
            JOIN person as person2 \
            USING (id)";
        let expected = "Projection: person.first_name, person.id\
        \n  Inner Join: Using person.id = person2.id\
        \n    TableScan: person\
        \n    SubqueryAlias: person2\
        \n      TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn project_wildcard_on_join_with_using() {
        let sql = "SELECT * \
            FROM lineitem \
            JOIN lineitem as lineitem2 \
            USING (l_item_id)";
        let expected = "Projection: lineitem.l_item_id, lineitem.l_description, lineitem.price, lineitem2.l_description, lineitem2.price\
        \n  Inner Join: Using lineitem.l_item_id = lineitem2.l_item_id\
        \n    TableScan: lineitem\
        \n    SubqueryAlias: lineitem2\
        \n      TableScan: lineitem";
        quick_test(sql, expected);
    }

    #[test]
    fn equijoin_explicit_syntax_3_tables() {
        let sql = "SELECT id, order_id, l_description \
            FROM person \
            JOIN orders ON id = customer_id \
            JOIN lineitem ON o_item_id = l_item_id";
        let expected = "Projection: person.id, orders.order_id, lineitem.l_description\
            \n  Inner Join: orders.o_item_id = lineitem.l_item_id\
            \n    Inner Join: person.id = orders.customer_id\
            \n      TableScan: person\
            \n      TableScan: orders\
            \n    TableScan: lineitem";
        quick_test(sql, expected);
    }

    #[test]
    fn boolean_literal_in_condition_expression() {
        let sql = "SELECT order_id \
        FROM orders \
        WHERE delivered = false OR delivered = true";
        let expected = "Projection: orders.order_id\
            \n  Filter: orders.delivered = Boolean(false) OR orders.delivered = Boolean(true)\
            \n    TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn union() {
        let sql = "SELECT order_id from orders UNION SELECT order_id FROM orders";
        let expected = "\
        Distinct:\
        \n  Union\
        \n    Projection: orders.order_id\
        \n      TableScan: orders\
        \n    Projection: orders.order_id\
        \n      TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn union_all() {
        let sql = "SELECT order_id from orders UNION ALL SELECT order_id FROM orders";
        let expected = "Union\
            \n  Projection: orders.order_id\
            \n    TableScan: orders\
            \n  Projection: orders.order_id\
            \n    TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn union_4_combined_in_one() {
        let sql = "SELECT order_id from orders
                    UNION ALL SELECT order_id FROM orders
                    UNION ALL SELECT order_id FROM orders
                    UNION ALL SELECT order_id FROM orders";
        let expected = "Union\
            \n  Projection: orders.order_id\
            \n    TableScan: orders\
            \n  Projection: orders.order_id\
            \n    TableScan: orders\
            \n  Projection: orders.order_id\
            \n    TableScan: orders\
            \n  Projection: orders.order_id\
            \n    TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn union_with_different_column_names() {
        let sql = "SELECT order_id from orders UNION ALL SELECT customer_id FROM orders";
        let expected = "Union\
            \n  Projection: orders.order_id\
            \n    TableScan: orders\
            \n  Projection: orders.customer_id\
            \n    TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn union_values_with_no_alias() {
        let sql = "SELECT 1, 2 UNION ALL SELECT 3, 4";
        let expected = "Union\
            \n  Projection: Int64(1) AS Int64(1), Int64(2) AS Int64(2)\
            \n    EmptyRelation\
            \n  Projection: Int64(3) AS Int64(1), Int64(4) AS Int64(2)\
            \n    EmptyRelation";
        quick_test(sql, expected);
    }

    #[test]
    fn union_with_incompatible_data_type() {
        let sql = "SELECT interval '1 year 1 day' UNION ALL SELECT 1";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"UNION Column Int64(1) (type: Int64) is \
            not compatible with column IntervalMonthDayNano\
            (\\\"950737950189618795196236955648\\\") \
            (type: Interval(MonthDayNano))\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn union_with_different_decimal_data_types() {
        let sql = "SELECT 1 a UNION ALL SELECT 1.1 a";
        let expected = "Union\
            \n  Projection: CAST(Int64(1) AS Float64) AS a\
            \n    EmptyRelation\
            \n  Projection: Float64(1.1) AS a\
            \n    EmptyRelation";
        quick_test(sql, expected);
    }

    #[test]
    fn union_with_null() {
        let sql = "SELECT NULL a UNION ALL SELECT 1.1 a";
        let expected = "Union\
            \n  Projection: CAST(NULL AS Float64) AS a\
            \n    EmptyRelation\
            \n  Projection: Float64(1.1) AS a\
            \n    EmptyRelation";
        quick_test(sql, expected);
    }

    #[test]
    fn union_with_float_and_string() {
        let sql = "SELECT 'a' a UNION ALL SELECT 1.1 a";
        let expected = "Union\
            \n  Projection: Utf8(\"a\") AS a\
            \n    EmptyRelation\
            \n  Projection: CAST(Float64(1.1) AS Utf8) AS a\
            \n    EmptyRelation";
        quick_test(sql, expected);
    }

    #[test]
    fn union_with_multiply_cols() {
        let sql = "SELECT 'a' a, 1 b UNION ALL SELECT 1.1 a, 1.1 b";
        let expected = "Union\
            \n  Projection: Utf8(\"a\") AS a, CAST(Int64(1) AS Float64) AS b\
            \n    EmptyRelation\
            \n  Projection: CAST(Float64(1.1) AS Utf8) AS a, Float64(1.1) AS b\
            \n    EmptyRelation";
        quick_test(sql, expected);
    }

    #[test]
    fn sorted_union_with_different_types_and_group_by() {
        let sql = "SELECT a FROM (select 1 a) x GROUP BY 1 UNION ALL (SELECT a FROM (select 1.1 a) x GROUP BY 1) ORDER BY 1";
        let expected = "Sort: a ASC NULLS LAST\
        \n  Union\
        \n    Projection: CAST(x.a AS Float64) AS a\
        \n      Aggregate: groupBy=[[x.a]], aggr=[[]]\
        \n        SubqueryAlias: x\
        \n          Projection: Int64(1) AS a\
        \n            EmptyRelation\
        \n    Projection: x.a\
        \n      Aggregate: groupBy=[[x.a]], aggr=[[]]\
        \n        SubqueryAlias: x\
        \n          Projection: Float64(1.1) AS a\
        \n            EmptyRelation";
        quick_test(sql, expected);
    }

    #[test]
    fn union_with_binary_expr_and_cast() {
        let sql = "SELECT cast(0.0 + a as integer) FROM (select 1 a) x GROUP BY 1 UNION ALL (SELECT 2.1 + a FROM (select 1 a) x GROUP BY 1)";
        let expected = "Union\
        \n  Projection: CAST(Float64(0) + x.a AS Float64) AS Float64(0) + x.a\
        \n    Aggregate: groupBy=[[CAST(Float64(0) + x.a AS Int32)]], aggr=[[]]\
        \n      SubqueryAlias: x\
        \n        Projection: Int64(1) AS a\
        \n          EmptyRelation\
        \n  Projection: Float64(2.1) + x.a\
        \n    Aggregate: groupBy=[[Float64(2.1) + x.a]], aggr=[[]]\
        \n      SubqueryAlias: x\
        \n        Projection: Int64(1) AS a\
        \n          EmptyRelation";
        quick_test(sql, expected);
    }

    #[test]
    fn union_with_aliases() {
        let sql = "SELECT a as a1 FROM (select 1 a) x GROUP BY 1 UNION ALL (SELECT a as a1 FROM (select 1.1 a) x GROUP BY 1)";
        let expected = "Union\
        \n  Projection: CAST(x.a AS Float64) AS a1\
        \n    Aggregate: groupBy=[[x.a]], aggr=[[]]\
        \n      SubqueryAlias: x\
        \n        Projection: Int64(1) AS a\
        \n          EmptyRelation\
        \n  Projection: x.a AS a1\
        \n    Aggregate: groupBy=[[x.a]], aggr=[[]]\
        \n      SubqueryAlias: x\
        \n        Projection: Float64(1.1) AS a\
        \n          EmptyRelation";
        quick_test(sql, expected);
    }

    #[test]
    fn union_with_incompatible_data_types() {
        let sql = "SELECT 'a' a UNION ALL SELECT true a";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"UNION Column a (type: Boolean) is not compatible with column a (type: Utf8)\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn empty_over() {
        let sql = "SELECT order_id, MAX(order_id) OVER () from orders";
        let expected = "\
        Projection: orders.order_id, MAX(orders.order_id) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
        \n  WindowAggr: windowExpr=[[MAX(orders.order_id) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n    TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn empty_over_with_alias() {
        let sql = "SELECT order_id oid, MAX(order_id) OVER () max_oid from orders";
        let expected = "\
        Projection: orders.order_id AS oid, MAX(orders.order_id) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS max_oid\
        \n  WindowAggr: windowExpr=[[MAX(orders.order_id) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n    TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn empty_over_dup_with_alias() {
        let sql = "SELECT order_id oid, MAX(order_id) OVER () max_oid, MAX(order_id) OVER () max_oid_dup from orders";
        let expected = "\
        Projection: orders.order_id AS oid, MAX(orders.order_id) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS max_oid, MAX(orders.order_id) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS max_oid_dup\
        \n  WindowAggr: windowExpr=[[MAX(orders.order_id) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n    TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn empty_over_dup_with_different_sort() {
        let sql = "SELECT order_id oid, MAX(order_id) OVER (), MAX(order_id) OVER (ORDER BY order_id) from orders";
        let expected = "\
        Projection: orders.order_id AS oid, MAX(orders.order_id) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, MAX(orders.order_id) ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
        \n  WindowAggr: windowExpr=[[MAX(orders.order_id) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n    WindowAggr: windowExpr=[[MAX(orders.order_id) ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n      TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn empty_over_plus() {
        let sql = "SELECT order_id, MAX(qty * 1.1) OVER () from orders";
        let expected = "\
        Projection: orders.order_id, MAX(orders.qty * Float64(1.1)) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
        \n  WindowAggr: windowExpr=[[MAX(orders.qty * Float64(1.1)) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n    TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn empty_over_multiple() {
        let sql =
            "SELECT order_id, MAX(qty) OVER (), min(qty) over (), aVg(qty) OVER () from orders";
        let expected = "\
        Projection: orders.order_id, MAX(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, MIN(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, AVG(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
        \n  WindowAggr: windowExpr=[[MAX(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, MIN(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, AVG(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n    TableScan: orders";
        quick_test(sql, expected);
    }

    /// psql result
    /// ```text
    ///                               QUERY PLAN
    /// ----------------------------------------------------------------------
    /// WindowAgg  (cost=69.83..87.33 rows=1000 width=8)
    ///   ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
    ///         Sort Key: order_id
    ///         ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
    /// ```
    #[test]
    fn over_partition_by() {
        let sql = "SELECT order_id, MAX(qty) OVER (PARTITION BY order_id) from orders";
        let expected = "\
        Projection: orders.order_id, MAX(orders.qty) PARTITION BY [orders.order_id] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
        \n  WindowAggr: windowExpr=[[MAX(orders.qty) PARTITION BY [orders.order_id] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n    TableScan: orders";
        quick_test(sql, expected);
    }

    /// psql result
    /// ```text
    ///                                     QUERY PLAN
    /// ----------------------------------------------------------------------------------
    /// WindowAgg  (cost=137.16..154.66 rows=1000 width=12)
    /// ->  Sort  (cost=137.16..139.66 rows=1000 width=12)
    ///         Sort Key: order_id
    ///         ->  WindowAgg  (cost=69.83..87.33 rows=1000 width=12)
    ///             ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
    ///                     Sort Key: order_id DESC
    ///                     ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
    /// ```
    #[test]
    fn over_order_by() {
        let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY order_id), MIN(qty) OVER (ORDER BY order_id DESC) from orders";
        let expected = "\
        Projection: orders.order_id, MAX(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, MIN(orders.qty) ORDER BY [orders.order_id DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
        \n  WindowAggr: windowExpr=[[MAX(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n    WindowAggr: windowExpr=[[MIN(orders.qty) ORDER BY [orders.order_id DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n      TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn over_order_by_with_window_frame_double_end() {
        let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY order_id ROWS BETWEEN 3 PRECEDING and 3 FOLLOWING), MIN(qty) OVER (ORDER BY order_id DESC) from orders";
        let expected = "\
        Projection: orders.order_id, MAX(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING, MIN(orders.qty) ORDER BY [orders.order_id DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
        \n  WindowAggr: windowExpr=[[MAX(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING]]\
        \n    WindowAggr: windowExpr=[[MIN(orders.qty) ORDER BY [orders.order_id DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n      TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn over_order_by_with_window_frame_single_end() {
        let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY order_id ROWS 3 PRECEDING), MIN(qty) OVER (ORDER BY order_id DESC) from orders";
        let expected = "\
        Projection: orders.order_id, MAX(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] ROWS BETWEEN 3 PRECEDING AND CURRENT ROW, MIN(orders.qty) ORDER BY [orders.order_id DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
        \n  WindowAggr: windowExpr=[[MAX(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] ROWS BETWEEN 3 PRECEDING AND CURRENT ROW]]\
        \n    WindowAggr: windowExpr=[[MIN(orders.qty) ORDER BY [orders.order_id DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n      TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn over_order_by_with_window_frame_range_order_by_check() {
        let sql =
            "SELECT order_id, MAX(qty) OVER (RANGE UNBOUNDED PRECEDING) from orders";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"With window frame of type RANGE, the order by expression must be of length 1, got 0\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn over_order_by_with_window_frame_range_order_by_check_2() {
        let sql =
            "SELECT order_id, MAX(qty) OVER (ORDER BY order_id, qty RANGE UNBOUNDED PRECEDING) from orders";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"With window frame of type RANGE, the order by expression must be of length 1, got 2\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn over_order_by_with_window_frame_single_end_groups() {
        let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY order_id GROUPS 3 PRECEDING), MIN(qty) OVER (ORDER BY order_id DESC) from orders";
        let expected = "\
        Projection: orders.order_id, MAX(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] GROUPS BETWEEN 3 PRECEDING AND CURRENT ROW, MIN(orders.qty) ORDER BY [orders.order_id DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
        \n  WindowAggr: windowExpr=[[MAX(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] GROUPS BETWEEN 3 PRECEDING AND CURRENT ROW]]\
        \n    WindowAggr: windowExpr=[[MIN(orders.qty) ORDER BY [orders.order_id DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n      TableScan: orders";
        quick_test(sql, expected);
    }

    /// psql result
    /// ```text
    ///                                     QUERY PLAN
    /// -----------------------------------------------------------------------------------
    /// WindowAgg  (cost=142.16..162.16 rows=1000 width=16)
    ///   ->  Sort  (cost=142.16..144.66 rows=1000 width=16)
    ///         Sort Key: order_id
    ///         ->  WindowAgg  (cost=72.33..92.33 rows=1000 width=16)
    ///               ->  Sort  (cost=72.33..74.83 rows=1000 width=12)
    ///                     Sort Key: ((order_id + 1))
    ///                     ->  Seq Scan on orders  (cost=0.00..22.50 rows=1000 width=12)
    /// ```
    #[test]
    fn over_order_by_two_sort_keys() {
        let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY order_id), MIN(qty) OVER (ORDER BY (order_id + 1)) from orders";
        let expected = "\
        Projection: orders.order_id, MAX(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, MIN(orders.qty) ORDER BY [orders.order_id + Int64(1) ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
        \n  WindowAggr: windowExpr=[[MAX(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n    WindowAggr: windowExpr=[[MIN(orders.qty) ORDER BY [orders.order_id + Int64(1) ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n      TableScan: orders";
        quick_test(sql, expected);
    }

    /// psql result
    /// ```text
    ///                                        QUERY PLAN
    /// ----------------------------------------------------------------------------------------
    /// WindowAgg  (cost=139.66..172.16 rows=1000 width=24)
    ///   ->  WindowAgg  (cost=139.66..159.66 rows=1000 width=16)
    ///         ->  Sort  (cost=139.66..142.16 rows=1000 width=12)
    ///               Sort Key: qty, order_id
    ///               ->  WindowAgg  (cost=69.83..89.83 rows=1000 width=12)
    ///                     ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
    ///                           Sort Key: order_id, qty
    ///                           ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
    /// ```
    #[test]
    fn over_order_by_sort_keys_sorting() {
        let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY qty, order_id), SUM(qty) OVER (), MIN(qty) OVER (ORDER BY order_id, qty) from orders";
        let expected = "\
        Projection: orders.order_id, MAX(orders.qty) ORDER BY [orders.qty ASC NULLS LAST, orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, SUM(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, MIN(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST, orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
        \n  WindowAggr: windowExpr=[[SUM(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n    WindowAggr: windowExpr=[[MAX(orders.qty) ORDER BY [orders.qty ASC NULLS LAST, orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n      WindowAggr: windowExpr=[[MIN(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST, orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n        TableScan: orders";
        quick_test(sql, expected);
    }

    /// psql result
    /// ```text
    ///                                     QUERY PLAN
    /// ----------------------------------------------------------------------------------
    /// WindowAgg  (cost=69.83..117.33 rows=1000 width=24)
    ///   ->  WindowAgg  (cost=69.83..104.83 rows=1000 width=16)
    ///         ->  WindowAgg  (cost=69.83..89.83 rows=1000 width=12)
    ///               ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
    ///                     Sort Key: order_id, qty
    ///                     ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
    /// ```
    ///
    /// FIXME: for now we are not detecting prefix of sorting keys in order to save one sort exec phase
    #[test]
    fn over_order_by_sort_keys_sorting_prefix_compacting() {
        let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY order_id), SUM(qty) OVER (), MIN(qty) OVER (ORDER BY order_id, qty) from orders";
        let expected = "\
        Projection: orders.order_id, MAX(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, SUM(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, MIN(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST, orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
        \n  WindowAggr: windowExpr=[[SUM(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n    WindowAggr: windowExpr=[[MAX(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n      WindowAggr: windowExpr=[[MIN(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST, orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n        TableScan: orders";
        quick_test(sql, expected);
    }

    /// psql result
    /// ```text
    ///                                        QUERY PLAN
    /// ----------------------------------------------------------------------------------------
    /// WindowAgg  (cost=139.66..172.16 rows=1000 width=24)
    ///   ->  WindowAgg  (cost=139.66..159.66 rows=1000 width=16)
    ///         ->  Sort  (cost=139.66..142.16 rows=1000 width=12)
    ///               Sort Key: order_id, qty
    ///               ->  WindowAgg  (cost=69.83..89.83 rows=1000 width=12)
    ///                     ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
    ///                           Sort Key: qty, order_id
    ///                           ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
    /// ```
    ///
    /// FIXME: for now we are not detecting prefix of sorting keys in order to re-arrange with global
    /// sort
    #[test]
    fn over_order_by_sort_keys_sorting_global_order_compacting() {
        let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY qty, order_id), SUM(qty) OVER (), MIN(qty) OVER (ORDER BY order_id, qty) from orders ORDER BY order_id";
        let expected = "\
        Sort: orders.order_id ASC NULLS LAST\
        \n  Projection: orders.order_id, MAX(orders.qty) ORDER BY [orders.qty ASC NULLS LAST, orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, SUM(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, MIN(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST, orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
        \n    WindowAggr: windowExpr=[[SUM(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n      WindowAggr: windowExpr=[[MAX(orders.qty) ORDER BY [orders.qty ASC NULLS LAST, orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n        WindowAggr: windowExpr=[[MIN(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST, orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n          TableScan: orders";
        quick_test(sql, expected);
    }

    /// psql result
    /// ```text
    ///                               QUERY PLAN
    /// ----------------------------------------------------------------------
    /// WindowAgg  (cost=69.83..89.83 rows=1000 width=12)
    ///   ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
    ///         Sort Key: order_id, qty
    ///         ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
    /// ```
    #[test]
    fn over_partition_by_order_by() {
        let sql =
            "SELECT order_id, MAX(qty) OVER (PARTITION BY order_id ORDER BY qty) from orders";
        let expected = "\
        Projection: orders.order_id, MAX(orders.qty) PARTITION BY [orders.order_id] ORDER BY [orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
        \n  WindowAggr: windowExpr=[[MAX(orders.qty) PARTITION BY [orders.order_id] ORDER BY [orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n    TableScan: orders";
        quick_test(sql, expected);
    }

    /// psql result
    /// ```text
    ///                               QUERY PLAN
    /// ----------------------------------------------------------------------
    /// WindowAgg  (cost=69.83..89.83 rows=1000 width=12)
    ///   ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
    ///         Sort Key: order_id, qty
    ///         ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
    /// ```
    #[test]
    fn over_partition_by_order_by_no_dup() {
        let sql =
            "SELECT order_id, MAX(qty) OVER (PARTITION BY order_id, qty ORDER BY qty) from orders";
        let expected = "\
        Projection: orders.order_id, MAX(orders.qty) PARTITION BY [orders.order_id, orders.qty] ORDER BY [orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
        \n  WindowAggr: windowExpr=[[MAX(orders.qty) PARTITION BY [orders.order_id, orders.qty] ORDER BY [orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n    TableScan: orders";
        quick_test(sql, expected);
    }

    /// psql result
    /// ```text
    ///                                     QUERY PLAN
    /// ----------------------------------------------------------------------------------
    /// WindowAgg  (cost=142.16..162.16 rows=1000 width=16)
    ///   ->  Sort  (cost=142.16..144.66 rows=1000 width=12)
    ///         Sort Key: qty, order_id
    ///         ->  WindowAgg  (cost=69.83..92.33 rows=1000 width=12)
    ///               ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
    ///                     Sort Key: order_id, qty
    ///                     ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
    /// ```
    #[test]
    fn over_partition_by_order_by_mix_up() {
        let sql =
            "SELECT order_id, MAX(qty) OVER (PARTITION BY order_id, qty ORDER BY qty), MIN(qty) OVER (PARTITION BY qty ORDER BY order_id) from orders";
        let expected = "\
        Projection: orders.order_id, MAX(orders.qty) PARTITION BY [orders.order_id, orders.qty] ORDER BY [orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, MIN(orders.qty) PARTITION BY [orders.qty] ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
        \n  WindowAggr: windowExpr=[[MIN(orders.qty) PARTITION BY [orders.qty] ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n    WindowAggr: windowExpr=[[MAX(orders.qty) PARTITION BY [orders.order_id, orders.qty] ORDER BY [orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n      TableScan: orders";
        quick_test(sql, expected);
    }

    /// psql result
    /// ```text
    ///                                  QUERY PLAN
    /// -----------------------------------------------------------------------------
    /// WindowAgg  (cost=69.83..109.83 rows=1000 width=24)
    ///   ->  WindowAgg  (cost=69.83..92.33 rows=1000 width=20)
    ///         ->  Sort  (cost=69.83..72.33 rows=1000 width=16)
    ///               Sort Key: order_id, qty, price
    ///               ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=16)
    /// ```
    /// FIXME: for now we are not detecting prefix of sorting keys in order to save one sort exec phase
    #[test]
    fn over_partition_by_order_by_mix_up_prefix() {
        let sql =
            "SELECT order_id, MAX(qty) OVER (PARTITION BY order_id ORDER BY qty), MIN(qty) OVER (PARTITION BY order_id, qty ORDER BY price) from orders";
        let expected = "\
        Projection: orders.order_id, MAX(orders.qty) PARTITION BY [orders.order_id] ORDER BY [orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, MIN(orders.qty) PARTITION BY [orders.order_id, orders.qty] ORDER BY [orders.price ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
        \n  WindowAggr: windowExpr=[[MAX(orders.qty) PARTITION BY [orders.order_id] ORDER BY [orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n    WindowAggr: windowExpr=[[MIN(orders.qty) PARTITION BY [orders.order_id, orders.qty] ORDER BY [orders.price ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n      TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn approx_median_window() {
        let sql =
            "SELECT order_id, APPROX_MEDIAN(qty) OVER(PARTITION BY order_id) from orders";
        let expected = "\
        Projection: orders.order_id, APPROXMEDIAN(orders.qty) PARTITION BY [orders.order_id] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
        \n  WindowAggr: windowExpr=[[APPROXMEDIAN(orders.qty) PARTITION BY [orders.order_id] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n    TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn select_typed_date_string() {
        let sql = "SELECT date '2020-12-10' AS date";
        let expected = "Projection: CAST(Utf8(\"2020-12-10\") AS Date32) AS date\
            \n  EmptyRelation";
        quick_test(sql, expected);
    }

    #[test]
    fn select_typed_time_string() {
        let sql = "SELECT TIME '08:09:10.123' AS time";
        let expected =
            "Projection: CAST(Utf8(\"08:09:10.123\") AS Time64(Nanosecond)) AS time\
            \n  EmptyRelation";
        quick_test(sql, expected);
    }

    #[test]
    fn select_multibyte_column() {
        let sql = r#"SELECT "😀" FROM person"#;
        let expected = "Projection: person.😀\
            \n  TableScan: person";
        quick_test(sql, expected);
    }

    fn logical_plan(sql: &str) -> Result<LogicalPlan> {
        logical_plan_with_options(sql, ParserOptions::default())
    }

    fn logical_plan_with_options(
        sql: &str,
        options: ParserOptions,
    ) -> Result<LogicalPlan> {
        let dialect = &GenericDialect {};
        logical_plan_with_dialect_and_options(sql, dialect, options)
    }

    fn logical_plan_with_dialect(
        sql: &str,
        dialect: &dyn Dialect,
    ) -> Result<LogicalPlan> {
        let planner = SqlToRel::new(&MockContextProvider {});
        let result = DFParser::parse_sql_with_dialect(sql, dialect);
        let mut ast = result?;
        planner.statement_to_plan(ast.pop_front().unwrap())
    }

    fn logical_plan_with_dialect_and_options(
        sql: &str,
        dialect: &dyn Dialect,
        options: ParserOptions,
    ) -> Result<LogicalPlan> {
        let planner = SqlToRel::new_with_options(&MockContextProvider {}, options);
        let result = DFParser::parse_sql_with_dialect(sql, dialect);
        let mut ast = result?;
        planner.statement_to_plan(ast.pop_front().unwrap())
    }

    /// Create logical plan, write with formatter, compare to expected output
    fn quick_test(sql: &str, expected: &str) {
        let plan = logical_plan(sql).unwrap();
        assert_eq!(format!("{:?}", plan), expected);
    }

    fn quick_test_with_options(sql: &str, expected: &str, options: ParserOptions) {
        let plan = logical_plan_with_options(sql, options).unwrap();
        assert_eq!(format!("{:?}", plan), expected);
    }

    fn prepare_stmt_quick_test(
        sql: &str,
        expected_plan: &str,
        expected_data_types: &str,
    ) {
        let plan = logical_plan(sql).unwrap();
        // verify plan
        assert_eq!(format!("{:?}", plan), expected_plan);
        // verify data types
        if let LogicalPlan::Prepare(Prepare { data_types, .. }) = plan {
            let dt = format!("{:?}", data_types);
            assert_eq!(dt, expected_data_types);
        }
    }

    struct MockContextProvider {}

    impl ContextProvider for MockContextProvider {
        fn get_table_provider(
            &self,
            name: TableReference,
        ) -> Result<Arc<dyn TableSource>> {
            let schema = match name.table() {
                "test" => Ok(Schema::new(vec![
                    Field::new("t_date32", DataType::Date32, false),
                    Field::new("t_date64", DataType::Date64, false),
                ])),
                "j1" => Ok(Schema::new(vec![
                    Field::new("j1_id", DataType::Int32, false),
                    Field::new("j1_string", DataType::Utf8, false),
                ])),
                "j2" => Ok(Schema::new(vec![
                    Field::new("j2_id", DataType::Int32, false),
                    Field::new("j2_string", DataType::Utf8, false),
                ])),
                "j3" => Ok(Schema::new(vec![
                    Field::new("j3_id", DataType::Int32, false),
                    Field::new("j3_string", DataType::Utf8, false),
                ])),
                "test_decimal" => Ok(Schema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("price", DataType::Decimal128(10, 2), false),
                ])),
                "person" => Ok(Schema::new(vec![
                    Field::new("id", DataType::UInt32, false),
                    Field::new("first_name", DataType::Utf8, false),
                    Field::new("last_name", DataType::Utf8, false),
                    Field::new("age", DataType::Int32, false),
                    Field::new("state", DataType::Utf8, false),
                    Field::new("salary", DataType::Float64, false),
                    Field::new(
                        "birth_date",
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        false,
                    ),
                    Field::new("😀", DataType::Int32, false),
                ])),
                "orders" => Ok(Schema::new(vec![
                    Field::new("order_id", DataType::UInt32, false),
                    Field::new("customer_id", DataType::UInt32, false),
                    Field::new("o_item_id", DataType::Utf8, false),
                    Field::new("qty", DataType::Int32, false),
                    Field::new("price", DataType::Float64, false),
                    Field::new("delivered", DataType::Boolean, false),
                ])),
                "lineitem" => Ok(Schema::new(vec![
                    Field::new("l_item_id", DataType::UInt32, false),
                    Field::new("l_description", DataType::Utf8, false),
                    Field::new("price", DataType::Float64, false),
                ])),
                "aggregate_test_100" => Ok(Schema::new(vec![
                    Field::new("c1", DataType::Utf8, false),
                    Field::new("c2", DataType::UInt32, false),
                    Field::new("c3", DataType::Int8, false),
                    Field::new("c4", DataType::Int16, false),
                    Field::new("c5", DataType::Int32, false),
                    Field::new("c6", DataType::Int64, false),
                    Field::new("c7", DataType::UInt8, false),
                    Field::new("c8", DataType::UInt16, false),
                    Field::new("c9", DataType::UInt32, false),
                    Field::new("c10", DataType::UInt64, false),
                    Field::new("c11", DataType::Float32, false),
                    Field::new("c12", DataType::Float64, false),
                    Field::new("c13", DataType::Utf8, false),
                ])),
                _ => Err(DataFusionError::Plan(format!(
                    "No table named: {} found",
                    name.table()
                ))),
            };

            match schema {
                Ok(t) => Ok(Arc::new(EmptyTable::new(Arc::new(t)))),
                Err(e) => Err(e),
            }
        }

        fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
            unimplemented!()
        }

        fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
            unimplemented!()
        }

        fn get_variable_type(&self, _: &[String]) -> Option<DataType> {
            unimplemented!()
        }

        fn get_config_option(&self, _: &str) -> Option<ScalarValue> {
            unimplemented!()
        }
    }

    #[test]
    fn select_partially_qualified_column() {
        let sql = r#"SELECT person.first_name FROM public.person"#;
        let expected = "Projection: public.person.first_name\
            \n  TableScan: public.person";
        quick_test(sql, expected);
    }

    #[test]
    fn cross_join_not_to_inner_join() {
        let sql = "select person.id from person, orders, lineitem where person.id = person.age;";
        let expected = "Projection: person.id\
                                    \n  Filter: person.id = person.age\
                                    \n    CrossJoin:\
                                    \n      CrossJoin:\
                                    \n        TableScan: person\
                                    \n        TableScan: orders\
                                    \n      TableScan: lineitem";
        quick_test(sql, expected);
    }

    #[test]
    fn join_with_aliases() {
        let sql = "select peeps.id, folks.first_name from person as peeps join person as folks on peeps.id = folks.id";
        let expected = "Projection: peeps.id, folks.first_name\
                                    \n  Inner Join: peeps.id = folks.id\
                                    \n    SubqueryAlias: peeps\
                                    \n      TableScan: person\
                                    \n    SubqueryAlias: folks\
                                    \n      TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn cte_use_same_name_multiple_times() {
        let sql = "with a as (select * from person), a as (select * from orders) select * from a;";
        let expected = "SQL error: ParserError(\"WITH query name \\\"a\\\" specified more than once\")";
        let result = logical_plan(sql).err().unwrap();
        assert_eq!(result.to_string(), expected);
    }

    #[test]
    fn date_plus_interval_in_projection() {
        let sql = "select t_date32 + interval '5 days' FROM test";
        let expected = "Projection: test.t_date32 + IntervalDayTime(\"21474836480\")\
                            \n  TableScan: test";
        quick_test(sql, expected);
    }

    #[test]
    fn date_plus_interval_in_filter() {
        let sql = "select t_date64 FROM test \
                    WHERE t_date64 \
                    BETWEEN cast('1999-12-31' as date) \
                        AND cast('1999-12-31' as date) + interval '30 days'";
        let expected =
            "Projection: test.t_date64\
            \n  Filter: test.t_date64 BETWEEN CAST(Utf8(\"1999-12-31\") AS Date32) AND CAST(Utf8(\"1999-12-31\") AS Date32) + IntervalDayTime(\"128849018880\")\
            \n    TableScan: test";
        quick_test(sql, expected);
    }

    #[test]
    fn exists_subquery() {
        let sql = "SELECT id FROM person p WHERE EXISTS \
            (SELECT first_name FROM person \
            WHERE last_name = p.last_name \
            AND state = p.state)";

        let expected = "Projection: p.id\
        \n  Filter: EXISTS (<subquery>)\
        \n    Subquery:\
        \n      Projection: person.first_name\
        \n        Filter: person.last_name = p.last_name AND person.state = p.state\
        \n          TableScan: person\
        \n    SubqueryAlias: p\
        \n      TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn exists_subquery_schema_outer_schema_overlap() {
        // both the outer query and the schema select from unaliased "person"
        let sql = "SELECT person.id FROM person, person p \
            WHERE person.id = p.id AND EXISTS \
            (SELECT person.first_name FROM person, person p2 \
            WHERE person.id = p2.id \
            AND person.last_name = p.last_name \
            AND person.state = p.state)";

        let expected = "Projection: person.id\
        \n  Filter: person.id = p.id AND EXISTS (<subquery>)\
        \n    Subquery:\
        \n      Projection: person.first_name\
        \n        Filter: person.id = p2.id AND person.last_name = p.last_name AND person.state = p.state\
        \n          CrossJoin:\
        \n            TableScan: person\
        \n            SubqueryAlias: p2\
        \n              TableScan: person\
        \n    CrossJoin:\
        \n      TableScan: person\
        \n      SubqueryAlias: p\
        \n        TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn exists_subquery_wildcard() {
        let sql = "SELECT id FROM person p WHERE EXISTS \
            (SELECT * FROM person \
            WHERE last_name = p.last_name \
            AND state = p.state)";

        let expected = "Projection: p.id\
        \n  Filter: EXISTS (<subquery>)\
        \n    Subquery:\
        \n      Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀\
        \n        Filter: person.last_name = p.last_name AND person.state = p.state\
        \n          TableScan: person\
        \n    SubqueryAlias: p\
        \n      TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn in_subquery_uncorrelated() {
        let sql = "SELECT id FROM person p WHERE id IN \
            (SELECT id FROM person)";

        let expected = "Projection: p.id\
        \n  Filter: p.id IN (<subquery>)\
        \n    Subquery:\
        \n      Projection: person.id\
        \n        TableScan: person\
        \n    SubqueryAlias: p\
        \n      TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn not_in_subquery_correlated() {
        let sql = "SELECT id FROM person p WHERE id NOT IN \
            (SELECT id FROM person WHERE last_name = p.last_name AND state = 'CO')";

        let expected = "Projection: p.id\
        \n  Filter: p.id NOT IN (<subquery>)\
        \n    Subquery:\
        \n      Projection: person.id\
        \n        Filter: person.last_name = p.last_name AND person.state = Utf8(\"CO\")\
        \n          TableScan: person\
        \n    SubqueryAlias: p\
        \n      TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn scalar_subquery() {
        let sql = "SELECT p.id, (SELECT MAX(id) FROM person WHERE last_name = p.last_name) FROM person p";

        let expected = "Projection: p.id, (<subquery>)\
        \n  Subquery:\
        \n    Projection: MAX(person.id)\
        \n      Aggregate: groupBy=[[]], aggr=[[MAX(person.id)]]\
        \n        Filter: person.last_name = p.last_name\
        \n          TableScan: person\
        \n  SubqueryAlias: p\
        \n    TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn scalar_subquery_reference_outer_field() {
        let sql = "SELECT j1_string, j2_string \
        FROM j1, j2 \
        WHERE j1_id = j2_id - 1 \
        AND j2_id < (SELECT count(*) \
            FROM j1, j3 \
            WHERE j2_id = j1_id \
            AND j1_id = j3_id)";

        let expected = "Projection: j1.j1_string, j2.j2_string\
        \n  Filter: j1.j1_id = j2.j2_id - Int64(1) AND j2.j2_id < (<subquery>)\
        \n    Subquery:\
        \n      Projection: COUNT(UInt8(1))\
        \n        Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1))]]\
        \n          Filter: j2.j2_id = j1.j1_id AND j1.j1_id = j3.j3_id\
        \n            CrossJoin:\
        \n              TableScan: j1\
        \n              TableScan: j3\
        \n    CrossJoin:\
        \n      TableScan: j1\
        \n      TableScan: j2";

        quick_test(sql, expected);
    }

    #[test]
    fn subquery_references_cte() {
        let sql = "WITH \
        cte AS (SELECT * FROM person) \
        SELECT * FROM person WHERE EXISTS (SELECT * FROM cte WHERE id = person.id)";

        let expected = "Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀\
        \n  Filter: EXISTS (<subquery>)\
        \n    Subquery:\
        \n      Projection: cte.id, cte.first_name, cte.last_name, cte.age, cte.state, cte.salary, cte.birth_date, cte.😀\
        \n        Filter: cte.id = person.id\
        \n          SubqueryAlias: cte\
        \n            Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀\
        \n              TableScan: person\
        \n    TableScan: person";

        quick_test(sql, expected)
    }

    #[test]
    fn cte_with_no_column_names() {
        let sql = "WITH \
        numbers AS ( \
            SELECT 1 as a, 2 as b, 3 as c \
        ) \
        SELECT * FROM numbers;";

        let expected = "Projection: numbers.a, numbers.b, numbers.c\
        \n  SubqueryAlias: numbers\
        \n    Projection: Int64(1) AS a, Int64(2) AS b, Int64(3) AS c\
        \n      EmptyRelation";

        quick_test(sql, expected)
    }

    #[test]
    fn cte_with_column_names() {
        let sql = "WITH \
        numbers(a, b, c) AS ( \
            SELECT 1, 2, 3 \
        ) \
        SELECT * FROM numbers;";

        let expected = "Projection: a, b, c\
        \n  Projection: numbers.Int64(1) AS a, numbers.Int64(2) AS b, numbers.Int64(3) AS c\
        \n    SubqueryAlias: numbers\
        \n      Projection: Int64(1), Int64(2), Int64(3)\
        \n        EmptyRelation";

        quick_test(sql, expected)
    }

    #[test]
    fn cte_with_column_aliases_precedence() {
        // The end result should always be what CTE specification says
        let sql = "WITH \
        numbers(a, b, c) AS ( \
            SELECT 1 as x, 2 as y, 3 as z \
        ) \
        SELECT * FROM numbers;";

        let expected = "Projection: a, b, c\
        \n  Projection: numbers.x AS a, numbers.y AS b, numbers.z AS c\
        \n    SubqueryAlias: numbers\
        \n      Projection: Int64(1) AS x, Int64(2) AS y, Int64(3) AS z\
        \n        EmptyRelation";
        quick_test(sql, expected)
    }

    #[test]
    fn cte_unbalanced_number_of_columns() {
        let sql = "WITH \
        numbers(a) AS ( \
            SELECT 1, 2, 3 \
        ) \
        SELECT * FROM numbers;";

        let expected = "Error during planning: Source table contains 3 columns but only 1 names given as column alias";
        let result = logical_plan(sql).err().unwrap();
        assert_eq!(result.to_string(), expected);
    }

    #[test]
    fn aggregate_with_rollup() {
        let sql = "SELECT id, state, age, COUNT(*) FROM person GROUP BY id, ROLLUP (state, age)";
        let expected = "Projection: person.id, person.state, person.age, COUNT(UInt8(1))\
        \n  Aggregate: groupBy=[[person.id, ROLLUP (person.state, person.age)]], aggr=[[COUNT(UInt8(1))]]\
        \n    TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn aggregate_with_rollup_with_grouping() {
        let sql = "SELECT id, state, age, grouping(state), grouping(age), grouping(state) + grouping(age), COUNT(*) \
        FROM person GROUP BY id, ROLLUP (state, age)";
        let expected = "Projection: person.id, person.state, person.age, GROUPING(person.state), GROUPING(person.age), GROUPING(person.state) + GROUPING(person.age), COUNT(UInt8(1))\
        \n  Aggregate: groupBy=[[person.id, ROLLUP (person.state, person.age)]], aggr=[[GROUPING(person.state), GROUPING(person.age), COUNT(UInt8(1))]]\
        \n    TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn rank_partition_grouping() {
        let sql = "select
            sum(age) as total_sum,
            state,
            last_name,
            grouping(state) + grouping(last_name) as x,
            rank() over (
                partition by grouping(state) + grouping(last_name),
                case when grouping(last_name) = 0 then state end
                order by sum(age) desc
                ) as the_rank
            from
                person
            group by rollup(state, last_name)";
        let expected = "Projection: SUM(person.age) AS total_sum, person.state, person.last_name, GROUPING(person.state) + GROUPING(person.last_name) AS x, RANK() PARTITION BY [GROUPING(person.state) + GROUPING(person.last_name), CASE WHEN GROUPING(person.last_name) = Int64(0) THEN person.state END] ORDER BY [SUM(person.age) DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS the_rank\
        \n  WindowAggr: windowExpr=[[RANK() PARTITION BY [GROUPING(person.state) + GROUPING(person.last_name), CASE WHEN GROUPING(person.last_name) = Int64(0) THEN person.state END] ORDER BY [SUM(person.age) DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n    Aggregate: groupBy=[[ROLLUP (person.state, person.last_name)]], aggr=[[SUM(person.age), GROUPING(person.state), GROUPING(person.last_name)]]\
        \n      TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn aggregate_with_cube() {
        let sql =
            "SELECT id, state, age, COUNT(*) FROM person GROUP BY id, CUBE (state, age)";
        let expected = "Projection: person.id, person.state, person.age, COUNT(UInt8(1))\
        \n  Aggregate: groupBy=[[person.id, CUBE (person.state, person.age)]], aggr=[[COUNT(UInt8(1))]]\
        \n    TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn round_decimal() {
        let sql = "SELECT round(price/3, 2) FROM test_decimal";
        let expected = "Projection: round(test_decimal.price / Int64(3), Int64(2))\
        \n  TableScan: test_decimal";
        quick_test(sql, expected);
    }

    #[ignore] // see https://github.com/apache/arrow-datafusion/issues/2469
    #[test]
    fn aggregate_with_grouping_sets() {
        let sql = "SELECT id, state, age, COUNT(*) FROM person GROUP BY id, GROUPING SETS ((state), (state, age), (id, state))";
        let expected = "TBD";
        quick_test(sql, expected);
    }

    #[test]
    fn join_on_disjunction_condition() {
        let sql = "SELECT id, order_id \
            FROM person \
            JOIN orders ON id = customer_id OR person.age > 30";
        let expected = "Projection: person.id, orders.order_id\
            \n  Filter: person.id = orders.customer_id OR person.age > Int64(30)\
            \n    CrossJoin:\
            \n      TableScan: person\
            \n      TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn join_on_complex_condition() {
        let sql = "SELECT id, order_id \
            FROM person \
            JOIN orders ON id = customer_id AND (person.age > 30 OR person.last_name = 'X')";
        let expected = "Projection: person.id, orders.order_id\
            \n  Inner Join: person.id = orders.customer_id Filter: person.age > Int64(30) OR person.last_name = Utf8(\"X\")\
            \n    TableScan: person\
            \n    TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn hive_aggregate_with_filter() -> Result<()> {
        let dialect = &HiveDialect {};
        let sql = "SELECT SUM(age) FILTER (WHERE age > 4) FROM person";
        let plan = logical_plan_with_dialect(sql, dialect)?;
        let expected = "Projection: SUM(person.age) FILTER (WHERE age > Int64(4))\
        \n  Aggregate: groupBy=[[]], aggr=[[SUM(person.age) FILTER (WHERE age > Int64(4))]]\
        \n    TableScan: person".to_string();
        assert_eq!(plan.display_indent().to_string(), expected);
        Ok(())
    }

    #[test]
    fn order_by_unaliased_name() {
        // https://github.com/apache/arrow-datafusion/issues/3160
        // This query was failing with:
        // SchemaError(FieldNotFound { qualifier: Some("p"), name: "state", valid_fields: Some(["z", "q"]) })
        let sql = "select p.state z, sum(age) q from person p group by p.state order by p.state";
        let expected = "Projection: z, q\
        \n  Sort: p.state ASC NULLS LAST\
        \n    Projection: p.state AS z, SUM(p.age) AS q, p.state\
        \n      Aggregate: groupBy=[[p.state]], aggr=[[SUM(p.age)]]\
        \n        SubqueryAlias: p\
        \n          TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn test_zero_offset_with_limit() {
        let sql = "select id from person where person.id > 100 LIMIT 5 OFFSET 0;";
        let expected = "Limit: skip=0, fetch=5\
                                    \n  Projection: person.id\
                                    \n    Filter: person.id > Int64(100)\
                                    \n      TableScan: person";
        quick_test(sql, expected);

        // Flip the order of LIMIT and OFFSET in the query. Plan should remain the same.
        let sql = "SELECT id FROM person WHERE person.id > 100 OFFSET 0 LIMIT 5;";
        quick_test(sql, expected);
    }

    #[test]
    fn test_offset_no_limit() {
        let sql = "SELECT id FROM person WHERE person.id > 100 OFFSET 5;";
        let expected = "Limit: skip=5, fetch=None\
        \n  Projection: person.id\
        \n    Filter: person.id > Int64(100)\
        \n      TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn test_offset_after_limit() {
        let sql = "select id from person where person.id > 100 LIMIT 5 OFFSET 3;";
        let expected = "Limit: skip=3, fetch=5\
        \n  Projection: person.id\
        \n    Filter: person.id > Int64(100)\
        \n      TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn test_offset_before_limit() {
        let sql = "select id from person where person.id > 100 OFFSET 3 LIMIT 5;";
        let expected = "Limit: skip=3, fetch=5\
        \n  Projection: person.id\
        \n    Filter: person.id > Int64(100)\
        \n      TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn test_distribute_by() {
        let sql = "select id from person distribute by state";
        let expected = "Repartition: DistributeBy(state)\
        \n  Projection: person.id\
        \n    TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn test_double_quoted_literal_string() {
        // Assert double quoted literal string is parsed correctly like single quoted one in specific dialect.
        let dialect = &MySqlDialect {};
        let single_quoted_res = format!(
            "{:?}",
            logical_plan_with_dialect("SELECT '1'", dialect).unwrap()
        );
        let double_quoted_res = format!(
            "{:?}",
            logical_plan_with_dialect("SELECT \"1\"", dialect).unwrap()
        );
        assert_eq!(single_quoted_res, double_quoted_res);

        // It should return error in other dialect.
        assert!(logical_plan("SELECT \"1\"").is_err());
    }

    #[test]
    fn test_constant_expr_eq_join() {
        let sql = "SELECT id, order_id \
            FROM person \
            INNER JOIN orders \
            ON person.id = 10";

        let expected = "Projection: person.id, orders.order_id\
        \n  Filter: person.id = Int64(10)\
        \n    CrossJoin:\
        \n      TableScan: person\
        \n      TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn test_right_left_expr_eq_join() {
        let sql = "SELECT id, order_id \
            FROM person \
            INNER JOIN orders \
            ON orders.customer_id * 2 = person.id + 10";

        let expected = "Projection: person.id, orders.order_id\
        \n  Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀, orders.order_id, orders.customer_id, orders.o_item_id, orders.qty, orders.price, orders.delivered\
        \n    Inner Join: person.id + Int64(10) = orders.customer_id * Int64(2)\
        \n      Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀, person.id + Int64(10)\
        \n        TableScan: person\
        \n      Projection: orders.order_id, orders.customer_id, orders.o_item_id, orders.qty, orders.price, orders.delivered, orders.customer_id * Int64(2)\
        \n        TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn test_single_column_expr_eq_join() {
        let sql = "SELECT id, order_id \
            FROM person \
            INNER JOIN orders \
            ON person.id + 10 = orders.customer_id * 2";

        let expected = "Projection: person.id, orders.order_id\
        \n  Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀, orders.order_id, orders.customer_id, orders.o_item_id, orders.qty, orders.price, orders.delivered\
        \n    Inner Join: person.id + Int64(10) = orders.customer_id * Int64(2)\
        \n      Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀, person.id + Int64(10)\
        \n        TableScan: person\
        \n      Projection: orders.order_id, orders.customer_id, orders.o_item_id, orders.qty, orders.price, orders.delivered, orders.customer_id * Int64(2)\
        \n        TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn test_multiple_column_expr_eq_join() {
        let sql = "SELECT id, order_id \
            FROM person \
            INNER JOIN orders \
            ON person.id + person.age + 10 = orders.customer_id * 2 - orders.price";

        let expected = "Projection: person.id, orders.order_id\
        \n  Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀, orders.order_id, orders.customer_id, orders.o_item_id, orders.qty, orders.price, orders.delivered\
        \n    Inner Join: person.id + person.age + Int64(10) = orders.customer_id * Int64(2) - orders.price\
        \n      Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀, person.id + person.age + Int64(10)\
        \n        TableScan: person\
        \n      Projection: orders.order_id, orders.customer_id, orders.o_item_id, orders.qty, orders.price, orders.delivered, orders.customer_id * Int64(2) - orders.price\
        \n        TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn test_left_projection_expr_eq_join() {
        let sql = "SELECT id, order_id \
            FROM person \
            INNER JOIN orders \
            ON person.id + person.age + 10 = orders.customer_id";

        let expected = "Projection: person.id, orders.order_id\
        \n  Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀, orders.order_id, orders.customer_id, orders.o_item_id, orders.qty, orders.price, orders.delivered\
        \n    Inner Join: person.id + person.age + Int64(10) = orders.customer_id\
        \n      Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀, person.id + person.age + Int64(10)\
        \n        TableScan: person\
        \n      TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn test_right_projection_expr_eq_join() {
        let sql = "SELECT id, order_id \
            FROM person \
            INNER JOIN orders \
            ON person.id = orders.customer_id * 2 - orders.price";

        let expected = "Projection: person.id, orders.order_id\
        \n  Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀, orders.order_id, orders.customer_id, orders.o_item_id, orders.qty, orders.price, orders.delivered\
        \n    Inner Join: person.id = orders.customer_id * Int64(2) - orders.price\
        \n      TableScan: person\
        \n      Projection: orders.order_id, orders.customer_id, orders.o_item_id, orders.qty, orders.price, orders.delivered, orders.customer_id * Int64(2) - orders.price\
        \n        TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn test_one_side_constant_full_join() {
        // TODO: this sql should be parsed as join after
        // https://github.com/apache/arrow-datafusion/issues/2877 is resolved.
        let sql = "SELECT id, order_id \
            FROM person \
            FULL OUTER JOIN orders \
            ON person.id = 10";

        let expected = "Projection: person.id, orders.order_id\
        \n  Filter: person.id = Int64(10)\
        \n    CrossJoin:\
        \n      TableScan: person\
        \n      TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn test_select_all_inner_join() {
        let sql = "SELECT *
            FROM person \
            INNER JOIN orders \
            ON orders.customer_id * 2 = person.id + 10";

        let expected = "Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀, orders.order_id, orders.customer_id, orders.o_item_id, orders.qty, orders.price, orders.delivered\
        \n  Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀, orders.order_id, orders.customer_id, orders.o_item_id, orders.qty, orders.price, orders.delivered\
        \n    Inner Join: person.id + Int64(10) = orders.customer_id * Int64(2)\
        \n      Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀, person.id + Int64(10)\
        \n        TableScan: person\
        \n      Projection: orders.order_id, orders.customer_id, orders.o_item_id, orders.qty, orders.price, orders.delivered, orders.customer_id * Int64(2)\
        \n        TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn test_select_join_key_inner_join() {
        let sql = "SELECT  orders.customer_id * 2,  person.id + 10
            FROM person
            INNER JOIN orders
            ON orders.customer_id * 2 = person.id + 10";

        let expected = "Projection: orders.customer_id * Int64(2), person.id + Int64(10)\
        \n  Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀, orders.order_id, orders.customer_id, orders.o_item_id, orders.qty, orders.price, orders.delivered\
        \n    Inner Join: person.id + Int64(10) = orders.customer_id * Int64(2)\
        \n      Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀, person.id + Int64(10)\
        \n        TableScan: person\
        \n      Projection: orders.order_id, orders.customer_id, orders.o_item_id, orders.qty, orders.price, orders.delivered, orders.customer_id * Int64(2)\
        \n        TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn test_non_projetion_after_inner_join() {
        // There's no need to add projection for left and right, so does adding projection after join.
        let sql = "SELECT  person.id, person.age
            FROM person
            INNER JOIN orders
            ON orders.customer_id = person.id";

        let expected = "Projection: person.id, person.age\
        \n  Inner Join: person.id = orders.customer_id\
        \n    TableScan: person\
        \n    TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn test_duplicated_left_join_key_inner_join() {
        //  person.id * 2 happen twice in left side.
        let sql = "SELECT person.id, person.age
            FROM person
            INNER JOIN orders
            ON person.id * 2 = orders.customer_id + 10 and person.id * 2 = orders.order_id";

        let expected = "Projection: person.id, person.age\
        \n  Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀, orders.order_id, orders.customer_id, orders.o_item_id, orders.qty, orders.price, orders.delivered\
        \n    Inner Join: person.id * Int64(2) = orders.customer_id + Int64(10), person.id * Int64(2) = orders.order_id\
        \n      Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀, person.id * Int64(2)\
        \n        TableScan: person\
        \n      Projection: orders.order_id, orders.customer_id, orders.o_item_id, orders.qty, orders.price, orders.delivered, orders.customer_id + Int64(10)\
        \n        TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn test_duplicated_right_join_key_inner_join() {
        //  orders.customer_id + 10 happen twice in right side.
        let sql = "SELECT person.id, person.age
            FROM person
            INNER JOIN orders
            ON person.id * 2 = orders.customer_id + 10 and person.id =  orders.customer_id + 10";

        let expected = "Projection: person.id, person.age\
        \n  Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀, orders.order_id, orders.customer_id, orders.o_item_id, orders.qty, orders.price, orders.delivered\n    Inner Join: person.id * Int64(2) = orders.customer_id + Int64(10), person.id = orders.customer_id + Int64(10)\
        \n      Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀, person.id * Int64(2)\
        \n        TableScan: person\
        \n      Projection: orders.order_id, orders.customer_id, orders.o_item_id, orders.qty, orders.price, orders.delivered, orders.customer_id + Int64(10)\
        \n        TableScan: orders";
        quick_test(sql, expected);
    }

    #[test]
    fn test_ambiguous_column_references_in_on_join() {
        let sql = "select p1.id, p1.age, p2.id
            from person as p1
            INNER JOIN person as p2
            ON id = 1";

        let expected =
            "Error during planning: reference 'id' is ambiguous, could be p1.id,p2.id;";

        // It should return error.
        let result = logical_plan(sql);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert_eq!(err.to_string(), expected);
    }

    #[test]
    fn test_ambiguous_column_references_with_in_using_join() {
        let sql = "select p1.id, p1.age, p2.id
            from person as p1
            INNER JOIN person as p2
            using(id)";

        let expected = "Projection: p1.id, p1.age, p2.id\
            \n  Inner Join: Using p1.id = p2.id\
            \n    SubqueryAlias: p1\
            \n      TableScan: person\
            \n    SubqueryAlias: p2\
            \n      TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    #[should_panic(
        expected = "value: Internal(\"Invalid placeholder, not a number: $foo\""
    )]
    fn test_prepare_statement_to_plan_panic_param_format() {
        // param is not number following the $ sign
        // panic due to error returned from the parser
        let sql = "PREPARE my_plan(INT) AS SELECT id, age  FROM person WHERE age = $foo";

        let expected_plan = "whatever";
        let expected_dt = "whatever";

        prepare_stmt_quick_test(sql, expected_plan, expected_dt);
    }

    #[test]
    #[should_panic(expected = "value: SQL(ParserError(\"Expected AS, found: SELECT\"))")]
    fn test_prepare_statement_to_plan_panic_prepare_wrong_syntax() {
        // param is not number following the $ sign
        // panic due to error returned from the parser
        let sql = "PREPARE AS SELECT id, age  FROM person WHERE age = $foo";

        let expected_plan = "whatever";
        let expected_dt = "whatever";

        prepare_stmt_quick_test(sql, expected_plan, expected_dt);
    }

    #[test]
    #[should_panic(
        expected = "value: SchemaError(FieldNotFound { field: Column { relation: None, name: \"id\" }, valid_fields: Some([]) })"
    )]
    fn test_prepare_statement_to_plan_panic_no_relation_and_constant_param() {
        let sql = "PREPARE my_plan(INT) AS SELECT id + $1";

        let expected_plan = "whatever";
        let expected_dt = "whatever";

        prepare_stmt_quick_test(sql, expected_plan, expected_dt);
    }

    #[test]
    #[should_panic(
        expected = "value: Internal(\"Placehoder $2 does not exist in the parameter list: [Int32]\")"
    )]
    fn test_prepare_statement_to_plan_panic_no_data_types() {
        // only provide 1 data type while using 2 params
        let sql = "PREPARE my_plan(INT) AS SELECT 1 + $1 + $2";

        let expected_plan = "whatever";
        let expected_dt = "whatever";

        prepare_stmt_quick_test(sql, expected_plan, expected_dt);
    }

    #[test]
    #[should_panic(
        expected = "value: SQL(ParserError(\"Expected [NOT] NULL or TRUE|FALSE or [NOT] DISTINCT FROM after IS, found: $1\""
    )]
    fn test_prepare_statement_to_plan_panic_is_param() {
        let sql = "PREPARE my_plan(INT) AS SELECT id, age  FROM person WHERE age is $1";

        let expected_plan = "whatever";
        let expected_dt = "whatever";

        prepare_stmt_quick_test(sql, expected_plan, expected_dt);
    }

    #[test]
    fn test_prepare_statement_to_plan_no_param() {
        // no embedded parameter but still declare it
        let sql = "PREPARE my_plan(INT) AS SELECT id, age  FROM person WHERE age = 10";

        let expected_plan = "Prepare: \"my_plan\" [Int32] \
        \n  Projection: person.id, person.age\
        \n    Filter: person.age = Int64(10)\
        \n      TableScan: person";

        let expected_dt = "[Int32]";

        prepare_stmt_quick_test(sql, expected_plan, expected_dt);

        /////////////////////////
        // no embedded parameter and no declare it
        let sql = "PREPARE my_plan AS SELECT id, age  FROM person WHERE age = 10";

        let expected_plan = "Prepare: \"my_plan\" [] \
        \n  Projection: person.id, person.age\
        \n    Filter: person.age = Int64(10)\
        \n      TableScan: person";

        let expected_dt = "[]";

        prepare_stmt_quick_test(sql, expected_plan, expected_dt);
    }

    #[test]
    fn test_prepare_statement_to_plan_params_as_constants() {
        let sql = "PREPARE my_plan(INT) AS SELECT $1";

        let expected_plan = "Prepare: \"my_plan\" [Int32] \
        \n  Projection: $1\n    EmptyRelation";
        let expected_dt = "[Int32]";

        prepare_stmt_quick_test(sql, expected_plan, expected_dt);

        /////////////////////////
        let sql = "PREPARE my_plan(INT) AS SELECT 1 + $1";

        let expected_plan = "Prepare: \"my_plan\" [Int32] \
        \n  Projection: Int64(1) + $1\n    EmptyRelation";
        let expected_dt = "[Int32]";

        prepare_stmt_quick_test(sql, expected_plan, expected_dt);

        /////////////////////////
        let sql = "PREPARE my_plan(INT, DOUBLE) AS SELECT 1 + $1 + $2";

        let expected_plan = "Prepare: \"my_plan\" [Int32, Float64] \
        \n  Projection: Int64(1) + $1 + $2\n    EmptyRelation";
        let expected_dt = "[Int32, Float64]";

        prepare_stmt_quick_test(sql, expected_plan, expected_dt);
    }

    #[test]
    fn test_prepare_statement_to_plan_one_param() {
        let sql = "PREPARE my_plan(INT) AS SELECT id, age  FROM person WHERE age = $1";

        let expected_plan = "Prepare: \"my_plan\" [Int32] \
        \n  Projection: person.id, person.age\
        \n    Filter: person.age = $1\
        \n      TableScan: person";

        let expected_dt = "[Int32]";

        prepare_stmt_quick_test(sql, expected_plan, expected_dt);
    }

    #[test]
    fn test_prepare_statement_to_plan_multi_params() {
        let sql = "PREPARE my_plan(INT, STRING, DOUBLE, INT, DOUBLE, STRING) AS
        SELECT id, age, $6
        FROM person
        WHERE age IN ($1, $4) AND salary > $3 and salary < $5 OR first_name < $2";

        let expected_plan = "Prepare: \"my_plan\" [Int32, Utf8, Float64, Int32, Float64, Utf8] \
        \n  Projection: person.id, person.age, $6\
        \n    Filter: person.age IN ([$1, $4]) AND person.salary > $3 AND person.salary < $5 OR person.first_name < $2\
        \n      TableScan: person";

        let expected_dt = "[Int32, Utf8, Float64, Int32, Float64, Utf8]";

        prepare_stmt_quick_test(sql, expected_plan, expected_dt);
    }

    #[test]
    fn test_prepare_statement_to_plan_having() {
        let sql = "PREPARE my_plan(INT, DOUBLE, DOUBLE, DOUBLE) AS
        SELECT id, SUM(age)
        FROM person \
        WHERE salary > $2
        GROUP BY id
        HAVING sum(age) < $1 AND SUM(age) > 10 OR SUM(age) in ($3, $4)\
        ";

        let expected_plan = "Prepare: \"my_plan\" [Int32, Float64, Float64, Float64] \
        \n  Projection: person.id, SUM(person.age)\
        \n    Filter: SUM(person.age) < $1 AND SUM(person.age) > Int64(10) OR SUM(person.age) IN ([$3, $4])\
        \n      Aggregate: groupBy=[[person.id]], aggr=[[SUM(person.age)]]\
        \n        Filter: person.salary > $2\
        \n          TableScan: person";

        let expected_dt = "[Int32, Float64, Float64, Float64]";

        prepare_stmt_quick_test(sql, expected_plan, expected_dt);
    }

    #[test]
    fn test_prepare_statement_to_plan_value_list() {
        let sql = "PREPARE my_plan(STRING, STRING) AS SELECT * FROM (VALUES(1, $1), (2, $2)) AS t (num, letter);";

        let expected_plan = "Prepare: \"my_plan\" [Utf8, Utf8] \
        \n  Projection: num, letter\
        \n    Projection: t.column1 AS num, t.column2 AS letter\
        \n      SubqueryAlias: t\
        \n        Values: (Int64(1), $1), (Int64(2), $2)";

        let expected_dt = "[Utf8, Utf8]";

        prepare_stmt_quick_test(sql, expected_plan, expected_dt);
    }

    #[test]
    fn test_table_alias() {
        let sql = "select * from (\
          (select id from person) t1 \
            CROSS JOIN \
          (select age from person) t2 \
        ) as f";

        let expected = "Projection: f.id, f.age\
        \n  SubqueryAlias: f\
        \n    CrossJoin:\
        \n      SubqueryAlias: t1\
        \n        Projection: person.id\
        \n          TableScan: person\
        \n      SubqueryAlias: t2\
        \n        Projection: person.age\
        \n          TableScan: person";
        quick_test(sql, expected);

        let sql = "select * from (\
          (select id from person) t1 \
            CROSS JOIN \
          (select age from person) t2 \
        ) as f (c1, c2)";

        let expected = "Projection: c1, c2\
        \n  Projection: f.id AS c1, f.age AS c2\
        \n    SubqueryAlias: f\
        \n      CrossJoin:\
        \n        SubqueryAlias: t1\
        \n          Projection: person.id\
        \n            TableScan: person\
        \n        SubqueryAlias: t2\
        \n          Projection: person.age\
        \n            TableScan: person";
        quick_test(sql, expected);
    }

    #[test]
    fn test_inner_join_with_cast_key() {
        let sql = "SELECT person.id, person.age
            FROM person
            INNER JOIN orders
            ON cast(person.id as Int) = cast(orders.customer_id as Int)";

        let expected = "Projection: person.id, person.age\
        \n  Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀, orders.order_id, orders.customer_id, orders.o_item_id, orders.qty, orders.price, orders.delivered\
        \n    Inner Join: CAST(person.id AS Int32) = CAST(orders.customer_id AS Int32)\
        \n      Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀, CAST(person.id AS Int32) AS CAST(person.id AS Int32)\
        \n        TableScan: person\
        \n      Projection: orders.order_id, orders.customer_id, orders.o_item_id, orders.qty, orders.price, orders.delivered, CAST(orders.customer_id AS Int32) AS CAST(orders.customer_id AS Int32)\
        \n        TableScan: orders";
        quick_test(sql, expected);
    }

    fn assert_field_not_found(err: DataFusionError, name: &str) {
        match err {
            DataFusionError::SchemaError { .. } => {
                let msg = format!("{}", err);
                let expected = format!("Schema error: No field named '{}'.", name);
                if !msg.starts_with(&expected) {
                    panic!("error [{}] did not start with [{}]", msg, expected);
                }
            }
            _ => panic!("assert_field_not_found wrong error type"),
        }
    }

    struct EmptyTable {
        table_schema: SchemaRef,
    }

    impl EmptyTable {
        fn new(table_schema: SchemaRef) -> Self {
            Self { table_schema }
        }
    }

    impl TableSource for EmptyTable {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.table_schema.clone()
        }
    }
}
