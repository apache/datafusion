// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use crate::ast::{
    Assignment, BuiltinStatement, ColumnDef, ColumnOption, CreateIndex as SQLCreateIndex,
    CreateTable, CreateView as SQLCreateView, Delete, Explain as SQLExplain,
    ExplainTarget, Expr as SQLExpr, ExprKind, Ident, Insert, InsertSource, Merge,
    MergeAction, MergeClause, ObjectName, ObjectType, Query, SetExpr, SetStatement, Show,
    ShowClause, Statement, StatementKind, TableConstraint, TableConstraintKind,
    TableFactor, TableRef, TableWithJoins, UnaryOp, Update,
};
use crate::parser::{
    CopyToSource, CopyToStatement, CreateExternalCatalog, CreateExternalTable,
    ExplainStatement, LexOrdering, OptionValue, ResetStatement, Statement as DFStatement,
};
use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use crate::utils::normalize_ident;

use arrow::datatypes::{FieldRef, Fields, Metadata};
use datafusion_common::datatype::FieldExt;
use datafusion_common::error::_plan_err;
use datafusion_common::format::ExplainStatementOptions;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{
    Column, Constraint, Constraints, DFSchema, DFSchemaRef, DataFusionError, Result,
    ScalarValue, SchemaError, SchemaReference, TableReference, ToDFSchema, exec_err,
    not_impl_err, plan_datafusion_err, plan_err, schema_err, unqualified_field_not_found,
};
use datafusion_expr::dml::{
    CopyTo, InsertOp, MergeIntoAction, MergeIntoClause, MergeIntoClauseKind, MergeIntoOp,
};
use datafusion_expr::expr_rewriter::normalize_col_with_schemas_and_ambiguity_check;
use datafusion_expr::logical_plan::DdlStatement;
use datafusion_expr::logical_plan::builder::project;
use datafusion_expr::logical_plan::information_schema::ColumnsDetail;
use datafusion_expr::utils::expr_to_columns;
use datafusion_expr::{
    Analyze, Cast, CreateCatalog, CreateCatalogSchema,
    CreateExternalCatalog as PlanCreateExternalCatalog,
    CreateExternalTable as PlanCreateExternalTable, CreateFunction, CreateFunctionBody,
    CreateIndex as PlanCreateIndex, CreateMemoryTable, CreateView, Deallocate,
    DmlStatement, DropCatalog, DropCatalogSchema, DropFunction, DropTable, DropView,
    EmptyRelation, Execute, Explain, ExplainFormat, Expr, ExprSchemable, Filter,
    LogicalPlan, LogicalPlanBuilder, OperateFunctionArg, PlanType, Prepare,
    ResetVariable, SetVariable, SortExpr, Statement as PlanStatement, ToStringifiedPlan,
    TransactionAccessMode, TransactionConclusion, TransactionEnd,
    TransactionIsolationLevel, TransactionStart, Volatility, WriteOp, cast,
};
use sqlparser::ParseError;
use sqlparser::ast::{
    CreateSchema, DropBehavior, DropObject, FinishKind, IsolationLevel, MergeClauseKind,
    Transaction, TransactionMode, Truncate,
};
use sqlparser::dialect::mysql::{MySqlInsert, MySqlStatement};
use sqlparser::dialect::postgres::{
    CreateFunction as PgCreateFunction, ExecuteStatement, FunctionBody, FunctionReturn,
    PostgresInsert, PostgresStatement, PostgresTruncate, PrepareStatement,
};
use sqlparser::dialect::spark::{SparkInsert, SparkStatement};
use sqlparser::dialect::sqlite::{SqliteInsert, SqliteStatement};

type DropRoutine<'a> = sqlparser::ast::DropRoutine<'a, crate::ast::Builtin>;

fn ident_to_string(ident: &Ident<'_>) -> String {
    normalize_ident(ident.to_owned())
}

fn object_name_to_string(object_name: &ObjectName<'_>) -> String {
    object_name
        .parts
        .iter()
        .map(ident_to_string)
        .collect::<Vec<String>>()
        .join(".")
}

/// Converts a parsed identifier to DataFusion's owned identifier.
fn to_df_ident(ident: &Ident<'_>) -> datafusion_expr::sql::Ident {
    datafusion_expr::sql::Ident {
        value: ident.value().into_owned(),
        quote_style: ident.quote,
        span: String::new(),
    }
}

/// A [`DataFusionError`] for a statement the planner rejects as malformed.
fn parser_error(message: &str) -> DataFusionError {
    DataFusionError::from(ParseError {
        message: message.to_string(),
        span: Default::default(),
    })
}

fn get_schema_name(schema: &CreateSchema<'_>) -> String {
    match (&schema.name, &schema.authorization) {
        (Some(name), None) => object_name_to_string(name),
        (None, Some(auth)) => ident_to_string(auth),
        (Some(name), Some(auth)) => {
            format!("{}.{}", object_name_to_string(name), ident_to_string(auth))
        }
        (None, None) => String::new(),
    }
}

/// A query that is only `VALUES` rows, for an `INSERT ... VALUES`.
fn values_query(body: SetExpr<'_>) -> Query<'_> {
    Query {
        with: None,
        body,
        order_by: vec![],
        distribution: None,
        limit: None,
        fetch: None,
        locks: vec![],
        tail: Default::default(),
        pipes: vec![],
    }
}

/// The query that supplies the rows of an `INSERT`.
fn insert_source_to_query(source: Option<InsertSource<'_>>) -> Result<Box<Query<'_>>> {
    match source {
        Some(InsertSource::Query(query)) => Ok(query),
        Some(InsertSource::Values(rows)) => {
            Ok(Box::new(values_query(SetExpr::Values(rows))))
        }
        Some(InsertSource::RowValues(rows)) => {
            Ok(Box::new(values_query(SetExpr::RowValues(rows))))
        }
        Some(InsertSource::Set(_)) => plan_err!("Inserts with assignments not supported"),
        Some(InsertSource::DefaultValues) | None => {
            plan_err!("Inserts without a source not supported")
        }
    }
}

/// Construct `TableConstraint`(s) for the given columns by iterating over
/// `columns` and extracting individual inline constraint definitions.
fn calc_inline_constraints_from_columns<'s>(
    columns: &[ColumnDef<'s>],
) -> Vec<TableConstraint<'s>> {
    let mut constraints: Vec<TableConstraint> = vec![];
    for column in columns {
        for option in &column.options {
            // Look through `CONSTRAINT name`
            let mut name = None;
            let mut option = option;
            while let ColumnOption::Named {
                name: constraint_name,
                option: inner,
            } = option
            {
                name = Some(constraint_name.clone());
                option = inner;
            }
            let kind = match option {
                ColumnOption::Unique => {
                    TableConstraintKind::Unique(vec![column.name.clone()])
                }
                ColumnOption::PrimaryKey => {
                    TableConstraintKind::PrimaryKey(vec![column.name.clone()])
                }
                ColumnOption::References(references) => TableConstraintKind::ForeignKey {
                    index_name: None,
                    columns: vec![],
                    references: references.clone(),
                },
                ColumnOption::Check(expr) => TableConstraintKind::Check(expr.clone()),
                _ => continue,
            };
            constraints.push(TableConstraint {
                name,
                kind,
                include: vec![],
                nulls_distinct: None,
                attributes: vec![],
            });
        }
    }
    constraints
}

/// Maps PostgreSQL's `timezone` and MySQL's `time.zone` to DataFusion's name.
fn canonical_variable_name(variable: &ObjectName<'_>) -> String {
    let variable_lower = object_name_to_string(variable).to_lowercase();
    if variable_lower == "timezone" || variable_lower == "time.zone" {
        "datafusion.execution.time_zone".to_string()
    } else {
        variable_lower
    }
}

impl<S: ContextProvider> SqlToRel<'_, S> {
    /// Generate a logical plan from an DataFusion SQL statement
    pub fn statement_to_plan(&self, statement: DFStatement<'_>) -> Result<LogicalPlan> {
        let plan = match statement {
            DFStatement::CreateExternalTable(s) => self.external_table_to_plan(s)?,
            DFStatement::CreateExternalCatalog(s) => self.external_catalog_to_plan(s)?,
            DFStatement::Statement(s) => self.sql_statement_to_plan(*s)?,
            DFStatement::CopyTo(s) => self.copy_to_plan(s)?,
            DFStatement::Explain(ExplainStatement { options, statement }) => {
                self.explain_to_plan(options, *statement)?
            }
            DFStatement::Reset(statement) => self.reset_statement_to_plan(statement)?,
        };
        check_plan(&plan)?;
        Ok(plan)
    }

    /// Generate a logical plan from an SQL statement
    pub fn sql_statement_to_plan(&self, statement: Statement<'_>) -> Result<LogicalPlan> {
        self.sql_statement_to_plan_with_context(statement, &mut PlannerContext::new())
    }

    /// Generate a logical plan from an SQL statement
    pub fn sql_statement_to_plan_with_context(
        &self,
        statement: Statement<'_>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let plan =
            self.sql_statement_to_plan_with_context_impl(statement, planner_context)?;
        check_plan(&plan)?;
        Ok(plan)
    }

    fn sql_statement_to_plan_with_context_impl(
        &self,
        statement: Statement<'_>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        match statement.kind {
            StatementKind::Explain(explain) => self.sql_explain_to_plan(*explain),
            StatementKind::Query(query) => self.query_to_plan(*query, planner_context),
            StatementKind::Show(show) => self.show_to_plan(*show),
            StatementKind::Set(statement) => self.set_statement_to_plan(*statement),
            StatementKind::CreateTable(create) => {
                self.create_table_to_plan(*create, planner_context)
            }
            StatementKind::CreateView(view) => self.create_view_to_plan(*view),
            StatementKind::CreateSchema(schema) => Ok(LogicalPlan::Ddl(
                DdlStatement::CreateCatalogSchema(CreateCatalogSchema {
                    schema_name: get_schema_name(&schema),
                    if_not_exists: schema.if_not_exists,
                    schema: Arc::new(DFSchema::empty()),
                }),
            )),
            StatementKind::CreateDatabase(database) => Ok(LogicalPlan::Ddl(
                DdlStatement::CreateCatalog(CreateCatalog {
                    catalog_name: object_name_to_string(&database.name),
                    if_not_exists: database.if_not_exists,
                    schema: Arc::new(DFSchema::empty()),
                }),
            )),
            StatementKind::Drop(drop) => self.drop_to_plan(*drop),
            StatementKind::Insert(insert) => {
                self.insert_statement_to_plan(*insert, false)
            }
            StatementKind::Update(update) => self.update_statement_to_plan(*update),
            StatementKind::Delete(delete) => self.delete_statement_to_plan(*delete),
            StatementKind::Merge(merge) => self.merge_to_plan(*merge),
            StatementKind::Transaction(transaction) => {
                self.transaction_to_plan(*transaction)
            }
            StatementKind::DropRoutine(drop) => self.drop_function_to_plan(*drop),
            StatementKind::Truncate(truncate) => {
                let Truncate {
                    table_keyword: _, // Support TRUNCATE TABLE and TRUNCATE syntax
                    if_exists,
                    mut names,
                } = *truncate;
                if names.len() != 1 {
                    return not_impl_err!(
                        "TRUNCATE with multiple tables is not supported"
                    );
                }
                if if_exists {
                    return not_impl_err!("TRUNCATE .. with IF EXISTS is not supported");
                }
                self.truncate_to_plan(names.pop().unwrap())
            }
            StatementKind::CreateIndex(create) => {
                self.create_index_to_plan(*create, planner_context)
            }
            StatementKind::Ext(BuiltinStatement::Postgres(statement)) => {
                self.postgres_statement_to_plan(statement, planner_context)
            }
            StatementKind::Ext(BuiltinStatement::MySql(MySqlStatement::Insert(
                insert,
            ))) => self.mysql_insert_to_plan(*insert),
            StatementKind::Ext(BuiltinStatement::Sqlite(SqliteStatement::Insert(
                insert,
            ))) => self.sqlite_insert_to_plan(*insert),
            StatementKind::Ext(BuiltinStatement::Spark(SparkStatement::Insert(
                insert,
            ))) => self.spark_insert_to_plan(*insert),
            kind => {
                not_impl_err!("Unsupported SQL statement: {kind}")
            }
        }
    }

    /// Plans the statements that the PostgreSQL syntax adds, which DataFusion's
    /// default DuckDB dialect also produces.
    fn postgres_statement_to_plan(
        &self,
        statement: PostgresStatement<'_>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        match statement {
            PostgresStatement::Prepare(prepare) => {
                let PrepareStatement {
                    name,
                    types,
                    statement,
                } = *prepare;
                // Convert parser data types to DataFusion data types
                let mut fields: Vec<FieldRef> = types
                    .into_iter()
                    .map(|t| self.convert_data_type_to_field(&t))
                    .collect::<Result<_>>()?;

                // Create planner context with parameters
                let mut planner_context = PlannerContext::new()
                    .with_prepare_param_data_types(
                        fields.iter().cloned().map(Some).collect(),
                    );

                // Build logical plan for inner statement of the prepare statement
                let plan = self.sql_statement_to_plan_with_context_impl(
                    *statement,
                    &mut planner_context,
                )?;

                if fields.is_empty() {
                    let map_types = plan.get_parameter_fields()?;
                    let param_types: Option<Vec<FieldRef>> = (1..=map_types.len())
                        .map(|i| {
                            let key = format!("${i}");
                            map_types.get(&key).and_then(|opt| opt.clone())
                        })
                        .collect();
                    if let Some(param_types) = param_types {
                        fields.extend(param_types.iter().cloned());
                        planner_context.with_prepare_param_data_types(
                            param_types.into_iter().map(Some).collect(),
                        );
                    }
                }

                Ok(LogicalPlan::Statement(PlanStatement::Prepare(Prepare {
                    name: ident_to_string(&name),
                    fields,
                    input: Arc::new(plan),
                })))
            }
            PostgresStatement::Execute(execute) => {
                let ExecuteStatement { name, args, using } = *execute;
                // `USING` is a MySQL-specific syntax and currently not supported.
                if !using.is_empty() {
                    return not_impl_err!(
                        "Execute statement with USING is not supported"
                    );
                }

                let empty_schema = DFSchema::empty();
                let parameters = args
                    .unwrap_or_default()
                    .into_iter()
                    .map(|expr| self.sql_to_expr(expr, &empty_schema, planner_context))
                    .collect::<Result<Vec<Expr>>>()?;

                Ok(LogicalPlan::Statement(PlanStatement::Execute(Execute {
                    name: ident_to_string(&name),
                    parameters,
                })))
            }
            PostgresStatement::Deallocate {
                name,
                // Similar to PostgreSQL, the PREPARE keyword is ignored
                prepare: _,
            } => {
                let Some(name) = name else {
                    return not_impl_err!("DEALLOCATE ALL is not supported");
                };
                Ok(LogicalPlan::Statement(PlanStatement::Deallocate(
                    Deallocate {
                        name: ident_to_string(&name),
                    },
                )))
            }
            PostgresStatement::CreateFunction(create) => {
                self.create_function_to_plan(*create)
            }
            PostgresStatement::Insert(insert) => self.postgres_insert_to_plan(*insert),
            PostgresStatement::Truncate(truncate) => {
                let PostgresTruncate {
                    table_keyword: _, // Support TRUNCATE TABLE and TRUNCATE syntax
                    mut tables,
                    identity,
                    behavior,
                } = *truncate;
                if tables.len() != 1 {
                    return not_impl_err!(
                        "TRUNCATE with multiple tables is not supported"
                    );
                }
                let target = tables.pop().unwrap();
                if target.only {
                    return not_impl_err!("TRUNCATE with ONLY is not supported");
                }
                if target.star {
                    return not_impl_err!("TRUNCATE with * is not supported");
                }
                if identity.is_some() {
                    return not_impl_err!(
                        "TRUNCATE with RESTART/CONTINUE IDENTITY is not supported"
                    );
                }
                if behavior.is_some() {
                    return not_impl_err!(
                        "TRUNCATE with CASCADE/RESTRICT is not supported"
                    );
                }
                self.truncate_to_plan(target.name)
            }
            statement => not_impl_err!("Unsupported SQL statement: {statement}"),
        }
    }

    /// Plans `EXPLAIN`, `DESCRIBE` or `DESC` as parsed by the SQL parser
    fn sql_explain_to_plan(&self, explain: SQLExplain<'_>) -> Result<LogicalPlan> {
        let SQLExplain {
            keyword,
            analyze,
            verbose,
            format,
            target,
            ..
        } = explain;
        // only plan 'DESCRIBE table_name' or 'DESC table_name' and not 'EXPLAIN table_name'
        let describe = !keyword.eq_ignore_ascii_case("EXPLAIN");
        match target {
            ExplainTarget::Table { name, .. } => {
                if describe {
                    self.describe_table_to_plan(name)
                } else {
                    not_impl_err!("Unsupported SQL statement: {keyword} {name}")
                }
            }
            ExplainTarget::Statement(statement) => {
                if describe {
                    return match statement.kind {
                        StatementKind::Query(query) => {
                            self.describe_query_to_plan(*query)
                        }
                        _ => not_impl_err!(
                            "Describing statements other than SELECT not supported"
                        ),
                    };
                }
                let format = format
                    .map(|format| ExplainFormat::from_str(&format.name))
                    .transpose()?;
                let statement = DFStatement::Statement(statement);
                let options = ExplainStatementOptions {
                    analyze,
                    verbose,
                    format,
                    analyze_level: None,
                    analyze_categories: None,
                    show_statistics: None,
                };
                self.explain_to_plan(options, statement)
            }
        }
    }

    fn create_table_to_plan(
        &self,
        create: CreateTable<'_>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let CreateTable {
            or_replace,
            temporary,
            name,
            if_not_exists,
            columns,
            constraints,
            query,
        } = create;
        if temporary {
            return not_impl_err!("Temporary tables not supported");
        }
        // Merge inline constraints and existing constraints
        let mut all_constraints = constraints;
        let inline_constraints = calc_inline_constraints_from_columns(&columns);
        all_constraints.extend(inline_constraints);
        // Build column default values
        let column_defaults = self.build_column_defaults(&columns, planner_context)?;

        let has_columns = !columns.is_empty();
        let schema = self.build_schema(columns)?.to_dfschema_ref()?;
        if has_columns {
            planner_context.set_table_schema(Some(Arc::clone(&schema)));
        }

        match query {
            Some(query) => {
                let plan = self.query_to_plan(*query, planner_context)?;
                let input_schema = plan.schema();

                let plan = if has_columns {
                    if schema.fields().len() != input_schema.fields().len() {
                        return plan_err!(
                            "Mismatch: {} columns specified, but result has {} columns",
                            schema.fields().len(),
                            input_schema.fields().len()
                        );
                    }
                    let input_columns = input_schema.columns();
                    let project_exprs = schema
                        .fields()
                        .iter()
                        .zip(input_columns)
                        .map(|(field, input_column)| {
                            cast(Expr::Column(input_column), field.data_type().clone())
                                .alias(field.name())
                        })
                        .collect::<Vec<_>>();

                    LogicalPlanBuilder::from(plan.clone())
                        .project(project_exprs)?
                        .build()?
                } else {
                    plan
                };

                let constraints = self.new_constraint_from_table_constraints(
                    &all_constraints,
                    plan.schema(),
                )?;

                Ok(LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(
                    CreateMemoryTable {
                        name: self.object_name_to_table_reference(name)?,
                        constraints,
                        input: Arc::new(plan),
                        if_not_exists,
                        or_replace,
                        column_defaults,
                        temporary,
                    },
                )))
            }

            None => {
                let plan = EmptyRelation {
                    produce_one_row: false,
                    schema,
                };
                let plan = LogicalPlan::EmptyRelation(plan);
                let constraints = self.new_constraint_from_table_constraints(
                    &all_constraints,
                    plan.schema(),
                )?;
                Ok(LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(
                    CreateMemoryTable {
                        name: self.object_name_to_table_reference(name)?,
                        constraints,
                        input: Arc::new(plan),
                        if_not_exists,
                        or_replace,
                        column_defaults,
                        temporary,
                    },
                )))
            }
        }
    }

    fn create_view_to_plan(&self, view: SQLCreateView<'_>) -> Result<LogicalPlan> {
        // the SQL string representation of the whole statement
        let sql = view.to_string();
        let SQLCreateView {
            or_replace,
            temporary,
            materialized,
            if_not_exists,
            name,
            columns,
            with,
            query,
            no_schema_binding,
        } = view;
        if materialized {
            return not_impl_err!("Materialized views not supported")?;
        }
        if !with.is_empty() {
            return not_impl_err!("View options not supported")?;
        }
        if no_schema_binding {
            return not_impl_err!("With no schema binding not supported")?;
        }
        if if_not_exists {
            return not_impl_err!("If not exists not supported")?;
        }

        let mut plan = self.query_to_plan(*query, &mut PlannerContext::new())?;
        plan = self.apply_expr_alias(plan, columns)?;

        Ok(LogicalPlan::Ddl(DdlStatement::CreateView(CreateView {
            name: self.object_name_to_table_reference(name)?,
            input: Arc::new(plan),
            or_replace,
            definition: Some(sql),
            temporary,
        })))
    }

    fn drop_to_plan(&self, drop: DropObject<'_>) -> Result<LogicalPlan> {
        let DropObject {
            object_type,
            if_exists,
            mut names,
            behavior,
            temporary: _,
            on: _,
            purge: _,
        } = drop;
        let cascade = behavior == Some(DropBehavior::Cascade);
        // We don't support cascade and purge for now.
        // nor do we support multiple object names
        let name = match names.len() {
            0 => Err(parser_error("Missing table name.")),
            1 => Ok(names.pop().unwrap()),
            _ => Err(parser_error("Multiple objects not supported")),
        }?;

        match object_type {
            ObjectType::Table => {
                Ok(LogicalPlan::Ddl(DdlStatement::DropTable(DropTable {
                    name: self.object_name_to_table_reference(name)?,
                    if_exists,
                    schema: DFSchemaRef::new(DFSchema::empty()),
                })))
            }
            ObjectType::View => Ok(LogicalPlan::Ddl(DdlStatement::DropView(DropView {
                name: self.object_name_to_table_reference(name)?,
                if_exists,
                schema: DFSchemaRef::new(DFSchema::empty()),
            }))),
            ObjectType::Schema => {
                let name = match self.object_name_to_table_reference(name)? {
                    TableReference::Bare { table } => {
                        Ok(SchemaReference::Bare { schema: table })
                    }
                    TableReference::Partial { schema, table } => {
                        Ok(SchemaReference::Full {
                            schema: table,
                            catalog: schema,
                        })
                    }
                    TableReference::Full {
                        catalog: _,
                        schema: _,
                        table: _,
                    } => Err(parser_error("Invalid schema specifier (has 3 parts)")),
                }?;
                Ok(LogicalPlan::Ddl(DdlStatement::DropCatalogSchema(
                    DropCatalogSchema {
                        name,
                        if_exists,
                        cascade,
                        schema: DFSchemaRef::new(DFSchema::empty()),
                    },
                )))
            }
            ObjectType::Database => Ok(LogicalPlan::Ddl(DdlStatement::DropCatalog(
                DropCatalog::builder(object_name_to_string(&name))
                    .with_if_exists(if_exists)
                    .with_cascade(cascade)
                    .build(),
            ))),
            _ => not_impl_err!(
                "Only `DROP TABLE/VIEW/SCHEMA/CATALOG/DATABASE  ...` statement is supported currently"
            ),
        }
    }

    fn create_function_to_plan(
        &self,
        create: PgCreateFunction<'_>,
    ) -> Result<LogicalPlan> {
        let PgCreateFunction {
            or_replace,
            name,
            args,
            returns,
            language,
            volatility,
            body,
            ..
        } = create;
        // PostgreSQL has no temporary functions
        let temporary = false;
        let return_type = match returns {
            Some(FunctionReturn::Type(t)) => Some(self.convert_data_type_to_field(&t)?),
            Some(FunctionReturn::SetOf(_)) => {
                return not_impl_err!(
                    "RETURNS SETOF in CREATE FUNCTION is not supported"
                );
            }
            Some(FunctionReturn::Table(_)) => {
                return not_impl_err!(
                    "RETURNS TABLE in CREATE FUNCTION is not supported"
                );
            }
            None => None,
        };
        let mut planner_context = PlannerContext::new();
        let empty_schema = &DFSchema::empty();

        let args = args
            .into_iter()
            .map(|arg| {
                // Name the field after the argument, so that it can also
                // be used as the type of the placeholder in the body
                let arg_name = arg.name.as_ref().map(|name| name.value());
                let field = self
                    .convert_data_type_to_field(&arg.data_type)?
                    .renamed(arg_name.as_deref().unwrap_or(""));

                let default_expr = match arg.default {
                    Some((_, expr)) => Some(self.sql_to_expr(
                        expr,
                        empty_schema,
                        &mut planner_context,
                    )?),
                    None => None,
                };
                Ok(OperateFunctionArg {
                    name: arg.name.as_ref().map(to_df_ident),
                    default_expr,
                    field,
                })
            })
            .collect::<Result<Vec<OperateFunctionArg>>>()?;
        let args = Some(args);
        // Validate default arguments
        let first_default = args
            .as_ref()
            .and_then(|arg| arg.iter().position(|t| t.default_expr.is_some()));
        let last_non_default = args
            .as_ref()
            .and_then(|arg| arg.iter().rposition(|t| t.default_expr.is_none()));
        if let (Some(pos_default), Some(pos_non_default)) =
            (first_default, last_non_default)
            && pos_non_default > pos_default
        {
            return plan_err!("Non-default arguments cannot follow default arguments.");
        }
        // At the moment functions can't be qualified `schema.name`
        let name = match &name.parts[..] {
            [] => exec_err!("Function should have name")?,
            [n] => n.value().into_owned(),
            [..] => not_impl_err!("Qualified functions are not supported")?,
        };
        //
        // Convert resulting expression to data fusion expression
        //
        let arg_types = args
            .as_ref()
            .map(|arg| arg.iter().map(|t| Arc::clone(&t.field)).collect::<Vec<_>>());
        // Validate parameter style
        if let Some(ref fields) = arg_types {
            let count_positional = fields.iter().filter(|f| f.name() == "").count();
            if !(count_positional == 0 || count_positional == fields.len()) {
                return plan_err!(
                    "All function arguments must use either named or positional style."
                );
            }
        }
        let mut planner_context = PlannerContext::new().with_prepare_param_data_types(
            arg_types
                .unwrap_or_default()
                .into_iter()
                .map(Some)
                .collect(),
        );

        let function_body = match body {
            Some(body) => {
                let expr: SQLExpr = match body {
                    // `link_symbol` indicates if the primary expression contains the name of shared library file.
                    FunctionBody::As {
                        body,
                        link_symbol: _link_symbol,
                    } => ExprKind::Literal(body).into(),
                    FunctionBody::Return(expr) => expr,
                    FunctionBody::BeginAtomic(_) => {
                        return not_impl_err!(
                            "BEGIN/END enclosed function body syntax is not supported"
                        )?;
                    }
                };
                Some(self.sql_to_expr(expr, &DFSchema::empty(), &mut planner_context)?)
            }
            None => None,
        };

        let behavior = match volatility {
            Some(volatility) => {
                if volatility.eq_ignore_ascii_case("IMMUTABLE") {
                    Some(Volatility::Immutable)
                } else if volatility.eq_ignore_ascii_case("STABLE") {
                    Some(Volatility::Stable)
                } else if volatility.eq_ignore_ascii_case("VOLATILE") {
                    Some(Volatility::Volatile)
                } else {
                    return plan_err!("Unknown function volatility: {volatility}");
                }
            }
            None => None,
        };

        let params = CreateFunctionBody {
            language: language.as_ref().map(to_df_ident),
            behavior,
            function_body,
        };

        let statement = DdlStatement::CreateFunction(Box::new(CreateFunction::try_new(
            or_replace,
            temporary,
            name,
            args,
            return_type,
            params,
        )?));

        Ok(LogicalPlan::Ddl(statement))
    }

    fn drop_function_to_plan(&self, drop: DropRoutine<'_>) -> Result<LogicalPlan> {
        let DropRoutine {
            procedure,
            if_exists,
            routines,
            behavior: _,
        } = drop;
        if procedure {
            return not_impl_err!("DROP PROCEDURE is not supported");
        }
        // According to postgresql documentation it can be only one function
        // specified in drop statement
        if let Some(desc) = routines.first() {
            // At the moment functions can't be qualified `schema.name`
            let name = match &desc.name.parts[..] {
                [] => exec_err!("Function should have name")?,
                [n] => n.value().into_owned(),
                [..] => not_impl_err!("Qualified functions are not supported")?,
            };
            let statement = DdlStatement::DropFunction(DropFunction {
                if_exists,
                name,
                schema: DFSchemaRef::new(DFSchema::empty()),
            });
            Ok(LogicalPlan::Ddl(statement))
        } else {
            exec_err!("Function name not provided")
        }
    }

    fn truncate_to_plan(&self, name: ObjectName<'_>) -> Result<LogicalPlan> {
        let table = self.object_name_to_table_reference(name)?;
        let source = self.context_provider.get_table_source(table.clone())?;

        // TRUNCATE does not operate on input rows. The EmptyRelation is a logical placeholder
        // since the real operation is executed directly by the TableProvider's truncate() hook.
        Ok(LogicalPlan::Dml(DmlStatement::new(
            table.clone(),
            source,
            WriteOp::Truncate,
            Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: DFSchemaRef::new(DFSchema::empty()),
            })),
        )))
    }

    fn create_index_to_plan(
        &self,
        create: SQLCreateIndex<'_>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let SQLCreateIndex {
            name,
            table,
            using,
            using_before_on,
            trailing_using,
            columns,
            unique,
            if_not_exists,
            ..
        } = create;
        let name: Option<String> = name.as_ref().map(object_name_to_string);
        let table = self.object_name_to_table_reference(table)?;
        let table_schema = self
            .context_provider
            .get_table_source(table.clone())?
            .schema()
            .to_dfschema_ref()?;
        let using: Option<String> = using
            .or(using_before_on)
            .or(trailing_using)
            .as_ref()
            .map(ident_to_string);
        let order_by_exprs = columns.into_iter().map(|col| col.item).collect();
        let columns = self.order_by_to_sort_expr(
            order_by_exprs,
            &table_schema,
            planner_context,
            false,
            None,
        )?;
        Ok(LogicalPlan::Ddl(DdlStatement::CreateIndex(
            PlanCreateIndex {
                name,
                table,
                using,
                columns,
                unique,
                if_not_exists,
                schema: DFSchemaRef::new(DFSchema::empty()),
            },
        )))
    }

    fn transaction_to_plan(&self, transaction: Transaction<'_>) -> Result<LogicalPlan> {
        match transaction {
            Transaction::Start(modes) => {
                let isolation_level = modes
                    .iter()
                    .filter_map(|m| match m {
                        TransactionMode::IsolationLevel(level) => Some(level),
                        TransactionMode::ReadOnly | TransactionMode::ReadWrite => None,
                    })
                    .next_back()
                    .copied()
                    .unwrap_or(IsolationLevel::Serializable);
                let read_only = modes
                    .iter()
                    .filter_map(|m| match m {
                        TransactionMode::ReadOnly => Some(true),
                        TransactionMode::ReadWrite => Some(false),
                        TransactionMode::IsolationLevel(_) => None,
                    })
                    .next_back()
                    .unwrap_or(false);
                let isolation_level = match isolation_level {
                    IsolationLevel::ReadUncommitted => {
                        TransactionIsolationLevel::ReadUncommitted
                    }
                    IsolationLevel::ReadCommitted => {
                        TransactionIsolationLevel::ReadCommitted
                    }
                    IsolationLevel::RepeatableRead => {
                        TransactionIsolationLevel::RepeatableRead
                    }
                    IsolationLevel::Serializable => {
                        TransactionIsolationLevel::Serializable
                    }
                    IsolationLevel::Snapshot => TransactionIsolationLevel::Snapshot,
                };
                let access_mode = if read_only {
                    TransactionAccessMode::ReadOnly
                } else {
                    TransactionAccessMode::ReadWrite
                };
                let statement = PlanStatement::TransactionStart(TransactionStart {
                    access_mode,
                    isolation_level,
                });
                Ok(LogicalPlan::Statement(statement))
            }
            Transaction::Finish {
                kind: FinishKind::Commit,
                chain,
                savepoint: _,
            } => {
                let statement = PlanStatement::TransactionEnd(TransactionEnd {
                    conclusion: TransactionConclusion::Commit,
                    chain,
                });
                Ok(LogicalPlan::Statement(statement))
            }
            Transaction::Finish {
                kind: FinishKind::End,
                ..
            } => not_impl_err!("COMMIT AND END not supported"),
            Transaction::Finish {
                kind: FinishKind::Rollback | FinishKind::Abort,
                chain,
                savepoint,
            } => {
                if savepoint.is_some() {
                    plan_err!("Savepoints not supported")?;
                }
                let statement = PlanStatement::TransactionEnd(TransactionEnd {
                    conclusion: TransactionConclusion::Rollback,
                    chain,
                });
                Ok(LogicalPlan::Statement(statement))
            }
            // `BEGIN`, `SAVEPOINT` and `RELEASE`
            transaction => not_impl_err!("Unsupported SQL statement: {transaction}"),
        }
    }

    /// Plans `SHOW ...`
    fn show_to_plan(&self, show: Show<'_>) -> Result<LogicalPlan> {
        let Show {
            modifiers,
            object,
            name,
            clauses,
        } = show;
        let has_modifier =
            |word: &str| modifiers.iter().any(|m| m.eq_ignore_ascii_case(word));
        let words: Vec<String> = object.iter().map(|w| w.to_ascii_uppercase()).collect();
        let words: Vec<&str> = words.iter().map(String::as_str).collect();
        match words.as_slice() {
            ["CREATE", kind] => match (*kind, name) {
                ("TABLE", Some(name)) => self.show_create_table_to_plan(name),
                _ => {
                    not_impl_err!("Only `SHOW CREATE TABLE  ...` statement is supported")
                }
            },
            ["TABLES"] | ["EXTERNAL", "TABLES"] => {
                // We only support the basic "SHOW TABLES"
                // https://github.com/apache/datafusion/issues/3188
                if has_modifier("EXTENDED") {
                    return not_impl_err!("SHOW TABLES EXTENDED not supported")?;
                }
                if has_modifier("FULL") {
                    return not_impl_err!("SHOW FULL TABLES not supported")?;
                }
                if has_modifier("TERSE") {
                    return not_impl_err!("SHOW TERSE TABLES not supported")?;
                }
                if let Some(modifier) = modifiers.first() {
                    return not_impl_err!("SHOW {modifier} TABLES not supported")?;
                }
                if clauses.iter().any(|c| matches!(c, ShowClause::History)) {
                    return not_impl_err!("SHOW TABLES HISTORY not supported")?;
                }
                if words.len() == 2 {
                    return not_impl_err!("SHOW EXTERNAL TABLES not supported")?;
                }
                for clause in &clauses {
                    match clause {
                        ShowClause::History => {}
                        ShowClause::Location { .. } => {
                            return not_impl_err!("SHOW TABLES IN not supported")?;
                        }
                        ShowClause::StartsWith(_) => {
                            return not_impl_err!("SHOW TABLES LIKE not supported")?;
                        }
                        ShowClause::Limit(_) => {
                            return not_impl_err!("SHOW TABLES LIMIT not supported")?;
                        }
                        ShowClause::FromValue(_) => {
                            return not_impl_err!(
                                "SHOW TABLES LIMIT FROM not supported"
                            )?;
                        }
                        ShowClause::Like(_)
                        | ShowClause::Where(_)
                        | ShowClause::Pattern(_) => {
                            return not_impl_err!("SHOW TABLES FILTER not supported")?;
                        }
                    }
                }
                self.show_tables_to_plan()
            }
            ["COLUMNS"] => {
                let extended = has_modifier("EXTENDED");
                let full = has_modifier("FULL");
                let mut table_name = None;
                for clause in clauses {
                    match clause {
                        ShowClause::StartsWith(_) => {
                            return not_impl_err!("SHOW COLUMNS LIKE not supported")?;
                        }
                        ShowClause::Limit(_) => {
                            return not_impl_err!("SHOW COLUMNS LIMIT not supported")?;
                        }
                        ShowClause::FromValue(_) => {
                            return not_impl_err!(
                                "SHOW COLUMNS LIMIT FROM not supported"
                            )?;
                        }
                        ShowClause::Like(_)
                        | ShowClause::Where(_)
                        | ShowClause::Pattern(_) => {
                            return not_impl_err!(
                                "SHOW COLUMNS with WHERE or LIKE is not supported"
                            )?;
                        }
                        ShowClause::History => {
                            return not_impl_err!("SHOW COLUMNS HISTORY not supported")?;
                        }
                        // specifies if the syntax was `SHOW COLUMNS IN` or `SHOW
                        // COLUMNS FROM` which is not different in DataFusion
                        ShowClause::Location {
                            from: _,
                            scope,
                            name,
                        } => {
                            if let Some(scope) = scope {
                                return not_impl_err!(
                                    "SHOW COLUMNS IN {scope} not supported"
                                );
                            }
                            if table_name.is_some() {
                                return not_impl_err!(
                                    "SHOW COLUMNS with more than one IN or FROM not supported"
                                );
                            }
                            let Some(name) = name else {
                                return plan_err!("SHOW COLUMNS requires a table name");
                            };
                            table_name = Some(name);
                        }
                    }
                }
                let Some(table_name) = table_name else {
                    return plan_err!("SHOW COLUMNS requires a table name");
                };
                self.show_columns_to_plan(extended, full, table_name)
            }
            ["FUNCTIONS"] => {
                let filter = match clauses.as_slice() {
                    [] => None,
                    [ShowClause::Like(like)] => Some(like.value().into_owned()),
                    _ => return plan_err!("Unsupported SHOW FUNCTIONS filter"),
                };
                LogicalPlanBuilder::show_functions(filter)?.build()
            }
            _ => {
                if !modifiers.is_empty() || !clauses.is_empty() || name.is_some() {
                    return not_impl_err!("Unsupported SHOW statement");
                }
                let variable = object.iter().map(|w| w.to_lowercase()).collect();
                self.show_variable_to_plan(variable)
            }
        }
    }

    fn get_delete_target<'s>(&self, table: TableRef<'s>) -> Result<ObjectName<'s>> {
        let TableFactor::Named(name) = table.factor else {
            return not_impl_err!(
                "DELETE FROM only supports single table, got: {}",
                table.factor
            );
        };
        Ok(name)
    }

    /// Generate a logical plan from a "SHOW TABLES" query
    fn show_tables_to_plan(&self) -> Result<LogicalPlan> {
        LogicalPlanBuilder::show_tables()?.build()
    }

    fn describe_table_to_plan(&self, table_name: ObjectName<'_>) -> Result<LogicalPlan> {
        let table_ref = self.object_name_to_table_reference(table_name)?;
        // Do a table lookup to verify the table exists
        let _ = self.context_provider.get_table_source(table_ref.clone())?;

        LogicalPlanBuilder::show_columns(table_ref, ColumnsDetail::Describe)?.build()
    }

    fn describe_query_to_plan(&self, query: Query<'_>) -> Result<LogicalPlan> {
        let plan = self.query_to_plan(query, &mut PlannerContext::new())?;

        let schema = Arc::new(plan.schema().as_arrow().clone());

        LogicalPlanBuilder::describe(schema)?.build()
    }

    fn copy_to_plan(&self, statement: CopyToStatement<'_>) -> Result<LogicalPlan> {
        // Determine if source is table or query and handle accordingly
        let copy_source = statement.source;
        let (input, input_schema, table_ref) = match copy_source {
            CopyToSource::Relation(object_name) => {
                let table_name = object_name_to_string(&object_name);
                let table_ref = self.object_name_to_table_reference(object_name)?;
                let table_source =
                    self.context_provider.get_table_source(table_ref.clone())?;
                let plan =
                    LogicalPlanBuilder::scan(table_name, table_source, None)?.build()?;
                let input_schema = Arc::clone(plan.schema());
                (plan, input_schema, Some(table_ref))
            }
            CopyToSource::Query(query) => {
                let plan = self.query_to_plan(*query, &mut PlannerContext::new())?;
                let input_schema = Arc::clone(plan.schema());
                (plan, input_schema, None)
            }
        };

        let options_map = self.parse_options_map(statement.options, true)?;

        let maybe_file_type = if let Some(stored_as) = &statement.stored_as {
            self.context_provider.get_file_type(stored_as).ok()
        } else {
            None
        };

        let file_type = match maybe_file_type {
            Some(ft) => ft,
            None => {
                let e = || {
                    DataFusionError::Configuration(
                        "Format not explicitly set and unable to get file extension! Use STORED AS to define file format."
                            .to_string(),
                    )
                };
                // Try to infer file format from file extension
                let extension: &str = &Path::new(&statement.target)
                    .extension()
                    .ok_or_else(e)?
                    .to_str()
                    .ok_or_else(e)?
                    .to_lowercase();

                self.context_provider.get_file_type(extension)?
            }
        };

        let partition_by = statement
            .partitioned_by
            .iter()
            .map(|col| input_schema.field_with_name(table_ref.as_ref(), col))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|f| f.name().to_owned())
            .collect();

        Ok(LogicalPlan::Copy(CopyTo::new(
            Arc::new(input),
            statement.target,
            partition_by,
            file_type,
            options_map,
        )))
    }

    fn build_order_by(
        &self,
        order_exprs: Vec<LexOrdering<'_>>,
        schema: &DFSchemaRef,
        planner_context: &mut PlannerContext,
    ) -> Result<Vec<Vec<SortExpr>>> {
        if !order_exprs.is_empty() && schema.fields().is_empty() {
            let results = order_exprs
                .into_iter()
                .map(|lex_order| {
                    let result = lex_order
                        .into_iter()
                        .map(|order_by_expr| {
                            if let Some(op) = &order_by_expr.using {
                                return not_impl_err!(
                                    "ORDER BY USING is not supported: {op}"
                                );
                            }
                            let asc = order_by_expr.asc.unwrap_or(true);
                            let nulls_first =
                                order_by_expr.nulls_first.unwrap_or_else(|| {
                                    self.options.default_null_ordering.nulls_first(asc)
                                });
                            let ordered_expr = self.sql_expr_to_logical_expr(
                                order_by_expr.expr,
                                schema,
                                planner_context,
                            )?;

                            Ok(SortExpr::new(ordered_expr, asc, nulls_first))
                        })
                        .collect::<Result<Vec<SortExpr>>>()?;
                    Ok(result)
                })
                .collect::<Result<Vec<Vec<SortExpr>>>>()?;

            return Ok(results);
        }

        let mut all_results = Vec::with_capacity(order_exprs.len());
        for expr in order_exprs {
            // Convert each OrderByItem to a SortExpr:
            let expr_vec =
                self.order_by_to_sort_expr(expr, schema, planner_context, true, None)?;
            // Verify that columns of all SortExprs exist in the schema:
            for sort in expr_vec.iter() {
                for column in sort.expr.column_refs().iter() {
                    if !schema.has_column(column) {
                        // Return an error if any column is not in the schema:
                        return plan_err!("Column {column} is not in schema");
                    }
                }
            }
            // If all SortExprs are valid, return them as an expression vector
            all_results.push(expr_vec)
        }
        Ok(all_results)
    }

    /// Generate a logical plan from a CREATE EXTERNAL TABLE statement
    fn external_table_to_plan(
        &self,
        statement: CreateExternalTable<'_>,
    ) -> Result<LogicalPlan> {
        let definition = Some(statement.to_string());
        let CreateExternalTable {
            name,
            columns,
            file_type,
            locations,
            table_partition_cols,
            if_not_exists,
            temporary,
            order_exprs,
            unbounded,
            options,
            constraints,
            or_replace,
        } = statement;

        // Merge inline constraints and existing constraints
        let mut all_constraints = constraints;
        let inline_constraints = calc_inline_constraints_from_columns(&columns);
        all_constraints.extend(inline_constraints);

        let options_map = self.parse_options_map(options, false)?;

        let compression = options_map
            .get("format.compression")
            .map(|c| CompressionTypeVariant::from_str(c))
            .transpose()?;
        if (file_type == "PARQUET" || file_type == "AVRO" || file_type == "ARROW")
            && compression
                .map(|c| c != CompressionTypeVariant::UNCOMPRESSED)
                .unwrap_or(false)
        {
            plan_err!(
                "File compression type cannot be set for PARQUET, AVRO, or ARROW files."
            )?;
        }

        let mut planner_context = PlannerContext::new();

        let column_defaults = self
            .build_column_defaults(&columns, &mut planner_context)?
            .into_iter()
            .collect();

        let schema = self.build_schema(columns)?;
        let df_schema = schema.to_dfschema_ref()?;
        df_schema.check_names()?;

        let ordered_exprs =
            self.build_order_by(order_exprs, &df_schema, &mut planner_context)?;

        let name = self.object_name_to_table_reference(name)?;
        let constraints =
            self.new_constraint_from_table_constraints(&all_constraints, &df_schema)?;

        let Some(location) = locations.first().cloned() else {
            return plan_err!("CREATE EXTERNAL TABLE requires at least one location");
        };

        // Keep the existing single-location builder API: seed it with the first
        // location, then replace it with the complete list.
        Ok(LogicalPlan::Ddl(DdlStatement::CreateExternalTable(
            Box::new(
                PlanCreateExternalTable::builder(name, location, file_type, df_schema)
                    .with_locations(locations)
                    .with_partition_cols(table_partition_cols)
                    .with_if_not_exists(if_not_exists)
                    .with_or_replace(or_replace)
                    .with_temporary(temporary)
                    .with_definition(definition)
                    .with_order_exprs(ordered_exprs)
                    .with_unbounded(unbounded)
                    .with_options(options_map)
                    .with_constraints(constraints)
                    .with_column_defaults(column_defaults)
                    .build(),
            ),
        )))
    }

    fn external_catalog_to_plan(
        &self,
        statement: CreateExternalCatalog<'_>,
    ) -> Result<LogicalPlan> {
        let CreateExternalCatalog {
            catalog_name,
            catalog_type,
            location,
            if_not_exists,
            or_replace,
            options,
        } = statement;

        let mut options_map = HashMap::with_capacity(options.len());
        for (key, value) in options {
            if options_map.contains_key(&key) {
                return plan_err!("Option {key} is specified multiple times");
            }
            let Some(value_string) = value.as_str() else {
                return plan_err!("Unsupported Value {}", value);
            };
            options_map.insert(key, value_string.to_string());
        }

        Ok(LogicalPlan::Ddl(DdlStatement::CreateExternalCatalog(
            Box::new(
                PlanCreateExternalCatalog::builder(
                    object_name_to_string(&catalog_name),
                    catalog_type,
                )
                .with_location(location)
                .with_if_not_exists(if_not_exists)
                .with_or_replace(or_replace)
                .with_options(options_map)
                .build(),
            ),
        )))
    }

    /// Get the indices of the constraint columns in the schema.
    /// If any column is not found, return an error.
    fn get_constraint_column_indices(
        &self,
        df_schema: &DFSchemaRef,
        columns: &[Ident<'_>],
        constraint_name: &str,
    ) -> Result<Vec<usize>> {
        let field_names = df_schema.field_names();
        columns
            .iter()
            .map(|ident| {
                let column = self.ident_normalizer.normalize(ident.clone());
                field_names
                    .iter()
                    .position(|item| *item == column)
                    .ok_or_else(|| {
                        plan_datafusion_err!(
                            "Column for {constraint_name} not found in schema: {column}"
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Convert each [TableConstraint] to corresponding [Constraint]
    pub fn new_constraint_from_table_constraints(
        &self,
        constraints: &[TableConstraint<'_>],
        df_schema: &DFSchemaRef,
    ) -> Result<Constraints> {
        let constraints = constraints
            .iter()
            .map(|c: &TableConstraint| match &c.kind {
                TableConstraintKind::Unique(columns) => {
                    let constraint_name = match &c.name {
                        Some(name) => &format!("unique constraint with name '{name}'"),
                        None => "unique constraint",
                    };
                    // Get unique constraint indices in the schema
                    let indices = self.get_constraint_column_indices(
                        df_schema,
                        columns,
                        constraint_name,
                    )?;
                    Ok(Constraint::Unique(indices))
                }
                TableConstraintKind::PrimaryKey(columns) => {
                    // Get primary key indices in the schema
                    let indices = self.get_constraint_column_indices(
                        df_schema,
                        columns,
                        "primary key",
                    )?;
                    Ok(Constraint::PrimaryKey(indices))
                }
                TableConstraintKind::ForeignKey { .. } => {
                    _plan_err!("Foreign key constraints are not currently supported")
                }
                TableConstraintKind::Exclude(_) => {
                    _plan_err!("Exclude constraints are not currently supported")
                }
                TableConstraintKind::Check(_) => {
                    _plan_err!("Check constraints are not currently supported")
                }
                TableConstraintKind::Key { .. } => {
                    _plan_err!("Indexes are not currently supported")
                }
                TableConstraintKind::NotNull(_) => {
                    _plan_err!("NOT NULL table constraints are not currently supported")
                }
                TableConstraintKind::PrimaryKeyUsingIndex(_) => {
                    _plan_err!(
                        "PRIMARY KEY USING INDEX constraints are not currently supported"
                    )
                }
                TableConstraintKind::UniqueUsingIndex(_) => {
                    _plan_err!(
                        "UNIQUE USING INDEX constraints are not currently supported"
                    )
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Constraints::new_unverified(constraints))
    }

    fn parse_options_map(
        &self,
        options: Vec<(String, OptionValue)>,
        allow_duplicates: bool,
    ) -> Result<HashMap<String, String>> {
        let mut options_map = HashMap::with_capacity(options.len());
        for (key, value) in options {
            if !allow_duplicates && options_map.contains_key(&key) {
                return plan_err!("Option {key} is specified multiple times");
            }

            let Some(value_string) = value.as_str().map(str::to_string) else {
                return plan_err!("Unsupported Value {}", value);
            };

            if !(&key.contains('.')) {
                // If config does not belong to any namespace, assume it is
                // a format option and apply the format prefix for backwards
                // compatibility.
                let renamed_key = format!("format.{key}");
                options_map.insert(renamed_key.to_lowercase(), value_string);
            } else {
                options_map.insert(key.to_lowercase(), value_string);
            }
        }

        Ok(options_map)
    }

    /// Generate a plan for EXPLAIN ... that will print out a plan
    ///
    /// Note this is the sqlparser explain statement, not the
    /// datafusion `EXPLAIN` statement.
    fn explain_to_plan(
        &self,
        opts: ExplainStatementOptions,
        statement: DFStatement<'_>,
    ) -> Result<LogicalPlan> {
        let plan = self.statement_to_plan(statement)?;
        if matches!(plan, LogicalPlan::Explain(_)) {
            return plan_err!("Nested EXPLAINs are not supported");
        }

        let plan = Arc::new(plan);
        let schema = LogicalPlan::explain_schema();
        let schema = schema.to_dfschema_ref()?;

        let ExplainStatementOptions {
            analyze,
            verbose,
            format,
            analyze_level,
            analyze_categories,
            show_statistics,
        } = opts;

        // Mutual exclusivity checks
        if verbose && format.is_some() {
            return plan_err!("EXPLAIN VERBOSE with FORMAT is not supported");
        }
        if !analyze {
            if analyze_level.is_some() {
                return plan_err!("EXPLAIN option LEVEL requires ANALYZE");
            }
            if analyze_categories.is_some() {
                return plan_err!("EXPLAIN option METRICS requires ANALYZE");
            }
        }
        if analyze && show_statistics.is_some() {
            return plan_err!("EXPLAIN option COSTS cannot be combined with ANALYZE");
        }

        // Resolve the requested output format.
        //
        // Verbose mode only supports indent format, and for EXPLAIN ANALYZE
        // only `Indent` and `PostgresJSON` are supported today — `Tree` and
        // `Graphviz` require additional work to render with live metrics.
        let options = self.context_provider.options();
        let format = if verbose {
            ExplainFormat::Indent
        } else if let Some(format) = format {
            format
        } else if analyze {
            ExplainFormat::Indent
        } else {
            options.explain.format.clone()
        };

        if analyze {
            match &format {
                ExplainFormat::Indent => {}
                ExplainFormat::PostgresJSON => {
                    // The pgjson renderer does not emit statistics yet, so
                    // reject the combination rather than silently ignoring it.
                    if options.explain.show_statistics {
                        return plan_err!(
                            "EXPLAIN ANALYZE with FORMAT pgjson does not support show_statistics"
                        );
                    }
                }
                ExplainFormat::Tree | ExplainFormat::Graphviz => {
                    return plan_err!(
                        "EXPLAIN ANALYZE with FORMAT {format} is not supported"
                    );
                }
            }
            Ok(LogicalPlan::Analyze(Analyze {
                verbose,
                format,
                input: plan,
                schema,
                analyze_level,
                analyze_categories,
            }))
        } else {
            let stringified_plans =
                vec![plan.to_stringified(PlanType::InitialLogicalPlan)];

            Ok(LogicalPlan::Explain(Explain {
                verbose,
                explain_format: format,
                plan,
                stringified_plans,
                schema,
                logical_optimization_succeeded: false,
                show_statistics,
            }))
        }
    }

    /// `SHOW variable`, where `variable` holds the lower-cased words of the name
    fn show_variable_to_plan(&self, mut variable: Vec<String>) -> Result<LogicalPlan> {
        let verbose = variable.last().map(|s| s == "verbose").unwrap_or(false);

        if verbose {
            variable.pop();
        }

        let variable = variable.join(".");
        let name = if variable == "all" {
            None
        } else {
            // No need to validate if the variable exists here; that is handled at execution time.
            Some(variable)
        };

        LogicalPlanBuilder::show_variables(name, verbose)?.build()
    }

    fn set_statement_to_plan(&self, statement: SetStatement<'_>) -> Result<LogicalPlan> {
        let (scope, variable, values) = match statement {
            SetStatement::Variable {
                scope,
                name,
                values,
            } => (scope, name, values),
            // A single assignment, in dialects that can assign several at once
            SetStatement::Assignments(mut assignments) if assignments.len() == 1 => {
                let assignment = assignments.pop().unwrap();
                (assignment.scope, assignment.name, vec![assignment.value])
            }
            other => return not_impl_err!("SET variant not implemented yet: {other:?}"),
        };
        if scope.is_some() {
            return not_impl_err!("SET with scope modifiers is not supported");
        }

        let variable_lower = canonical_variable_name(&variable);

        if values.len() != 1 {
            return plan_err!("SET only supports single value assignment");
        }

        let value_string = match &values[0].kind {
            ExprKind::Ident(i) => ident_to_string(i),
            ExprKind::Literal(v) => match crate::utils::value_to_string(v) {
                None => {
                    return plan_err!("Unsupported value {:?}", v);
                }
                Some(s) => s,
            },
            ExprKind::Unary { op, expr } => match op {
                UnaryOp::Plus => format!("+{expr}"),
                UnaryOp::Neg => format!("-{expr}"),
                _ => return plan_err!("Unsupported unary op {:?}", op),
            },
            _ => return plan_err!("Unsupported expr {:?}", values[0]),
        };

        Ok(LogicalPlan::Statement(PlanStatement::SetVariable(
            SetVariable {
                variable: variable_lower,
                value: value_string,
            },
        )))
    }

    fn reset_statement_to_plan(
        &self,
        statement: ResetStatement<'_>,
    ) -> Result<LogicalPlan> {
        match statement {
            ResetStatement::Variable(variable) => Ok(LogicalPlan::Statement(
                PlanStatement::ResetVariable(ResetVariable {
                    variable: canonical_variable_name(&variable),
                }),
            )),
        }
    }

    fn delete_statement_to_plan(&self, delete: Delete<'_>) -> Result<LogicalPlan> {
        let Delete {
            table,
            multi,
            using,
            output,
            selection,
            order_by,
            limit,
            returning,
        } = delete;
        if let Some(multi) = multi {
            if multi.from_keyword {
                plan_err!("DELETE <TABLE> not supported")?;
            }
            return not_impl_err!(
                "DELETE FROM only supports single table, got {}",
                multi.targets.len()
            );
        }

        if !using.is_empty() {
            plan_err!("Using clause not supported")?;
        }

        if !returning.is_empty() {
            plan_err!("Delete-returning clause not yet supported")?;
        }

        if !order_by.is_empty() {
            plan_err!("Delete-order-by clause not yet supported")?;
        }

        if limit.is_some() {
            return plan_err!("Delete-limit clause is not supported");
        }

        if output.is_some() {
            plan_err!("Delete OUTPUT clause not supported")?;
        }

        let table_name = self.get_delete_target(table)?;
        self.delete_to_plan(&table_name, selection)
    }

    fn delete_to_plan(
        &self,
        table_name: &ObjectName<'_>,
        predicate_expr: Option<SQLExpr<'_>>,
    ) -> Result<LogicalPlan> {
        // Do a table lookup to verify the table exists
        let table_ref = self.object_name_to_table_reference(table_name.clone())?;
        let table_source = self.context_provider.get_table_source(table_ref.clone())?;
        let schema = DFSchema::try_from_qualified_schema(
            table_ref.clone(),
            &table_source.schema(),
        )?;
        let scan =
            LogicalPlanBuilder::scan(table_ref.clone(), Arc::clone(&table_source), None)?
                .build()?;
        let mut planner_context = PlannerContext::new();

        let source = match predicate_expr {
            None => scan,
            Some(predicate_expr) => {
                let filter_expr =
                    self.sql_to_expr(predicate_expr, &schema, &mut planner_context)?;
                let schema = Arc::new(schema);
                let mut using_columns = HashSet::new();
                expr_to_columns(&filter_expr, &mut using_columns)?;
                let filter_expr = normalize_col_with_schemas_and_ambiguity_check(
                    filter_expr,
                    &[&[&schema]],
                    &[using_columns],
                )?;
                LogicalPlan::Filter(Filter::try_new(filter_expr, Arc::new(scan))?)
            }
        };

        let plan = LogicalPlan::Dml(DmlStatement::new(
            table_ref,
            table_source,
            WriteOp::Delete,
            Arc::new(source),
        ));
        Ok(plan)
    }

    fn update_statement_to_plan(&self, update: Update<'_>) -> Result<LogicalPlan> {
        let Update {
            table,
            joins,
            assignments,
            mut from,
            from_before_set: _,
            output,
            selection,
            order_by,
            limit,
            returning,
        } = update;
        // TODO: support multiple tables in UPDATE SET FROM
        if from.len() > 1 {
            not_impl_err!("Multiple tables in UPDATE SET FROM not yet supported")?;
        }
        let update_from = from.pop();

        // UPDATE ... FROM is currently not working
        // TODO fix https://github.com/apache/datafusion/issues/19950
        if update_from.is_some() {
            return not_impl_err!("UPDATE ... FROM is not supported");
        }

        if !returning.is_empty() {
            plan_err!("Update-returning clause not yet supported")?;
        }
        if limit.is_some() {
            return not_impl_err!("Update-limit clause not supported")?;
        }
        if output.is_some() {
            plan_err!("Update OUTPUT clause not supported")?;
        }
        if !order_by.is_empty() {
            plan_err!("Update ORDER BY not supported")?;
        }
        let table = TableWithJoins {
            relation: table,
            joins,
        };
        self.update_to_plan(table, &assignments, update_from, selection)
    }

    fn update_to_plan<'s>(
        &self,
        table: TableWithJoins<'s>,
        assignments: &[Assignment<'s>],
        from: Option<TableWithJoins<'s>>,
        predicate_expr: Option<SQLExpr<'s>>,
    ) -> Result<LogicalPlan> {
        let (table_name, table_alias) = match &table.relation.factor {
            TableFactor::Named(name) => (name.clone(), table.relation.alias.clone()),
            _ => plan_err!("Cannot update non-table relation!")?,
        };

        // Do a table lookup to verify the table exists
        let table_name = self.object_name_to_table_reference(table_name)?;
        let table_source = self.context_provider.get_table_source(table_name.clone())?;
        let table_schema = Arc::new(DFSchema::try_from_qualified_schema(
            table_name.clone(),
            &table_source.schema(),
        )?);

        // Overwrite with assignment expressions
        let mut planner_context = PlannerContext::new();
        let mut assign_map = assignments
            .iter()
            .map(|assign| {
                let col_name: &Ident = assign
                    .target
                    .parts
                    .last()
                    .ok_or_else(|| plan_datafusion_err!("Empty column id"))?;
                let col_name = col_name.value().into_owned();
                // Validate that the assignment target column exists
                table_schema.field_with_unqualified_name(&col_name)?;
                Ok((col_name, assign.value.clone()))
            })
            .collect::<Result<HashMap<String, SQLExpr>>>()?;

        // Build scan, join with from table if it exists.
        let mut input_tables = vec![table];
        input_tables.extend(from);
        let scan = self.plan_from_tables(input_tables, &mut planner_context)?;

        // Filter
        let source = match predicate_expr {
            None => scan,
            Some(predicate_expr) => {
                let filter_expr = self.sql_to_expr(
                    predicate_expr,
                    scan.schema(),
                    &mut planner_context,
                )?;
                let mut using_columns = HashSet::new();
                expr_to_columns(&filter_expr, &mut using_columns)?;
                let filter_expr = normalize_col_with_schemas_and_ambiguity_check(
                    filter_expr,
                    &[&[scan.schema()]],
                    &[using_columns],
                )?;
                LogicalPlan::Filter(Filter::try_new(filter_expr, Arc::new(scan))?)
            }
        };

        // Build updated values for each column, using the previous value if not modified
        let exprs = table_schema
            .iter()
            .map(|(qualifier, field)| {
                let expr = match assign_map.remove(field.name()) {
                    Some(new_value) => {
                        let mut expr = self.sql_to_expr(
                            new_value,
                            source.schema(),
                            &mut planner_context,
                        )?;
                        // Update placeholder's datatype to the type of the target column
                        if let Expr::Placeholder(placeholder) = &mut expr {
                            placeholder.field = placeholder
                                .field
                                .take()
                                .or_else(|| Some(Arc::clone(field)));
                        }
                        // Cast to target column type, if necessary
                        expr.cast_to(field.data_type(), source.schema())?
                    }
                    None => {
                        // If the target table has an alias, use it to qualify the column name
                        if let Some(alias) = &table_alias {
                            Expr::Column(Column::new(
                                Some(self.ident_normalizer.normalize(alias.name.clone())),
                                field.name(),
                            ))
                        } else {
                            Expr::Column(Column::from((qualifier, field)))
                        }
                    }
                };
                Ok(expr.alias(field.name()))
            })
            .collect::<Result<Vec<_>>>()?;

        let source = project(source, exprs)?;

        let plan = LogicalPlan::Dml(DmlStatement::new(
            table_name,
            table_source,
            WriteOp::Update,
            Arc::new(source),
        ));
        Ok(plan)
    }

    fn merge_to_plan(&self, merge: Merge<'_>) -> Result<LogicalPlan> {
        let Merge {
            table,
            source,
            on,
            clauses,
            into: _,
            output,
            returning,
        } = merge;

        if output.is_some() {
            return not_impl_err!("MERGE OUTPUT clause is not supported");
        }

        if !returning.is_empty() {
            return not_impl_err!("MERGE RETURNING clause is not supported");
        }

        if clauses.is_empty() {
            return plan_err!("MERGE INTO requires at least one WHEN clause");
        }

        // 1. Resolve target table
        let TableRef {
            factor,
            alias: target_alias,
            ..
        } = table;
        let target_table_name = match factor {
            TableFactor::Named(name) => {
                if target_alias
                    .as_ref()
                    .is_some_and(|alias| !alias.columns.is_empty())
                {
                    return not_impl_err!(
                        "MERGE target alias column lists are not supported"
                    );
                }
                name
            }
            TableFactor::TimeTravel(_)
            | TableFactor::Versioned { .. }
            | TableFactor::Sampled(_)
            | TableFactor::Ext(_) => {
                return not_impl_err!("MERGE target table modifiers are not supported");
            }
            _ => plan_err!("Cannot MERGE INTO non-table relation!")?,
        };
        let target_table_ref = self.object_name_to_table_reference(target_table_name)?;
        let target_table_source = self
            .context_provider
            .get_table_source(target_table_ref.clone())?;
        // Use alias as schema qualifier so `t.col` resolves when user writes
        // `MERGE INTO target AS t`. Fall back to the table reference itself.
        let target_qualifier = target_alias
            .as_ref()
            .map(|a| {
                TableReference::bare(self.ident_normalizer.normalize(a.name.clone()))
            })
            .unwrap_or_else(|| target_table_ref.clone());
        let target_schema = Arc::new(DFSchema::try_from_qualified_schema(
            target_qualifier.clone(),
            &target_table_source.schema(),
        )?);

        // 2. Plan the source (USING clause) as a LogicalPlan
        let mut planner_context = PlannerContext::new();
        let source_table_with_joins = TableWithJoins {
            relation: source,
            joins: vec![],
        };
        let source_plan =
            self.plan_from_tables(vec![source_table_with_joins], &mut planner_context)?;

        // 3. Build a combined schema for resolving expressions in ON and WHEN clauses
        let combined_schema = Arc::new(MergeIntoOp::expression_schema_for(
            &target_qualifier,
            &target_table_source.schema(),
            source_plan.schema(),
        )?);

        // 4. Convert the ON condition from sqlparser Expr to datafusion Expr
        let on_expr = self.sql_to_expr(on, &combined_schema, &mut planner_context)?;

        // 5. Convert each WHEN clause
        let df_clauses = clauses
            .into_iter()
            .map(|clause| {
                self.merge_clause_to_plan(
                    clause,
                    &combined_schema,
                    &target_schema,
                    &target_qualifier,
                    &mut planner_context,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        // 6. Preserve the target's visible qualifier in the public MERGE
        // representation. It is a scope-local SQL name, distinct from the
        // provider identity stored in `DmlStatement::table_name`.
        let merge_op = MergeIntoOp::new(target_qualifier, on_expr, df_clauses);

        Ok(LogicalPlan::Dml(DmlStatement::new(
            target_table_ref,
            target_table_source,
            WriteOp::MergeInto(Box::new(merge_op)),
            Arc::new(source_plan),
        )))
    }

    fn merge_target_column_name(
        &self,
        name: &ObjectName<'_>,
        target_qualifier: &TableReference,
    ) -> Result<String> {
        let ident = name
            .parts
            .last()
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("Empty column name"))?;

        if name.parts.len() > 1 {
            let qualifier = self.object_name_to_table_reference(ObjectName {
                parts: name.parts[..name.parts.len() - 1].to_vec(),
            })?;
            if !qualifier.resolved_eq(target_qualifier) {
                return plan_err!(
                    "MERGE assignment target '{name}' must reference target table \
                     '{target_qualifier}'"
                );
            }
        }

        Ok(self.ident_normalizer.normalize(ident))
    }

    fn merge_clause_to_plan(
        &self,
        clause: MergeClause<'_>,
        combined_schema: &DFSchema,
        target_schema: &DFSchema,
        target_qualifier: &TableReference,
        planner_context: &mut PlannerContext,
    ) -> Result<MergeIntoClause> {
        let kind = match clause.kind {
            MergeClauseKind::Matched => MergeIntoClauseKind::Matched,
            MergeClauseKind::NotMatched => MergeIntoClauseKind::NotMatched,
            MergeClauseKind::NotMatchedByTarget => {
                MergeIntoClauseKind::NotMatchedByTarget
            }
            MergeClauseKind::NotMatchedBySource => {
                MergeIntoClauseKind::NotMatchedBySource
            }
        };

        let predicate = clause
            .predicate
            .map(|p| self.sql_to_expr(p, combined_schema, planner_context))
            .transpose()?;

        let action = match clause.action {
            MergeAction::Update {
                assignments,
                predicate,
                delete_predicate,
            } => {
                if predicate.is_some() {
                    return not_impl_err!(
                        "MERGE UPDATE WHERE predicates are not supported"
                    );
                }
                if delete_predicate.is_some() {
                    return not_impl_err!(
                        "MERGE UPDATE DELETE WHERE predicates are not supported"
                    );
                }
                let Some(sql_assignments) = assignments else {
                    return not_impl_err!("MERGE UPDATE SET * is not supported");
                };
                let assignments = sql_assignments
                    .into_iter()
                    .map(|assign| {
                        let col_name = self
                            .merge_target_column_name(&assign.target, target_qualifier)?;
                        // Validate column exists in target
                        target_schema.field_with_unqualified_name(&col_name)?;
                        let value = self.sql_to_expr(
                            assign.value,
                            combined_schema,
                            planner_context,
                        )?;
                        Ok((col_name, value))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let mut seen = HashSet::with_capacity(assignments.len());
                for (column, _) in &assignments {
                    if !seen.insert(column.as_str()) {
                        return plan_err!("Duplicate column '{column}' in MERGE UPDATE");
                    }
                }
                MergeIntoAction::Update(assignments)
            }
            MergeAction::Insert {
                columns,
                rows,
                predicate,
            } => {
                if predicate.is_some() {
                    return not_impl_err!(
                        "MERGE INSERT WHERE predicates are not supported"
                    );
                }
                let columns: Vec<String> = columns
                    .iter()
                    .map(|c| self.merge_target_column_name(c, target_qualifier))
                    .collect::<Result<Vec<_>>>()?;

                // Validate: no duplicates, all columns exist in target schema
                let mut seen = HashSet::with_capacity(columns.len());
                for col in &columns {
                    if !seen.insert(col.as_str()) {
                        return plan_err!("Duplicate column '{col}' in MERGE INSERT");
                    }
                    target_schema.field_with_unqualified_name(col)?;
                }

                let num_target_cols = target_schema.fields().len();

                if rows.len() != 1 {
                    return plan_err!("MERGE INSERT must have exactly one row of values");
                }
                let row = rows.into_iter().next().unwrap();
                let expected = if columns.is_empty() {
                    num_target_cols
                } else {
                    columns.len()
                };
                if row.len() != expected {
                    return plan_err!(
                        "MERGE INSERT has {expected} column(s) but {} value(s)",
                        row.len()
                    );
                }
                let values = row
                    .into_iter()
                    .map(|v| self.sql_to_expr(v, combined_schema, planner_context))
                    .collect::<Result<Vec<_>>>()?;

                MergeIntoAction::Insert { columns, values }
            }
            MergeAction::InsertRow { .. } => {
                return not_impl_err!("MERGE INSERT ROW is not supported");
            }
            MergeAction::InsertAll => {
                return not_impl_err!("MERGE INSERT * is not supported");
            }
            MergeAction::Delete => MergeIntoAction::Delete,
            MergeAction::DoNothing => {
                return not_impl_err!(
                    "MERGE WHEN ... THEN DO NOTHING action is not supported"
                );
            }
        };

        Ok(MergeIntoClause {
            kind,
            predicate,
            action,
        })
    }

    /// Plans the standard `INSERT`, as a `REPLACE` if `replace_into` is set.
    fn insert_statement_to_plan(
        &self,
        insert: Insert<'_>,
        replace_into: bool,
    ) -> Result<LogicalPlan> {
        let Insert {
            modifiers,
            into: _, // optional keywords don't change behavior
            table,
            columns,
            output,
            source,
            returning,
        } = insert;
        if !returning.is_empty() {
            plan_err!("Insert-returning clause not supported")?;
        }
        if modifiers.ignore {
            plan_err!("Insert-ignore clause not supported")?;
        }
        if let Some(priority) = modifiers.priority {
            plan_err!("Inserts with a `PRIORITY` clause not supported: {priority:?}")?
        }
        if output.is_some() {
            plan_err!("Insert OUTPUT clause not supported")?;
        }
        let source = insert_source_to_query(Some(source))?;
        self.insert_to_plan(table, columns, source, modifiers.overwrite, replace_into)
    }

    /// `INSERT` with PostgreSQL's clauses
    fn postgres_insert_to_plan(&self, insert: PostgresInsert<'_>) -> Result<LogicalPlan> {
        let PostgresInsert {
            insert,
            alias,
            overriding,
            on_conflict,
        } = insert;
        if on_conflict.is_some() {
            plan_err!("Insert-on clause not supported")?;
        }
        if let Some(table_alias) = alias {
            plan_err!("Inserts with a table alias not supported: {table_alias:?}")?
        }
        if overriding.is_some() {
            plan_err!("Inserts with OVERRIDING not supported")?;
        }
        self.insert_statement_to_plan(insert, false)
    }

    /// `INSERT` or `REPLACE` with MySQL's clauses
    fn mysql_insert_to_plan(&self, insert: MySqlInsert<'_>) -> Result<LogicalPlan> {
        let MySqlInsert {
            replace,
            insert,
            alias,
            on_duplicate,
        } = insert;
        if !on_duplicate.is_empty() {
            plan_err!("Insert-on clause not supported")?;
        }
        if alias.is_some() {
            plan_err!("Inserts with an alias not supported")?;
        }
        self.insert_statement_to_plan(*insert, replace)
    }

    /// `INSERT OR ...` or `REPLACE`, with SQLite's clauses
    fn sqlite_insert_to_plan(&self, insert: SqliteInsert<'_>) -> Result<LogicalPlan> {
        let SqliteInsert {
            or_action,
            table,
            columns,
            source,
            on_conflict,
            returning,
        } = insert;
        let replace_into = match or_action {
            Some(action) if action.eq_ignore_ascii_case("REPLACE") => true,
            Some(action) => {
                return plan_err!("Inserts with OR {action} clause is not supported");
            }
            None => false,
        };
        if on_conflict.is_some() {
            plan_err!("Insert-on clause not supported")?;
        }
        if !returning.is_empty() {
            plan_err!("Insert-returning clause not supported")?;
        }
        let source = insert_source_to_query(source)?;
        self.insert_to_plan(table, columns, source, false, replace_into)
    }

    /// `INSERT [OVERWRITE]` with Spark's clauses
    fn spark_insert_to_plan(&self, insert: SparkInsert<'_>) -> Result<LogicalPlan> {
        let SparkInsert {
            overwrite,
            into: _,
            table_keyword: _,
            table,
            columns,
            partition,
            columns_after_partition,
            source,
        } = insert;
        if !partition.is_empty() {
            plan_err!("Partitioned inserts not yet supported")?;
        }
        if columns_after_partition {
            plan_err!("After-columns clause not supported")?;
        }
        let source = insert_source_to_query(Some(source))?;
        self.insert_to_plan(table, columns, source, overwrite, false)
    }

    fn insert_to_plan(
        &self,
        table_name: ObjectName<'_>,
        columns: Vec<Ident<'_>>,
        source: Box<Query<'_>>,
        overwrite: bool,
        replace_into: bool,
    ) -> Result<LogicalPlan> {
        // Do a table lookup to verify the table exists
        let table_name = self.object_name_to_table_reference(table_name)?;
        let table_source = self.context_provider.get_table_source(table_name.clone())?;
        let table_schema = DFSchema::try_from(table_source.schema())?;

        // Get insert fields and target table's value indices
        //
        // If value_indices[i] = Some(j), it means that the value of the i-th target table's column is
        // derived from the j-th output of the source.
        //
        // If value_indices[i] = None, it means that the value of the i-th target table's column is
        // not provided, and should be filled with a default value later.
        let (fields, value_indices) = if columns.is_empty() {
            // Empty means we're inserting into all columns of the table
            (
                table_schema.fields().clone(),
                (0..table_schema.fields().len())
                    .map(Some)
                    .collect::<Vec<_>>(),
            )
        } else {
            let mut value_indices = vec![None; table_schema.fields().len()];
            let fields = columns
                .into_iter()
                .enumerate()
                .map(|(i, c)| {
                    let c = self.ident_normalizer.normalize(c);
                    let column_index = table_schema
                        .index_of_column_by_name(None, &c)
                        .ok_or_else(|| unqualified_field_not_found(&c, &table_schema))?;

                    if value_indices[column_index].is_some() {
                        return schema_err!(SchemaError::DuplicateUnqualifiedField {
                            name: c,
                        });
                    } else {
                        value_indices[column_index] = Some(i);
                    }
                    Ok(Arc::clone(table_schema.field(column_index)))
                })
                .collect::<Result<Vec<_>>>()?;
            (Fields::from(fields), value_indices)
        };

        // infer types for Values clause... other types should be resolvable the regular way
        let mut prepare_param_data_types = BTreeMap::new();
        if let SetExpr::Values(rows) = &source.body {
            for row in rows.iter() {
                for (idx, val) in row.iter().enumerate() {
                    if let ExprKind::Placeholder(name) = &val.kind {
                        let index = match name[1..].parse::<usize>().map_err(|_| {
                            plan_datafusion_err!("Can't parse placeholder: {name}")
                        })? {
                            0 => {
                                return plan_err!(
                                    "Invalid placeholder, zero is not a valid index: {name}"
                                );
                            }
                            index => index - 1,
                        };
                        let field = fields.get(idx).ok_or_else(|| {
                            plan_datafusion_err!(
                                "Placeholder ${} refers to a non existent column",
                                idx + 1
                            )
                        })?;
                        let _ = prepare_param_data_types.insert(index, Arc::clone(field));
                    }
                }
            }
        }
        let prepare_param_data_types = {
            let len = prepare_param_data_types.keys().last().map_or(0, |&k| k + 1);
            (0..len)
                .map(|i| prepare_param_data_types.remove(&i))
                .collect()
        };

        // Projection
        let mut planner_context =
            PlannerContext::new().with_prepare_param_data_types(prepare_param_data_types);
        planner_context.set_table_schema(Some(DFSchemaRef::new(
            DFSchema::from_unqualified_fields(fields.clone(), Metadata::new())?,
        )));
        let source = self.query_to_plan(*source, &mut planner_context)?;
        if fields.len() != source.schema().fields().len() {
            plan_err!("Column count doesn't match insert query!")?;
        }

        let exprs = value_indices
            .into_iter()
            .enumerate()
            .map(|(i, value_index)| {
                let target_field = table_schema.field(i);
                let expr = match value_index {
                    Some(v) => {
                        Expr::Column(Column::from(source.schema().qualified_field(v)))
                            .cast_to(target_field.data_type(), source.schema())?
                    }
                    // The value is not specified. Fill in the default value for the column.
                    None => table_source
                        .get_column_default(target_field.name())
                        .cloned()
                        .unwrap_or_else(|| {
                            // If there is no default for the column, then the default is NULL
                            Expr::Literal(ScalarValue::Null, None)
                        })
                        .cast_to(target_field.data_type(), &DFSchema::empty())?,
                };
                let (_, expr_field) = expr.to_field(source.schema())?;
                // A storage-type cast alone does not apply extension metadata from the
                // table schema when the source and target storage types are identical.
                let expr = if target_field.extension_type_name().is_none()
                    || expr_field.metadata() == target_field.metadata()
                {
                    expr
                } else {
                    match expr {
                        Expr::Cast(cast) => Expr::Cast(Cast::new_from_field(
                            cast.expr,
                            Arc::clone(target_field),
                        )),
                        expr => Expr::Cast(Cast::new_from_field(
                            Box::new(expr),
                            Arc::clone(target_field),
                        )),
                    }
                };
                Ok(expr.alias(target_field.name()))
            })
            .collect::<Result<Vec<Expr>>>()?;
        let source = project(source, exprs)?;

        let insert_op = match (overwrite, replace_into) {
            (false, false) => InsertOp::Append,
            (true, false) => InsertOp::Overwrite,
            (false, true) => InsertOp::Replace,
            (true, true) => plan_err!(
                "Conflicting insert operations: `overwrite` and `replace_into` cannot both be true"
            )?,
        };

        let plan = LogicalPlan::Dml(DmlStatement::new(
            table_name,
            Arc::clone(&table_source),
            WriteOp::Insert(insert_op),
            Arc::new(source),
        ));
        Ok(plan)
    }

    fn show_columns_to_plan(
        &self,
        extended: bool,
        full: bool,
        sql_table_name: ObjectName<'_>,
    ) -> Result<LogicalPlan> {
        let table_ref = self.object_name_to_table_reference(sql_table_name)?;
        // Do a table lookup to verify the table exists
        let _ = self.context_provider.get_table_source(table_ref.clone())?;

        // Treat both FULL and EXTENDED as the same
        let detail = if full || extended {
            ColumnsDetail::Full
        } else {
            ColumnsDetail::Basic
        };

        LogicalPlanBuilder::show_columns(table_ref, detail)?.build()
    }

    fn show_create_table_to_plan(
        &self,
        sql_table_name: ObjectName<'_>,
    ) -> Result<LogicalPlan> {
        let table_ref = self.object_name_to_table_reference(sql_table_name)?;
        // Do a table lookup to verify the table exists
        let _ = self.context_provider.get_table_source(table_ref.clone())?;

        LogicalPlanBuilder::show_create_table(table_ref)?.build()
    }
}

fn check_plan(plan: &LogicalPlan) -> Result<()> {
    use datafusion_common::plan_err;
    use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
    plan.apply(|node| {
        for field in node.schema().fields() {
            if field.name().starts_with("__common_expr") {
                return plan_err!(
                    "{} is a reserved DataFusion column name, please use another name",
                    field.name()
                );
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .map(|_| ())
}
