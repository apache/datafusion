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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use crate::parser::{
    CopyToSource, CopyToStatement, CreateExternalTable, DFParser, ExplainStatement,
    LexOrdering, ResetStatement, Statement as DFStatement,
};
use crate::planner::{
    object_name_to_qualifier, ContextProvider, PlannerContext, SqlToRel,
};
use crate::utils::normalize_ident;

use arrow::datatypes::{Field, FieldRef, Fields};
use datafusion_common::error::_plan_err;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{
    exec_err, internal_err, not_impl_err, plan_datafusion_err, plan_err, schema_err,
    unqualified_field_not_found, Column, Constraint, Constraints, DFSchema, DFSchemaRef,
    DataFusionError, Result, ScalarValue, SchemaError, SchemaReference, TableReference,
    ToDFSchema,
};
use datafusion_expr::dml::{CopyTo, InsertOp};
use datafusion_expr::expr_rewriter::normalize_col_with_schemas_and_ambiguity_check;
use datafusion_expr::logical_plan::builder::project;
use datafusion_expr::logical_plan::DdlStatement;
use datafusion_expr::utils::expr_to_columns;
use datafusion_expr::{
    cast, col, Analyze, CreateCatalog, CreateCatalogSchema,
    CreateExternalTable as PlanCreateExternalTable, CreateFunction, CreateFunctionBody,
    CreateIndex as PlanCreateIndex, CreateMemoryTable, CreateView, Deallocate,
    DescribeTable, DmlStatement, DropCatalogSchema, DropFunction, DropTable, DropView,
    EmptyRelation, Execute, Explain, ExplainFormat, Expr, ExprSchemable, Filter,
    LogicalPlan, LogicalPlanBuilder, OperateFunctionArg, PlanType, Prepare,
    ResetVariable, SetVariable, SortExpr, Statement as PlanStatement, ToStringifiedPlan,
    TransactionAccessMode, TransactionConclusion, TransactionEnd,
    TransactionIsolationLevel, TransactionStart, Volatility, WriteOp,
};
use sqlparser::ast::{
    self, BeginTransactionKind, IndexColumn, IndexType, NullsDistinctOption, OrderByExpr,
    OrderByOptions, Set, ShowStatementIn, ShowStatementOptions, SqliteOnConflict,
    TableObject, UpdateTableFromKind, ValueWithSpan,
};
use sqlparser::ast::{
    Assignment, AssignmentTarget, ColumnDef, CreateIndex, CreateTable,
    CreateTableOptions, Delete, DescribeAlias, Expr as SQLExpr, FromTable, Ident, Insert,
    ObjectName, ObjectType, Query, SchemaName, SetExpr, ShowCreateObject,
    ShowStatementFilter, Statement, TableConstraint, TableFactor, TableWithJoins,
    TransactionMode, UnaryOperator, Value,
};
use sqlparser::parser::ParserError::ParserError;

fn ident_to_string(ident: &Ident) -> String {
    normalize_ident(ident.to_owned())
}

fn object_name_to_string(object_name: &ObjectName) -> String {
    object_name
        .0
        .iter()
        .map(|object_name_part| {
            object_name_part
                .as_ident()
                // TODO: It might be better to return an error
                // than to silently use a default value.
                .map_or_else(String::new, ident_to_string)
        })
        .collect::<Vec<String>>()
        .join(".")
}

fn get_schema_name(schema_name: &SchemaName) -> String {
    match schema_name {
        SchemaName::Simple(schema_name) => object_name_to_string(schema_name),
        SchemaName::UnnamedAuthorization(auth) => ident_to_string(auth),
        SchemaName::NamedAuthorization(schema_name, auth) => format!(
            "{}.{}",
            object_name_to_string(schema_name),
            ident_to_string(auth)
        ),
    }
}

/// Construct `TableConstraint`(s) for the given columns by iterating over
/// `columns` and extracting individual inline constraint definitions.
fn calc_inline_constraints_from_columns(columns: &[ColumnDef]) -> Vec<TableConstraint> {
    let mut constraints = vec![];
    for column in columns {
        for ast::ColumnOptionDef { name, option } in &column.options {
            match option {
                ast::ColumnOption::Unique {
                    is_primary: false,
                    characteristics,
                } => constraints.push(TableConstraint::Unique {
                    name: name.clone(),
                    columns: vec![IndexColumn {
                        column: OrderByExpr {
                            expr: SQLExpr::Identifier(column.name.clone()),
                            options: OrderByOptions {
                                asc: None,
                                nulls_first: None,
                            },
                            with_fill: None,
                        },
                        operator_class: None,
                    }],
                    characteristics: *characteristics,
                    index_name: None,
                    index_type_display: ast::KeyOrIndexDisplay::None,
                    index_type: None,
                    index_options: vec![],
                    nulls_distinct: NullsDistinctOption::None,
                }),
                ast::ColumnOption::Unique {
                    is_primary: true,
                    characteristics,
                } => constraints.push(TableConstraint::PrimaryKey {
                    name: name.clone(),
                    columns: vec![IndexColumn {
                        column: OrderByExpr {
                            expr: SQLExpr::Identifier(column.name.clone()),
                            options: OrderByOptions {
                                asc: None,
                                nulls_first: None,
                            },
                            with_fill: None,
                        },
                        operator_class: None,
                    }],
                    characteristics: *characteristics,
                    index_name: None,
                    index_type: None,
                    index_options: vec![],
                }),
                ast::ColumnOption::ForeignKey {
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                    characteristics,
                } => constraints.push(TableConstraint::ForeignKey {
                    name: name.clone(),
                    columns: vec![],
                    foreign_table: foreign_table.clone(),
                    referred_columns: referred_columns.to_vec(),
                    on_delete: *on_delete,
                    on_update: *on_update,
                    characteristics: *characteristics,
                    index_name: None,
                }),
                ast::ColumnOption::Check(expr) => {
                    constraints.push(TableConstraint::Check {
                        name: name.clone(),
                        expr: Box::new(expr.clone()),
                        enforced: None,
                    })
                }
                // Other options are not constraint related.
                ast::ColumnOption::Default(_)
                | ast::ColumnOption::Null
                | ast::ColumnOption::NotNull
                | ast::ColumnOption::DialectSpecific(_)
                | ast::ColumnOption::CharacterSet(_)
                | ast::ColumnOption::Generated { .. }
                | ast::ColumnOption::Comment(_)
                | ast::ColumnOption::Options(_)
                | ast::ColumnOption::OnUpdate(_)
                | ast::ColumnOption::Materialized(_)
                | ast::ColumnOption::Ephemeral(_)
                | ast::ColumnOption::Identity(_)
                | ast::ColumnOption::OnConflict(_)
                | ast::ColumnOption::Policy(_)
                | ast::ColumnOption::Tags(_)
                | ast::ColumnOption::Alias(_)
                | ast::ColumnOption::Srid(_)
                | ast::ColumnOption::Collation(_) => {}
            }
        }
    }
    constraints
}

impl<S: ContextProvider> SqlToRel<'_, S> {
    /// Generate a logical plan from an DataFusion SQL statement
    pub fn statement_to_plan(&self, statement: DFStatement) -> Result<LogicalPlan> {
        match statement {
            DFStatement::CreateExternalTable(s) => self.external_table_to_plan(s),
            DFStatement::Statement(s) => self.sql_statement_to_plan(*s),
            DFStatement::CopyTo(s) => self.copy_to_plan(s),
            DFStatement::Explain(ExplainStatement {
                verbose,
                analyze,
                format,
                statement,
            }) => self.explain_to_plan(verbose, analyze, format, *statement),
            DFStatement::Reset(statement) => self.reset_statement_to_plan(statement),
        }
    }

    /// Generate a logical plan from an SQL statement
    pub fn sql_statement_to_plan(&self, statement: Statement) -> Result<LogicalPlan> {
        self.sql_statement_to_plan_with_context_impl(
            statement,
            &mut PlannerContext::new(),
        )
    }

    /// Generate a logical plan from an SQL statement
    pub fn sql_statement_to_plan_with_context(
        &self,
        statement: Statement,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        self.sql_statement_to_plan_with_context_impl(statement, planner_context)
    }

    fn sql_statement_to_plan_with_context_impl(
        &self,
        statement: Statement,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        match statement {
            Statement::ExplainTable {
                describe_alias: DescribeAlias::Describe | DescribeAlias::Desc, // only parse 'DESCRIBE table_name' or 'DESC table_name' and not 'EXPLAIN table_name'
                table_name,
                ..
            } => self.describe_table_to_plan(table_name),
            Statement::Explain {
                describe_alias: DescribeAlias::Describe | DescribeAlias::Desc, // only parse 'DESCRIBE statement' or 'DESC statement' and not 'EXPLAIN statement'
                statement,
                ..
            } => match *statement {
                Statement::Query(query) => self.describe_query_to_plan(*query),
                _ => {
                    not_impl_err!("Describing statements other than SELECT not supported")
                }
            },
            Statement::Explain {
                verbose,
                statement,
                analyze,
                format,
                describe_alias: _,
                ..
            } => {
                let format = format.map(|format| format.to_string());
                let statement = DFStatement::Statement(statement);
                self.explain_to_plan(verbose, analyze, format, statement)
            }
            Statement::Query(query) => self.query_to_plan(*query, planner_context),
            Statement::ShowVariable { variable } => self.show_variable_to_plan(&variable),
            Statement::Set(statement) => self.set_statement_to_plan(statement),
            Statement::CreateTable(CreateTable {
                temporary,
                external,
                global,
                transient,
                volatile,
                hive_distribution,
                hive_formats,
                file_format,
                location,
                query,
                name,
                columns,
                constraints,
                if_not_exists,
                or_replace,
                without_rowid,
                like,
                clone,
                comment,
                on_commit,
                on_cluster,
                primary_key,
                order_by,
                partition_by,
                cluster_by,
                clustered_by,
                strict,
                copy_grants,
                enable_schema_evolution,
                change_tracking,
                data_retention_time_in_days,
                max_data_extension_time_in_days,
                default_ddl_collation,
                with_aggregation_policy,
                with_row_access_policy,
                with_tags,
                iceberg,
                external_volume,
                base_location,
                catalog,
                catalog_sync,
                storage_serialization_policy,
                inherits,
                table_options: CreateTableOptions::None,
                dynamic,
                version,
                target_lag,
                warehouse,
                refresh_mode,
                initialize,
                require_user,
            }) => {
                if temporary {
                    return not_impl_err!("Temporary tables not supported")?;
                }
                if external {
                    return not_impl_err!("External tables not supported")?;
                }
                if global.is_some() {
                    return not_impl_err!("Global tables not supported")?;
                }
                if transient {
                    return not_impl_err!("Transient tables not supported")?;
                }
                if volatile {
                    return not_impl_err!("Volatile tables not supported")?;
                }
                if hive_distribution != ast::HiveDistributionStyle::NONE {
                    return not_impl_err!(
                        "Hive distribution not supported: {hive_distribution:?}"
                    )?;
                }
                if !matches!(
                    hive_formats,
                    Some(ast::HiveFormat {
                        row_format: None,
                        serde_properties: None,
                        storage: None,
                        location: None,
                    })
                ) {
                    return not_impl_err!(
                        "Hive formats not supported: {hive_formats:?}"
                    )?;
                }
                if file_format.is_some() {
                    return not_impl_err!("File format not supported")?;
                }
                if location.is_some() {
                    return not_impl_err!("Location not supported")?;
                }
                if without_rowid {
                    return not_impl_err!("Without rowid not supported")?;
                }
                if like.is_some() {
                    return not_impl_err!("Like not supported")?;
                }
                if clone.is_some() {
                    return not_impl_err!("Clone not supported")?;
                }
                if comment.is_some() {
                    return not_impl_err!("Comment not supported")?;
                }
                if on_commit.is_some() {
                    return not_impl_err!("On commit not supported")?;
                }
                if on_cluster.is_some() {
                    return not_impl_err!("On cluster not supported")?;
                }
                if primary_key.is_some() {
                    return not_impl_err!("Primary key not supported")?;
                }
                if order_by.is_some() {
                    return not_impl_err!("Order by not supported")?;
                }
                if partition_by.is_some() {
                    return not_impl_err!("Partition by not supported")?;
                }
                if cluster_by.is_some() {
                    return not_impl_err!("Cluster by not supported")?;
                }
                if clustered_by.is_some() {
                    return not_impl_err!("Clustered by not supported")?;
                }
                if strict {
                    return not_impl_err!("Strict not supported")?;
                }
                if copy_grants {
                    return not_impl_err!("Copy grants not supported")?;
                }
                if enable_schema_evolution.is_some() {
                    return not_impl_err!("Enable schema evolution not supported")?;
                }
                if change_tracking.is_some() {
                    return not_impl_err!("Change tracking not supported")?;
                }
                if data_retention_time_in_days.is_some() {
                    return not_impl_err!("Data retention time in days not supported")?;
                }
                if max_data_extension_time_in_days.is_some() {
                    return not_impl_err!(
                        "Max data extension time in days not supported"
                    )?;
                }
                if default_ddl_collation.is_some() {
                    return not_impl_err!("Default DDL collation not supported")?;
                }
                if with_aggregation_policy.is_some() {
                    return not_impl_err!("With aggregation policy not supported")?;
                }
                if with_row_access_policy.is_some() {
                    return not_impl_err!("With row access policy not supported")?;
                }
                if with_tags.is_some() {
                    return not_impl_err!("With tags not supported")?;
                }
                if iceberg {
                    return not_impl_err!("Iceberg not supported")?;
                }
                if external_volume.is_some() {
                    return not_impl_err!("External volume not supported")?;
                }
                if base_location.is_some() {
                    return not_impl_err!("Base location not supported")?;
                }
                if catalog.is_some() {
                    return not_impl_err!("Catalog not supported")?;
                }
                if catalog_sync.is_some() {
                    return not_impl_err!("Catalog sync not supported")?;
                }
                if storage_serialization_policy.is_some() {
                    return not_impl_err!("Storage serialization policy not supported")?;
                }
                if inherits.is_some() {
                    return not_impl_err!("Table inheritance not supported")?;
                }
                if dynamic {
                    return not_impl_err!("Dynamic tables not supported")?;
                }
                if version.is_some() {
                    return not_impl_err!("Version not supported")?;
                }
                if target_lag.is_some() {
                    return not_impl_err!("Target lag not supported")?;
                }
                if warehouse.is_some() {
                    return not_impl_err!("Warehouse not supported")?;
                }
                if refresh_mode.is_some() {
                    return not_impl_err!("Refresh mode not supported")?;
                }
                if initialize.is_some() {
                    return not_impl_err!("Initialize not supported")?;
                }
                if require_user {
                    return not_impl_err!("Require user not supported")?;
                }
                // Merge inline constraints and existing constraints
                let mut all_constraints = constraints;
                let inline_constraints = calc_inline_constraints_from_columns(&columns);
                all_constraints.extend(inline_constraints);
                // Build column default values
                let column_defaults =
                    self.build_column_defaults(&columns, planner_context)?;

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
                            let input_fields = input_schema.fields();
                            let project_exprs = schema
                                .fields()
                                .iter()
                                .zip(input_fields)
                                .map(|(field, input_field)| {
                                    cast(
                                        col(input_field.name()),
                                        field.data_type().clone(),
                                    )
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
            Statement::CreateView {
                or_replace,
                materialized,
                name,
                columns,
                query,
                options: CreateTableOptions::None,
                cluster_by,
                comment,
                with_no_schema_binding,
                if_not_exists,
                temporary,
                to,
                params,
                or_alter,
                secure,
                name_before_not_exists,
            } => {
                if materialized {
                    return not_impl_err!("Materialized views not supported")?;
                }
                if !cluster_by.is_empty() {
                    return not_impl_err!("Cluster by not supported")?;
                }
                if comment.is_some() {
                    return not_impl_err!("Comment not supported")?;
                }
                if with_no_schema_binding {
                    return not_impl_err!("With no schema binding not supported")?;
                }
                if if_not_exists {
                    return not_impl_err!("If not exists not supported")?;
                }
                if to.is_some() {
                    return not_impl_err!("To not supported")?;
                }

                // put the statement back together temporarily to get the SQL
                // string representation
                let stmt = Statement::CreateView {
                    or_replace,
                    materialized,
                    name,
                    columns,
                    query,
                    options: CreateTableOptions::None,
                    cluster_by,
                    comment,
                    with_no_schema_binding,
                    if_not_exists,
                    temporary,
                    to,
                    params,
                    or_alter,
                    secure,
                    name_before_not_exists,
                };
                let sql = stmt.to_string();
                let Statement::CreateView {
                    name,
                    columns,
                    query,
                    or_replace,
                    temporary,
                    ..
                } = stmt
                else {
                    return internal_err!("Unreachable code in create view");
                };

                let columns = columns
                    .into_iter()
                    .map(|view_column_def| {
                        if let Some(options) = view_column_def.options {
                            plan_err!(
                                "Options not supported for view columns: {options:?}"
                            )
                        } else {
                            Ok(view_column_def.name)
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;

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
            Statement::ShowCreate { obj_type, obj_name } => match obj_type {
                ShowCreateObject::Table => self.show_create_table_to_plan(obj_name),
                _ => {
                    not_impl_err!("Only `SHOW CREATE TABLE  ...` statement is supported")
                }
            },
            Statement::CreateSchema {
                schema_name,
                if_not_exists,
                ..
            } => Ok(LogicalPlan::Ddl(DdlStatement::CreateCatalogSchema(
                CreateCatalogSchema {
                    schema_name: get_schema_name(&schema_name),
                    if_not_exists,
                    schema: Arc::new(DFSchema::empty()),
                },
            ))),
            Statement::CreateDatabase {
                db_name,
                if_not_exists,
                ..
            } => Ok(LogicalPlan::Ddl(DdlStatement::CreateCatalog(
                CreateCatalog {
                    catalog_name: object_name_to_string(&db_name),
                    if_not_exists,
                    schema: Arc::new(DFSchema::empty()),
                },
            ))),
            Statement::Drop {
                object_type,
                if_exists,
                mut names,
                cascade,
                restrict: _,
                purge: _,
                temporary: _,
                table: _,
            } => {
                // We don't support cascade and purge for now.
                // nor do we support multiple object names
                let name = match names.len() {
                    0 => Err(ParserError("Missing table name.".to_string()).into()),
                    1 => self.object_name_to_table_reference(names.pop().unwrap()),
                    _ => {
                        Err(ParserError("Multiple objects not supported".to_string())
                            .into())
                    }
                }?;

                match object_type {
                    ObjectType::Table => {
                        Ok(LogicalPlan::Ddl(DdlStatement::DropTable(DropTable {
                            name,
                            if_exists,
                            schema: DFSchemaRef::new(DFSchema::empty()),
                        })))
                    }
                    ObjectType::View => {
                        Ok(LogicalPlan::Ddl(DdlStatement::DropView(DropView {
                            name,
                            if_exists,
                            schema: DFSchemaRef::new(DFSchema::empty()),
                        })))
                    }
                    ObjectType::Schema => {
                        let name = match name {
                            TableReference::Bare { table } => Ok(SchemaReference::Bare { schema: table }),
                            TableReference::Partial { schema, table } => Ok(SchemaReference::Full { schema: table, catalog: schema }),
                            TableReference::Full { catalog: _, schema: _, table: _ } => {
                                Err(ParserError("Invalid schema specifier (has 3 parts)".to_string()))
                            }
                        }?;
                        Ok(LogicalPlan::Ddl(DdlStatement::DropCatalogSchema(DropCatalogSchema {
                            name,
                            if_exists,
                            cascade,
                            schema: DFSchemaRef::new(DFSchema::empty()),
                        })))
                    }
                    _ => not_impl_err!(
                        "Only `DROP TABLE/VIEW/SCHEMA  ...` statement is supported currently"
                    ),
                }
            }
            Statement::Prepare {
                name,
                data_types,
                statement,
            } => {
                // Convert parser data types to DataFusion data types
                let mut fields: Vec<FieldRef> = data_types
                    .into_iter()
                    .map(|t| self.convert_data_type_to_field(&t))
                    .collect::<Result<_>>()?;

                // Create planner context with parameters
                let mut planner_context =
                    PlannerContext::new().with_prepare_param_data_types(fields.clone());

                // Build logical plan for inner statement of the prepare statement
                let plan = self.sql_statement_to_plan_with_context_impl(
                    *statement,
                    &mut planner_context,
                )?;

                if fields.is_empty() {
                    let map_types = plan.get_parameter_fields()?;
                    let param_types: Vec<_> = (1..=map_types.len())
                        .filter_map(|i| {
                            let key = format!("${i}");
                            map_types.get(&key).and_then(|opt| opt.clone())
                        })
                        .collect();
                    fields.extend(param_types.iter().cloned());
                    planner_context.with_prepare_param_data_types(param_types);
                }

                Ok(LogicalPlan::Statement(PlanStatement::Prepare(Prepare {
                    name: ident_to_string(&name),
                    fields,
                    input: Arc::new(plan),
                })))
            }
            Statement::Execute {
                name,
                parameters,
                using,
                // has_parentheses specifies the syntax, but the plan is the
                // same no matter the syntax used, so ignore it
                has_parentheses: _,
                immediate,
                into,
                output,
                default,
            } => {
                // `USING` is a MySQL-specific syntax and currently not supported.
                if !using.is_empty() {
                    return not_impl_err!(
                        "Execute statement with USING is not supported"
                    );
                }
                if immediate {
                    return not_impl_err!(
                        "Execute statement with IMMEDIATE is not supported"
                    );
                }
                if !into.is_empty() {
                    return not_impl_err!("Execute statement with INTO is not supported");
                }
                if output {
                    return not_impl_err!(
                        "Execute statement with OUTPUT is not supported"
                    );
                }
                if default {
                    return not_impl_err!(
                        "Execute statement with DEFAULT is not supported"
                    );
                }
                let empty_schema = DFSchema::empty();
                let parameters = parameters
                    .into_iter()
                    .map(|expr| self.sql_to_expr(expr, &empty_schema, planner_context))
                    .collect::<Result<Vec<Expr>>>()?;

                Ok(LogicalPlan::Statement(PlanStatement::Execute(Execute {
                    name: object_name_to_string(&name.unwrap()),
                    parameters,
                })))
            }
            Statement::Deallocate {
                name,
                // Similar to PostgreSQL, the PREPARE keyword is ignored
                prepare: _,
            } => Ok(LogicalPlan::Statement(PlanStatement::Deallocate(
                Deallocate {
                    name: ident_to_string(&name),
                },
            ))),

            Statement::ShowTables {
                extended,
                full,
                terse,
                history,
                external,
                show_options,
            } => {
                // We only support the basic "SHOW TABLES"
                // https://github.com/apache/datafusion/issues/3188
                if extended {
                    return not_impl_err!("SHOW TABLES EXTENDED not supported")?;
                }
                if full {
                    return not_impl_err!("SHOW FULL TABLES not supported")?;
                }
                if terse {
                    return not_impl_err!("SHOW TERSE TABLES not supported")?;
                }
                if history {
                    return not_impl_err!("SHOW TABLES HISTORY not supported")?;
                }
                if external {
                    return not_impl_err!("SHOW EXTERNAL TABLES not supported")?;
                }
                let ShowStatementOptions {
                    show_in,
                    starts_with,
                    limit,
                    limit_from,
                    filter_position,
                } = show_options;
                if show_in.is_some() {
                    return not_impl_err!("SHOW TABLES IN not supported")?;
                }
                if starts_with.is_some() {
                    return not_impl_err!("SHOW TABLES LIKE not supported")?;
                }
                if limit.is_some() {
                    return not_impl_err!("SHOW TABLES LIMIT not supported")?;
                }
                if limit_from.is_some() {
                    return not_impl_err!("SHOW TABLES LIMIT FROM not supported")?;
                }
                if filter_position.is_some() {
                    return not_impl_err!("SHOW TABLES FILTER not supported")?;
                }
                self.show_tables_to_plan()
            }

            Statement::ShowColumns {
                extended,
                full,
                show_options,
            } => {
                let ShowStatementOptions {
                    show_in,
                    starts_with,
                    limit,
                    limit_from,
                    filter_position,
                } = show_options;
                if starts_with.is_some() {
                    return not_impl_err!("SHOW COLUMNS LIKE not supported")?;
                }
                if limit.is_some() {
                    return not_impl_err!("SHOW COLUMNS LIMIT not supported")?;
                }
                if limit_from.is_some() {
                    return not_impl_err!("SHOW COLUMNS LIMIT FROM not supported")?;
                }
                if filter_position.is_some() {
                    return not_impl_err!(
                        "SHOW COLUMNS with WHERE or LIKE is not supported"
                    )?;
                }
                let Some(ShowStatementIn {
                    // specifies if the syntax was `SHOW COLUMNS IN` or `SHOW
                    // COLUMNS FROM` which is not different in DataFusion
                    clause: _,
                    parent_type,
                    parent_name,
                }) = show_in
                else {
                    return plan_err!("SHOW COLUMNS requires a table name");
                };

                if let Some(parent_type) = parent_type {
                    return not_impl_err!("SHOW COLUMNS IN {parent_type} not supported");
                }
                let Some(table_name) = parent_name else {
                    return plan_err!("SHOW COLUMNS requires a table name");
                };

                self.show_columns_to_plan(extended, full, table_name)
            }

            Statement::ShowFunctions { filter, .. } => {
                self.show_functions_to_plan(filter)
            }

            Statement::Insert(Insert {
                or,
                into,
                columns,
                overwrite,
                source,
                partitioned,
                after_columns,
                table,
                on,
                returning,
                ignore,
                table_alias,
                mut replace_into,
                priority,
                insert_alias,
                assignments,
                has_table_keyword,
                settings,
                format_clause,
            }) => {
                let table_name = match table {
                    TableObject::TableName(table_name) => table_name,
                    TableObject::TableFunction(_) => {
                        return not_impl_err!("INSERT INTO Table functions not supported")
                    }
                };
                if let Some(or) = or {
                    match or {
                        SqliteOnConflict::Replace => replace_into = true,
                        _ => plan_err!("Inserts with {or} clause is not supported")?,
                    }
                }
                if partitioned.is_some() {
                    plan_err!("Partitioned inserts not yet supported")?;
                }
                if !after_columns.is_empty() {
                    plan_err!("After-columns clause not supported")?;
                }
                if on.is_some() {
                    plan_err!("Insert-on clause not supported")?;
                }
                if returning.is_some() {
                    plan_err!("Insert-returning clause not supported")?;
                }
                if ignore {
                    plan_err!("Insert-ignore clause not supported")?;
                }
                let Some(source) = source else {
                    plan_err!("Inserts without a source not supported")?
                };
                if let Some(table_alias) = table_alias {
                    plan_err!(
                        "Inserts with a table alias not supported: {table_alias:?}"
                    )?
                };
                if let Some(priority) = priority {
                    plan_err!(
                        "Inserts with a `PRIORITY` clause not supported: {priority:?}"
                    )?
                };
                if insert_alias.is_some() {
                    plan_err!("Inserts with an alias not supported")?;
                }
                if !assignments.is_empty() {
                    plan_err!("Inserts with assignments not supported")?;
                }
                if settings.is_some() {
                    plan_err!("Inserts with settings not supported")?;
                }
                if format_clause.is_some() {
                    plan_err!("Inserts with format clause not supported")?;
                }
                // optional keywords don't change behavior
                let _ = into;
                let _ = has_table_keyword;
                self.insert_to_plan(table_name, columns, source, overwrite, replace_into)
            }
            Statement::Update {
                table,
                assignments,
                from,
                selection,
                returning,
                or,
                limit,
            } => {
                let from_clauses =
                    from.map(|update_table_from_kind| match update_table_from_kind {
                        UpdateTableFromKind::BeforeSet(from_clauses) => from_clauses,
                        UpdateTableFromKind::AfterSet(from_clauses) => from_clauses,
                    });
                // TODO: support multiple tables in UPDATE SET FROM
                if from_clauses.as_ref().is_some_and(|f| f.len() > 1) {
                    plan_err!("Multiple tables in UPDATE SET FROM not yet supported")?;
                }
                let update_from = from_clauses.and_then(|mut f| f.pop());
                if returning.is_some() {
                    plan_err!("Update-returning clause not yet supported")?;
                }
                if or.is_some() {
                    plan_err!("ON conflict not supported")?;
                }
                if limit.is_some() {
                    return not_impl_err!("Update-limit clause not supported")?;
                }
                self.update_to_plan(table, &assignments, update_from, selection)
            }

            Statement::Delete(Delete {
                tables,
                using,
                selection,
                returning,
                from,
                order_by,
                limit,
            }) => {
                if !tables.is_empty() {
                    plan_err!("DELETE <TABLE> not supported")?;
                }

                if using.is_some() {
                    plan_err!("Using clause not supported")?;
                }

                if returning.is_some() {
                    plan_err!("Delete-returning clause not yet supported")?;
                }

                if !order_by.is_empty() {
                    plan_err!("Delete-order-by clause not yet supported")?;
                }

                if limit.is_some() {
                    plan_err!("Delete-limit clause not yet supported")?;
                }

                let table_name = self.get_delete_target(from)?;
                self.delete_to_plan(&table_name, selection)
            }

            Statement::StartTransaction {
                modes,
                begin: false,
                modifier,
                transaction,
                statements,
                has_end_keyword,
                exception,
            } => {
                if let Some(modifier) = modifier {
                    return not_impl_err!(
                        "Transaction modifier not supported: {modifier}"
                    );
                }
                if !statements.is_empty() {
                    return not_impl_err!(
                        "Transaction with multiple statements not supported"
                    );
                }
                if exception.is_some() {
                    return not_impl_err!(
                        "Transaction with exception statements not supported"
                    );
                }
                if has_end_keyword {
                    return not_impl_err!("Transaction with END keyword not supported");
                }
                self.validate_transaction_kind(transaction.as_ref())?;
                let isolation_level: ast::TransactionIsolationLevel = modes
                    .iter()
                    .filter_map(|m: &TransactionMode| match m {
                        TransactionMode::AccessMode(_) => None,
                        TransactionMode::IsolationLevel(level) => Some(level),
                    })
                    .next_back()
                    .copied()
                    .unwrap_or(ast::TransactionIsolationLevel::Serializable);
                let access_mode: ast::TransactionAccessMode = modes
                    .iter()
                    .filter_map(|m: &TransactionMode| match m {
                        TransactionMode::AccessMode(mode) => Some(mode),
                        TransactionMode::IsolationLevel(_) => None,
                    })
                    .next_back()
                    .copied()
                    .unwrap_or(ast::TransactionAccessMode::ReadWrite);
                let isolation_level = match isolation_level {
                    ast::TransactionIsolationLevel::ReadUncommitted => {
                        TransactionIsolationLevel::ReadUncommitted
                    }
                    ast::TransactionIsolationLevel::ReadCommitted => {
                        TransactionIsolationLevel::ReadCommitted
                    }
                    ast::TransactionIsolationLevel::RepeatableRead => {
                        TransactionIsolationLevel::RepeatableRead
                    }
                    ast::TransactionIsolationLevel::Serializable => {
                        TransactionIsolationLevel::Serializable
                    }
                    ast::TransactionIsolationLevel::Snapshot => {
                        TransactionIsolationLevel::Snapshot
                    }
                };
                let access_mode = match access_mode {
                    ast::TransactionAccessMode::ReadOnly => {
                        TransactionAccessMode::ReadOnly
                    }
                    ast::TransactionAccessMode::ReadWrite => {
                        TransactionAccessMode::ReadWrite
                    }
                };
                let statement = PlanStatement::TransactionStart(TransactionStart {
                    access_mode,
                    isolation_level,
                });
                Ok(LogicalPlan::Statement(statement))
            }
            Statement::Commit {
                chain,
                end,
                modifier,
            } => {
                if end {
                    return not_impl_err!("COMMIT AND END not supported");
                };
                if let Some(modifier) = modifier {
                    return not_impl_err!("COMMIT {modifier} not supported");
                };
                let statement = PlanStatement::TransactionEnd(TransactionEnd {
                    conclusion: TransactionConclusion::Commit,
                    chain,
                });
                Ok(LogicalPlan::Statement(statement))
            }
            Statement::Rollback { chain, savepoint } => {
                if savepoint.is_some() {
                    plan_err!("Savepoints not supported")?;
                }
                let statement = PlanStatement::TransactionEnd(TransactionEnd {
                    conclusion: TransactionConclusion::Rollback,
                    chain,
                });
                Ok(LogicalPlan::Statement(statement))
            }
            Statement::CreateFunction(ast::CreateFunction {
                or_replace,
                temporary,
                name,
                args,
                return_type,
                function_body,
                behavior,
                language,
                ..
            }) => {
                let return_type = match return_type {
                    Some(t) => Some(self.convert_data_type_to_field(&t)?),
                    None => None,
                };
                let mut planner_context = PlannerContext::new();
                let empty_schema = &DFSchema::empty();

                let args = match args {
                    Some(function_args) => {
                        let function_args = function_args
                            .into_iter()
                            .map(|arg| {
                                let data_type =
                                    self.convert_data_type_to_field(&arg.data_type)?;

                                let default_expr = match arg.default_expr {
                                    Some(expr) => Some(self.sql_to_expr(
                                        expr,
                                        empty_schema,
                                        &mut planner_context,
                                    )?),
                                    None => None,
                                };
                                Ok(OperateFunctionArg {
                                    name: arg.name,
                                    default_expr,
                                    data_type: data_type.data_type().clone(),
                                })
                            })
                            .collect::<Result<Vec<OperateFunctionArg>>>();
                        Some(function_args?)
                    }
                    None => None,
                };
                // Validate default arguments
                let first_default = match args.as_ref() {
                    Some(arg) => arg.iter().position(|t| t.default_expr.is_some()),
                    None => None,
                };
                let last_non_default = match args.as_ref() {
                    Some(arg) => arg
                        .iter()
                        .rev()
                        .position(|t| t.default_expr.is_none())
                        .map(|reverse_pos| arg.len() - reverse_pos - 1),
                    None => None,
                };
                if let (Some(pos_default), Some(pos_non_default)) =
                    (first_default, last_non_default)
                {
                    if pos_non_default > pos_default {
                        return plan_err!(
                            "Non-default arguments cannot follow default arguments."
                        );
                    }
                }
                // At the moment functions can't be qualified `schema.name`
                let name = match &name.0[..] {
                    [] => exec_err!("Function should have name")?,
                    [n] => n.as_ident().unwrap().value.clone(),
                    [..] => not_impl_err!("Qualified functions are not supported")?,
                };
                //
                // Convert resulting expression to data fusion expression
                //
                let arg_types = args.as_ref().map(|arg| {
                    arg.iter()
                        .map(|t| {
                            let name = match t.name.clone() {
                                Some(name) => name.value,
                                None => "".to_string(),
                            };
                            Arc::new(Field::new(name, t.data_type.clone(), true))
                        })
                        .collect::<Vec<_>>()
                });
                // Validate parameter style
                if let Some(ref fields) = arg_types {
                    let count_positional =
                        fields.iter().filter(|f| f.name() == "").count();
                    if !(count_positional == 0 || count_positional == fields.len()) {
                        return plan_err!("All function arguments must use either named or positional style.");
                    }
                }
                let mut planner_context = PlannerContext::new()
                    .with_prepare_param_data_types(arg_types.unwrap_or_default());

                let function_body = match function_body {
                    Some(r) => Some(self.sql_to_expr(
                        match r {
                            ast::CreateFunctionBody::AsBeforeOptions(expr) => expr,
                            ast::CreateFunctionBody::AsAfterOptions(expr) => expr,
                            ast::CreateFunctionBody::Return(expr) => expr,
                            ast::CreateFunctionBody::AsBeginEnd(_) => {
                                return not_impl_err!(
                                    "BEGIN/END enclosed function body syntax is not supported"
                                )?;
                            }
                            ast::CreateFunctionBody::AsReturnExpr(_)
                            | ast::CreateFunctionBody::AsReturnSelect(_) => {
                                return not_impl_err!(
                                    "AS RETURN function syntax is not supported"
                                )?
                            }
                        },
                        &DFSchema::empty(),
                        &mut planner_context,
                    )?),
                    None => None,
                };

                let params = CreateFunctionBody {
                    language,
                    behavior: behavior.map(|b| match b {
                        ast::FunctionBehavior::Immutable => Volatility::Immutable,
                        ast::FunctionBehavior::Stable => Volatility::Stable,
                        ast::FunctionBehavior::Volatile => Volatility::Volatile,
                    }),
                    function_body,
                };

                let statement = DdlStatement::CreateFunction(CreateFunction {
                    or_replace,
                    temporary,
                    name,
                    return_type: return_type.map(|f| f.data_type().clone()),
                    args,
                    params,
                    schema: DFSchemaRef::new(DFSchema::empty()),
                });

                Ok(LogicalPlan::Ddl(statement))
            }
            Statement::DropFunction {
                if_exists,
                func_desc,
                ..
            } => {
                // According to postgresql documentation it can be only one function
                // specified in drop statement
                if let Some(desc) = func_desc.first() {
                    // At the moment functions can't be qualified `schema.name`
                    let name = match &desc.name.0[..] {
                        [] => exec_err!("Function should have name")?,
                        [n] => n.as_ident().unwrap().value.clone(),
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
            Statement::CreateIndex(CreateIndex {
                name,
                table_name,
                using,
                columns,
                unique,
                if_not_exists,
                ..
            }) => {
                let name: Option<String> = name.as_ref().map(object_name_to_string);
                let table = self.object_name_to_table_reference(table_name)?;
                let table_schema = self
                    .context_provider
                    .get_table_source(table.clone())?
                    .schema()
                    .to_dfschema_ref()?;
                let using: Option<String> =
                    using.as_ref().map(|index_type| match index_type {
                        IndexType::Custom(ident) => ident_to_string(ident),
                        _ => index_type.to_string().to_ascii_lowercase(),
                    });
                let order_by_exprs: Vec<OrderByExpr> =
                    columns.into_iter().map(|col| col.column).collect();
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
            stmt => {
                not_impl_err!("Unsupported SQL statement: {stmt}")
            }
        }
    }

    fn get_delete_target(&self, from: FromTable) -> Result<ObjectName> {
        let mut from = match from {
            FromTable::WithFromKeyword(v) => v,
            FromTable::WithoutKeyword(v) => v,
        };

        if from.len() != 1 {
            return not_impl_err!(
                "DELETE FROM only supports single table, got {}: {from:?}",
                from.len()
            );
        }
        let table_factor = from.pop().unwrap();
        if !table_factor.joins.is_empty() {
            return not_impl_err!("DELETE FROM only supports single table, got: joins");
        }
        let TableFactor::Table { name, .. } = table_factor.relation else {
            return not_impl_err!(
                "DELETE FROM only supports single table, got: {table_factor:?}"
            );
        };

        Ok(name)
    }

    /// Generate a logical plan from a "SHOW TABLES" query
    fn show_tables_to_plan(&self) -> Result<LogicalPlan> {
        if self.has_table("information_schema", "tables") {
            let query = "SELECT * FROM information_schema.tables;";
            let mut rewrite = DFParser::parse_sql(query)?;
            assert_eq!(rewrite.len(), 1);
            self.statement_to_plan(rewrite.pop_front().unwrap()) // length of rewrite is 1
        } else {
            plan_err!("SHOW TABLES is not supported unless information_schema is enabled")
        }
    }

    fn describe_table_to_plan(&self, table_name: ObjectName) -> Result<LogicalPlan> {
        let table_ref = self.object_name_to_table_reference(table_name)?;

        let table_source = self.context_provider.get_table_source(table_ref)?;

        let schema = table_source.schema();

        let output_schema = DFSchema::try_from(LogicalPlan::describe_schema()).unwrap();

        Ok(LogicalPlan::DescribeTable(DescribeTable {
            schema,
            output_schema: Arc::new(output_schema),
        }))
    }

    fn describe_query_to_plan(&self, query: Query) -> Result<LogicalPlan> {
        let plan = self.query_to_plan(query, &mut PlannerContext::new())?;

        let schema = Arc::new(plan.schema().as_arrow().clone());

        let output_schema = DFSchema::try_from(LogicalPlan::describe_schema()).unwrap();

        Ok(LogicalPlan::DescribeTable(DescribeTable {
            schema,
            output_schema: Arc::new(output_schema),
        }))
    }

    fn copy_to_plan(&self, statement: CopyToStatement) -> Result<LogicalPlan> {
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
        order_exprs: Vec<LexOrdering>,
        schema: &DFSchemaRef,
        planner_context: &mut PlannerContext,
    ) -> Result<Vec<Vec<SortExpr>>> {
        if !order_exprs.is_empty() && schema.fields().is_empty() {
            let results = order_exprs
                .iter()
                .map(|lex_order| {
                    let result = lex_order
                        .iter()
                        .map(|order_by_expr| {
                            let ordered_expr = &order_by_expr.expr;
                            let ordered_expr = ordered_expr.to_owned();
                            let ordered_expr = self.sql_expr_to_logical_expr(
                                ordered_expr,
                                schema,
                                planner_context,
                            )?;
                            let asc = order_by_expr.options.asc.unwrap_or(true);
                            let nulls_first =
                                order_by_expr.options.nulls_first.unwrap_or_else(|| {
                                    self.options.default_null_ordering.nulls_first(asc)
                                });

                            Ok(SortExpr::new(ordered_expr, asc, nulls_first))
                        })
                        .collect::<Result<Vec<SortExpr>>>()?;
                    Ok(result)
                })
                .collect::<Result<Vec<Vec<SortExpr>>>>()?;

            return Ok(results);
        }

        let mut all_results = vec![];
        for expr in order_exprs {
            // Convert each OrderByExpr to a SortExpr:
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
        statement: CreateExternalTable,
    ) -> Result<LogicalPlan> {
        let definition = Some(statement.to_string());
        let CreateExternalTable {
            name,
            columns,
            file_type,
            location,
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
        Ok(LogicalPlan::Ddl(DdlStatement::CreateExternalTable(
            PlanCreateExternalTable {
                schema: df_schema,
                name,
                location,
                file_type,
                table_partition_cols,
                if_not_exists,
                or_replace,
                temporary,
                definition,
                order_exprs: ordered_exprs,
                unbounded,
                options: options_map,
                constraints,
                column_defaults,
            },
        )))
    }

    /// Get the indices of the constraint columns in the schema.
    /// If any column is not found, return an error.
    fn get_constraint_column_indices(
        &self,
        df_schema: &DFSchemaRef,
        columns: &[IndexColumn],
        constraint_name: &str,
    ) -> Result<Vec<usize>> {
        let field_names = df_schema.field_names();
        columns
            .iter()
            .map(|index_column| {
                let expr = &index_column.column.expr;
                let ident = if let SQLExpr::Identifier(ident) = expr {
                    ident
                } else {
                    return Err(plan_datafusion_err!(
                        "Column name for {constraint_name} must be an identifier: {expr}"
                    ));
                };
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
        constraints: &[TableConstraint],
        df_schema: &DFSchemaRef,
    ) -> Result<Constraints> {
        let constraints = constraints
            .iter()
            .map(|c: &TableConstraint| match c {
                TableConstraint::Unique { name, columns, .. } => {
                    let constraint_name = match name {
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
                TableConstraint::PrimaryKey { columns, .. } => {
                    // Get primary key indices in the schema
                    let indices = self.get_constraint_column_indices(
                        df_schema,
                        columns,
                        "primary key",
                    )?;
                    Ok(Constraint::PrimaryKey(indices))
                }
                TableConstraint::ForeignKey { .. } => {
                    _plan_err!("Foreign key constraints are not currently supported")
                }
                TableConstraint::Check { .. } => {
                    _plan_err!("Check constraints are not currently supported")
                }
                TableConstraint::Index { .. } => {
                    _plan_err!("Indexes are not currently supported")
                }
                TableConstraint::FulltextOrSpatial { .. } => {
                    _plan_err!("Indexes are not currently supported")
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Constraints::new_unverified(constraints))
    }

    fn parse_options_map(
        &self,
        options: Vec<(String, Value)>,
        allow_duplicates: bool,
    ) -> Result<HashMap<String, String>> {
        let mut options_map = HashMap::new();
        for (key, value) in options {
            if !allow_duplicates && options_map.contains_key(&key) {
                return plan_err!("Option {key} is specified multiple times");
            }

            let Some(value_string) = crate::utils::value_to_string(&value) else {
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
        verbose: bool,
        analyze: bool,
        format: Option<String>,
        statement: DFStatement,
    ) -> Result<LogicalPlan> {
        let plan = self.statement_to_plan(statement)?;
        if matches!(plan, LogicalPlan::Explain(_)) {
            return plan_err!("Nested EXPLAINs are not supported");
        }

        let plan = Arc::new(plan);
        let schema = LogicalPlan::explain_schema();
        let schema = schema.to_dfschema_ref()?;

        if verbose && format.is_some() {
            return plan_err!("EXPLAIN VERBOSE with FORMAT is not supported");
        }

        if analyze {
            if format.is_some() {
                return plan_err!("EXPLAIN ANALYZE with FORMAT is not supported");
            }
            Ok(LogicalPlan::Analyze(Analyze {
                verbose,
                input: plan,
                schema,
            }))
        } else {
            let stringified_plans =
                vec![plan.to_stringified(PlanType::InitialLogicalPlan)];

            // default to configuration value
            // verbose mode only supports indent format
            let options = self.context_provider.options();
            let format = if verbose {
                ExplainFormat::Indent
            } else if let Some(format) = format {
                ExplainFormat::from_str(&format)?
            } else {
                options.explain.format.clone()
            };

            Ok(LogicalPlan::Explain(Explain {
                verbose,
                explain_format: format,
                plan,
                stringified_plans,
                schema,
                logical_optimization_succeeded: false,
            }))
        }
    }

    fn show_variable_to_plan(&self, variable: &[Ident]) -> Result<LogicalPlan> {
        if !self.has_table("information_schema", "df_settings") {
            return plan_err!(
                "SHOW [VARIABLE] is not supported unless information_schema is enabled"
            );
        }

        let verbose = variable
            .last()
            .map(|s| ident_to_string(s) == "verbose")
            .unwrap_or(false);
        let mut variable_vec = variable.to_vec();
        let mut columns: String = "name, value".to_owned();

        if verbose {
            columns = format!("{columns}, description");
            variable_vec = variable_vec.split_at(variable_vec.len() - 1).0.to_vec();
        }

        let variable = object_name_to_string(&ObjectName::from(variable_vec));
        let base_query = format!("SELECT {columns} FROM information_schema.df_settings");
        let query = if variable == "all" {
            // Add an ORDER BY so the output comes out in a consistent order
            format!("{base_query} ORDER BY name")
        } else if variable == "timezone" || variable == "time.zone" {
            // we could introduce alias in OptionDefinition if this string matching thing grows
            format!("{base_query} WHERE name = 'datafusion.execution.time_zone'")
        } else {
            // These values are what are used to make the information_schema table, so we just
            // check here, before actually planning or executing the query, if it would produce no
            // results, and error preemptively if it would (for a better UX)
            let is_valid_variable = self
                .context_provider
                .options()
                .entries()
                .iter()
                .any(|opt| opt.key == variable);

            // Check if it's a runtime variable
            let is_runtime_variable = variable.starts_with("datafusion.runtime.");

            if !is_valid_variable && !is_runtime_variable {
                return plan_err!(
                    "'{variable}' is not a variable which can be viewed with 'SHOW'"
                );
            }

            format!("{base_query} WHERE name = '{variable}'")
        };

        let mut rewrite = DFParser::parse_sql(&query)?;
        assert_eq!(rewrite.len(), 1);

        self.statement_to_plan(rewrite.pop_front().unwrap())
    }

    fn set_statement_to_plan(&self, statement: Set) -> Result<LogicalPlan> {
        match statement {
            Set::SingleAssignment {
                scope,
                hivevar,
                variable,
                values,
            } => {
                if scope.is_some() {
                    return not_impl_err!("SET with scope modifiers is not supported");
                }

                if hivevar {
                    return not_impl_err!("SET HIVEVAR is not supported");
                }

                let variable = object_name_to_string(&variable);
                let mut variable_lower = variable.to_lowercase();

                // Map PostgreSQL "timezone" and MySQL "time.zone" aliases to DataFusion's canonical name
                if variable_lower == "timezone" || variable_lower == "time.zone" {
                    variable_lower = "datafusion.execution.time_zone".to_string();
                }

                if values.len() != 1 {
                    return plan_err!("SET only supports single value assignment");
                }

                let value_string = match &values[0] {
                    SQLExpr::Identifier(i) => ident_to_string(i),
                    SQLExpr::Value(v) => match crate::utils::value_to_string(&v.value) {
                        None => {
                            return plan_err!("Unsupported value {:?}", v.value);
                        }
                        Some(s) => s,
                    },
                    SQLExpr::UnaryOp { op, expr } => match op {
                        UnaryOperator::Plus => format!("+{expr}"),
                        UnaryOperator::Minus => format!("-{expr}"),
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
            other => not_impl_err!("SET variant not implemented yet: {other:?}"),
        }
    }

    fn reset_statement_to_plan(&self, statement: ResetStatement) -> Result<LogicalPlan> {
        match statement {
            ResetStatement::Variable(variable) => {
                let variable = object_name_to_string(&variable);
                let mut variable_lower = variable.to_lowercase();

                // Map PostgreSQL "timezone" and MySQL "time.zone" aliases to DataFusion's canonical name
                if variable_lower == "timezone" || variable_lower == "time.zone" {
                    variable_lower = "datafusion.execution.time_zone".to_string();
                }

                Ok(LogicalPlan::Statement(PlanStatement::ResetVariable(
                    ResetVariable {
                        variable: variable_lower,
                    },
                )))
            }
        }
    }

    fn delete_to_plan(
        &self,
        table_name: &ObjectName,
        predicate_expr: Option<SQLExpr>,
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

    fn update_to_plan(
        &self,
        table: TableWithJoins,
        assignments: &[Assignment],
        from: Option<TableWithJoins>,
        predicate_expr: Option<SQLExpr>,
    ) -> Result<LogicalPlan> {
        let (table_name, table_alias) = match &table.relation {
            TableFactor::Table { name, alias, .. } => (name.clone(), alias.clone()),
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
                let cols = match &assign.target {
                    AssignmentTarget::ColumnName(cols) => cols,
                    _ => plan_err!("Tuples are not supported")?,
                };
                let col_name: &Ident = cols
                    .0
                    .iter()
                    .last()
                    .ok_or_else(|| plan_datafusion_err!("Empty column id"))?
                    .as_ident()
                    .unwrap();
                // Validate that the assignment target column exists
                table_schema.field_with_unqualified_name(&col_name.value)?;
                Ok((col_name.value.clone(), assign.value.clone()))
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

    fn insert_to_plan(
        &self,
        table_name: ObjectName,
        columns: Vec<Ident>,
        source: Box<Query>,
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
        if let SetExpr::Values(ast::Values { rows, .. }) = (*source.body).clone() {
            for row in rows.iter() {
                for (idx, val) in row.iter().enumerate() {
                    if let SQLExpr::Value(ValueWithSpan {
                        value: Value::Placeholder(name),
                        span: _,
                    }) = val
                    {
                        let name =
                            name.replace('$', "").parse::<usize>().map_err(|_| {
                                plan_datafusion_err!("Can't parse placeholder: {name}")
                            })? - 1;
                        let field = fields.get(idx).ok_or_else(|| {
                            plan_datafusion_err!(
                                "Placeholder ${} refers to a non existent column",
                                idx + 1
                            )
                        })?;
                        let _ = prepare_param_data_types.insert(name, Arc::clone(field));
                    }
                }
            }
        }
        let prepare_param_data_types = prepare_param_data_types.into_values().collect();

        // Projection
        let mut planner_context =
            PlannerContext::new().with_prepare_param_data_types(prepare_param_data_types);
        planner_context.set_table_schema(Some(DFSchemaRef::new(
            DFSchema::from_unqualified_fields(fields.clone(), Default::default())?,
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
                Ok(expr.alias(target_field.name()))
            })
            .collect::<Result<Vec<Expr>>>()?;
        let source = project(source, exprs)?;

        let insert_op = match (overwrite, replace_into) {
            (false, false) => InsertOp::Append,
            (true, false) => InsertOp::Overwrite,
            (false, true) => InsertOp::Replace,
            (true, true) => plan_err!("Conflicting insert operations: `overwrite` and `replace_into` cannot both be true")?,
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
        sql_table_name: ObjectName,
    ) -> Result<LogicalPlan> {
        // Figure out the where clause
        let where_clause = object_name_to_qualifier(
            &sql_table_name,
            self.options.enable_ident_normalization,
        )?;

        if !self.has_table("information_schema", "columns") {
            return plan_err!(
                "SHOW COLUMNS is not supported unless information_schema is enabled"
            );
        }

        // Do a table lookup to verify the table exists
        let table_ref = self.object_name_to_table_reference(sql_table_name)?;
        let _ = self.context_provider.get_table_source(table_ref)?;

        // Treat both FULL and EXTENDED as the same
        let select_list = if full || extended {
            "*"
        } else {
            "table_catalog, table_schema, table_name, column_name, data_type, is_nullable"
        };

        let query = format!(
            "SELECT {select_list} FROM information_schema.columns WHERE {where_clause}"
        );

        let mut rewrite = DFParser::parse_sql(&query)?;
        assert_eq!(rewrite.len(), 1);
        self.statement_to_plan(rewrite.pop_front().unwrap()) // length of rewrite is 1
    }

    /// Rewrite `SHOW FUNCTIONS` to another SQL query
    /// The query is based on the `information_schema.routines` and `information_schema.parameters` tables
    ///
    /// The output columns:
    /// - function_name: The name of function
    /// - return_type: The return type of the function
    /// - parameters: The name of parameters (ordered by the ordinal position)
    /// - parameter_types: The type of parameters (ordered by the ordinal position)
    /// - description: The description of the function (the description defined in the document)
    /// - syntax_example: The syntax_example of the function (the syntax_example defined in the document)
    fn show_functions_to_plan(
        &self,
        filter: Option<ShowStatementFilter>,
    ) -> Result<LogicalPlan> {
        let where_clause = if let Some(filter) = filter {
            match filter {
                ShowStatementFilter::Like(like) => {
                    format!("WHERE p.function_name like '{like}'")
                }
                _ => return plan_err!("Unsupported SHOW FUNCTIONS filter"),
            }
        } else {
            "".to_string()
        };

        let query = format!(
            r#"
SELECT DISTINCT
    p.*,
    r.function_type function_type,
    r.description description,
    r.syntax_example syntax_example
FROM
    (
        SELECT
            i.specific_name function_name,
            o.data_type return_type,
            array_agg(i.parameter_name ORDER BY i.ordinal_position ASC) parameters,
            array_agg(i.data_type ORDER BY i.ordinal_position ASC) parameter_types
        FROM (
                 SELECT
                     specific_catalog,
                     specific_schema,
                     specific_name,
                     ordinal_position,
                     parameter_name,
                     data_type,
                     rid
                 FROM
                     information_schema.parameters
                 WHERE
                     parameter_mode = 'IN'
             ) i
                 JOIN
             (
                 SELECT
                     specific_catalog,
                     specific_schema,
                     specific_name,
                     ordinal_position,
                     parameter_name,
                     data_type,
                     rid
                 FROM
                     information_schema.parameters
                 WHERE
                     parameter_mode = 'OUT'
             ) o
             ON i.specific_catalog = o.specific_catalog
                 AND i.specific_schema = o.specific_schema
                 AND i.specific_name = o.specific_name
                 AND i.rid = o.rid
        GROUP BY 1, 2, i.rid
    ) as p
JOIN information_schema.routines r
ON p.function_name = r.routine_name
{where_clause}
            "#
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
            return plan_err!(
                "SHOW CREATE TABLE is not supported unless information_schema is enabled"
            );
        }
        // Figure out the where clause
        let where_clause = object_name_to_qualifier(
            &sql_table_name,
            self.options.enable_ident_normalization,
        )?;

        // Do a table lookup to verify the table exists
        let table_ref = self.object_name_to_table_reference(sql_table_name)?;
        let _ = self.context_provider.get_table_source(table_ref)?;

        let query = format!(
            "SELECT table_catalog, table_schema, table_name, definition FROM information_schema.views WHERE {where_clause}"
        );

        let mut rewrite = DFParser::parse_sql(&query)?;
        assert_eq!(rewrite.len(), 1);
        self.statement_to_plan(rewrite.pop_front().unwrap()) // length of rewrite is 1
    }

    /// Return true if there is a table provider available for "schema.table"
    fn has_table(&self, schema: &str, table: &str) -> bool {
        let tables_reference = TableReference::Partial {
            schema: schema.into(),
            table: table.into(),
        };
        self.context_provider
            .get_table_source(tables_reference)
            .is_ok()
    }

    fn validate_transaction_kind(
        &self,
        kind: Option<&BeginTransactionKind>,
    ) -> Result<()> {
        match kind {
            // BEGIN
            None => Ok(()),
            // BEGIN TRANSACTION
            Some(BeginTransactionKind::Transaction) => Ok(()),
            Some(BeginTransactionKind::Work) => {
                not_impl_err!("Transaction kind not supported: {kind:?}")
            }
        }
    }
}
