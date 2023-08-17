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

use crate::parser::{
    CopyToSource, CopyToStatement, CreateExternalTable, DFParser, DescribeTableStmt,
    LexOrdering, Statement as DFStatement,
};
use crate::planner::{
    object_name_to_qualifier, ContextProvider, PlannerContext, SqlToRel,
};
use crate::utils::normalize_ident;

use arrow_schema::DataType;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{
    unqualified_field_not_found, Column, Constraints, DFField, DFSchema, DFSchemaRef,
    DataFusionError, ExprSchema, OwnedTableReference, Result, SchemaReference,
    TableReference, ToDFSchema,
};
use datafusion_expr::dml::{CopyTo, OutputFileFormat};
use datafusion_expr::expr::Placeholder;
use datafusion_expr::expr_rewriter::normalize_col_with_schemas_and_ambiguity_check;
use datafusion_expr::logical_plan::builder::project;
use datafusion_expr::logical_plan::DdlStatement;
use datafusion_expr::utils::expr_to_columns;
use datafusion_expr::{
    cast, col, Analyze, CreateCatalog, CreateCatalogSchema,
    CreateExternalTable as PlanCreateExternalTable, CreateMemoryTable, CreateView,
    DescribeTable, DmlStatement, DropCatalogSchema, DropTable, DropView, EmptyRelation,
    Explain, ExprSchemable, Filter, LogicalPlan, LogicalPlanBuilder, PlanType, Prepare,
    SetVariable, Statement as PlanStatement, ToStringifiedPlan, TransactionAccessMode,
    TransactionConclusion, TransactionEnd, TransactionIsolationLevel, TransactionStart,
    WriteOp,
};
use sqlparser::ast;
use sqlparser::ast::{
    Assignment, Expr as SQLExpr, Expr, Ident, ObjectName, ObjectType, Query, SchemaName,
    SetExpr, ShowCreateObject, ShowStatementFilter, Statement, TableFactor,
    TableWithJoins, TransactionMode, UnaryOperator, Value,
};
use sqlparser::parser::ParserError::ParserError;

use datafusion_common::plan_err;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

fn ident_to_string(ident: &Ident) -> String {
    normalize_ident(ident.to_owned())
}

fn object_name_to_string(object_name: &ObjectName) -> String {
    object_name
        .0
        .iter()
        .map(ident_to_string)
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

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    /// Generate a logical plan from an DataFusion SQL statement
    pub fn statement_to_plan(&self, statement: DFStatement) -> Result<LogicalPlan> {
        match statement {
            DFStatement::CreateExternalTable(s) => self.external_table_to_plan(s),
            DFStatement::Statement(s) => self.sql_statement_to_plan(*s),
            DFStatement::DescribeTableStmt(s) => self.describe_table_to_plan(s),
            DFStatement::CopyTo(s) => self.copy_to_plan(s),
        }
    }

    /// Generate a logical plan from an SQL statement
    pub fn sql_statement_to_plan(&self, statement: Statement) -> Result<LogicalPlan> {
        self.sql_statement_to_plan_with_context(statement, &mut PlannerContext::new())
    }

    /// Generate a logical plan from an SQL statement
    fn sql_statement_to_plan_with_context(
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
                query,
                name,
                columns,
                constraints,
                table_properties,
                with_options,
                if_not_exists,
                or_replace,
                ..
            } if table_properties.is_empty() && with_options.is_empty() => match query {
                Some(query) => {
                    let plan = self.query_to_plan(*query, planner_context)?;
                    let input_schema = plan.schema();

                    let plan = if !columns.is_empty() {
                        let schema = self.build_schema(columns)?.to_dfschema_ref()?;
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

                    let constraints = Constraints::new_from_table_constraints(
                        &constraints,
                        plan.schema(),
                    )?;

                    Ok(LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(
                        CreateMemoryTable {
                            name: self.object_name_to_table_reference(name)?,
                            constraints,
                            input: Arc::new(plan),
                            if_not_exists,
                            or_replace,
                        },
                    )))
                }

                None => {
                    let schema = self.build_schema(columns)?.to_dfschema_ref()?;
                    let plan = EmptyRelation {
                        produce_one_row: false,
                        schema,
                    };
                    let plan = LogicalPlan::EmptyRelation(plan);
                    let constraints = Constraints::new_from_table_constraints(
                        &constraints,
                        plan.schema(),
                    )?;
                    Ok(LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(
                        CreateMemoryTable {
                            name: self.object_name_to_table_reference(name)?,
                            constraints,
                            input: Arc::new(plan),
                            if_not_exists,
                            or_replace,
                        },
                    )))
                }
            },

            Statement::CreateView {
                or_replace,
                name,
                columns,
                query,
                with_options,
                ..
            } if with_options.is_empty() => {
                let mut plan = self.query_to_plan(*query, &mut PlannerContext::new())?;
                plan = self.apply_expr_alias(plan, columns)?;

                Ok(LogicalPlan::Ddl(DdlStatement::CreateView(CreateView {
                    name: self.object_name_to_table_reference(name)?,
                    input: Arc::new(plan),
                    or_replace,
                    definition: sql,
                })))
            }
            Statement::ShowCreate { obj_type, obj_name } => match obj_type {
                ShowCreateObject::Table => self.show_create_table_to_plan(obj_name),
                _ => Err(DataFusionError::NotImplemented(
                    "Only `SHOW CREATE TABLE  ...` statement is supported".to_string(),
                )),
            },
            Statement::CreateSchema {
                schema_name,
                if_not_exists,
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
                            TableReference::Bare { table } => Ok(SchemaReference::Bare { schema: table } ) ,
                            TableReference::Partial { schema, table } => Ok(SchemaReference::Full { schema: table,catalog: schema }),
                            TableReference::Full { catalog: _, schema: _, table: _ } => {
                                Err(ParserError("Invalid schema specifier (has 3 parts)".to_string()))
                            },
                        }?;
                        Ok(LogicalPlan::Ddl(DdlStatement::DropCatalogSchema(DropCatalogSchema {
                            name,
                            if_exists,
                            cascade,
                            schema: DFSchemaRef::new(DFSchema::empty()),
                        })))},
                    _ => Err(DataFusionError::NotImplemented(
                        "Only `DROP TABLE/VIEW/SCHEMA  ...` statement is supported currently"
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
                let mut planner_context = PlannerContext::new()
                    .with_prepare_param_data_types(data_types.clone());

                // Build logical plan for inner statement of the prepare statement
                let plan = self.sql_statement_to_plan_with_context(
                    *statement,
                    &mut planner_context,
                )?;
                Ok(LogicalPlan::Prepare(Prepare {
                    name: ident_to_string(&name),
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

            Statement::Insert {
                or,
                into,
                table_name,
                columns,
                overwrite,
                source,
                partitioned,
                after_columns,
                table,
                on,
                returning,
            } => {
                if or.is_some() {
                    plan_err!("Inserts with or clauses not supported")?;
                }
                if partitioned.is_some() {
                    plan_err!("Partitioned inserts not yet supported")?;
                }
                if !after_columns.is_empty() {
                    plan_err!("After-columns clause not supported")?;
                }
                if table {
                    plan_err!("Table clause not supported")?;
                }
                if on.is_some() {
                    plan_err!("Insert-on clause not supported")?;
                }
                if returning.is_some() {
                    plan_err!("Insert-returning clause not supported")?;
                }
                let _ = into; // optional keyword doesn't change behavior
                self.insert_to_plan(table_name, columns, source, overwrite)
            }
            Statement::Update {
                table,
                assignments,
                from,
                selection,
                returning,
            } => {
                if returning.is_some() {
                    plan_err!("Update-returning clause not yet supported")?;
                }
                self.update_to_plan(table, assignments, from, selection)
            }

            Statement::Delete {
                tables,
                using,
                selection,
                returning,
                from,
            } => {
                if !tables.is_empty() {
                    plan_err!("DELETE <TABLE> not supported")?;
                }

                if using.is_some() {
                    plan_err!("Using clause not supported")?;
                }

                if returning.is_some() {
                    plan_err!("Delete-returning clause not yet supported")?;
                }
                let table_name = self.get_delete_target(from)?;
                self.delete_to_plan(table_name, selection)
            }

            Statement::StartTransaction { modes } => {
                let isolation_level: ast::TransactionIsolationLevel = modes
                    .iter()
                    .filter_map(|m: &ast::TransactionMode| match m {
                        TransactionMode::AccessMode(_) => None,
                        TransactionMode::IsolationLevel(level) => Some(level),
                    })
                    .last()
                    .copied()
                    .unwrap_or(ast::TransactionIsolationLevel::Serializable);
                let access_mode: ast::TransactionAccessMode = modes
                    .iter()
                    .filter_map(|m: &ast::TransactionMode| match m {
                        TransactionMode::AccessMode(mode) => Some(mode),
                        TransactionMode::IsolationLevel(_) => None,
                    })
                    .last()
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
                    schema: DFSchemaRef::new(DFSchema::empty()),
                });
                Ok(LogicalPlan::Statement(statement))
            }
            Statement::Commit { chain } => {
                let statement = PlanStatement::TransactionEnd(TransactionEnd {
                    conclusion: TransactionConclusion::Commit,
                    chain,
                    schema: DFSchemaRef::new(DFSchema::empty()),
                });
                Ok(LogicalPlan::Statement(statement))
            }
            Statement::Rollback { chain } => {
                let statement = PlanStatement::TransactionEnd(TransactionEnd {
                    conclusion: TransactionConclusion::Rollback,
                    chain,
                    schema: DFSchemaRef::new(DFSchema::empty()),
                });
                Ok(LogicalPlan::Statement(statement))
            }

            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported SQL statement: {sql:?}"
            ))),
        }
    }

    fn get_delete_target(&self, mut from: Vec<TableWithJoins>) -> Result<ObjectName> {
        if from.len() != 1 {
            return Err(DataFusionError::NotImplemented(format!(
                "DELETE FROM only supports single table, got {}: {from:?}",
                from.len()
            )));
        }
        let table_factor = from.pop().unwrap();
        if !table_factor.joins.is_empty() {
            return Err(DataFusionError::NotImplemented(
                "DELETE FROM only supports single table, got: joins".to_string(),
            ));
        }
        let TableFactor::Table{name, ..} = table_factor.relation else {
            return Err(DataFusionError::NotImplemented(format!(
                "DELETE FROM only supports single table, got: {table_factor:?}"
            )))
        };

        Ok(name)
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
                plan_err!("Unsupported parameters to SHOW TABLES")
            } else {
                let query = "SELECT * FROM information_schema.tables;";
                let mut rewrite = DFParser::parse_sql(query)?;
                assert_eq!(rewrite.len(), 1);
                self.statement_to_plan(rewrite.pop_front().unwrap()) // length of rewrite is 1
            }
        } else {
            plan_err!("SHOW TABLES is not supported unless information_schema is enabled")
        }
    }

    fn describe_table_to_plan(
        &self,
        statement: DescribeTableStmt,
    ) -> Result<LogicalPlan> {
        let DescribeTableStmt { table_name } = statement;
        let table_ref = self.object_name_to_table_reference(table_name)?;

        let table_source = self.schema_provider.get_table_provider(table_ref)?;

        let schema = table_source.schema();

        Ok(LogicalPlan::DescribeTable(DescribeTable {
            schema,
            dummy_schema: DFSchemaRef::new(DFSchema::empty()),
        }))
    }

    fn copy_to_plan(&self, statement: CopyToStatement) -> Result<LogicalPlan> {
        // determine if source is table or query and handle accordingly
        let copy_source = statement.source;
        let input = match copy_source {
            CopyToSource::Relation(object_name) => {
                let table_ref =
                    self.object_name_to_table_reference(object_name.clone())?;
                let table_source = self.schema_provider.get_table_provider(table_ref)?;
                LogicalPlanBuilder::scan(
                    object_name_to_string(&object_name),
                    table_source,
                    None,
                )?
                .build()?
            }
            CopyToSource::Query(query) => {
                self.query_to_plan(query, &mut PlannerContext::new())?
            }
        };

        // convert options to lowercase strings, check for explicitly set "format" option
        let mut options = vec![];
        let mut explicit_format = None;
        // default behavior is to assume the user is specifying a single file to which
        // we should output all data regardless of input partitioning.
        let mut per_thread_output: bool = false;
        for (key, value) in statement.options {
            let (k, v) = (key.to_lowercase(), value.to_string().to_lowercase());
            // check for options important to planning
            if k == "format" {
                explicit_format = Some(OutputFileFormat::from_str(&v)?);
            }
            if k == "per_thread_output" {
                per_thread_output = match v.as_str(){
                    "true" => true,
                    "false" => false,
                    _ => return Err(DataFusionError::Plan(
                        format!("Copy to option 'per_thread_output' must be true or false, got {value}")
                    ))
                }
            }
            options.push((k, v));
        }
        let format = match explicit_format {
            Some(file_format) => file_format,
            None => {
                // try to infer file format from file extension
                let extension: &str = &Path::new(&statement.target)
                    .extension()
                    .ok_or(
                        DataFusionError::Plan("Copy To format not explicitly set and unable to get file extension!".to_string()))?
                    .to_str()
                    .ok_or(DataFusionError::Plan("Copy to format not explicitly set and failed to parse file extension!".to_string()))?
                    .to_lowercase();

                OutputFileFormat::from_str(extension)?
            }
        };
        Ok(LogicalPlan::Copy(CopyTo {
            input: Arc::new(input),
            output_url: statement.target,
            file_format: format,
            per_thread_output,
            options,
        }))
    }

    fn build_order_by(
        &self,
        order_exprs: Vec<LexOrdering>,
        schema: &DFSchemaRef,
        planner_context: &mut PlannerContext,
    ) -> Result<Vec<Vec<datafusion_expr::Expr>>> {
        // Ask user to provide a schema if schema is empty.
        if !order_exprs.is_empty() && schema.fields().is_empty() {
            return plan_err!(
                "Provide a schema before specifying the order while creating a table."
            );
        }

        let mut all_results = vec![];
        for expr in order_exprs {
            // Convert each OrderByExpr to a SortExpr:
            let expr_vec = self.order_by_to_sort_expr(&expr, schema, planner_context)?;
            // Verify that columns of all SortExprs exist in the schema:
            for expr in expr_vec.iter() {
                for column in expr.to_columns()?.iter() {
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
            has_header,
            delimiter,
            location,
            table_partition_cols,
            if_not_exists,
            file_compression_type,
            order_exprs,
            unbounded,
            options,
        } = statement;

        if file_type != "CSV"
            && file_type != "JSON"
            && file_compression_type != CompressionTypeVariant::UNCOMPRESSED
        {
            plan_err!("File compression type can be specified for CSV/JSON files.")?;
        }

        let schema = self.build_schema(columns)?;
        let df_schema = schema.to_dfschema_ref()?;

        let ordered_exprs =
            self.build_order_by(order_exprs, &df_schema, &mut PlannerContext::new())?;

        // External tables do not support schemas at the moment, so the name is just a table name
        let name = OwnedTableReference::bare(name);

        Ok(LogicalPlan::Ddl(DdlStatement::CreateExternalTable(
            PlanCreateExternalTable {
                schema: df_schema,
                name,
                location,
                file_type,
                has_header,
                delimiter,
                table_partition_cols,
                if_not_exists,
                definition,
                file_compression_type,
                order_exprs: ordered_exprs,
                unbounded,
                options,
            },
        )))
    }

    /// Generate a plan for EXPLAIN ... that will print out a plan
    ///
    fn explain_statement_to_plan(
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
                logical_optimization_succeeded: false,
            }))
        }
    }

    fn show_variable_to_plan(&self, variable: &[Ident]) -> Result<LogicalPlan> {
        let variable = object_name_to_string(&ObjectName(variable.to_vec()));

        if !self.has_table("information_schema", "df_settings") {
            return plan_err!(
                "SHOW [VARIABLE] is not supported unless information_schema is enabled"
            );
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
                "SELECT name, setting FROM information_schema.df_settings WHERE name = '{variable}'"
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

        let variable = object_name_to_string(variable);
        let mut variable_lower = variable.to_lowercase();

        if variable_lower == "timezone" || variable_lower == "time.zone" {
            // we could introduce alias in OptionDefinition if this string matching thing grows
            variable_lower = "datafusion.execution.time_zone".to_string();
        }

        // parse value string from Expr
        let value_string = match &value[0] {
            SQLExpr::Identifier(i) => ident_to_string(i),
            SQLExpr::Value(v) => match v {
                Value::SingleQuotedString(s) => s.to_string(),
                Value::DollarQuotedString(s) => s.to_string(),
                Value::Number(_, _) | Value::Boolean(_) => v.to_string(),
                Value::DoubleQuotedString(_)
                | Value::UnQuotedString(_)
                | Value::EscapedStringLiteral(_)
                | Value::NationalStringLiteral(_)
                | Value::SingleQuotedByteStringLiteral(_)
                | Value::DoubleQuotedByteStringLiteral(_)
                | Value::RawStringLiteral(_)
                | Value::HexStringLiteral(_)
                | Value::Null
                | Value::Placeholder(_) => {
                    return plan_err!("Unsupported Value {}", value[0]);
                }
            },
            // for capture signed number e.g. +8, -8
            SQLExpr::UnaryOp { op, expr } => match op {
                UnaryOperator::Plus => format!("+{expr}"),
                UnaryOperator::Minus => format!("-{expr}"),
                _ => {
                    return plan_err!("Unsupported Value {}", value[0]);
                }
            },
            _ => {
                return plan_err!("Unsupported Value {}", value[0]);
            }
        };

        let statement = PlanStatement::SetVariable(SetVariable {
            variable: variable_lower,
            value: value_string,
            schema: DFSchemaRef::new(DFSchema::empty()),
        });

        Ok(LogicalPlan::Statement(statement))
    }

    fn delete_to_plan(
        &self,
        table_name: ObjectName,
        predicate_expr: Option<Expr>,
    ) -> Result<LogicalPlan> {
        // Do a table lookup to verify the table exists
        let table_ref = self.object_name_to_table_reference(table_name.clone())?;
        let provider = self.schema_provider.get_table_provider(table_ref.clone())?;
        let schema = (*provider.schema()).clone();
        let schema = DFSchema::try_from(schema)?;
        let scan =
            LogicalPlanBuilder::scan(object_name_to_string(&table_name), provider, None)?
                .build()?;
        let mut planner_context = PlannerContext::new();

        let source = match predicate_expr {
            None => scan,
            Some(predicate_expr) => {
                let filter_expr =
                    self.sql_to_expr(predicate_expr, &schema, &mut planner_context)?;
                let schema = Arc::new(schema.clone());
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

        let plan = LogicalPlan::Dml(DmlStatement {
            table_name: table_ref,
            table_schema: schema.into(),
            op: WriteOp::Delete,
            input: Arc::new(source),
        });
        Ok(plan)
    }

    fn update_to_plan(
        &self,
        table: TableWithJoins,
        assignments: Vec<Assignment>,
        from: Option<TableWithJoins>,
        predicate_expr: Option<Expr>,
    ) -> Result<LogicalPlan> {
        let table_name = match &table.relation {
            TableFactor::Table { name, .. } => name.clone(),
            _ => plan_err!("Cannot update non-table relation!")?,
        };

        // Do a table lookup to verify the table exists
        let table_name = self.object_name_to_table_reference(table_name)?;
        let provider = self
            .schema_provider
            .get_table_provider(table_name.clone())?;
        let arrow_schema = (*provider.schema()).clone();
        let table_schema = Arc::new(DFSchema::try_from_qualified_schema(
            table_name.clone(),
            &arrow_schema,
        )?);
        let values = table_schema.fields().iter().map(|f| {
            (
                f.name().clone(),
                ast::Expr::Identifier(ast::Ident::from(f.name().as_str())),
            )
        });

        // Overwrite with assignment expressions
        let mut planner_context = PlannerContext::new();
        let mut assign_map = assignments
            .iter()
            .map(|assign| {
                let col_name: &Ident = assign.id.iter().last().ok_or_else(|| {
                    DataFusionError::Plan("Empty column id".to_string())
                })?;
                // Validate that the assignment target column exists
                table_schema.field_with_unqualified_name(&col_name.value)?;
                Ok((col_name.value.clone(), assign.value.clone()))
            })
            .collect::<Result<HashMap<String, Expr>>>()?;

        let values = values
            .into_iter()
            .map(|(k, v)| {
                let val = assign_map.remove(&k).unwrap_or(v);
                (k, val)
            })
            .collect::<Vec<_>>();

        // Build scan
        let from = from.unwrap_or(table);
        let scan = self.plan_from_tables(vec![from], &mut planner_context)?;

        // Filter
        let source = match predicate_expr {
            None => scan,
            Some(predicate_expr) => {
                let filter_expr = self.sql_to_expr(
                    predicate_expr,
                    &table_schema,
                    &mut planner_context,
                )?;
                let mut using_columns = HashSet::new();
                expr_to_columns(&filter_expr, &mut using_columns)?;
                let filter_expr = normalize_col_with_schemas_and_ambiguity_check(
                    filter_expr,
                    &[&[&table_schema]],
                    &[using_columns],
                )?;
                LogicalPlan::Filter(Filter::try_new(filter_expr, Arc::new(scan))?)
            }
        };

        // Projection
        let mut exprs = vec![];
        for (col_name, expr) in values.into_iter() {
            let expr = self.sql_to_expr(expr, &table_schema, &mut planner_context)?;
            let expr = match expr {
                datafusion_expr::Expr::Placeholder(Placeholder {
                    ref id,
                    ref data_type,
                }) => match data_type {
                    None => {
                        let dt = table_schema.data_type(&Column::from_name(&col_name))?;
                        datafusion_expr::Expr::Placeholder(Placeholder::new(
                            id.clone(),
                            Some(dt.clone()),
                        ))
                    }
                    Some(_) => expr,
                },
                _ => expr,
            };
            let expr = expr.alias(col_name);
            exprs.push(expr);
        }
        let source = project(source, exprs)?;

        let plan = LogicalPlan::Dml(DmlStatement {
            table_name,
            table_schema,
            op: WriteOp::Update,
            input: Arc::new(source),
        });
        Ok(plan)
    }

    fn insert_to_plan(
        &self,
        table_name: ObjectName,
        columns: Vec<Ident>,
        source: Box<Query>,
        overwrite: bool,
    ) -> Result<LogicalPlan> {
        // Do a table lookup to verify the table exists
        let table_name = self.object_name_to_table_reference(table_name)?;
        let provider = self
            .schema_provider
            .get_table_provider(table_name.clone())?;
        let arrow_schema = (*provider.schema()).clone();
        let table_schema = DFSchema::try_from(arrow_schema)?;

        // Get insert fields and index_mapping
        // The i-th field of the table is `fields[index_mapping[i]]`
        let (fields, index_mapping) = if columns.is_empty() {
            // Empty means we're inserting into all columns of the table
            (
                table_schema.fields().clone(),
                (0..table_schema.fields().len())
                    .map(Some)
                    .collect::<Vec<_>>(),
            )
        } else {
            let mut mapping = vec![None; table_schema.fields().len()];
            let fields = columns
                .into_iter()
                .map(|c| self.normalizer.normalize(c))
                .enumerate()
                .map(|(i, c)| {
                    let column_index = table_schema
                        .index_of_column_by_name(None, &c)?
                        .ok_or_else(|| unqualified_field_not_found(&c, &table_schema))?;
                    if mapping[column_index].is_some() {
                        return Err(DataFusionError::SchemaError(
                            datafusion_common::SchemaError::DuplicateUnqualifiedField {
                                name: c,
                            },
                        ));
                    } else {
                        mapping[column_index] = Some(i);
                    }
                    Ok(table_schema.field(column_index).clone())
                })
                .collect::<Result<Vec<DFField>>>()?;
            (fields, mapping)
        };

        // infer types for Values clause... other types should be resolvable the regular way
        let mut prepare_param_data_types = BTreeMap::new();
        if let SetExpr::Values(ast::Values { rows, .. }) = (*source.body).clone() {
            for row in rows.iter() {
                for (idx, val) in row.iter().enumerate() {
                    if let ast::Expr::Value(Value::Placeholder(name)) = val {
                        let name =
                            name.replace('$', "").parse::<usize>().map_err(|_| {
                                DataFusionError::Plan(format!(
                                    "Can't parse placeholder: {name}"
                                ))
                            })? - 1;
                        let field = fields.get(idx).ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "Placeholder ${} refers to a non existent column",
                                idx + 1
                            ))
                        })?;
                        let dt = field.field().data_type().clone();
                        let _ = prepare_param_data_types.insert(name, dt);
                    }
                }
            }
        }
        let prepare_param_data_types = prepare_param_data_types.into_values().collect();

        // Projection
        let mut planner_context =
            PlannerContext::new().with_prepare_param_data_types(prepare_param_data_types);
        let source = self.query_to_plan(*source, &mut planner_context)?;
        if fields.len() != source.schema().fields().len() {
            plan_err!("Column count doesn't match insert query!")?;
        }

        let exprs = index_mapping
            .into_iter()
            .flatten()
            .map(|i| {
                let target_field = &fields[i];
                let source_field = source.schema().field(i);
                let expr =
                    datafusion_expr::Expr::Column(source_field.unqualified_column())
                        .cast_to(target_field.data_type(), source.schema())?
                        .alias(target_field.name());
                Ok(expr)
            })
            .collect::<Result<Vec<datafusion_expr::Expr>>>()?;
        let source = project(source, exprs)?;

        let op = if overwrite {
            WriteOp::InsertOverwrite
        } else {
            WriteOp::InsertInto
        };

        let plan = LogicalPlan::Dml(DmlStatement {
            table_name,
            table_schema: Arc::new(table_schema),
            op,
            input: Arc::new(source),
        });
        Ok(plan)
    }

    fn show_columns_to_plan(
        &self,
        extended: bool,
        full: bool,
        sql_table_name: ObjectName,
        filter: Option<ShowStatementFilter>,
    ) -> Result<LogicalPlan> {
        if filter.is_some() {
            return plan_err!("SHOW COLUMNS with WHERE or LIKE is not supported");
        }

        if !self.has_table("information_schema", "columns") {
            return plan_err!(
                "SHOW COLUMNS is not supported unless information_schema is enabled"
            );
        }
        // Figure out the where clause
        let where_clause = object_name_to_qualifier(
            &sql_table_name,
            self.options.enable_ident_normalization,
        );

        // Do a table lookup to verify the table exists
        let table_ref = self.object_name_to_table_reference(sql_table_name)?;
        let _ = self.schema_provider.get_table_provider(table_ref)?;

        // treat both FULL and EXTENDED as the same
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
        );

        // Do a table lookup to verify the table exists
        let table_ref = self.object_name_to_table_reference(sql_table_name)?;
        let _ = self.schema_provider.get_table_provider(table_ref)?;

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
        self.schema_provider
            .get_table_provider(tables_reference)
            .is_ok()
    }
}
