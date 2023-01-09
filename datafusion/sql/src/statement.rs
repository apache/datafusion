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
    CreateExternalTable, DFParser, DescribeTable, Statement as DFStatement,
};
use crate::planner::{
    object_name_to_qualifier, object_name_to_table_reference, ContextProvider,
    PlannerContext, SqlToRel,
};
use arrow_schema::DataType;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{
    DFSchema, DFSchemaRef, DataFusionError, OwnedTableReference, Result, TableReference,
    ToDFSchema,
};
use datafusion_expr::logical_plan::{Analyze, Prepare};
use datafusion_expr::{
    cast, col, CreateCatalog, CreateCatalogSchema,
    CreateExternalTable as PlanCreateExternalTable, CreateMemoryTable, CreateView,
    DropTable, DropView, Explain, LogicalPlan, LogicalPlanBuilder, PlanType, SetVariable,
    ToStringifiedPlan,
};
use sqlparser::ast::{
    Expr as SQLExpr, Ident, ObjectName, ObjectType, ShowCreateObject,
    ShowStatementFilter, Statement, UnaryOperator, Value,
};
use sqlparser::parser::ParserError::ParserError;
use std::sync::Arc;

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
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
                plan = self.apply_expr_alias(plan, columns)?;

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
                "Unsupported SQL statement: {sql:?}"
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

    fn describe_table_to_plan(&self, statement: DescribeTable) -> Result<LogicalPlan> {
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
            options,
        } = statement;

        // semantic checks
        if file_type == "PARQUET" && !columns.is_empty() {
            Err(DataFusionError::Plan(
                "Column definitions can not be specified for PARQUET files.".into(),
            ))?;
        }

        if file_type != "CSV"
            && file_type != "JSON"
            && file_compression_type != CompressionTypeVariant::UNCOMPRESSED
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
            }))
        }
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
                Value::DollarQuotedString(s) => s.to_string(),
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
                    )));
                }
            },
            // for capture signed number e.g. +8, -8
            SQLExpr::UnaryOp { op, expr } => match op {
                UnaryOperator::Plus => format!("+{expr}"),
                UnaryOperator::Minus => format!("-{expr}"),
                _ => {
                    return Err(DataFusionError::Plan(format!(
                        "Unsupported Value {}",
                        value[0]
                    )));
                }
            },
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "Unsupported Value {}",
                    value[0]
                )));
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
            "SELECT table_catalog, table_schema, table_name, definition FROM information_schema.views WHERE {where_clause}"
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
}
