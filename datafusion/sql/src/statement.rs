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
    CreateExternalTable, DFParser, DescribeTableStmt, Statement as DFStatement,
};
use crate::planner::{
    object_name_to_qualifier, ContextProvider, PlannerContext, SqlToRel,
};
use crate::utils::normalize_ident;
use arrow_schema::DataType;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{
    Column, DFField, DFSchema, DFSchemaRef, DataFusionError, ExprSchema,
    OwnedTableReference, Result, TableReference, ToDFSchema,
};
use datafusion_expr::expr_rewriter::normalize_col_with_schemas_and_ambiguity_check;
use datafusion_expr::logical_plan::builder::project;
use datafusion_expr::logical_plan::{Analyze, Prepare};
use datafusion_expr::utils::expr_to_columns;
use datafusion_expr::{
    cast, col, CreateCatalog, CreateCatalogSchema,
    CreateExternalTable as PlanCreateExternalTable, CreateMemoryTable, CreateView,
    DescribeTable, DmlStatement, DropTable, DropView, Explain, ExprSchemable, Filter,
    LogicalPlan, LogicalPlanBuilder, PlanType, SetVariable, ToStringifiedPlan, WriteOp,
};
use sqlparser::ast;
use sqlparser::ast::{
    Assignment, Expr as SQLExpr, Expr, Ident, ObjectName, ObjectType, Query, SchemaName,
    SetExpr, ShowCreateObject, ShowStatementFilter, Statement, TableFactor,
    TableWithJoins, UnaryOperator, Value,
};
use sqlparser::parser::ParserError::ParserError;
use std::collections::{BTreeMap, HashMap, HashSet};
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
                    name: self.object_name_to_table_reference(name)?,
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
                    name: self.object_name_to_table_reference(name)?,
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
                schema_name: get_schema_name(&schema_name),
                if_not_exists,
                schema: Arc::new(DFSchema::empty()),
            })),
            Statement::CreateDatabase {
                db_name,
                if_not_exists,
                ..
            } => Ok(LogicalPlan::CreateCatalog(CreateCatalog {
                catalog_name: object_name_to_string(&db_name),
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
                    1 => self.object_name_to_table_reference(names.pop().unwrap()),
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
                    Err(DataFusionError::Plan(
                        "Inserts with or clauses not supported".to_owned(),
                    ))?;
                }
                if overwrite {
                    Err(DataFusionError::Plan(
                        "Insert overwrite is not supported".to_owned(),
                    ))?;
                }
                if partitioned.is_some() {
                    Err(DataFusionError::Plan(
                        "Partitioned inserts not yet supported".to_owned(),
                    ))?;
                }
                if !after_columns.is_empty() {
                    Err(DataFusionError::Plan(
                        "After-columns clause not supported".to_owned(),
                    ))?;
                }
                if table {
                    Err(DataFusionError::Plan(
                        "Table clause not supported".to_owned(),
                    ))?;
                }
                if on.is_some() {
                    Err(DataFusionError::Plan(
                        "Insert-on clause not supported".to_owned(),
                    ))?;
                }
                if returning.is_some() {
                    Err(DataFusionError::Plan(
                        "Insert-returning clause not yet supported".to_owned(),
                    ))?;
                }
                let _ = into; // optional keyword doesn't change behavior
                self.insert_to_plan(table_name, columns, source)
            }

            Statement::Update {
                table,
                assignments,
                from,
                selection,
                returning,
            } => {
                if returning.is_some() {
                    Err(DataFusionError::Plan(
                        "Update-returning clause not yet supported".to_owned(),
                    ))?;
                }
                self.update_to_plan(table, assignments, from, selection)
            }

            Statement::Delete {
                table_name,
                using,
                selection,
                returning,
            } => {
                if using.is_some() {
                    Err(DataFusionError::Plan(
                        "Using clause not supported".to_owned(),
                    ))?;
                }
                if returning.is_some() {
                    Err(DataFusionError::Plan(
                        "Delete-returning clause not yet supported".to_owned(),
                    ))?;
                }
                self.delete_to_plan(table_name, selection)
            }

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

    fn describe_table_to_plan(
        &self,
        statement: DescribeTableStmt,
    ) -> Result<LogicalPlan> {
        let DescribeTableStmt { table_name } = statement;
        let table_ref = self.object_name_to_table_reference(table_name)?;

        let table_source = self
            .schema_provider
            .get_table_provider((&table_ref).into())?;

        let schema = table_source.schema();

        Ok(LogicalPlan::DescribeTable(DescribeTable {
            schema,
            dummy_schema: DFSchemaRef::new(DFSchema::empty()),
        }))
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
                logical_optimization_succeeded: false,
            }))
        }
    }

    fn show_variable_to_plan(&self, variable: &[Ident]) -> Result<LogicalPlan> {
        let variable = object_name_to_string(&ObjectName(variable.to_vec()));

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

    fn delete_to_plan(
        &self,
        table_factor: TableFactor,
        predicate_expr: Option<Expr>,
    ) -> Result<LogicalPlan> {
        let table_name = match &table_factor {
            TableFactor::Table { name, .. } => name.clone(),
            _ => Err(DataFusionError::Plan(
                "Cannot delete from non-table relations!".to_string(),
            ))?,
        };

        // Do a table lookup to verify the table exists
        let table_ref = self.object_name_to_table_reference(table_name.clone())?;
        let provider = self
            .schema_provider
            .get_table_provider((&table_ref).into())?;
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
            _ => Err(DataFusionError::Plan(
                "Cannot update non-table relation!".to_string(),
            ))?,
        };

        // Do a table lookup to verify the table exists
        let table_name = self.object_name_to_table_reference(table_name)?;
        let provider = self
            .schema_provider
            .get_table_provider((&table_name).into())?;
        let arrow_schema = (*provider.schema()).clone();
        let table_schema = Arc::new(DFSchema::try_from(arrow_schema)?);
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
                let col_name: &Ident = assign
                    .id
                    .iter()
                    .last()
                    .ok_or(DataFusionError::Plan("Empty column id".to_string()))?;
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
                datafusion_expr::Expr::Placeholder {
                    ref id,
                    ref data_type,
                } => match data_type {
                    None => {
                        let dt = table_schema.data_type(&Column::from_name(&col_name))?;
                        datafusion_expr::Expr::Placeholder {
                            id: id.clone(),
                            data_type: Some(dt.clone()),
                        }
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
    ) -> Result<LogicalPlan> {
        // Do a table lookup to verify the table exists
        let table_name = self.object_name_to_table_reference(table_name)?;
        let provider = self
            .schema_provider
            .get_table_provider((&table_name).into())?;
        let arrow_schema = (*provider.schema()).clone();
        let table_schema = DFSchema::try_from(arrow_schema)?;

        let fields = if columns.is_empty() {
            // Empty means we're inserting into all columns of the table
            table_schema.fields().clone()
        } else {
            let fields = columns
                .iter()
                .map(|c| {
                    Ok(table_schema
                        .field_with_unqualified_name(&normalize_ident(c.clone()))?
                        .clone())
                })
                .collect::<Result<Vec<DFField>>>()?;
            // Validate no duplicate fields
            let table_schema =
                DFSchema::new_with_metadata(fields, table_schema.metadata().clone())?;
            table_schema.fields().clone()
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
            PlannerContext::new_with_prepare_param_data_types(prepare_param_data_types);
        let source = self.query_to_plan(*source, &mut planner_context)?;
        if fields.len() != source.schema().fields().len() {
            Err(DataFusionError::Plan(
                "Column count doesn't match insert query!".to_owned(),
            ))?;
        }
        let exprs = fields
            .iter()
            .zip(source.schema().fields().iter())
            .map(|(target_field, source_field)| {
                let expr =
                    datafusion_expr::Expr::Column(source_field.unqualified_column())
                        .cast_to(target_field.data_type(), source.schema())?
                        .alias(target_field.name());
                Ok(expr)
            })
            .collect::<Result<Vec<datafusion_expr::Expr>>>()?;
        let source = project(source, exprs)?;

        let plan = LogicalPlan::Dml(DmlStatement {
            table_name,
            table_schema: Arc::new(table_schema),
            op: WriteOp::Insert,
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
        let where_clause = object_name_to_qualifier(
            &sql_table_name,
            self.options.enable_ident_normalization,
        );

        // Do a table lookup to verify the table exists
        let table_ref = self.object_name_to_table_reference(sql_table_name)?;
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
        let where_clause = object_name_to_qualifier(
            &sql_table_name,
            self.options.enable_ident_normalization,
        );

        // Do a table lookup to verify the table exists
        let table_ref = self.object_name_to_table_reference(sql_table_name)?;
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
        let tables_reference = TableReference::Partial {
            schema: schema.into(),
            table: table.into(),
        };
        self.schema_provider
            .get_table_provider(tables_reference)
            .is_ok()
    }
}
