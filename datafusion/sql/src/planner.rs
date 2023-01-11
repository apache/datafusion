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
use std::collections::HashMap;
use std::sync::Arc;
use std::vec;

use arrow_schema::*;
use sqlparser::ast::ExactNumberInfo;
use sqlparser::ast::TimezoneInfo;
use sqlparser::ast::{ColumnDef as SQLColumnDef, ColumnOption};
use sqlparser::ast::{DataType as SQLDataType, Ident, ObjectName, TableAlias};

use datafusion_common::config::ConfigOptions;
use datafusion_common::{field_not_found, DFSchema, DataFusionError, Result};
use datafusion_common::{OwnedTableReference, TableReference};
use datafusion_expr::logical_plan::{LogicalPlan, LogicalPlanBuilder};
use datafusion_expr::utils::find_column_exprs;
use datafusion_expr::TableSource;
use datafusion_expr::{col, AggregateUDF, Expr, ScalarUDF, SubqueryAlias};

use crate::utils::{make_decimal_type, normalize_ident};

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

    /// Get configuration options
    fn options(&self) -> &ConfigOptions;
}

/// SQL parser options
#[derive(Debug, Default)]
pub struct ParserOptions {
    pub parse_float_as_decimal: bool,
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
    pub(crate) schema_provider: &'a S,
    pub(crate) options: ParserOptions,
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

    pub(crate) fn build_schema(&self, columns: Vec<SQLColumnDef>) -> Result<Schema> {
        let mut fields = Vec::with_capacity(columns.len());

        for column in columns {
            let data_type = self.convert_simple_data_type(&column.data_type)?;
            let allow_null = column
                .options
                .iter()
                .any(|x| x.option == ColumnOption::Null);
            fields.push(Field::new(
                normalize_ident(column.name),
                data_type,
                allow_null,
            ));
        }

        Ok(Schema::new(fields))
    }

    /// Apply the given TableAlias to the top-level projection.
    pub(crate) fn apply_table_alias(
        &self,
        plan: LogicalPlan,
        alias: TableAlias,
    ) -> Result<LogicalPlan> {
        let apply_name_plan = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            plan,
            normalize_ident(alias.name),
        )?);

        self.apply_expr_alias(apply_name_plan, alias.columns)
    }

    pub(crate) fn apply_expr_alias(
        &self,
        plan: LogicalPlan,
        idents: Vec<Ident>,
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
                .project(fields.iter().zip(idents.into_iter()).map(|(field, ident)| {
                    col(field.name()).alias(normalize_ident(ident))
                }))?
                .build()
        }
    }

    /// Validate the schema provides all of the columns referenced in the expressions.
    pub(crate) fn validate_schema_satisfies_exprs(
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

    pub(crate) fn convert_data_type(&self, sql_type: &SQLDataType) -> Result<DataType> {
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
                    self.schema_provider.options().execution.time_zone.clone()
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
                        "Unsupported SQL type {sql_type:?}"
                    )))
                }
            }
            SQLDataType::Numeric(exact_number_info)
            | SQLDataType::Decimal(exact_number_info) => {
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
                "Unsupported SQL type {sql_type:?}"
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
pub(crate) fn idents_to_table_reference(
    idents: Vec<Ident>,
) -> Result<OwnedTableReference> {
    struct IdentTaker(Vec<Ident>);
    /// take the next identifier from the back of idents, panic'ing if
    /// there are none left
    impl IdentTaker {
        fn take(&mut self) -> String {
            let ident = self.0.pop().expect("no more identifiers");
            normalize_ident(ident)
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
            format!(r#"{} = '{}'"#, column_name, normalize_ident(ident.clone()))
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}
