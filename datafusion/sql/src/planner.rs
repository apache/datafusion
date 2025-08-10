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

//! [`SqlToRel`]: SQL Query Planner (produces [`LogicalPlan`] from SQL AST)
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::vec;

use arrow::datatypes::*;
use datafusion_common::config::SqlParserOptions;
use datafusion_common::error::add_possible_columns_to_diag;
use datafusion_common::TableReference;
use datafusion_common::{
    field_not_found, internal_err, plan_datafusion_err, DFSchemaRef, Diagnostic,
    SchemaError,
};
use datafusion_common::{not_impl_err, plan_err, DFSchema, DataFusionError, Result};
use datafusion_expr::logical_plan::{LogicalPlan, LogicalPlanBuilder};
use datafusion_expr::utils::find_column_exprs;
use datafusion_expr::{col, Expr};
use sqlparser::ast::{ArrayElemTypeDef, ExactNumberInfo, TimezoneInfo};
use sqlparser::ast::{ColumnDef as SQLColumnDef, ColumnOption};
use sqlparser::ast::{DataType as SQLDataType, Ident, ObjectName, TableAlias};

use crate::utils::make_decimal_type;
pub use datafusion_expr::planner::ContextProvider;

/// SQL parser options
#[derive(Debug, Clone, Copy)]
pub struct ParserOptions {
    /// Whether to parse float as decimal.
    pub parse_float_as_decimal: bool,
    /// Whether to normalize identifiers.
    pub enable_ident_normalization: bool,
    /// Whether to support varchar with length.
    pub support_varchar_with_length: bool,
    /// Whether to normalize options value.
    pub enable_options_value_normalization: bool,
    /// Whether to collect spans
    pub collect_spans: bool,
    /// Whether string types (VARCHAR, CHAR, Text, and String) are mapped to `Utf8View` during SQL planning.
    pub map_string_types_to_utf8view: bool,
    /// Default null ordering for sorting expressions.
    pub default_null_ordering: NullOrdering,
}

impl ParserOptions {
    /// Creates a new `ParserOptions` instance with default values.
    ///
    /// # Examples
    ///
    /// ```
    /// use datafusion_sql::planner::ParserOptions;
    /// let opts = ParserOptions::new();
    /// assert_eq!(opts.parse_float_as_decimal, false);
    /// assert_eq!(opts.enable_ident_normalization, true);
    /// ```
    pub fn new() -> Self {
        Self {
            parse_float_as_decimal: false,
            enable_ident_normalization: true,
            support_varchar_with_length: true,
            map_string_types_to_utf8view: true,
            enable_options_value_normalization: false,
            collect_spans: false,
            // By default, `nulls_max` is used to follow Postgres's behavior.
            // postgres rule: https://www.postgresql.org/docs/current/queries-order.html
            default_null_ordering: NullOrdering::NullsMax,
        }
    }

    /// Sets the `parse_float_as_decimal` option.
    ///
    /// # Examples
    ///
    /// ```
    /// use datafusion_sql::planner::ParserOptions;
    /// let opts = ParserOptions::new().with_parse_float_as_decimal(true);
    /// assert_eq!(opts.parse_float_as_decimal, true);
    /// ```
    pub fn with_parse_float_as_decimal(mut self, value: bool) -> Self {
        self.parse_float_as_decimal = value;
        self
    }

    /// Sets the `enable_ident_normalization` option.
    ///
    /// # Examples
    ///
    /// ```
    /// use datafusion_sql::planner::ParserOptions;
    /// let opts = ParserOptions::new().with_enable_ident_normalization(false);
    /// assert_eq!(opts.enable_ident_normalization, false);
    /// ```
    pub fn with_enable_ident_normalization(mut self, value: bool) -> Self {
        self.enable_ident_normalization = value;
        self
    }

    /// Sets the `support_varchar_with_length` option.
    pub fn with_support_varchar_with_length(mut self, value: bool) -> Self {
        self.support_varchar_with_length = value;
        self
    }

    /// Sets the `map_string_types_to_utf8view` option.
    pub fn with_map_string_types_to_utf8view(mut self, value: bool) -> Self {
        self.map_string_types_to_utf8view = value;
        self
    }

    /// Sets the `enable_options_value_normalization` option.
    pub fn with_enable_options_value_normalization(mut self, value: bool) -> Self {
        self.enable_options_value_normalization = value;
        self
    }

    /// Sets the `collect_spans` option.
    pub fn with_collect_spans(mut self, value: bool) -> Self {
        self.collect_spans = value;
        self
    }

    /// Sets the `default_null_ordering` option.
    pub fn with_default_null_ordering(mut self, value: NullOrdering) -> Self {
        self.default_null_ordering = value;
        self
    }
}

impl Default for ParserOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl From<&SqlParserOptions> for ParserOptions {
    fn from(options: &SqlParserOptions) -> Self {
        Self {
            parse_float_as_decimal: options.parse_float_as_decimal,
            enable_ident_normalization: options.enable_ident_normalization,
            support_varchar_with_length: options.support_varchar_with_length,
            map_string_types_to_utf8view: options.map_string_types_to_utf8view,
            enable_options_value_normalization: options
                .enable_options_value_normalization,
            collect_spans: options.collect_spans,
            default_null_ordering: options.default_null_ordering.as_str().into(),
        }
    }
}

/// Represents the null ordering for sorting expressions.
#[derive(Debug, Clone, Copy)]
pub enum NullOrdering {
    /// Nulls appear last in ascending order.
    NullsMax,
    /// Nulls appear first in descending order.
    NullsMin,
    /// Nulls appear first.
    NullsFirst,
    /// Nulls appear last.
    NullsLast,
}

impl NullOrdering {
    /// Evaluates the null ordering based on the given ascending flag.
    ///
    /// # Returns
    /// * `true` if nulls should appear first.
    /// * `false` if nulls should appear last.
    pub fn nulls_first(&self, asc: bool) -> bool {
        match self {
            Self::NullsMax => !asc,
            Self::NullsMin => asc,
            Self::NullsFirst => true,
            Self::NullsLast => false,
        }
    }
}

impl FromStr for NullOrdering {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "nulls_max" => Ok(Self::NullsMax),
            "nulls_min" => Ok(Self::NullsMin),
            "nulls_first" => Ok(Self::NullsFirst),
            "nulls_last" => Ok(Self::NullsLast),
            _ => plan_err!("Unknown null ordering: Expected one of 'nulls_first', 'nulls_last', 'nulls_min' or 'nulls_max'. Got {s}"),
        }
    }
}

impl From<&str> for NullOrdering {
    fn from(s: &str) -> Self {
        Self::from_str(s).unwrap_or(Self::NullsMax)
    }
}

/// Ident Normalizer
#[derive(Debug)]
pub struct IdentNormalizer {
    normalize: bool,
}

impl Default for IdentNormalizer {
    fn default() -> Self {
        Self { normalize: true }
    }
}

impl IdentNormalizer {
    pub fn new(normalize: bool) -> Self {
        Self { normalize }
    }

    pub fn normalize(&self, ident: Ident) -> String {
        if self.normalize {
            crate::utils::normalize_ident(ident)
        } else {
            ident.value
        }
    }
}

/// Struct to store the states used by the Planner. The Planner will leverage the states
/// to resolve CTEs, Views, subqueries and PREPARE statements. The states include
/// Common Table Expression (CTE) provided with WITH clause and
/// Parameter Data Types provided with PREPARE statement and the query schema of the
/// outer query plan.
///
/// # Cloning
///
/// Only the `ctes` are truly cloned when the `PlannerContext` is cloned.
/// This helps resolve scoping issues of CTEs.
/// By using cloning, a subquery can inherit CTEs from the outer query
/// and can also define its own private CTEs without affecting the outer query.
///
#[derive(Debug, Clone)]
pub struct PlannerContext {
    /// Data types for numbered parameters ($1, $2, etc), if supplied
    /// in `PREPARE` statement
    prepare_param_data_types: Arc<Vec<DataType>>,
    /// Map of CTE name to logical plan of the WITH clause.
    /// Use `Arc<LogicalPlan>` to allow cheap cloning
    ctes: HashMap<String, Arc<LogicalPlan>>,
    /// The query schema of the outer query plan, used to resolve the columns in subquery
    outer_query_schema: Option<DFSchemaRef>,
    /// The joined schemas of all FROM clauses planned so far. When planning LATERAL
    /// FROM clauses, this should become a suffix of the `outer_query_schema`.
    outer_from_schema: Option<DFSchemaRef>,
    /// The query schema defined by the table
    create_table_schema: Option<DFSchemaRef>,
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
            prepare_param_data_types: Arc::new(vec![]),
            ctes: HashMap::new(),
            outer_query_schema: None,
            outer_from_schema: None,
            create_table_schema: None,
        }
    }

    /// Update the PlannerContext with provided prepare_param_data_types
    pub fn with_prepare_param_data_types(
        mut self,
        prepare_param_data_types: Vec<DataType>,
    ) -> Self {
        self.prepare_param_data_types = prepare_param_data_types.into();
        self
    }

    // Return a reference to the outer query's schema
    pub fn outer_query_schema(&self) -> Option<&DFSchema> {
        self.outer_query_schema.as_ref().map(|s| s.as_ref())
    }

    /// Sets the outer query schema, returning the existing one, if
    /// any
    pub fn set_outer_query_schema(
        &mut self,
        mut schema: Option<DFSchemaRef>,
    ) -> Option<DFSchemaRef> {
        std::mem::swap(&mut self.outer_query_schema, &mut schema);
        schema
    }

    pub fn set_table_schema(
        &mut self,
        mut schema: Option<DFSchemaRef>,
    ) -> Option<DFSchemaRef> {
        std::mem::swap(&mut self.create_table_schema, &mut schema);
        schema
    }

    pub fn table_schema(&self) -> Option<DFSchemaRef> {
        self.create_table_schema.clone()
    }

    // Return a clone of the outer FROM schema
    pub fn outer_from_schema(&self) -> Option<Arc<DFSchema>> {
        self.outer_from_schema.clone()
    }

    /// Sets the outer FROM schema, returning the existing one, if any
    pub fn set_outer_from_schema(
        &mut self,
        mut schema: Option<DFSchemaRef>,
    ) -> Option<DFSchemaRef> {
        std::mem::swap(&mut self.outer_from_schema, &mut schema);
        schema
    }

    /// Extends the FROM schema, returning the existing one, if any
    pub fn extend_outer_from_schema(&mut self, schema: &DFSchemaRef) -> Result<()> {
        match self.outer_from_schema.as_mut() {
            Some(from_schema) => Arc::make_mut(from_schema).merge(schema),
            None => self.outer_from_schema = Some(Arc::clone(schema)),
        };
        Ok(())
    }

    /// Return the types of parameters (`$1`, `$2`, etc) if known
    pub fn prepare_param_data_types(&self) -> &[DataType] {
        &self.prepare_param_data_types
    }

    /// Returns true if there is a Common Table Expression (CTE) /
    /// Subquery for the specified name
    pub fn contains_cte(&self, cte_name: &str) -> bool {
        self.ctes.contains_key(cte_name)
    }

    /// Inserts a LogicalPlan for the Common Table Expression (CTE) /
    /// Subquery for the specified name
    pub fn insert_cte(&mut self, cte_name: impl Into<String>, plan: LogicalPlan) {
        let cte_name = cte_name.into();
        self.ctes.insert(cte_name, Arc::new(plan));
    }

    /// Return a plan for the Common Table Expression (CTE) / Subquery for the
    /// specified name
    pub fn get_cte(&self, cte_name: &str) -> Option<&LogicalPlan> {
        self.ctes.get(cte_name).map(|cte| cte.as_ref())
    }

    /// Remove the plan of CTE / Subquery for the specified name
    pub(super) fn remove_cte(&mut self, cte_name: &str) {
        self.ctes.remove(cte_name);
    }
}

/// SQL query planner and binder
///
/// This struct is used to convert a SQL AST into a [`LogicalPlan`].
///
/// You can control the behavior of the planner by providing [`ParserOptions`].
///
/// It performs the following tasks:
///
/// 1. Name and type resolution (called "binding" in other systems). This
///    phase looks up table and column names using the [`ContextProvider`].
/// 2. Mechanical translation of the AST into a [`LogicalPlan`].
///
/// It does not perform type coercion, or perform optimization, which are done
/// by subsequent passes.
///
/// Key interfaces are:
/// * [`Self::sql_statement_to_plan`]: Convert a statement
///   (e.g. `SELECT ...`) into a [`LogicalPlan`]
/// * [`Self::sql_to_expr`]: Convert an expression (e.g. `1 + 2`) into an [`Expr`]
pub struct SqlToRel<'a, S: ContextProvider> {
    pub(crate) context_provider: &'a S,
    pub(crate) options: ParserOptions,
    pub(crate) ident_normalizer: IdentNormalizer,
}

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    /// Create a new query planner.
    ///
    /// The query planner derives the parser options from the context provider.
    pub fn new(context_provider: &'a S) -> Self {
        let parser_options = ParserOptions::from(&context_provider.options().sql_parser);
        Self::new_with_options(context_provider, parser_options)
    }

    /// Create a new query planner with the given parser options.
    ///
    /// The query planner ignores the parser options from the context provider
    /// and uses the given parser options instead.
    pub fn new_with_options(context_provider: &'a S, options: ParserOptions) -> Self {
        let ident_normalize = options.enable_ident_normalization;

        SqlToRel {
            context_provider,
            options,
            ident_normalizer: IdentNormalizer::new(ident_normalize),
        }
    }

    pub fn build_schema(&self, columns: Vec<SQLColumnDef>) -> Result<Schema> {
        let mut fields = Vec::with_capacity(columns.len());

        for column in columns {
            let data_type = self.convert_data_type(&column.data_type)?;
            let not_nullable = column
                .options
                .iter()
                .any(|x| x.option == ColumnOption::NotNull);
            fields.push(Field::new(
                self.ident_normalizer.normalize(column.name),
                data_type,
                !not_nullable,
            ));
        }

        Ok(Schema::new(fields))
    }

    /// Returns a vector of (column_name, default_expr) pairs
    pub(super) fn build_column_defaults(
        &self,
        columns: &Vec<SQLColumnDef>,
        planner_context: &mut PlannerContext,
    ) -> Result<Vec<(String, Expr)>> {
        let mut column_defaults = vec![];
        // Default expressions are restricted, column references are not allowed
        let empty_schema = DFSchema::empty();
        let error_desc = |e: DataFusionError| match e {
            DataFusionError::SchemaError(ref err, _)
                if matches!(**err, SchemaError::FieldNotFound { .. }) =>
            {
                plan_datafusion_err!(
                    "Column reference is not allowed in the DEFAULT expression : {}",
                    e
                )
            }
            _ => e,
        };

        for column in columns {
            if let Some(default_sql_expr) =
                column.options.iter().find_map(|o| match &o.option {
                    ColumnOption::Default(expr) => Some(expr),
                    _ => None,
                })
            {
                let default_expr = self
                    .sql_to_expr(default_sql_expr.clone(), &empty_schema, planner_context)
                    .map_err(error_desc)?;
                column_defaults.push((
                    self.ident_normalizer.normalize(column.name.clone()),
                    default_expr,
                ));
            }
        }
        Ok(column_defaults)
    }

    /// Apply the given TableAlias to the input plan
    pub(crate) fn apply_table_alias(
        &self,
        plan: LogicalPlan,
        alias: TableAlias,
    ) -> Result<LogicalPlan> {
        let idents = alias.columns.into_iter().map(|c| c.name).collect();
        let plan = self.apply_expr_alias(plan, idents)?;

        LogicalPlanBuilder::from(plan)
            .alias(TableReference::bare(
                self.ident_normalizer.normalize(alias.name),
            ))?
            .build()
    }

    pub(crate) fn apply_expr_alias(
        &self,
        plan: LogicalPlan,
        idents: Vec<Ident>,
    ) -> Result<LogicalPlan> {
        if idents.is_empty() {
            Ok(plan)
        } else if idents.len() != plan.schema().fields().len() {
            plan_err!(
                "Source table contains {} columns but only {} \
                names given as column alias",
                plan.schema().fields().len(),
                idents.len()
            )
        } else {
            let fields = plan.schema().fields().clone();
            LogicalPlanBuilder::from(plan)
                .project(fields.iter().zip(idents.into_iter()).map(|(field, ident)| {
                    col(field.name()).alias(self.ident_normalizer.normalize(ident))
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
                    Some(r) => schema.field_with_qualified_name(r, &col.name).map(|_| ()),
                    None => {
                        if !schema.fields_with_unqualified_name(&col.name).is_empty() {
                            Ok(())
                        } else {
                            Err(field_not_found(
                                col.relation.clone(),
                                col.name.as_str(),
                                schema,
                            ))
                        }
                    }
                }
                .map_err(|err: DataFusionError| match &err {
                    DataFusionError::SchemaError(inner, _)
                        if matches!(
                            inner.as_ref(),
                            SchemaError::FieldNotFound { .. }
                        ) =>
                    {
                        let SchemaError::FieldNotFound {
                            field,
                            valid_fields,
                        } = inner.as_ref()
                        else {
                            unreachable!()
                        };
                        let mut diagnostic = if let Some(relation) = &col.relation {
                            Diagnostic::new_error(
                                format!(
                                    "column '{}' not found in '{}'",
                                    &col.name, relation
                                ),
                                col.spans().first(),
                            )
                        } else {
                            Diagnostic::new_error(
                                format!("column '{}' not found", &col.name),
                                col.spans().first(),
                            )
                        };
                        add_possible_columns_to_diag(
                            &mut diagnostic,
                            field,
                            valid_fields,
                        );
                        err.with_diagnostic(diagnostic)
                    }
                    _ => err,
                }),
                _ => internal_err!("Not a column"),
            })
    }

    pub(crate) fn convert_data_type(&self, sql_type: &SQLDataType) -> Result<DataType> {
        // First check if any of the registered type_planner can handle this type
        if let Some(type_planner) = self.context_provider.get_type_planner() {
            if let Some(data_type) = type_planner.plan_type(sql_type)? {
                return Ok(data_type);
            }
        }

        // If no type_planner can handle this type, use the default conversion
        match sql_type {
            SQLDataType::Array(ArrayElemTypeDef::AngleBracket(inner_sql_type)) => {
                // Arrays may be multi-dimensional.
                let inner_data_type = self.convert_data_type(inner_sql_type)?;
                Ok(DataType::new_list(inner_data_type, true))
            }
            SQLDataType::Array(ArrayElemTypeDef::SquareBracket(
                inner_sql_type,
                maybe_array_size,
            )) => {
                let inner_data_type = self.convert_data_type(inner_sql_type)?;
                if let Some(array_size) = maybe_array_size {
                    Ok(DataType::new_fixed_size_list(
                        inner_data_type,
                        *array_size as i32,
                        true,
                    ))
                } else {
                    Ok(DataType::new_list(inner_data_type, true))
                }
            }
            SQLDataType::Array(ArrayElemTypeDef::None) => {
                not_impl_err!("Arrays with unspecified type is not supported")
            }
            other => self.convert_simple_data_type(other),
        }
    }

    fn convert_simple_data_type(&self, sql_type: &SQLDataType) -> Result<DataType> {
        match sql_type {
            SQLDataType::Boolean | SQLDataType::Bool => Ok(DataType::Boolean),
            SQLDataType::TinyInt(_) => Ok(DataType::Int8),
            SQLDataType::SmallInt(_) | SQLDataType::Int2(_) => Ok(DataType::Int16),
            SQLDataType::Int(_) | SQLDataType::Integer(_) | SQLDataType::Int4(_) => {
                Ok(DataType::Int32)
            }
            SQLDataType::BigInt(_) | SQLDataType::Int8(_) => Ok(DataType::Int64),
            SQLDataType::TinyIntUnsigned(_) => Ok(DataType::UInt8),
            SQLDataType::SmallIntUnsigned(_) | SQLDataType::Int2Unsigned(_) => {
                Ok(DataType::UInt16)
            }
            SQLDataType::IntUnsigned(_)
            | SQLDataType::IntegerUnsigned(_)
            | SQLDataType::Int4Unsigned(_) => Ok(DataType::UInt32),
            SQLDataType::Varchar(length) => {
                match (length, self.options.support_varchar_with_length) {
                    (Some(_), false) => plan_err!(
                        "does not support Varchar with length, \
                    please set `support_varchar_with_length` to be true"
                    ),
                    _ => {
                        if self.options.map_string_types_to_utf8view {
                            Ok(DataType::Utf8View)
                        } else {
                            Ok(DataType::Utf8)
                        }
                    }
                }
            }
            SQLDataType::BigIntUnsigned(_) | SQLDataType::Int8Unsigned(_) => {
                Ok(DataType::UInt64)
            }
            SQLDataType::Float(_) => Ok(DataType::Float32),
            SQLDataType::Real | SQLDataType::Float4 => Ok(DataType::Float32),
            SQLDataType::Double(ExactNumberInfo::None)
            | SQLDataType::DoublePrecision
            | SQLDataType::Float8 => Ok(DataType::Float64),
            SQLDataType::Double(
                ExactNumberInfo::Precision(_) | ExactNumberInfo::PrecisionAndScale(_, _),
            ) => {
                not_impl_err!(
                    "Unsupported SQL type (precision/scale not supported) {sql_type}"
                )
            }
            SQLDataType::Char(_) | SQLDataType::Text | SQLDataType::String(_) => {
                if self.options.map_string_types_to_utf8view {
                    Ok(DataType::Utf8View)
                } else {
                    Ok(DataType::Utf8)
                }
            }
            SQLDataType::Timestamp(precision, tz_info)
                if precision.is_none() || [0, 3, 6, 9].contains(&precision.unwrap()) =>
            {
                let tz = if matches!(tz_info, TimezoneInfo::Tz)
                    || matches!(tz_info, TimezoneInfo::WithTimeZone)
                {
                    // Timestamp With Time Zone
                    // INPUT : [SQLDataType]   TimestampTz + [Config] Time Zone
                    // OUTPUT: [ArrowDataType] Timestamp<TimeUnit, Some(Time Zone)>
                    Some(self.context_provider.options().execution.time_zone.clone())
                } else {
                    // Timestamp Without Time zone
                    None
                };
                let precision = match precision {
                    Some(0) => TimeUnit::Second,
                    Some(3) => TimeUnit::Millisecond,
                    Some(6) => TimeUnit::Microsecond,
                    None | Some(9) => TimeUnit::Nanosecond,
                    _ => unreachable!(),
                };
                Ok(DataType::Timestamp(precision, tz.map(Into::into)))
            }
            SQLDataType::Date => Ok(DataType::Date32),
            SQLDataType::Time(None, tz_info) => {
                if matches!(tz_info, TimezoneInfo::None)
                    || matches!(tz_info, TimezoneInfo::WithoutTimeZone)
                {
                    Ok(DataType::Time64(TimeUnit::Nanosecond))
                } else {
                    // We don't support TIMETZ and TIME WITH TIME ZONE for now
                    not_impl_err!("Unsupported SQL type {sql_type:?}")
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
            SQLDataType::Interval => Ok(DataType::Interval(IntervalUnit::MonthDayNano)),
            SQLDataType::Struct(fields, _) => {
                let fields = fields
                    .iter()
                    .enumerate()
                    .map(|(idx, field)| {
                        let data_type = self.convert_data_type(&field.field_type)?;
                        let field_name = match &field.field_name {
                            Some(ident) => ident.clone(),
                            None => Ident::new(format!("c{idx}")),
                        };
                        Ok(Arc::new(Field::new(
                            self.ident_normalizer.normalize(field_name),
                            data_type,
                            true,
                        )))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(DataType::Struct(Fields::from(fields)))
            }
            SQLDataType::Nvarchar(_)
            | SQLDataType::JSON
            | SQLDataType::Uuid
            | SQLDataType::Binary(_)
            | SQLDataType::Varbinary(_)
            | SQLDataType::Blob(_)
            | SQLDataType::Datetime(_)
            | SQLDataType::Regclass
            | SQLDataType::Custom(_, _)
            | SQLDataType::Array(_)
            | SQLDataType::Enum(_, _)
            | SQLDataType::Set(_)
            | SQLDataType::MediumInt(_)
            | SQLDataType::MediumIntUnsigned(_)
            | SQLDataType::Character(_)
            | SQLDataType::CharacterVarying(_)
            | SQLDataType::CharVarying(_)
            | SQLDataType::CharacterLargeObject(_)
            | SQLDataType::CharLargeObject(_)
            | SQLDataType::Timestamp(_, _)
            | SQLDataType::Time(Some(_), _)
            | SQLDataType::Dec(_)
            | SQLDataType::BigNumeric(_)
            | SQLDataType::BigDecimal(_)
            | SQLDataType::Clob(_)
            | SQLDataType::Bytes(_)
            | SQLDataType::Int64
            | SQLDataType::Float64
            | SQLDataType::JSONB
            | SQLDataType::Unspecified
            | SQLDataType::Int16
            | SQLDataType::Int32
            | SQLDataType::Int128
            | SQLDataType::Int256
            | SQLDataType::UInt8
            | SQLDataType::UInt16
            | SQLDataType::UInt32
            | SQLDataType::UInt64
            | SQLDataType::UInt128
            | SQLDataType::UInt256
            | SQLDataType::Float32
            | SQLDataType::Date32
            | SQLDataType::Datetime64(_, _)
            | SQLDataType::FixedString(_)
            | SQLDataType::Map(_, _)
            | SQLDataType::Tuple(_)
            | SQLDataType::Nested(_)
            | SQLDataType::Union(_)
            | SQLDataType::Nullable(_)
            | SQLDataType::LowCardinality(_)
            | SQLDataType::Trigger
            | SQLDataType::TinyBlob
            | SQLDataType::MediumBlob
            | SQLDataType::LongBlob
            | SQLDataType::TinyText
            | SQLDataType::MediumText
            | SQLDataType::LongText
            | SQLDataType::Bit(_)
            | SQLDataType::BitVarying(_)
            | SQLDataType::Signed
            | SQLDataType::SignedInteger
            | SQLDataType::Unsigned
            | SQLDataType::UnsignedInteger
            | SQLDataType::AnyType
            | SQLDataType::Table(_)
            | SQLDataType::VarBit(_)
            | SQLDataType::GeometricType(_) => {
                not_impl_err!("Unsupported SQL type {sql_type:?}")
            }
        }
    }

    pub(crate) fn object_name_to_table_reference(
        &self,
        object_name: ObjectName,
    ) -> Result<TableReference> {
        object_name_to_table_reference(
            object_name,
            self.options.enable_ident_normalization,
        )
    }
}

/// Create a [`TableReference`] after normalizing the specified ObjectName
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
    enable_normalization: bool,
) -> Result<TableReference> {
    // Use destructure to make it clear no fields on ObjectName are ignored
    let ObjectName(object_name_parts) = object_name;
    let idents = object_name_parts
        .into_iter()
        .map(|object_name_part| {
            object_name_part.as_ident().cloned().ok_or_else(|| {
                plan_datafusion_err!(
                    "Expected identifier, but found: {:?}",
                    object_name_part
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;
    idents_to_table_reference(idents, enable_normalization)
}

struct IdentTaker {
    normalizer: IdentNormalizer,
    idents: Vec<Ident>,
}

/// Take the next identifier from the back of idents, panic'ing if
/// there are none left
impl IdentTaker {
    fn new(idents: Vec<Ident>, enable_normalization: bool) -> Self {
        Self {
            normalizer: IdentNormalizer::new(enable_normalization),
            idents,
        }
    }

    fn take(&mut self) -> String {
        let ident = self.idents.pop().expect("no more identifiers");
        self.normalizer.normalize(ident)
    }

    /// Returns the number of remaining identifiers
    fn len(&self) -> usize {
        self.idents.len()
    }
}

// impl Display for a nicer error message
impl std::fmt::Display for IdentTaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut first = true;
        for ident in self.idents.iter() {
            if !first {
                write!(f, ".")?;
            }
            write!(f, "{ident}")?;
            first = false;
        }

        Ok(())
    }
}

/// Create a [`TableReference`] after normalizing the specified identifier
pub(crate) fn idents_to_table_reference(
    idents: Vec<Ident>,
    enable_normalization: bool,
) -> Result<TableReference> {
    let mut taker = IdentTaker::new(idents, enable_normalization);

    match taker.len() {
        1 => {
            let table = taker.take();
            Ok(TableReference::bare(table))
        }
        2 => {
            let table = taker.take();
            let schema = taker.take();
            Ok(TableReference::partial(schema, table))
        }
        3 => {
            let table = taker.take();
            let schema = taker.take();
            let catalog = taker.take();
            Ok(TableReference::full(catalog, schema, table))
        }
        _ => plan_err!(
            "Unsupported compound identifier '{}'. Expected 1, 2 or 3 parts, got {}",
            taker,
            taker.len()
        ),
    }
}

/// Construct a WHERE qualifier suitable for e.g. information_schema filtering
/// from the provided object identifiers (catalog, schema and table names).
pub fn object_name_to_qualifier(
    sql_table_name: &ObjectName,
    enable_normalization: bool,
) -> Result<String> {
    let columns = vec!["table_name", "table_schema", "table_catalog"].into_iter();
    let normalizer = IdentNormalizer::new(enable_normalization);
    sql_table_name
        .0
        .iter()
        .rev()
        .zip(columns)
        .map(|(object_name_part, column_name)| {
            object_name_part
                .as_ident()
                .map(|ident| {
                    format!(
                        r#"{} = '{}'"#,
                        column_name,
                        normalizer.normalize(ident.clone())
                    )
                })
                .ok_or_else(|| {
                    plan_datafusion_err!(
                        "Expected identifier, but found: {:?}",
                        object_name_part
                    )
                })
        })
        .collect::<Result<Vec<_>>>()
        .map(|parts| parts.join(" AND "))
}
