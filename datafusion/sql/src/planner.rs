// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`SqlToRel`]: SQL Query Planner (produces [`LogicalPlan`] from SQL AST)
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::vec;

use crate::ast::{
    AngleArguments, ArrayType, ColumnDef as SQLColumnDef, ColumnOption,
    DataType as SQLDataType, DataTypeSuffix, Ident, ObjectName, TableAlias,
};
use crate::utils::make_decimal_type;
use arrow::datatypes::*;
use arrow_schema::extension::{Json, Uuid};
use datafusion_common::TableReference;
use datafusion_common::config::SqlParserOptions;
use datafusion_common::datatype::{DataTypeExt, FieldExt};
use datafusion_common::error::add_possible_columns_to_diag;
use datafusion_common::{DFSchema, DataFusionError, Result, not_impl_err, plan_err};
use datafusion_common::{
    DFSchemaRef, Diagnostic, SchemaError, field_not_found, internal_err,
    plan_datafusion_err,
};
use datafusion_expr::Expr;
use datafusion_expr::logical_plan::{LogicalPlan, LogicalPlanBuilder};
pub use datafusion_expr::planner::ContextProvider;
use datafusion_expr::utils::find_column_exprs;

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
            _ => plan_err!(
                "Unknown null ordering: Expected one of 'nulls_first', 'nulls_last', 'nulls_min' or 'nulls_max'. Got {s}"
            ),
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

    pub fn normalize(&self, ident: Ident<'_>) -> String {
        if self.normalize {
            crate::utils::normalize_ident(ident)
        } else {
            ident.value().into_owned()
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
#[derive(Debug, Clone)]
pub struct PlannerContext {
    /// Data types for numbered parameters ($1, $2, etc), if supplied
    /// in `PREPARE` statement
    prepare_param_data_types: Arc<Vec<Option<FieldRef>>>,
    /// Map of CTE name to logical plan of the WITH clause.
    /// Use `Arc<LogicalPlan>` to allow cheap cloning
    ctes: HashMap<String, Arc<LogicalPlan>>,

    /// The queries schemas of outer query relations, used to resolve the outer referenced
    /// columns in subquery (recursive aware)
    outer_queries_schemas_stack: Vec<DFSchemaRef>,
    /// The joined schemas of all FROM clauses planned so far. When planning LATERAL
    /// FROM clauses, this should become a suffix of the `outer_query_schema`.
    outer_from_schema: Option<DFSchemaRef>,
    /// The query schema defined by the table
    create_table_schema: Option<DFSchemaRef>,
    /// When planning non-first queries in a set expression
    /// (UNION/INTERSECT/EXCEPT), holds the schema of the left-most query.
    /// Used to alias duplicate expressions to match the left side's field names.
    set_expr_left_schema: Option<DFSchemaRef>,
    /// The parameters of all lambdas seen so far
    lambda_parameters: HashMap<String, FieldRef>,
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
            outer_queries_schemas_stack: vec![],
            outer_from_schema: None,
            create_table_schema: None,
            set_expr_left_schema: None,
            lambda_parameters: HashMap::new(),
        }
    }

    /// Update the PlannerContext with provided prepare_param_data_types
    pub fn with_prepare_param_data_types(
        mut self,
        prepare_param_data_types: Vec<Option<FieldRef>>,
    ) -> Self {
        self.prepare_param_data_types = prepare_param_data_types.into();
        self
    }

    /// Return the stack of outer relations' schemas, the outer most
    /// relation are at the first entry
    pub fn outer_queries_schemas(&self) -> &[DFSchemaRef] {
        &self.outer_queries_schemas_stack
    }

    /// Return an iterator of the subquery relations' schemas, innermost
    /// relation is returned first.
    ///
    /// This order corresponds to the order of resolution when looking up column
    /// references in subqueries, which start from the innermost relation and
    /// then look up the outer relations one by one until a match is found or no
    /// more outer relation exist.
    ///
    /// NOTE this is *REVERSED* order of [`Self::outer_queries_schemas`]
    ///
    /// This is useful to resolve the column reference in the subquery by
    /// looking up the outer query schemas one by one.
    pub fn outer_schemas_iter(&self) -> impl Iterator<Item = &DFSchemaRef> {
        self.outer_queries_schemas_stack.iter().rev()
    }

    /// Sets the outer query schema, returning the existing one, if
    /// any
    pub fn append_outer_query_schema(&mut self, schema: DFSchemaRef) {
        self.outer_queries_schemas_stack.push(schema);
    }

    /// The schema of the adjacent outer relation
    pub fn latest_outer_query_schema(&self) -> Option<&DFSchemaRef> {
        self.outer_queries_schemas_stack.last()
    }

    /// Remove the schema of the adjacent outer relation
    pub fn pop_outer_query_schema(&mut self) -> Option<DFSchemaRef> {
        self.outer_queries_schemas_stack.pop()
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
        }
        Ok(())
    }

    /// Return the types of parameters (`$1`, `$2`, etc) if known
    pub fn prepare_param_data_types(&self) -> &[Option<FieldRef>] {
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

    pub fn lambda_parameters(&self) -> &HashMap<String, FieldRef> {
        &self.lambda_parameters
    }

    pub fn with_lambda_parameters(
        mut self,
        parameters: impl IntoIterator<Item = FieldRef>,
    ) -> Self {
        self.lambda_parameters
            .extend(parameters.into_iter().map(|f| (f.name().clone(), f)));

        self
    }

    /// Remove the plan of CTE / Subquery for the specified name
    pub(super) fn remove_cte(&mut self, cte_name: &str) {
        self.ctes.remove(cte_name);
    }

    /// Sets the left-most set expression schema, returning the previous value
    pub(super) fn set_set_expr_left_schema(
        &mut self,
        schema: Option<DFSchemaRef>,
    ) -> Option<DFSchemaRef> {
        std::mem::replace(&mut self.set_expr_left_schema, schema)
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
    warnings: Mutex<Vec<Diagnostic>>,
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
            warnings: Mutex::new(vec![]),
        }
    }

    pub(crate) fn add_warning(&self, warning: Diagnostic) {
        self.warnings
            .lock()
            .expect("warning diagnostic lock poisoned")
            .push(warning);
    }

    /// Drain and return non-fatal warnings collected during SQL planning.
    pub fn take_warnings(&self) -> Vec<Diagnostic> {
        std::mem::take(
            &mut self
                .warnings
                .lock()
                .expect("warning diagnostic lock poisoned"),
        )
    }

    pub fn build_schema(&self, columns: Vec<SQLColumnDef<'_>>) -> Result<Schema> {
        let mut fields = Vec::with_capacity(columns.len());

        for column in columns {
            let data_type = self.convert_data_type_to_field(&column.data_type)?;
            let not_nullable = column_options(&column.options)
                .any(|option| matches!(option, ColumnOption::NotNull));
            fields.push(
                data_type
                    .as_ref()
                    .clone()
                    .with_name(self.ident_normalizer.normalize(column.name))
                    .with_nullable(!not_nullable),
            );
        }

        Ok(Schema::new(fields))
    }

    /// Returns a vector of (column_name, default_expr) pairs
    pub(super) fn build_column_defaults(
        &self,
        columns: &[SQLColumnDef<'_>],
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
                column_options(&column.options).find_map(|option| match option {
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
        alias: TableAlias<'_>,
    ) -> Result<LogicalPlan> {
        let idents = alias.columns;
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
        idents: Vec<Ident<'_>>,
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
            let columns = plan.schema().columns();
            LogicalPlanBuilder::from(plan)
                .project(columns.into_iter().zip(idents).map(|(col, ident)| {
                    Expr::Column(col).alias(self.ident_normalizer.normalize(ident))
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
                                    col.name, relation
                                ),
                                col.spans().first(),
                            )
                        } else {
                            Diagnostic::new_error(
                                format!("column '{}' not found", col.name),
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

    pub(crate) fn convert_data_type_to_field(
        &self,
        sql_type: &SQLDataType<'_>,
    ) -> Result<FieldRef> {
        // First check if any of the registered type_planner can handle this type
        if let Some(type_planner) = self.context_provider.get_type_planner()
            && let Some(data_type) = type_planner.plan_type_field(sql_type)?
        {
            return Ok(data_type);
        }

        // `INT[]`, `INT[3]` and `INT ARRAY`: the element type is the same type
        // without its last dimension.
        match &sql_type.array {
            ArrayType::None => {}
            ArrayType::Brackets(dimensions) => {
                let mut element = sql_type.clone();
                let mut inner = dimensions.clone();
                let size = inner.pop().flatten();
                element.array = if inner.is_empty() {
                    ArrayType::None
                } else {
                    ArrayType::Brackets(inner)
                };
                return self.array_field(&element, size.as_deref());
            }
            ArrayType::Keyword(size) => {
                let mut element = sql_type.clone();
                element.array = ArrayType::None;
                return self.array_field(&element, size.as_deref());
            }
        }

        let name = type_name(sql_type);
        match name.as_str() {
            // Canonical Arrow extension types
            "UUID" => Ok(Arc::new(
                Field::new("", DataType::FixedSizeBinary(16), true)
                    .with_extension_type(Uuid),
            )),
            "JSON" => Ok(Arc::new(
                Field::new("", DataType::Utf8, true).with_extension_type(Json::default()),
            )),
            "ARRAY" => match sql_type.angle.as_deref() {
                Some(
                    AngleArguments::Types(types) | AngleArguments::ParenTypes(types),
                ) if types.len() == 1 => {
                    // Arrays may be multi-dimensional.
                    Ok(self.convert_data_type_to_field(&types[0])?.into_list())
                }
                _ => not_impl_err!("Arrays with unspecified type is not supported"),
            },
            _ => Ok(self
                .convert_simple_data_type(sql_type)?
                .into_nullable_field_ref()),
        }
    }

    /// A list of `element`, of fixed size if `size` is given.
    fn array_field(
        &self,
        element: &SQLDataType<'_>,
        size: Option<&str>,
    ) -> Result<FieldRef> {
        let inner_field = self.convert_data_type_to_field(element)?;
        match size {
            Some(size) => {
                let array_size: i32 = size.parse().map_err(|_| {
                    plan_datafusion_err!(
                        "Array size must be a positive 32 bit integer, got {size}"
                    )
                })?;
                Ok(inner_field.into_fixed_size_list(array_size))
            }
            None => Ok(inner_field.into_list()),
        }
    }

    fn convert_simple_data_type(&self, sql_type: &SQLDataType<'_>) -> Result<DataType> {
        let unsupported = || not_impl_err!("Unsupported SQL type {sql_type}");
        if sql_type.angle.is_some()
            && !matches!(type_name(sql_type).as_str(), "STRUCT" | "MAP")
        {
            return unsupported();
        }
        let unsigned = sql_type.suffix == DataTypeSuffix::Unsigned;
        let suffix = if unsigned {
            DataTypeSuffix::None
        } else {
            sql_type.suffix
        };
        let args = &sql_type.args;
        let string_type = || {
            if self.options.map_string_types_to_utf8view {
                DataType::Utf8View
            } else {
                DataType::Utf8
            }
        };
        let name = type_name(sql_type);
        if unsigned
            && !matches!(
                name.as_str(),
                "TINYINT"
                    | "SMALLINT"
                    | "INT2"
                    | "INT"
                    | "INTEGER"
                    | "INT4"
                    | "BIGINT"
                    | "INT8"
            )
        {
            return unsupported();
        }
        match (name.as_str(), suffix) {
            ("BOOLEAN" | "BOOL", DataTypeSuffix::None) => Ok(DataType::Boolean),
            ("TINYINT", DataTypeSuffix::None) if unsigned => Ok(DataType::UInt8),
            ("TINYINT", DataTypeSuffix::None) => Ok(DataType::Int8),
            ("UTINYINT", DataTypeSuffix::None) => Ok(DataType::UInt8),
            ("SMALLINT" | "INT2", DataTypeSuffix::None) if unsigned => {
                Ok(DataType::UInt16)
            }
            ("SMALLINT" | "INT2", DataTypeSuffix::None) => Ok(DataType::Int16),
            ("USMALLINT", DataTypeSuffix::None) => Ok(DataType::UInt16),
            ("INT" | "INTEGER" | "INT4", DataTypeSuffix::None) if unsigned => {
                Ok(DataType::UInt32)
            }
            ("INT" | "INTEGER" | "INT4", DataTypeSuffix::None) => Ok(DataType::Int32),
            ("BIGINT" | "INT8", DataTypeSuffix::None) if unsigned => Ok(DataType::UInt64),
            ("BIGINT" | "INT8", DataTypeSuffix::None) => Ok(DataType::Int64),
            ("UBIGINT", DataTypeSuffix::None) => Ok(DataType::UInt64),
            ("VARCHAR" | "NVARCHAR", DataTypeSuffix::None)
            | ("CHARACTER" | "CHAR", DataTypeSuffix::Varying) => {
                match (args.is_empty(), self.options.support_varchar_with_length) {
                    (false, false) => plan_err!(
                        "does not support Varchar with length, \
                    please set `support_varchar_with_length` to be true"
                    ),
                    _ => Ok(string_type()),
                }
            }
            ("FLOAT", DataTypeSuffix::None) => Ok(DataType::Float32),
            ("REAL" | "FLOAT4", DataTypeSuffix::None) if args.is_empty() => {
                Ok(DataType::Float32)
            }
            ("DOUBLE", DataTypeSuffix::None) if !args.is_empty() => not_impl_err!(
                "Unsupported SQL type (precision/scale not supported) {sql_type}"
            ),
            ("DOUBLE", DataTypeSuffix::None | DataTypeSuffix::Precision)
            | ("FLOAT8", DataTypeSuffix::None)
                if args.is_empty() =>
            {
                Ok(DataType::Float64)
            }
            ("CHAR" | "TEXT" | "STRING", DataTypeSuffix::None) => Ok(string_type()),
            ("TIMESTAMP" | "TIMESTAMPTZ" | "DATETIME", _) => {
                let with_time_zone = name == "TIMESTAMPTZ"
                    || (name == "TIMESTAMP" && suffix == DataTypeSuffix::WithTimeZone);
                if !matches!(
                    suffix,
                    DataTypeSuffix::None
                        | DataTypeSuffix::WithTimeZone
                        | DataTypeSuffix::WithoutTimeZone
                ) || (name == "DATETIME" && suffix != DataTypeSuffix::None)
                {
                    return unsupported();
                }
                let precision = match args.as_slice() {
                    [] => None,
                    [precision] => Some(precision.parse::<u64>().map_err(|_| {
                        plan_datafusion_err!("Invalid precision in {sql_type}")
                    })?),
                    _ => return unsupported(),
                };
                let precision = match precision {
                    Some(0) => TimeUnit::Second,
                    Some(3) => TimeUnit::Millisecond,
                    Some(6) => TimeUnit::Microsecond,
                    None | Some(9) => TimeUnit::Nanosecond,
                    _ => return unsupported(),
                };
                let tz = if with_time_zone {
                    // Timestamp With Time Zone
                    // INPUT : [SQLDataType]   TimestampTz + [Config] Time Zone
                    // OUTPUT: [ArrowDataType] Timestamp<TimeUnit, Time Zone>
                    //
                    // Note that the configured time zone is an `Option` that is
                    // unset by default, so by default this yields the
                    // timezone-naive `Timestamp<TimeUnit, None>`. Whether that
                    // should remain the behavior is tracked in
                    // https://github.com/apache/datafusion/issues/25166
                    self.context_provider
                        .options()
                        .execution
                        .time_zone
                        .as_ref()
                        .map(|tz| tz.as_str().to_string())
                } else {
                    // Timestamp Without Time zone
                    None
                };
                Ok(DataType::Timestamp(precision, tz.map(Into::into)))
            }
            ("DATE", DataTypeSuffix::None) => Ok(DataType::Date32),
            ("TIME", DataTypeSuffix::None | DataTypeSuffix::WithoutTimeZone)
                if args.is_empty() =>
            {
                Ok(DataType::Time64(TimeUnit::Nanosecond))
            }
            ("NUMERIC" | "DECIMAL", DataTypeSuffix::None) => {
                let number = |text: &str| {
                    text.parse::<i64>()
                        .map_err(|_| plan_datafusion_err!("Invalid number in {sql_type}"))
                };
                let (precision, scale) = match args.as_slice() {
                    [] => (None, None),
                    [precision] => (Some(number(precision)? as u64), None),
                    [precision, scale] => {
                        (Some(number(precision)? as u64), Some(number(scale)? as u64))
                    }
                    _ => return unsupported(),
                };
                make_decimal_type(precision, scale)
            }
            ("BYTEA", DataTypeSuffix::None) => Ok(DataType::Binary),
            ("BLOB" | "BINARY" | "VARBINARY" | "BYTES", DataTypeSuffix::None)
                if args.is_empty() =>
            {
                Ok(DataType::Binary)
            }
            ("INTERVAL", DataTypeSuffix::None) if args.is_empty() => {
                Ok(DataType::Interval(IntervalUnit::MonthDayNano))
            }
            ("STRUCT", DataTypeSuffix::None) => {
                let fields = match sql_type.angle.as_deref() {
                    Some(
                        AngleArguments::Fields(fields)
                        | AngleArguments::ParenFields(fields),
                    ) => fields,
                    _ => return unsupported(),
                };
                let fields = fields
                    .iter()
                    .enumerate()
                    .map(|(idx, sql_struct_field)| {
                        let field =
                            self.convert_data_type_to_field(&sql_struct_field.data_type)?;
                        let field_name = match &sql_struct_field.name {
                            Some(ident) => self.ident_normalizer.normalize(ident.clone()),
                            None => format!("c{idx}"),
                        };
                        Ok(field.as_ref().clone().with_name(field_name))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(DataType::Struct(Fields::from(fields)))
            }
            ("MAP", DataTypeSuffix::None) => {
                let [key_type, value_type] = match sql_type.angle.as_deref() {
                    Some(
                        AngleArguments::Types(types) | AngleArguments::ParenTypes(types),
                    ) if types.len() == 2 => [&types[0], &types[1]],
                    _ => return unsupported(),
                };
                let key_field = Arc::new(Field::new(
                    "key",
                    self.convert_data_type_to_field(key_type)?
                        .data_type()
                        .clone(),
                    false,
                ));
                let value_field = Arc::new(Field::new(
                    "value",
                    self.convert_data_type_to_field(value_type)?
                        .data_type()
                        .clone(),
                    true,
                ));
                let entries = DataType::Struct(Fields::from([key_field, value_field]));
                Ok(DataType::Map(
                    Arc::new(Field::new("entries", entries, false)),
                    false,
                ))
            }
            _ => unsupported(),
        }
    }

    pub(crate) fn object_name_to_table_reference(
        &self,
        object_name: ObjectName<'_>,
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
    object_name: ObjectName<'_>,
    enable_normalization: bool,
) -> Result<TableReference> {
    idents_to_table_reference(object_name.parts, enable_normalization)
}

struct IdentTaker<'a> {
    normalizer: IdentNormalizer,
    idents: Vec<Ident<'a>>,
}

/// Take the next identifier from the back of idents, panic'ing if
/// there are none left
impl<'a> IdentTaker<'a> {
    fn new(idents: Vec<Ident<'a>>, enable_normalization: bool) -> Self {
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
impl std::fmt::Display for IdentTaker<'_> {
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
    idents: Vec<Ident<'_>>,
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
    sql_table_name: &ObjectName<'_>,
    enable_normalization: bool,
) -> Result<String> {
    let columns = vec!["table_name", "table_schema", "table_catalog"].into_iter();
    let normalizer = IdentNormalizer::new(enable_normalization);
    Ok(sql_table_name
        .parts
        .iter()
        .rev()
        .zip(columns)
        .map(|(ident, column_name)| {
            format!(
                r#"{} = '{}'"#,
                column_name,
                normalizer.normalize(ident.clone())
            )
        })
        .collect::<Vec<_>>()
        .join(" AND "))
}

/// The upper-case name of a type, or the qualified name for a qualified type.
fn type_name(sql_type: &SQLDataType<'_>) -> String {
    if !sql_type.qualifier.is_empty() {
        return sql_type.to_string();
    }
    sql_type.name.text.to_ascii_uppercase()
}

/// The options of a column, looking through `CONSTRAINT name`.
pub(crate) fn column_options<'o, 'a>(
    options: &'o [ColumnOption<'a>],
) -> impl Iterator<Item = &'o ColumnOption<'a>> {
    options.iter().map(|option| {
        let mut option = option;
        while let ColumnOption::Named { option: inner, .. } = option {
            option = inner;
        }
        option
    })
}
