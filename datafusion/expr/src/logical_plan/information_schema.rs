// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Schema and [`RecordBatch`] builders for the `information_schema` virtual
//! tables and `SHOW` statements.

use crate::TableType;
use arrow::array::ArrayRef;
use arrow::array::builder::{BooleanBuilder, StringBuilder, UInt8Builder, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_common::config::ConfigEntry;
use std::sync::Arc;

/// Returns the Arrow schema of `information_schema.tables` rows.
///
/// Columns are based on <https://www.postgresql.org/docs/current/infoschema-tables.html>.
pub fn tables_schema() -> Schema {
    Schema::new(vec![
        Field::new("table_catalog", DataType::Utf8, false),
        Field::new("table_schema", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("table_type", DataType::Utf8, false),
    ])
}

/// Builds `information_schema.tables` rows.
#[derive(Debug)]
pub struct InformationSchemaTablesBuilder {
    schema: SchemaRef,
    catalog_names: StringBuilder,
    schema_names: StringBuilder,
    table_names: StringBuilder,
    table_types: StringBuilder,
}

impl Default for InformationSchemaTablesBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl InformationSchemaTablesBuilder {
    /// Construct an empty builder.
    pub fn new() -> Self {
        Self {
            schema: Arc::new(tables_schema()),
            catalog_names: StringBuilder::new(),
            schema_names: StringBuilder::new(),
            table_names: StringBuilder::new(),
            table_types: StringBuilder::new(),
        }
    }

    /// Append one row.
    pub fn add_table(
        &mut self,
        catalog_name: impl AsRef<str>,
        schema_name: impl AsRef<str>,
        table_name: impl AsRef<str>,
        table_type: TableType,
    ) {
        // Note: append_value is actually infallible.
        self.catalog_names.append_value(catalog_name.as_ref());
        self.schema_names.append_value(schema_name.as_ref());
        self.table_names.append_value(table_name.as_ref());
        self.table_types.append_value(match table_type {
            TableType::Base => "BASE TABLE",
            TableType::View => "VIEW",
            TableType::Temporary => "LOCAL TEMPORARY",
        });
    }

    /// Finalize the builder into a [`RecordBatch`].
    pub fn finish(&mut self) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.catalog_names.finish()),
                Arc::new(self.schema_names.finish()),
                Arc::new(self.table_names.finish()),
                Arc::new(self.table_types.finish()),
            ],
        )
        .unwrap()
    }
}

/// Returns the Arrow schema of `information_schema.views` rows.
///
/// Columns are based on <https://www.postgresql.org/docs/current/infoschema-views.html>.
pub fn views_schema() -> Schema {
    Schema::new(vec![
        Field::new("table_catalog", DataType::Utf8, false),
        Field::new("table_schema", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("definition", DataType::Utf8, true),
    ])
}

/// Builds `information_schema.views` rows.
#[derive(Debug)]
pub struct InformationSchemaViewBuilder {
    schema: SchemaRef,
    catalog_names: StringBuilder,
    schema_names: StringBuilder,
    table_names: StringBuilder,
    definitions: StringBuilder,
}

impl Default for InformationSchemaViewBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl InformationSchemaViewBuilder {
    /// Construct an empty builder.
    pub fn new() -> Self {
        Self {
            schema: Arc::new(views_schema()),
            catalog_names: StringBuilder::new(),
            schema_names: StringBuilder::new(),
            table_names: StringBuilder::new(),
            definitions: StringBuilder::new(),
        }
    }

    /// Append one row. `definition` is the statement that created the table, if known.
    pub fn add_view(
        &mut self,
        catalog_name: impl AsRef<str>,
        schema_name: impl AsRef<str>,
        table_name: impl AsRef<str>,
        definition: Option<&(impl AsRef<str> + ?Sized)>,
    ) {
        // Note: append_value is actually infallible.
        self.catalog_names.append_value(catalog_name.as_ref());
        self.schema_names.append_value(schema_name.as_ref());
        self.table_names.append_value(table_name.as_ref());
        self.definitions.append_option(definition.as_ref());
    }

    /// Finalize the builder into a [`RecordBatch`].
    pub fn finish(&mut self) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.catalog_names.finish()),
                Arc::new(self.schema_names.finish()),
                Arc::new(self.table_names.finish()),
                Arc::new(self.definitions.finish()),
            ],
        )
        .unwrap()
    }
}

/// Which subset of the `information_schema.columns` columns to produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ColumnsDetail {
    /// `column_name`, `data_type` and `is_nullable`, as output by `DESCRIBE`.
    Describe,
    /// [`Self::Describe`] plus the table's catalog, schema and name, as output
    /// by `SHOW COLUMNS`.
    Basic,
    /// All columns, as output by `SHOW FULL COLUMNS` and `information_schema.columns`.
    Full,
}

/// Returns the Arrow schema of `information_schema.columns` rows at the
/// given [`ColumnsDetail`] level.
///
/// The `Full` columns are based on
/// <https://www.postgresql.org/docs/current/infoschema-columns.html>.
pub fn columns_schema(detail: ColumnsDetail) -> Schema {
    match detail {
        ColumnsDetail::Describe => Schema::new(vec![
            Field::new("column_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("is_nullable", DataType::Utf8, false),
        ]),
        ColumnsDetail::Basic => Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("is_nullable", DataType::Utf8, false),
        ]),
        ColumnsDetail::Full => Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("ordinal_position", DataType::UInt64, false),
            Field::new("column_default", DataType::Utf8, true),
            Field::new("is_nullable", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("character_maximum_length", DataType::UInt64, true),
            Field::new("character_octet_length", DataType::UInt64, true),
            Field::new("numeric_precision", DataType::UInt64, true),
            Field::new("numeric_precision_radix", DataType::UInt64, true),
            Field::new("numeric_scale", DataType::UInt64, true),
            Field::new("datetime_precision", DataType::UInt64, true),
            Field::new("interval_type", DataType::Utf8, true),
        ]),
    }
}

/// Builds `information_schema.columns` rows at a given [`ColumnsDetail`] level.
#[derive(Debug)]
pub struct InformationSchemaColumnsBuilder {
    schema: SchemaRef,
    detail: ColumnsDetail,
    catalog_names: Option<StringBuilder>,
    schema_names: Option<StringBuilder>,
    table_names: Option<StringBuilder>,
    column_names: StringBuilder,
    ordinal_positions: Option<UInt64Builder>,
    column_defaults: Option<StringBuilder>,
    is_nullables: StringBuilder,
    data_types: StringBuilder,
    character_maximum_lengths: Option<UInt64Builder>,
    character_octet_lengths: Option<UInt64Builder>,
    numeric_precisions: Option<UInt64Builder>,
    numeric_precision_radixes: Option<UInt64Builder>,
    numeric_scales: Option<UInt64Builder>,
    datetime_precisions: Option<UInt64Builder>,
    interval_types: Option<StringBuilder>,
}

impl InformationSchemaColumnsBuilder {
    /// Construct an empty builder that will only retain the columns needed
    /// for `detail`.
    pub fn new(detail: ColumnsDetail) -> Self {
        // StringBuilder requires providing an initial capacity, so
        // pick 10 here arbitrarily as this is not performance
        // critical code and the number of tables is unavailable here.
        let default_capacity = 10;

        let with_table_qualifiers =
            matches!(detail, ColumnsDetail::Basic | ColumnsDetail::Full);
        let full = detail == ColumnsDetail::Full;

        Self {
            schema: Arc::new(columns_schema(detail)),
            detail,
            catalog_names: with_table_qualifiers.then(StringBuilder::new),
            schema_names: with_table_qualifiers.then(StringBuilder::new),
            table_names: with_table_qualifiers.then(StringBuilder::new),
            column_names: StringBuilder::new(),
            ordinal_positions: full
                .then(|| UInt64Builder::with_capacity(default_capacity)),
            column_defaults: full.then(StringBuilder::new),
            is_nullables: StringBuilder::new(),
            data_types: StringBuilder::new(),
            character_maximum_lengths: full
                .then(|| UInt64Builder::with_capacity(default_capacity)),
            character_octet_lengths: full
                .then(|| UInt64Builder::with_capacity(default_capacity)),
            numeric_precisions: full
                .then(|| UInt64Builder::with_capacity(default_capacity)),
            numeric_precision_radixes: full
                .then(|| UInt64Builder::with_capacity(default_capacity)),
            numeric_scales: full.then(|| UInt64Builder::with_capacity(default_capacity)),
            datetime_precisions: full
                .then(|| UInt64Builder::with_capacity(default_capacity)),
            interval_types: full.then(StringBuilder::new),
        }
    }

    /// Append one row describing `field`. Values not needed at this builder's
    /// detail level are discarded.
    pub fn add_column(
        &mut self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        field_position: usize,
        field: &Field,
    ) {
        use DataType::*;

        // Note: append_value is actually infallible.
        if let Some(b) = &mut self.catalog_names {
            b.append_value(catalog_name);
        }
        if let Some(b) = &mut self.schema_names {
            b.append_value(schema_name);
        }
        if let Some(b) = &mut self.table_names {
            b.append_value(table_name);
        }

        self.column_names.append_value(field.name());

        if let Some(b) = &mut self.ordinal_positions {
            b.append_value(field_position as u64);
        }

        // DataFusion does not support column default values, so null
        if let Some(b) = &mut self.column_defaults {
            b.append_null();
        }

        // "YES if the column is possibly nullable, NO if it is known not nullable. "
        let nullable_str = if field.is_nullable() { "YES" } else { "NO" };
        self.is_nullables.append_value(nullable_str);

        // "System supplied type" --> Use display format of the datatype
        self.data_types.append_value(field.data_type().to_string());

        // "If data_type identifies a character or bit string type, the
        // declared maximum length; null for all other data types or
        // if no maximum length was declared."
        //
        // Arrow has no equivalent of VARCHAR(20), so we leave this as Null
        if let Some(b) = &mut self.character_maximum_lengths {
            b.append_option(None);
        }

        // "Maximum length, in bytes, for binary data, character data,
        // or text and image data."
        if let Some(b) = &mut self.character_octet_lengths {
            let char_len: Option<u64> = match field.data_type() {
                Utf8 | Binary => Some(i32::MAX as u64),
                LargeBinary | LargeUtf8 => Some(i64::MAX as u64),
                _ => None,
            };
            b.append_option(char_len);
        }

        // numeric_precision: "If data_type identifies a numeric type, this column
        // contains the (declared or implicit) precision of the type
        // for this column. The precision indicates the number of
        // significant digits. It can be expressed in decimal (base
        // 10) or binary (base 2) terms, as specified in the column
        // numeric_precision_radix. For all other data types, this
        // column is null."
        //
        // numeric_radix: If data_type identifies a numeric type, this
        // column indicates in which base the values in the columns
        // numeric_precision and numeric_scale are expressed. The
        // value is either 2 or 10. For all other data types, this
        // column is null.
        //
        // numeric_scale: If data_type identifies an exact numeric
        // type, this column contains the (declared or implicit) scale
        // of the type for this column. The scale indicates the number
        // of significant digits to the right of the decimal point. It
        // can be expressed in decimal (base 10) or binary (base 2)
        // terms, as specified in the column
        // numeric_precision_radix. For all other data types, this
        // column is null.
        if let (Some(precisions), Some(radixes), Some(scales)) = (
            &mut self.numeric_precisions,
            &mut self.numeric_precision_radixes,
            &mut self.numeric_scales,
        ) {
            let (numeric_precision, numeric_radix, numeric_scale) =
                match field.data_type() {
                    Int8 | UInt8 => (Some(8), Some(2), None),
                    Int16 | UInt16 => (Some(16), Some(2), None),
                    Int32 | UInt32 => (Some(32), Some(2), None),
                    // From max value of 65504 as explained on
                    // https://en.wikipedia.org/wiki/Half-precision_floating-point_format#Exponent_encoding
                    Float16 => (Some(15), Some(2), None),
                    // Numbers from postgres `real` type
                    Float32 => (Some(24), Some(2), None),
                    // Numbers from postgres `double` type
                    Float64 => (Some(24), Some(2), None),
                    Decimal128(precision, scale) => {
                        (Some(*precision as u64), Some(10), Some(*scale as u64))
                    }
                    _ => (None, None, None),
                };

            precisions.append_option(numeric_precision);
            radixes.append_option(numeric_radix);
            scales.append_option(numeric_scale);
        }

        if let Some(b) = &mut self.datetime_precisions {
            b.append_option(None);
        }
        if let Some(b) = &mut self.interval_types {
            b.append_null();
        }
    }

    /// Finalize the builder into a [`RecordBatch`].
    pub fn finish(&mut self) -> RecordBatch {
        let mut columns: Vec<ArrayRef> = Vec::new();
        if self.detail != ColumnsDetail::Describe {
            columns.push(Arc::new(self.catalog_names.as_mut().unwrap().finish()));
            columns.push(Arc::new(self.schema_names.as_mut().unwrap().finish()));
            columns.push(Arc::new(self.table_names.as_mut().unwrap().finish()));
        }
        columns.push(Arc::new(self.column_names.finish()));
        if self.detail == ColumnsDetail::Full {
            columns.push(Arc::new(self.ordinal_positions.as_mut().unwrap().finish()));
            columns.push(Arc::new(self.column_defaults.as_mut().unwrap().finish()));
        }
        if self.detail == ColumnsDetail::Full {
            columns.push(Arc::new(self.is_nullables.finish()));
            columns.push(Arc::new(self.data_types.finish()));
        } else {
            // `Describe`/`Basic` list `data_type` before `is_nullable`,
            // unlike the SQL-standard `information_schema.columns` order
            // used by `Full`.
            columns.push(Arc::new(self.data_types.finish()));
            columns.push(Arc::new(self.is_nullables.finish()));
        }
        if self.detail == ColumnsDetail::Full {
            columns.push(Arc::new(
                self.character_maximum_lengths.as_mut().unwrap().finish(),
            ));
            columns.push(Arc::new(
                self.character_octet_lengths.as_mut().unwrap().finish(),
            ));
            columns.push(Arc::new(self.numeric_precisions.as_mut().unwrap().finish()));
            columns.push(Arc::new(
                self.numeric_precision_radixes.as_mut().unwrap().finish(),
            ));
            columns.push(Arc::new(self.numeric_scales.as_mut().unwrap().finish()));
            columns.push(Arc::new(
                self.datetime_precisions.as_mut().unwrap().finish(),
            ));
            columns.push(Arc::new(self.interval_types.as_mut().unwrap().finish()));
        }
        RecordBatch::try_new(Arc::clone(&self.schema), columns).unwrap()
    }
}

/// Returns the Arrow schema of `information_schema.schemata` rows.
///
/// Columns and nullability match
/// <https://www.postgresql.org/docs/current/infoschema-schemata.html>.
pub fn schemata_schema() -> Schema {
    Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, false),
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("schema_owner", DataType::Utf8, true),
        Field::new("default_character_set_catalog", DataType::Utf8, true),
        Field::new("default_character_set_schema", DataType::Utf8, true),
        Field::new("default_character_set_name", DataType::Utf8, true),
        Field::new("sql_path", DataType::Utf8, true),
    ])
}

/// Builds `information_schema.schemata` rows.
#[derive(Debug)]
pub struct InformationSchemataBuilder {
    schema: SchemaRef,
    catalog_name: StringBuilder,
    schema_name: StringBuilder,
    schema_owner: StringBuilder,
    default_character_set_catalog: StringBuilder,
    default_character_set_schema: StringBuilder,
    default_character_set_name: StringBuilder,
    sql_path: StringBuilder,
}

impl Default for InformationSchemataBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl InformationSchemataBuilder {
    /// Construct an empty builder.
    pub fn new() -> Self {
        Self {
            schema: Arc::new(schemata_schema()),
            catalog_name: StringBuilder::new(),
            schema_name: StringBuilder::new(),
            schema_owner: StringBuilder::new(),
            default_character_set_catalog: StringBuilder::new(),
            default_character_set_schema: StringBuilder::new(),
            default_character_set_name: StringBuilder::new(),
            sql_path: StringBuilder::new(),
        }
    }

    /// Append one row. The `default_character_set_*` and `sql_path` columns are
    /// always null.
    pub fn add_schemata(
        &mut self,
        catalog_name: &str,
        schema_name: &str,
        schema_owner: Option<&str>,
    ) {
        self.catalog_name.append_value(catalog_name);
        self.schema_name.append_value(schema_name);
        match schema_owner {
            Some(owner) => self.schema_owner.append_value(owner),
            None => self.schema_owner.append_null(),
        }
        self.default_character_set_catalog.append_null();
        self.default_character_set_schema.append_null();
        self.default_character_set_name.append_null();
        self.sql_path.append_null();
    }

    /// Finalize the builder into a [`RecordBatch`].
    pub fn finish(&mut self) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.catalog_name.finish()),
                Arc::new(self.schema_name.finish()),
                Arc::new(self.schema_owner.finish()),
                Arc::new(self.default_character_set_catalog.finish()),
                Arc::new(self.default_character_set_schema.finish()),
                Arc::new(self.default_character_set_name.finish()),
                Arc::new(self.sql_path.finish()),
            ],
        )?)
    }
}

/// Returns the Arrow schema of `information_schema.df_settings` rows.
/// The `description` column is only included when `verbose` is set.
pub fn df_settings_schema(verbose: bool) -> Schema {
    let mut fields = vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, true),
    ];
    if verbose {
        fields.push(Field::new("description", DataType::Utf8, true));
    }
    Schema::new(fields)
}

/// Builds `information_schema.df_settings` rows.
#[derive(Debug)]
pub struct InformationSchemaDfSettingsBuilder {
    schema: SchemaRef,
    names: StringBuilder,
    values: StringBuilder,
    descriptions: Option<StringBuilder>,
}

impl InformationSchemaDfSettingsBuilder {
    /// Construct an empty builder producing the columns of
    /// [`df_settings_schema`]`(verbose)`.
    pub fn new(verbose: bool) -> Self {
        Self {
            schema: Arc::new(df_settings_schema(verbose)),
            names: StringBuilder::new(),
            values: StringBuilder::new(),
            descriptions: verbose.then(StringBuilder::new),
        }
    }

    /// Append one row.
    pub fn add_setting(&mut self, entry: ConfigEntry) {
        self.names.append_value(entry.key);
        self.values.append_option(entry.value);
        if let Some(descriptions) = &mut self.descriptions {
            descriptions.append_value(entry.description);
        }
    }

    /// Finalize the builder into a [`RecordBatch`].
    pub fn finish(&mut self) -> RecordBatch {
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(self.names.finish()),
            Arc::new(self.values.finish()),
        ];
        if let Some(descriptions) = &mut self.descriptions {
            columns.push(Arc::new(descriptions.finish()));
        }
        RecordBatch::try_new(Arc::clone(&self.schema), columns).unwrap()
    }
}

/// Returns the Arrow schema of `information_schema.routines` rows.
pub fn routines_schema() -> Schema {
    Schema::new(vec![
        Field::new("specific_catalog", DataType::Utf8, false),
        Field::new("specific_schema", DataType::Utf8, false),
        Field::new("specific_name", DataType::Utf8, false),
        Field::new("routine_catalog", DataType::Utf8, false),
        Field::new("routine_schema", DataType::Utf8, false),
        Field::new("routine_name", DataType::Utf8, false),
        Field::new("routine_type", DataType::Utf8, false),
        Field::new("is_deterministic", DataType::Boolean, true),
        Field::new("data_type", DataType::Utf8, true),
        Field::new("function_type", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("syntax_example", DataType::Utf8, true),
    ])
}

/// Builds `information_schema.routines` rows.
#[derive(Debug)]
pub struct InformationSchemaRoutinesBuilder {
    schema: SchemaRef,
    specific_catalog: StringBuilder,
    specific_schema: StringBuilder,
    specific_name: StringBuilder,
    routine_catalog: StringBuilder,
    routine_schema: StringBuilder,
    routine_name: StringBuilder,
    routine_type: StringBuilder,
    is_deterministic: BooleanBuilder,
    data_type: StringBuilder,
    function_type: StringBuilder,
    description: StringBuilder,
    syntax_example: StringBuilder,
}

impl Default for InformationSchemaRoutinesBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl InformationSchemaRoutinesBuilder {
    /// Construct an empty builder.
    pub fn new() -> Self {
        Self {
            schema: Arc::new(routines_schema()),
            specific_catalog: StringBuilder::new(),
            specific_schema: StringBuilder::new(),
            specific_name: StringBuilder::new(),
            routine_catalog: StringBuilder::new(),
            routine_schema: StringBuilder::new(),
            routine_name: StringBuilder::new(),
            routine_type: StringBuilder::new(),
            is_deterministic: BooleanBuilder::new(),
            data_type: StringBuilder::new(),
            function_type: StringBuilder::new(),
            description: StringBuilder::new(),
            syntax_example: StringBuilder::new(),
        }
    }

    /// Append one row.
    #[expect(clippy::too_many_arguments)]
    pub fn add_routine(
        &mut self,
        catalog_name: impl AsRef<str>,
        schema_name: impl AsRef<str>,
        routine_name: impl AsRef<str>,
        routine_type: impl AsRef<str>,
        is_deterministic: bool,
        data_type: Option<&impl AsRef<str>>,
        function_type: impl AsRef<str>,
        description: Option<impl AsRef<str>>,
        syntax_example: Option<impl AsRef<str>>,
    ) {
        self.specific_catalog.append_value(catalog_name.as_ref());
        self.specific_schema.append_value(schema_name.as_ref());
        self.specific_name.append_value(routine_name.as_ref());
        self.routine_catalog.append_value(catalog_name.as_ref());
        self.routine_schema.append_value(schema_name.as_ref());
        self.routine_name.append_value(routine_name.as_ref());
        self.routine_type.append_value(routine_type.as_ref());
        self.is_deterministic.append_value(is_deterministic);
        self.data_type.append_option(data_type.as_ref());
        self.function_type.append_value(function_type.as_ref());
        self.description.append_option(description);
        self.syntax_example.append_option(syntax_example);
    }

    /// Finalize the builder into a [`RecordBatch`].
    pub fn finish(&mut self) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.specific_catalog.finish()),
                Arc::new(self.specific_schema.finish()),
                Arc::new(self.specific_name.finish()),
                Arc::new(self.routine_catalog.finish()),
                Arc::new(self.routine_schema.finish()),
                Arc::new(self.routine_name.finish()),
                Arc::new(self.routine_type.finish()),
                Arc::new(self.is_deterministic.finish()),
                Arc::new(self.data_type.finish()),
                Arc::new(self.function_type.finish()),
                Arc::new(self.description.finish()),
                Arc::new(self.syntax_example.finish()),
            ],
        )
        .unwrap()
    }
}

/// Returns the Arrow schema of `information_schema.parameters` rows.
pub fn parameters_schema() -> Schema {
    Schema::new(vec![
        Field::new("specific_catalog", DataType::Utf8, false),
        Field::new("specific_schema", DataType::Utf8, false),
        Field::new("specific_name", DataType::Utf8, false),
        Field::new("ordinal_position", DataType::UInt64, false),
        Field::new("parameter_mode", DataType::Utf8, false),
        Field::new("parameter_name", DataType::Utf8, true),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("parameter_default", DataType::Utf8, true),
        Field::new("is_variadic", DataType::Boolean, false),
        // `rid` (short for `routine id`) is used to differentiate parameters from different signatures
        // (It serves as the group-by key when generating the `SHOW FUNCTIONS` query).
        // For example, the following signatures have different `rid` values:
        //     - `datetrunc(Utf8, Timestamp(Microsecond, Some("+TZ"))) -> Timestamp(Microsecond, Some("+TZ"))`
        //     - `datetrunc(Utf8View, Timestamp(Nanosecond, None)) -> Timestamp(Nanosecond, None)`
        Field::new("rid", DataType::UInt8, false),
    ])
}

/// Builds `information_schema.parameters` rows.
#[derive(Debug)]
pub struct InformationSchemaParametersBuilder {
    schema: SchemaRef,
    specific_catalog: StringBuilder,
    specific_schema: StringBuilder,
    specific_name: StringBuilder,
    ordinal_position: UInt64Builder,
    parameter_mode: StringBuilder,
    parameter_name: StringBuilder,
    data_type: StringBuilder,
    parameter_default: StringBuilder,
    is_variadic: BooleanBuilder,
    rid: UInt8Builder,
}

impl Default for InformationSchemaParametersBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl InformationSchemaParametersBuilder {
    /// Construct an empty builder.
    pub fn new() -> Self {
        Self {
            schema: Arc::new(parameters_schema()),
            specific_catalog: StringBuilder::new(),
            specific_schema: StringBuilder::new(),
            specific_name: StringBuilder::new(),
            ordinal_position: UInt64Builder::new(),
            parameter_mode: StringBuilder::new(),
            parameter_name: StringBuilder::new(),
            data_type: StringBuilder::new(),
            parameter_default: StringBuilder::new(),
            is_variadic: BooleanBuilder::new(),
            rid: UInt8Builder::new(),
        }
    }

    /// Append one row.
    #[expect(clippy::too_many_arguments)]
    pub fn add_parameter(
        &mut self,
        specific_catalog: impl AsRef<str>,
        specific_schema: impl AsRef<str>,
        specific_name: impl AsRef<str>,
        ordinal_position: u64,
        parameter_mode: impl AsRef<str>,
        parameter_name: Option<&(impl AsRef<str> + ?Sized)>,
        data_type: impl AsRef<str>,
        parameter_default: Option<impl AsRef<str>>,
        is_variadic: bool,
        rid: u8,
    ) {
        self.specific_catalog
            .append_value(specific_catalog.as_ref());
        self.specific_schema.append_value(specific_schema.as_ref());
        self.specific_name.append_value(specific_name.as_ref());
        self.ordinal_position.append_value(ordinal_position);
        self.parameter_mode.append_value(parameter_mode.as_ref());
        self.parameter_name.append_option(parameter_name.as_ref());
        self.data_type.append_value(data_type.as_ref());
        self.parameter_default.append_option(parameter_default);
        self.is_variadic.append_value(is_variadic);
        self.rid.append_value(rid);
    }

    /// Finalize the builder into a [`RecordBatch`].
    pub fn finish(&mut self) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.specific_catalog.finish()),
                Arc::new(self.specific_schema.finish()),
                Arc::new(self.specific_name.finish()),
                Arc::new(self.ordinal_position.finish()),
                Arc::new(self.parameter_mode.finish()),
                Arc::new(self.parameter_name.finish()),
                Arc::new(self.data_type.finish()),
                Arc::new(self.parameter_default.finish()),
                Arc::new(self.is_variadic.finish()),
                Arc::new(self.rid.finish()),
            ],
        )
        .unwrap()
    }
}

/// Returns the Arrow schema of `SHOW FUNCTIONS` output, with one row per function signature:
///
/// - `function_name`
/// - `return_type`
/// - `parameters`: parameter names, in order
/// - `parameter_types`: parameter types, in order
/// - `function_type`: `SCALAR`, `AGGREGATE`, `WINDOW` or `TABLE`
/// - `description` and `syntax_example`: from the function's documentation
///
/// Unlike the other schemas in this module, this does not correspond to an
/// `information_schema` table.
pub fn show_functions_schema() -> Schema {
    Schema::new(vec![
        Field::new("function_name", DataType::Utf8, false),
        Field::new("return_type", DataType::Utf8, true),
        Field::new("parameters", DataType::new_list(DataType::Utf8, true), true),
        Field::new(
            "parameter_types",
            DataType::new_list(DataType::Utf8, true),
            true,
        ),
        Field::new("function_type", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("syntax_example", DataType::Utf8, true),
    ])
}
