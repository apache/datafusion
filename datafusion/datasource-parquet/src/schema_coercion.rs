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

//! Arrow-schema coercion utilities used by the Parquet reader to make a
//! file schema match the table schema (binary→string, regular→view,
//! INT96→Timestamp, plain->Dictionary).
//!
//! These helpers are independent of the [`ParquetFormat`](crate::file_format::ParquetFormat)
//! type and several have been re-exported at the crate root for use by
//! callers outside the format implementation.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef, Schema, TimeUnit};
use parquet::basic::Type;
use parquet::schema::types::SchemaDescriptor;

/// Apply necessary schema type coercions to make file schema match table schema.
///
/// This function performs two main types of transformations in a single pass:
/// 1. Binary types to string types conversion - Converts binary data types to their
///    corresponding string types when the table schema expects string data
/// 2. Regular to view types conversion - Converts standard string/binary types to
///    view types when the table schema uses view types
///
/// # Arguments
/// * `table_schema` - The table schema containing the desired types
/// * `file_schema` - The file schema to be transformed
///
/// # Returns
/// * `Some(Schema)` - If any transformations were applied, returns the transformed schema
/// * `None` - If no transformations were needed
pub fn apply_file_schema_type_coercions(
    table_schema: &Schema,
    file_schema: &Schema,
) -> Option<Schema> {
    apply_file_schema_type_coercions_with_rle(table_schema, file_schema, false)
}

/// Like [`apply_file_schema_type_coercions`], but also coerces compatible
/// string/binary file fields to dictionary types already present in the table
/// schema.
pub(crate) fn apply_file_schema_type_coercions_with_rle(
    table_schema: &Schema,
    file_schema: &Schema,
    enable_rle_to_dictionary: bool,
) -> Option<Schema> {
    let mut needs_view_transform = false;
    let mut needs_string_transform = false;
    let mut needs_dict_transform = false;

    // Create a mapping of table field names to their data types for fast lookup
    // and simultaneously check if we need any transformations
    let table_fields: HashMap<_, _> = table_schema
        .fields()
        .iter()
        .map(|field| {
            let data_type = field.data_type();
            // Check if we need view type transformation
            if matches!(data_type, &DataType::Utf8View | &DataType::BinaryView) {
                needs_view_transform = true;
            }
            // Check if we need string type transformation
            if matches!(
                data_type,
                &DataType::Utf8 | &DataType::LargeUtf8 | &DataType::Utf8View
            ) {
                needs_string_transform = true;
            }
            if enable_rle_to_dictionary
                && matches!(data_type, &DataType::Dictionary(_, _))
            {
                needs_dict_transform = true;
            }

            (field.name(), data_type)
        })
        .collect();

    // Early return if no transformation needed
    if !needs_view_transform && !needs_string_transform && !needs_dict_transform {
        return None;
    }

    let transformed_fields: Vec<Arc<Field>> = file_schema
        .fields()
        .iter()
        .map(|field| {
            let field_name = field.name();
            let field_type = field.data_type();

            // Look up the corresponding field type in the table schema
            if let Some(table_type) = table_fields.get(field_name) {
                match (table_type, field_type) {
                    // table schema uses string type, coerce the file schema to use string type
                    (
                        &DataType::Utf8,
                        DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
                    ) => {
                        return field_with_new_type(field, DataType::Utf8);
                    }
                    // table schema uses large string type, coerce the file schema to use large string type
                    (
                        &DataType::LargeUtf8,
                        DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
                    ) => {
                        return field_with_new_type(field, DataType::LargeUtf8);
                    }
                    // table schema uses string view type, coerce the file schema to use view type
                    (
                        &DataType::Utf8View,
                        DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
                    ) => {
                        return field_with_new_type(field, DataType::Utf8View);
                    }
                    // Handle view type conversions
                    (&DataType::Utf8View, DataType::Utf8 | DataType::LargeUtf8) => {
                        return field_with_new_type(field, DataType::Utf8View);
                    }
                    (&DataType::BinaryView, DataType::Binary | DataType::LargeBinary) => {
                        return field_with_new_type(field, DataType::BinaryView);
                    }
                    (DataType::Dictionary(_, _), _)
                        if enable_rle_to_dictionary
                            && can_promote_to_dictionary_type(field_type, table_type) =>
                    {
                        return field_with_new_type(field, (*table_type).clone());
                    }
                    _ => {}
                }
            }
            // If no transformation is needed, keep the original field
            Arc::clone(field)
        })
        .collect();

    Some(Schema::new_with_metadata(
        transformed_fields,
        file_schema.metadata.clone(),
    ))
}

// Find the value type that can represent both sides without narrowing offsets
// or crossing string/binary families.
fn common_dictionary_value_type(
    field_type: &DataType,
    dictionary_value_type: &DataType,
) -> Option<DataType> {
    let field_type = match field_type {
        DataType::Dictionary(_, field_value_type) => field_value_type.as_ref(),
        _ => field_type,
    };

    match (field_type, dictionary_value_type) {
        (DataType::Utf8, DataType::Utf8) => Some(DataType::Utf8),
        (DataType::Utf8 | DataType::LargeUtf8, DataType::Utf8 | DataType::LargeUtf8) => {
            Some(DataType::LargeUtf8)
        }
        (DataType::Binary, DataType::Binary) => Some(DataType::Binary),
        (
            DataType::Binary | DataType::LargeBinary,
            DataType::Binary | DataType::LargeBinary,
        ) => Some(DataType::LargeBinary),
        _ => None,
    }
}

// Same family (both signed or both unsigned): return the wider member.
// Mixed: return the smallest signed type covering the larger capacity; Int64 is the ceiling
// because Parquet only supports signed integer dictionary key types.
// Returns None only for non-integer key types.
fn common_dictionary_key_type(a: &DataType, b: &DataType) -> Option<DataType> {
    fn key_capacity(dt: &DataType) -> Option<u128> {
        match dt {
            DataType::Int8 => Some(1u128 << 7),
            DataType::Int16 => Some(1u128 << 15),
            DataType::Int32 => Some(1u128 << 31),
            DataType::Int64 => Some(1u128 << 63),
            DataType::UInt8 => Some(1u128 << 8),
            DataType::UInt16 => Some(1u128 << 16),
            DataType::UInt32 => Some(1u128 << 32),
            DataType::UInt64 => Some(1u128 << 64),
            _ => None,
        }
    }
    fn is_signed(dt: &DataType) -> bool {
        matches!(
            dt,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        )
    }
    let cap_a = key_capacity(a)?;
    let cap_b = key_capacity(b)?;
    if is_signed(a) == is_signed(b) {
        return Some(if cap_a >= cap_b { a.clone() } else { b.clone() });
    }
    let max_cap = cap_a.max(cap_b);
    Some(match max_cap {
        c if c <= (1u128 << 7) => DataType::Int8,
        c if c <= (1u128 << 15) => DataType::Int16,
        c if c <= (1u128 << 31) => DataType::Int32,
        _ => DataType::Int64,
    })
}

fn can_promote_to_dictionary_type(
    file_field_type: &DataType,
    table_dictionary_type: &DataType,
) -> bool {
    let DataType::Dictionary(table_key, table_value) = table_dictionary_type else {
        return false;
    };
    if common_dictionary_value_type(file_field_type, table_value)
        .is_none_or(|common| &common != table_value.as_ref())
    {
        return false;
    }
    if let DataType::Dictionary(file_key, _) = file_field_type {
        return common_dictionary_key_type(file_key, table_key)
            .is_some_and(|common| &common == table_key.as_ref());
    }
    true
}

/// Normalize per-file schemas so that a column promoted to `Dictionary` in
/// *any* file is promoted to the same `Dictionary` type in *all* files.
///
/// This lets [`Schema::try_merge`] accept directories that mix dictionary and
/// plain encodings for the same column.
pub(crate) fn uniform_dict_schemas(schemas: Vec<Schema>) -> Vec<Schema> {
    // First pass: record the dictionary type for every column that is Dictionary in
    // at least one schema.
    let mut dict_types: HashMap<String, DataType> = HashMap::new();
    for schema in &schemas {
        for field in schema.fields() {
            if matches!(field.data_type(), DataType::Dictionary(_, _)) {
                dict_types
                    .entry(field.name().clone())
                    .or_insert_with(|| field.data_type().clone());
            }
        }
    }
    if dict_types.is_empty() {
        return schemas;
    }

    for schema in &schemas {
        for field in schema.fields() {
            let Some(dict_type) = dict_types.get_mut(field.name()) else {
                continue;
            };
            let DataType::Dictionary(key_type, value_type) = dict_type else {
                continue;
            };
            let key_type = key_type.clone();
            let value_type = value_type.as_ref().clone();
            let new_key = if let DataType::Dictionary(file_key, _) = field.data_type() {
                common_dictionary_key_type(&key_type, file_key)
                    .map(Box::new)
                    .unwrap_or_else(|| key_type.clone())
            } else {
                key_type.clone()
            };
            if let Some(common_type) =
                common_dictionary_value_type(field.data_type(), &value_type)
            {
                *dict_type = DataType::Dictionary(new_key, Box::new(common_type));
            }
        }
    }

    // Only promote fields whose type family matches the dictionary value type,
    // so schema normalization does not reinterpret binary bytes as UTF-8.
    schemas
        .into_iter()
        .map(|schema| {
            let fields: Vec<Arc<Field>> = schema
                .fields()
                .iter()
                .map(|field| {
                    if let Some(dict_type) = dict_types.get(field.name())
                        && can_promote_to_dictionary_type(field.data_type(), dict_type)
                    {
                        return field_with_new_type(field, dict_type.clone());
                    }
                    Arc::clone(field)
                })
                .collect();
            Schema::new_with_metadata(fields, schema.metadata().clone())
        })
        .collect()
}

/// Coerces the file schema's Timestamps to the provided TimeUnit if the
/// Parquet schema contains INT96.
///
/// Deprecated wrapper around [`Int96Coercer`]; use the builder directly
/// instead — it also supports attaching a timezone via
/// [`Int96Coercer::with_timezone`].
#[deprecated(since = "53.2.0", note = "use `Int96Coercer` instead")]
pub fn coerce_int96_to_resolution(
    parquet_schema: &SchemaDescriptor,
    file_schema: &Schema,
    time_unit: &TimeUnit,
) -> Option<Schema> {
    Int96Coercer::new(parquet_schema, file_schema, time_unit).coerce()
}

/// Builder for coercing INT96-originated Timestamp columns in `file_schema`
/// to a specific [`TimeUnit`], optionally attaching a timezone.
///
/// INT96 is the legacy Parquet representation that systems like Spark use for
/// timestamps. Arrow surfaces it as `Timestamp(Nanosecond, None)`, but the
/// underlying values are written as UTC-adjusted instants. Use this builder
/// to:
///
/// - Coerce INT96-derived columns to a smaller [`TimeUnit`] (e.g. microseconds)
///   to extend the representable date range.
/// - Optionally attach a timezone so the resulting Arrow type carries the
///   timezone-aware semantic (`Timestamp(unit, Some(tz))`). Without a
///   timezone, INT96-derived columns become `Timestamp(unit, None)` — the
///   historical default.
///
/// Returns `None` if `file_schema` contains no INT96-derived columns.
///
/// # Example
///
/// ```ignore
/// use std::sync::Arc;
/// use arrow::datatypes::TimeUnit;
/// use datafusion_datasource_parquet::Int96Coercer;
///
/// let coerced = Int96Coercer::new(parquet_schema, file_schema, &TimeUnit::Microsecond)
///     .with_timezone(Some(Arc::from("UTC")))
///     .coerce();
/// ```
pub struct Int96Coercer<'a> {
    parquet_schema: &'a SchemaDescriptor,
    file_schema: &'a Schema,
    time_unit: &'a TimeUnit,
    timezone: Option<Arc<str>>,
}

impl<'a> Int96Coercer<'a> {
    /// Create a new builder. INT96-derived columns will coerce to
    /// `Timestamp(time_unit, None)` unless [`Self::with_timezone`] is set.
    pub fn new(
        parquet_schema: &'a SchemaDescriptor,
        file_schema: &'a Schema,
        time_unit: &'a TimeUnit,
    ) -> Self {
        Self {
            parquet_schema,
            file_schema,
            time_unit,
            timezone: None,
        }
    }

    /// Attach a timezone to INT96-derived columns. When `Some`, INT96-derived
    /// columns coerce to `Timestamp(time_unit, Some(timezone))` instead of
    /// the default `Timestamp(time_unit, None)`. Spark and other systems
    /// write INT96 as UTC-adjusted instants, so callers that need the
    /// resulting Arrow type to be timezone-aware should pass
    /// `Some(Arc::from("UTC"))`.
    pub fn with_timezone(mut self, timezone: Option<Arc<str>>) -> Self {
        self.timezone = timezone;
        self
    }

    /// Run the coercion, returning the rewritten schema or `None` if
    /// `file_schema` contains no INT96-derived columns.
    pub fn coerce(self) -> Option<Schema> {
        let Self {
            parquet_schema,
            file_schema,
            time_unit,
            timezone,
        } = self;
        coerce_int96_to_resolution_impl(
            parquet_schema,
            file_schema,
            time_unit,
            timezone.as_ref(),
        )
    }
}

fn coerce_int96_to_resolution_impl(
    parquet_schema: &SchemaDescriptor,
    file_schema: &Schema,
    time_unit: &TimeUnit,
    timezone: Option<&Arc<str>>,
) -> Option<Schema> {
    // Traverse the parquet_schema columns looking for int96 physical types. If encountered, insert
    // the field's full path into a set.
    let int96_fields: HashSet<_> = parquet_schema
        .columns()
        .iter()
        .filter(|f| f.physical_type() == Type::INT96)
        .map(|f| f.path().string())
        .collect();

    if int96_fields.is_empty() {
        // The schema doesn't contain any int96 fields, so skip the remaining logic.
        return None;
    }

    // Do a DFS into the schema using a stack, looking for timestamp(nanos) fields that originated
    // as int96 to coerce to the provided time_unit.

    type NestedFields = Rc<RefCell<Vec<FieldRef>>>;
    type StackContext<'a> = (
        Vec<&'a str>, // The Parquet column path (e.g., "c0.list.element.c1") for the current field.
        &'a FieldRef, // The current field to be processed.
        NestedFields, // The parent's fields that this field will be (possibly) type-coerced and
        // inserted into. All fields have a parent, so this is not an Option type.
        Option<NestedFields>, // Nested types need to create their own vector of fields for their
                              // children. For primitive types this will remain None. For nested
                              // types it is None the first time they are processed. Then, we
                              // instantiate a vector for its children, push the field back onto the
                              // stack to be processed again, and DFS into its children. The next
                              // time we process the field, we know we have DFS'd into the children
                              // because this field is Some.
    );

    // This is our top-level fields from which we will construct our schema. We pass this into our
    // initial stack context as the parent fields, and the DFS populates it.
    let fields = Rc::new(RefCell::new(Vec::with_capacity(file_schema.fields.len())));

    // TODO: It might be possible to only DFS into nested fields that we know contain an int96 if we
    // use some sort of LPM data structure to check if we're currently DFS'ing nested types that are
    // in a column path that contains an int96. That can be a future optimization for large schemas.
    let transformed_schema = {
        // Populate the stack with our top-level fields.
        let mut stack: Vec<StackContext> = file_schema
            .fields()
            .iter()
            .rev()
            .map(|f| (vec![f.name().as_str()], f, Rc::clone(&fields), None))
            .collect();

        // Pop fields to DFS into until we have exhausted the stack.
        while let Some((parquet_path, current_field, parent_fields, child_fields)) =
            stack.pop()
        {
            match (current_field.data_type(), child_fields) {
                (DataType::Struct(unprocessed_children), None) => {
                    // This is the first time popping off this struct. We don't yet know the
                    // correct types of its children (i.e., if they need coercing) so we create
                    // a vector for child_fields, push the struct node back onto the stack to be
                    // processed again (see below) after processing all its children.
                    let child_fields = Rc::new(RefCell::new(Vec::with_capacity(
                        unprocessed_children.len(),
                    )));
                    // Note that here we push the struct back onto the stack with its
                    // parent_fields in the same position, now with Some(child_fields).
                    stack.push((
                        parquet_path.clone(),
                        current_field,
                        parent_fields,
                        Some(Rc::clone(&child_fields)),
                    ));
                    // Push all the children in reverse to maintain original schema order due to
                    // stack processing.
                    for child in unprocessed_children.into_iter().rev() {
                        let mut child_path = parquet_path.clone();
                        // Build up a normalized path that we'll use as a key into the original
                        // int96_fields set above to test if this originated as int96.
                        child_path.push(".");
                        child_path.push(child.name());
                        // Note that here we push the field onto the stack using the struct's
                        // new child_fields vector as the field's parent_fields.
                        stack.push((child_path, child, Rc::clone(&child_fields), None));
                    }
                }
                (DataType::Struct(unprocessed_children), Some(processed_children)) => {
                    // This is the second time popping off this struct. The child_fields vector
                    // now contains each field that has been DFS'd into, and we can construct
                    // the resulting struct with correct child types.
                    let processed_children = processed_children.borrow();
                    assert_eq!(processed_children.len(), unprocessed_children.len());
                    let processed_struct = Field::new_struct(
                        current_field.name(),
                        processed_children.as_slice(),
                        current_field.is_nullable(),
                    );
                    parent_fields.borrow_mut().push(Arc::new(processed_struct));
                }
                (DataType::List(unprocessed_child), None) => {
                    // This is the first time popping off this list. See struct docs above.
                    let child_fields = Rc::new(RefCell::new(Vec::with_capacity(1)));
                    stack.push((
                        parquet_path.clone(),
                        current_field,
                        parent_fields,
                        Some(Rc::clone(&child_fields)),
                    ));
                    let mut child_path = parquet_path.clone();
                    // Spark uses a definition for arrays/lists that results in a group
                    // named "list" that is not maintained when parsing to Arrow. We just push
                    // this name into the path.
                    child_path.push(".list.");
                    child_path.push(unprocessed_child.name());
                    stack.push((
                        child_path.clone(),
                        unprocessed_child,
                        Rc::clone(&child_fields),
                        None,
                    ));
                }
                (DataType::List(_), Some(processed_children)) => {
                    // This is the second time popping off this list. See struct docs above.
                    let processed_children = processed_children.borrow();
                    assert_eq!(processed_children.len(), 1);
                    let processed_list = Field::new_list(
                        current_field.name(),
                        Arc::clone(&processed_children[0]),
                        current_field.is_nullable(),
                    );
                    parent_fields.borrow_mut().push(Arc::new(processed_list));
                }
                (DataType::Map(unprocessed_child, _), None) => {
                    // This is the first time popping off this map. See struct docs above.
                    let child_fields = Rc::new(RefCell::new(Vec::with_capacity(1)));
                    stack.push((
                        parquet_path.clone(),
                        current_field,
                        parent_fields,
                        Some(Rc::clone(&child_fields)),
                    ));
                    let mut child_path = parquet_path.clone();
                    child_path.push(".");
                    child_path.push(unprocessed_child.name());
                    stack.push((
                        child_path.clone(),
                        unprocessed_child,
                        Rc::clone(&child_fields),
                        None,
                    ));
                }
                (DataType::Map(_, sorted), Some(processed_children)) => {
                    // This is the second time popping off this map. See struct docs above.
                    let processed_children = processed_children.borrow();
                    assert_eq!(processed_children.len(), 1);
                    let processed_map = Field::new(
                        current_field.name(),
                        DataType::Map(Arc::clone(&processed_children[0]), *sorted),
                        current_field.is_nullable(),
                    );
                    parent_fields.borrow_mut().push(Arc::new(processed_map));
                }
                (DataType::Timestamp(TimeUnit::Nanosecond, None), None)
                    if int96_fields.contains(parquet_path.concat().as_str()) =>
                // We found a timestamp(nanos) and it originated as int96. Coerce it to the correct
                // time_unit, optionally attaching the requested timezone.
                {
                    parent_fields.borrow_mut().push(field_with_new_type(
                        current_field,
                        DataType::Timestamp(*time_unit, timezone.cloned()),
                    ));
                }
                // Other types can be cloned as they are.
                _ => parent_fields.borrow_mut().push(Arc::clone(current_field)),
            }
        }
        assert_eq!(fields.borrow().len(), file_schema.fields.len());
        Schema::new_with_metadata(
            fields.borrow_mut().clone(),
            file_schema.metadata.clone(),
        )
    };

    Some(transformed_schema)
}

/// Create a new field with the specified data type, copying the other
/// properties from the input field
fn field_with_new_type(field: &FieldRef, new_type: DataType) -> FieldRef {
    Arc::new(field.as_ref().clone().with_data_type(new_type))
}

/// Transform a schema to use view types for Utf8 and Binary
///
/// See [`ParquetFormat::force_view_types`](crate::file_format::ParquetFormat::force_view_types) for details
pub fn transform_schema_to_view(schema: &Schema) -> Schema {
    let transformed_fields: Vec<Arc<Field>> = schema
        .fields
        .iter()
        .map(|field| match field.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                field_with_new_type(field, DataType::Utf8View)
            }
            DataType::Binary | DataType::LargeBinary => {
                field_with_new_type(field, DataType::BinaryView)
            }
            _ => Arc::clone(field),
        })
        .collect();
    Schema::new_with_metadata(transformed_fields, schema.metadata.clone())
}

/// Transform a schema so that any binary types are strings
pub fn transform_binary_to_string(schema: &Schema) -> Schema {
    let transformed_fields: Vec<Arc<Field>> = schema
        .fields
        .iter()
        .map(|field| match field.data_type() {
            DataType::Binary => field_with_new_type(field, DataType::Utf8),
            DataType::LargeBinary => field_with_new_type(field, DataType::LargeUtf8),
            DataType::BinaryView => field_with_new_type(field, DataType::Utf8View),
            _ => Arc::clone(field),
        })
        .collect();
    Schema::new_with_metadata(transformed_fields, schema.metadata.clone())
}
#[cfg(test)]
mod tests {
    use parquet::arrow::parquet_to_arrow_schema;

    use super::*;

    use parquet::schema::parser::parse_message_type;

    #[test]
    fn coerce_int96_to_resolution_with_mixed_timestamps() {
        // Unclear if Spark (or other writer) could generate a file with mixed timestamps like this,
        // but we want to test the scenario just in case since it's at least a valid schema as far
        // as the Parquet spec is concerned.
        let spark_schema = "
        message spark_schema {
          optional int96 c0;
          optional int64 c1 (TIMESTAMP(NANOS,true));
          optional int64 c2 (TIMESTAMP(NANOS,false));
          optional int64 c3 (TIMESTAMP(MILLIS,true));
          optional int64 c4 (TIMESTAMP(MILLIS,false));
          optional int64 c5 (TIMESTAMP(MICROS,true));
          optional int64 c6 (TIMESTAMP(MICROS,false));
        }
        ";

        let schema = parse_message_type(spark_schema).expect("should parse schema");
        let descr = SchemaDescriptor::new(Arc::new(schema));

        let arrow_schema = parquet_to_arrow_schema(&descr, None).unwrap();

        let result = Int96Coercer::new(&descr, &arrow_schema, &TimeUnit::Microsecond)
            .coerce()
            .unwrap();

        // Only the first field (c0) should be converted to a microsecond timestamp because it's the
        // only timestamp that originated from an INT96.
        let expected_schema = Schema::new(vec![
            Field::new("c0", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new(
                "c1",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            Field::new("c2", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new(
                "c3",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                true,
            ),
            Field::new("c4", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new(
                "c5",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new("c6", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        ]);

        assert_eq!(result, expected_schema);
    }

    #[test]
    fn coerce_int96_to_resolution_with_tz_applies_timezone() {
        // Same input schema as `coerce_int96_to_resolution_with_mixed_timestamps`, but with a
        // non-empty `timezone` argument. Only c0 (the INT96 column) should pick up the timezone;
        // the other timestamp columns must keep whatever timezone they were declared with.
        let spark_schema = "
        message spark_schema {
          optional int96 c0;
          optional int64 c1 (TIMESTAMP(NANOS,true));
          optional int64 c2 (TIMESTAMP(NANOS,false));
          optional int64 c3 (TIMESTAMP(MILLIS,true));
          optional int64 c4 (TIMESTAMP(MILLIS,false));
          optional int64 c5 (TIMESTAMP(MICROS,true));
          optional int64 c6 (TIMESTAMP(MICROS,false));
        }
        ";

        let schema = parse_message_type(spark_schema).expect("should parse schema");
        let descr = SchemaDescriptor::new(Arc::new(schema));

        let arrow_schema = parquet_to_arrow_schema(&descr, None).unwrap();

        let result = Int96Coercer::new(&descr, &arrow_schema, &TimeUnit::Microsecond)
            .with_timezone(Some(Arc::from("UTC")))
            .coerce()
            .unwrap();

        let expected_schema = Schema::new(vec![
            Field::new(
                "c0",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "c1",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            Field::new("c2", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new(
                "c3",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                true,
            ),
            Field::new("c4", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new(
                "c5",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new("c6", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        ]);

        assert_eq!(result, expected_schema);
    }

    #[test]
    fn coerce_int96_to_resolution_with_nested_types() {
        // This schema is derived from Comet's CometFuzzTestSuite ParquetGenerator only using int96
        // primitive types with generateStruct, generateArray, and generateMap set to true, with one
        // additional field added to c4's struct to make sure all fields in a struct get modified.
        // https://github.com/apache/datafusion-comet/blob/main/spark/src/main/scala/org/apache/comet/testing/ParquetGenerator.scala
        let spark_schema = "
        message spark_schema {
          optional int96 c0;
          optional group c1 {
            optional int96 c0;
          }
          optional group c2 {
            optional group c0 (LIST) {
              repeated group list {
                optional int96 element;
              }
            }
          }
          optional group c3 (LIST) {
            repeated group list {
              optional int96 element;
            }
          }
          optional group c4 (LIST) {
            repeated group list {
              optional group element {
                optional int96 c0;
                optional int96 c1;
              }
            }
          }
          optional group c5 (MAP) {
            repeated group key_value {
              required int96 key;
              optional int96 value;
            }
          }
          optional group c6 (LIST) {
            repeated group list {
              optional group element (MAP) {
                repeated group key_value {
                  required int96 key;
                  optional int96 value;
                }
              }
            }
          }
        }
        ";

        let schema = parse_message_type(spark_schema).expect("should parse schema");
        let descr = SchemaDescriptor::new(Arc::new(schema));

        let arrow_schema = parquet_to_arrow_schema(&descr, None).unwrap();

        let result = Int96Coercer::new(&descr, &arrow_schema, &TimeUnit::Microsecond)
            .coerce()
            .unwrap();

        let expected_schema = Schema::new(vec![
            Field::new("c0", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new_struct(
                "c1",
                vec![Field::new(
                    "c0",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                )],
                true,
            ),
            Field::new_struct(
                "c2",
                vec![Field::new_list(
                    "c0",
                    Field::new(
                        "element",
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        true,
                    ),
                    true,
                )],
                true,
            ),
            Field::new_list(
                "c3",
                Field::new(
                    "element",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ),
                true,
            ),
            Field::new_list(
                "c4",
                Field::new_struct(
                    "element",
                    vec![
                        Field::new(
                            "c0",
                            DataType::Timestamp(TimeUnit::Microsecond, None),
                            true,
                        ),
                        Field::new(
                            "c1",
                            DataType::Timestamp(TimeUnit::Microsecond, None),
                            true,
                        ),
                    ],
                    true,
                ),
                true,
            ),
            Field::new_map(
                "c5",
                "key_value",
                Field::new(
                    "key",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
                Field::new(
                    "value",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ),
                false,
                true,
            ),
            Field::new_list(
                "c6",
                Field::new_map(
                    "element",
                    "key_value",
                    Field::new(
                        "key",
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        false,
                    ),
                    Field::new(
                        "value",
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        true,
                    ),
                    false,
                    true,
                ),
                true,
            ),
        ]);

        assert_eq!(result, expected_schema);
    }

    fn dict(value_type: DataType) -> DataType {
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(value_type))
    }

    fn one_field_schema(data_type: DataType) -> Schema {
        Schema::new(vec![Field::new("col", data_type, true)])
    }

    #[test]
    fn uniform_dict_schemas_respects_value_type_families() {
        // String/binary dictionary promotion must preserve the concrete value type
        // family so schema merging does not silently reinterpret bytes as UTF-8.
        let cases = vec![
            (
                "utf8",
                dict(DataType::Utf8),
                DataType::Utf8,
                dict(DataType::Utf8),
                dict(DataType::Utf8),
            ),
            (
                "large_utf8",
                dict(DataType::LargeUtf8),
                DataType::LargeUtf8,
                dict(DataType::LargeUtf8),
                dict(DataType::LargeUtf8),
            ),
            (
                "utf8_large_utf8",
                dict(DataType::Utf8),
                DataType::LargeUtf8,
                dict(DataType::LargeUtf8),
                dict(DataType::LargeUtf8),
            ),
            (
                "binary",
                dict(DataType::Binary),
                DataType::Binary,
                dict(DataType::Binary),
                dict(DataType::Binary),
            ),
            (
                "large_binary",
                dict(DataType::LargeBinary),
                DataType::LargeBinary,
                dict(DataType::LargeBinary),
                dict(DataType::LargeBinary),
            ),
            (
                "binary_large_binary",
                dict(DataType::Binary),
                DataType::LargeBinary,
                dict(DataType::LargeBinary),
                dict(DataType::LargeBinary),
            ),
            (
                "binary_not_utf8",
                dict(DataType::Utf8),
                DataType::Binary,
                dict(DataType::Utf8),
                DataType::Binary,
            ),
            (
                "utf8_not_binary",
                dict(DataType::Binary),
                DataType::Utf8,
                dict(DataType::Binary),
                DataType::Utf8,
            ),
        ];

        for (name, dict_type, plain_type, expected_dict_type, expected_plain_type) in
            cases
        {
            let result = uniform_dict_schemas(vec![
                one_field_schema(dict_type.clone()),
                one_field_schema(plain_type),
            ]);

            assert_eq!(
                result[0].field(0).data_type(),
                &expected_dict_type,
                "{name}"
            );
            assert_eq!(
                result[1].field(0).data_type(),
                &expected_plain_type,
                "{name}"
            );
        }
    }

    #[test]
    fn rle_schema_coercion_respects_dictionary_value_type() {
        // Scan-time schema coercion can only use dictionary types already chosen by
        // schema inference, and must leave incompatible file fields unchanged.
        let cases = vec![
            (
                "utf8",
                dict(DataType::Utf8),
                DataType::Utf8,
                dict(DataType::Utf8),
            ),
            (
                "large_utf8",
                dict(DataType::LargeUtf8),
                DataType::LargeUtf8,
                dict(DataType::LargeUtf8),
            ),
            (
                "large_utf8_to_utf8_dict_not_safe",
                dict(DataType::Utf8),
                DataType::LargeUtf8,
                DataType::LargeUtf8,
            ),
            (
                "utf8_to_large_utf8_dict",
                dict(DataType::LargeUtf8),
                DataType::Utf8,
                dict(DataType::LargeUtf8),
            ),
            (
                "binary",
                dict(DataType::Binary),
                DataType::Binary,
                dict(DataType::Binary),
            ),
            (
                "large_binary",
                dict(DataType::LargeBinary),
                DataType::LargeBinary,
                dict(DataType::LargeBinary),
            ),
            (
                "large_binary_to_binary_dict_not_safe",
                dict(DataType::Binary),
                DataType::LargeBinary,
                DataType::LargeBinary,
            ),
            (
                "binary_to_large_binary_dict",
                dict(DataType::LargeBinary),
                DataType::Binary,
                dict(DataType::LargeBinary),
            ),
            (
                "binary_not_utf8",
                dict(DataType::Utf8),
                DataType::Binary,
                DataType::Binary,
            ),
            (
                "utf8_not_binary",
                dict(DataType::Binary),
                DataType::Utf8,
                DataType::Utf8,
            ),
            (
                "binary_not_int64_dict",
                dict(DataType::Int64),
                DataType::Binary,
                DataType::Binary,
            ),
        ];

        for (name, table_type, file_type, expected_type) in cases {
            let table_schema = one_field_schema(table_type);
            let file_schema = one_field_schema(file_type);

            let output_type = apply_file_schema_type_coercions_with_rle(
                &table_schema,
                &file_schema,
                true,
            )
            .as_ref()
            .map(|s| s.field(0).data_type().clone())
            .unwrap_or_else(|| file_schema.field(0).data_type().clone());

            assert_eq!(output_type, expected_type, "{name}");
        }

        let table_schema = Schema::new(vec![
            Field::new("dict_col", dict(DataType::Utf8), true),
            Field::new("string_col", DataType::Utf8, true),
        ]);
        let file_schema = Schema::new(vec![
            Field::new("dict_col", DataType::Utf8, true),
            Field::new("string_col", DataType::Binary, true),
        ]);
        let result =
            apply_file_schema_type_coercions_with_rle(&table_schema, &file_schema, false)
                .unwrap();

        assert_eq!(result.field(0).data_type(), &DataType::Utf8);
        assert_eq!(result.field(1).data_type(), &DataType::Utf8);
    }

    fn dk(key: DataType) -> DataType {
        DataType::Dictionary(Box::new(key), Box::new(DataType::Utf8))
    }

    #[test]
    fn uniform_dict_schemas_key_type_widening() {
        // (name, file_a, file_b, expected_a, expected_b)
        let cases = [
            (
                "signed wider wins",
                dk(DataType::Int8),
                dk(DataType::Int32),
                dk(DataType::Int32),
                dk(DataType::Int32),
            ),
            (
                "signed wider wins reversed",
                dk(DataType::Int32),
                dk(DataType::Int8),
                dk(DataType::Int32),
                dk(DataType::Int32),
            ),
            (
                "same unsigned stays",
                dk(DataType::UInt8),
                dk(DataType::UInt8),
                dk(DataType::UInt8),
                dk(DataType::UInt8),
            ),
            (
                "uint8+int32→int32",
                dk(DataType::UInt8),
                dk(DataType::Int32),
                dk(DataType::Int32),
                dk(DataType::Int32),
            ),
            (
                "uint8+int8→int16",
                dk(DataType::UInt8),
                dk(DataType::Int8),
                dk(DataType::Int16),
                dk(DataType::Int16),
            ),
            (
                "uint64+int64→int64",
                dk(DataType::UInt64),
                dk(DataType::Int64),
                dk(DataType::Int64),
                dk(DataType::Int64),
            ),
        ];
        for (name, a, b, exp_a, exp_b) in cases {
            let result =
                uniform_dict_schemas(vec![one_field_schema(a), one_field_schema(b)]);
            assert_eq!(result[0].field(0).data_type(), &exp_a, "{name}");
            assert_eq!(result[1].field(0).data_type(), &exp_b, "{name}");
        }
    }

    #[test]
    fn rle_schema_coercion_rejects_key_narrowing() {
        let coerce = |table: DataType, file: DataType| {
            let t = one_field_schema(table);
            let f = one_field_schema(file.clone());
            apply_file_schema_type_coercions_with_rle(&t, &f, true)
                .as_ref()
                .map(|s| s.field(0).data_type().clone())
                .unwrap_or(file)
        };
        // Dict(Int32) file must not be narrowed to Dict(Int8) at scan time.
        assert_eq!(
            coerce(dk(DataType::Int8), dk(DataType::Int32)),
            dk(DataType::Int32)
        );
        // Plain Utf8 file is still promoted to a narrow-key dict (no file key to narrow from).
        assert_eq!(
            coerce(dk(DataType::Int8), DataType::Utf8),
            dk(DataType::Int8)
        );
    }
}
