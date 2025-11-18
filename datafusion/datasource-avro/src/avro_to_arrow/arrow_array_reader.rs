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

//! Avro to Arrow array readers

use apache_avro::{
    error::Details as AvroErrorDetails,
    schema::{
        EnumSchema, FixedSchema, Name, RecordSchema, Schema as AvroSchema, SchemaKind,
    },
    types::Value,
    Error as AvroError, Reader as AvroReader,
};
use arrow::array::{
    make_array, Array, ArrayBuilder, ArrayData, ArrayDataBuilder, ArrayRef,
    BooleanBuilder, LargeStringArray, ListBuilder, NullArray, OffsetSizeTrait,
    PrimitiveArray, StringArray, StringBuilder, StringDictionaryBuilder,
};
use arrow::array::{BinaryArray, FixedSizeBinaryArray, GenericListArray};
use arrow::buffer::{Buffer, MutableBuffer};
use arrow::datatypes::{
    ArrowDictionaryKeyType, ArrowNumericType, ArrowPrimitiveType, DataType, Date32Type,
    Date64Type, Field, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type,
    UInt8Type,
};
use arrow::datatypes::{Fields, SchemaRef};
use arrow::error::ArrowError;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use arrow::util::bit_util;
use datafusion_common::error::{DataFusionError, Result};
use datafusion_common::{arrow_err, HashMap, HashSet};
use num_traits::NumCast;
use std::collections::BTreeMap;
use std::io::Read;
use std::sync::Arc;

type RecordSlice<'a> = &'a [&'a Vec<(String, Value)>];

#[derive(Default)]
pub struct NamesResolver {
    in_progress: HashSet<Name>,
    all_subnames_in_name: HashMap<Name, BTreeMap<String, usize>>,
}

pub struct AvroArrowArrayReader<'a, R: Read> {
    reader: AvroReader<'a, R>,
    schema: SchemaRef,
    schema_lookup: BTreeMap<String, usize>,
}

impl<R: Read> AvroArrowArrayReader<'_, R> {
    pub fn try_new(reader: R, schema: SchemaRef) -> Result<Self> {
        let reader = AvroReader::new(reader)?;
        let writer_schema = reader.writer_schema().clone();

        let schema_lookup = Self::schema_lookup(writer_schema)?;

        Ok(Self {
            reader,
            schema,
            schema_lookup,
        })
    }

    pub fn schema_lookup(schema: AvroSchema) -> Result<BTreeMap<String, usize>> {
        match schema {
            AvroSchema::Record(RecordSchema {
                fields,
                mut lookup,
                name,
                ..
            }) => {
                let mut unresolved_names = NamesResolver::default();
                unresolved_names.in_progress.insert(name.clone());

                for field in fields {
                    Self::child_schema_lookup(
                        &field.name,
                        &field.schema,
                        &mut lookup,
                        &mut unresolved_names,
                    )?;
                }

                unresolved_names.in_progress.remove(&name);
                assert!(unresolved_names.in_progress.is_empty());

                Ok(lookup)
            }
            _ => arrow_err!(ArrowError::SchemaError(
                "expected avro schema to be a record".to_string(),
            )),
        }
    }

    fn child_schema_lookup<'b>(
        parent_field_name: &str,
        schema: &AvroSchema,
        schema_lookup: &'b mut BTreeMap<String, usize>,
        unresolved_names: &'b mut NamesResolver,
    ) -> Result<&'b BTreeMap<String, usize>> {
        match schema {
            AvroSchema::Union(us) => {
                let has_nullable = us
                    .find_schema_with_known_schemata::<apache_avro::Schema>(
                        &Value::Null,
                        None,
                        &None,
                    )
                    .is_some();
                let sub_schemas = us.variants();
                if has_nullable && sub_schemas.len() == 2 {
                    if let Some(sub_schema) =
                        sub_schemas.iter().find(|&s| !matches!(s, AvroSchema::Null))
                    {
                        Self::child_schema_lookup(
                            parent_field_name,
                            sub_schema,
                            schema_lookup,
                            unresolved_names,
                        )?;
                    }
                }
            }
            AvroSchema::Record(RecordSchema {
                fields,
                lookup,
                name,
                ..
            }) => {
                let inserted = unresolved_names.in_progress.insert(name.clone());
                if !inserted {
                    return arrow_err!(ArrowError::SchemaError(format!("Detected circular reference while resolving schema lookup for record schema with name: {name}")));
                }

                let mut inner_lookup = BTreeMap::new();
                lookup.iter().for_each(|(field_name, pos)| {
                    inner_lookup.insert(field_name.clone(), *pos);
                });

                for field in fields {
                    Self::child_schema_lookup(
                        &field.name,
                        &field.schema,
                        &mut inner_lookup,
                        unresolved_names,
                    )?;
                }

                // Extend the parent schema lookup with the inner lookup entries
                schema_lookup.extend(inner_lookup.iter().map(|(lookup_entry, pos)| {
                    (format!("{parent_field_name}.{lookup_entry}"), *pos)
                }));

                // Store the inner lookup for potential references
                unresolved_names
                    .all_subnames_in_name
                    .insert(name.clone(), inner_lookup);

                unresolved_names.in_progress.remove(name);
            }
            AvroSchema::Array(schema) => {
                Self::child_schema_lookup(
                    parent_field_name,
                    &schema.items,
                    schema_lookup,
                    unresolved_names,
                )?;
            }
            AvroSchema::Map(map_schema) => {
                let sub_parent_field_name = format!("{parent_field_name}.value");
                Self::child_schema_lookup(
                    &sub_parent_field_name,
                    &map_schema.types,
                    schema_lookup,
                    unresolved_names,
                )?;
            }
            AvroSchema::Fixed(FixedSchema { name, .. }) => {
                unresolved_names
                    .all_subnames_in_name
                    .insert(name.clone(), BTreeMap::new());
            }
            AvroSchema::Enum(EnumSchema { name, .. }) => {
                unresolved_names
                    .all_subnames_in_name
                    .insert(name.clone(), BTreeMap::new());
            }
            AvroSchema::Ref { name } => {
                // Detect circular references
                if unresolved_names.in_progress.contains(name) {
                    return arrow_err!(ArrowError::SchemaError(format!("Detected circular reference while resolving schema lookup for record schema with name: {name}")));
                }

                let subnames = unresolved_names
                    .all_subnames_in_name
                    .get(name)
                    .ok_or_else(|| {
                        AvroError::new(AvroErrorDetails::SchemaResolutionError(
                            name.clone(),
                        ))
                    })?;

                schema_lookup.extend(
                    subnames
                        .iter()
                        .map(|(k, v)| (format!("{parent_field_name}.{k}"), *v)),
                );
            }
            _ => (),
        }
        Ok(schema_lookup)
    }

    /// Read the next batch of records
    pub fn next_batch(&mut self, batch_size: usize) -> Option<ArrowResult<RecordBatch>> {
        let rows_result = self
            .reader
            .by_ref()
            .take(batch_size)
            .map(|value| match value {
                Ok(Value::Record(v)) => Ok(v),
                Err(e) => Err(ArrowError::ParseError(format!(
                    "Failed to parse avro value: {e}"
                ))),
                other => Err(ArrowError::ParseError(format!(
                    "Row needs to be of type object, got: {other:?}"
                ))),
            })
            .collect::<ArrowResult<Vec<Vec<(String, Value)>>>>();

        let rows = match rows_result {
            // Return error early
            Err(e) => return Some(Err(e)),
            // No rows: return None early
            Ok(rows) if rows.is_empty() => return None,
            Ok(rows) => rows,
        };

        let rows = rows.iter().collect::<Vec<&Vec<(String, Value)>>>();
        let arrays = self.build_struct_array(&rows, "", self.schema.fields());

        Some(arrays.and_then(|arr| RecordBatch::try_new(Arc::clone(&self.schema), arr)))
    }

    fn build_boolean_array(&self, rows: RecordSlice, col_name: &str) -> ArrayRef {
        let mut builder = BooleanBuilder::with_capacity(rows.len());
        for row in rows {
            if let Some(value) = self.field_lookup(col_name, row) {
                if let Some(boolean) = resolve_boolean(value) {
                    builder.append_value(boolean)
                } else {
                    builder.append_null();
                }
            } else {
                builder.append_null();
            }
        }
        Arc::new(builder.finish())
    }

    fn build_primitive_array<T>(&self, rows: RecordSlice, col_name: &str) -> ArrayRef
    where
        T: ArrowNumericType + Resolver,
        T::Native: NumCast,
    {
        Arc::new(
            rows.iter()
                .map(|row| {
                    self.field_lookup(col_name, row)
                        .and_then(|value| resolve_item::<T>(value))
                })
                .collect::<PrimitiveArray<T>>(),
        )
    }

    #[inline(always)]
    fn build_string_dictionary_builder<T>(
        &self,
        row_len: usize,
    ) -> StringDictionaryBuilder<T>
    where
        T: ArrowPrimitiveType + ArrowDictionaryKeyType,
    {
        StringDictionaryBuilder::with_capacity(row_len, row_len, row_len)
    }

    fn build_wrapped_list_array(
        &self,
        rows: RecordSlice,
        col_name: &str,
        key_type: &DataType,
    ) -> ArrowResult<ArrayRef> {
        match *key_type {
            DataType::Int8 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::Int8),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<Int8Type>(&dtype, col_name, rows)
            }
            DataType::Int16 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::Int16),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<Int16Type>(&dtype, col_name, rows)
            }
            DataType::Int32 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<Int32Type>(&dtype, col_name, rows)
            }
            DataType::Int64 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::Int64),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<Int64Type>(&dtype, col_name, rows)
            }
            DataType::UInt8 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::UInt8),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<UInt8Type>(&dtype, col_name, rows)
            }
            DataType::UInt16 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::UInt16),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<UInt16Type>(&dtype, col_name, rows)
            }
            DataType::UInt32 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::UInt32),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<UInt32Type>(&dtype, col_name, rows)
            }
            DataType::UInt64 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::UInt64),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<UInt64Type>(&dtype, col_name, rows)
            }
            ref e => Err(ArrowError::SchemaError(format!(
                "Data type is currently not supported for dictionaries in list : {e}"
            ))),
        }
    }

    #[inline(always)]
    fn list_array_string_array_builder<D>(
        &self,
        data_type: &DataType,
        col_name: &str,
        rows: RecordSlice,
    ) -> ArrowResult<ArrayRef>
    where
        D: ArrowPrimitiveType + ArrowDictionaryKeyType,
    {
        let mut builder: Box<dyn ArrayBuilder> = match data_type {
            DataType::Utf8 => {
                let values_builder = StringBuilder::with_capacity(rows.len(), 5);
                Box::new(ListBuilder::new(values_builder))
            }
            DataType::Dictionary(_, _) => {
                let values_builder =
                    self.build_string_dictionary_builder::<D>(rows.len() * 5);
                Box::new(ListBuilder::new(values_builder))
            }
            e => {
                return Err(ArrowError::SchemaError(format!(
                    "Nested list data builder type is not supported: {e}"
                )))
            }
        };

        for row in rows {
            if let Some(value) = self.field_lookup(col_name, row) {
                let value = maybe_resolve_union(value);
                // value can be an array or a scalar
                let vals: Vec<Option<String>> = if let Value::String(v) = value {
                    vec![Some(v.to_string())]
                } else if let Value::Array(n) = value {
                    n.iter()
                        .map(resolve_string)
                        .collect::<ArrowResult<Vec<Option<String>>>>()?
                        .into_iter()
                        .collect::<Vec<Option<String>>>()
                } else if let Value::Null = value {
                    vec![None]
                } else if !matches!(value, Value::Record(_)) {
                    vec![resolve_string(value)?]
                } else {
                    return Err(ArrowError::SchemaError(
                        "Only scalars are currently supported in Avro arrays".to_string(),
                    ));
                };

                // TODO: ARROW-10335: APIs of dictionary arrays and others are different. Unify
                // them.
                match data_type {
                    DataType::Utf8 => {
                        let builder = builder
                            .as_any_mut()
                            .downcast_mut::<ListBuilder<StringBuilder>>()
                            .ok_or_else(||ArrowError::SchemaError(
                                "Cast failed for ListBuilder<StringBuilder> during nested data parsing".to_string(),
                            ))?;
                        for val in vals {
                            if let Some(v) = val {
                                builder.values().append_value(&v)
                            } else {
                                builder.values().append_null()
                            };
                        }

                        // Append to the list
                        builder.append(true);
                    }
                    DataType::Dictionary(_, _) => {
                        let builder = builder.as_any_mut().downcast_mut::<ListBuilder<StringDictionaryBuilder<D>>>().ok_or_else(||ArrowError::SchemaError(
                            "Cast failed for ListBuilder<StringDictionaryBuilder> during nested data parsing".to_string(),
                        ))?;
                        for val in vals {
                            if let Some(v) = val {
                                let _ = builder.values().append(&v)?;
                            } else {
                                builder.values().append_null()
                            };
                        }

                        // Append to the list
                        builder.append(true);
                    }
                    e => {
                        return Err(ArrowError::SchemaError(format!(
                            "Nested list data builder type is not supported: {e}"
                        )))
                    }
                }
            }
        }

        Ok(builder.finish() as ArrayRef)
    }

    #[inline(always)]
    fn build_dictionary_array<T>(
        &self,
        rows: RecordSlice,
        col_name: &str,
    ) -> ArrowResult<ArrayRef>
    where
        T::Native: NumCast,
        T: ArrowPrimitiveType + ArrowDictionaryKeyType,
    {
        let mut builder: StringDictionaryBuilder<T> =
            self.build_string_dictionary_builder(rows.len());
        for row in rows {
            if let Some(value) = self.field_lookup(col_name, row) {
                if let Ok(Some(str_v)) = resolve_string(value) {
                    builder.append(str_v).map(drop)?
                } else {
                    builder.append_null()
                }
            } else {
                builder.append_null()
            }
        }
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }

    #[inline(always)]
    fn build_string_dictionary_array(
        &self,
        rows: RecordSlice,
        col_name: &str,
        key_type: &DataType,
        value_type: &DataType,
    ) -> ArrowResult<ArrayRef> {
        if let DataType::Utf8 = *value_type {
            match *key_type {
                DataType::Int8 => self.build_dictionary_array::<Int8Type>(rows, col_name),
                DataType::Int16 => {
                    self.build_dictionary_array::<Int16Type>(rows, col_name)
                }
                DataType::Int32 => {
                    self.build_dictionary_array::<Int32Type>(rows, col_name)
                }
                DataType::Int64 => {
                    self.build_dictionary_array::<Int64Type>(rows, col_name)
                }
                DataType::UInt8 => {
                    self.build_dictionary_array::<UInt8Type>(rows, col_name)
                }
                DataType::UInt16 => {
                    self.build_dictionary_array::<UInt16Type>(rows, col_name)
                }
                DataType::UInt32 => {
                    self.build_dictionary_array::<UInt32Type>(rows, col_name)
                }
                DataType::UInt64 => {
                    self.build_dictionary_array::<UInt64Type>(rows, col_name)
                }
                _ => Err(ArrowError::SchemaError(
                    "unsupported dictionary key type".to_string(),
                )),
            }
        } else {
            Err(ArrowError::SchemaError(
                "dictionary types other than UTF-8 not yet supported".to_string(),
            ))
        }
    }

    /// Build a nested GenericListArray from a list of unnested `Value`s
    fn build_nested_list_array<OffsetSize: OffsetSizeTrait>(
        &self,
        parent_field_name: &str,
        rows: &[&Value],
        list_field: &Field,
    ) -> ArrowResult<ArrayRef> {
        // build list offsets
        let mut cur_offset = OffsetSize::zero();
        let list_len = rows.len();
        let num_list_bytes = bit_util::ceil(list_len, 8);
        let mut offsets = Vec::with_capacity(list_len + 1);
        let mut list_nulls = MutableBuffer::from_len_zeroed(num_list_bytes);
        offsets.push(cur_offset);
        rows.iter().enumerate().for_each(|(i, v)| {
            // TODO: unboxing Union(Array(Union(...))) should probably be done earlier
            let v = maybe_resolve_union(v);
            if let Value::Array(a) = v {
                cur_offset += OffsetSize::from_usize(a.len()).unwrap();
                bit_util::set_bit(&mut list_nulls, i);
            } else if let Value::Null = v {
                // value is null, not incremented
            } else {
                cur_offset += OffsetSize::one();
            }
            offsets.push(cur_offset);
        });
        let valid_len = cur_offset.to_usize().unwrap();
        let array_data = match list_field.data_type() {
            DataType::Null => NullArray::new(valid_len).into_data(),
            DataType::Boolean => {
                let num_bytes = bit_util::ceil(valid_len, 8);
                let mut bool_values = MutableBuffer::from_len_zeroed(num_bytes);
                let mut bool_nulls =
                    MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);
                let mut curr_index = 0;
                rows.iter().for_each(|v| {
                    if let Value::Array(vs) = v {
                        vs.iter().for_each(|value| {
                            if let Value::Boolean(child) = value {
                                // if valid boolean, append value
                                if *child {
                                    bit_util::set_bit(&mut bool_values, curr_index);
                                }
                            } else {
                                // null slot
                                bit_util::unset_bit(&mut bool_nulls, curr_index);
                            }
                            curr_index += 1;
                        });
                    }
                });
                ArrayData::builder(list_field.data_type().clone())
                    .len(valid_len)
                    .add_buffer(bool_values.into())
                    .null_bit_buffer(Some(bool_nulls.into()))
                    .build()
                    .unwrap()
            }
            DataType::Int8 => self.read_primitive_list_values::<Int8Type>(rows),
            DataType::Int16 => self.read_primitive_list_values::<Int16Type>(rows),
            DataType::Int32 => self.read_primitive_list_values::<Int32Type>(rows),
            DataType::Int64 => self.read_primitive_list_values::<Int64Type>(rows),
            DataType::UInt8 => self.read_primitive_list_values::<UInt8Type>(rows),
            DataType::UInt16 => self.read_primitive_list_values::<UInt16Type>(rows),
            DataType::UInt32 => self.read_primitive_list_values::<UInt32Type>(rows),
            DataType::UInt64 => self.read_primitive_list_values::<UInt64Type>(rows),
            DataType::Float16 => {
                return Err(ArrowError::SchemaError("Float16 not supported".to_string()))
            }
            DataType::Float32 => self.read_primitive_list_values::<Float32Type>(rows),
            DataType::Float64 => self.read_primitive_list_values::<Float64Type>(rows),
            DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_) => {
                return Err(ArrowError::SchemaError(
                    "Temporal types are not yet supported, see ARROW-4803".to_string(),
                ))
            }
            DataType::Utf8 => flatten_string_values(rows)
                .into_iter()
                .collect::<StringArray>()
                .into_data(),
            DataType::LargeUtf8 => flatten_string_values(rows)
                .into_iter()
                .collect::<LargeStringArray>()
                .into_data(),
            DataType::List(field) => {
                let child = self.build_nested_list_array::<i32>(
                    parent_field_name,
                    &flatten_values(rows),
                    field,
                )?;
                child.to_data()
            }
            DataType::LargeList(field) => {
                let child = self.build_nested_list_array::<i64>(
                    parent_field_name,
                    &flatten_values(rows),
                    field,
                )?;
                child.to_data()
            }
            DataType::Struct(fields) => {
                // extract list values, with non-lists converted to Value::Null
                let array_item_count = rows
                    .iter()
                    .map(|row| match maybe_resolve_union(row) {
                        Value::Array(values) => values.len(),
                        _ => 1,
                    })
                    .sum();
                let num_bytes = bit_util::ceil(array_item_count, 8);
                let mut null_buffer = MutableBuffer::from_len_zeroed(num_bytes);
                let mut struct_index = 0;
                let null_struct_array = vec![("null".to_string(), Value::Null)];
                let rows: Vec<&Vec<(String, Value)>> = rows
                    .iter()
                    .map(|v| maybe_resolve_union(v))
                    .flat_map(|row| {
                        if let Value::Array(values) = row {
                            values
                                .iter()
                                .map(maybe_resolve_union)
                                .map(|v| match v {
                                    Value::Record(record) => {
                                        bit_util::set_bit(&mut null_buffer, struct_index);
                                        struct_index += 1;
                                        record
                                    }
                                    Value::Null => {
                                        struct_index += 1;
                                        &null_struct_array
                                    }
                                    other => panic!("expected Record, got {other:?}"),
                                })
                                .collect::<Vec<&Vec<(String, Value)>>>()
                        } else {
                            struct_index += 1;
                            vec![&null_struct_array]
                        }
                    })
                    .collect();

                let arrays = self.build_struct_array(&rows, parent_field_name, fields)?;
                let data_type = DataType::Struct(fields.clone());
                ArrayDataBuilder::new(data_type)
                    .len(rows.len())
                    .null_bit_buffer(Some(null_buffer.into()))
                    .child_data(arrays.into_iter().map(|a| a.to_data()).collect())
                    .build()
                    .unwrap()
            }
            datatype => {
                return Err(ArrowError::SchemaError(format!(
                    "Nested list of {datatype} not supported"
                )));
            }
        };
        // build list
        let list_data = ArrayData::builder(DataType::List(Arc::new(list_field.clone())))
            .len(list_len)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_child_data(array_data)
            .null_bit_buffer(Some(list_nulls.into()))
            .build()
            .unwrap();
        Ok(Arc::new(GenericListArray::<OffsetSize>::from(list_data)))
    }

    /// Builds the child values of a `StructArray`, falling short of constructing the StructArray.
    /// The function does not construct the StructArray as some callers would want the child arrays.
    ///
    /// *Note*: The function is recursive, and will read nested structs.
    fn build_struct_array(
        &self,
        rows: RecordSlice,
        parent_field_name: &str,
        struct_fields: &Fields,
    ) -> ArrowResult<Vec<ArrayRef>> {
        let arrays: ArrowResult<Vec<ArrayRef>> = struct_fields
            .iter()
            .map(|field| {
                let field_path = if parent_field_name.is_empty() {
                    field.name().to_string()
                } else {
                    format!("{}.{}", parent_field_name, field.name())
                };
                let arr = match field.data_type() {
                    DataType::Null => Arc::new(NullArray::new(rows.len())) as ArrayRef,
                    DataType::Boolean => self.build_boolean_array(rows, &field_path),
                    DataType::Float64 => {
                        self.build_primitive_array::<Float64Type>(rows, &field_path)
                    }
                    DataType::Float32 => {
                        self.build_primitive_array::<Float32Type>(rows, &field_path)
                    }
                    DataType::Int64 => {
                        self.build_primitive_array::<Int64Type>(rows, &field_path)
                    }
                    DataType::Int32 => {
                        self.build_primitive_array::<Int32Type>(rows, &field_path)
                    }
                    DataType::Int16 => {
                        self.build_primitive_array::<Int16Type>(rows, &field_path)
                    }
                    DataType::Int8 => {
                        self.build_primitive_array::<Int8Type>(rows, &field_path)
                    }
                    DataType::UInt64 => {
                        self.build_primitive_array::<UInt64Type>(rows, &field_path)
                    }
                    DataType::UInt32 => {
                        self.build_primitive_array::<UInt32Type>(rows, &field_path)
                    }
                    DataType::UInt16 => {
                        self.build_primitive_array::<UInt16Type>(rows, &field_path)
                    }
                    DataType::UInt8 => {
                        self.build_primitive_array::<UInt8Type>(rows, &field_path)
                    }
                    // TODO: this is incomplete
                    DataType::Timestamp(unit, _) => match unit {
                        TimeUnit::Second => self
                            .build_primitive_array::<TimestampSecondType>(
                                rows,
                                &field_path,
                            ),
                        TimeUnit::Microsecond => self
                            .build_primitive_array::<TimestampMicrosecondType>(
                                rows,
                                &field_path,
                            ),
                        TimeUnit::Millisecond => self
                            .build_primitive_array::<TimestampMillisecondType>(
                                rows,
                                &field_path,
                            ),
                        TimeUnit::Nanosecond => self
                            .build_primitive_array::<TimestampNanosecondType>(
                                rows,
                                &field_path,
                            ),
                    },
                    DataType::Date64 => {
                        self.build_primitive_array::<Date64Type>(rows, &field_path)
                    }
                    DataType::Date32 => {
                        self.build_primitive_array::<Date32Type>(rows, &field_path)
                    }
                    DataType::Time64(unit) => match unit {
                        TimeUnit::Microsecond => self
                            .build_primitive_array::<Time64MicrosecondType>(
                                rows,
                                &field_path,
                            ),
                        TimeUnit::Nanosecond => self
                            .build_primitive_array::<Time64NanosecondType>(
                                rows,
                                &field_path,
                            ),
                        t => {
                            return Err(ArrowError::SchemaError(format!(
                                "TimeUnit {t:?} not supported with Time64"
                            )))
                        }
                    },
                    DataType::Time32(unit) => match unit {
                        TimeUnit::Second => self
                            .build_primitive_array::<Time32SecondType>(rows, &field_path),
                        TimeUnit::Millisecond => self
                            .build_primitive_array::<Time32MillisecondType>(
                                rows,
                                &field_path,
                            ),
                        t => {
                            return Err(ArrowError::SchemaError(format!(
                                "TimeUnit {t:?} not supported with Time32"
                            )))
                        }
                    },
                    DataType::Utf8 | DataType::LargeUtf8 => Arc::new(
                        rows.iter()
                            .map(|row| {
                                let maybe_value = self.field_lookup(&field_path, row);
                                match maybe_value {
                                    None => Ok(None),
                                    Some(v) => resolve_string(v),
                                }
                            })
                            .collect::<ArrowResult<StringArray>>()?,
                    )
                        as ArrayRef,
                    DataType::Binary | DataType::LargeBinary => Arc::new(
                        rows.iter()
                            .map(|row| {
                                let maybe_value = self.field_lookup(&field_path, row);
                                maybe_value.and_then(resolve_bytes)
                            })
                            .collect::<BinaryArray>(),
                    )
                        as ArrayRef,
                    DataType::FixedSizeBinary(ref size) => {
                        Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                            rows.iter().map(|row| {
                                let maybe_value = self.field_lookup(&field_path, row);
                                maybe_value.and_then(|v| resolve_fixed(v, *size as usize))
                            }),
                            *size,
                        )?) as ArrayRef
                    }
                    DataType::List(ref list_field) => {
                        match list_field.data_type() {
                            DataType::Dictionary(ref key_ty, _) => {
                                self.build_wrapped_list_array(rows, &field_path, key_ty)?
                            }
                            _ => {
                                // extract rows by name
                                let extracted_rows = rows
                                    .iter()
                                    .map(|row| {
                                        self.field_lookup(&field_path, row)
                                            .unwrap_or(&Value::Null)
                                    })
                                    .collect::<Vec<&Value>>();
                                self.build_nested_list_array::<i32>(
                                    &field_path,
                                    &extracted_rows,
                                    list_field,
                                )?
                            }
                        }
                    }
                    DataType::Dictionary(ref key_ty, ref val_ty) => self
                        .build_string_dictionary_array(
                            rows,
                            &field_path,
                            key_ty,
                            val_ty,
                        )?,
                    DataType::Struct(fields) => {
                        let len = rows.len();
                        let num_bytes = bit_util::ceil(len, 8);
                        let mut null_buffer = MutableBuffer::from_len_zeroed(num_bytes);
                        let empty_vec = vec![];
                        let struct_rows = rows
                            .iter()
                            .enumerate()
                            .map(|(i, row)| (i, self.field_lookup(&field_path, row)))
                            .map(|(i, v)| {
                                let v = v.map(maybe_resolve_union);
                                match v {
                                    Some(Value::Record(value)) => {
                                        bit_util::set_bit(&mut null_buffer, i);
                                        value
                                    }
                                    None | Some(Value::Null) => &empty_vec,
                                    other => {
                                        panic!("expected struct got {other:?}");
                                    }
                                }
                            })
                            .collect::<Vec<&Vec<(String, Value)>>>();
                        let arrays =
                            self.build_struct_array(&struct_rows, &field_path, fields)?;
                        // construct a struct array's data in order to set null buffer
                        let data_type = DataType::Struct(fields.clone());
                        let data = ArrayDataBuilder::new(data_type)
                            .len(len)
                            .null_bit_buffer(Some(null_buffer.into()))
                            .child_data(arrays.into_iter().map(|a| a.to_data()).collect())
                            .build()?;
                        make_array(data)
                    }
                    _ => {
                        return Err(ArrowError::SchemaError(format!(
                            "type {} not supported",
                            field.data_type()
                        )))
                    }
                };
                Ok(arr)
            })
            .collect();
        arrays
    }

    /// Read the primitive list's values into ArrayData
    fn read_primitive_list_values<T>(&self, rows: &[&Value]) -> ArrayData
    where
        T: ArrowPrimitiveType + ArrowNumericType,
        T::Native: NumCast,
    {
        let values = rows
            .iter()
            .flat_map(|row| {
                let row = maybe_resolve_union(row);
                if let Value::Array(values) = row {
                    values
                        .iter()
                        .map(resolve_item::<T>)
                        .collect::<Vec<Option<T::Native>>>()
                } else if let Some(f) = resolve_item::<T>(row) {
                    vec![Some(f)]
                } else {
                    vec![]
                }
            })
            .collect::<Vec<Option<T::Native>>>();
        let array = values.iter().collect::<PrimitiveArray<T>>();
        array.to_data()
    }

    fn field_lookup<'b>(
        &self,
        name: &str,
        row: &'b [(String, Value)],
    ) -> Option<&'b Value> {
        self.schema_lookup
            .get(name)
            .and_then(|i| row.get(*i))
            .map(|o| &o.1)
    }
}

/// Flattens a list of Avro values, by flattening lists, and treating all other values as
/// single-value lists.
/// This is used to read into nested lists (list of list, list of struct) and non-dictionary lists.
#[inline]
fn flatten_values<'a>(values: &[&'a Value]) -> Vec<&'a Value> {
    values
        .iter()
        .flat_map(|row| {
            let v = maybe_resolve_union(row);
            if let Value::Array(values) = v {
                values.iter().collect()
            } else {
                // we interpret a scalar as a single-value list to minimise data loss
                vec![v]
            }
        })
        .collect()
}

/// Flattens a list into string values, dropping Value::Null in the process.
/// This is useful for interpreting any Avro array as string, dropping nulls.
/// See `value_as_string`.
#[inline]
fn flatten_string_values(values: &[&Value]) -> Vec<Option<String>> {
    values
        .iter()
        .flat_map(|row| {
            let row = maybe_resolve_union(row);
            if let Value::Array(values) = row {
                values
                    .iter()
                    .map(|s| resolve_string(s).ok().flatten())
                    .collect::<Vec<Option<_>>>()
            } else if let Value::Null = row {
                vec![]
            } else {
                vec![resolve_string(row).ok().flatten()]
            }
        })
        .collect::<Vec<Option<_>>>()
}

/// Reads an Avro value as a string, regardless of its type.
/// This is useful if the expected datatype is a string, in which case we preserve
/// all the values regardless of they type.
fn resolve_string(v: &Value) -> ArrowResult<Option<String>> {
    let v = if let Value::Union(_, b) = v { b } else { v };
    match v {
        Value::String(s) => Ok(Some(s.clone())),
        Value::Bytes(bytes) => String::from_utf8(bytes.to_vec())
            .map_err(|e| AvroError::new(AvroErrorDetails::ConvertToUtf8(e)))
            .map(Some),
        Value::Enum(_, s) => Ok(Some(s.clone())),
        Value::Null => Ok(None),
        other => Err(AvroError::new(AvroErrorDetails::GetString(other.clone()))),
    }
    .map_err(|e| ArrowError::SchemaError(format!("expected resolvable string : {e}")))
}

fn resolve_u8(v: &Value) -> Option<u8> {
    let v = match v {
        Value::Union(_, inner) => inner.as_ref(),
        _ => v,
    };

    match v {
        Value::Int(n) => u8::try_from(*n).ok(),
        Value::Long(n) => u8::try_from(*n).ok(),
        _ => None,
    }
}

fn resolve_bytes(v: &Value) -> Option<Vec<u8>> {
    let v = match v {
        Value::Union(_, inner) => inner.as_ref(),
        _ => v,
    };

    match v {
        Value::Bytes(bytes) => Some(bytes.clone()),
        Value::String(s) => Some(s.as_bytes().to_vec()),
        Value::Array(items) => items.iter().map(resolve_u8).collect::<Option<Vec<u8>>>(),
        _ => None,
    }
}

fn resolve_fixed(v: &Value, size: usize) -> Option<Vec<u8>> {
    let v = if let Value::Union(_, b) = v { b } else { v };
    match v {
        Value::Fixed(n, bytes) => {
            if *n == size {
                Some(bytes.clone())
            } else {
                None
            }
        }
        _ => None,
    }
}

fn resolve_boolean(value: &Value) -> Option<bool> {
    let v = if let Value::Union(_, b) = value {
        b
    } else {
        value
    };
    match v {
        Value::Boolean(boolean) => Some(*boolean),
        _ => None,
    }
}

trait Resolver: ArrowPrimitiveType {
    fn resolve(value: &Value) -> Option<Self::Native>;
}

fn resolve_item<T: Resolver>(value: &Value) -> Option<T::Native> {
    T::resolve(value)
}

fn maybe_resolve_union(value: &Value) -> &Value {
    if SchemaKind::from(value) == SchemaKind::Union {
        // Pull out the Union, and attempt to resolve against it.
        match value {
            Value::Union(_, b) => b,
            _ => unreachable!(),
        }
    } else {
        value
    }
}

impl<N> Resolver for N
where
    N: ArrowNumericType,
    N::Native: NumCast,
{
    fn resolve(value: &Value) -> Option<Self::Native> {
        let value = maybe_resolve_union(value);
        match value {
            Value::Int(i) | Value::TimeMillis(i) | Value::Date(i) => NumCast::from(*i),
            Value::Long(l)
            | Value::TimeMicros(l)
            | Value::TimestampMillis(l)
            | Value::TimestampMicros(l) => NumCast::from(*l),
            Value::Float(f) => NumCast::from(*f),
            Value::Double(f) => NumCast::from(*f),
            Value::Duration(_d) => unimplemented!(), // shenanigans type
            Value::Null => None,
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::avro_to_arrow::{Reader, ReaderBuilder};
    use arrow::array::{Array, FixedSizeBinaryArray, StringArray};
    use arrow::datatypes::{DataType, Fields};
    use arrow::datatypes::{Field, TimeUnit};
    use datafusion_common::assert_batches_eq;
    use datafusion_common::cast::{
        as_int32_array, as_int64_array, as_list_array, as_timestamp_microsecond_array,
    };
    use std::fs::File;
    use std::sync::Arc;

    fn build_reader(name: &'_ str, batch_size: usize) -> Reader<'_, File> {
        let testdata = datafusion_common::test_util::arrow_test_data();
        let filename = format!("{testdata}/avro/{name}");
        let builder = ReaderBuilder::new()
            .read_schema()
            .with_batch_size(batch_size);
        builder.build(File::open(filename).unwrap()).unwrap()
    }

    // TODO: Fixed, Enum, Dictionary

    #[test]
    fn test_time_avro_milliseconds() {
        let mut reader = build_reader("alltypes_plain.avro", 10);
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(11, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let timestamp_col = schema.column_with_name("timestamp_col").unwrap();
        assert_eq!(
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            timestamp_col.1.data_type()
        );
        let timestamp_array =
            as_timestamp_microsecond_array(batch.column(timestamp_col.0)).unwrap();
        for i in 0..timestamp_array.len() {
            assert!(timestamp_array.is_valid(i));
        }
        assert_eq!(1235865600000000, timestamp_array.value(0));
        assert_eq!(1235865660000000, timestamp_array.value(1));
        assert_eq!(1238544000000000, timestamp_array.value(2));
        assert_eq!(1238544060000000, timestamp_array.value(3));
        assert_eq!(1233446400000000, timestamp_array.value(4));
        assert_eq!(1233446460000000, timestamp_array.value(5));
        assert_eq!(1230768000000000, timestamp_array.value(6));
        assert_eq!(1230768060000000, timestamp_array.value(7));
    }

    #[test]
    fn test_avro_read_list() {
        let mut reader = build_reader("list_columns.avro", 3);
        let schema = reader.schema();
        let (col_id_index, _) = schema.column_with_name("int64_list").unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);
        let a_array = as_list_array(batch.column(col_id_index)).unwrap();
        assert_eq!(
            *a_array.data_type(),
            DataType::new_list(DataType::Int64, true)
        );
        let array = a_array.value(0);
        assert_eq!(*array.data_type(), DataType::Int64);

        assert_eq!(
            6,
            as_int64_array(&array)
                .unwrap()
                .iter()
                .flatten()
                .sum::<i64>()
        );
    }
    #[test]
    fn test_avro_read_nested_list() {
        let mut reader = build_reader("nested_lists.snappy.avro", 3);
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn test_complex_list() {
        let schema = apache_avro::Schema::parse_str(
            r#"
            {
              "type": "record",
              "name": "r1",
              "fields": [
                {
                  "name": "headers",
                  "type": ["null", {
                        "type": "array",
                        "items": ["null",{
                            "name":"r2",
                            "type": "record",
                            "fields":[
                                {"name":"name", "type": ["null", "string"], "default": null},
                                {"name":"value", "type": ["null", "string"], "default": null}
                            ]
                        }]
                    }],
                    "default": null
                }
              ]
            }"#,
        )
        .unwrap();
        let r1 = apache_avro::to_value(serde_json::json!({
            "headers": [
                {
                    "name": "a",
                    "value": "b"
                }
            ]
        }))
        .unwrap()
        .resolve(&schema)
        .unwrap();

        let mut w = apache_avro::Writer::new(&schema, vec![]);
        w.append(r1).unwrap();
        let bytes = w.into_inner().unwrap();

        let mut reader = ReaderBuilder::new()
            .read_schema()
            .with_batch_size(2)
            .build(std::io::Cursor::new(bytes))
            .unwrap();

        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
        let expected = [
            "+-----------------------+",
            "| headers               |",
            "+-----------------------+",
            "| [{name: a, value: b}] |",
            "+-----------------------+",
        ];
        assert_batches_eq!(expected, &[batch]);
    }

    #[test]
    fn test_complex_struct() {
        let schema = apache_avro::Schema::parse_str(
            r#"
        {
          "type": "record",
          "name": "r1",
          "fields": [
            {
              "name": "dns",
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "r13",
                  "fields": [
                    {
                      "name": "answers",
                      "type": [
                        "null",
                        {
                          "type": "array",
                          "items": [
                            "null",
                            {
                              "type": "record",
                              "name": "r292",
                              "fields": [
                                {
                                  "name": "class",
                                  "type": ["null", "string"],
                                  "default": null
                                },
                                {
                                  "name": "data",
                                  "type": ["null", "string"],
                                  "default": null
                                },
                                {
                                  "name": "name",
                                  "type": ["null", "string"],
                                  "default": null
                                },
                                {
                                  "name": "ttl",
                                  "type": ["null", "long"],
                                  "default": null
                                },
                                {
                                  "name": "type",
                                  "type": ["null", "string"],
                                  "default": null
                                }
                              ]
                            }
                          ]
                        }
                      ],
                      "default": null
                    },
                    {
                      "name": "header_flags",
                      "type": [
                        "null",
                        {
                          "type": "array",
                          "items": ["null", "string"]
                        }
                      ],
                      "default": null
                    },
                    {
                      "name": "id",
                      "type": ["null", "string"],
                      "default": null
                    },
                    {
                      "name": "op_code",
                      "type": ["null", "string"],
                      "default": null
                    },
                    {
                      "name": "question",
                      "type": [
                        "null",
                        {
                          "type": "record",
                          "name": "r288",
                          "fields": [
                            {
                              "name": "class",
                              "type": ["null", "string"],
                              "default": null
                            },
                            {
                              "name": "name",
                              "type": ["null", "string"],
                              "default": null
                            },
                            {
                              "name": "registered_domain",
                              "type": ["null", "string"],
                              "default": null
                            },
                            {
                              "name": "subdomain",
                              "type": ["null", "string"],
                              "default": null
                            },
                            {
                              "name": "top_level_domain",
                              "type": ["null", "string"],
                              "default": null
                            },
                            {
                              "name": "type",
                              "type": ["null", "string"],
                              "default": null
                            }
                          ]
                        }
                      ],
                      "default": null
                    },
                    {
                      "name": "resolved_ip",
                      "type": [
                        "null",
                        {
                          "type": "array",
                          "items": ["null", "string"]
                        }
                      ],
                      "default": null
                    },
                    {
                      "name": "response_code",
                      "type": ["null", "string"],
                      "default": null
                    },
                    {
                      "name": "type",
                      "type": ["null", "string"],
                      "default": null
                    }
                  ]
                }
              ],
              "default": null
            }
          ]
        }"#,
        )
        .unwrap();

        let jv1 = serde_json::json!({
          "dns": {
            "answers": [
                {
                    "data": "CHNlY3VyaXR5BnVidW50dQMjb20AAAEAAQAAAAgABLl9vic=",
                    "type": "1"
                },
                {
                    "data": "CHNlY3VyaXR5BnVidW50dQNjb20AAAEAABAAAAgABLl9viQ=",
                    "type": "1"
                },
                {
                    "data": "CHNlT3VyaXR5BnVidW50dQNjb20AAAEAAQAAAAgABFu9Wyc=",
                    "type": "1"
                }
            ],
            "question": {
                "name": "security.ubuntu.com",
                "type": "A"
            },
            "resolved_ip": [
                "67.43.156.1",
                "67.43.156.2",
                "67.43.156.3"
            ],
            "response_code": "0"
          }
        });
        let r1 = apache_avro::to_value(jv1)
            .unwrap()
            .resolve(&schema)
            .unwrap();

        let mut w = apache_avro::Writer::new(&schema, vec![]);
        w.append(r1).unwrap();
        let bytes = w.into_inner().unwrap();

        let mut reader = ReaderBuilder::new()
            .read_schema()
            .with_batch_size(1)
            .build(std::io::Cursor::new(bytes))
            .unwrap();

        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);

        let expected = [
            "+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| dns                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |",
            "+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| {answers: [{class: , data: CHNlY3VyaXR5BnVidW50dQMjb20AAAEAAQAAAAgABLl9vic=, name: , ttl: , type: 1}, {class: , data: CHNlY3VyaXR5BnVidW50dQNjb20AAAEAABAAAAgABLl9viQ=, name: , ttl: , type: 1}, {class: , data: CHNlT3VyaXR5BnVidW50dQNjb20AAAEAAQAAAAgABFu9Wyc=, name: , ttl: , type: 1}], header_flags: , id: , op_code: , question: {class: , name: security.ubuntu.com, registered_domain: , subdomain: , top_level_domain: , type: A}, resolved_ip: [67.43.156.1, 67.43.156.2, 67.43.156.3], response_code: 0, type: } |",
            "+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
        ];
        assert_batches_eq!(expected, &[batch]);
    }

    #[test]
    fn test_deep_nullable_struct() {
        let schema = apache_avro::Schema::parse_str(
            r#"
            {
                "type": "record",
                "name": "r1",
                "fields": [
                  {
                    "name": "col1",
                    "type": [
                      "null",
                      {
                        "type": "record",
                        "name": "r2",
                        "fields": [
                          {
                            "name": "col2",
                            "type": [
                              "null",
                              {
                                "type": "record",
                                "name": "r3",
                                "fields": [
                                  {
                                    "name": "col3",
                                    "type": [
                                      "null",
                                      {
                                        "type": "record",
                                        "name": "r4",
                                        "fields": [
                                          {
                                            "name": "col4",
                                            "type": [
                                              "null",
                                              {
                                                "type": "record",
                                                "name": "r5",
                                                "fields": [
                                                  {
                                                    "name": "col5",
                                                    "type": ["null", "string"]
                                                  }
                                                ]
                                              }
                                            ]
                                          }
                                        ]
                                      }
                                    ]
                                  }
                                ]
                              }
                            ]
                          }
                        ]
                      }
                    ]
                  }
                ]
              }
            "#,
        )
        .unwrap();
        let r1 = apache_avro::to_value(serde_json::json!({
            "col1": {
                "col2": {
                    "col3": {
                        "col4": {
                            "col5": "hello"
                        }
                    }
                }
            }
        }))
        .unwrap()
        .resolve(&schema)
        .unwrap();
        let r2 = apache_avro::to_value(serde_json::json!({
            "col1": {
                "col2": {
                    "col3": {
                        "col4": {
                            "col5": null
                        }
                    }
                }
            }
        }))
        .unwrap()
        .resolve(&schema)
        .unwrap();
        let r3 = apache_avro::to_value(serde_json::json!({
            "col1": {
                "col2": {
                    "col3": null
                }
            }
        }))
        .unwrap()
        .resolve(&schema)
        .unwrap();
        let r4 = apache_avro::to_value(serde_json::json!({ "col1": null }))
            .unwrap()
            .resolve(&schema)
            .unwrap();

        let mut w = apache_avro::Writer::new(&schema, vec![]);
        w.append(r1).unwrap();
        w.append(r2).unwrap();
        w.append(r3).unwrap();
        w.append(r4).unwrap();
        let bytes = w.into_inner().unwrap();

        let mut reader = ReaderBuilder::new()
            .read_schema()
            .with_batch_size(4)
            .build(std::io::Cursor::new(bytes))
            .unwrap();

        let batch = reader.next().unwrap().unwrap();

        let expected = [
            "+---------------------------------------+",
            "| col1                                  |",
            "+---------------------------------------+",
            "| {col2: {col3: {col4: {col5: hello}}}} |",
            "| {col2: {col3: {col4: {col5: }}}}      |",
            "| {col2: {col3: }}                      |",
            "|                                       |",
            "+---------------------------------------+",
        ];
        assert_batches_eq!(expected, &[batch]);
    }

    #[test]
    fn test_avro_nullable_struct() {
        let schema = apache_avro::Schema::parse_str(
            r#"
            {
              "type": "record",
              "name": "r1",
              "fields": [
                {
                  "name": "col1",
                  "type": [
                    "null",
                    {
                      "type": "record",
                      "name": "r2",
                      "fields": [
                        {
                          "name": "col2",
                          "type": ["null", "string"]
                        }
                      ]
                    }
                  ],
                  "default": null
                }
              ]
            }"#,
        )
        .unwrap();
        let r1 = apache_avro::to_value(serde_json::json!({ "col1": null }))
            .unwrap()
            .resolve(&schema)
            .unwrap();
        let r2 = apache_avro::to_value(serde_json::json!({
            "col1": {
                "col2": "hello"
            }
        }))
        .unwrap()
        .resolve(&schema)
        .unwrap();
        let r3 = apache_avro::to_value(serde_json::json!({
            "col1": {
                "col2": null
            }
        }))
        .unwrap()
        .resolve(&schema)
        .unwrap();

        let mut w = apache_avro::Writer::new(&schema, vec![]);
        w.append(r1).unwrap();
        w.append(r2).unwrap();
        w.append(r3).unwrap();
        let bytes = w.into_inner().unwrap();

        let mut reader = ReaderBuilder::new()
            .read_schema()
            .with_batch_size(3)
            .build(std::io::Cursor::new(bytes))
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 1);

        let expected = [
            "+---------------+",
            "| col1          |",
            "+---------------+",
            "|               |",
            "| {col2: hello} |",
            "| {col2: }      |",
            "+---------------+",
        ];
        assert_batches_eq!(expected, &[batch]);
    }

    #[test]
    fn test_avro_nullable_struct_array() {
        let schema = apache_avro::Schema::parse_str(
            r#"
            {
              "type": "record",
              "name": "r1",
              "fields": [
                {
                  "name": "col1",
                  "type": [
                    "null",
                    {
                        "type": "array",
                        "items": {
                            "type": [
                                "null",
                                {
                                    "type": "record",
                                    "name": "Item",
                                    "fields": [
                                        {
                                            "name": "id",
                                            "type": "long"
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                  ],
                  "default": null
                }
              ]
            }"#,
        )
        .unwrap();
        let jv1 = serde_json::json!({
            "col1": [
                {
                    "id": 234
                },
                {
                    "id": 345
                }
            ]
        });
        let r1 = apache_avro::to_value(jv1)
            .unwrap()
            .resolve(&schema)
            .unwrap();
        let r2 = apache_avro::to_value(serde_json::json!({ "col1": null }))
            .unwrap()
            .resolve(&schema)
            .unwrap();

        let mut w = apache_avro::Writer::new(&schema, vec![]);
        for _i in 0..5 {
            w.append(r1.clone()).unwrap();
        }
        w.append(r2).unwrap();
        let bytes = w.into_inner().unwrap();

        let mut reader = ReaderBuilder::new()
            .read_schema()
            .with_batch_size(20)
            .build(std::io::Cursor::new(bytes))
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 6);
        assert_eq!(batch.num_columns(), 1);

        let expected = [
            "+------------------------+",
            "| col1                   |",
            "+------------------------+",
            "| [{id: 234}, {id: 345}] |",
            "| [{id: 234}, {id: 345}] |",
            "| [{id: 234}, {id: 345}] |",
            "| [{id: 234}, {id: 345}] |",
            "| [{id: 234}, {id: 345}] |",
            "|                        |",
            "+------------------------+",
        ];
        assert_batches_eq!(expected, &[batch]);
    }

    #[test]
    fn test_avro_iterator() {
        let reader = build_reader("alltypes_plain.avro", 5);
        let schema = reader.schema();
        let (col_id_index, _) = schema.column_with_name("id").unwrap();

        let mut sum_num_rows = 0;
        let mut num_batches = 0;
        let mut sum_id = 0;
        for batch in reader {
            let batch = batch.unwrap();
            assert_eq!(11, batch.num_columns());
            sum_num_rows += batch.num_rows();
            num_batches += 1;
            let batch_schema = batch.schema();
            assert_eq!(schema, batch_schema);
            let a_array = as_int32_array(batch.column(col_id_index)).unwrap();
            sum_id += (0..a_array.len()).map(|i| a_array.value(i)).sum::<i32>();
        }
        assert_eq!(8, sum_num_rows);
        assert_eq!(2, num_batches);
        assert_eq!(28, sum_id);
    }

    #[test]
    fn test_list_of_structs_with_custom_field_name() {
        let schema = apache_avro::Schema::parse_str(
            r#"
        {
          "type": "record",
          "name": "root",
          "fields": [
            {
              "name": "items",
              "type": {
                "type": "array",
                "items": {
                  "type": "record",
                  "name": "item_record",
                  "fields": [
                    {
                      "name": "id",
                      "type": "long"
                    },
                    {
                      "name": "name",
                      "type": "string"
                    }
                  ]
                }
              }
            }
          ]
        }"#,
        )
        .unwrap();

        let r1 = apache_avro::to_value(serde_json::json!({
            "items": [
                {
                    "id": 1,
                    "name": "first"
                },
                {
                    "id": 2,
                    "name": "second"
                }
            ]
        }))
        .unwrap()
        .resolve(&schema)
        .unwrap();

        let mut w = apache_avro::Writer::new(&schema, vec![]);
        w.append(r1).unwrap();
        let bytes = w.into_inner().unwrap();

        // Create an Arrow schema where the list field is NOT named "element"
        let arrow_schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "items",
            DataType::List(Arc::new(Field::new(
                "item", // This is NOT "element"
                DataType::Struct(Fields::from(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("name", DataType::Utf8, false),
                ])),
                false,
            ))),
            false,
        )]));

        let mut reader = ReaderBuilder::new()
            .with_schema(arrow_schema)
            .with_batch_size(10)
            .build(std::io::Cursor::new(bytes))
            .unwrap();

        // This used to fail because schema_lookup would have "items.element.id" and "items.element.name"
        // but build_struct_array will try to look up "items.item.id" and "items.item.name",
        // Now it it is simply "items.id" and "items.name"
        let batch = reader.next().unwrap().unwrap();

        let expected = [
            "+-----------------------------------------------+",
            "| items                                         |",
            "+-----------------------------------------------+",
            "| [{id: 1, name: first}, {id: 2, name: second}] |",
            "+-----------------------------------------------+",
        ];
        assert_batches_eq!(expected, &[batch]);
    }

    #[test]
    fn test_avro_record_ref() {
        // This schema defines an Address record once, then references it multiple times
        let schema = apache_avro::Schema::parse_str(
            r#"
    {
      "type": "record",
      "name": "Person",
      "fields": [
        {
          "name": "name",
          "type": "string"
        },
        {
          "name": "home_address",
          "type": {
            "type": "record",
            "name": "Address",
            "fields": [
              {
                "name": "street",
                "type": "string"
              },
              {
                "name": "city",
                "type": "string"
              },
              {
                "name": "zip",
                "type": "int"
              }
            ]
          }
        },
        {
          "name": "work_address",
          "type": "Address"
        },
        {
          "name": "billing_address",
          "type": ["null", "Address"]
        }
      ]
    }"#,
        )
        .unwrap();

        let person1 = apache_avro::to_value(serde_json::json!({
            "name": "Alice",
            "home_address": {
                "street": "123 Main St",
                "city": "Springfield",
                "zip": 12345
            },
            "work_address": {
                "street": "456 Business Ave",
                "city": "Metropolis",
                "zip": 67890
            },
            "billing_address": {
                "street": "789 Payment Ln",
                "city": "Capital City",
                "zip": 11111
            }
        }))
        .unwrap()
        .resolve(&schema)
        .unwrap();

        let person2 = apache_avro::to_value(serde_json::json!({
            "name": "Bob",
            "home_address": {
                "street": "321 Oak Dr",
                "city": "Shelbyville",
                "zip": 54321
            },
            "work_address": {
                "street": "654 Corporate Blvd",
                "city": "Tech City",
                "zip": 98765
            },
            "billing_address": null
        }))
        .unwrap()
        .resolve(&schema)
        .unwrap();

        let mut w = apache_avro::Writer::new(&schema, vec![]);
        w.append(person1).unwrap();
        w.append(person2).unwrap();
        let bytes = w.into_inner().unwrap();

        // Define the Arrow schema explicitly to avoid schema conversion with Refs
        let address_fields = Fields::from(vec![
            Field::new("street", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
            Field::new("zip", DataType::Int32, false),
        ]);

        let arrow_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new(
                "home_address",
                DataType::Struct(address_fields.clone()),
                false,
            ),
            Field::new(
                "work_address",
                DataType::Struct(address_fields.clone()),
                false,
            ),
            Field::new("billing_address", DataType::Struct(address_fields), true),
        ]));

        let mut reader = ReaderBuilder::new()
            .with_schema(arrow_schema)
            .with_batch_size(2)
            .build(std::io::Cursor::new(bytes))
            .unwrap();

        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);

        let expected = [
            "+-------+------------------------------------------------------+-----------------------------------------------------------+----------------------------------------------------------+",
            "| name  | home_address                                         | work_address                                              | billing_address                                          |",
            "+-------+------------------------------------------------------+-----------------------------------------------------------+----------------------------------------------------------+",
            "| Alice | {street: 123 Main St, city: Springfield, zip: 12345} | {street: 456 Business Ave, city: Metropolis, zip: 67890}  | {street: 789 Payment Ln, city: Capital City, zip: 11111} |",
            "| Bob   | {street: 321 Oak Dr, city: Shelbyville, zip: 54321}  | {street: 654 Corporate Blvd, city: Tech City, zip: 98765} |                                                          |",
            "+-------+------------------------------------------------------+-----------------------------------------------------------+----------------------------------------------------------+",
        ];
        assert_batches_eq!(expected, &[batch]);
    }

    #[test]
    fn test_avro_enum_ref() {
        let schema = apache_avro::Schema::parse_str(
            r#"
            {
              "type": "record",
              "name": "Product",
              "fields": [
                {
                  "name": "name",
                  "type": "string"
                },
                {
                  "name": "primary_category",
                  "type": {
                    "type": "enum",
                    "name": "Category",
                    "symbols": ["ELECTRONICS", "CLOTHING", "FOOD", "BOOKS"]
                  }
                },
                {
                  "name": "secondary_category",
                  "type": ["null", "Category"]
                },
                {
                  "name": "tertiary_category",
                  "type": "Category"
                }
              ]
            }"#,
        )
        .unwrap();

        let p1 = apache_avro::to_value(serde_json::json!({
            "name": "Laptop",
            "primary_category": "ELECTRONICS",
            "secondary_category": "ELECTRONICS",
            "tertiary_category": "ELECTRONICS"
        }))
        .unwrap()
        .resolve(&schema)
        .unwrap();

        let p2 = apache_avro::to_value(serde_json::json!({
            "name": "T-Shirt",
            "primary_category": "CLOTHING",
            "secondary_category": null,
            "tertiary_category": "CLOTHING"
        }))
        .unwrap()
        .resolve(&schema)
        .unwrap();

        let mut w = apache_avro::Writer::new(&schema, vec![]);
        w.append(p1).unwrap();
        w.append(p2).unwrap();
        let bytes = w.into_inner().unwrap();

        // Define Arrow schema explicitly since read_schema doesn't support Refs yet
        let arrow_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("primary_category", DataType::Utf8, false),
            Field::new("secondary_category", DataType::Utf8, true),
            Field::new("tertiary_category", DataType::Utf8, false),
        ]));

        let mut reader = ReaderBuilder::new()
            .with_schema(arrow_schema)
            .with_batch_size(2)
            .build(std::io::Cursor::new(bytes))
            .unwrap();

        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);

        let expected = [
            "+---------+------------------+--------------------+-------------------+",
            "| name    | primary_category | secondary_category | tertiary_category |",
            "+---------+------------------+--------------------+-------------------+",
            "| Laptop  | ELECTRONICS      | ELECTRONICS        | ELECTRONICS       |",
            "| T-Shirt | CLOTHING         |                    | CLOTHING          |",
            "+---------+------------------+--------------------+-------------------+",
        ];
        assert_batches_eq!(expected, &[batch]);
    }

    #[test]
    fn test_avro_fixed_ref() {
        let schema = apache_avro::Schema::parse_str(
            r#"
            {
              "type": "record",
              "name": "SecurityEvent",
              "fields": [
                {
                  "name": "event_id",
                  "type": "string"
                },
                {
                  "name": "hash1",
                  "type": {
                    "type": "fixed",
                    "name": "MD5Hash",
                    "size": 16
                  }
                },
                {
                  "name": "hash2",
                  "type": "MD5Hash"
                },
                {
                  "name": "optional_hash",
                  "type": ["null", "MD5Hash"]
                }
              ]
            }"#,
        )
        .unwrap();

        // For Avro fixed types, we need to use apache_avro::types::Value::Fixed directly
        use apache_avro::types::Value;

        let hash1_bytes = vec![1u8; 16];
        let hash2_bytes = vec![2u8; 16];
        let hash3_bytes = vec![3u8; 16];

        let e1 = Value::Record(vec![
            ("event_id".to_string(), Value::String("evt001".to_string())),
            ("hash1".to_string(), Value::Fixed(16, hash1_bytes.clone())),
            ("hash2".to_string(), Value::Fixed(16, hash2_bytes.clone())),
            (
                "optional_hash".to_string(),
                Value::Union(1, Box::new(Value::Fixed(16, hash3_bytes.clone()))),
            ),
        ]);

        let e2 = Value::Record(vec![
            ("event_id".to_string(), Value::String("evt002".to_string())),
            ("hash1".to_string(), Value::Fixed(16, hash2_bytes.clone())),
            ("hash2".to_string(), Value::Fixed(16, hash1_bytes.clone())),
            (
                "optional_hash".to_string(),
                Value::Union(0, Box::new(Value::Null)),
            ),
        ]);

        let mut w = apache_avro::Writer::new(&schema, vec![]);
        w.append(e1).unwrap();
        w.append(e2).unwrap();
        let bytes = w.into_inner().unwrap();

        // Define Arrow schema explicitly
        let arrow_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("event_id", DataType::Utf8, false),
            Field::new("hash1", DataType::FixedSizeBinary(16), false),
            Field::new("hash2", DataType::FixedSizeBinary(16), false),
            Field::new("optional_hash", DataType::FixedSizeBinary(16), true),
        ]));

        let mut reader = ReaderBuilder::new()
            .with_schema(arrow_schema)
            .with_batch_size(2)
            .build(std::io::Cursor::new(bytes))
            .unwrap();

        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);

        // Verify the data types
        let schema = batch.schema();
        assert_eq!(schema.field(1).data_type(), &DataType::FixedSizeBinary(16));
        assert_eq!(schema.field(2).data_type(), &DataType::FixedSizeBinary(16));
        assert_eq!(schema.field(3).data_type(), &DataType::FixedSizeBinary(16));

        // Verify we can read the data
        let hash1_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(hash1_array.len(), 2);
        assert_eq!(hash1_array.value(0), hash1_bytes.as_slice());
        assert_eq!(hash1_array.value(1), hash2_bytes.as_slice());
    }

    #[test]
    fn test_avro_nested_record_ref() {
        // Test record refs nested within arrays and other records
        let schema = apache_avro::Schema::parse_str(
            r#"
            {
              "type": "record",
              "name": "Organization",
              "fields": [
                {
                  "name": "name",
                  "type": "string"
                },
                {
                  "name": "primary_contact",
                  "type": {
                    "type": "record",
                    "name": "Contact",
                    "fields": [
                      {
                        "name": "name",
                        "type": "string"
                      },
                      {
                        "name": "email",
                        "type": "string"
                      }
                    ]
                  }
                },
                {
                  "name": "secondary_contact",
                  "type": ["null", "Contact"]
                },
                {
                  "name": "all_contacts",
                  "type": {
                    "type": "array",
                    "items": "Contact"
                  }
                }
              ]
            }"#,
        )
        .unwrap();

        let o1 = apache_avro::to_value(serde_json::json!({
            "name": "Acme Corp",
            "primary_contact": {
                "name": "Alice",
                "email": "alice@acme.com"
            },
            "secondary_contact": {
                "name": "Bob",
                "email": "bob@acme.com"
            },
            "all_contacts": [
                {
                    "name": "Alice",
                    "email": "alice@acme.com"
                },
                {
                    "name": "Bob",
                    "email": "bob@acme.com"
                },
                {
                    "name": "Charlie",
                    "email": "charlie@acme.com"
                }
            ]
        }))
        .unwrap()
        .resolve(&schema)
        .unwrap();

        let o2 = apache_avro::to_value(serde_json::json!({
            "name": "Beta Inc",
            "primary_contact": {
                "name": "Dave",
                "email": "dave@beta.com"
            },
            "secondary_contact": null,
            "all_contacts": [
                {
                    "name": "Dave",
                    "email": "dave@beta.com"
                }
            ]
        }))
        .unwrap()
        .resolve(&schema)
        .unwrap();

        let mut w = apache_avro::Writer::new(&schema, vec![]);
        w.append(o1).unwrap();
        w.append(o2).unwrap();
        let bytes = w.into_inner().unwrap();

        // Define Arrow schema explicitly
        let contact_fields = Fields::from(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, false),
        ]);

        let arrow_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new(
                "primary_contact",
                DataType::Struct(contact_fields.clone()),
                false,
            ),
            Field::new(
                "secondary_contact",
                DataType::Struct(contact_fields.clone()),
                true,
            ),
            Field::new(
                "all_contacts",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(contact_fields),
                    false,
                ))),
                false,
            ),
        ]));

        let mut reader = ReaderBuilder::new()
            .with_schema(arrow_schema)
            .with_batch_size(2)
            .build(std::io::Cursor::new(bytes))
            .unwrap();

        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);

        let expected = [
            "+-----------+--------------------------------------+----------------------------------+--------------------------------------------------------------------------------------------------------------------+",
            "| name      | primary_contact                      | secondary_contact                | all_contacts                                                                                                       |",
            "+-----------+--------------------------------------+----------------------------------+--------------------------------------------------------------------------------------------------------------------+",
            "| Acme Corp | {name: Alice, email: alice@acme.com} | {name: Bob, email: bob@acme.com} | [{name: Alice, email: alice@acme.com}, {name: Bob, email: bob@acme.com}, {name: Charlie, email: charlie@acme.com}] |",
            "| Beta Inc  | {name: Dave, email: dave@beta.com}   |                                  | [{name: Dave, email: dave@beta.com}]                                                                               |",
            "+-----------+--------------------------------------+----------------------------------+--------------------------------------------------------------------------------------------------------------------+",
        ];
        assert_batches_eq!(expected, &[batch]);
    }

    #[test]
    fn test_avro_combined_refs() {
        // Test combining record, enum, and fixed refs in a single schema
        let schema = apache_avro::Schema::parse_str(
            r#"
        {
          "type": "record",
          "name": "Transaction",
          "fields": [
            {
              "name": "id",
              "type": "string"
            },
            {
              "name": "status",
              "type": {
                "type": "enum",
                "name": "Status",
                "symbols": ["PENDING", "APPROVED", "REJECTED", "CANCELLED"]
              }
            },
            {
              "name": "previous_status",
              "type": ["null", "Status"]
            },
            {
              "name": "signature",
              "type": {
                "type": "fixed",
                "name": "Signature",
                "size": 32
              }
            },
            {
              "name": "backup_signature",
              "type": ["null", "Signature"]
            },
            {
              "name": "user",
              "type": {
                "type": "record",
                "name": "User",
                "fields": [
                  {
                    "name": "id",
                    "type": "string"
                  },
                  {
                    "name": "role",
                    "type": "Status"
                  }
                ]
              }
            },
            {
              "name": "approver",
              "type": ["null", "User"]
            },
            {
              "name": "status_history",
              "type": {
                "type": "array",
                "items": "Status"
              }
            }
          ]
        }"#,
        )
        .unwrap();

        use apache_avro::types::Value;

        let sig1 = vec![1u8; 32];
        let sig2 = vec![2u8; 32];

        let t1 = Value::Record(vec![
            ("id".to_string(), Value::String("txn001".to_string())),
            ("status".to_string(), Value::Enum(1, "APPROVED".to_string())),
            (
                "previous_status".to_string(),
                Value::Union(1, Box::new(Value::Enum(0, "PENDING".to_string()))),
            ),
            ("signature".to_string(), Value::Fixed(32, sig1.clone())),
            (
                "backup_signature".to_string(),
                Value::Union(1, Box::new(Value::Fixed(32, sig2.clone()))),
            ),
            (
                "user".to_string(),
                Value::Record(vec![
                    ("id".to_string(), Value::String("user001".to_string())),
                    ("role".to_string(), Value::Enum(1, "APPROVED".to_string())),
                ]),
            ),
            (
                "approver".to_string(),
                Value::Union(
                    1,
                    Box::new(Value::Record(vec![
                        ("id".to_string(), Value::String("admin001".to_string())),
                        ("role".to_string(), Value::Enum(1, "APPROVED".to_string())),
                    ])),
                ),
            ),
            (
                "status_history".to_string(),
                Value::Array(vec![
                    Value::Enum(0, "PENDING".to_string()),
                    Value::Enum(1, "APPROVED".to_string()),
                ]),
            ),
        ]);

        let t2 = Value::Record(vec![
            ("id".to_string(), Value::String("txn002".to_string())),
            ("status".to_string(), Value::Enum(2, "REJECTED".to_string())),
            (
                "previous_status".to_string(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            ("signature".to_string(), Value::Fixed(32, sig2.clone())),
            (
                "backup_signature".to_string(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "user".to_string(),
                Value::Record(vec![
                    ("id".to_string(), Value::String("user002".to_string())),
                    ("role".to_string(), Value::Enum(2, "REJECTED".to_string())),
                ]),
            ),
            (
                "approver".to_string(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "status_history".to_string(),
                Value::Array(vec![
                    Value::Enum(0, "PENDING".to_string()),
                    Value::Enum(2, "REJECTED".to_string()),
                ]),
            ),
        ]);

        let mut w = apache_avro::Writer::new(&schema, vec![]);
        w.append(t1).unwrap();
        w.append(t2).unwrap();
        let bytes = w.into_inner().unwrap();

        // Define Arrow schema explicitly
        let user_fields = Fields::from(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("role", DataType::Utf8, false),
        ]);

        let arrow_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
            Field::new("previous_status", DataType::Utf8, true),
            Field::new("signature", DataType::FixedSizeBinary(32), false),
            Field::new("backup_signature", DataType::FixedSizeBinary(32), true),
            Field::new("user", DataType::Struct(user_fields.clone()), false),
            Field::new("approver", DataType::Struct(user_fields), true),
            Field::new(
                "status_history",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
                false,
            ),
        ]));

        let mut reader = ReaderBuilder::new()
            .with_schema(arrow_schema)
            .with_batch_size(2)
            .build(std::io::Cursor::new(bytes))
            .unwrap();

        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 8);

        // Verify the schema types
        let schema = batch.schema();
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8); // enum as string
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8); // nullable enum as string
        assert_eq!(schema.field(3).data_type(), &DataType::FixedSizeBinary(32));
        assert_eq!(schema.field(4).data_type(), &DataType::FixedSizeBinary(32));
        assert!(matches!(schema.field(5).data_type(), DataType::Struct(_))); // User record
        assert!(matches!(schema.field(6).data_type(), DataType::Struct(_))); // nullable User
        assert!(matches!(schema.field(7).data_type(), DataType::List(_))); // status_history

        // Verify data content - check a few key fields
        let status_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(status_array.value(0), "APPROVED");
        assert_eq!(status_array.value(1), "REJECTED");

        let sig_array = batch
            .column(3)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(sig_array.value(0), sig1.as_slice());
        assert_eq!(sig_array.value(1), sig2.as_slice());
    }

    #[test]
    fn test_avro_multiple_record_refs() {
        // Test multiple different record types being referenced
        let schema = apache_avro::Schema::parse_str(
            r#"
        {
          "type": "record",
          "name": "Order",
          "fields": [
            {
              "name": "order_id",
              "type": "string"
            },
            {
              "name": "shipping_address",
              "type": {
                "type": "record",
                "name": "Address",
                "fields": [
                  {
                    "name": "street",
                    "type": "string"
                  },
                  {
                    "name": "city",
                    "type": "string"
                  }
                ]
              }
            },
            {
              "name": "billing_address",
              "type": "Address"
            },
            {
              "name": "customer",
              "type": {
                "type": "record",
                "name": "Customer",
                "fields": [
                  {
                    "name": "name",
                    "type": "string"
                  },
                  {
                    "name": "home_address",
                    "type": "Address"
                  }
                ]
              }
            },
            {
              "name": "gift_recipient",
              "type": ["null", "Customer"]
            }
          ]
        }"#,
        )
        .unwrap();

        let o1 = apache_avro::to_value(serde_json::json!({
            "order_id": "ord001",
            "shipping_address": {
                "street": "123 Ship St",
                "city": "Shipping City"
            },
            "billing_address": {
                "street": "456 Bill Ave",
                "city": "Billing Town"
            },
            "customer": {
                "name": "Alice",
                "home_address": {
                    "street": "789 Home Rd",
                    "city": "Home City"
                }
            },
            "gift_recipient": {
                "name": "Bob",
                "home_address": {
                    "street": "321 Gift Ln",
                    "city": "Gift Town"
                }
            }
        }))
        .unwrap()
        .resolve(&schema)
        .unwrap();

        let o2 = apache_avro::to_value(serde_json::json!({
            "order_id": "ord002",
            "shipping_address": {
                "street": "111 Main St",
                "city": "Main City"
            },
            "billing_address": {
                "street": "111 Main St",
                "city": "Main City"
            },
            "customer": {
                "name": "Charlie",
                "home_address": {
                    "street": "111 Main St",
                    "city": "Main City"
                }
            },
            "gift_recipient": null
        }))
        .unwrap()
        .resolve(&schema)
        .unwrap();

        let mut w = apache_avro::Writer::new(&schema, vec![]);
        w.append(o1).unwrap();
        w.append(o2).unwrap();
        let bytes = w.into_inner().unwrap();

        // Define Arrow schema explicitly
        // When Address is used in nullable contexts (like inside nullable Customer),
        // its fields need to be nullable too
        let address_fields = Fields::from(vec![
            Field::new("street", DataType::Utf8, true),
            Field::new("city", DataType::Utf8, true),
        ]);

        let customer_fields = Fields::from(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "home_address",
                DataType::Struct(address_fields.clone()),
                true,
            ),
        ]);

        let arrow_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("order_id", DataType::Utf8, false),
            Field::new(
                "shipping_address",
                DataType::Struct(address_fields.clone()),
                false,
            ),
            Field::new("billing_address", DataType::Struct(address_fields), false),
            Field::new("customer", DataType::Struct(customer_fields.clone()), false),
            Field::new("gift_recipient", DataType::Struct(customer_fields), true),
        ]));

        let mut reader = ReaderBuilder::new()
            .with_schema(arrow_schema)
            .with_batch_size(2)
            .build(std::io::Cursor::new(bytes))
            .unwrap();

        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 5);

        let expected = [
            "+----------+--------------------------------------------+--------------------------------------------+-----------------------------------------------------------------------+-------------------------------------------------------------------+",
            "| order_id | shipping_address                           | billing_address                            | customer                                                              | gift_recipient                                                    |",
            "+----------+--------------------------------------------+--------------------------------------------+-----------------------------------------------------------------------+-------------------------------------------------------------------+",
            "| ord001   | {street: 123 Ship St, city: Shipping City} | {street: 456 Bill Ave, city: Billing Town} | {name: Alice, home_address: {street: 789 Home Rd, city: Home City}}   | {name: Bob, home_address: {street: 321 Gift Ln, city: Gift Town}} |",
            "| ord002   | {street: 111 Main St, city: Main City}     | {street: 111 Main St, city: Main City}     | {name: Charlie, home_address: {street: 111 Main St, city: Main City}} |                                                                   |",
            "+----------+--------------------------------------------+--------------------------------------------+-----------------------------------------------------------------------+-------------------------------------------------------------------+"
        ];
        assert_batches_eq!(expected, &[batch]);
    }
}
