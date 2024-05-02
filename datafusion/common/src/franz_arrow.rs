use core::slice;
use std::io::Cursor;
use std::{collections::HashMap, io::BufWriter, sync::Arc};

use apache_avro::{from_avro_datum, types::Value, Error, Schema as AvSchema, Writer};
use arrow::array::{Array, ListArray, StringBuilder, StructArray};
use arrow::array::{
    BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
};
use arrow::datatypes::Fields;
use arrow::error::ArrowError;
use arrow::{
    array::{GenericListBuilder, ListBuilder},
    buffer::OffsetBuffer,
    datatypes::Field,
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use arrow_json::ReaderBuilder;
use base64::{engine::general_purpose, Engine as _};
use serde_json::{json, Value as JValue};

//use crate::record::Record;

pub trait Decoder {
    fn decode(&self, data: &[u8]) -> RecordBatch;
    fn decode_avro(&self, data: &[u8]) -> Value;
    fn write_to_avro_file(&self, value: Value) -> Result<(), Error>;
}

pub struct AvroSchema {
    pub schema: AvSchema,
}

/// .
///
/// # Panics
///
/// Panics if .
pub fn json_records_to_arrow_record_batch(
    records: Vec<serde_json::Value>,
    schema: Arc<Schema>,
) -> RecordBatch {
    if records.len() == 0 {
        return RecordBatch::new_empty(schema);
    }
    let string_stream: Vec<String> = records.iter().map(|r| r.to_string()).collect();
    let cursor: Cursor<String> = Cursor::new(string_stream.join("\n"));

    let mut reader = ReaderBuilder::new(schema).build(cursor).unwrap();
    reader.next().unwrap().unwrap()
}

pub fn avro_value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => json!(null),
        Value::Boolean(b) => json!(b),
        Value::Int(i) => json!(i),
        Value::Long(l) => json!(l),
        Value::Float(f) => json!(f),
        Value::Double(d) => json!(d),
        Value::Bytes(bytes) => json!(general_purpose::STANDARD.encode(bytes)),
        Value::String(s) => json!(s),
        Value::Fixed(_, bytes) => json!(general_purpose::STANDARD.encode(bytes)),
        Value::Enum(_, symbol) => json!(symbol),
        Value::Union(_, boxed_value) => avro_value_to_json(boxed_value),
        Value::Array(items) => {
            json!(items.iter().map(avro_value_to_json).collect::<Vec<_>>())
        }
        Value::Map(map) => {
            let json_map: HashMap<_, _> = map
                .iter()
                .map(|(k, v)| (k, avro_value_to_json(v)))
                .collect();
            json!(json_map)
        }
        Value::Record(fields) => {
            let json_fields: serde_json::Value = fields
                .iter()
                .map(|(k, v)| (k, avro_value_to_json(v)))
                .collect();
            json!(json_fields)
        }
        Value::Date(d) => json!(d),
        _ => unimplemented!(),
    }
}

pub fn json_to_avro_value(
    json_value: &JValue,
    schema: &AvSchema,
) -> Result<Value, String> {
    // Add schema as input
    match json_value {
        JValue::Null => Ok(Value::Null),
        JValue::Bool(b) => Ok(Value::Boolean(*b)),
        JValue::Number(n) => {
            if n.is_i64() {
                Ok(Value::Long(n.as_i64().unwrap()))
            } else if n.is_f64() {
                Ok(Value::Double(n.as_f64().unwrap()))
            } else {
                Err("Unsupported numeric type for Avro".to_string())
            }
        }
        JValue::String(s) => Ok(Value::String(s.clone())),
        JValue::Array(arr) => Ok(Value::Array(
            arr.iter()
                .map(|item| json_to_avro_value(item, schema))
                .collect::<Result<Vec<_>, String>>()?, // Handle potential errors
        )),
        JValue::Object(obj) => match schema {
            AvSchema::Record(record_schema) => {
                let fields = record_schema.fields.clone();
                let mut record_fields = Vec::with_capacity(fields.len());

                for field in fields {
                    let field_json = obj.get(&field.name).ok_or_else(|| {
                        format!("Missing field '{}' in JSON object", field.name)
                    })?;

                    let field_value = json_to_avro_value(field_json, &field.schema)?;
                    record_fields.push((field.name, field_value));
                }
                Ok(Value::Record(record_fields))
            }
            _ => Err("JSON object encountered, but schema is not a Record".to_string()),
        },
        // @TODO: analyzer says this is unreachable, is that true?
        _ => Err("Unsupported JSON type for Avro conversion".to_string()),
    }
}

pub fn avro_schema_to_json(schema: &AvSchema) -> Result<String, serde_json::Error> {
    serde_json::to_string(schema)
}

fn infer_arrow_schema_from_avro_value(value: &Value, name: String) -> Field {
    match value {
        Value::Null => Field::new(name, DataType::Null, false),
        Value::Boolean(_) => Field::new(name, DataType::Boolean, false),
        Value::Int(_) => Field::new(name, DataType::Int32, true),
        Value::Long(_) => Field::new(name, DataType::Int64, true),
        Value::Float(_) => Field::new(name, DataType::Float32, true),
        Value::Double(_) => Field::new(name, DataType::Float64, true),
        Value::Bytes(_) | Value::Fixed(_, _) => Field::new(name, DataType::Binary, true),
        Value::String(_) => Field::new(name, DataType::Utf8, true),
        Value::Array(items) => {
            if let Some(first_item) = items.first() {
                let item_type = infer_arrow_schema_from_avro_value(
                    first_item,
                    format!("{}_item", name),
                );
                Field::new(name, DataType::List(Arc::new(item_type)), true)
            } else {
                Field::new(
                    name,
                    DataType::List(Arc::new(Field::new("item", DataType::Null, true))),
                    true,
                )
            }
        }
        Value::Map(_) => Field::new(name, DataType::Utf8, true), // This might need to be more complex for non-string keys
        Value::Record(fields) => {
            let schema_fields: Vec<Field> = fields
                .iter()
                .map(|(field_name, value)| {
                    infer_arrow_schema_from_avro_value(value, String::from(field_name))
                })
                .collect();
            Field::new(name, DataType::Struct(schema_fields.into()), true)
        }
        Value::Union(_, value) => infer_arrow_schema_from_avro_value(&value, name),
        Value::Enum(_, _) => todo!(),
        Value::Date(_) => todo!(),
        Value::Decimal(_) => todo!(),
        Value::TimeMillis(_) => todo!(),
        Value::TimeMicros(_) => todo!(),
        Value::TimestampMillis(_) => todo!(),
        Value::TimestampMicros(_) => todo!(),
        Value::LocalTimestampMillis(_) => todo!(),
        Value::LocalTimestampMicros(_) => todo!(),
        Value::Duration(_) => todo!(),
        Value::Uuid(_) => todo!(),
    }
}

fn avro_record_to_arrow_schema(record: &Value) -> Schema {
    match record {
        Value::Record(fields) => {
            let schema_fields: Vec<Field> = fields
                .iter()
                .map(|(field_name, value)| {
                    infer_arrow_schema_from_avro_value(value, String::from(field_name))
                })
                .collect();
            Schema::new(schema_fields)
        }
        _ => panic!("Excpected an avro record."),
    }
}

//TODO: Remove this with proper Union handling
fn strip_union_value<'a>(value: &'a Value) -> &'a Value {
    match value {
        Value::Union(_, inner_value) => inner_value.as_ref(),
        _ => value,
    }
}

/**
 * fn make_struct_array recursively converts values from an Avro Struct Record
 * into a StructArray.
 */
fn make_struct_array(fields: &Fields, values: &[Value]) -> Arc<StructArray> {
    let mut child_arrays: Vec<(Arc<Field>, Arc<dyn Array>)> = Vec::new();
    for field in fields {
        let target_name = field.name();
        let field_values: Vec<Value> = values
            .iter()
            .map(|v: &Value| {
                let extracted_value = strip_union_value(&v);
                match extracted_value {
                    Value::Record(r) => r
                        .iter()
                        .find(|(name, _)| name == target_name)
                        .unwrap()
                        .1
                        .clone(),
                    _ => panic!("Expected Avro Record"),
                }
            })
            .collect();
        let builder: Arc<dyn Array> =
            avro_values_to_arrow_array(field_values.as_slice(), field.data_type());
        child_arrays.push((field.clone(), builder));
    }
    Arc::new(StructArray::from(child_arrays))
}

fn infer_arrow_schema_fields_from_json_value(value: &JValue, name: String) -> Field {
    match value {
        JValue::Null => Field::new(&name, DataType::Null, true),
        JValue::Bool(_) => Field::new(&name, DataType::Boolean, true),
        JValue::Number(n) => {
            if n.is_f64() {
                Field::new(&name, DataType::Float64, true)
            } else if n.is_i64() {
                Field::new(&name, DataType::Int64, true)
            } else {
                // JSON numbers are either f64 or i64 in serde_json; assuming f64 if not i64 for simplicity
                Field::new(&name, DataType::Float64, true)
            }
        }
        JValue::String(_) => Field::new(&name, DataType::Utf8, true),
        JValue::Array(items) => {
            if let Some(first_item) = items.first() {
                let item_field = infer_arrow_schema_fields_from_json_value(
                    first_item,
                    format!("{}_item", name),
                );
                Field::new(&name, DataType::List(Arc::new(item_field)), true)
            } else {
                Field::new(
                    &name,
                    DataType::List(Arc::new(Field::new("item", DataType::Null, true))),
                    true,
                )
            }
        }
        JValue::Object(obj) => {
            let fields: Vec<Field> = obj
                .iter()
                .map(|(key, val)| {
                    infer_arrow_schema_fields_from_json_value(val, key.clone())
                })
                .collect();
            Field::new(&name, DataType::Struct(fields.into()), true)
        }
    }
}

/// Infers an Arrow schema from a JSON value.
/// This function attempts to construct an Arrow `Schema` by examining the structure of a JSON object.
/// It iterates over each field of the JSON object, recursively inferring the schema for each field.
/// If the input JSON value is not an object, it logs a warning and returns a `YaspError::DecodeError`.
///
/// # Arguments
///
/// * `value` - A reference to a JSON value from which to infer the schema.
///
/// # Returns
///
/// A `Result` which is `Ok(Schema)` if the schema could be successfully inferred from the JSON object,
/// or `Err(YaspError)` if the input is not a JSON object or if any other error occurs during schema inference.
pub fn infer_arrow_schema_from_json_value(value: &JValue) -> Result<Schema, ArrowError> {
    match value {
        JValue::Object(obj) => {
            let fields: Vec<Field> = obj
                .iter()
                .map(|(key, val)| {
                    infer_arrow_schema_fields_from_json_value(val, key.clone())
                })
                .collect();
            Ok(Schema::new(fields))
        }
        _ => Err(ArrowError::JsonError(
            "Was expecting a JSON object".to_string(),
        )),
    }
}
/**
 *  Main entry point to Avro > Arrow conversion. Currently handles primitive types, lists and nested structs.
 */
fn avro_values_to_arrow_array(values: &[Value], data_type: &DataType) -> Arc<dyn Array> {
    match data_type {
        DataType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for value in values {
                let extracted_value = strip_union_value(value);
                if let Value::Boolean(b) = extracted_value {
                    builder.append_value(*b);
                } else {
                    panic!("Type mismatch for Boolean");
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Int32 => {
            let mut builder = Int32Builder::new();
            for value in values {
                let extracted_value = strip_union_value(value);
                if let Value::Int(i) = extracted_value {
                    builder.append_value(*i);
                } else {
                    panic!("Type mismatch for Int32");
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::new();
            for value in values {
                let extracted_value = strip_union_value(value);
                if let Value::Long(l) = extracted_value {
                    builder.append_value(*l);
                } else {
                    panic!("Type mismatch for Int64");
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Float32 => {
            let mut builder = Float32Builder::new();
            for value in values {
                let extracted_value: &Value = strip_union_value(value);
                if let Value::Float(f) = extracted_value {
                    builder.append_value(*f);
                } else {
                    panic!("Type mismatch for Float32");
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::new();
            for value in values {
                let extracted_value: &Value = strip_union_value(value);
                if let Value::Double(d) = extracted_value {
                    builder.append_value(*d);
                } else {
                    panic!("Type mismatch for Float64");
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Utf8 => {
            let mut builder = arrow::array::StringBuilder::new();
            for value in values {
                let extracted_value = strip_union_value(value);
                if let Value::String(s) = extracted_value {
                    builder.append_value(s);
                } else {
                    panic!("Type mismatch for String");
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Struct(fields) => make_struct_array(fields, values),
        DataType::List(field) => {
            match field.data_type() {
                DataType::Int32 => {
                    let builder: arrow::array::PrimitiveBuilder<
                        arrow::datatypes::Int32Type,
                    > = Int32Builder::new();
                    let mut list_builder: GenericListBuilder<
                        i32,
                        arrow::array::PrimitiveBuilder<arrow::datatypes::Int32Type>,
                    > = ListBuilder::new(builder);
                    for value in values {
                        let extracted_value = strip_union_value(value);
                        if let Value::Array(arr) = extracted_value {
                            for item in arr {
                                let extracted_item = strip_union_value(item);
                                if let Value::Int(i) = extracted_item {
                                    list_builder.values().append_value(*i);
                                }
                            }
                            list_builder.append(true)
                        } else {
                            panic!("Expected an array.")
                        }
                    }
                    Arc::new(list_builder.finish())
                }
                DataType::Int64 => {
                    let builder = Int64Builder::new();
                    let mut list_builder = ListBuilder::new(builder);
                    for value in values {
                        let extracted_value = strip_union_value(value);
                        if let Value::Array(arr) = extracted_value {
                            for item in arr {
                                let extracted_item = strip_union_value(item);
                                if let Value::Long(i) = extracted_item {
                                    list_builder.values().append_value(*i);
                                }
                            }
                            list_builder.append(true)
                        } else {
                            panic!("Expected an array.")
                        }
                    }
                    Arc::new(list_builder.finish())
                }
                DataType::Float32 => {
                    let builder = Float32Builder::new();
                    let mut list_builder = ListBuilder::new(builder);
                    for value in values {
                        let extracted_value = strip_union_value(value);
                        if let Value::Array(arr) = extracted_value {
                            for item in arr {
                                let extracted_item = strip_union_value(item);
                                if let Value::Float(f) = extracted_item {
                                    list_builder.values().append_value(*f);
                                }
                            }
                            list_builder.append(true)
                        } else {
                            panic!("Expected an array.")
                        }
                    }
                    Arc::new(list_builder.finish())
                }
                DataType::Float64 => {
                    let builder = Float64Builder::new();
                    let mut list_builder = ListBuilder::new(builder);
                    for value in values {
                        let extracted_value = strip_union_value(value);
                        if let Value::Array(arr) = extracted_value {
                            for item in arr {
                                let extracted_item = strip_union_value(item);
                                if let Value::Double(d) = extracted_item {
                                    list_builder.values().append_value(*d);
                                }
                            }
                            list_builder.append(true)
                        } else {
                            panic!("Expected an array.")
                        }
                    }
                    Arc::new(list_builder.finish())
                }
                DataType::Utf8 => {
                    let builder = StringBuilder::new();
                    let mut list_builder = ListBuilder::new(builder);
                    for value in values {
                        let extracted_value = strip_union_value(value);
                        if let Value::Array(arr) = extracted_value {
                            for item in arr {
                                let extracted_item = strip_union_value(item);
                                if let Value::String(s) = extracted_item {
                                    list_builder.values().append_value(s);
                                }
                            }
                            list_builder.append(true)
                        } else {
                            panic!("Expected an array.")
                        }
                    }
                    Arc::new(list_builder.finish())
                }
                DataType::Struct(_) => {
                    let mut all_arrays: Vec<Arc<dyn Array>> = Vec::new();
                    for value in values {
                        let extracted_value = strip_union_value(value);
                        if let Value::Array(arr) = extracted_value {
                            let child_array: Arc<dyn Array> =
                                avro_values_to_arrow_array(arr, field.data_type());
                            all_arrays.push(child_array);
                        }
                    }
                    let mut offsets: Vec<usize> = vec![];
                    for array in &all_arrays {
                        offsets.push(array.len());
                    }
                    let offset_buffer: OffsetBuffer<i32> =
                        OffsetBuffer::<i32>::from_lengths(offsets);

                    let array_refs: Vec<&dyn Array> = all_arrays
                        .iter()
                        .map(|a: &Arc<dyn Array>| a.as_ref())
                        .collect();

                    let concatenated_array = arrow::compute::concat(&array_refs).unwrap();

                    //TODO: Add null handling. For now we test without nulls.
                    let list_array: arrow::array::GenericListArray<i32> = ListArray::new(
                        field.clone(),
                        offset_buffer,
                        Arc::new(concatenated_array.clone()) as Arc<dyn Array>,
                        None, // No validity bitmap
                    );
                    Arc::new(list_array)
                }
                DataType::List(field) => {
                    let mut all_arrays: Vec<Arc<dyn Array>> = Vec::new();
                    for value in values {
                        let extracted_value = strip_union_value(value);
                        let child_array: Arc<dyn Array> = avro_values_to_arrow_array(
                            slice::from_ref(extracted_value),
                            field.data_type(),
                        );
                        all_arrays.push(child_array);
                    }
                    let mut array_lenghts: Vec<usize> = vec![];
                    for array in &all_arrays {
                        array_lenghts.push(array.len());
                    }
                    let offset_buffer: OffsetBuffer<i32> =
                        OffsetBuffer::<i32>::from_lengths(array_lenghts);

                    let array_refs: Vec<&dyn Array> = all_arrays
                        .iter()
                        .map(|a: &Arc<dyn Array>| a.as_ref())
                        .collect();
                    let concatenated_array = arrow::compute::concat(&array_refs).unwrap();

                    let list_array: arrow::array::GenericListArray<i32> = ListArray::new(
                        field.clone(),
                        offset_buffer,
                        Arc::new(concatenated_array.clone()) as Arc<dyn Array>,
                        None, // No validity bitmap
                    );
                    Arc::new(list_array)
                }
                _ => panic!("at the list disco."),
            }
        }
        _ => unimplemented!("Converter for {:?} not implemented", data_type),
    }
}

fn main() {}
/* pub fn avro_records_to_record_batch(
    records: Vec<Record>,
) -> Result<RecordBatch, ArrowError> {
    let maybe_schema: Option<Schema> = records
        .iter()
        .filter_map(|r| match r {
            Record::AvroRecord(value, _) => Some(avro_record_to_arrow_schema(value)),
            _ => None,
        })
        .next();

    let values: Vec<&Value> = records
        .iter()
        .filter_map(|r: &Record| match r {
            Record::RecordBatch(_) => None,
            Record::AvroRecord(value, _) => Some(value),
            Record::DataFrame(_) => todo!(),
            Record::JsonRecord(_) => todo!(),
        })
        .collect();

    let schema: Schema = maybe_schema.unwrap();
    let arrays: Vec<Arc<dyn Array>> = schema
        .fields()
        .iter()
        .map(|field_schema| {
            let field_values: Vec<Value> = values
                .iter()
                .map(|av| match av {
                    Value::Record(record_fields) => {
                        let record_value = record_fields
                            .iter()
                            .find(|(field_name, _)| field_name == field_schema.name());
                        record_value.map(|t| t.1.clone()).unwrap() //TODO: Unclone
                    }
                    _ => panic!("expected a record here."),
                })
                .collect();
            avro_values_to_arrow_array(&field_values, field_schema.data_type())
        })
        .collect();
    RecordBatch::try_new(Arc::new(schema), arrays)
} */

fn avro_record_to_arrow_record_batch(
    avro_record: &Value,
    schema: Arc<Schema>,
) -> RecordBatch {
    match avro_record {
        Value::Record(fields) => {
            let arrays: Vec<Arc<dyn Array>> = schema
                .fields()
                .iter()
                .map(|field_schema| {
                    let field_value: &Value = fields
                        .iter()
                        .find(|(name, _)| name == field_schema.name())
                        .map(|(_, v)| v)
                        .expect("Field not found in record");
                    avro_values_to_arrow_array(
                        slice::from_ref(field_value),
                        field_schema.data_type(),
                    )
                })
                .collect();

            RecordBatch::try_new(schema, arrays).expect("Failed to create RecordBatch")
        }
        _ => panic!("Expected Value::Record for conversion"),
    }
}

/* impl AvroSchema {
    pub fn new(schema_str: &str) -> AvroSchema {
        let avro_schema = AvSchema::parse_str(schema_str).expect("Invalid schema!");
        AvroSchema {
            schema: avro_schema,
        }
    }
}
impl Decoder for AvroSchema {
    fn decode(&self, data: &[u8]) -> RecordBatch {
        let mut data_copy: &[u8] = data;
        let avro_record: Value =
            from_avro_datum(&self.schema, &mut data_copy, None).unwrap();
        let arrow_schema: Schema = avro_record_to_arrow_schema(&avro_record); // probs should use avro schema > arrow schema converter
        let arrow_record: RecordBatch =
            avro_record_to_arrow_record_batch(&avro_record, Arc::new(arrow_schema));
        arrow_record
    }

    fn write_to_avro_file(&self, value: Value) -> Result<(), Error> {
        let output_file: std::fs::File = std::fs::File::create("sample.avro").unwrap();
        let mut writer: Writer<'_, BufWriter<std::fs::File>> =
            Writer::new(&self.schema, BufWriter::new(output_file));
        writer.append(value)?;
        writer.flush()?;
        Ok(())
    }

    fn decode_avro(&self, data: &[u8]) -> Value {
        let mut data_copy: &[u8] = data;
        from_avro_datum(&self.schema, &mut data_copy, None).unwrap()
    }
} */
