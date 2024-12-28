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

// TODO upstream this to DataFusion as long as we have a way to specify all
// of the Spark-specific compatibility features that we need (including
// being able to specify Spark-compatible cast from all types to string)

use crate::cast::SparkCastOptions;
use crate::{spark_cast, EvalMode};
use arrow_array::builder::StringBuilder;
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Schema};
use datafusion_common::Result;
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::PhysicalExpr;
use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;

/// to_json function
#[derive(Debug, Eq)]
pub struct ToJson {
    /// The input to convert to JSON
    expr: Arc<dyn PhysicalExpr>,
    /// Timezone to use when converting timestamps to JSON
    timezone: String,
}

impl Hash for ToJson {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.timezone.hash(state);
    }
}
impl PartialEq for ToJson {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr) && self.timezone.eq(&other.timezone)
    }
}

impl ToJson {
    pub fn new(expr: Arc<dyn PhysicalExpr>, timezone: &str) -> Self {
        Self {
            expr,
            timezone: timezone.to_owned(),
        }
    }
}

impl Display for ToJson {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "to_json({}, timezone={})", self.expr, self.timezone)
    }
}

impl PartialEq<dyn Any> for ToJson {
    fn eq(&self, other: &dyn Any) -> bool {
        if let Some(other) = other.downcast_ref::<ToJson>() {
            self.expr.eq(&other.expr) && self.timezone.eq(&other.timezone)
        } else {
            false
        }
    }
}

impl PhysicalExpr for ToJson {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _: &Schema) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.expr.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let input = self.expr.evaluate(batch)?.into_array(batch.num_rows())?;
        Ok(ColumnarValue::Array(array_to_json_string(
            &input,
            &self.timezone,
        )?))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        assert!(children.len() == 1);
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            &self.timezone,
        )))
    }
}

/// Convert an array into a JSON value string representation
fn array_to_json_string(arr: &Arc<dyn Array>, timezone: &str) -> Result<ArrayRef> {
    if let Some(struct_array) = arr.as_any().downcast_ref::<StructArray>() {
        struct_to_json(struct_array, timezone)
    } else {
        spark_cast(
            ColumnarValue::Array(Arc::clone(arr)),
            &DataType::Utf8,
            &SparkCastOptions::new(EvalMode::Legacy, timezone, false),
        )?
        .into_array(arr.len())
    }
}

fn escape_string(input: &str) -> String {
    let mut escaped_string = String::with_capacity(input.len());
    let mut is_escaped = false;
    for c in input.chars() {
        match c {
            '\"' | '\\' if !is_escaped => {
                escaped_string.push('\\');
                escaped_string.push(c);
                is_escaped = false;
            }
            '\t' => {
                escaped_string.push('\\');
                escaped_string.push('t');
                is_escaped = false;
            }
            '\r' => {
                escaped_string.push('\\');
                escaped_string.push('r');
                is_escaped = false;
            }
            '\n' => {
                escaped_string.push('\\');
                escaped_string.push('n');
                is_escaped = false;
            }
            '\x0C' => {
                escaped_string.push('\\');
                escaped_string.push('f');
                is_escaped = false;
            }
            '\x08' => {
                escaped_string.push('\\');
                escaped_string.push('b');
                is_escaped = false;
            }
            '\\' => {
                escaped_string.push('\\');
                is_escaped = true;
            }
            _ => {
                escaped_string.push(c);
                is_escaped = false;
            }
        }
    }
    escaped_string
}

fn struct_to_json(array: &StructArray, timezone: &str) -> Result<ArrayRef> {
    // get field names and escape any quotes
    let field_names: Vec<String> = array
        .fields()
        .iter()
        .map(|f| escape_string(f.name().as_str()))
        .collect();
    // determine which fields need to have their values quoted
    let is_string: Vec<bool> = array
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => true,
            DataType::Dictionary(_, dt) => {
                matches!(dt.as_ref(), DataType::Utf8 | DataType::LargeUtf8)
            }
            _ => false,
        })
        .collect();
    // create JSON string representation of each column
    let string_arrays: Vec<ArrayRef> = array
        .columns()
        .iter()
        .map(|arr| array_to_json_string(arr, timezone))
        .collect::<Result<Vec<_>>>()?;
    let string_arrays: Vec<&StringArray> = string_arrays
        .iter()
        .map(|arr| {
            arr.as_any()
                .downcast_ref::<StringArray>()
                .expect("string array")
        })
        .collect();
    // build the JSON string containing entries in the format `"field_name":field_value`
    let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 16);
    let mut json = String::with_capacity(array.len() * 16);
    for row_index in 0..array.len() {
        if array.is_null(row_index) {
            builder.append_null();
        } else {
            json.clear();
            let mut any_fields_written = false;
            json.push('{');
            for col_index in 0..string_arrays.len() {
                if !string_arrays[col_index].is_null(row_index) {
                    if any_fields_written {
                        json.push(',');
                    }
                    // quoted field name
                    json.push('"');
                    json.push_str(&field_names[col_index]);
                    json.push_str("\":");
                    // value
                    let string_value = string_arrays[col_index].value(row_index);
                    if is_string[col_index] {
                        json.push('"');
                        json.push_str(&escape_string(string_value));
                        json.push('"');
                    } else {
                        json.push_str(string_value);
                    }
                    any_fields_written = true;
                }
            }
            json.push('}');
            builder.append_value(&json);
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod test {
    use crate::to_json::struct_to_json;
    use arrow_array::types::Int32Type;
    use arrow_array::{Array, PrimitiveArray, StringArray};
    use arrow_array::{ArrayRef, BooleanArray, Int32Array, StructArray};
    use arrow_schema::{DataType, Field};
    use datafusion_common::Result;
    use std::sync::Arc;

    #[test]
    fn test_primitives() -> Result<()> {
        let bools: ArrayRef = create_bools();
        let ints: ArrayRef = create_ints();
        let strings: ArrayRef = create_strings();
        let struct_array = StructArray::from(vec![
            (Arc::new(Field::new("a", DataType::Boolean, true)), bools),
            (Arc::new(Field::new("b", DataType::Int32, true)), ints),
            (Arc::new(Field::new("c", DataType::Utf8, true)), strings),
        ]);
        let json = struct_to_json(&struct_array, "UTC")?;
        let json = json
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        assert_eq!(4, json.len());
        assert_eq!(r#"{"b":123}"#, json.value(0));
        assert_eq!(r#"{"a":true,"c":"foo"}"#, json.value(1));
        assert_eq!(r#"{"a":false,"b":2147483647,"c":"bar"}"#, json.value(2));
        assert_eq!(r#"{"a":false,"b":-2147483648,"c":""}"#, json.value(3));
        Ok(())
    }

    #[test]
    fn test_nested_struct() -> Result<()> {
        let bools: ArrayRef = create_bools();
        let ints: ArrayRef = create_ints();

        // create first struct array
        let struct_fields = vec![
            Arc::new(Field::new("a", DataType::Boolean, true)),
            Arc::new(Field::new("b", DataType::Int32, true)),
        ];
        let struct_values = vec![bools, ints];
        let struct_array = StructArray::from(
            struct_fields
                .clone()
                .into_iter()
                .zip(struct_values)
                .collect::<Vec<_>>(),
        );

        // create second struct array containing the first struct array
        let struct_fields2 = vec![Arc::new(Field::new(
            "a",
            DataType::Struct(struct_fields.into()),
            true,
        ))];
        let struct_values2: Vec<ArrayRef> = vec![Arc::new(struct_array.clone())];
        let struct_array2 = StructArray::from(
            struct_fields2
                .into_iter()
                .zip(struct_values2)
                .collect::<Vec<_>>(),
        );

        let json = struct_to_json(&struct_array2, "UTC")?;
        let json = json
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        assert_eq!(4, json.len());
        assert_eq!(r#"{"a":{"b":123}}"#, json.value(0));
        assert_eq!(r#"{"a":{"a":true}}"#, json.value(1));
        assert_eq!(r#"{"a":{"a":false,"b":2147483647}}"#, json.value(2));
        assert_eq!(r#"{"a":{"a":false,"b":-2147483648}}"#, json.value(3));
        Ok(())
    }

    fn create_ints() -> Arc<PrimitiveArray<Int32Type>> {
        Arc::new(Int32Array::from(vec![
            Some(123),
            None,
            Some(i32::MAX),
            Some(i32::MIN),
        ]))
    }

    fn create_bools() -> Arc<BooleanArray> {
        Arc::new(BooleanArray::from(vec![
            None,
            Some(true),
            Some(false),
            Some(false),
        ]))
    }

    fn create_strings() -> Arc<StringArray> {
        Arc::new(StringArray::from(vec![
            None,
            Some("foo"),
            Some("bar"),
            Some(""),
        ]))
    }
}
