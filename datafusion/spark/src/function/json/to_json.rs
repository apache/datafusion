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

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringBuilder};
use arrow::datatypes::{DataType, FieldRef};
use arrow::json::writer::{EncoderOptions, make_encoder};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Column, Result, ScalarValue, exec_datafusion_err, plan_err};
use datafusion_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility, lit,
};
use datafusion_functions::core::expr_fn::named_struct;

/// Spark-compatible to_json expression
///
/// <https://spark.apache.org/docs/latest/api/sql/index.html#to_json>
///
/// Serializes a struct value to a JSON string, one JSON object per row.
///
/// to_json(struct<...>) -> Utf8
///
/// Semantics (matching Spark's defaults):
/// - A NULL struct row produces NULL
/// - NULL fields are omitted from the object (`ignoreNullFields = true`)
/// - Nested structs, lists and maps are serialized recursively
///
/// The serialization is done batch by batch with the arrow-json row encoder:
/// an encoder is built once per input array and each row is written straight
/// into a StringBuilder, so no intermediate RecordBatch or serde_json::Value
/// is created and nothing is collected.
///
/// Not yet supported (Spark accepts these): map/array top-level inputs and the
/// options map argument (`ignoreNullFields`, timestampFormat, ...).

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToJson {
    signature: Signature,
}

impl Default for ToJson {
    fn default() -> Self {
        Self::new()
    }
}

impl ToJson {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToJson {
    fn name(&self) -> &str {
        "to_json"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [DataType::Struct(_)] => Ok(DataType::Utf8),
            [other] => plan_err!("to_json expects a struct argument, got {other}"),
            _ => plan_err!(
                "to_json expects exactly one argument, got {}",
                arg_types.len()
            ),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, arg_fields, ..
        } = args;
        let [input] = take_function_args(self.name(), args)?;
        let [field] = take_function_args(self.name(), arg_fields)?;

        match input {
            ColumnarValue::Array(array) => {
                Ok(ColumnarValue::Array(struct_to_json(&field, &array)?))
            }
            ColumnarValue::Scalar(scalar) => {
                let array = scalar.to_array()?;
                let result = struct_to_json(&field, &array)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &result, 0,
                )?))
            }
        }
    }
}

/// Encodes every row of a struct array as a JSON object string.
///
/// field describes array (its name and the nested field names/types are
/// what the encoder uses for the JSON keys).
fn struct_to_json(field: &FieldRef, array: &ArrayRef) -> Result<ArrayRef> {
    // Spark default: ignoreNullFields = true -> omit null fields entirely
    let options = EncoderOptions::default().with_explicit_nulls(false);
    let mut encoder = make_encoder(field, array.as_ref(), &options)?;
    let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 32);
    let mut buf: Vec<u8> = Vec::with_capacity(128);
    for row in 0..array.len() {
        if encoder.is_null(row) {
            builder.append_null();
            continue;
        }
        buf.clear();
        encoder.encode(row, &mut buf);
        let json = std::str::from_utf8(&buf)
            .map_err(|e| exec_datafusion_err!("to_json produced invalid UTF-8: {e}"))?;
        builder.append_value(json);
    }

    Ok(Arc::new(builder.finish()))
}

/// Input accepted by the to_json expression builder.
///
/// Implemented for:
/// - [`Expr`]: any expression that evaluates to a struct, e.g. a struct column
///   or named_struct(...); it is passed through unchanged.
/// - &[S] / &[S; N] where S: AsRef<str>: a list of column names. The
///   columns are packed into a struct with [`named_struct`], keyed by their
///   unqualified names, so &["t.foo", "bar"] serializes as
///   {"foo": ..., "bar": ...}.
pub trait IntoJsonStruct {
    /// Converts self into the struct-valued [`Expr`] to serialize.
    fn into_struct_expr(self) -> Expr;
}

impl IntoJsonStruct for Expr {
    fn into_struct_expr(self) -> Expr {
        self
    }
}

impl<S: AsRef<str>> IntoJsonStruct for &[S] {
    fn into_struct_expr(self) -> Expr {
        let args = self
            .iter()
            .flat_map(|name| {
                let column = Column::from_qualified_name(name.as_ref());
                [lit(column.name().to_owned()), Expr::Column(column)]
            })
            .collect();
        named_struct(args)
    }
}

impl<S: AsRef<str>, const N: usize> IntoJsonStruct for &[S; N] {
    fn into_struct_expr(self) -> Expr {
        self.as_slice().into_struct_expr()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{
        BooleanArray, Int32Array, Int64Array, ListBuilder, StringArray, StructArray,
    };
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::{Field, Fields};
    use datafusion_common::cast::as_string_array;
    use datafusion_common::config::ConfigOptions;

    /// Runs to_json over input (whose type is described by `field`) and
    /// returns the resulting string array.
    fn run(input: ColumnarValue, field: FieldRef) -> Result<ArrayRef> {
        let number_rows = match &input {
            ColumnarValue::Array(a) => a.len(),
            ColumnarValue::Scalar(_) => 1,
        };
        let args = ScalarFunctionArgs {
            args: vec![input],
            arg_fields: vec![field],
            number_rows,
            return_field: Field::new("to_json", DataType::Utf8, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        ToJson::new().invoke_with_args(args)?.to_array(number_rows)
    }

    fn sample_struct() -> StructArray {
        let a = Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef;
        let b = Arc::new(StringArray::from(vec![Some("x"), None, Some("z")])) as ArrayRef;
        let c =
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as ArrayRef;
        let fields = Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Boolean, true),
        ]);
        let nulls = NullBuffer::from(vec![true, true, false]);
        StructArray::new(fields, vec![a, b, c], Some(nulls))
    }

    fn field_of(array: &StructArray) -> FieldRef {
        Field::new("s", array.data_type().clone(), true).into()
    }

    #[test]
    fn test_return_type() {
        let func = ToJson::new();
        let fields = Fields::from(vec![Field::new("a", DataType::Int32, true)]);
        assert_eq!(
            func.return_type(&[DataType::Struct(fields)]).unwrap(),
            DataType::Utf8
        );
    }

    #[test]
    fn test_return_type_rejects_non_struct() {
        let err = ToJson::new().return_type(&[DataType::Utf8]).unwrap_err();
        assert!(err.to_string().contains("expects a struct argument"));

        let err = ToJson::new().return_type(&[]).unwrap_err();
        assert!(err.to_string().contains("exactly one argument"));
    }

    #[test]
    fn test_array_input() {
        let input = sample_struct();
        let field = field_of(&input);
        let out = run(ColumnarValue::Array(Arc::new(input)), field).unwrap();
        let out = as_string_array(&out).unwrap();

        assert_eq!(out.len(), 3);
        assert_eq!(out.value(0), r#"{"a":1,"b":"x","c":true}"#);
        // NULL field is omitted (Spark ignoreNullFields = true)
        assert_eq!(out.value(1), r#"{"a":2,"c":false}"#);
        // NULL struct row -> NULL
        assert!(out.is_null(2));
    }

    #[test]
    fn test_scalar_input() {
        let input = sample_struct();
        let field = field_of(&input);
        let scalar = ScalarValue::Struct(Arc::new(input.slice(0, 1)));
        let out = run(ColumnarValue::Scalar(scalar), field).unwrap();
        let out = as_string_array(&out).unwrap();

        assert_eq!(out.len(), 1);
        assert_eq!(out.value(0), r#"{"a":1,"b":"x","c":true}"#);
    }

    #[test]
    fn test_scalar_null_struct() {
        let input = sample_struct();
        let field = field_of(&input);
        let scalar = ScalarValue::Struct(Arc::new(input.slice(2, 1)));
        let out = run(ColumnarValue::Scalar(scalar), field).unwrap();

        assert_eq!(out.len(), 1);
        assert!(out.is_null(0));
    }

    #[test]
    fn test_nested_struct_and_list() {
        // {inner: {x: Int64}, tags: List<Utf8>}
        let x = Arc::new(Int64Array::from(vec![Some(10), None])) as ArrayRef;
        let inner_fields = Fields::from(vec![Field::new("x", DataType::Int64, true)]);
        let inner = Arc::new(StructArray::new(inner_fields, vec![x], None)) as ArrayRef;

        let mut tags = ListBuilder::new(StringBuilder::new());
        tags.values().append_value("p");
        tags.values().append_value("q");
        tags.append(true);
        tags.append(false);
        let tags = Arc::new(tags.finish()) as ArrayRef;

        let fields = Fields::from(vec![
            Field::new("inner", inner.data_type().clone(), true),
            Field::new("tags", tags.data_type().clone(), true),
        ]);
        let input = StructArray::new(fields, vec![inner, tags], None);
        let field = field_of(&input);

        let out = run(ColumnarValue::Array(Arc::new(input)), field).unwrap();
        let out = as_string_array(&out).unwrap();

        assert_eq!(out.value(0), r#"{"inner":{"x":10},"tags":["p","q"]}"#);
        // nested NULL field and NULL list are both omitted
        assert_eq!(out.value(1), r#"{"inner":{}}"#);
    }

    #[test]
    fn test_string_escaping() {
        let a = Arc::new(StringArray::from(vec![Some("he said \"hi\"\n")])) as ArrayRef;
        let fields = Fields::from(vec![Field::new("a", DataType::Utf8, true)]);
        let input = StructArray::new(fields, vec![a], None);
        let field = field_of(&input);

        let out = run(ColumnarValue::Array(Arc::new(input)), field).unwrap();
        let out = as_string_array(&out).unwrap();

        assert_eq!(out.value(0), r#"{"a":"he said \"hi\"\n"}"#);
    }
    /// Unwraps a named_struct(...) call and returns its arguments.
    fn named_struct_args(expr: &Expr) -> &[Expr] {
        match expr {
            Expr::ScalarFunction(f) if f.func.name() == "named_struct" => &f.args,
            other => panic!("expected named_struct call, got {other:?}"),
        }
    }

    #[test]
    fn test_into_json_struct_passes_expr_through() {
        let expr = Expr::Column(Column::from_name("event"));
        assert_eq!(expr.clone().into_struct_expr(), expr);
    }

    #[test]
    fn test_into_json_struct_from_column_names() {
        let expr = ["foo", "bar"].into_struct_expr();
        let args = named_struct_args(&expr);

        assert_eq!(
            args,
            &[
                lit("foo"),
                Expr::Column(Column::from_name("foo")),
                lit("bar"),
                Expr::Column(Column::from_name("bar")),
            ]
        );
    }

    #[test]
    fn test_into_json_struct_accepts_slice_and_strings() {
        let names: Vec<String> = vec!["foo".to_string(), "bar".to_string()];
        let from_vec = names.as_slice().into_struct_expr();
        let from_array = ["foo", "bar"].into_struct_expr();
        assert_eq!(from_vec, from_array);
    }

    #[test]
    fn test_into_json_struct_uses_unqualified_key_for_qualified_column() {
        let expr = ["t.foo"].into_struct_expr();
        let args = named_struct_args(&expr);

        // JSON key is the bare column name...
        assert_eq!(args[0], lit("foo"));
        // ...while the column reference keeps its qualifier
        assert_eq!(args[1], Expr::Column(Column::from_qualified_name("t.foo")));
    }
}
