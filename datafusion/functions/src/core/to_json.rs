// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringBuilder, new_empty_array};
use arrow::datatypes::{DataType, Field, FieldRef};
use arrow::json::writer::{EncoderOptions, make_encoder};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, plan_datafusion_err};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Struct Functions"),
    description = "Returns the JSON text of a value: a struct becomes an object keyed by its field names, \
        a list an array, a map an object, and a scalar its JSON form. The text is what DataFusion's \
        JSON writer (`COPY ... TO 'file.json'`) produces: NULL struct fields and NULL map entries are \
        omitted, non-finite floats are `null`, binary is hexadecimal, and timestamps are written in \
        RFC 3339 form. A NULL input returns NULL.",
    syntax_example = "to_json(expression)",
    sql_example = r#"```sql
> select to_json(named_struct('a', 1, 'b', 'x', 'c', NULL)) as s, to_json([1, 2]) as l, to_json('text') as t;
+-----------------+-------+--------+
| s               | l     | t      |
+-----------------+-------+--------+
| {"a":1,"b":"x"} | [1,2] | "text" |
+-----------------+-------+--------+
```"#,
    argument(
        name = "expression",
        description = "Value to serialize: a struct, list, map or scalar expression. Types without a JSON \
            form, such as unions or maps with non-string keys, are rejected when the query is planned."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToJsonFunc {
    signature: Signature,
}

impl Default for ToJsonFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ToJsonFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToJsonFunc {
    fn name(&self) -> &str {
        "to_json"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [arg_type] = take_function_args(self.name(), arg_types)?;
        // arrow-json decides what it can encode; probe with an empty array so an
        // unsupported type fails when the query is planned, not per batch
        let field = Arc::new(Field::new("value", arg_type.clone(), true));
        let empty = new_empty_array(arg_type);
        make_encoder(&field, empty.as_ref(), &EncoderOptions::default()).map_err(
            |e| plan_datafusion_err!("to_json does not support {arg_type}: {e}"),
        )?;
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, arg_fields, ..
        } = args;
        let [input] = take_function_args(self.name(), args)?;
        let [field] = take_function_args(self.name(), arg_fields)?;

        match input {
            ColumnarValue::Array(array) => {
                Ok(ColumnarValue::Array(to_json(&field, &array)?))
            }
            ColumnarValue::Scalar(scalar) => {
                let result = to_json(&field, &scalar.to_array()?)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &result, 0,
                )?))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Encodes every row of `array` as JSON text; a NULL row produces NULL.
/// `field` describes `array` and supplies the key names of nested fields.
fn to_json(field: &FieldRef, array: &ArrayRef) -> Result<ArrayRef> {
    let options = EncoderOptions::default();
    let mut encoder = make_encoder(field, array.as_ref(), &options)?;
    let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 32);
    let mut buf = Vec::with_capacity(128);
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

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{
        BinaryArray, BooleanArray, Float64Array, Int32Array, Int32Builder, ListArray,
        MapBuilder, StringArray, StringBuilder, StructArray, TimestampMicrosecondArray,
    };
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::{Field, Fields, Int32Type, UnionFields, UnionMode};
    use datafusion_common::cast::as_string_array;
    use datafusion_common::config::ConfigOptions;

    /// Runs to_json over `input` (whose type is described by `field`) and
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
        ToJsonFunc::new()
            .invoke_with_args(args)?
            .to_array(number_rows)
    }

    /// Serializes `array` and returns the JSON text of each row (None for NULL).
    fn json_of(array: ArrayRef) -> Vec<Option<String>> {
        let field = Arc::new(Field::new("v", array.data_type().clone(), true));
        let out = run(ColumnarValue::Array(array), field).unwrap();
        as_string_array(&out)
            .unwrap()
            .iter()
            .map(|v| v.map(str::to_string))
            .collect()
    }

    /// {a: 1, b: "x", c: true}, {a: 2, b: NULL, c: false}, NULL
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

    #[test]
    fn test_return_type() {
        let func = ToJsonFunc::new();
        for t in [
            DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int32, true)])),
            DataType::Int64,
            DataType::Utf8,
            DataType::Null,
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        ] {
            assert_eq!(func.return_type(&[t]).unwrap(), DataType::Utf8);
        }
    }

    #[test]
    fn test_return_type_rejects_types_without_json_form() {
        let func = ToJsonFunc::new();

        // arrow-json cannot encode unions
        let union = DataType::Union(
            UnionFields::from_fields(vec![Field::new("a", DataType::Int32, true)]),
            UnionMode::Sparse,
        );
        let err = func.return_type(&[union]).unwrap_err().to_string();
        assert!(err.contains("to_json does not support Union"), "{err}");

        // ...nor maps with non-string keys, even nested in a struct
        let entries = Fields::from(vec![
            Field::new("keys", DataType::Int32, false),
            Field::new("values", DataType::Int32, true),
        ]);
        let map = DataType::Map(
            Arc::new(Field::new("entries", DataType::Struct(entries), false)),
            false,
        );
        let nested = DataType::Struct(Fields::from(vec![Field::new("m", map, true)]));
        let err = func.return_type(&[nested]).unwrap_err().to_string();
        assert!(err.contains("to_json does not support Struct"), "{err}");
    }

    #[test]
    fn test_return_type_rejects_wrong_arity() {
        let func = ToJsonFunc::new();
        assert!(func.return_type(&[]).is_err());
        assert!(
            func.return_type(&[DataType::Int64, DataType::Int64])
                .is_err()
        );
    }

    #[test]
    fn test_struct_array_input() {
        let out = json_of(Arc::new(sample_struct()));
        assert_eq!(
            out,
            vec![
                Some(r#"{"a":1,"b":"x","c":true}"#.to_string()),
                // NULL fields are omitted
                Some(r#"{"a":2,"c":false}"#.to_string()),
                // a NULL struct is NULL
                None,
            ]
        );
    }

    #[test]
    fn test_struct_scalar_input() {
        let input = sample_struct().slice(0, 1);
        let field = Arc::new(Field::new("v", input.data_type().clone(), true));
        let scalar = ScalarValue::Struct(Arc::new(input));

        let out = run(ColumnarValue::Scalar(scalar), field).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(
            as_string_array(&out).unwrap().value(0),
            r#"{"a":1,"b":"x","c":true}"#
        );
    }

    #[test]
    fn test_sliced_input() {
        let out = json_of(Arc::new(sample_struct().slice(1, 2)));
        assert_eq!(out, vec![Some(r#"{"a":2,"c":false}"#.to_string()), None]);
    }

    #[test]
    fn test_scalar_values() {
        let cases: Vec<(ScalarValue, Option<&str>)> = vec![
            (ScalarValue::Int64(Some(1)), Some("1")),
            (ScalarValue::Utf8(Some("text".to_string())), Some(r#""text""#)),
            (ScalarValue::Boolean(Some(true)), Some("true")),
            (ScalarValue::Float64(Some(1.5)), Some("1.5")),
            // NULL of any type is NULL
            (ScalarValue::Int64(None), None),
            (ScalarValue::Null, None),
        ];
        for (scalar, expected) in cases {
            let field = Arc::new(Field::new("v", scalar.data_type(), true));
            let out = run(ColumnarValue::Scalar(scalar.clone()), field).unwrap();
            let out = as_string_array(&out).unwrap();
            let actual = (!out.is_null(0)).then(|| out.value(0));
            assert_eq!(actual, expected, "scalar: {scalar:?}");
        }
    }

    #[test]
    fn test_list_input() {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), None, Some(3)]),
            None,
            Some(vec![]),
        ]);
        let out = json_of(Arc::new(list));
        // NULL list elements are kept as null; a NULL list is NULL
        assert_eq!(
            out,
            vec![Some("[1,null,3]".to_string()), None, Some("[]".to_string())]
        );
    }

    #[test]
    fn test_map_input() {
        // {"a":1,"b":NULL}, NULL
        let mut m = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        m.keys().append_value("a");
        m.values().append_value(1);
        m.keys().append_value("b");
        m.values().append_null();
        m.append(true).unwrap();
        m.append(false).unwrap();

        let out = json_of(Arc::new(m.finish()));
        // a NULL map entry is omitted, like a NULL struct field
        assert_eq!(out, vec![Some(r#"{"a":1}"#.to_string()), None]);
    }

    #[test]
    fn test_nested_struct_list_and_map() {
        let x = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
        let n = Arc::new(StructArray::new(
            Fields::from(vec![Field::new("x", DataType::Int32, true)]),
            vec![x],
            None,
        )) as ArrayRef;
        let l = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(9)]),
        ])) as ArrayRef;
        let mut m = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        m.keys().append_value("k");
        m.values().append_value(1);
        m.append(true).unwrap();
        let m = Arc::new(m.finish()) as ArrayRef;

        let fields = Fields::from(vec![
            Field::new("n", n.data_type().clone(), true),
            Field::new("l", l.data_type().clone(), true),
            Field::new("m", m.data_type().clone(), true),
        ]);
        let input = StructArray::new(fields, vec![n, l, m], None);

        let out = json_of(Arc::new(input));
        assert_eq!(
            out,
            vec![Some(r#"{"n":{"x":1},"l":[1,9],"m":{"k":1}}"#.to_string())]
        );
    }

    #[test]
    fn test_floats() {
        let out = json_of(Arc::new(Float64Array::from(vec![
            Some(1e-5),
            Some(1e10),
            Some(f64::NAN),
            Some(f64::INFINITY),
            None,
        ])));
        // non-finite values are JSON null, as arrow-json writes them
        assert_eq!(
            out,
            vec![
                Some("0.00001".to_string()),
                Some("1.0e10".to_string()),
                Some("null".to_string()),
                Some("null".to_string()),
                None,
            ]
        );
    }

    #[test]
    fn test_binary_is_hex() {
        let out = json_of(Arc::new(BinaryArray::from(vec![Some(b"abc".as_ref()), None])));
        assert_eq!(out, vec![Some(r#""616263""#.to_string()), None]);
    }

    #[test]
    fn test_timestamps() {
        // 2024-01-01 01:02:03.123456 UTC
        let micros = 1_704_070_923_123_456;
        let ntz = Arc::new(TimestampMicrosecondArray::from(vec![micros])) as ArrayRef;
        let tz = Arc::new(
            TimestampMicrosecondArray::from(vec![micros]).with_timezone("+02:00"),
        ) as ArrayRef;

        // without a time zone the wall-clock time is written as is; with one, the
        // instant is written in that zone with its offset
        assert_eq!(
            json_of(ntz),
            vec![Some(r#""2024-01-01T01:02:03.123456""#.to_string())]
        );
        assert_eq!(
            json_of(tz),
            vec![Some(r#""2024-01-01T03:02:03.123456+02:00""#.to_string())]
        );
    }

    #[test]
    fn test_string_escaping() {
        let out = json_of(Arc::new(StringArray::from(vec![
            "he said \"hi\"\n",
            "a\\b",
            "π",
        ])));
        assert_eq!(
            out,
            vec![
                Some(r#""he said \"hi\"\n""#.to_string()),
                Some(r#""a\\b""#.to_string()),
                Some(r#""π""#.to_string()),
            ]
        );
    }
}
