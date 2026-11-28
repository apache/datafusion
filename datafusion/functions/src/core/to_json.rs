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
        BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
        Decimal32Array, Decimal128Array, Decimal256Array, DictionaryArray,
        DurationSecondArray, FixedSizeBinaryArray, Float32Array, Float64Array,
        Int32Array, Int32Builder, Int64Array, IntervalMonthDayNanoArray,
        LargeBinaryArray, ListArray, ListBuilder, MapBuilder, StringArray, StringBuilder,
        StructArray, Time32SecondArray, Time64MicrosecondArray,
        TimestampMicrosecondArray, TimestampSecondArray, UInt64Array,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{
        Field, Fields, Float64Type, Int32Type, IntervalMonthDayNano, IntervalUnit,
        TimeUnit, UnionFields, UnionMode, i256,
    };
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

    fn field_of(array: &dyn Array) -> FieldRef {
        Arc::new(Field::new("v", array.data_type().clone(), true))
    }

    /// Serializes `array` and returns the JSON text of each row (None for NULL).
    fn json_of(array: ArrayRef) -> Vec<Option<String>> {
        let field = field_of(array.as_ref());
        let out = run(ColumnarValue::Array(array), field).unwrap();
        as_string_array(&out)
            .unwrap()
            .iter()
            .map(|v| v.map(str::to_string))
            .collect()
    }

    /// Serializes a one-row struct `{"v": array}` and returns its JSON text.
    fn struct_json_of(array: ArrayRef) -> String {
        let fields = Fields::from(vec![Field::new("v", array.data_type().clone(), true)]);
        let input = StructArray::new(fields, vec![array], None);
        json_of(Arc::new(input)).remove(0).unwrap()
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

    fn map_of(keys: DataType, values: DataType) -> DataType {
        let entries = Fields::from(vec![
            Field::new("keys", keys, false),
            Field::new("values", values, true),
        ]);
        DataType::Map(
            Arc::new(Field::new("entries", DataType::Struct(entries), false)),
            false,
        )
    }

    #[test]
    fn test_return_type() {
        let fields = Fields::from(vec![Field::new("a", DataType::Int32, true)]);
        assert_eq!(
            ToJsonFunc::new()
                .return_type(&[DataType::Struct(fields)])
                .unwrap(),
            DataType::Utf8
        );
    }

    #[test]
    fn test_return_type_accepts_encodable_types() {
        let item = |t: DataType| Arc::new(Field::new("item", t, true));
        let types = vec![
            DataType::Null,
            DataType::Boolean,
            DataType::Int8,
            DataType::UInt64,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal128(10, 2),
            DataType::Decimal256(50, 2),
            DataType::Utf8,
            DataType::Utf8View,
            DataType::Binary,
            DataType::FixedSizeBinary(3),
            DataType::Date32,
            DataType::Date64,
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            DataType::Interval(IntervalUnit::MonthDayNano),
            DataType::List(item(DataType::Date32)),
            DataType::FixedSizeList(item(DataType::Float64), 2),
            DataType::Struct(Fields::from(vec![Field::new("x", DataType::Utf8, true)])),
            map_of(DataType::Utf8, DataType::List(item(DataType::Int32))),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        ];
        let func = ToJsonFunc::new();
        for t in &types {
            assert_eq!(
                func.return_type(&[t.clone()]).unwrap(),
                DataType::Utf8,
                "{t}"
            );
        }
        // and all of them together inside one struct
        let fields: Fields = types
            .into_iter()
            .enumerate()
            .map(|(i, t)| Field::new(format!("f{i}"), t, true))
            .collect();
        assert_eq!(
            func.return_type(&[DataType::Struct(fields)]).unwrap(),
            DataType::Utf8
        );
    }

    #[test]
    fn test_return_type_rejects_types_without_json_form() {
        let return_type_err =
            |t: DataType| ToJsonFunc::new().return_type(&[t]).unwrap_err().to_string();
        let item = |t: DataType| Arc::new(Field::new("item", t, true));

        // arrow-json cannot encode unions...
        let union = DataType::Union(
            UnionFields::from_fields(vec![Field::new("a", DataType::Int32, true)]),
            UnionMode::Sparse,
        );
        let err = return_type_err(union);
        assert!(err.contains("to_json does not support Union"), "{err}");

        // ...nor maps with non-string keys
        let int_keys = map_of(DataType::Int32, DataType::Int32);
        let err = return_type_err(int_keys.clone());
        assert!(err.contains("to_json does not support Map"), "{err}");

        // at any nesting level
        for t in [
            DataType::List(item(int_keys.clone())),
            DataType::Struct(Fields::from(vec![Field::new("m", int_keys.clone(), true)])),
            map_of(DataType::Utf8, int_keys.clone()),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(int_keys)),
        ] {
            let err = return_type_err(t.clone());
            assert!(err.contains("to_json does not support"), "{t}: {err}");
        }
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
    fn test_array_input() {
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
    fn test_scalar_input() {
        let input = sample_struct();
        let field = field_of(&input);
        let scalar = ScalarValue::Struct(Arc::new(input.slice(0, 1)));

        let out = run(ColumnarValue::Scalar(scalar), field).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(
            as_string_array(&out).unwrap().value(0),
            r#"{"a":1,"b":"x","c":true}"#
        );
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
    fn test_scalar_values() {
        let cases: Vec<(ScalarValue, Option<&str>)> = vec![
            (ScalarValue::Int64(Some(1)), Some("1")),
            (
                ScalarValue::Utf8(Some("text".to_string())),
                Some(r#""text""#),
            ),
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

        let out = json_of(Arc::new(input));
        assert_eq!(
            out,
            vec![
                Some(r#"{"inner":{"x":10},"tags":["p","q"]}"#.to_string()),
                // nested NULL field and NULL list are both omitted
                Some(r#"{"inner":{}}"#.to_string()),
            ]
        );
    }

    #[test]
    fn test_sliced_nested_struct_and_list() {
        // 4 rows of {inner: {x: Int64}, tags: List<Utf8>} with uneven list
        // lengths (non-trivial list offsets) and a NULL parent in row 2:
        //   0: {x:1}, []        1: {x:2}, [a,b]
        //   2: NULL             3: {x:4}, [d,e,f]
        let x = Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef;
        let inner_fields = Fields::from(vec![Field::new("x", DataType::Int64, true)]);
        let inner = Arc::new(StructArray::new(inner_fields, vec![x], None)) as ArrayRef;

        let mut tags = ListBuilder::new(StringBuilder::new());
        tags.append(true); // []
        for t in ["a", "b"] {
            tags.values().append_value(t);
        }
        tags.append(true);
        tags.values().append_value("c");
        tags.append(true);
        for t in ["d", "e", "f"] {
            tags.values().append_value(t);
        }
        tags.append(true);
        let tags = Arc::new(tags.finish()) as ArrayRef;

        let fields = Fields::from(vec![
            Field::new("inner", inner.data_type().clone(), true),
            Field::new("tags", tags.data_type().clone(), true),
        ]);
        let nulls = NullBuffer::from(vec![true, true, false, true]);
        let full = StructArray::new(fields, vec![inner, tags], Some(nulls));

        // encode rows 1..4 through a non-zero-offset slice
        let out = json_of(Arc::new(full.slice(1, 3)));
        assert_eq!(
            out,
            vec![
                Some(r#"{"inner":{"x":2},"tags":["a","b"]}"#.to_string()),
                // the NULL parent row stays NULL after slicing
                None,
                Some(r#"{"inner":{"x":4},"tags":["d","e","f"]}"#.to_string()),
            ]
        );
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
    fn test_null_map_entries_and_struct_fields_are_omitted() {
        // m: Map<Utf8, Int32> with rows {"k":null}, {"a":1,"b":null}, NULL
        let mut mb = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        mb.keys().append_value("k");
        mb.values().append_null();
        mb.append(true).unwrap();
        mb.keys().append_value("a");
        mb.values().append_value(1);
        mb.keys().append_value("b");
        mb.values().append_null();
        mb.append(true).unwrap();
        mb.append(false).unwrap();
        let m = Arc::new(mb.finish()) as ArrayRef;

        // s: Int32 with a NULL in row 1
        let s = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;

        let fields = Fields::from(vec![
            Field::new("m", m.data_type().clone(), true),
            Field::new("s", DataType::Int32, true),
        ]);
        let input = StructArray::new(fields, vec![m, s], None);

        let out = json_of(Arc::new(input));
        assert_eq!(
            out,
            vec![
                // a NULL map entry is dropped, leaving an empty object
                Some(r#"{"m":{},"s":1}"#.to_string()),
                // a NULL struct field is dropped as well
                Some(r#"{"m":{"a":1}}"#.to_string()),
                // a NULL map is a NULL struct field
                Some(r#"{"s":3}"#.to_string()),
            ]
        );
    }

    #[test]
    fn test_string_escaping() {
        let out = json_of(Arc::new(StringArray::from(vec![
            "he said \"hi\"\n",
            "a\\b",
            "a\u{1f}b",
            "π",
            "🦀",
        ])));
        // serde_json escaping: short escapes, lowercase \u00xx, non-ASCII as is
        assert_eq!(
            out,
            vec![
                Some(r#""he said \"hi\"\n""#.to_string()),
                Some(r#""a\\b""#.to_string()),
                Some(r#""a\u001fb""#.to_string()),
                Some(r#""π""#.to_string()),
                Some(r#""🦀""#.to_string()),
            ]
        );
    }

    #[test]
    fn test_binary_is_hex() {
        let bytes: &[u8] = b"abc";
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(BinaryArray::from(vec![Some(bytes), None])),
            Arc::new(LargeBinaryArray::from(vec![Some(bytes), None])),
            Arc::new(BinaryViewArray::from(vec![Some(bytes), None])),
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    [Some(bytes), None].into_iter(),
                    3,
                )
                .unwrap(),
            ),
        ];
        for array in arrays {
            let t = array.data_type().clone();
            assert_eq!(
                json_of(array),
                vec![Some(r#""616263""#.to_string()), None],
                "{t}"
            );
        }

        // nested in a list
        let mut l = ListBuilder::new(arrow::array::BinaryBuilder::new());
        l.values().append_value(bytes);
        l.values().append_null();
        l.append(true);
        assert_eq!(
            json_of(Arc::new(l.finish())),
            vec![Some(r#"["616263",null]"#.to_string())]
        );
    }

    #[test]
    fn test_non_finite_floats_are_null() {
        let f64s = Float64Array::from(vec![f64::NAN, f64::INFINITY, f64::NEG_INFINITY]);
        assert_eq!(
            json_of(Arc::new(f64s)),
            vec![
                Some("null".to_string()),
                Some("null".to_string()),
                Some("null".to_string())
            ]
        );
        let f32s = Float32Array::from(vec![f32::NAN, 1.5]);
        assert_eq!(
            json_of(Arc::new(f32s)),
            vec![Some("null".to_string()), Some("1.5".to_string())]
        );

        // inside a list and a struct the null is written in place
        let list = ListArray::from_iter_primitive::<Float64Type, _, _>(vec![Some(vec![
            Some(f64::NAN),
            Some(1.5),
        ])]);
        assert_eq!(
            json_of(Arc::new(list)),
            vec![Some("[null,1.5]".to_string())]
        );
        assert_eq!(
            struct_json_of(Arc::new(Float64Array::from(vec![f64::NAN]))),
            r#"{"v":null}"#
        );
    }

    #[test]
    fn test_floats() {
        let out = json_of(Arc::new(Float64Array::from(vec![
            Some(1.0),
            Some(0.001),
            Some(1e-5),
            Some(1e10),
            Some(-2.5),
            None,
        ])));
        assert_eq!(
            out,
            vec![
                Some("1.0".to_string()),
                Some("0.001".to_string()),
                Some("0.00001".to_string()),
                Some("1.0e10".to_string()),
                Some("-2.5".to_string()),
                None,
            ]
        );
        assert_eq!(
            json_of(Arc::new(Float32Array::from(vec![0.1f32]))),
            vec![Some("0.1".to_string())]
        );
    }

    #[test]
    fn test_decimals() {
        // decimals keep their declared scale
        let d = Decimal128Array::from(vec![Some(150), Some(-5), Some(0), None])
            .with_precision_and_scale(10, 2)
            .unwrap();
        assert_eq!(
            json_of(Arc::new(d)),
            vec![
                Some("1.50".to_string()),
                Some("-0.05".to_string()),
                Some("0.00".to_string()),
                None,
            ]
        );
        let d = Decimal128Array::from(vec![10])
            .with_precision_and_scale(10, 8)
            .unwrap();
        assert_eq!(json_of(Arc::new(d)), vec![Some("0.00000010".to_string())]);
        let d = Decimal128Array::from(vec![0])
            .with_precision_and_scale(38, 10)
            .unwrap();
        assert_eq!(json_of(Arc::new(d)), vec![Some("0.0000000000".to_string())]);

        // the other decimal widths are written the same way
        let d = Decimal32Array::from(vec![150])
            .with_precision_and_scale(5, 2)
            .unwrap();
        assert_eq!(json_of(Arc::new(d)), vec![Some("1.50".to_string())]);
        let d = Decimal256Array::from(vec![i256::from(150)])
            .with_precision_and_scale(50, 2)
            .unwrap();
        assert_eq!(json_of(Arc::new(d)), vec![Some("1.50".to_string())]);
    }

    #[test]
    fn test_dates() {
        // 2024-01-01 is day 19723
        assert_eq!(
            json_of(Arc::new(Date32Array::from(vec![Some(19723), None]))),
            vec![Some(r#""2024-01-01""#.to_string()), None]
        );
        // arrow-json writes a Date64 as a datetime
        assert_eq!(
            json_of(Arc::new(Date64Array::from(vec![19723 * 86_400_000]))),
            vec![Some(r#""2024-01-01T00:00:00""#.to_string())]
        );
    }

    #[test]
    fn test_timestamps() {
        // 2024-01-01 01:02:03.123456 UTC
        let micros = 1_704_070_923_123_456;

        // without a time zone the wall-clock time is written as is, with as
        // many fraction digits as needed
        assert_eq!(
            json_of(Arc::new(TimestampMicrosecondArray::from(vec![
                Some(micros),
                None
            ]))),
            vec![Some(r#""2024-01-01T01:02:03.123456""#.to_string()), None]
        );
        assert_eq!(
            json_of(Arc::new(TimestampSecondArray::from(vec![
                micros / 1_000_000
            ]))),
            vec![Some(r#""2024-01-01T01:02:03""#.to_string())]
        );

        // with a time zone the instant is written in that zone, RFC 3339 style
        assert_eq!(
            json_of(Arc::new(
                TimestampMicrosecondArray::from(vec![micros]).with_timezone("UTC")
            )),
            vec![Some(r#""2024-01-01T01:02:03.123456Z""#.to_string())]
        );
        assert_eq!(
            json_of(Arc::new(
                TimestampMicrosecondArray::from(vec![micros]).with_timezone("+02:00")
            )),
            vec![Some(r#""2024-01-01T03:02:03.123456+02:00""#.to_string())]
        );
    }

    #[test]
    fn test_dictionary_values() {
        // dictionary-encoded values are written like their values
        let strings = Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef;
        let dict = DictionaryArray::<Int32Type>::new(
            Int32Array::from(vec![Some(1), Some(0), None]),
            strings,
        );
        assert_eq!(
            json_of(Arc::new(dict)),
            vec![Some(r#""b""#.to_string()), Some(r#""a""#.to_string()), None]
        );

        let floats = Arc::new(Float64Array::from(vec![1e10])) as ArrayRef;
        let dict = DictionaryArray::<Int32Type>::new(Int32Array::from(vec![0]), floats);
        assert_eq!(json_of(Arc::new(dict)), vec![Some("1.0e10".to_string())]);
    }

    #[test]
    fn test_large_numbers() {
        // integers are written as bare JSON numbers, whatever their size
        assert_eq!(
            json_of(Arc::new(Int64Array::from(vec![i64::MAX, i64::MIN]))),
            vec![
                Some("9223372036854775807".to_string()),
                Some("-9223372036854775808".to_string()),
            ]
        );
        assert_eq!(
            json_of(Arc::new(UInt64Array::from(vec![u64::MAX]))),
            vec![Some("18446744073709551615".to_string())]
        );

        // decimals at their maximum precision
        let d = Decimal128Array::from(vec![i128::pow(10, 38) - 1])
            .with_precision_and_scale(38, 0)
            .unwrap();
        assert_eq!(json_of(Arc::new(d)), vec![Some("9".repeat(38))]);
        let d = Decimal256Array::from(vec![i256::from_string(&"9".repeat(76)).unwrap()])
            .with_precision_and_scale(76, 0)
            .unwrap();
        assert_eq!(json_of(Arc::new(d)), vec![Some("9".repeat(76))]);
    }

    #[test]
    fn test_times_durations_and_intervals() {
        // 01:02:03 and 01:02:03.123456
        assert_eq!(
            json_of(Arc::new(Time32SecondArray::from(vec![Some(3723), None]))),
            vec![Some(r#""01:02:03""#.to_string()), None]
        );
        assert_eq!(
            json_of(Arc::new(Time64MicrosecondArray::from(vec![3_723_123_456]))),
            vec![Some(r#""01:02:03.123456""#.to_string())]
        );
        // durations are ISO 8601, intervals use arrow's text form
        assert_eq!(
            json_of(Arc::new(DurationSecondArray::from(vec![90]))),
            vec![Some(r#""PT90S""#.to_string())]
        );
        assert_eq!(
            json_of(Arc::new(IntervalMonthDayNanoArray::from(vec![
                IntervalMonthDayNano::new(0, 1, 0),
                IntervalMonthDayNano::new(1, 0, 0),
            ]))),
            vec![
                Some(r#""1 days""#.to_string()),
                Some(r#""1 mons""#.to_string())
            ]
        );
    }

    #[test]
    fn test_deeply_nested_values() {
        // l: List<Struct<m: Map<Utf8, List<Int32>>>>, n: Struct<n: Struct<x: Int32>>
        //   0: l = [{m: {k: [1, NULL]}}, NULL, {m: {k: NULL}}], n = {n: {x: 1}}
        //   1: l = NULL,                                        n = NULL
        //   2: l = [],                                          n = {n: {x: 3}}
        let mut m = MapBuilder::new(
            None,
            StringBuilder::new(),
            ListBuilder::new(Int32Builder::new()),
        );
        m.keys().append_value("k");
        m.values().values().append_value(1);
        m.values().values().append_null();
        m.values().append(true);
        m.append(true).unwrap();
        m.append(false).unwrap();
        m.keys().append_value("k");
        m.values().append(false);
        m.append(true).unwrap();
        let m = Arc::new(m.finish()) as ArrayRef;

        let elems = StructArray::new(
            Fields::from(vec![Field::new("m", m.data_type().clone(), true)]),
            vec![m],
            Some(NullBuffer::from(vec![true, false, true])),
        );
        let l = ListArray::new(
            Arc::new(Field::new("item", elems.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 3, 3, 3].into()),
            Arc::new(elems),
            Some(NullBuffer::from(vec![true, false, true])),
        );

        let x = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let inner = StructArray::new(
            Fields::from(vec![Field::new("x", DataType::Int32, true)]),
            vec![x],
            None,
        );
        let n = StructArray::new(
            Fields::from(vec![Field::new("n", inner.data_type().clone(), true)]),
            vec![Arc::new(inner)],
            Some(NullBuffer::from(vec![true, false, true])),
        );

        let fields = Fields::from(vec![
            Field::new("l", l.data_type().clone(), true),
            Field::new("n", n.data_type().clone(), true),
        ]);
        let input = StructArray::new(fields, vec![Arc::new(l), Arc::new(n)], None);

        let expected = [
            // a NULL list element is written as null, a NULL map entry and a
            // NULL struct field are omitted
            Some(
                r#"{"l":[{"m":{"k":[1,null]}},null,{"m":{}}],"n":{"n":{"x":1}}}"#
                    .to_string(),
            ),
            Some("{}".to_string()),
            Some(r#"{"l":[],"n":{"n":{"x":3}}}"#.to_string()),
        ];
        assert_eq!(json_of(Arc::new(input.clone())), expected);
        // and the same through a slice with a non-zero offset
        assert_eq!(json_of(Arc::new(input.slice(1, 2))), expected[1..]);
    }
}
