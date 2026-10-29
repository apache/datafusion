// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::{Arc, LazyLock};

use arrow::array::{
    Array, ArrayAccessor, ArrayRef, AsArray, MapArray, PrimitiveArray, StringBuilder,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, FieldRef, Float32Type, Float64Type,
};
use arrow::error::ArrowError;
use arrow::json::writer::{
    Encoder, EncoderFactory, EncoderOptions, NullableEncoder, make_encoder,
};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
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
    // Spark default: ignoreNullFields = true -> NULL struct fields are omitted.
    // The factory covers the places where Spark's output differs from arrow's
    // defaults (NULL map entries are kept, binary is Base64, non-finite floats
    // are strings; see `SparkJsonEncoderFactory`).
    let options = EncoderOptions::default()
        .with_explicit_nulls(false)
        .with_encoder_factory(Arc::new(SparkJsonEncoderFactory));
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

/// Overrides arrow-json's default encoders where Spark's `to_json` output
/// differs from arrow's. `make_encoder` consults the factory for every array at
/// every nesting level, so the overrides also apply inside lists, structs and
/// maps.
#[derive(Debug, Default)]
struct SparkJsonEncoderFactory;

impl EncoderFactory for SparkJsonEncoderFactory {
    fn make_default_encoder<'a>(
        &self,
        field: &'a FieldRef,
        array: &'a dyn Array,
        options: &'a EncoderOptions,
    ) -> Result<Option<NullableEncoder<'a>>, ArrowError> {
        match array.data_type() {
            DataType::Map(_, _) => {
                let array = array.as_map();
                let encoder = SparkMapEncoder::try_new(field, array, options)?;
                Ok(Some(NullableEncoder::new(
                    Box::new(encoder),
                    array.nulls().cloned(),
                )))
            }
            // Spark serializes binary values as Base64 strings, arrow-json as hex
            DataType::Binary => Ok(Some(base64_encoder(array.as_binary::<i32>()))),
            DataType::LargeBinary => Ok(Some(base64_encoder(array.as_binary::<i64>()))),
            DataType::BinaryView => Ok(Some(base64_encoder(array.as_binary_view()))),
            DataType::FixedSizeBinary(_) => {
                Ok(Some(base64_encoder(array.as_fixed_size_binary())))
            }
            // Spark writes non-finite floats as the strings "NaN", "Infinity"
            // and "-Infinity"; arrow-json writes JSON null, which loses the value
            DataType::Float32 => Ok(Some(spark_float_encoder(
                field,
                array.as_primitive::<Float32Type>(),
            )?)),
            DataType::Float64 => Ok(Some(spark_float_encoder(
                field,
                array.as_primitive::<Float64Type>(),
            )?)),
            // everything else uses arrow's default encoder
            _ => Ok(None),
        }
    }
}

/// arrow-json's default options, used to build the inner encoders that the
/// Spark overrides delegate to (going through `SparkJsonEncoderFactory` again
/// for the same array would recurse).
static ARROW_DEFAULT_OPTIONS: LazyLock<EncoderOptions> =
    LazyLock::new(EncoderOptions::default);

/// Floating point types whose non-finite values Spark serializes as strings.
trait SparkFloat: Copy {
    /// The JSON text Spark emits for a non-finite value, or `None` if finite.
    fn non_finite_json(self) -> Option<&'static [u8]>;
}

macro_rules! spark_float {
    ($($t:ty),*) => {$(
        impl SparkFloat for $t {
            fn non_finite_json(self) -> Option<&'static [u8]> {
                if self.is_nan() {
                    Some(b"\"NaN\"")
                } else if self.is_infinite() {
                    Some(if self.is_sign_positive() {
                        b"\"Infinity\""
                    } else {
                        b"\"-Infinity\""
                    })
                } else {
                    None
                }
            }
        }
    )*};
}
spark_float!(f32, f64);

/// Builds a float encoder that writes finite values exactly like arrow-json
/// and non-finite values the way Spark does.
fn spark_float_encoder<'a, T>(
    field: &'a FieldRef,
    array: &'a PrimitiveArray<T>,
) -> Result<NullableEncoder<'a>, ArrowError>
where
    T: ArrowPrimitiveType,
    T::Native: SparkFloat,
{
    let finite = make_encoder(field, array, &ARROW_DEFAULT_OPTIONS)?;
    let encoder = SparkFloatEncoder {
        values: array.values(),
        finite,
    };
    Ok(NullableEncoder::new(
        Box::new(encoder),
        array.nulls().cloned(),
    ))
}

struct SparkFloatEncoder<'a, N: SparkFloat> {
    values: &'a [N],
    /// arrow's default float encoder, used for finite values
    finite: NullableEncoder<'a>,
}

impl<N: SparkFloat> Encoder for SparkFloatEncoder<'_, N> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        match self.values[idx].non_finite_json() {
            Some(text) => out.extend_from_slice(text),
            None => self.finite.encode(idx, out),
        }
    }
}

/// Builds a Base64 encoder for any of arrow's binary array types.
fn base64_encoder<'a, B>(array: B) -> NullableEncoder<'a>
where
    B: ArrayAccessor<Item = &'a [u8]> + Array + 'a,
{
    let nulls = array.nulls().cloned();
    NullableEncoder::new(Box::new(Base64Encoder::new(array)), nulls)
}

/// Encodes binary values as Base64 strings (standard alphabet, padded), which
/// is what Spark's `to_json` produces; arrow-json's default is hexadecimal.
struct Base64Encoder<B> {
    array: B,
    scratch: String,
}

impl<B> Base64Encoder<B> {
    fn new(array: B) -> Self {
        Self {
            array,
            scratch: String::new(),
        }
    }
}

impl<'a, B> Encoder for Base64Encoder<B>
where
    B: ArrayAccessor<Item = &'a [u8]>,
{
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        self.scratch.clear();
        BASE64.encode_string(self.array.value(idx), &mut self.scratch);
        // Base64 output is [A-Za-z0-9+/=], so it needs no JSON escaping
        out.push(b'"');
        out.extend_from_slice(self.scratch.as_bytes());
        out.push(b'"');
    }
}

/// Encodes a map as a JSON object, always writing NULL values as `"key":null`.
///
/// arrow-json's own map encoder drops entries with a NULL value when
/// `explicit_nulls` is off, but Spark's `ignoreNullFields` only applies to
/// struct fields: map entries with a NULL value are kept.
struct SparkMapEncoder<'a> {
    offsets: OffsetBuffer<i32>,
    keys: NullableEncoder<'a>,
    values: NullableEncoder<'a>,
}

impl<'a> SparkMapEncoder<'a> {
    fn try_new(
        field: &'a FieldRef,
        array: &'a MapArray,
        options: &'a EncoderOptions,
    ) -> Result<Self, ArrowError> {
        let keys = array.keys();
        if !matches!(
            keys.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            return Err(ArrowError::JsonError(format!(
                "to_json: only string map keys are supported, got {}",
                keys.data_type()
            )));
        }

        // Key and value encoders are built with the same options, so nested
        // structs keep omitting NULL fields and nested maps use this encoder.
        let keys = make_encoder(field, keys, options)?;
        let values = make_encoder(field, array.values(), options)?;

        if keys.has_nulls() {
            return Err(ArrowError::InvalidArgumentError(
                "to_json: encountered a NULL map key".to_string(),
            ));
        }

        Ok(Self {
            offsets: array.offsets().clone(),
            keys,
            values,
        })
    }
}

impl Encoder for SparkMapEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let start = self.offsets[idx] as usize;
        let end = self.offsets[idx + 1] as usize;

        out.push(b'{');
        for i in start..end {
            if i != start {
                out.push(b',');
            }
            self.keys.encode(i, out);
            out.push(b':');
            if self.values.is_null(i) {
                out.extend_from_slice(b"null");
            } else {
                self.values.encode(i, out);
            }
        }
        out.push(b'}');
    }
}

/// Input accepted by the to_json expression builder.
///
/// Implemented for an expression that evaluates to a struct, such as a struct column or a `named_struct` call,
/// which is used as-is; and for a slice or array of column names, which are packed into a struct first.
/// In the second form the JSON keys are the plain column names without any table qualifier.
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
        BinaryArray, BinaryBuilder, BinaryViewArray, BooleanArray, FixedSizeBinaryArray,
        Float32Array, Float64Array, Float64Builder, Int32Array, Int32Builder, Int64Array,
        LargeBinaryArray, ListBuilder, MapBuilder, StringArray, StructArray,
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
    #[test]
    fn test_map_null_values_kept_but_null_struct_fields_omitted() {
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
        let field = field_of(&input);

        let out = run(ColumnarValue::Array(Arc::new(input)), field).unwrap();
        let out = as_string_array(&out).unwrap();

        // a NULL map *value* is written explicitly (Spark keeps map entries)
        assert_eq!(out.value(0), r#"{"m":{"k":null},"s":1}"#);
        // ...while a NULL struct *field* (`s`) is still omitted
        assert_eq!(out.value(1), r#"{"m":{"a":1,"b":null}}"#);
        // a NULL map is a NULL struct field, so it is omitted as well
        assert_eq!(out.value(2), r#"{"s":3}"#);
    }
    #[test]
    fn test_binary_is_base64_encoded() {
        // b: Binary ["abc", "", NULL] -> "YWJj", "", omitted
        let b = Arc::new(BinaryArray::from(vec![
            Some(b"abc".as_slice()),
            Some(b"".as_slice()),
            None,
        ])) as ArrayRef;
        let abc = vec![Some(b"abc".as_slice()); 3];
        let lb = Arc::new(LargeBinaryArray::from(abc.clone())) as ArrayRef;
        let bv = Arc::new(BinaryViewArray::from(abc)) as ArrayRef;
        let fsb = Arc::new(
            FixedSizeBinaryArray::try_from_iter(vec![b"ab".as_slice(); 3].into_iter())
                .unwrap(),
        ) as ArrayRef;

        // l: List<Binary> [["abc"]] per row; n: Struct<b: Binary> {"abc"} per row
        let mut l = ListBuilder::new(BinaryBuilder::new());
        for _ in 0..3 {
            l.values().append_value(b"abc");
            l.append(true);
        }
        let l = Arc::new(l.finish()) as ArrayRef;
        let n = Arc::new(StructArray::new(
            Fields::from(vec![Field::new("b", DataType::LargeBinary, true)]),
            vec![Arc::clone(&lb)],
            None,
        )) as ArrayRef;

        let fields = Fields::from(vec![
            Field::new("b", DataType::Binary, true),
            Field::new("lb", DataType::LargeBinary, true),
            Field::new("bv", DataType::BinaryView, true),
            Field::new("fsb", DataType::FixedSizeBinary(2), true),
            Field::new("l", l.data_type().clone(), true),
            Field::new("n", n.data_type().clone(), true),
        ]);
        let input = StructArray::new(fields, vec![b, lb, bv, fsb, l, n], None);
        let field = field_of(&input);

        let out = run(ColumnarValue::Array(Arc::new(input)), field).unwrap();
        let out = as_string_array(&out).unwrap();

        assert_eq!(
            out.value(0),
            r#"{"b":"YWJj","lb":"YWJj","bv":"YWJj","fsb":"YWI=","l":["YWJj"],"n":{"b":"YWJj"}}"#
        );
        // empty binary is an empty string, not NULL
        assert_eq!(
            out.value(1),
            r#"{"b":"","lb":"YWJj","bv":"YWJj","fsb":"YWI=","l":["YWJj"],"n":{"b":"YWJj"}}"#
        );
        // NULL binary is a NULL struct field -> omitted
        assert_eq!(
            out.value(2),
            r#"{"lb":"YWJj","bv":"YWJj","fsb":"YWI=","l":["YWJj"],"n":{"b":"YWJj"}}"#
        );
    }
    #[test]
    fn test_non_finite_floats_are_spark_strings() {
        // d: Float64 [NaN, +inf, -inf, 1.5, NULL]; f: Float32 the same
        let d = Arc::new(Float64Array::from(vec![
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(1.5),
            None,
        ])) as ArrayRef;
        let f = Arc::new(Float32Array::from(vec![
            Some(f32::NAN),
            Some(f32::INFINITY),
            Some(f32::NEG_INFINITY),
            Some(1.5),
            None,
        ])) as ArrayRef;

        // l: List<Float64> [[NaN, 1.5]] per row; n: Struct<d: Float64>
        let mut l = ListBuilder::new(Float64Builder::new());
        for _ in 0..5 {
            l.values().append_value(f64::NAN);
            l.values().append_value(1.5);
            l.append(true);
        }
        let l = Arc::new(l.finish()) as ArrayRef;
        let n = Arc::new(StructArray::new(
            Fields::from(vec![Field::new("d", DataType::Float64, true)]),
            vec![Arc::clone(&d)],
            None,
        )) as ArrayRef;

        let fields = Fields::from(vec![
            Field::new("d", DataType::Float64, true),
            Field::new("f", DataType::Float32, true),
            Field::new("l", l.data_type().clone(), true),
            Field::new("n", n.data_type().clone(), true),
        ]);
        let input = StructArray::new(fields, vec![d, f, l, n], None);
        let field = field_of(&input);

        let out = run(ColumnarValue::Array(Arc::new(input)), field).unwrap();
        let out = as_string_array(&out).unwrap();

        assert_eq!(
            out.value(0),
            r#"{"d":"NaN","f":"NaN","l":["NaN",1.5],"n":{"d":"NaN"}}"#
        );
        assert_eq!(
            out.value(1),
            r#"{"d":"Infinity","f":"Infinity","l":["NaN",1.5],"n":{"d":"Infinity"}}"#
        );
        assert_eq!(
            out.value(2),
            r#"{"d":"-Infinity","f":"-Infinity","l":["NaN",1.5],"n":{"d":"-Infinity"}}"#
        );
        // finite values keep arrow's formatting
        assert_eq!(
            out.value(3),
            r#"{"d":1.5,"f":1.5,"l":["NaN",1.5],"n":{"d":1.5}}"#
        );
        // NULL is still a NULL struct field -> omitted
        assert_eq!(out.value(4), r#"{"l":["NaN",1.5],"n":{}}"#);
    }
}

/// DataFrame-level tests: to_json used through DataFrame::with_column
/// and through SQL, the way a user of the crate calls it.
#[cfg(test)]
mod dataframe_tests {
    use super::*;

    use datafusion::prelude::*;
    use datafusion::test_util::{register_aggregate_csv, test_table};
    use datafusion_common::cast::as_string_array;
    use datafusion_common::test_util::batches_to_sort_string;
    use datafusion_common::{assert_batches_sorted_eq, assert_contains};
    use datafusion_functions_nested::expr_fn::make_array;

    use crate::function::json::expr_fn::to_json;

    #[tokio::test]
    async fn with_column_to_json() -> Result<()> {
        let df = test_table()
            .await?
            .select_columns(&["c1", "c2", "c3"])?
            .filter(col("c2").eq(lit(3)).and(col("c1").eq(lit("a"))))?
            .with_column("json", to_json(&["c1", "c2", "c3"]))?;

        // check that the new column is a nullable string
        let json_field = df.schema().field_with_unqualified_name("json")?;
        assert_eq!(json_field.data_type(), &DataType::Utf8);

        // check that new column added
        let df_results = df.clone().collect().await?;
        assert_batches_sorted_eq!(
            [
                "+----+----+-----+----------------------------+",
                "| c1 | c2 | c3  | json                       |",
                "+----+----+-----+----------------------------+",
                "| a  | 3  | -12 | {\"c1\":\"a\",\"c2\":3,\"c3\":-12} |",
                "| a  | 3  | -72 | {\"c1\":\"a\",\"c2\":3,\"c3\":-72} |",
                "| a  | 3  | 13  | {\"c1\":\"a\",\"c2\":3,\"c3\":13}  |",
                "| a  | 3  | 13  | {\"c1\":\"a\",\"c2\":3,\"c3\":13}  |",
                "| a  | 3  | 14  | {\"c1\":\"a\",\"c2\":3,\"c3\":14}  |",
                "| a  | 3  | 17  | {\"c1\":\"a\",\"c2\":3,\"c3\":17}  |",
                "+----+----+-----+----------------------------+",
            ],
            &df_results
        );

        // check that a column with the same name is overwritten, which folds the
        // source columns into a single JSON column
        let df_results_overwrite = df
            .clone()
            .with_column("c3", to_json(&["c2", "c3"]))?
            .collect()
            .await?;
        assert_batches_sorted_eq!(
            [
                "+----+----+-------------------+----------------------------+",
                "| c1 | c2 | c3                | json                       |",
                "+----+----+-------------------+----------------------------+",
                "| a  | 3  | {\"c2\":3,\"c3\":-12} | {\"c1\":\"a\",\"c2\":3,\"c3\":-12} |",
                "| a  | 3  | {\"c2\":3,\"c3\":-72} | {\"c1\":\"a\",\"c2\":3,\"c3\":-72} |",
                "| a  | 3  | {\"c2\":3,\"c3\":13}  | {\"c1\":\"a\",\"c2\":3,\"c3\":13}  |",
                "| a  | 3  | {\"c2\":3,\"c3\":13}  | {\"c1\":\"a\",\"c2\":3,\"c3\":13}  |",
                "| a  | 3  | {\"c2\":3,\"c3\":14}  | {\"c1\":\"a\",\"c2\":3,\"c3\":14}  |",
                "| a  | 3  | {\"c2\":3,\"c3\":17}  | {\"c1\":\"a\",\"c2\":3,\"c3\":17}  |",
                "+----+----+-------------------+----------------------------+",
            ],
            &df_results_overwrite
        );

        // check that an existing struct column can be serialized with col(..),
        // and that it gives the same result as packing the columns by name
        let df_struct_col = df
            .clone()
            .with_column(
                "s",
                named_struct(vec![
                    lit("c1"),
                    col("c1"),
                    lit("c2"),
                    col("c2"),
                    lit("c3"),
                    col("c3"),
                ]),
            )?
            .with_column("json_from_struct", to_json(col("s")))?
            .select(vec![
                col("c1"),
                col("c2"),
                col("c3"),
                col("json_from_struct"),
            ])?
            .with_column_renamed("json_from_struct", "json")?;
        assert_eq!(
            batches_to_sort_string(&df_struct_col.collect().await?),
            batches_to_sort_string(&df_results)
        );

        // check that a non-struct argument is rejected when the plan is built
        let err = df
            .with_column("bad", to_json(col("c1")))
            .unwrap_err()
            .to_string();
        assert_contains!(err, "to_json expects a struct argument");

        // check that the same result is produced through SQL: to_json is a
        // regular scalar UDF, so once registered it works on any struct
        // expression (`with_spark_features()` registers it together with all
        // other Spark functions; here only this one is registered)
        let ctx = SessionContext::new();
        ctx.register_udf((*crate::function::json::to_json()).clone());
        register_aggregate_csv(&ctx, "aggregate_test_100").await?;

        let sql_results = ctx
            .sql(
                "SELECT c1, c2, c3, \
                        to_json(named_struct('c1', c1, 'c2', c2, 'c3', c3)) AS json \
                 FROM aggregate_test_100 \
                 WHERE c2 = 3 AND c1 = 'a'",
            )
            .await?
            .collect()
            .await?;
        assert_eq!(
            batches_to_sort_string(&sql_results),
            batches_to_sort_string(&df_results)
        );

        // the SQL path rejects a non-struct argument at plan time as well
        let err = ctx
            .sql("SELECT to_json(c1) FROM aggregate_test_100")
            .await
            .unwrap_err()
            .to_string();
        assert_contains!(err, "to_json expects a struct argument");

        Ok(())
    }

    #[tokio::test]
    async fn with_column_to_json_edge_cases() -> Result<()> {
        // NULL fields are omitted from the object (Spark ignoreNullFields
        // default), so a row whose fields are all NULL becomes an empty object
        let df = dataframe!(
            "a" => [Some(1_i32), None],
            "b" => [None, None::<&str>],
        )?
        .with_column("json", to_json(&["a", "b"]))?;
        assert_batches_sorted_eq!(
            [
                "+---+---+---------+",
                "| a | b | json    |",
                "+---+---+---------+",
                "|   |   | {}      |",
                "| 1 |   | {\"a\":1} |",
                "+---+---+---------+",
            ],
            &df.collect().await?
        );

        // a constant struct is evaluated once at plan time (scalar path) and
        // broadcast to every row
        let df = dataframe!("id" => [1_i32, 2])?
            .with_column("json", to_json(named_struct(vec![lit("a"), lit(1)])))?;
        assert_batches_sorted_eq!(
            [
                "+----+---------+",
                "| id | json    |",
                "+----+---------+",
                "| 1  | {\"a\":1} |",
                "| 2  | {\"a\":1} |",
                "+----+---------+",
            ],
            &df.collect().await?
        );
        // nested structs and lists are serialized recursively
        let df = dataframe!("a" => [1_i32, 2])?.with_column(
            "json",
            to_json(named_struct(vec![
                lit("n"),
                named_struct(vec![lit("x"), col("a")]),
                lit("l"),
                make_array(vec![col("a"), lit(9)]),
            ])),
        )?;
        assert_batches_sorted_eq!(
            [
                "+---+-------------------------+",
                "| a | json                    |",
                "+---+-------------------------+",
                "| 1 | {\"n\":{\"x\":1},\"l\":[1,9]} |",
                "| 2 | {\"n\":{\"x\":2},\"l\":[2,9]} |",
                "+---+-------------------------+",
            ],
            &df.collect().await?
        );

        // string values are JSON-escaped; non-ASCII is emitted as-is
        let df = dataframe!("s" => [r#"he said "hi""#, r"a\b", "π"])?
            .with_column("json", to_json(&["s"]))?;
        assert_batches_sorted_eq!(
            [
                "+--------------+------------------------+",
                "| s            | json                   |",
                "+--------------+------------------------+",
                "| a\\b          | {\"s\":\"a\\\\b\"}           |",
                "| he said \"hi\" | {\"s\":\"he said \\\"hi\\\"\"} |",
                "| π            | {\"s\":\"π\"}              |",
                "+--------------+------------------------+",
            ],
            &df.collect().await?
        );

        // a qualified column name is resolved as such, but the JSON key is the
        // bare column name
        let df = test_table()
            .await?
            .filter(col("c2").eq(lit(3)).and(col("c1").eq(lit("a"))))?
            .with_column("json", to_json(&["aggregate_test_100.c1", "c2"]))?
            .select(vec![col("json")])?
            .distinct()?;
        assert_batches_sorted_eq!(
            [
                "+-------------------+",
                "| json              |",
                "+-------------------+",
                "| {\"c1\":\"a\",\"c2\":3} |",
                "+-------------------+",
            ],
            &df.collect().await?
        );

        // an empty input produces no rows but still carries the new column
        let df = test_table()
            .await?
            .filter(col("c2").eq(lit(999)))?
            .with_column("json", to_json(&["c1"]))?;
        assert_eq!(
            df.schema().field_with_unqualified_name("json")?.data_type(),
            &DataType::Utf8
        );
        assert_eq!(df.count().await?, 0);

        // an empty column list cannot build a struct and fails when planning
        // (rejected by the named_struct signature check)
        let err = dataframe!("a" => [1_i32])?
            .with_column("json", to_json(&[] as &[&str]))
            .unwrap_err()
            .to_string();
        assert_contains!(err, "'named_struct' does not support zero arguments");

        Ok(())
    }
    /// Evaluates `SELECT to_json(<expr>)` for each `(expr, expected)` pair and
    /// checks the JSON string.
    ///
    /// The `datafusion` dev-dependency is built without the
    /// `nested_expressions` feature, so the nested functions used to build
    /// test values (`make_array`, `map`, ...) are registered here explicitly.
    async fn assert_sql_json(cases: &[(&str, &str)]) -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_udf((*crate::function::json::to_json()).clone());
        for udf in datafusion_functions_nested::all_default_nested_functions() {
            ctx.register_udf((*udf).clone());
        }

        for (expr, expected) in cases {
            let batches = ctx
                .sql(&format!("SELECT to_json({expr}) AS j"))
                .await?
                .collect()
                .await?;
            let j = as_string_array(batches[0].column(0))?;
            assert_eq!(j.value(0), *expected, "expression: {expr}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn to_json_keeps_null_map_values_sql() -> Result<()> {
        assert_sql_json(&[
            // NULL map value is kept
            (
                "named_struct('m', map(make_array('k'), make_array(CAST(NULL AS INT))))",
                r#"{"m":{"k":null}}"#,
            ),
            // nested map inside a map value
            (
                "named_struct('m', map(make_array('o'), make_array(map(make_array('k'), make_array(CAST(NULL AS INT))))))",
                r#"{"m":{"o":{"k":null}}}"#,
            ),
            // a struct inside a map value still omits its NULL fields
            (
                "named_struct('m', map(make_array('k'), make_array(named_struct('a', 1, 'b', CAST(NULL AS INT)))))",
                r#"{"m":{"k":{"a":1}}}"#,
            ),
        ])
        .await
    }

    #[tokio::test]
    async fn to_json_encodes_binary_as_base64_sql() -> Result<()> {
        assert_sql_json(&[
            (
                "named_struct('b', arrow_cast('abc', 'Binary'))",
                r#"{"b":"YWJj"}"#,
            ),
            (
                "named_struct('b', arrow_cast('abc', 'LargeBinary'))",
                r#"{"b":"YWJj"}"#,
            ),
            (
                "named_struct('b', arrow_cast('abc', 'BinaryView'))",
                r#"{"b":"YWJj"}"#,
            ),
            // nested in a list, in a struct and in a map value
            (
                "named_struct('l', make_array(arrow_cast('abc', 'Binary')))",
                r#"{"l":["YWJj"]}"#,
            ),
            (
                "named_struct('n', named_struct('b', arrow_cast('abc', 'Binary')))",
                r#"{"n":{"b":"YWJj"}}"#,
            ),
            (
                "named_struct('m', map(make_array('k'), make_array(arrow_cast('abc', 'Binary'))))",
                r#"{"m":{"k":"YWJj"}}"#,
            ),
        ])
        .await
    }
    #[tokio::test]
    async fn to_json_non_finite_floats_sql() -> Result<()> {
        assert_sql_json(&[
            (
                "named_struct('x', CAST('NaN' AS DOUBLE))",
                r#"{"x":"NaN"}"#,
            ),
            (
                "named_struct('x', CAST('Infinity' AS DOUBLE))",
                r#"{"x":"Infinity"}"#,
            ),
            (
                "named_struct('x', CAST('-Infinity' AS DOUBLE))",
                r#"{"x":"-Infinity"}"#,
            ),
            (
                "named_struct('x', arrow_cast('NaN', 'Float32'))",
                r#"{"x":"NaN"}"#,
            ),
            // finite values are unchanged
            ("named_struct('x', 1.5)", r#"{"x":1.5}"#),
            // nested in a list, a struct and a map value
            (
                "named_struct('l', make_array(CAST('NaN' AS DOUBLE), 1.5))",
                r#"{"l":["NaN",1.5]}"#,
            ),
            (
                "named_struct('n', named_struct('x', CAST('Infinity' AS DOUBLE)))",
                r#"{"n":{"x":"Infinity"}}"#,
            ),
            (
                "named_struct('m', map(make_array('k'), make_array(CAST('NaN' AS DOUBLE))))",
                r#"{"m":{"k":"NaN"}}"#,
            ),
        ])
        .await
    }
}
