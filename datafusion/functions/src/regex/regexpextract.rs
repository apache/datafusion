use arrow::array::ArrayAccessor;
use arrow::array::{
    Array, ArrayIter, ArrayRef, AsArray, GenericStringArray, StringViewArray,
};
use arrow::datatypes::DataType;
use datafusion_common::exec_err;
use datafusion_common::plan_err;
use datafusion_common::ScalarValue;
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_expr::ColumnarValue;
use datafusion_expr::TypeSignature;
use datafusion_expr::{Documentation, ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;
use regex::Regex;
use std::any::Any;
use std::sync::Arc;

#[user_doc(
    doc_section(label = "Regular Expression Functions"),
    description = "Extract a specific group matched by regexp, from the specified string column. If the regex did not match, or the specified group did not match, an empty string is returned.",
    syntax_example = "regexp_extract(str, regexp, group)",
    standard_argument(name = "str", prefix = "String"),
    argument(
        name = "regexp",
        description = "Regular expression to match against. Can be a constant string only"
    ),
    argument(
        name = "group",
        description = "Matched group id. Can be an integer literal only."
    )
)]
#[derive(Debug)]
pub struct RegexpExtractFunc {
    signature: Signature,
}
impl Default for RegexpExtractFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpExtractFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![Utf8, Utf8, UInt32]),
                    TypeSignature::Exact(vec![Utf8View, Utf8, UInt32]),
                    TypeSignature::Exact(vec![LargeUtf8, Utf8, UInt32]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpExtractFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;
        Ok(match &arg_types[0] {
            supported @ (Utf8 | LargeUtf8 | Utf8View) => supported.clone(),
            Null => Null,
            Dictionary(_, t) => match t.as_ref() {
                supported @ (Utf8 | LargeUtf8 | Utf8View) => supported.clone(),
                Null => Null,
                _ => {
                    return plan_err!(
                        "the regexp_extract can only accept strings but got {:?}",
                        **t
                    );
                }
            },
            other => {
                return plan_err!(
                    "The regexp_extract function can only accept strings. Got {other}"
                );
            }
        })
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let is_scalar = !args
            .args
            .iter()
            .any(|arg| matches!(arg, ColumnarValue::Array(_)));
        let result = regexp_extract_func(args.args);
        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn regexp_extract_func(args: Vec<ColumnarValue>) -> Result<ArrayRef> {
    let (input_col, regexp, group) = extract_args(args)?;

    let datatype = input_col.data_type().to_owned();
    let input_array = input_col.into_array(1)?;
    match datatype {
        DataType::Utf8 => {
            let string_array = input_array.as_string::<i32>();
            regexp_extract::<_, GenericStringArray<i32>>(string_array, regexp, group)
        }
        DataType::Utf8View => {
            let string_array = input_array.as_string_view();
            regexp_extract::<_, StringViewArray>(string_array, regexp, group)
        }
        DataType::LargeUtf8 => {
            let string_array = input_array.as_string::<i64>();
            regexp_extract::<_, GenericStringArray<i64>>(string_array, regexp, group)
        }
        other => {
            internal_err!("Unsupported data type {other:?} for function regexp_extract")
        }
    }
}

pub fn regexp_extract<'a, A, B>(
    string_array: A,
    pattern: Regex,
    group: usize,
) -> Result<ArrayRef>
where
    A: ArrayAccessor<Item = &'a str>,
    B: FromIterator<Option<&'a str>> + Array + 'static,
{
    let result = if group < pattern.captures_len() {
        ArrayIter::new(string_array)
            .map(|row| {
                row.and_then(|input| pattern.captures(input))
                    .and_then(|c| c.get(group))
                    .map(|m| m.as_str())
            })
            .collect::<B>()
    } else {
        // do not perform search if the group is not present
        std::iter::repeat_n(None, string_array.len()).collect::<B>()
    };
    Ok(Arc::new(result) as ArrayRef)
}

fn extract_args(args: Vec<ColumnarValue>) -> Result<(ColumnarValue, Regex, usize)> {
    let Ok([input_col, pattern_col, group_col]): Result<[ColumnarValue; 3], _> =
        args.try_into()
    else {
        return exec_err!("regexp_extract expect 3 arguments");
    };
    let ColumnarValue::Scalar(pattern_scalar) = pattern_col else {
        return exec_err!("regexp_extract 2 argument must be scalar");
    };
    let ColumnarValue::Scalar(group_scalar) = group_col else {
        return exec_err!("regexp_extract 3 argument must be scalar");
    };
    let Some(Some(pattern)) = pattern_scalar.try_as_str() else {
        return exec_err!("regexp_extract 2 argument must be non-null utf8 valid string");
    };
    let Ok(group): Result<u32, _> = group_scalar.try_into() else {
        return exec_err!("regexp_extract 3 argument must be integer");
    };
    let Ok(group): Result<usize, _> = group.try_into() else {
        return exec_err!("regexp_extract 3 argument must be valid integer");
    };

    let regexp =
        Regex::new(pattern).map_err(|err| DataFusionError::External(Box::new(err)))?;

    Ok((input_col, regexp, group))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use arrow::array::StringArray;

    use super::*;

    #[test]
    fn test_empty_args() {
        // given
        let args = Vec::new();

        // when
        let result = regexp_extract_func(args);

        // then
        assert!(matches!(result, Err(DataFusionError::Execution(_))))
    }

    #[test]
    fn test_input_scalar() {
        // given
        let input = ColumnarValue::from(ScalarValue::from_str("hello").unwrap());
        let pattern = ColumnarValue::from(ScalarValue::from_str("[a-h]+").unwrap());
        let group = ColumnarValue::from(ScalarValue::from(0u32));
        let args = vec![input, pattern, group];

        // when
        let result = regexp_extract_func(args).expect("valid result");

        // then
        assert_eq!(result.len(), 1);
        assert_eq!(*result.data_type(), DataType::Utf8);
        let string_array = result
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected an StringArray");
        assert_eq!(string_array.value(0), "he");
    }

    #[test]
    fn test_input_array() {
        // given
        let input =
            ColumnarValue::from(
                Arc::new(StringArray::from(vec!["hello", "world"])) as ArrayRef
            );
        let pattern = ColumnarValue::from(ScalarValue::from_str("[a-h]+").unwrap());
        let group = ColumnarValue::from(ScalarValue::from(0u32));
        let args = vec![input, pattern, group];

        // when
        let result = regexp_extract_func(args).expect("valid result");

        // then
        assert_eq!(result.len(), 2);
        assert_eq!(*result.data_type(), DataType::Utf8);
        let string_array = result
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected an StringArray");
        assert_eq!(string_array.value(0), "he");
        assert_eq!(string_array.value(1), "d");
    }

    #[test]
    fn test_group_is_out_of_range() {
        // given
        let input =
            ColumnarValue::from(
                Arc::new(StringArray::from(vec!["hello", "world"])) as ArrayRef
            );
        let pattern = ColumnarValue::from(ScalarValue::from_str("[a-h]+").unwrap());
        let group = ColumnarValue::from(ScalarValue::from(1u32));
        let args = vec![input, pattern, group];

        // when
        let result = regexp_extract_func(args).expect("valid result");

        // then
        assert_eq!(result.len(), 2);
        assert_eq!(*result.data_type(), DataType::Utf8);
        let string_array = result
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected an StringArray");
        assert_eq!(string_array.value(0), "");
        assert_eq!(string_array.value(1), "");
    }

    #[test]
    fn test_input_large_utf8() {
        // given
        let input =
            ColumnarValue::from(ScalarValue::LargeUtf8(Some("hello".to_string())));
        let pattern = ColumnarValue::from(ScalarValue::from_str("[a-h]+").unwrap());
        let group = ColumnarValue::from(ScalarValue::from(0u32));
        let args = vec![input, pattern, group];

        // when
        let result = regexp_extract_func(args).expect("valid result");

        // then
        assert_eq!(result.len(), 1);
        assert_eq!(*result.data_type(), DataType::LargeUtf8);
        let string_array = result
            .as_any()
            .downcast_ref::<GenericStringArray<i64>>()
            .expect("Expected an GenericStringArray");
        assert_eq!(string_array.value(0), "he");
    }

    #[test]
    fn test_input_utf8_view() {
        // given
        let input = ColumnarValue::from(ScalarValue::Utf8View(Some("hello".to_string())));
        let pattern = ColumnarValue::from(ScalarValue::from_str("[a-h]+").unwrap());
        let group = ColumnarValue::from(ScalarValue::from(0u32));
        let args = vec![input, pattern, group];

        // when
        let result = regexp_extract_func(args).expect("valid result");

        // then
        assert_eq!(result.len(), 1);
        assert_eq!(*result.data_type(), DataType::Utf8View);
        let string_array = result
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("Expected an StringViewArray");
        assert_eq!(string_array.value(0), "he");
    }

    #[test]
    fn test_args_extraction() {
        // given
        let scalar_str = ColumnarValue::from(ScalarValue::from_str("hello").unwrap());
        let invalid_pattern_str =
            ColumnarValue::from(ScalarValue::from_str("foo(bar").unwrap());
        let array_col =
            ColumnarValue::from(
                Arc::new(StringArray::from(vec!["hello", "world"])) as ArrayRef
            );
        let scalar_uint = ColumnarValue::from(ScalarValue::from(0u32));
        let scalar_int = ColumnarValue::from(ScalarValue::from(0i32));

        // when
        let valid = extract_args(vec![
            scalar_str.clone(),
            scalar_str.clone(),
            scalar_uint.clone(),
        ]);
        let not_scalar_pattern = extract_args(vec![
            scalar_str.clone(),
            array_col.clone(),
            scalar_uint.clone(),
        ]);
        let not_scalar_group =
            extract_args(vec![scalar_str.clone(), scalar_str.clone(), array_col]);
        let not_uint_group =
            extract_args(vec![scalar_str.clone(), scalar_str.clone(), scalar_int]);
        let invalid_pattern =
            extract_args(vec![scalar_str, invalid_pattern_str, scalar_uint]);
        // then
        assert!(valid.is_ok());
        assert!(matches!(
            not_scalar_pattern,
            Err(DataFusionError::Execution(_))
        ));
        assert!(matches!(
            not_scalar_group,
            Err(DataFusionError::Execution(_))
        ));
        assert!(matches!(not_uint_group, Err(DataFusionError::Execution(_))));
        assert!(matches!(invalid_pattern, Err(DataFusionError::External(_))));
    }
}
