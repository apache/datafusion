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

use arrow::array::{Array, ArrayRef, AsArray, ListArray, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion_common::cast::as_list_array;
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_functions_nested::arrays_zip::ArraysZip;
use std::sync::Arc;

use super::arrays_zip_rewrite::{ARRAYS_ZIP_NAMES_KEY, dedup_names};

/// Spark-compatible `arrays_zip` — struct field names follow the original
/// SQL arguments rather than positional ordinals.
///
/// During SQL planning, names come from `arg_fields`. After
/// [`SparkArraysZipRewrite`] runs, they ride in a `List<Utf8>` literal in
/// `arg_fields[0]` (tagged with `datafusion.spark.arrays_zip.names`
/// metadata) so optimizer renames can't drop them. Both paths dedup so the
/// schemas agree across phases.
///
/// [`SparkArraysZipRewrite`]: super::arrays_zip_rewrite::SparkArraysZipRewrite
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArraysZip {
    signature: Signature,
}

impl Default for SparkArraysZip {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArraysZip {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkArraysZip {
    fn name(&self) -> &str {
        "arrays_zip"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let (names, array_arg_types): (Vec<String>, Vec<DataType>) =
            if has_names_literal(args.arg_fields, args.scalar_arguments) {
                let names =
                    names_from_scalar(args.scalar_arguments[0].as_ref().unwrap())?;
                let types = args
                    .arg_fields
                    .iter()
                    .skip(1)
                    .map(|f| f.data_type().clone())
                    .collect();
                (names, types)
            } else {
                let raw_names = args
                    .arg_fields
                    .iter()
                    .enumerate()
                    .map(|(i, f)| natural_name_or_ordinal(f.name(), i))
                    .collect();
                let names = dedup_names(raw_names);
                let types = args
                    .arg_fields
                    .iter()
                    .map(|f| f.data_type().clone())
                    .collect();
                (names, types)
            };
        let inner_dt = ArraysZip::new().return_type(&array_arg_types)?;
        let new_dt = rename_return_type(&inner_dt, &names)?;
        Ok(Arc::new(Field::new(self.name(), new_dt, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args: all_args,
            arg_fields,
            number_rows,
            return_field,
            config_options,
        } = args;

        if all_args.is_empty() {
            return exec_err!("arrays_zip expects at least one array argument");
        }

        let has_marker = arg_fields
            .first()
            .is_some_and(|f| f.metadata().contains_key(ARRAYS_ZIP_NAMES_KEY));
        let (names, inner_args_slice, inner_fields_slice): (
            Vec<String>,
            &[ColumnarValue],
            &[FieldRef],
        ) = if has_marker {
            let names = match &all_args[0] {
                ColumnarValue::Scalar(sv) => names_from_scalar(sv)?,
                ColumnarValue::Array(arr) => names_from_array(arr.as_ref())?,
            };
            (names, &all_args[1..], &arg_fields[1..])
        } else {
            let raw_names = arg_fields
                .iter()
                .enumerate()
                .map(|(i, f)| natural_name_or_ordinal(f.name(), i))
                .collect();
            let names = dedup_names(raw_names);
            (names, all_args.as_slice(), arg_fields.as_slice())
        };

        let inner_args = ScalarFunctionArgs {
            args: inner_args_slice.to_vec(),
            arg_fields: inner_fields_slice.to_vec(),
            number_rows,
            return_field: Arc::clone(&return_field),
            config_options,
        };
        let result = ArraysZip::new().invoke_with_args(inner_args)?;

        match result {
            ColumnarValue::Array(arr) => {
                let renamed = rename_list_struct_fields(&arr, &names)?;
                Ok(ColumnarValue::Array(renamed))
            }
            ColumnarValue::Scalar(scalar) => {
                let arr = scalar.to_array_of_size(number_rows.max(1))?;
                let renamed = rename_list_struct_fields(&arr, &names)?;
                let new_scalar = ScalarValue::try_from_array(&renamed, 0)?;
                Ok(ColumnarValue::Scalar(new_scalar))
            }
        }
    }
}

/// True iff `arg_fields[0]` is a constant carrying our marker metadata.
/// The metadata check (not just the value's type) prevents collision with a
/// user-supplied `List<Utf8>` first argument.
fn has_names_literal(
    arg_fields: &[FieldRef],
    scalar_arguments: &[Option<&ScalarValue>],
) -> bool {
    let Some(Some(_)) = scalar_arguments.first() else {
        return false;
    };
    arg_fields
        .first()
        .is_some_and(|f| f.metadata().contains_key(ARRAYS_ZIP_NAMES_KEY))
}

/// Fallback field-name heuristic for paths the analyzer rewrite hasn't
/// touched. Returns the trailing dotted segment of `field_name` if it looks
/// like an unquoted identifier, otherwise the 0-based ordinal.
///
/// [`SparkArraysZipRewrite`]: super::arrays_zip_rewrite::SparkArraysZipRewrite
fn natural_name_or_ordinal(field_name: &str, ordinal: usize) -> String {
    let candidate = field_name.rsplit('.').next().unwrap_or(field_name);
    if is_unquoted_identifier(candidate) {
        candidate.to_string()
    } else {
        ordinal.to_string()
    }
}

fn is_unquoted_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_alphabetic() || c == '_' => {
            chars.all(|c| c.is_alphanumeric() || c == '_')
        }
        _ => false,
    }
}

fn names_from_scalar(sv: &ScalarValue) -> Result<Vec<String>> {
    let ScalarValue::List(list) = sv else {
        return exec_err!(
            "arrays_zip names argument must be a List<Utf8>, got {}",
            sv.data_type()
        );
    };
    names_from_array(list.as_ref())
}

fn names_from_array(arr: &dyn Array) -> Result<Vec<String>> {
    let list = as_list_array(arr)?;
    if list.len() != 1 {
        return exec_err!(
            "arrays_zip names argument must contain a single row, got {}",
            list.len()
        );
    }
    let values = list.value(0);
    let Some(strs) = values.as_any().downcast_ref::<StringArray>() else {
        return exec_err!(
            "arrays_zip names argument must be List<Utf8>, got element type {}",
            values.data_type()
        );
    };
    (0..strs.len())
        .map(|i| {
            if strs.is_null(i) {
                exec_err!("arrays_zip names argument must not contain NULL")
            } else {
                Ok(strs.value(i).to_string())
            }
        })
        .collect()
}

/// Rename struct fields inside a `List<Struct<..>>` data type using the given names.
fn rename_return_type(data_type: &DataType, names: &[String]) -> Result<DataType> {
    let DataType::List(list_field) = data_type else {
        return exec_err!("arrays_zip expected List return type, got {data_type}");
    };
    let DataType::Struct(fields) = list_field.data_type() else {
        return exec_err!(
            "arrays_zip expected List<Struct<..>> return type, got {data_type}"
        );
    };
    let new_struct = DataType::Struct(rename_fields(fields, names));
    Ok(DataType::List(Arc::new(Field::new(
        list_field.name(),
        new_struct,
        list_field.is_nullable(),
    ))))
}

fn rename_list_struct_fields(array: &dyn Array, names: &[String]) -> Result<ArrayRef> {
    let list = as_list_array(array)?;
    let struct_array = list.values().as_struct();
    let new_fields = rename_fields(struct_array.fields(), names);

    let new_struct = StructArray::try_new(
        new_fields,
        struct_array.columns().to_vec(),
        struct_array.nulls().cloned(),
    )?;

    let new_list_field =
        Arc::new(Field::new_list_field(new_struct.data_type().clone(), true));
    let new_list = ListArray::try_new(
        new_list_field,
        list.offsets().clone(),
        Arc::new(new_struct),
        list.nulls().cloned(),
    )?;
    Ok(Arc::new(new_list))
}

fn rename_fields(fields: &Fields, names: &[String]) -> Fields {
    fields
        .iter()
        .zip(names.iter())
        .map(|(f, n)| {
            Arc::new(Field::new(
                n.clone(),
                f.data_type().clone(),
                f.is_nullable(),
            ))
        })
        .collect::<Vec<_>>()
        .into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::buffer::OffsetBuffer;

    #[test]
    fn naming_strips_dotted_qualifier() {
        assert_eq!(natural_name_or_ordinal("t.a", 0), "a");
        assert_eq!(natural_name_or_ordinal("db.schema.col", 0), "col");
        assert_eq!(natural_name_or_ordinal("a.b.c", 7), "c");
    }

    #[test]
    fn naming_keeps_plain_identifier() {
        assert_eq!(natural_name_or_ordinal("a", 9), "a");
        assert_eq!(natural_name_or_ordinal("_foo", 0), "_foo");
        assert_eq!(natural_name_or_ordinal("Bar123", 1), "Bar123");
    }

    #[test]
    fn naming_falls_back_to_ordinal_for_non_identifier() {
        assert_eq!(natural_name_or_ordinal("a + 1", 0), "0");
        assert_eq!(natural_name_or_ordinal("foo(x)", 3), "3");
        assert_eq!(natural_name_or_ordinal("", 5), "5");
        assert_eq!(natural_name_or_ordinal("123abc", 1), "1");
        assert_eq!(natural_name_or_ordinal("t.", 4), "4");
        assert_eq!(natural_name_or_ordinal("t.123", 2), "2");
    }

    #[test]
    fn names_from_array_extracts_single_row_of_utf8() {
        let strs = StringArray::from(vec![Some("x"), Some("y")]);
        let offsets = OffsetBuffer::from_lengths([2]);
        let field = Arc::new(Field::new_list_field(DataType::Utf8, true));
        let list = ListArray::new(field, offsets, Arc::new(strs), None);

        let names = names_from_array(&list).expect("valid input must succeed");
        assert_eq!(names, vec!["x", "y"]);
    }
}
