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

use arrow::array::{Array, ArrayRef, AsArray, ListArray, StructArray};
use arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::cast::as_list_array;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions_nested::arrays_zip::ArraysZip;
use std::sync::Arc;

/// Spark-compatible `arrays_zip` function.
///
/// Delegates to DataFusion's `arrays_zip` and renames the inner struct fields
/// to use 0-based ordinals (`0`, `1`, `2`, ...) instead of DataFusion's 1-based
/// ordinals, matching Spark's [`arrays_zip`] semantics.
///
/// [`arrays_zip`]: https://spark.apache.org/docs/latest/api/sql/index.html#arrays_zip
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let inner = ArraysZip::new().return_type(arg_types)?;
        rename_return_type_zero_based(&inner)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let number_rows = args.number_rows;
        let result = ArraysZip::new().invoke_with_args(args)?;
        match result {
            ColumnarValue::Array(arr) => {
                let renamed = rename_list_struct_fields_zero_based(&arr)?;
                Ok(ColumnarValue::Array(renamed))
            }
            ColumnarValue::Scalar(scalar) => {
                let arr = scalar.to_array_of_size(number_rows.max(1))?;
                let renamed = rename_list_struct_fields_zero_based(&arr)?;
                let new_scalar = ScalarValue::try_from_array(&renamed, 0)?;
                Ok(ColumnarValue::Scalar(new_scalar))
            }
        }
    }
}

/// Rename struct fields inside a `List<Struct<..>>` data type to use 0-based ordinals.
fn rename_return_type_zero_based(data_type: &DataType) -> Result<DataType> {
    let DataType::List(list_field) = data_type else {
        return exec_err!("arrays_zip expected List return type, got {data_type}");
    };
    let DataType::Struct(fields) = list_field.data_type() else {
        return exec_err!(
            "arrays_zip expected List<Struct<..>> return type, got {data_type}"
        );
    };

    let new_struct = DataType::Struct(zero_based_fields(fields));
    Ok(DataType::List(Arc::new(Field::new(
        list_field.name(),
        new_struct,
        list_field.is_nullable(),
    ))))
}

/// Rebuild a `List<Struct<..>>` array so that the inner struct fields use 0-based
/// ordinal names. The underlying column data and null buffers are reused; only
/// the schema is replaced.
fn rename_list_struct_fields_zero_based(array: &dyn Array) -> Result<ArrayRef> {
    let list = as_list_array(array)?;
    let struct_array = list.values().as_struct();
    let new_fields = zero_based_fields(struct_array.fields());

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

fn zero_based_fields(fields: &Fields) -> Fields {
    fields
        .iter()
        .enumerate()
        .map(|(i, f)| {
            Arc::new(Field::new(
                i.to_string(),
                f.data_type().clone(),
                f.is_nullable(),
            ))
        })
        .collect::<Vec<_>>()
        .into()
}
