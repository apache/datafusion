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

use arrow::array::{
    Array, ArrayRef, AsArray, PrimitiveArray, StringArray, StringBuilder,
};
use arrow::compute::{can_cast_types, cast};
use arrow::datatypes::DataType::{Int64, Utf8};
use arrow::datatypes::{DataType, Int64Type};
use datafusion_common::cast::as_string_array;
use datafusion_common::{DataFusionError, Result, exec_err, plan_datafusion_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkElt {
    signature: Signature,
}

impl Default for SparkElt {
    fn default() -> Self {
        SparkElt::new()
    }
}

impl SparkElt {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkElt {
    fn name(&self) -> &str {
        "elt"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let enable_ansi_mode = args.config_options.execution.enable_ansi_mode;
        make_scalar_function(
            move |arrays: &[ArrayRef]| elt(arrays, enable_ansi_mode),
            vec![],
        )(&args.args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let length = arg_types.len();
        if length < 2 {
            return Err(plan_datafusion_err!(
                "ELT function expects at least 2 arguments: index, value1"
            ));
        }

        let idx_dt: &DataType = &arg_types[0];
        if *idx_dt != Int64 && !can_cast_types(idx_dt, &Int64) {
            return Err(DataFusionError::Plan(format!(
                "ELT index must be Int64 (or castable to Int64), got {idx_dt:?}"
            )));
        }
        let mut coerced = vec![Utf8; length];
        coerced[0] = Int64;
        Ok(coerced)
    }
}

fn elt(args: &[ArrayRef], enable_ansi_mode: bool) -> Result<ArrayRef> {
    let n_rows = args[0].len();

    let idx: &PrimitiveArray<Int64Type> =
        args[0].as_primitive_opt::<Int64Type>().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "ELT function: first argument must be Int64 (got {:?})",
                args[0].data_type()
            ))
        })?;

    let num_values = args.len() - 1;
    let mut cols: Vec<StringArray> = Vec::with_capacity(num_values);
    for a in args.iter().skip(1) {
        let casted = cast(a, &Utf8)?;
        cols.push(as_string_array(&casted)?.clone());
    }

    let mut builder = StringBuilder::new();

    for i in 0..n_rows {
        if idx.is_null(i) {
            builder.append_null();
            continue;
        }

        let index = idx.value(i);

        if index < 1 || (index as usize) > num_values {
            if enable_ansi_mode {
                return exec_err!(
                    "The index {index} is out of bounds. The array has {num_values} elements."
                );
            }
            builder.append_null();
            continue;
        }

        let value_idx = (index as usize) - 1;
        let col = &cols[value_idx];

        if col.is_null(i) {
            builder.append_null();
        } else {
            builder.append_value(col.value(i));
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;

    fn run_elt_arrays(arrs: Vec<ArrayRef>) -> Result<StringArray> {
        run_elt_arrays_with(arrs, false)
    }

    fn run_elt_arrays_ansi(arrs: Vec<ArrayRef>) -> Result<StringArray> {
        run_elt_arrays_with(arrs, true)
    }

    fn run_elt_arrays_with(arrs: Vec<ArrayRef>, ansi: bool) -> Result<StringArray> {
        let arr = elt(&arrs, ansi)?;
        Ok(as_string_array(&arr)?.clone())
    }

    #[test]
    fn elt_utf8_basic() -> Result<()> {
        let idx = Arc::new(Int64Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(0),
            None,
        ]));
        let v1 = Arc::new(StringArray::from(vec![
            Some("a1"),
            Some("a2"),
            Some("a3"),
            Some("a4"),
            Some("a5"),
            Some("a6"),
        ]));
        let v2 = Arc::new(StringArray::from(vec![
            Some("b1"),
            Some("b2"),
            None,
            Some("b4"),
            Some("b5"),
            Some("b6"),
        ]));
        let v3 = Arc::new(StringArray::from(vec![
            Some("c1"),
            Some("c2"),
            Some("c3"),
            None,
            Some("c5"),
            Some("c6"),
        ]));

        let out = run_elt_arrays(vec![idx, v1, v2, v3])?;
        assert_eq!(out.len(), 6);
        assert_eq!(out.value(0), "a1");
        assert_eq!(out.value(1), "b2");
        assert_eq!(out.value(2), "c3");
        assert!(out.is_null(3));
        assert!(out.is_null(4));
        assert!(out.is_null(5));
        Ok(())
    }

    #[test]
    fn elt_int64_basic() -> Result<()> {
        let idx = Arc::new(Int64Array::from(vec![Some(2), Some(1), Some(2)]));
        let v1 = Arc::new(Int64Array::from(vec![Some(10), Some(20), Some(30)]));
        let v2 = Arc::new(Int64Array::from(vec![Some(100), None, Some(300)]));

        let out = run_elt_arrays(vec![idx, v1, v2])?;
        assert_eq!(out.len(), 3);
        assert_eq!(out.value(0), "100");
        assert_eq!(out.value(1), "20");
        assert_eq!(out.value(2), "300");
        Ok(())
    }

    #[test]
    fn elt_out_of_range_all_null() -> Result<()> {
        let idx = Arc::new(Int64Array::from(vec![Some(5), Some(-1), Some(0)]));
        let v1 = Arc::new(StringArray::from(vec![Some("x"), Some("y"), Some("z")]));
        let v2 = Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")]));

        let out = run_elt_arrays(vec![idx, v1, v2])?;
        assert!(out.is_null(0));
        assert!(out.is_null(1));
        assert!(out.is_null(2));
        Ok(())
    }

    #[test]
    fn elt_utf8_returns_utf8() -> Result<()> {
        let idx = Arc::new(Int64Array::from(vec![Some(1)]));
        let v1 = Arc::new(StringArray::from(vec![Some("scala")]));
        let v2 = Arc::new(StringArray::from(vec![Some("java")]));

        let out = run_elt_arrays(vec![idx, v1, v2])?;
        assert_eq!(out.data_type(), &Utf8);
        Ok(())
    }

    #[test]
    fn elt_ansi_valid_indices_ok() -> Result<()> {
        let idx = Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)]));
        let v1 = Arc::new(StringArray::from(vec![Some("a1"), Some("a2"), Some("a3")]));
        let v2 = Arc::new(StringArray::from(vec![Some("b1"), Some("b2"), Some("b3")]));
        let v3 = Arc::new(StringArray::from(vec![Some("c1"), Some("c2"), Some("c3")]));

        let out = run_elt_arrays_ansi(vec![idx, v1, v2, v3])?;
        assert_eq!(out.value(0), "a1");
        assert_eq!(out.value(1), "b2");
        assert_eq!(out.value(2), "c3");
        Ok(())
    }

    #[test]
    fn elt_ansi_null_index_returns_null() -> Result<()> {
        // NULL index does not raise an error even in ANSI mode; returns NULL.
        let idx = Arc::new(Int64Array::from(vec![Some(1), None]));
        let v1 = Arc::new(StringArray::from(vec![Some("a1"), Some("a2")]));
        let v2 = Arc::new(StringArray::from(vec![Some("b1"), Some("b2")]));

        let out = run_elt_arrays_ansi(vec![idx, v1, v2])?;
        assert_eq!(out.value(0), "a1");
        assert!(out.is_null(1));
        Ok(())
    }

    #[test]
    fn elt_ansi_index_too_large_errors() {
        let idx = Arc::new(Int64Array::from(vec![Some(3)]));
        let v1 = Arc::new(StringArray::from(vec![Some("a1")]));
        let v2 = Arc::new(StringArray::from(vec![Some("b1")]));

        let err = run_elt_arrays_ansi(vec![idx, v1, v2]).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("The index 3 is out of bounds. The array has 2 elements."),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn elt_ansi_index_zero_errors() {
        let idx = Arc::new(Int64Array::from(vec![Some(0)]));
        let v1 = Arc::new(StringArray::from(vec![Some("a1")]));
        let v2 = Arc::new(StringArray::from(vec![Some("b1")]));

        let err = run_elt_arrays_ansi(vec![idx, v1, v2]).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("The index 0 is out of bounds. The array has 2 elements."),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn elt_ansi_index_negative_errors() {
        let idx = Arc::new(Int64Array::from(vec![Some(-1)]));
        let v1 = Arc::new(StringArray::from(vec![Some("a1")]));
        let v2 = Arc::new(StringArray::from(vec![Some("b1")]));

        let err = run_elt_arrays_ansi(vec![idx, v1, v2]).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("The index -1 is out of bounds. The array has 2 elements."),
            "unexpected error: {msg}"
        );
    }
}
