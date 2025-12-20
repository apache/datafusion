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

use std::any::Any;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, PrimitiveArray, StringArray, StringBuilder,
};
use arrow::compute::{can_cast_types, cast};
use arrow::datatypes::DataType::{Int64, Utf8};
use arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use datafusion_common::cast::as_string_array;
use datafusion_common::{DataFusionError, Result, internal_err, plan_datafusion_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
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
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "elt"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        Ok(Arc::new(Field::new(self.name(), Utf8, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(elt, vec![])(&args.args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let length = arg_types.len();
        if length < 2 {
            plan_datafusion_err!(
                "ELT function expects at least 2 arguments: index, value1"
            );
        }

        let idx_dt: &DataType = &arg_types[0];
        if *idx_dt != Int64 && !can_cast_types(idx_dt, &Int64) {
            return Err(DataFusionError::Plan(format!(
                "ELT index must be Int64 (or castable to Int64), got {idx_dt:?}"
            )));
        }
        let mut coerced = Vec::with_capacity(arg_types.len());
        coerced.push(Int64);

        for _ in 1..length {
            coerced.push(Utf8);
        }

        Ok(coerced)
    }
}

fn elt(args: &[ArrayRef]) -> Result<ArrayRef, DataFusionError> {
    let n_rows = args[0].len();

    let idx: &PrimitiveArray<Int64Type> =
        args[0].as_primitive_opt::<Int64Type>().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "ELT function: first argument must be Int64 (got {:?})",
                args[0].data_type()
            ))
        })?;

    let num_values = args.len() - 1;
    let mut cols: Vec<Arc<StringArray>> = Vec::with_capacity(num_values);
    for a in args.iter().skip(1) {
        let casted = cast(a, &Utf8)?;
        let sa = as_string_array(&casted)?;
        cols.push(Arc::new(sa.clone()));
    }

    let mut builder = StringBuilder::new();

    for i in 0..n_rows {
        if idx.is_null(i) {
            builder.append_null();
            continue;
        }

        let index = idx.value(i);

        // TODO: if spark.sql.ansi.enabled is true,
        //  throw ArrayIndexOutOfBoundsException for invalid indices;
        //  if false, return NULL instead (current behavior).
        if index < 1 || (index as usize) > num_values {
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
    use datafusion_common::Result;

    use arrow::array::{ArrayRef, StringArray};
    use datafusion_common::DataFusionError;
    use std::sync::Arc;

    fn run_elt_arrays(arrs: Vec<ArrayRef>) -> Result<Arc<StringArray>> {
        let arr = elt(&arrs)?;
        let string_array = arr
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Internal("expected Utf8".into()))?;
        Ok(Arc::new(string_array.clone()))
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
    fn test_elt_nullability() -> Result<()> {
        use datafusion_expr::ReturnFieldArgs;

        let elt_func = SparkElt::new();

        // Test with all non-nullable args - result should be non-nullable
        let non_nullable_idx: FieldRef = Arc::new(Field::new("idx", Int64, false));
        let non_nullable_v1: FieldRef = Arc::new(Field::new("v1", Utf8, false));
        let non_nullable_v2: FieldRef = Arc::new(Field::new("v2", Utf8, false));

        let result = elt_func.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[
                Arc::clone(&non_nullable_idx),
                Arc::clone(&non_nullable_v1),
                Arc::clone(&non_nullable_v2),
            ],
            scalar_arguments: &[None, None, None],
        })?;
        assert!(
            !result.is_nullable(),
            "elt should NOT be nullable when all args are non-nullable"
        );

        // Test with nullable index - result should be nullable
        let nullable_idx: FieldRef = Arc::new(Field::new("idx", Int64, true));
        let result = elt_func.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[
                nullable_idx,
                Arc::clone(&non_nullable_v1),
                Arc::clone(&non_nullable_v2),
            ],
            scalar_arguments: &[None, None, None],
        })?;
        assert!(
            result.is_nullable(),
            "elt should be nullable when index is nullable"
        );

        // Test with nullable value - result should be nullable
        let nullable_v1: FieldRef = Arc::new(Field::new("v1", Utf8, true));
        let result = elt_func.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[non_nullable_idx, nullable_v1, non_nullable_v2],
            scalar_arguments: &[None, None, None],
        })?;
        assert!(
            result.is_nullable(),
            "elt should be nullable when any value is nullable"
        );

        Ok(())
    }
}
