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

use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, StringArray, StringBuilder};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{Int32, Int64, Utf8};
use datafusion_common::{exec_err, DataFusionError, Result};
use datafusion_expr::Volatility::Immutable;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature};
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
            signature: Signature::variadic_any(Immutable),
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() < 2 {
            return exec_err!("elt expects at least 2 arguments: index, value1");
        }
        Ok(Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(elt, vec![])(&args.args)
    }
}

fn elt(args: &[ArrayRef]) -> Result<ArrayRef, DataFusionError> {
    if args.len() < 2 {
        return exec_err!("elt expects at least 2 arguments: index, value1");
    }

    let num_rows = args[0].len();
    let k = args.len() - 1;

    let mut vals: Vec<Arc<StringArray>> = Vec::with_capacity(k);
    for (j, a) in args.iter().enumerate().skip(1) {
        if a.len() != num_rows {
            return exec_err!(
                "elt: all arguments must have the same length (arg {} has {}, expected {})",
                j,
                a.len(),
                num_rows
            );
        }
        let casted = cast(a, &Utf8)?;
        let sa = casted
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Internal("downcast Utf8 failed".into()))?
            .clone();
        vals.push(Arc::new(sa));
    }

    let mut builder = StringBuilder::new();
    for row in 0..num_rows {
        let n_opt: Option<i64> =
            match args[0].data_type() {
                Int32 => {
                    let arr = args[0].as_any().downcast_ref::<Int32Array>().ok_or_else(
                        || DataFusionError::Internal("downcast Int32 failed".into()),
                    )?;
                    if arr.is_null(row) {
                        None
                    } else {
                        Some(arr.value(row) as i64)
                    }
                }
                Int64 => {
                    let arr = args[0].as_any().downcast_ref::<Int64Array>().ok_or_else(
                        || DataFusionError::Internal("downcast Int64 failed".into()),
                    )?;
                    if arr.is_null(row) {
                        None
                    } else {
                        Some(arr.value(row))
                    }
                }
                other => {
                    return exec_err!(
                        "elt: first argument must be Int32 or Int64 (got {:?})",
                        other
                    )
                }
            };

        let Some(n) = n_opt else {
            builder.append_null();
            continue;
        };
        // If spark.sql.ansi.enabled is set to true, it throws ArrayIndexOutOfBoundsException for invalid indices.
        let ansi_enable: bool = false; // I need get value -> spark.sql.ansi.enabled
        if n < 1 || (n as usize) > k {
            if !ansi_enable {
                builder.append_null();
                continue;
            } else {
                return exec_err!("ArrayIndexOutOfBoundsException");
            }
        }

        let j = (n as usize) - 1;
        let col = &vals[j];

        if col.is_null(row) {
            builder.append_null();
        } else {
            builder.append_value(col.value(row));
        }
    }

    Ok(cast(&(Arc::new(builder.finish()) as ArrayRef), &Utf8)?)
}

#[cfg(test)]
mod tests {
    use datafusion_common::Result;

    use super::*;

    fn run_elt_arrays(arrs: Vec<ArrayRef>) -> Result<ArrayRef> {
        elt(&arrs)
    }

    #[test]
    fn elt_utf8_basic() -> Result<()> {
        let idx = Arc::new(Int32Array::from(vec![
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
        let out = out
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Internal("expected Utf8".into()))?;
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
        let idx = Arc::new(Int32Array::from(vec![Some(2), Some(1), Some(2)]));
        let v1 = Arc::new(Int64Array::from(vec![Some(10), Some(20), Some(30)]));
        let v2 = Arc::new(Int64Array::from(vec![Some(100), None, Some(300)]));

        let out = run_elt_arrays(vec![idx, v1, v2])?;
        let out = out
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Internal("expected Utf8".into()))?;
        assert_eq!(out.len(), 3);
        assert_eq!(out.value(0), "100");
        assert_eq!(out.value(1), "20");
        assert_eq!(out.value(2), "300");
        Ok(())
    }

    #[test]
    fn elt_out_of_range_all_null() -> Result<()> {
        let idx = Arc::new(Int32Array::from(vec![Some(5), Some(-1), Some(0)]));
        let v1 = Arc::new(StringArray::from(vec![Some("x"), Some("y"), Some("z")]));
        let v2 = Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")]));

        let out = run_elt_arrays(vec![idx, v1, v2])?;
        let out = out
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Internal("expected Utf8".into()))?;
        assert!(out.is_null(0));
        assert!(out.is_null(1));
        assert!(out.is_null(2));
        Ok(())
    }

    #[test]
    fn elt_len_mismatch_error() -> Result<()> {
        let idx = Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(1)]));
        let v1 = Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")]));
        let v2 = Arc::new(StringArray::from(vec![Some("x"), Some("y")]));

        let res = run_elt_arrays(vec![idx, v1, v2]);
        let msg = match res {
            Ok(_) => {
                return Err(DataFusionError::Internal(
                    "expected error due to length mismatch".into(),
                ));
            }
            Err(e) => e.to_string(),
        };

        assert!(msg.contains("all arguments must have the same length"));
        Ok(())
    }

    #[test]
    fn elt_utf8_returns_utf8view() -> Result<()> {
        let idx = Arc::new(Int32Array::from(vec![Some(1)]));
        let v1 = Arc::new(StringArray::from(vec![Some("scala")]));
        let v2 = Arc::new(StringArray::from(vec![Some("java")]));

        let out = run_elt_arrays(vec![idx, v1, v2])?;
        assert_eq!(out.data_type(), &Utf8);
        Ok(())
    }
}
