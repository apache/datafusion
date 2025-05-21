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

use arrow::array::{Array, ArrayRef, AsArray, Int32Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Int32Type};
use datafusion_common::{exec_err, Result};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;
use std::sync::Arc;

use crate::utils::make_scalar_function;

macro_rules! grouping_id {
    ($grouping_id:expr, $indices:expr, $type:ty, $array_type:ty) => {{
        let grouping_id = match $grouping_id.as_any().downcast_ref::<$array_type>() {
            Some(array) => array,
            None => return exec_err!("grouping function requires {} grouping_id array", stringify!($type)),
        };
        grouping_id
            .iter()
            .zip($indices.iter())
            .map(|(grouping_id, indices)| {
                grouping_id.map(|grouping_id| {
                    let mut result = 0 as $type;
                    match indices {
                        Some(indices) => {
                            for index in indices.as_primitive::<Int32Type>().iter() {
                                if let Some(index) = index {
                                    let bit = (grouping_id >> index) & 1;
                                    result = (result << 1) | bit;
                                }
                            }
                        }
                        None => {
                            result = grouping_id;
                        }
                    }
                    result as i32
                })
            })
            .collect()
    }};
}

#[user_doc(
    doc_section(label = "Other Functions"),
    description = "Developer API: Returns the level of grouping, equals to (((grouping_id >> array[0]) & 1) << (n-1)) + (((grouping_id >> array[1]) & 1) << (n-2)) + ... + (((grouping_id >> array[n-1]) & 1) << 0). Returns grouping_id if indices is not provided.",
    syntax_example = "grouping(grouping_id[, indices])",
    sql_example = r#"```sql
> SELECT grouping(__grouping_id, make_array(0)) FROM table GROUP BY GROUPING SETS ((a), (b));
+----------------+
| grouping       |
+----------------+
| 1              |
| 0              |
+----------------+
```"#,
    argument(
        name = "grouping_id",
        description = "The internal grouping ID column (UInt8/16/32/64)"
    ),
    argument(
        name = "indices",
        description = "The indices of the column in the grouping set (Int32)"
    )
)]
#[derive(Debug)]
pub struct GroupingFunc {
    signature: Signature,
}

impl Default for GroupingFunc {
    fn default() -> Self {
        GroupingFunc::new()
    }
}

impl GroupingFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for GroupingFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "grouping"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(grouping_inner, vec![])(&args.args)
    }

    fn short_circuits(&self) -> bool {
        false
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 && arg_types.len() != 1 {
            return exec_err!(
                "grouping function requires 1 or 2 arguments, got {}",
                arg_types.len()
            );
        }

        if !arg_types[0].is_unsigned_integer() {
            return exec_err!(
                "grouping function requires unsigned integer for first argument, got {}",
                arg_types[0]
            )
        }

        if arg_types.len() == 1 {
            return Ok(vec![arg_types[0].clone()]);
        }

        let DataType::List(field) = &arg_types[1] else {
            return exec_err!(
                "grouping function requires list for second argument, got {}",
                arg_types[1]
            );
        };

        if !field.data_type().is_integer() {
            return exec_err!(
                "grouping function requires list of integers for second argument, got {}",
                arg_types[1]
            );
        }

        Ok(vec![arg_types[0].clone(), DataType::List(Arc::new(Field::new_list_field(DataType::Int32, false))),])
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn grouping_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 && args.len() != 1 {
        return exec_err!(
            "grouping function requires 1 or 2 arguments, got {}",
            args.len()
        );
    }

    if args.len() == 1 {
        return cast(&args[0], &DataType::Int32).map_err(|e| e.into());
    }

    let grouping_id = &args[0];
    let indices = &args[1];
    let indices = indices.as_list::<i32>();

    let result: Int32Array = match grouping_id.data_type() {
        DataType::UInt8 => grouping_id!(grouping_id, indices, u8, UInt8Array),
        DataType::UInt16 => grouping_id!(grouping_id, indices, u16, UInt16Array),
        DataType::UInt32 => grouping_id!(grouping_id, indices, u32, UInt32Array),
        DataType::UInt64 => grouping_id!(grouping_id, indices, u64, UInt64Array),
        _ => {
            return exec_err!(
                "grouping function requires UInt8/16/32/64 for grouping_id, got {}",
                grouping_id.data_type()
            )
        }
    };

    Ok(Arc::new(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{array::{Int32Array, ListArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array}, datatypes::Int32Type};
    use datafusion_common::{Result, ScalarValue};

    #[test]
    fn test_grouping_uint8() -> Result<()> {
        let grouping_id = UInt8Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
        let indices = vec![Some(vec![Some(0), Some(1)])];
        
        let args = vec![
            ColumnarValue::Array(Arc::new(grouping_id)),
            ColumnarValue::Scalar(ScalarValue::List(Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(indices.clone())))),
        ];

        let arg_fields_owned = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| Field::new(format!("arg_{idx}"), arg.data_type(), true))
            .collect::<Vec<_>>();

        let func = GroupingFunc::new();
        let result = func.invoke_with_args(ScalarFunctionArgs { 
            args,
            number_rows: 4,
            arg_fields: arg_fields_owned.iter().collect::<Vec<_>>(),
            return_field: &Field::new("f", DataType::Int32, true),
        })?;
        
        let result = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected array result"),
        };

        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(result.values().to_vec(), vec![2, 1, 3, 0]);
        Ok(())
    }

    #[test]
    fn test_grouping_uint16() -> Result<()> {
        let grouping_id = UInt16Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
        let indices = vec![Some(vec![Some(0), Some(1)])];
    
        let args = vec![
            ColumnarValue::Array(Arc::new(grouping_id)),
            ColumnarValue::Scalar(ScalarValue::List(Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(indices)))),
        ];

        let arg_fields_owned = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| Field::new(format!("arg_{idx}"), arg.data_type(), true))
            .collect::<Vec<_>>();

        let func = GroupingFunc::new();
        let result = func.invoke_with_args(ScalarFunctionArgs { 
            args,
            number_rows: 4,
            arg_fields: arg_fields_owned.iter().collect::<Vec<_>>(),
            return_field: &Field::new("f", DataType::Int32, true),
        })?;
        
        let result = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected array result"),
        };

        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(result.values().to_vec(), vec![2, 1, 3, 0]);
        Ok(())
    }

    #[test]
    fn test_grouping_uint32() -> Result<()> {
        let grouping_id = UInt32Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
        let indices = vec![Some(vec![Some(0), Some(1)])];
        
        let args = vec![
            ColumnarValue::Array(Arc::new(grouping_id)),
            ColumnarValue::Scalar(ScalarValue::List(Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(indices)))),
        ];

        let arg_fields_owned = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| Field::new(format!("arg_{idx}"), arg.data_type(), true))
            .collect::<Vec<_>>();

        let func = GroupingFunc::new();
        let result = func.invoke_with_args(ScalarFunctionArgs { 
            args,
            number_rows: 4,
            arg_fields: arg_fields_owned.iter().collect::<Vec<_>>(),
            return_field: &Field::new("f", DataType::Int32, true),
        })?;
        
        let result = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected array result"),
        };

        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(result.values().to_vec(), vec![2, 1, 3, 0]);
        Ok(())
    }

    #[test]
    fn test_grouping_uint64() -> Result<()> {
        let grouping_id = UInt64Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
        let indices = vec![Some(vec![Some(0), Some(1)])];
        
        let args = vec![
            ColumnarValue::Array(Arc::new(grouping_id)),
            ColumnarValue::Scalar(ScalarValue::List(Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(indices)))),
        ];

        let arg_fields_owned = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| Field::new(format!("arg_{idx}"), arg.data_type(), true))
            .collect::<Vec<_>>();

        let func = GroupingFunc::new();
        let result = func.invoke_with_args(ScalarFunctionArgs { 
            args,
            arg_fields: arg_fields_owned.iter().collect::<Vec<_>>(),
            number_rows: 4,
            return_field: &Field::new("f", DataType::Int32, true),
        })?;
        
        let result = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected array result"),
        };

        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(result.values().to_vec(), vec![2, 1, 3, 0]);
        Ok(())
    }

    #[test]
    fn test_grouping_with_invalid_args() -> Result<()> {
        let grouping_id = UInt8Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
        let indices = vec![Some(vec![Some(0)])];
        
        // Test with too many arguments
        let args = vec![
            ColumnarValue::Array(Arc::new(grouping_id)),
            ColumnarValue::Scalar(ScalarValue::List(Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(indices.clone())))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
        ];

        let arg_fields_owned = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| Field::new(format!("arg_{idx}"), arg.data_type(), true))
            .collect::<Vec<_>>();

        let func = GroupingFunc::new();
        let result = func.invoke_with_args(ScalarFunctionArgs { 
            args,
            arg_fields: arg_fields_owned.iter().collect::<Vec<_>>(),
            number_rows: 4,
            return_field: &Field::new("f", DataType::Int32, true),
        });
        assert!(result.is_err());

        // Test with invalid array type
        let args = vec![
            ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(1)]))),
            ColumnarValue::Scalar(ScalarValue::List(Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(indices)))),
        ];

        let func = GroupingFunc::new();
        let result = func.invoke_with_args(ScalarFunctionArgs { 
            args,
            arg_fields: arg_fields_owned.iter().collect::<Vec<_>>(),
            number_rows: 1,
            return_field: &Field::new("f", DataType::Int32, true),
        });
        assert!(result.is_err());
        Ok(())
    }
}

