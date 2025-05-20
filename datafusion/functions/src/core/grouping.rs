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

use arrow::array::{Array, Int32Array, UInt8Array, UInt16Array, UInt32Array, UInt64Array};
use arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, Documentation, ReturnInfo, ReturnTypeArgs, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;
use std::sync::Arc;

macro_rules! grouping_id {
    ($grouping_id:expr, $indices:expr, $type:ty, $array_type:ty) => {{
        let grouping_id = match $grouping_id.as_any().downcast_ref::<$array_type>() {
            Some(array) => array,
            None => return exec_err!("grouping function requires {} grouping_id array", stringify!($type)),
        };
        grouping_id
            .iter()
            .map(|grouping_id| {
                grouping_id.map(|grouping_id| {
                    let mut result = 0 as $type;
                    for (i, index) in $indices.iter().enumerate() {
                        if let Some(index) = index {
                            let bit = (grouping_id >> index) & 1;
                            result |= bit << i;
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

    fn return_type_from_args(&self, _args: ReturnTypeArgs) -> Result<ReturnInfo> {
        Ok(ReturnInfo::new(DataType::Int32, false))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = args.args;
        if args.len() != 2 && args.len() != 1 {
            return exec_err!(
                "grouping function requires 1 or 2 arguments, got {}",
                args.len()
            );
        }

        let grouping_id = match &args[0] {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(_) => {
                return exec_err!("grouping function requires array input for grouping_id")
            }
        };

        if args.len() == 1 {
            return args[0].cast_to(grouping_id.data_type(), None);
        }

        let indices = match &args[1] {
            ColumnarValue::Scalar(scalar) => {
                match scalar {
                    ScalarValue::List(array) => {
                        // Get the values array from the list array
                        let Some(values) = array.values().as_any().downcast_ref::<Int32Array>() else {
                            return exec_err!("grouping function requires Int32 indices array")
                        };
                        values
                    }
                    ScalarValue::FixedSizeList(array) => {
                        // Get the values array from the list array
                        let Some(values) = array.values().as_any().downcast_ref::<Int32Array>() else {
                            return exec_err!("grouping function requires Int32 indices array")
                        };
                        values
                    }
                    _ => {
                        return exec_err!("grouping function requires list of Int32 indices")
                    }
                }
            }
            ColumnarValue::Array(_) => {
                return exec_err!("grouping function requires scalar input for indices")
            }
        };

        if indices.null_count() > 0 {
            return exec_err!("grouping function requires non-null indices array");
        }

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

        Ok(ColumnarValue::Array(Arc::new(result)))
    }

    fn short_circuits(&self) -> bool {
        false
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return exec_err!(
                "grouping function requires exactly 2 arguments, got {}",
                arg_types.len()
            );
        }

        match arg_types[0] {
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {}
            _ => {
                return exec_err!(
                    "grouping function requires UInt8/16/32/64 for first argument, got {}",
                    arg_types[0]
                )
            }
        }

        if arg_types[1] != DataType::Int32 {
            return exec_err!(
                "grouping function requires Int32 for second argument, got {}",
                arg_types[1]
            );
        }

        Ok(arg_types.to_vec())
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{array::{Int32Array, ListArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array}, datatypes::Int32Type};
    use datafusion_common::Result;

    #[test]
    fn test_grouping_uint8() -> Result<()> {
        let grouping_id = UInt8Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
        let indices = vec![Some(vec![Some(0), Some(1)])];
        
        let args = vec![
            ColumnarValue::Array(Arc::new(grouping_id)),
            ColumnarValue::Scalar(ScalarValue::List(Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(indices)))),
        ];

        let func = GroupingFunc::new();
        let return_type = DataType::Int32;
        let result = func.invoke_with_args(ScalarFunctionArgs { 
            args,
            number_rows: 4,
            return_type: &return_type,
        })?;
        
        let result = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected array result"),
        };

        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(result.values().to_vec(), vec![1, 2, 3, 3]);
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

        let func = GroupingFunc::new();
        let return_type = DataType::Int32;
        let result = func.invoke_with_args(ScalarFunctionArgs { 
            args,
            number_rows: 4,
            return_type: &return_type,
        })?;
        
        let result = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected array result"),
        };

        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(result.values().to_vec(), vec![1, 2, 3, 3]);
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

        let func = GroupingFunc::new();
        let return_type = DataType::Int32;
        let result = func.invoke_with_args(ScalarFunctionArgs { 
            args,
            number_rows: 4,
            return_type: &return_type,
        })?;
        
        let result = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected array result"),
        };

        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(result.values().to_vec(), vec![1, 2, 3, 3]);
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

        let func = GroupingFunc::new();
        let return_type = DataType::Int32;
        let result = func.invoke_with_args(ScalarFunctionArgs { 
            args,
            number_rows: 4,
            return_type: &return_type,
        })?;
        
        let result = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected array result"),
        };

        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(result.values().to_vec(), vec![1, 2, 3, 3]);
        Ok(())
    }

    #[test]
    fn test_grouping_with_null_indices() -> Result<()> {
        let grouping_id = UInt8Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
        let indices = vec![Some(vec![Some(0), None])];
        
        let args = vec![
            ColumnarValue::Array(Arc::new(grouping_id)),
            ColumnarValue::Scalar(ScalarValue::List(Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(indices)))),
        ];

        let func = GroupingFunc::new();
        let return_type = DataType::Int32;
        let result = func.invoke_with_args(ScalarFunctionArgs { 
            args,
            number_rows: 4,
            return_type: &return_type,
        });
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn test_grouping_with_invalid_args() -> Result<()> {
        let grouping_id = UInt8Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
        let indices = vec![Some(vec![Some(0)])];
        
        // Test with too many arguments
        let args = vec![
            ColumnarValue::Array(Arc::new(grouping_id)),
            ColumnarValue::Scalar(ScalarValue::List(Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(indices)))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
        ];

        let func = GroupingFunc::new();
        let return_type = DataType::Int32;
        let result = func.invoke_with_args(ScalarFunctionArgs { 
            args,
            number_rows: 4,
            return_type: &return_type,
        });
        assert!(result.is_err());

        // Test with invalid array type
        let args = vec![
            ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(1)]))),
            ColumnarValue::Scalar(ScalarValue::List(Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(indices)))),
        ];

        let func = GroupingFunc::new();
        let return_type = DataType::Int32;
        let result = func.invoke_with_args(ScalarFunctionArgs { 
            args,
            number_rows: 1,
            return_type: &return_type,
        });
        assert!(result.is_err());
        Ok(())
    }
}

