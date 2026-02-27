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

use arrow::array::{ArrayRef, AsArray, Decimal128Array};
use arrow::datatypes::DataType;
use datafusion_common::Result;
use std::sync::Arc;

/// Check if DataFusion's built-in cast from Boolean to the target type is
/// compatible with Spark behavior.
pub fn is_df_cast_from_bool_spark_compatible(to_type: &DataType) -> bool {
    use DataType::*;
    matches!(
        to_type,
        Int8 | Int16 | Int32 | Int64 | Float32 | Float64 | Utf8
    )
}

/// Cast a Boolean array to Decimal128 with the given precision and scale.
/// true -> 1 * 10^scale, false -> 0, null -> null
pub fn cast_boolean_to_decimal(
    array: &ArrayRef,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef> {
    let bool_array = array.as_boolean();
    let scaled_val = 10_i128.pow(scale as u32);
    let result: Decimal128Array = bool_array
        .iter()
        .map(|v| v.map(|b| if b { scaled_val } else { 0 }))
        .collect();
    Ok(Arc::new(result.with_precision_and_scale(precision, scale)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, BooleanArray};

    #[test]
    fn test_is_df_cast_from_bool_spark_compatible() {
        assert!(!is_df_cast_from_bool_spark_compatible(&DataType::Boolean));
        assert!(is_df_cast_from_bool_spark_compatible(&DataType::Int8));
        assert!(is_df_cast_from_bool_spark_compatible(&DataType::Int16));
        assert!(is_df_cast_from_bool_spark_compatible(&DataType::Int32));
        assert!(is_df_cast_from_bool_spark_compatible(&DataType::Int64));
        assert!(is_df_cast_from_bool_spark_compatible(&DataType::Float32));
        assert!(is_df_cast_from_bool_spark_compatible(&DataType::Float64));
        assert!(is_df_cast_from_bool_spark_compatible(&DataType::Utf8));
        assert!(!is_df_cast_from_bool_spark_compatible(
            &DataType::Decimal128(10, 4)
        ));
        assert!(!is_df_cast_from_bool_spark_compatible(&DataType::Null));
    }

    #[test]
    fn test_cast_boolean_to_decimal() {
        let array: ArrayRef =
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None]));
        let result = cast_boolean_to_decimal(&array, 10, 4).unwrap();
        let arr = result
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(arr.value(0), 10000);
        assert_eq!(arr.value(1), 0);
        assert!(arr.is_null(2));
    }
}
