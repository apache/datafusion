use crate::from_slice::FromSlice;
use arrow::array::{ArrayRef, Float32Array, Float64Array};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use itertools::Itertools;
use std::sync::Arc;

use crate::{DataFusionError, Result};

pub fn bisect_left_arrow(
    item_arrs: &Vec<&[f64]>,
    target_value: Vec<f64>,
) -> Result<usize> {
    let mut low: usize = 0;
    let mut high: usize = item_arrs[0].len();
    while low < high {
        let mid = ((high - low) / 2) + low;
        let val: Vec<f64> = item_arrs
            .iter()
            .map(|arr| *arr.get(mid).unwrap())
            .collect_vec();
        // Search values that are greater than val - to right of current mid_index
        println!("{:?}, {:?}", val, target_value);
        if val < target_value {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    Ok(low)
}

pub fn bisect_right_arrow(
    item_arrs: &Vec<&[f64]>,
    target_value: Vec<f64>,
) -> Result<usize> {
    let mut low: usize = 0;
    let mut high: usize = item_arrs[0].len();
    while low < high {
        let mid = ((high - low) / 2) + low;
        let val: Vec<f64> = item_arrs
            .iter()
            .map(|arr| *arr.get(mid).unwrap())
            .collect_vec();
        // Search values that are greater than val - to right of current mid_index
        println!("{:?}, {:?}", val, target_value);
        if val > target_value {
            high = mid;
        } else {
            low = mid + 1;
        }
    }
    Ok(low)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::from_slice::FromSlice;
    use arrow::{
        array::{Float32Array, Float64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use super::*;
    use crate::ScalarValue;
    use crate::ScalarValue::Null;
    use arrow::compute::cast;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_bisect_left_and_right() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from_slice(&[5.0, 7.0, 8.0, 9., 10.])),
            Arc::new(Float32Array::from_slice(&[2.0, 3.0, 3.0, 4.0, 0.0])),
            Arc::new(Float32Array::from_slice(&[5.0, 7.0, 8.0, 10., 0.0])),
            Arc::new(Float32Array::from_slice(&[5.0, 7.0, 8.0, 10., 0.0])),
        ];
        let order_columns: Vec<ArrayRef> = arrays
            .iter()
            .map(|array| cast(&array, &DataType::Float64).unwrap())
            .collect();
        let search_tuple: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from_slice(&[8.0])),
            Arc::new(Float32Array::from_slice(&[3.0])),
            Arc::new(Float32Array::from_slice(&[8.0])),
            Arc::new(Float32Array::from_slice(&[8.0])),
        ];
        let k: Vec<ArrayRef> = search_tuple
            .iter()
            .map(|array| cast(&array, &DataType::Float64).unwrap())
            .collect();
        let item_arrs = order_columns
            .iter()
            .map(|item| {
                item.as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .values()
            })
            .collect_vec();
        let target_value = k
            .iter()
            .map(|item| {
                item.as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .values()
            })
            .collect_vec()
            .iter()
            .map(|arr| *arr.get(0).unwrap())
            .collect_vec();
        let res: usize = bisect_left_arrow(&item_arrs, target_value.clone()).unwrap();
        // define data in two partitions
        assert_eq!(res, 2);
        let res: usize = bisect_right_arrow(&item_arrs, target_value.clone()).unwrap();
        assert_eq!(res, 3);
    }

    #[tokio::test]
    async fn vector_ord() {
        assert_eq!(true, vec![0, 1] < vec![1]);
        assert_eq!(true, vec![1, 1] < vec![1, 2]);
        assert_eq!(
            true,
            vec![1, 0, 0, 0, 0, 0, 0, 1] < vec![1, 0, 0, 0, 0, 0, 0, 2]
        );
        assert_eq!(
            true,
            vec![1, 0, 0, 0, 0, 0, 1, 1] > vec![1, 0, 0, 0, 0, 0, 0, 2]
        );
        assert_eq!(
            true,
            vec![1, 0, 0, 0, 0, 1, 9, 9] < vec![1, 0, 0, 0, 0, 2, 0, 0]
        );
        assert_eq!(
            true,
            vec![
                ScalarValue::Int32(Some(2)),
                Null,
                ScalarValue::Int32(Some(0))
            ] < vec![
                ScalarValue::Int32(Some(2)),
                Null,
                ScalarValue::Int32(Some(1))
            ]
        );
    }
}
