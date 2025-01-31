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

use arrow::{array::MutableArrayData, datatypes::ArrowNativeType, record_batch::RecordBatch};
use arrow_array::{Array, GenericListArray, Int32Array, OffsetSizeTrait};
use arrow_schema::{DataType, FieldRef, Schema};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{
    cast::{as_int32_array, as_large_list_array, as_list_array},
    internal_err, DataFusionError, Result as DataFusionResult, ScalarValue,
};
use datafusion_physical_expr::PhysicalExpr;
use std::hash::Hash;
use std::{
    any::Any,
    fmt::{Debug, Display, Formatter},
    sync::Arc,
};

#[derive(Debug, Eq)]
pub struct ListExtract {
    child: Arc<dyn PhysicalExpr>,
    ordinal: Arc<dyn PhysicalExpr>,
    default_value: Option<Arc<dyn PhysicalExpr>>,
    one_based: bool,
    fail_on_error: bool,
}

impl Hash for ListExtract {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.ordinal.hash(state);
        self.default_value.hash(state);
        self.one_based.hash(state);
        self.fail_on_error.hash(state);
    }
}
impl PartialEq for ListExtract {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
            && self.ordinal.eq(&other.ordinal)
            && self.default_value.eq(&other.default_value)
            && self.one_based.eq(&other.one_based)
            && self.fail_on_error.eq(&other.fail_on_error)
    }
}

impl ListExtract {
    pub fn new(
        child: Arc<dyn PhysicalExpr>,
        ordinal: Arc<dyn PhysicalExpr>,
        default_value: Option<Arc<dyn PhysicalExpr>>,
        one_based: bool,
        fail_on_error: bool,
    ) -> Self {
        Self {
            child,
            ordinal,
            default_value,
            one_based,
            fail_on_error,
        }
    }

    fn child_field(&self, input_schema: &Schema) -> DataFusionResult<FieldRef> {
        match self.child.data_type(input_schema)? {
            DataType::List(field) | DataType::LargeList(field) => Ok(field),
            data_type => Err(DataFusionError::Internal(format!(
                "Unexpected data type in ListExtract: {:?}",
                data_type
            ))),
        }
    }
}

impl PhysicalExpr for ListExtract {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(self.child_field(input_schema)?.data_type().clone())
    }

    fn nullable(&self, input_schema: &Schema) -> DataFusionResult<bool> {
        // Only non-nullable if fail_on_error is enabled and the element is non-nullable
        Ok(!self.fail_on_error || self.child_field(input_schema)?.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let child_value = self.child.evaluate(batch)?.into_array(batch.num_rows())?;
        let ordinal_value = self.ordinal.evaluate(batch)?.into_array(batch.num_rows())?;

        let default_value = self
            .default_value
            .as_ref()
            .map(|d| {
                d.evaluate(batch).map(|value| match value {
                    ColumnarValue::Scalar(scalar)
                        if !scalar.data_type().equals_datatype(child_value.data_type()) =>
                    {
                        scalar.cast_to(child_value.data_type())
                    }
                    ColumnarValue::Scalar(scalar) => Ok(scalar),
                    v => Err(DataFusionError::Execution(format!(
                        "Expected scalar default value for ListExtract, got {:?}",
                        v
                    ))),
                })
            })
            .transpose()?
            .unwrap_or(self.data_type(&batch.schema())?.try_into())?;

        let adjust_index = if self.one_based {
            one_based_index
        } else {
            zero_based_index
        };

        match child_value.data_type() {
            DataType::List(_) => {
                let list_array = as_list_array(&child_value)?;
                let index_array = as_int32_array(&ordinal_value)?;

                list_extract(
                    list_array,
                    index_array,
                    &default_value,
                    self.fail_on_error,
                    adjust_index,
                )
            }
            DataType::LargeList(_) => {
                let list_array = as_large_list_array(&child_value)?;
                let index_array = as_int32_array(&ordinal_value)?;

                list_extract(
                    list_array,
                    index_array,
                    &default_value,
                    self.fail_on_error,
                    adjust_index,
                )
            }
            data_type => Err(DataFusionError::Internal(format!(
                "Unexpected child type for ListExtract: {:?}",
                data_type
            ))),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child, &self.ordinal]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        match children.len() {
            2 => Ok(Arc::new(ListExtract::new(
                Arc::clone(&children[0]),
                Arc::clone(&children[1]),
                self.default_value.clone(),
                self.one_based,
                self.fail_on_error,
            ))),
            _ => internal_err!("ListExtract should have exactly two children"),
        }
    }
}

fn one_based_index(index: i32, len: usize) -> DataFusionResult<Option<usize>> {
    if index == 0 {
        return Err(DataFusionError::Execution(
            "Invalid index of 0 for one-based ListExtract".to_string(),
        ));
    }

    let abs_index = index.abs().as_usize();
    if abs_index <= len {
        if index > 0 {
            Ok(Some(abs_index - 1))
        } else {
            Ok(Some(len - abs_index))
        }
    } else {
        Ok(None)
    }
}

fn zero_based_index(index: i32, len: usize) -> DataFusionResult<Option<usize>> {
    if index < 0 {
        Ok(None)
    } else {
        let positive_index = index.as_usize();
        if positive_index < len {
            Ok(Some(positive_index))
        } else {
            Ok(None)
        }
    }
}

fn list_extract<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
    index_array: &Int32Array,
    default_value: &ScalarValue,
    fail_on_error: bool,
    adjust_index: impl Fn(i32, usize) -> DataFusionResult<Option<usize>>,
) -> DataFusionResult<ColumnarValue> {
    let values = list_array.values();
    let offsets = list_array.offsets();

    let data = values.to_data();

    let default_data = default_value.to_array()?.to_data();

    let mut mutable = MutableArrayData::new(vec![&data, &default_data], true, index_array.len());

    for (row, (offset_window, index)) in offsets.windows(2).zip(index_array.values()).enumerate() {
        let start = offset_window[0].as_usize();
        let len = offset_window[1].as_usize() - start;

        if let Some(i) = adjust_index(*index, len)? {
            mutable.extend(0, start + i, start + i + 1);
        } else if list_array.is_null(row) {
            mutable.extend_nulls(1);
        } else if fail_on_error {
            return Err(DataFusionError::Execution(
                "Index out of bounds for array".to_string(),
            ));
        } else {
            mutable.extend(1, 0, 1);
        }
    }

    let data = mutable.freeze();
    Ok(ColumnarValue::Array(arrow::array::make_array(data)))
}

impl Display for ListExtract {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ListExtract [child: {:?}, ordinal: {:?}, default_value: {:?}, one_based: {:?}, fail_on_error: {:?}]",
            self.child, self.ordinal,  self.default_value, self.one_based, self.fail_on_error
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::datatypes::Int32Type;
    use arrow_array::{Array, Int32Array, ListArray};
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::ColumnarValue;

    #[test]
    fn test_list_extract_default_value() -> Result<()> {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1)]),
            None,
            Some(vec![]),
        ]);
        let indices = Int32Array::from(vec![0, 0, 0]);

        let null_default = ScalarValue::Int32(None);

        let ColumnarValue::Array(result) =
            list_extract(&list, &indices, &null_default, false, zero_based_index)?
        else {
            unreachable!()
        };

        assert_eq!(
            &result.to_data(),
            &Int32Array::from(vec![Some(1), None, None]).to_data()
        );

        let zero_default = ScalarValue::Int32(Some(0));

        let ColumnarValue::Array(result) =
            list_extract(&list, &indices, &zero_default, false, zero_based_index)?
        else {
            unreachable!()
        };

        assert_eq!(
            &result.to_data(),
            &Int32Array::from(vec![Some(1), None, Some(0)]).to_data()
        );
        Ok(())
    }
}
