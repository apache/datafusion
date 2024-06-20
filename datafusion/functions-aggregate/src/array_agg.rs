// Licensed to the Apache Software Foundation (ASF) under on
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

//! Defines physical expressions that can evaluated at runtime during query execution

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use arrow_array::Array;
use arrow_schema::Field;
use arrow_schema::Fields;
use datafusion_common::cast::as_list_array;
use datafusion_common::not_impl_err;
use datafusion_common::utils::array_into_list_array;
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::utils::format_state_name;
use datafusion_expr::AggregateUDF;
use datafusion_expr::AggregateUDFImpl;
use datafusion_expr::{Accumulator, Signature, Volatility};
use datafusion_physical_expr_common::sort_expr::limited_convert_logical_sort_exprs_to_physical;
use std::sync::Arc;

use crate::array_agg_distinct::DistinctArrayAggAccumulator;
use crate::array_agg_ordered::OrderSensitiveArrayAggAccumulator;

make_udaf_expr_and_func!(
    ArrayAgg,
    array_agg,
    expression,
    "Computes the nth value",
    array_agg_udaf
);

#[derive(Debug)]
/// ARRAY_AGG aggregate expression
pub struct ArrayAgg {
    signature: Signature,
    alias: Vec<String>,
    reverse: bool,
}

impl Default for ArrayAgg {
    fn default() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            alias: vec!["array_agg".to_string()],
            reverse: false,
        }
    }
}

impl AggregateUDFImpl for ArrayAgg {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "ARRAY_AGG"
    }

    fn aliases(&self) -> &[String] {
        &self.alias
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            arg_types[0].clone(),
            true,
        ))))
    }

    fn state_fields(
        &self,
        args: datafusion_expr::function::StateFieldsArgs,
    ) -> Result<Vec<Field>> {
        let mut fields = vec![Field::new_list(
            format_state_name(args.name, "array_agg"),
            Field::new("item", args.input_type.clone(), true),
            true,
        )];
        if !args.ordering_fields.is_empty() {
            fields.push(Field::new_list(
                format_state_name(args.name, "array_agg_orderings"),
                Field::new(
                    "item",
                    DataType::Struct(Fields::from(args.ordering_fields.to_vec())),
                    true,
                ),
                true,
            ));
        }
        Ok(fields)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if !acc_args.sort_exprs.is_empty() && acc_args.is_distinct {
            not_impl_err!("ARRAY_AGG(DISTINCT ORDER BY a ASC) order-sensitive aggregations are not available")
        } else if !acc_args.sort_exprs.is_empty() {
            let ordering_req = limited_convert_logical_sort_exprs_to_physical(
                acc_args.sort_exprs,
                acc_args.schema,
            )?;

            let ordering_dtypes = ordering_req
                .iter()
                .map(|e| e.expr.data_type(acc_args.schema))
                .collect::<Result<Vec<_>>>()?;

            Ok(Box::new(OrderSensitiveArrayAggAccumulator::try_new(
                acc_args.input_type,
                &ordering_dtypes,
                ordering_req,
                self.reverse,
            )?))
        } else if acc_args.is_distinct {
            Ok(Box::new(DistinctArrayAggAccumulator::try_new(
                acc_args.input_type,
            )?))
        } else {
            Ok(Box::new(ArrayAggAccumulator::try_new(acc_args.input_type)?))
        }
    }

    fn order_sensitivity(&self) -> datafusion_expr::utils::AggregateOrderSensitivity {
        datafusion_expr::utils::AggregateOrderSensitivity::HardRequirement
    }

    fn reverse_expr(&self) -> datafusion_expr::ReversedUDAF {
        datafusion_expr::ReversedUDAF::Reversed(Arc::new(AggregateUDF::from(Self {
            signature: self.signature.clone(),
            alias: self.alias.clone(),
            reverse: !self.reverse,
        })))
    }
}

#[derive(Debug)]
pub(crate) struct ArrayAggAccumulator {
    values: Vec<ArrayRef>,
    datatype: DataType,
}

impl ArrayAggAccumulator {
    /// new array_agg accumulator based on given item data type
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        Ok(Self {
            values: vec![],
            datatype: datatype.clone(),
        })
    }
}

impl Accumulator for ArrayAggAccumulator {
    // Append value like Int64Array(1,2,3)
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        assert!(values.len() == 1, "array_agg can only take 1 param!");
        let val = values[0].clone();
        self.values.push(val);
        Ok(())
    }

    // Append value like ListArray(Int64Array(1,2,3), Int64Array(4,5,6))
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        assert!(states.len() == 1, "array_agg states must be singleton!");

        let list_arr = as_list_array(&states[0])?;
        for arr in list_arr.iter().flatten() {
            self.values.push(arr);
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Transform Vec<ListArr> to ListArr

        let element_arrays: Vec<&dyn Array> =
            self.values.iter().map(|a| a.as_ref()).collect();

        if element_arrays.is_empty() {
            let arr = ScalarValue::new_list(&[], &self.datatype);
            return Ok(ScalarValue::List(arr));
        }

        let concated_array = arrow::compute::concat(&element_arrays)?;
        let list_array = array_into_list_array(concated_array);

        Ok(ScalarValue::List(Arc::new(list_array)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + (std::mem::size_of::<ArrayRef>() * self.values.capacity())
            + self
                .values
                .iter()
                .map(|arr| arr.get_array_memory_size())
                .sum::<usize>()
            + self.datatype.size()
            - std::mem::size_of_val(&self.datatype)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::Result;
    use datafusion_physical_expr_common::aggregate::create_aggregate_expr;
    use datafusion_physical_expr_common::expressions::column::Column;
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

    #[test]
    fn test_array_agg_expr() -> Result<()> {
        let data_types = vec![
            DataType::UInt32,
            DataType::Int32,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal128(10, 2),
            DataType::Utf8,
        ];
        for data_type in &data_types {
            let input_schema =
                Schema::new(vec![Field::new("c1", data_type.clone(), true)]);
            let input_phy_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(
                Column::new_with_schema("c1", &input_schema).unwrap(),
            )];
            let result_agg_phy_exprs = create_aggregate_expr(
                &array_agg_udaf(),
                &input_phy_exprs[0..1],
                &[],
                &[],
                &[],
                &input_schema,
                "c1",
                false,
                false,
            )?;
            assert_eq!("c1", result_agg_phy_exprs.name());
            assert_eq!(
                Field::new_list("c1", Field::new("item", data_type.clone(), true), true,),
                result_agg_phy_exprs.field().unwrap()
            );

            let result_distinct = create_aggregate_expr(
                &array_agg_udaf(),
                &input_phy_exprs[0..1],
                &[],
                &[],
                &[],
                &input_schema,
                "c1",
                false,
                true,
            )?;
            assert_eq!("c1", result_distinct.name());
            assert_eq!(
                Field::new_list("c1", Field::new("item", data_type.clone(), true), true,),
                result_agg_phy_exprs.field().unwrap()
            );
        }
        Ok(())
    }
}
