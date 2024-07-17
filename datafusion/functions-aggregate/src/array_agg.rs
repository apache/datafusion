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

//! Defines physical expressions that can evaluated at runtime during query execution

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType;
use arrow_schema::Field;

use datafusion_common::cast::as_list_array;
use datafusion_common::utils::array_into_list_array;
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::expr::AggregateFunctionDefinition;
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::simplify::SimplifyInfo;
use datafusion_expr::utils::format_state_name;
use datafusion_expr::AggregateUDFImpl;
use datafusion_expr::Expr;
use datafusion_expr::{Accumulator, Signature, Volatility};
use std::sync::Arc;

make_udaf_expr_and_func!(
    ArrayAgg,
    array_agg,
    expression,
    "input values, including nulls, concatenated into an array",
    array_agg_udaf
);

#[derive(Debug)]
/// ARRAY_AGG aggregate expression
pub struct ArrayAgg {
    signature: Signature,
    alias: Vec<String>,
}

impl Default for ArrayAgg {
    fn default() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            alias: vec!["array_agg".to_string()],
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
        Ok(vec![Field::new_list(
            format_state_name(args.name, "array_agg"),
            Field::new("item", args.input_type.clone(), true),
            args.input_nullable,
        )])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ArrayAggAccumulator::try_new(acc_args.input_type)?))
    }

    fn reverse_expr(&self) -> datafusion_expr::ReversedUDAF {
        datafusion_expr::ReversedUDAF::Identical
    }

    fn simplify(
        &self,
    ) -> Option<datafusion_expr::function::AggregateFunctionSimplification> {
        let simplify = |aggregate_function: AggregateFunction, _: &dyn SimplifyInfo| {
            if aggregate_function.order_by.is_some() || aggregate_function.distinct {
                Ok(Expr::AggregateFunction(AggregateFunction {
                    func_def: AggregateFunctionDefinition::BuiltIn(
                        datafusion_expr::aggregate_function::AggregateFunction::ArrayAgg,
                    ),
                    args: aggregate_function.args,
                    distinct: aggregate_function.distinct,
                    filter: aggregate_function.filter,
                    order_by: aggregate_function.order_by,
                    null_treatment: aggregate_function.null_treatment,
                }))
            } else {
                Ok(Expr::AggregateFunction(aggregate_function))
            }
        };

        Some(Box::new(simplify))
    }
}

#[derive(Debug)]
pub struct ArrayAggAccumulator {
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
