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

use crate::aggregate::utils::down_cast_any_ref;
use crate::expressions::format_state_name;
use crate::{AggregateExpr, PhysicalExpr, PhysicalSortExpr};
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field};
use arrow_array::{Array, ListArray};
use arrow_schema::Fields;
use datafusion_common::utils::{compare_rows, get_row_at_idx};
use datafusion_common::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::Accumulator;
use std::any::Any;
use std::sync::Arc;

/// ARRAY_AGG aggregate expression
#[derive(Debug)]
pub struct OrderSensitiveArrayAgg {
    name: String,
    data_types: Vec<DataType>,
    expr: Arc<dyn PhysicalExpr>,
    ordering_exprs: Vec<PhysicalSortExpr>,
}

impl OrderSensitiveArrayAgg {
    /// Create a new ArrayAgg aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_types: Vec<DataType>,
        ordering_exprs: Vec<PhysicalSortExpr>,
    ) -> Self {
        Self {
            name: name.into(),
            expr,
            data_types,
            ordering_exprs,
        }
    }
}

impl AggregateExpr for OrderSensitiveArrayAgg {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new_list(
            &self.name,
            Field::new("item", self.data_types[0].clone(), true),
            false,
        ))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ArrayAggAccumulator::try_new(
            &self.data_types[0],
            &self.data_types[1..],
            self.ordering_exprs.clone(),
        )?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        let mut fields = vec![Field::new_list(
            format_state_name(&self.name, "array_agg"),
            Field::new("item", self.data_types[0].clone(), true),
            false,
        )];
        if !self.ordering_exprs.is_empty() {
            let mut struct_fields = vec![];
            for dtype in self.data_types[1..].iter() {
                struct_fields.push(Field::new("dummy", dtype.clone(), true));
            }
            fields.push(Field::new_list(
                format_state_name(&self.name, "array_agg"),
                Field::new("item", DataType::Struct(Fields::from(struct_fields)), true),
                false,
            ));
        }
        for (expr, dtype) in self
            .ordering_exprs
            .iter()
            .zip(self.data_types.iter().skip(1))
        {
            let field = Field::new(
                format_state_name(expr.to_string().as_str(), "first_value_nn"),
                dtype.clone(),
                // Multi partitions may be empty hence field should be nullable.
                true,
            );
            fields.push(field);
        }
        Ok(fields)
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        let mut res = vec![self.expr.clone()];
        let ordering_exprs = self
            .ordering_exprs
            .iter()
            .map(|e| e.expr.clone())
            .collect::<Vec<_>>();
        res.extend(ordering_exprs.clone());
        res
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl PartialEq<dyn Any> for OrderSensitiveArrayAgg {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_types == x.data_types
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

#[derive(Debug)]
pub(crate) struct ArrayAggAccumulator {
    values: Vec<ScalarValue>,
    ordering_values: Vec<Vec<ScalarValue>>,
    datatypes: Vec<DataType>,
    orderings: Vec<ScalarValue>,
    sort_exprs: Vec<PhysicalSortExpr>,
}

impl ArrayAggAccumulator {
    /// new array_agg accumulator based on given item data type
    pub fn try_new(
        datatype: &DataType,
        ordering_dtypes: &[DataType],
        sort_exprs: Vec<PhysicalSortExpr>,
    ) -> Result<Self> {
        let mut orderings = vec![];
        for dtype in ordering_dtypes.iter() {
            orderings.push(ScalarValue::try_from(dtype)?);
        }
        let mut datatypes = vec![datatype.clone()];
        datatypes.extend(ordering_dtypes.iter().cloned());
        Ok(Self {
            values: vec![],
            ordering_values: vec![],
            datatypes,
            orderings,
            sort_exprs,
        })
    }
}

impl Accumulator for ArrayAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        // assert!(values.len() == 1, "array_agg can only take 1 param!");
        // let arr = &values[0];
        let n_row = values[0].len();
        (0..n_row).try_for_each(|index| {
            let row = get_row_at_idx(values, index)?;
            println!("row:{:?}", row);
            // let scalar = ScalarValue::try_from_array(arr, index)?;
            self.values.push(row[0].clone());
            self.ordering_values.push(row[1..].to_vec());
            Ok::<(), DataFusionError>(())
        })?;
        if n_row > 0 {
            let row = get_row_at_idx(values, n_row - 1)?;
            println!("row:{:?}", row);
            println!("self.orderings:{:?}", self.orderings);
            self.orderings = row[1..].to_vec();
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        for (idx, state) in states.iter().enumerate() {
            println!("idx:{:?}, state: {:?}", idx, state);
        }
        if states.len() > 1 {
            let agg_orderings = &states[1];
            if agg_orderings.as_any().is::<ListArray>() {
                for index in 0..agg_orderings.len() {
                    let ordering = ScalarValue::try_from_array(agg_orderings, index)?;
                    println!("ordering:{:?}", ordering);
                    let other_ordering = self.convert_struct_to_vec(ordering)?;
                    println!("ordering after conversion: {:?}", other_ordering);
                    let arr = &states[0];
                    let scalar = ScalarValue::try_from_array(arr, index)?;
                    if let ScalarValue::List(Some(values), _) = scalar {
                        let mut lidx = 0;
                        let mut ridx = 0;
                        let lend = self.ordering_values.len();
                        let rend = other_ordering.len();
                        let mut new_values = vec![];
                        let mut new_ordering_values = vec![];
                        assert_eq!(other_ordering.len(), values.len());
                        while lidx < lend || ridx < rend {
                            if lidx == lend {
                                new_values.extend(values[ridx..].to_vec());
                                new_ordering_values
                                    .extend(other_ordering[ridx..].to_vec());
                                ridx = rend;
                            } else if ridx == rend {
                                new_values.extend(self.values[lidx..].to_vec());
                                new_ordering_values
                                    .extend(self.ordering_values[lidx..].to_vec());
                                lidx = lend;
                            } else {
                                let sort_options = self
                                    .sort_exprs
                                    .iter()
                                    .map(|sort_expr| sort_expr.options)
                                    .collect::<Vec<_>>();
                                let compare_fn =
                                    |current: &[ScalarValue],
                                     target: &[ScalarValue]|
                                     -> Result<bool> {
                                        let cmp =
                                            compare_rows(current, target, &sort_options)?;
                                        // Ok(if SIDE { cmp.is_lt() } else { cmp.is_le() })
                                        Ok(cmp.is_lt())
                                    };
                                if compare_fn(
                                    &self.ordering_values[lidx],
                                    &other_ordering[ridx],
                                )? {
                                    new_values.push(self.values[lidx].clone());
                                    new_ordering_values
                                        .push(self.ordering_values[lidx].clone());
                                    lidx += 1;
                                } else {
                                    new_values.push(values[ridx].clone());
                                    new_ordering_values
                                        .push(other_ordering[ridx].clone());
                                    ridx += 1;
                                }
                            }
                        }
                        self.values = new_values;
                        self.ordering_values = new_ordering_values;
                        assert_eq!(self.values.len(), self.ordering_values.len());
                    } else {
                        return Err(DataFusionError::Internal(
                            "array_agg state must be list!".into(),
                        ));
                    }
                }
            } else {
                return Err(DataFusionError::Execution(
                    "Expects to receive list array".to_string(),
                ));
            }
        } else {
            if states.is_empty() {
                return Ok(());
            }
            let arr = &states[0];
            (0..arr.len()).try_for_each(|index| {
                let scalar = ScalarValue::try_from_array(arr, index)?;
                if let ScalarValue::List(Some(values), _) = scalar {
                    self.values.extend(values);
                    Ok(())
                } else {
                    Err(DataFusionError::Internal(
                        "array_agg state must be list!".into(),
                    ))
                }
            })?;
        }
        Ok(())
        // println!("other orderings: {:?}", other_orderings);
        // println!("self ordering_values: {:?}", self.ordering_values);
        // println!("self values:{:?}", self.values);
        // // assert!(states.len() == 1, "array_agg states must be singleton!");
        // let arr = &states[0];
        // (0..arr.len()).try_for_each(|index| {
        //     let scalar = ScalarValue::try_from_array(arr, index)?;
        //     if let ScalarValue::List(Some(values), _) = scalar {
        //         // while !other_orderings.is_empty() && !self.ordering_values.is_empty(){
        //         //
        //         // }
        //         self.values.extend(values);
        //         Ok(())
        //     } else {
        //         Err(DataFusionError::Internal(
        //             "array_agg state must be list!".into(),
        //         ))
        //     }
        // })
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        // Ok(vec![self.evaluate()?])
        println!("before state");
        let mut res = vec![self.evaluate()?];
        if !self.orderings.is_empty() {
            res.push(self.evaluate_orderings()?);
        }
        println!("after state");
        res.extend(self.orderings.clone());
        println!("after extend");
        Ok(res)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::new_list(
            Some(self.values.clone()),
            self.datatypes[0].clone(),
        ))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + ScalarValue::size_of_vec(&self.values)
            - std::mem::size_of_val(&self.values)
            + self.datatypes[0].size()
            - std::mem::size_of_val(&self.datatypes[0])
    }
}

impl ArrayAggAccumulator {
    fn convert_struct_to_vec(
        &self,
        in_data: ScalarValue,
    ) -> Result<Vec<Vec<ScalarValue>>> {
        if let ScalarValue::List(elem, _field_ref) = in_data {
            if let Some(elem) = elem {
                let mut res = vec![];
                for struct_vals in elem {
                    if let ScalarValue::Struct(elem, _fields) = struct_vals {
                        if let Some(elem) = elem {
                            res.push(elem);
                        } else {
                            return Err(DataFusionError::Execution(
                                "Cannot receive None".to_string(),
                            ));
                        }
                    } else {
                        return Err(DataFusionError::Execution(format!(
                            "Expects to receive ScalarValue::Struct but got:{:?}",
                            struct_vals.get_datatype()
                        )));
                    }
                }
                Ok(res)
            } else {
                Err(DataFusionError::Execution(
                    "Cannot receive None".to_string(),
                ))
            }
        } else {
            Err(DataFusionError::Execution(format!(
                "Expects to receive ScalarValue::List but got:{:?}",
                in_data.get_datatype()
            )))
        }
    }

    fn struct_dtype(&self) -> DataType {
        let mut struct_fields = vec![];
        for dtype in self.datatypes[1..].iter() {
            struct_fields.push(Field::new("dummy", dtype.clone(), true));
        }
        DataType::Struct(Fields::from(struct_fields))
    }

    fn evaluate_orderings(&self) -> Result<ScalarValue> {
        let mut orderings = vec![];
        for ordering in &self.ordering_values {
            let mut fields = vec![];
            for expr in ordering {
                let field = Field::new("dummy", expr.get_datatype(), true);
                fields.push(field);
            }
            let res = ScalarValue::Struct(Some(ordering.clone()), Fields::from(fields));
            orderings.push(res);
        }
        Ok(ScalarValue::new_list(Some(orderings), self.struct_dtype()))
    }
}
