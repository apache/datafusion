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

use crate::physical_plan::aggregates::group_values::GroupValues;
use arrow::array::BooleanBufferBuilder;
use arrow::buffer::NullBuffer;
use arrow_array::{ArrayRef, ArrowPrimitiveType, PrimitiveArray};
use arrow_schema::DataType;
use datafusion_common::Result;
use datafusion_physical_expr::EmitTo;
use std::collections::HashMap;
use std::sync::Arc;
use arrow_array::cast::AsArray;
use datafusion_execution::memory_pool::proxy::VecAllocExt;

/// A [`GroupValues`] storing raw primitive values
pub struct GroupValuesPrimitive<T: ArrowPrimitiveType> {
    data_type: DataType,
    map: HashMap<T::Native, usize>,
    null_group: Option<usize>,
    values: Vec<T::Native>,
}

impl<T: ArrowPrimitiveType> GroupValuesPrimitive<T> {
    pub fn new(data_type: DataType) -> Self {
        assert!(PrimitiveArray::<T>::is_compatible(&data_type));
        Self {
            data_type,
            map: HashMap::with_capacity(1024),
            values: Vec::with_capacity(1024),
            null_group: None,
        }
    }
}

impl<T: ArrowPrimitiveType> GroupValues for GroupValuesPrimitive<T>
where
    T::Native: std::hash::Hash + Eq,
{
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        assert_eq!(cols.len(), 1);
        groups.clear();

        for v in cols[0].as_primitive::<T>() {
            let group_id = match v {
                None => self.null_group.get_or_insert_with(|| {
                    let group_id = self.values.len();
                    self.values.push(Default::default());
                    group_id
                }),
                Some(key) => self.map.entry(key).or_insert_with(|| {
                    let group_id = self.values.len();
                    self.values.push(key);
                    group_id
                }),
            };
            groups.push(*group_id)
        }
        Ok(())
    }

    fn size(&self) -> usize {
        // This is an approximation
        self.map.capacity() * std::mem::size_of::<(T::Native, usize)>()
            + self.values.allocated_size()
    }

    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        fn build_primitive<T: ArrowPrimitiveType>(
            values: Vec<T::Native>,
            null_idx: Option<usize>,
        ) -> PrimitiveArray<T> {
            let nulls = null_idx.map(|null_idx| {
                let mut buffer = BooleanBufferBuilder::new(values.len());
                buffer.append_n(values.len(), true);
                buffer.set_bit(null_idx, false);
                unsafe { NullBuffer::new_unchecked(buffer.finish(), 1) }
            });
            PrimitiveArray::<T>::new(values.into(), nulls)
        }

        let array: PrimitiveArray<T> = match emit_to {
            EmitTo::All => {
                self.map.clear();
                build_primitive(std::mem::take(&mut self.values), self.null_group.take())
            }
            EmitTo::First(n) => {
                self.map.retain(|_, v| match v.checked_sub(n) {
                    Some(new) => {
                        *v = new;
                        true
                    }
                    None => false,
                });
                let null_group = match &mut self.null_group {
                    Some(v) if *v >= n => {
                        *v -= n;
                        None
                    }
                    Some(_) => self.null_group.take(),
                    None => None,
                };
                let mut split = self.values.split_off(n);
                std::mem::swap(&mut self.values, &mut split);
                build_primitive(split, null_group)
            }
        };
        Ok(vec![Arc::new(array.with_data_type(self.data_type.clone()))])
    }
}
