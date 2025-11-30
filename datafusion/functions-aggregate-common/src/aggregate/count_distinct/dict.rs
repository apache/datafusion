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

use arrow::array::{ArrayRef, BooleanArray};
use arrow::downcast_dictionary_array;
use datafusion_common::internal_err;
use datafusion_common::{arrow_datafusion_err, ScalarValue};
use datafusion_expr_common::accumulator::Accumulator;

#[derive(Debug)]
pub struct DictionaryCountAccumulator {
    inner: Box<dyn Accumulator>,
}

impl DictionaryCountAccumulator {
    pub fn new(inner: Box<dyn Accumulator>) -> Self {
        Self { inner }
    }
}

impl Accumulator for DictionaryCountAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        let values: Vec<_> = values
            .iter()
            .map(|dict| {
                downcast_dictionary_array! {
                    dict => {
                        let buff: BooleanArray = dict.occupancy().into();
                        arrow::compute::filter(
                            dict.values(),
                            &buff
                        ).map_err(|e| arrow_datafusion_err!(e))
                    },
                    _ => internal_err!("DictionaryCountAccumulator only supports dictionary arrays")
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.inner.update_batch(values.as_slice())
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        self.inner.evaluate()
    }

    fn size(&self) -> usize {
        self.inner.size()
    }

    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        self.inner.state()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        self.inner.merge_batch(states)
    }
}
