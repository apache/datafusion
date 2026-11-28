// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::array::{ArrayRef, BooleanArray};
use arrow::downcast_dictionary_array;
use datafusion_common::internal_err;
use datafusion_common::{ScalarValue, arrow_datafusion_err};
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
        size_of_val(self) + self.inner.size()
    }

    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        self.inner.state()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        self.inner.merge_batch(states)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate::count_distinct::FloatDistinctCountAccumulator;
    use arrow::datatypes::Float64Type;

    #[test]
    fn size_includes_boxed_accumulator_and_wrapper() {
        let inner = FloatDistinctCountAccumulator::<Float64Type>::new();
        let expected = size_of::<DictionaryCountAccumulator>() + size_of_val(&inner);
        let accumulator = DictionaryCountAccumulator::new(Box::new(inner));
        assert_eq!(accumulator.size(), expected);
    }
}
