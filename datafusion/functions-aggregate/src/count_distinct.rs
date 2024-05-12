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

use std::{collections::HashSet, hash::RandomState};

use arrow::{array::{ArrayRef, AsArray}, datatypes::{DataType, Date32Type, Date64Type, Decimal128Type, Decimal256Type, Field, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type}};
use datafusion_common::{internal_err, Result, ScalarValue};
use datafusion_expr::{
    function::AccumulatorArgs, utils::format_state_name, Accumulator, AggregateUDFImpl, Signature, Volatility
};
use datafusion_physical_expr_common::{aggregate::count_distinct::{BytesDistinctCountAccumulator, FloatDistinctCountAccumulator, PrimitiveDistinctCountAccumulator}, binary_map::OutputType};


make_udaf_expr_and_func!(
    CountDistinct,
    count_distinct,
    expression,
    "count_distinct doc",
    count_distinct_udaf
);

pub struct CountDistinct {
    aliases: Vec<String>,
    signature: Signature,
}

impl CountDistinct {
    pub fn new() -> Self {
        Self {
            aliases: vec!["count".to_string()],
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl Default for CountDistinct {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for CountDistinct {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("CountDistinct")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .field("accumulator", &"<FUNC>")
            .finish()
    }
}

impl AggregateUDFImpl for CountDistinct {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "COUNT"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[DataType],
    ) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn accumulator(
        &self,
        acc_args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>> {
        let data_type = acc_args.data_type;
        Ok(match data_type {
            // try and use a specialized accumulator if possible, otherwise fall back to generic accumulator
            DataType::Int8 => Box::new(PrimitiveDistinctCountAccumulator::<Int8Type>::new(
                data_type,
            )),
            DataType::Int16 => Box::new(PrimitiveDistinctCountAccumulator::<Int16Type>::new(
                data_type,
            )),
            DataType::Int32 => Box::new(PrimitiveDistinctCountAccumulator::<Int32Type>::new(
                data_type,
            )),
            DataType::Int64 => Box::new(PrimitiveDistinctCountAccumulator::<Int64Type>::new(
                data_type,
            )),
            DataType::UInt8 => Box::new(PrimitiveDistinctCountAccumulator::<UInt8Type>::new(
                data_type,
            )),
            DataType::UInt16 => Box::new(PrimitiveDistinctCountAccumulator::<UInt16Type>::new(
                data_type,
            )),
            DataType::UInt32 => Box::new(PrimitiveDistinctCountAccumulator::<UInt32Type>::new(
                data_type,
            )),
            DataType::UInt64 => Box::new(PrimitiveDistinctCountAccumulator::<UInt64Type>::new(
                data_type,
            )),
            DataType::Decimal128(_, _) => Box::new(PrimitiveDistinctCountAccumulator::<
                Decimal128Type,
            >::new(data_type)),
            DataType::Decimal256(_, _) => Box::new(PrimitiveDistinctCountAccumulator::<
                Decimal256Type,
            >::new(data_type)),

            DataType::Date32 => Box::new(PrimitiveDistinctCountAccumulator::<Date32Type>::new(
                data_type,
            )),
            DataType::Date64 => Box::new(PrimitiveDistinctCountAccumulator::<Date64Type>::new(
                data_type,
            )),
            DataType::Time32(TimeUnit::Millisecond) => Box::new(PrimitiveDistinctCountAccumulator::<
                Time32MillisecondType,
            >::new(data_type)),
            DataType::Time32(TimeUnit::Second) => Box::new(PrimitiveDistinctCountAccumulator::<
                Time32SecondType,
            >::new(data_type)),
            DataType::Time64(TimeUnit::Microsecond) => Box::new(PrimitiveDistinctCountAccumulator::<
                Time64MicrosecondType,
            >::new(data_type)),
            DataType::Time64(TimeUnit::Nanosecond) => Box::new(PrimitiveDistinctCountAccumulator::<
                Time64NanosecondType,
            >::new(data_type)),
            DataType::Timestamp(TimeUnit::Microsecond, _) => Box::new(PrimitiveDistinctCountAccumulator::<
                TimestampMicrosecondType,
            >::new(data_type)),
            DataType::Timestamp(TimeUnit::Millisecond, _) => Box::new(PrimitiveDistinctCountAccumulator::<
                TimestampMillisecondType,
            >::new(data_type)),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => Box::new(PrimitiveDistinctCountAccumulator::<
                TimestampNanosecondType,
            >::new(data_type)),
            DataType::Timestamp(TimeUnit::Second, _) => Box::new(PrimitiveDistinctCountAccumulator::<
                TimestampSecondType,
            >::new(data_type)),

            DataType::Float16 => Box::new(FloatDistinctCountAccumulator::<Float16Type>::new()),
            DataType::Float32 => Box::new(FloatDistinctCountAccumulator::<Float32Type>::new()),
            DataType::Float64 => Box::new(FloatDistinctCountAccumulator::<Float64Type>::new()),

            DataType::Utf8 => Box::new(BytesDistinctCountAccumulator::<i32>::new(OutputType::Utf8)),
            DataType::LargeUtf8 => {
                Box::new(BytesDistinctCountAccumulator::<i64>::new(OutputType::Utf8))
            }
            DataType::Binary => Box::new(BytesDistinctCountAccumulator::<i32>::new(
                OutputType::Binary,
            )),
            DataType::LargeBinary => Box::new(BytesDistinctCountAccumulator::<i64>::new(
                OutputType::Binary,
            )),

            // Use the generic accumulator based on `ScalarValue` for all other types
            _ => Box::new(DistinctCountAccumulator {
                values: HashSet::default(),
                state_data_type: data_type.clone(),
            }),
        })
    }

    fn state_fields(
        &self,
        name: &str,
        value_type: DataType,
        _ordering_fields: Vec<Field>,
    ) -> Result<Vec<Field>> {
        Ok(vec![Field::new_list(
            format_state_name(name, "count distinct"),
            Field::new("item", value_type, true),
            false,
        )])
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// General purpose distinct accumulator that works for any DataType by using
/// [`ScalarValue`].
///
/// It stores intermediate results as a `ListArray`
///
/// Note that many types have specialized accumulators that are (much)
/// more efficient such as [`PrimitiveDistinctCountAccumulator`] and
/// [`BytesDistinctCountAccumulator`]
#[derive(Debug)]
struct DistinctCountAccumulator {
    values: HashSet<ScalarValue, RandomState>,
    state_data_type: DataType,
}

impl DistinctCountAccumulator {
    // calculating the size for fixed length values, taking first batch size *
    // number of batches This method is faster than .full_size(), however it is
    // not suitable for variable length values like strings or complex types
    fn fixed_size(&self) -> usize {
        std::mem::size_of_val(self)
            + (std::mem::size_of::<ScalarValue>() * self.values.capacity())
            + self
                .values
                .iter()
                .next()
                .map(|vals| ScalarValue::size(vals) - std::mem::size_of_val(vals))
                .unwrap_or(0)
            + std::mem::size_of::<DataType>()
    }

    // calculates the size as accurately as possible. Note that calling this
    // method is expensive
    fn full_size(&self) -> usize {
        std::mem::size_of_val(self)
            + (std::mem::size_of::<ScalarValue>() * self.values.capacity())
            + self
                .values
                .iter()
                .map(|vals| ScalarValue::size(vals) - std::mem::size_of_val(vals))
                .sum::<usize>()
            + std::mem::size_of::<DataType>()
    }
}

impl Accumulator for DistinctCountAccumulator {
    /// Returns the distinct values seen so far as (one element) ListArray.
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let scalars = self.values.iter().cloned().collect::<Vec<_>>();
        let arr = ScalarValue::new_list(scalars.as_slice(), &self.state_data_type);
        Ok(vec![ScalarValue::List(arr)])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let arr = &values[0];
        if arr.data_type() == &DataType::Null {
            return Ok(());
        }

        (0..arr.len()).try_for_each(|index| {
            if !arr.is_null(index) {
                let scalar = ScalarValue::try_from_array(arr, index)?;
                self.values.insert(scalar);
            }
            Ok(())
        })
    }

    /// Merges multiple sets of distinct values into the current set.
    ///
    /// The input to this function is a `ListArray` with **multiple** rows,
    /// where each row contains the values from a partial aggregate's phase (e.g.
    /// the result of calling `Self::state` on multiple accumulators).
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        assert_eq!(states.len(), 1, "array_agg states must be singleton!");
        let array = &states[0];
        let list_array = array.as_list::<i32>();
        for inner_array in list_array.iter() {
            let Some(inner_array) = inner_array else {
                return internal_err!(
                    "Intermediate results of COUNT DISTINCT should always be non null"
                );
            };
            self.update_batch(&[inner_array])?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.values.len() as i64)))
    }

    fn size(&self) -> usize {
        match &self.state_data_type {
            DataType::Boolean | DataType::Null => self.fixed_size(),
            d if d.is_primitive() => self.fixed_size(),
            _ => self.full_size(),
        }
    }
}