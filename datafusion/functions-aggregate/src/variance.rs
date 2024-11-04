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

//! [`VarianceSample`]: variance sample aggregations.
//! [`VariancePopulation`]: variance population aggregations.

use arrow::{
    array::{Array, ArrayRef, BooleanArray, Float64Array, UInt64Array},
    buffer::NullBuffer,
    compute::kernels::cast,
    datatypes::{DataType, Field},
};
use std::mem::{size_of, size_of_val};
use std::sync::OnceLock;
use std::{fmt::Debug, sync::Arc};

use datafusion_common::{
    downcast_value, not_impl_err, plan_err, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::aggregate_doc_sections::DOC_SECTION_GENERAL;
use datafusion_expr::{
    function::{AccumulatorArgs, StateFieldsArgs},
    utils::format_state_name,
    Accumulator, AggregateUDFImpl, Documentation, GroupsAccumulator, Signature,
    Volatility,
};
use datafusion_functions_aggregate_common::{
    aggregate::groups_accumulator::accumulate::accumulate, stats::StatsType,
};

make_udaf_expr_and_func!(
    VarianceSample,
    var_sample,
    expression,
    "Computes the sample variance.",
    var_samp_udaf
);

make_udaf_expr_and_func!(
    VariancePopulation,
    var_pop,
    expression,
    "Computes the population variance.",
    var_pop_udaf
);

pub struct VarianceSample {
    signature: Signature,
    aliases: Vec<String>,
}

impl Debug for VarianceSample {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("VarianceSample")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for VarianceSample {
    fn default() -> Self {
        Self::new()
    }
}

impl VarianceSample {
    pub fn new() -> Self {
        Self {
            aliases: vec![String::from("var_sample"), String::from("var_samp")],
            signature: Signature::coercible(
                vec![DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for VarianceSample {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "var"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        let name = args.name;
        Ok(vec![
            Field::new(format_state_name(name, "count"), DataType::UInt64, true),
            Field::new(format_state_name(name, "mean"), DataType::Float64, true),
            Field::new(format_state_name(name, "m2"), DataType::Float64, true),
        ])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if acc_args.is_distinct {
            return not_impl_err!("VAR(DISTINCT) aggregations are not available");
        }

        Ok(Box::new(VarianceAccumulator::try_new(StatsType::Sample)?))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn groups_accumulator_supported(&self, acc_args: AccumulatorArgs) -> bool {
        !acc_args.is_distinct
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(VarianceGroupsAccumulator::new(StatsType::Sample)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_variance_sample_doc())
    }
}

static VARIANCE_SAMPLE_DOC: OnceLock<Documentation> = OnceLock::new();

fn get_variance_sample_doc() -> &'static Documentation {
    VARIANCE_SAMPLE_DOC.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_GENERAL)
            .with_description(
                "Returns the statistical sample variance of a set of numbers.",
            )
            .with_syntax_example("var(expression)")
            .with_standard_argument("expression", Some("Numeric"))
            .build()
            .unwrap()
    })
}

pub struct VariancePopulation {
    signature: Signature,
    aliases: Vec<String>,
}

impl Debug for VariancePopulation {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("VariancePopulation")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for VariancePopulation {
    fn default() -> Self {
        Self::new()
    }
}

impl VariancePopulation {
    pub fn new() -> Self {
        Self {
            aliases: vec![String::from("var_population")],
            signature: Signature::numeric(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for VariancePopulation {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "var_pop"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if !arg_types[0].is_numeric() {
            return plan_err!("Variance requires numeric input types");
        }

        Ok(DataType::Float64)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        let name = args.name;
        Ok(vec![
            Field::new(format_state_name(name, "count"), DataType::UInt64, true),
            Field::new(format_state_name(name, "mean"), DataType::Float64, true),
            Field::new(format_state_name(name, "m2"), DataType::Float64, true),
        ])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if acc_args.is_distinct {
            return not_impl_err!("VAR_POP(DISTINCT) aggregations are not available");
        }

        Ok(Box::new(VarianceAccumulator::try_new(
            StatsType::Population,
        )?))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn groups_accumulator_supported(&self, acc_args: AccumulatorArgs) -> bool {
        !acc_args.is_distinct
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(VarianceGroupsAccumulator::new(
            StatsType::Population,
        )))
    }
    fn documentation(&self) -> Option<&Documentation> {
        Some(get_variance_population_doc())
    }
}

static VARIANCE_POPULATION_DOC: OnceLock<Documentation> = OnceLock::new();

fn get_variance_population_doc() -> &'static Documentation {
    VARIANCE_POPULATION_DOC.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_GENERAL)
            .with_description(
                "Returns the statistical population variance of a set of numbers.",
            )
            .with_syntax_example("var_pop(expression)")
            .with_standard_argument("expression", Some("Numeric"))
            .build()
            .unwrap()
    })
}

/// An accumulator to compute variance
/// The algorithm used is an online implementation and numerically stable. It is based on this paper:
/// Welford, B. P. (1962). "Note on a method for calculating corrected sums of squares and products".
/// Technometrics. 4 (3): 419–420. doi:10.2307/1266577. JSTOR 1266577.
///
/// The algorithm has been analyzed here:
/// Ling, Robert F. (1974). "Comparison of Several Algorithms for Computing Sample Means and Variances".
/// Journal of the American Statistical Association. 69 (348): 859–866. doi:10.2307/2286154. JSTOR 2286154.

#[derive(Debug)]
pub struct VarianceAccumulator {
    m2: f64,
    mean: f64,
    count: u64,
    stats_type: StatsType,
}

impl VarianceAccumulator {
    /// Creates a new `VarianceAccumulator`
    pub fn try_new(s_type: StatsType) -> Result<Self> {
        Ok(Self {
            m2: 0_f64,
            mean: 0_f64,
            count: 0_u64,
            stats_type: s_type,
        })
    }

    pub fn get_count(&self) -> u64 {
        self.count
    }

    pub fn get_mean(&self) -> f64 {
        self.mean
    }

    pub fn get_m2(&self) -> f64 {
        self.m2
    }
}

#[inline]
fn merge(
    count: u64,
    mean: f64,
    m2: f64,
    count2: u64,
    mean2: f64,
    m22: f64,
) -> (u64, f64, f64) {
    let new_count = count + count2;
    let new_mean =
        mean * count as f64 / new_count as f64 + mean2 * count2 as f64 / new_count as f64;
    let delta = mean - mean2;
    let new_m2 =
        m2 + m22 + delta * delta * count as f64 * count2 as f64 / new_count as f64;

    (new_count, new_mean, new_m2)
}

#[inline]
fn update(count: u64, mean: f64, m2: f64, value: f64) -> (u64, f64, f64) {
    let new_count = count + 1;
    let delta1 = value - mean;
    let new_mean = delta1 / new_count as f64 + mean;
    let delta2 = value - new_mean;
    let new_m2 = m2 + delta1 * delta2;

    (new_count, new_mean, new_m2)
}

impl Accumulator for VarianceAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.count),
            ScalarValue::from(self.mean),
            ScalarValue::from(self.m2),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &cast(&values[0], &DataType::Float64)?;
        let arr = downcast_value!(values, Float64Array).iter().flatten();

        for value in arr {
            (self.count, self.mean, self.m2) =
                update(self.count, self.mean, self.m2, value)
        }

        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &cast(&values[0], &DataType::Float64)?;
        let arr = downcast_value!(values, Float64Array).iter().flatten();

        for value in arr {
            let new_count = self.count - 1;
            let delta1 = self.mean - value;
            let new_mean = delta1 / new_count as f64 + self.mean;
            let delta2 = new_mean - value;
            let new_m2 = self.m2 - delta1 * delta2;

            self.count -= 1;
            self.mean = new_mean;
            self.m2 = new_m2;
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let counts = downcast_value!(states[0], UInt64Array);
        let means = downcast_value!(states[1], Float64Array);
        let m2s = downcast_value!(states[2], Float64Array);

        for i in 0..counts.len() {
            let c = counts.value(i);
            if c == 0_u64 {
                continue;
            }
            (self.count, self.mean, self.m2) = merge(
                self.count,
                self.mean,
                self.m2,
                c,
                means.value(i),
                m2s.value(i),
            )
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let count = match self.stats_type {
            StatsType::Population => self.count,
            StatsType::Sample => {
                if self.count > 0 {
                    self.count - 1
                } else {
                    self.count
                }
            }
        };

        Ok(ScalarValue::Float64(match self.count {
            0 => None,
            1 => {
                if let StatsType::Population = self.stats_type {
                    Some(0.0)
                } else {
                    None
                }
            }
            _ => Some(self.m2 / count as f64),
        }))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

#[derive(Debug)]
pub struct VarianceGroupsAccumulator {
    m2s: Vec<f64>,
    means: Vec<f64>,
    counts: Vec<u64>,
    stats_type: StatsType,
}

impl VarianceGroupsAccumulator {
    pub fn new(s_type: StatsType) -> Self {
        Self {
            m2s: Vec::new(),
            means: Vec::new(),
            counts: Vec::new(),
            stats_type: s_type,
        }
    }

    fn resize(&mut self, total_num_groups: usize) {
        self.m2s.resize(total_num_groups, 0.0);
        self.means.resize(total_num_groups, 0.0);
        self.counts.resize(total_num_groups, 0);
    }

    fn merge<F>(
        group_indices: &[usize],
        counts: &UInt64Array,
        means: &Float64Array,
        m2s: &Float64Array,
        opt_filter: Option<&BooleanArray>,
        mut value_fn: F,
    ) where
        F: FnMut(usize, u64, f64, f64) + Send,
    {
        assert_eq!(counts.null_count(), 0);
        assert_eq!(means.null_count(), 0);
        assert_eq!(m2s.null_count(), 0);

        match opt_filter {
            None => {
                group_indices
                    .iter()
                    .zip(counts.values().iter())
                    .zip(means.values().iter())
                    .zip(m2s.values().iter())
                    .for_each(|(((&group_index, &count), &mean), &m2)| {
                        value_fn(group_index, count, mean, m2);
                    });
            }
            Some(filter) => {
                group_indices
                    .iter()
                    .zip(counts.values().iter())
                    .zip(means.values().iter())
                    .zip(m2s.values().iter())
                    .zip(filter.iter())
                    .for_each(
                        |((((&group_index, &count), &mean), &m2), filter_value)| {
                            if let Some(true) = filter_value {
                                value_fn(group_index, count, mean, m2);
                            }
                        },
                    );
            }
        }
    }

    pub fn variance(
        &mut self,
        emit_to: datafusion_expr::EmitTo,
    ) -> (Vec<f64>, NullBuffer) {
        let mut counts = emit_to.take_needed(&mut self.counts);
        // means are only needed for updating m2s and are not needed for the final result.
        // But we still need to take them to ensure the internal state is consistent.
        let _ = emit_to.take_needed(&mut self.means);
        let m2s = emit_to.take_needed(&mut self.m2s);

        if let StatsType::Sample = self.stats_type {
            counts.iter_mut().for_each(|count| {
                *count = count.saturating_sub(1);
            });
        }
        let nulls = NullBuffer::from_iter(counts.iter().map(|&count| count != 0));
        let variance = m2s
            .iter()
            .zip(counts)
            .map(|(m2, count)| m2 / count as f64)
            .collect();
        (variance, nulls)
    }
}

impl GroupsAccumulator for VarianceGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = &cast(&values[0], &DataType::Float64)?;
        let values = downcast_value!(values, Float64Array);

        self.resize(total_num_groups);
        accumulate(group_indices, values, opt_filter, |group_index, value| {
            let (new_count, new_mean, new_m2) = update(
                self.counts[group_index],
                self.means[group_index],
                self.m2s[group_index],
                value,
            );
            self.counts[group_index] = new_count;
            self.means[group_index] = new_mean;
            self.m2s[group_index] = new_m2;
        });
        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 3, "two arguments to merge_batch");
        // first batch is counts, second is partial means, third is partial m2s
        let partial_counts = downcast_value!(values[0], UInt64Array);
        let partial_means = downcast_value!(values[1], Float64Array);
        let partial_m2s = downcast_value!(values[2], Float64Array);

        self.resize(total_num_groups);
        Self::merge(
            group_indices,
            partial_counts,
            partial_means,
            partial_m2s,
            opt_filter,
            |group_index, partial_count, partial_mean, partial_m2| {
                let (new_count, new_mean, new_m2) = merge(
                    self.counts[group_index],
                    self.means[group_index],
                    self.m2s[group_index],
                    partial_count,
                    partial_mean,
                    partial_m2,
                );
                self.counts[group_index] = new_count;
                self.means[group_index] = new_mean;
                self.m2s[group_index] = new_m2;
            },
        );
        Ok(())
    }

    fn evaluate(&mut self, emit_to: datafusion_expr::EmitTo) -> Result<ArrayRef> {
        let (variances, nulls) = self.variance(emit_to);
        Ok(Arc::new(Float64Array::new(variances.into(), Some(nulls))))
    }

    fn state(&mut self, emit_to: datafusion_expr::EmitTo) -> Result<Vec<ArrayRef>> {
        let counts = emit_to.take_needed(&mut self.counts);
        let means = emit_to.take_needed(&mut self.means);
        let m2s = emit_to.take_needed(&mut self.m2s);

        Ok(vec![
            Arc::new(UInt64Array::new(counts.into(), None)),
            Arc::new(Float64Array::new(means.into(), None)),
            Arc::new(Float64Array::new(m2s.into(), None)),
        ])
    }

    fn size(&self) -> usize {
        self.m2s.capacity() * size_of::<f64>()
            + self.means.capacity() * size_of::<f64>()
            + self.counts.capacity() * size_of::<u64>()
    }
}
