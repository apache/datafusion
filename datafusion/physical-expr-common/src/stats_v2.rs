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

use std::sync::OnceLock;

use crate::stats_v2::StatisticsV2::{Bernoulli, Exponential, Gaussian, Uniform, Unknown};

use arrow::datatypes::DataType;
use datafusion_common::{internal_err, DataFusionError, Result, ScalarValue};
use datafusion_expr_common::interval_arithmetic::Interval;
use datafusion_expr_common::type_coercion::binary::binary_numeric_coercion;

static SCALAR_VALUE_ZERO_LOCK: OnceLock<ScalarValue> = OnceLock::new();
static SCALAR_VALUE_ONE_LOCK: OnceLock<ScalarValue> = OnceLock::new();
static LN_TWO_LOCK: OnceLock<ScalarValue> = OnceLock::new();

/// Returns a `0` as a [`ScalarValue`]
pub fn get_zero() -> &'static ScalarValue {
    SCALAR_VALUE_ZERO_LOCK.get_or_init(|| ScalarValue::Float64(Some(0.)))
}

/// Returns a `1` as a [`ScalarValue`]
pub fn get_one() -> &'static ScalarValue {
    SCALAR_VALUE_ONE_LOCK.get_or_init(|| ScalarValue::Float64(Some(1.)))
}

/// Returns a ln(2) as a [`ScalarValue`]
pub fn get_ln_two() -> &'static ScalarValue {
    LN_TWO_LOCK.get_or_init(|| ScalarValue::Float64(Some(2_f64.ln())))
}

/// New, enhanced `Statistics` definition, represents five core statistical distributions.
/// New variants will be added over time.
#[derive(Clone, Debug, PartialEq)]
pub enum StatisticsV2 {
    Uniform(UniformDistribution),
    Exponential(ExponentialDistribution),
    Gaussian(GaussianDistribution),
    Bernoulli(BernoulliDistribution),
    Unknown(UnknownDistribution),
}

impl StatisticsV2 {
    /// Constructs a new [`StatisticsV2`] with [`Uniform`] distribution from the given [`Interval`].
    pub fn new_uniform(interval: Interval) -> Result<Self> {
        UniformDistribution::new(interval).map(Uniform)
    }

    /// Constructs a new [`StatisticsV2`] with [`Exponential`] distribution from the given
    /// rate/offset pair, and checks the newly created statistic for validity.
    pub fn new_exponential(
        rate: ScalarValue,
        offset: ScalarValue,
        positive_tail: bool,
    ) -> Result<Self> {
        ExponentialDistribution::new(rate, offset, positive_tail).map(Exponential)
    }

    /// Constructs a new [`StatisticsV2`] with [`Gaussian`] distribution from the given
    /// mean/variance pair, and checks the newly created statistic for validity.
    pub fn new_gaussian(mean: ScalarValue, variance: ScalarValue) -> Result<Self> {
        GaussianDistribution::new(mean, variance).map(Gaussian)
    }

    /// Constructs a new [`StatisticsV2`] with [`Bernoulli`] distribution from the given probability,
    /// and checks the newly created statistic for validity.
    pub fn new_bernoulli(p: ScalarValue) -> Result<Self> {
        BernoulliDistribution::new(p).map(Bernoulli)
    }

    /// Constructs a new [`StatisticsV2`] with [`Unknown`] distribution from the given mean,
    /// median, variance, and range values. Then, checks the newly created statistic for validity.
    pub fn new_unknown(
        mean: ScalarValue,
        median: ScalarValue,
        variance: ScalarValue,
        range: Interval,
    ) -> Result<Self> {
        UnknownDistribution::new(mean, median, variance, range).map(Unknown)
    }

    /// Constructs a new [`Unknown`] statistics instance from the given range.
    /// Other parameters; mean, median and variance are initialized with null values.
    pub fn new_from_interval(range: &Interval) -> Result<Self> {
        let null = ScalarValue::try_from(range.data_type())?;
        StatisticsV2::new_unknown(null.clone(), null.clone(), null, range.clone())
    }

    /// Extracts the mean value of the given statistic, depending on its distribution:
    /// - A [`Uniform`] distribution's interval determines its mean value, which is the
    ///   arithmetic average of the interval endpoints.
    /// - An [`Exponential`] distribution's mean is calculable by formula `offset + 1/λ`,
    ///   where λ is the (non-negative) rate.
    /// - A [`Gaussian`] distribution contains the mean explicitly.
    /// - A [`Bernoulli`] distribution's mean is equal to its success probability `p`.
    /// - An [`Unknown`] distribution _may_ have it explicitly, or this information may
    ///   be absent.
    pub fn mean(&self) -> Result<ScalarValue> {
        match &self {
            Uniform(uni) => uni.mean(),
            Exponential(e) => e.mean(),
            Gaussian(g) => Ok(g.mean().clone()),
            Bernoulli(b) => Ok(b.mean().clone()),
            Unknown(unk) => Ok(unk.mean().clone()),
        }
    }

    /// Extracts the median value of the given statistic, depending on its distribution:
    /// - A [`Uniform`] distribution's interval determines its median value, which is the
    ///   arithmetic average of the interval endpoints.
    /// - An [`Exponential`] distribution's median is calculable by the formula `offset + ln2/λ`,
    ///   where λ is the (non-negative) rate.
    /// - A [`Gaussian`] distribution's median is equal to its mean, which is specified explicitly.
    /// - A [`Bernoulli`] distribution's median is `1` if `p > 0.5` and `0` otherwise.
    /// - An [`Unknown`] distribution _may_ have it explicitly, or this information may
    ///   be absent.
    pub fn median(&self) -> Result<ScalarValue> {
        match &self {
            Uniform(uni) => uni.median(),
            Exponential(e) => e.median(),
            Gaussian(g) => Ok(g.median().clone()),
            Bernoulli(b) => Ok(b.median()),
            Unknown(unk) => Ok(unk.median().clone()),
        }
    }

    /// Extracts the variance value of the given statistic, depending on its distribution:
    /// - A [`Uniform`] distribution's interval determines its variance value, which is calculable
    ///   by the formula `(upper - lower) ^ 2 / 12`.
    /// - An [`Exponential`] distribution's variance is calculable by the formula `1/(λ ^ 2)`,
    ///   where λ is the (non-negative) rate.
    /// - A [`Gaussian`] distribution's variance is specified explicitly.
    /// - A [`Bernoulli`] distribution's median is given by the formula `p * (1 - p)` where `p`
    ///   is the success probability.
    /// - An [`Unknown`] distribution _may_ have it explicitly, or this information may
    ///   be absent.
    pub fn variance(&self) -> Result<ScalarValue> {
        match &self {
            Uniform(uni) => uni.variance(),
            Exponential(e) => e.variance(),
            Gaussian(g) => Ok(g.variance.clone()),
            Bernoulli(b) => b.variance(),
            Unknown(unk) => Ok(unk.variance.clone()),
        }
    }

    /// Extracts the range of the given statistic, depending on its distribution:
    /// - A [`Uniform`] distribution's range is simply its interval.
    /// - An [`Exponential`] distribution's range is `[offset, +∞)`.
    /// - A [`Gaussian`] distribution's range is unbounded.
    /// - A [`Bernoulli`] distribution's range is [`Interval::UNCERTAIN`], if `p` is neither `0` nor `1`.
    ///   Otherwise, it is [`Interval::CERTAINLY_FALSE`] and [`Interval::CERTAINLY_TRUE`], respectively.
    /// - An [`Unknown`] distribution is unbounded by default, but more information may be present.
    pub fn range(&self) -> Result<Interval> {
        match &self {
            Uniform(uni) => Ok(uni.range().clone()),
            Exponential(e) => e.range(),
            Gaussian(g) => g.range(),
            Bernoulli(b) => Ok(b.range()),
            Unknown(unk) => Ok(unk.range().clone()),
        }
    }

    /// Returns the data type of the statistics.
    pub fn data_type(&self) -> DataType {
        match &self {
            Uniform(u) => u.interval.data_type(),
            Exponential(e) => e.offset.data_type(),
            Gaussian(g) => g.mean.data_type(),
            Bernoulli(_) => DataType::Boolean,
            Unknown(c) => c.range.data_type(),
        }
    }

    pub fn target_type(args: &[&ScalarValue]) -> Result<DataType> {
        let mut typed_args = args.iter().filter_map(|&arg| {
            if arg == &ScalarValue::Null {
                None
            } else {
                Some(arg.data_type())
            }
        });

        typed_args
            .next()
            .map_or(Some(DataType::Null), |first| {
                typed_args
                    .try_fold(first, |target, arg| binary_numeric_coercion(&target, &arg))
            })
            .ok_or(DataFusionError::Internal(
                "Statistics can only be evaluated for numeric types".to_string(),
            ))
    }
}

/// Uniform distribution, represented by its range. If the given range extends towards infinity,
/// the distribution will be improper -- which is OK. For a more in-depth discussion, see:
///
/// <https://en.wikipedia.org/wiki/Continuous_uniform_distribution>
/// <https://en.wikipedia.org/wiki/Prior_probability#Improper_priors>
#[derive(Clone, Debug, PartialEq)]
pub struct UniformDistribution {
    interval: Interval,
}

/// Exponential distribution with an optional shift. The probability density function (PDF)
/// is defined as follows:
///
/// For a positive tail (when `positive_tail` is `true`):
///
/// `f(x; λ, offset) = λ exp(-λ (x - offset))    for x ≥ offset`
///
/// For a negative tail (when `positive_tail` is `false`):
///
/// `f(x; λ, offset) = λ exp(-λ (offset - x))    for x ≤ offset`
///
///
/// In both cases, the PDF is 0 outside the specified domain.
///
/// For more information, see: <https://en.wikipedia.org/wiki/Exponential_distribution>
#[derive(Clone, Debug, PartialEq)]
pub struct ExponentialDistribution {
    rate: ScalarValue,
    offset: ScalarValue,
    /// Indicates whether the exponential distribution has a positive tail; i.e. it extends
    /// towards positive infinity.
    positive_tail: bool,
}

/// Gaussian (normal) distribution, represented by its mean and variance.
/// For a more in-depth discussion, see:
///
/// <https://en.wikipedia.org/wiki/Normal_distribution>
#[derive(Clone, Debug, PartialEq)]
pub struct GaussianDistribution {
    mean: ScalarValue,
    variance: ScalarValue,
}

/// Bernoulli distribution with success probability `p`, which always has the
/// data type [`DataType::Float64`]. If `p` is a null value, the success
/// probability is unknown.
///
/// For a more in-depth discussion, see:
///
/// <https://en.wikipedia.org/wiki/Bernoulli_distribution>
#[derive(Clone, Debug, PartialEq)]
pub struct BernoulliDistribution {
    p: ScalarValue,
}

/// An unknown distribution, only containing some summary statistics.
/// For a more in-depth discussion, see:
///
/// <https://en.wikipedia.org/wiki/Summary_statistics>
#[derive(Clone, Debug, PartialEq)]
pub struct UnknownDistribution {
    mean: ScalarValue,
    median: ScalarValue,
    variance: ScalarValue,
    range: Interval,
}

impl UniformDistribution {
    fn new(interval: Interval) -> Result<Self> {
        if interval.data_type().eq(&DataType::Boolean) {
            return internal_err!(
                "Construction of a boolean `Uniform` statistic is prohibited, create a `Bernoulli` statistic instead."
            );
        }

        Ok(Self { interval })
    }

    pub fn range(&self) -> &Interval {
        &self.interval
    }

    pub fn mean(&self) -> Result<ScalarValue> {
        let dt = self.interval.lower().data_type();
        self.interval
            .lower()
            .add_checked(self.interval.upper())?
            .div(ScalarValue::from(2).cast_to(&dt)?)
    }

    pub fn median(&self) -> Result<ScalarValue> {
        self.mean()
    }

    pub fn variance(&self) -> Result<ScalarValue> {
        let base_value_ref = self
            .interval
            .upper()
            .sub_checked(self.interval.lower())?
            .cast_to(&DataType::Float64)?;
        let base_pow = base_value_ref
            .mul_checked(&base_value_ref)?
            .cast_to(&DataType::Float64)?;
        base_pow.div(ScalarValue::Float64(Some(12.)))
    }
}

impl ExponentialDistribution {
    fn new(rate: ScalarValue, offset: ScalarValue, positive_tail: bool) -> Result<Self> {
        if offset.is_null() {
            internal_err!(
                "Offset argument of the Exponential distribution must be non-null"
            )
        } else if rate.is_null() {
            internal_err!(
                "Rate argument of Exponential Distribution cannot be a null ScalarValue"
            )
        } else if rate.le(&ScalarValue::new_zero(&rate.data_type())?) {
            internal_err!("Tried to construct an invalid Exponential statistic")
        } else {
            Ok(Self {
                rate,
                offset,
                positive_tail,
            })
        }
    }

    pub fn rate(&self) -> &ScalarValue {
        &self.rate
    }

    pub fn offset(&self) -> &ScalarValue {
        &self.offset
    }

    pub fn positive_tail(&self) -> bool {
        self.positive_tail
    }

    pub fn mean(&self) -> Result<ScalarValue> {
        let one = ScalarValue::new_one(&self.offset.data_type())?;
        one.div(&self.rate)?
            .add_checked(&self.offset)
            .map(|abs_mean| {
                if self.positive_tail {
                    Ok(abs_mean)
                } else {
                    abs_mean.arithmetic_negate()
                }
            })?
    }

    pub fn median(&self) -> Result<ScalarValue> {
        let abs_median = get_ln_two().div(&self.rate)?.add_checked(&self.offset)?;
        if self.positive_tail {
            Ok(abs_median)
        } else {
            abs_median.arithmetic_negate()
        }
    }

    pub fn variance(&self) -> Result<ScalarValue> {
        let one = ScalarValue::new_one(&self.rate.data_type())?;
        let rate_squared = self.rate.mul(&self.rate)?;
        one.div(rate_squared)
    }

    pub fn range(&self) -> Result<Interval> {
        let offset_data_type = self.offset.data_type();
        if self.positive_tail {
            Interval::try_new(
                self.offset.clone(),
                ScalarValue::try_from(offset_data_type)?,
            )
        } else {
            Interval::try_new(
                ScalarValue::try_from(offset_data_type)?,
                self.offset.clone(),
            )
        }
    }
}

impl GaussianDistribution {
    fn new(mean: ScalarValue, variance: ScalarValue) -> Result<Self> {
        if variance.is_null() {
            internal_err!(
                "Variance argument of Gaussian Distribution cannot be null ScalarValue"
            )
        } else if variance.lt(&ScalarValue::new_zero(&variance.data_type())?) {
            internal_err!("Tried to construct an invalid Gaussian statistic")
        } else {
            Ok(Self { mean, variance })
        }
    }

    pub fn mean(&self) -> &ScalarValue {
        &self.mean
    }

    pub fn variance(&self) -> &ScalarValue {
        &self.variance
    }

    pub fn median(&self) -> &ScalarValue {
        self.mean()
    }

    pub fn range(&self) -> Result<Interval> {
        Interval::make_unbounded(&self.mean.data_type())
    }
}

impl BernoulliDistribution {
    fn new(p: ScalarValue) -> Result<Self> {
        if p.is_null() {
            Ok(Self { p: p.clone() })
        } else if p.data_type().eq(&DataType::Float64) {
            let zero = ScalarValue::new_zero(&DataType::Float64)?;
            let one = ScalarValue::new_one(&DataType::Float64)?;
            if p.ge(&zero) && p.le(&one) {
                Ok(Self { p: p.clone() })
            } else {
                internal_err!("Tried to construct an invalid Bernoulli statistic")
            }
        } else {
            internal_err!("Bernoulli distribution can hold only float64 probability")
        }
    }

    pub fn p_value(&self) -> &ScalarValue {
        &self.p
    }

    pub fn mean(&self) -> &ScalarValue {
        &self.p
    }

    pub fn median(&self) -> ScalarValue {
        if self.p.gt(&ScalarValue::Float64(Some(0.5))) {
            get_one().clone()
        } else {
            get_zero().clone()
        }
    }

    pub fn variance(&self) -> Result<ScalarValue> {
        let one = ScalarValue::new_one(&DataType::Float64)?;
        one.sub_checked(&self.p)?.mul_checked(&self.p)
    }

    pub fn range(&self) -> Interval {
        if self.p.eq(get_zero()) {
            Interval::CERTAINLY_FALSE
        } else if self.p.eq(get_one()) {
            Interval::CERTAINLY_TRUE
        } else {
            Interval::UNCERTAIN
        }
    }
}

impl UnknownDistribution {
    fn new(
        mean: ScalarValue,
        median: ScalarValue,
        variance: ScalarValue,
        range: Interval,
    ) -> Result<Self> {
        let is_valid_mean_median = |m: &ScalarValue| -> Result<bool> {
            if m.is_null() {
                Ok(true)
            } else {
                range.contains_value(m)
            }
        };

        if range.data_type().eq(&DataType::Boolean) {
            internal_err!(
                "Construction of a boolean `Unknown` statistic is prohibited, create a `Bernoulli` statistic instead."
            )
        } else if (!is_valid_mean_median(&mean)? || !is_valid_mean_median(&median)?)
            || (!variance.is_null()
                && variance.lt(&ScalarValue::new_zero(&variance.data_type())?))
        {
            internal_err!("Tried to construct an invalid Unknown statistic")
        } else {
            Ok(Self {
                mean,
                median,
                variance,
                range,
            })
        }
    }

    pub fn mean(&self) -> &ScalarValue {
        &self.mean
    }

    pub fn median(&self) -> &ScalarValue {
        &self.median
    }

    pub fn variance(&self) -> &ScalarValue {
        &self.variance
    }

    pub fn range(&self) -> &Interval {
        &self.range
    }
}

#[cfg(test)]
mod tests {
    use crate::stats_v2::{StatisticsV2, UniformDistribution};

    use arrow::datatypes::DataType;
    use datafusion_common::ScalarValue;
    use datafusion_expr_common::interval_arithmetic::Interval;

    // The test data in the following tests are placed as follows: (stat -> expected answer)
    #[test]
    fn uniform_stats_is_valid_test() {
        assert_eq!(
            StatisticsV2::new_uniform(Interval::make_zero(&DataType::Int8).unwrap())
                .unwrap(),
            StatisticsV2::Uniform(UniformDistribution {
                interval: Interval::make_zero(&DataType::Int8).unwrap()
            })
        );

        assert!(StatisticsV2::new_uniform(Interval::UNCERTAIN).is_err());
    }

    #[test]
    fn exponential_stats_is_valid_test() {
        let exp_stats = vec![
            (
                StatisticsV2::new_exponential(ScalarValue::Null, ScalarValue::Null, true),
                false,
            ),
            (
                StatisticsV2::new_exponential(
                    ScalarValue::Float32(Some(0.)),
                    ScalarValue::Float32(Some(1.)),
                    true,
                ),
                false,
            ),
            (
                StatisticsV2::new_exponential(
                    ScalarValue::Float32(Some(100.)),
                    ScalarValue::Float32(Some(1.)),
                    true,
                ),
                true,
            ),
            (
                StatisticsV2::new_exponential(
                    ScalarValue::Float32(Some(-100.)),
                    ScalarValue::Float32(Some(1.)),
                    true,
                ),
                false,
            ),
        ];
        for case in exp_stats {
            assert_eq!(case.0.is_ok(), case.1);
        }
    }

    #[test]
    fn gaussian_stats_is_valid_test() {
        let gaussian_stats = vec![
            (
                StatisticsV2::new_gaussian(ScalarValue::Null, ScalarValue::Null),
                false,
            ),
            (
                StatisticsV2::new_gaussian(
                    ScalarValue::Float32(Some(0.)),
                    ScalarValue::Float32(Some(0.)),
                ),
                true,
            ),
            (
                StatisticsV2::new_gaussian(
                    ScalarValue::Float32(Some(0.)),
                    ScalarValue::Float32(Some(0.5)),
                ),
                true,
            ),
            (
                StatisticsV2::new_gaussian(
                    ScalarValue::Float32(Some(0.)),
                    ScalarValue::Float32(Some(-0.5)),
                ),
                false,
            ),
        ];
        for case in gaussian_stats {
            assert_eq!(case.0.is_ok(), case.1);
        }
    }

    #[test]
    fn bernoulli_stats_is_valid_test() {
        let gaussian_stats = vec![
            (StatisticsV2::new_bernoulli(ScalarValue::Null), true),
            (
                StatisticsV2::new_bernoulli(ScalarValue::Float64(Some(0.))),
                true,
            ),
            (
                StatisticsV2::new_bernoulli(ScalarValue::Int64(Some(0))),
                false,
            ),
            (
                StatisticsV2::new_bernoulli(ScalarValue::Float64(Some(0.25))),
                true,
            ),
            (
                StatisticsV2::new_bernoulli(ScalarValue::Float64(Some(1.))),
                true,
            ),
            (
                StatisticsV2::new_bernoulli(ScalarValue::Float64(Some(11.))),
                false,
            ),
            (
                StatisticsV2::new_bernoulli(ScalarValue::Float64(Some(-11.))),
                false,
            ),
        ];
        for case in gaussian_stats {
            assert_eq!(case.0.is_ok(), case.1);
        }
    }

    #[test]
    fn unknown_stats_is_valid_test() {
        let unknown_stats = vec![
            // Usage of boolean range during Unknown statistic construction is prohibited.
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Null,
                    ScalarValue::Null,
                    ScalarValue::Null,
                    Interval::UNCERTAIN,
                ),
                false,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Null,
                    ScalarValue::Null,
                    ScalarValue::Null,
                    Interval::make_zero(&DataType::Float32).unwrap(),
                ),
                true,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Float32(Some(0.)),
                    ScalarValue::Float32(None),
                    ScalarValue::Null,
                    Interval::make_zero(&DataType::Float32).unwrap(),
                ),
                true,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Null,
                    ScalarValue::Float32(Some(0.)),
                    ScalarValue::Float64(None),
                    Interval::make_zero(&DataType::Float32).unwrap(),
                ),
                true,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Float32(Some(-10.)),
                    ScalarValue::Null,
                    ScalarValue::Null,
                    Interval::make_zero(&DataType::Float32).unwrap(),
                ),
                false,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Null,
                    ScalarValue::Float32(Some(10.)),
                    ScalarValue::Null,
                    Interval::make_zero(&DataType::Float32).unwrap(),
                ),
                false,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Null,
                    ScalarValue::Null,
                    ScalarValue::Null,
                    Interval::make_zero(&DataType::Float32).unwrap(),
                ),
                true,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Int32(Some(0)),
                    ScalarValue::Int32(Some(0)),
                    ScalarValue::Null,
                    Interval::make_zero(&DataType::Int32).unwrap(),
                ),
                true,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Float32(Some(0.)),
                    ScalarValue::Float32(Some(0.)),
                    ScalarValue::Null,
                    Interval::make_zero(&DataType::Float32).unwrap(),
                ),
                true,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Float64(Some(50.)),
                    ScalarValue::Float64(Some(50.)),
                    ScalarValue::Null,
                    Interval::make(Some(0.), Some(100.)).unwrap(),
                ),
                true,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Float64(Some(50.)),
                    ScalarValue::Float64(Some(50.)),
                    ScalarValue::Null,
                    Interval::make(Some(-100.), Some(0.)).unwrap(),
                ),
                false,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Null,
                    ScalarValue::Null,
                    ScalarValue::Float64(Some(1.)),
                    Interval::make_zero(&DataType::Float64).unwrap(),
                ),
                true,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Null,
                    ScalarValue::Null,
                    ScalarValue::Float64(Some(-1.)),
                    Interval::make_zero(&DataType::Float64).unwrap(),
                ),
                false,
            ),
        ];
        for case in unknown_stats {
            assert_eq!(case.0.is_ok(), case.1, "{:?}", case.0);
        }
    }

    #[test]
    fn mean_extraction_test() {
        // The test data is placed as follows : (stat -> expected answer)
        let stats = vec![
            (
                StatisticsV2::new_uniform(Interval::make_zero(&DataType::Int64).unwrap()),
                ScalarValue::Int64(Some(0)),
            ),
            (
                StatisticsV2::new_uniform(
                    Interval::make_zero(&DataType::Float64).unwrap(),
                ),
                ScalarValue::Float64(Some(0.)),
            ),
            (
                StatisticsV2::new_uniform(Interval::make(Some(1), Some(100)).unwrap()),
                ScalarValue::Int32(Some(50)),
            ),
            (
                StatisticsV2::new_uniform(Interval::make(Some(-100), Some(-1)).unwrap()),
                ScalarValue::Int32(Some(-50)),
            ),
            (
                StatisticsV2::new_uniform(Interval::make(Some(-100), Some(100)).unwrap()),
                ScalarValue::Int32(Some(0)),
            ),
            (
                StatisticsV2::new_exponential(
                    ScalarValue::Float64(Some(2.)),
                    ScalarValue::Float64(Some(0.)),
                    true,
                ),
                ScalarValue::Float64(Some(0.5)),
            ),
            (
                StatisticsV2::new_exponential(
                    ScalarValue::Float64(Some(2.)),
                    ScalarValue::Float64(Some(1.)),
                    true,
                ),
                ScalarValue::Float64(Some(1.5)),
            ),
            (
                StatisticsV2::new_gaussian(
                    ScalarValue::Float64(Some(0.)),
                    ScalarValue::Float64(Some(1.)),
                ),
                ScalarValue::Float64(Some(0.)),
            ),
            (
                StatisticsV2::new_gaussian(
                    ScalarValue::Float64(Some(-2.)),
                    ScalarValue::Float64(Some(0.5)),
                ),
                ScalarValue::Float64(Some(-2.)),
            ),
            (
                StatisticsV2::new_bernoulli(ScalarValue::Float64(Some(0.5))),
                ScalarValue::Float64(Some(0.5)),
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Float64(Some(42.)),
                    ScalarValue::Float64(Some(42.)),
                    ScalarValue::Null,
                    Interval::make(Some(25.), Some(50.)).unwrap(),
                ),
                ScalarValue::Float64(Some(42.)),
            ),
        ];

        for case in stats {
            assert_eq!(case.0.unwrap().mean().unwrap(), case.1);
        }
    }

    #[test]
    fn median_extraction_test() {
        // The test data is placed as follows: (stat -> expected answer)
        let stats = vec![
            (
                StatisticsV2::new_uniform(Interval::make_zero(&DataType::Int64).unwrap()),
                ScalarValue::Int64(Some(0)),
            ),
            (
                StatisticsV2::new_uniform(Interval::make(Some(25.), Some(75.)).unwrap()),
                ScalarValue::Float64(Some(50.)),
            ),
            (
                StatisticsV2::new_exponential(
                    ScalarValue::Float64(Some(2_f64.ln())),
                    ScalarValue::Float64(Some(0.)),
                    true,
                ),
                ScalarValue::Float64(Some(1.)),
            ),
            (
                StatisticsV2::new_gaussian(
                    ScalarValue::Float64(Some(2.)),
                    ScalarValue::Float64(Some(1.)),
                ),
                ScalarValue::Float64(Some(2.)),
            ),
            (
                StatisticsV2::new_bernoulli(ScalarValue::Float64(Some(0.25))),
                ScalarValue::Float64(Some(0.)),
            ),
            (
                StatisticsV2::new_bernoulli(ScalarValue::Float64(Some(0.75))),
                ScalarValue::Float64(Some(1.)),
            ),
            (
                StatisticsV2::new_gaussian(
                    ScalarValue::Float64(Some(2.)),
                    ScalarValue::Float64(Some(1.)),
                ),
                ScalarValue::Float64(Some(2.)),
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Float64(Some(12.)),
                    ScalarValue::Float64(Some(12.)),
                    ScalarValue::Null,
                    Interval::make(Some(0.), Some(25.)).unwrap(),
                ),
                ScalarValue::Float64(Some(12.)),
            ),
        ];

        for case in stats {
            assert_eq!(case.0.unwrap().median().unwrap(), case.1);
        }
    }

    #[test]
    fn variance_extraction_test() {
        // The test data is placed as follows : (stat -> expected answer)
        let stats = vec![
            (
                StatisticsV2::new_uniform(Interval::make(Some(0.), Some(12.)).unwrap()),
                ScalarValue::Float64(Some(12.)),
            ),
            (
                StatisticsV2::new_exponential(
                    ScalarValue::Float64(Some(10.)),
                    ScalarValue::Float64(Some(0.)),
                    true,
                ),
                ScalarValue::Float64(Some(0.01)),
            ),
            (
                StatisticsV2::new_gaussian(
                    ScalarValue::Float64(Some(0.)),
                    ScalarValue::Float64(Some(1.)),
                ),
                ScalarValue::Float64(Some(1.)),
            ),
            (
                StatisticsV2::new_bernoulli(ScalarValue::Float64(Some(0.5))),
                ScalarValue::Float64(Some(0.25)),
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Null,
                    ScalarValue::Null,
                    ScalarValue::Float64(Some(0.02)),
                    Interval::make_zero(&DataType::Float64).unwrap(),
                ),
                ScalarValue::Float64(Some(0.02)),
            ),
        ];

        for case in stats {
            assert_eq!(case.0.unwrap().variance().unwrap(), case.1);
        }
    }
}
