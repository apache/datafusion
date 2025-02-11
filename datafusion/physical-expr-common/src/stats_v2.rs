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

use std::f64::consts::LN_2;
use std::sync::OnceLock;

use crate::stats_v2::StatisticsV2::{Bernoulli, Exponential, Gaussian, Uniform, Unknown};

use arrow::datatypes::DataType;
use datafusion_common::{internal_err, Result, ScalarValue};
use datafusion_expr_common::interval_arithmetic::Interval;
use datafusion_expr_common::type_coercion::binary::binary_numeric_coercion;

static SCALAR_VALUE_ZERO_LOCK: OnceLock<ScalarValue> = OnceLock::new();
static SCALAR_VALUE_ONE_LOCK: OnceLock<ScalarValue> = OnceLock::new();

/// Returns a `0` as a [`ScalarValue`].
pub fn get_zero() -> &'static ScalarValue {
    SCALAR_VALUE_ZERO_LOCK.get_or_init(|| ScalarValue::Float64(Some(0.)))
}

/// Returns a `1` as a [`ScalarValue`].
pub fn get_one() -> &'static ScalarValue {
    SCALAR_VALUE_ONE_LOCK.get_or_init(|| ScalarValue::Float64(Some(1.)))
}

/// New, enhanced `Statistics` definition, represents five core statistical
/// distributions. New variants will be added over time.
#[derive(Clone, Debug, PartialEq)]
pub enum StatisticsV2 {
    Uniform(UniformDistribution),
    Exponential(ExponentialDistribution),
    Gaussian(GaussianDistribution),
    Bernoulli(BernoulliDistribution),
    Unknown(UnknownDistribution),
}

impl StatisticsV2 {
    /// Constructs a new [`StatisticsV2`] instance with [`Uniform`]
    /// distribution from the given [`Interval`].
    pub fn new_uniform(interval: Interval) -> Result<Self> {
        UniformDistribution::try_new(interval).map(Uniform)
    }

    /// Constructs a new [`StatisticsV2`] instance with [`Exponential`]
    /// distribution from the given rate/offset pair, and checks the newly
    /// created statistic for validity.
    pub fn new_exponential(
        rate: ScalarValue,
        offset: ScalarValue,
        positive_tail: bool,
    ) -> Result<Self> {
        ExponentialDistribution::try_new(rate, offset, positive_tail).map(Exponential)
    }

    /// Constructs a new [`StatisticsV2`] instance with [`Gaussian`]
    /// distribution from the given mean/variance pair, and checks the newly
    /// created statistic for validity.
    pub fn new_gaussian(mean: ScalarValue, variance: ScalarValue) -> Result<Self> {
        GaussianDistribution::try_new(mean, variance).map(Gaussian)
    }

    /// Constructs a new [`StatisticsV2`] instance with [`Bernoulli`]
    /// distribution from the given probability, and checks the newly created
    /// statistic for validity.
    pub fn new_bernoulli(p: ScalarValue) -> Result<Self> {
        BernoulliDistribution::try_new(p).map(Bernoulli)
    }

    /// Constructs a new [`StatisticsV2`] instance with [`Unknown`]
    /// distribution from the given mean, median, variance, and range values.
    /// Then, checks the newly created statistic for validity.
    pub fn new_unknown(
        mean: ScalarValue,
        median: ScalarValue,
        variance: ScalarValue,
        range: Interval,
    ) -> Result<Self> {
        UnknownDistribution::try_new(mean, median, variance, range).map(Unknown)
    }

    /// Constructs a new [`Unknown`] statistics instance from the given range.
    /// Other parameters; mean, median and variance are initialized with null values.
    pub fn new_from_interval(range: &Interval) -> Result<Self> {
        let null = ScalarValue::try_from(range.data_type())?;
        StatisticsV2::new_unknown(null.clone(), null.clone(), null, range.clone())
    }

    /// Extracts the mean value of the given statistic, depending on its distribution:
    /// - A [`Uniform`] distribution's interval determines its mean value, which
    ///   is the arithmetic average of the interval endpoints.
    /// - An [`Exponential`] distribution's mean is calculable by the formula
    ///   `offset + 1 / λ`, where `λ` is the (non-negative) rate.
    /// - A [`Gaussian`] distribution contains the mean explicitly.
    /// - A [`Bernoulli`] distribution's mean is equal to its success probability `p`.
    /// - An [`Unknown`] distribution _may_ have it explicitly, or this information
    ///   may be absent.
    pub fn mean(&self) -> Result<ScalarValue> {
        match &self {
            Uniform(u) => u.mean(),
            Exponential(e) => e.mean(),
            Gaussian(g) => Ok(g.mean().clone()),
            Bernoulli(b) => Ok(b.mean().clone()),
            Unknown(u) => Ok(u.mean().clone()),
        }
    }

    /// Extracts the median value of the given statistic, depending on its distribution:
    /// - A [`Uniform`] distribution's interval determines its median value, which
    ///   is the arithmetic average of the interval endpoints.
    /// - An [`Exponential`] distribution's median is calculable by the formula
    ///   `offset + ln(2) / λ`, where `λ` is the (non-negative) rate.
    /// - A [`Gaussian`] distribution's median is equal to its mean, which is
    ///   specified explicitly.
    /// - A [`Bernoulli`] distribution's median is `1` if `p > 0.5` and `0`
    ///   otherwise, where `p` is the success probability.
    /// - An [`Unknown`] distribution _may_ have it explicitly, or this information
    ///   may be absent.
    pub fn median(&self) -> Result<ScalarValue> {
        match &self {
            Uniform(u) => u.median(),
            Exponential(e) => e.median(),
            Gaussian(g) => Ok(g.median().clone()),
            Bernoulli(b) => b.median(),
            Unknown(u) => Ok(u.median().clone()),
        }
    }

    /// Extracts the variance value of the given statistic, depending on its distribution:
    /// - A [`Uniform`] distribution's interval determines its variance value, which
    ///   is calculable by the formula `(upper - lower) ^ 2 / 12`.
    /// - An [`Exponential`] distribution's variance is calculable by the formula
    ///   `1 / (λ ^ 2)`, where `λ` is the (non-negative) rate.
    /// - A [`Gaussian`] distribution's variance is specified explicitly.
    /// - A [`Bernoulli`] distribution's median is given by the formula `p * (1 - p)`
    ///   where `p` is the success probability.
    /// - An [`Unknown`] distribution _may_ have it explicitly, or this information
    ///   may be absent.
    pub fn variance(&self) -> Result<ScalarValue> {
        match &self {
            Uniform(u) => u.variance(),
            Exponential(e) => e.variance(),
            Gaussian(g) => Ok(g.variance.clone()),
            Bernoulli(b) => b.variance(),
            Unknown(u) => Ok(u.variance.clone()),
        }
    }

    /// Extracts the range of the given statistic, depending on its distribution:
    /// - A [`Uniform`] distribution's range is simply its interval.
    /// - An [`Exponential`] distribution's range is `[offset, +∞)`.
    /// - A [`Gaussian`] distribution's range is unbounded.
    /// - A [`Bernoulli`] distribution's range is [`Interval::UNCERTAIN`], if
    ///   `p` is neither `0` nor `1`. Otherwise, it is [`Interval::CERTAINLY_FALSE`]
    ///   and [`Interval::CERTAINLY_TRUE`], respectively.
    /// - An [`Unknown`] distribution is unbounded by default, but more information
    ///   may be present.
    pub fn range(&self) -> Result<Interval> {
        match &self {
            Uniform(u) => Ok(u.range().clone()),
            Exponential(e) => e.range(),
            Gaussian(g) => g.range(),
            Bernoulli(b) => Ok(b.range()),
            Unknown(u) => Ok(u.range().clone()),
        }
    }

    /// Returns the data type of the statistics.
    pub fn data_type(&self) -> DataType {
        match &self {
            Uniform(u) => u.data_type(),
            Exponential(e) => e.data_type(),
            Gaussian(g) => g.data_type(),
            Bernoulli(_) => DataType::Boolean,
            Unknown(u) => u.data_type(),
        }
    }

    pub fn target_type(args: &[&ScalarValue]) -> Result<DataType> {
        let mut arg_types = args
            .iter()
            .filter(|&&arg| (arg != &ScalarValue::Null))
            .map(|&arg| arg.data_type());

        let Some(dt) = arg_types.next().map_or_else(
            || Some(DataType::Null),
            |first| {
                arg_types
                    .try_fold(first, |target, arg| binary_numeric_coercion(&target, &arg))
            },
        ) else {
            return internal_err!("Can only evaluate statistics for numeric types");
        };
        Ok(dt)
    }
}

/// Uniform distribution, represented by its range. If the given range extends
/// towards infinity, the distribution will be improper -- which is OK. For a
/// more in-depth discussion, see:
///
/// <https://en.wikipedia.org/wiki/Continuous_uniform_distribution>
/// <https://en.wikipedia.org/wiki/Prior_probability#Improper_priors>
#[derive(Clone, Debug, PartialEq)]
pub struct UniformDistribution {
    interval: Interval,
}

/// Exponential distribution with an optional shift. The probability density
/// function (PDF) is defined as follows:
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
/// In both cases, the PDF is `0` outside the specified domain.
///
/// For more information, see:
///
/// <https://en.wikipedia.org/wiki/Exponential_distribution>
#[derive(Clone, Debug, PartialEq)]
pub struct ExponentialDistribution {
    rate: ScalarValue,
    offset: ScalarValue,
    /// Indicates whether the exponential distribution has a positive tail;
    /// i.e. it extends towards positive infinity.
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

/// Bernoulli distribution with success probability `p`. If `p` has a null value,
/// the success probability is unknown. For a more in-depth discussion, see:
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
    fn try_new(interval: Interval) -> Result<Self> {
        if interval.data_type().eq(&DataType::Boolean) {
            return internal_err!(
                "Construction of a boolean `Uniform` statistic is prohibited, create a `Bernoulli` statistic instead."
            );
        }

        Ok(Self { interval })
    }

    pub fn data_type(&self) -> DataType {
        self.interval.data_type()
    }

    pub fn mean(&self) -> Result<ScalarValue> {
        let dt = self.data_type();
        let two = ScalarValue::from(2).cast_to(&dt)?;
        self.interval
            .lower()
            .add_checked(self.interval.upper())?
            .div(two)
    }

    pub fn median(&self) -> Result<ScalarValue> {
        self.mean()
    }

    pub fn variance(&self) -> Result<ScalarValue> {
        let width = self.interval.width()?;
        let dt = width.data_type();
        let twelve = ScalarValue::from(12).cast_to(&dt)?;
        width.mul_checked(&width)?.div(twelve)
    }

    pub fn range(&self) -> &Interval {
        &self.interval
    }
}

impl ExponentialDistribution {
    fn try_new(
        rate: ScalarValue,
        offset: ScalarValue,
        positive_tail: bool,
    ) -> Result<Self> {
        let dt = rate.data_type();
        if offset.data_type() != dt {
            internal_err!("Rate and offset must have the same data type")
        } else if offset.is_null() {
            internal_err!("Offset of an `ExponentialDistribution` cannot be null")
        } else if rate.is_null() {
            internal_err!("Rate of an `ExponentialDistribution` cannot be null")
        } else if rate.le(&ScalarValue::new_zero(&dt)?) {
            internal_err!("Rate of an `ExponentialDistribution` must be positive")
        } else {
            Ok(Self {
                rate,
                offset,
                positive_tail,
            })
        }
    }

    pub fn data_type(&self) -> DataType {
        self.rate.data_type()
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
        let one = ScalarValue::new_one(&self.data_type())?;
        let tail_mean = one.div(&self.rate)?;
        if self.positive_tail {
            self.offset.add_checked(tail_mean)
        } else {
            self.offset.sub_checked(tail_mean)
        }
    }

    pub fn median(&self) -> Result<ScalarValue> {
        let ln_two = ScalarValue::from(LN_2).cast_to(&self.data_type())?;
        let tail_median = ln_two.div(&self.rate)?;
        if self.positive_tail {
            self.offset.add_checked(tail_median)
        } else {
            self.offset.sub_checked(tail_median)
        }
    }

    pub fn variance(&self) -> Result<ScalarValue> {
        let one = ScalarValue::new_one(&self.data_type())?;
        let rate_squared = self.rate.mul_checked(&self.rate)?;
        one.div(rate_squared)
    }

    pub fn range(&self) -> Result<Interval> {
        let end = ScalarValue::try_from(&self.data_type())?;
        if self.positive_tail {
            Interval::try_new(self.offset.clone(), end)
        } else {
            Interval::try_new(end, self.offset.clone())
        }
    }
}

impl GaussianDistribution {
    fn try_new(mean: ScalarValue, variance: ScalarValue) -> Result<Self> {
        let dt = mean.data_type();
        if variance.data_type() != dt {
            internal_err!("Mean and variance must have the same data type")
        } else if variance.is_null() {
            internal_err!("Variance of a `GaussianDistribution` cannot be null")
        } else if variance.lt(&ScalarValue::new_zero(&dt)?) {
            internal_err!("Variance of a `GaussianDistribution` must be positive")
        } else {
            Ok(Self { mean, variance })
        }
    }

    pub fn data_type(&self) -> DataType {
        self.mean.data_type()
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
        Interval::make_unbounded(&self.data_type())
    }
}

impl BernoulliDistribution {
    fn try_new(p: ScalarValue) -> Result<Self> {
        if p.is_null() {
            Ok(Self { p })
        } else {
            let dt = p.data_type();
            let zero = ScalarValue::new_zero(&dt)?;
            let one = ScalarValue::new_one(&dt)?;
            if p.ge(&zero) && p.le(&one) {
                Ok(Self { p })
            } else {
                internal_err!(
                    "Success probability of a `BernoulliDistribution` must be in [0, 1]"
                )
            }
        }
    }

    pub fn p_value(&self) -> &ScalarValue {
        &self.p
    }

    pub fn mean(&self) -> &ScalarValue {
        &self.p
    }

    pub fn median(&self) -> Result<ScalarValue> {
        let dt = self.p.data_type();
        let one = ScalarValue::new_one(&dt)?;
        if one.sub_checked(&self.p)?.lt(&self.p) {
            ScalarValue::new_one(&dt)
        } else {
            ScalarValue::new_zero(&dt)
        }
    }

    pub fn variance(&self) -> Result<ScalarValue> {
        let dt = self.p.data_type();
        let one = ScalarValue::new_one(&dt)?;
        one.sub_checked(&self.p)?.mul_checked(&self.p)
    }

    pub fn range(&self) -> Interval {
        let dt = self.p.data_type();
        // Unwraps are safe as the constructor guarantees that the data type
        // supports zero and one values.
        if ScalarValue::new_zero(&dt).unwrap().eq(&self.p) {
            Interval::CERTAINLY_FALSE
        } else if ScalarValue::new_one(&dt).unwrap().eq(&self.p) {
            Interval::CERTAINLY_TRUE
        } else {
            Interval::UNCERTAIN
        }
    }
}

impl UnknownDistribution {
    fn try_new(
        mean: ScalarValue,
        median: ScalarValue,
        variance: ScalarValue,
        range: Interval,
    ) -> Result<Self> {
        let validate_location = |m: &ScalarValue| -> Result<bool> {
            // Checks whether the given location estimate is within the range.
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
        } else if !validate_location(&mean)?
            || !validate_location(&median)?
            || (!variance.is_null()
                && variance.lt(&ScalarValue::new_zero(&variance.data_type())?))
        {
            internal_err!("Tried to construct an invalid `UnknownDistribution` instance")
        } else {
            Ok(Self {
                mean,
                median,
                variance,
                range,
            })
        }
    }

    pub fn data_type(&self) -> DataType {
        self.mean.data_type()
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
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr_common::interval_arithmetic::Interval;

    // The test data in the following tests are placed as follows: (stat -> expected answer)
    #[test]
    fn uniform_stats_is_valid_test() -> Result<()> {
        assert_eq!(
            StatisticsV2::new_uniform(Interval::make_zero(&DataType::Int8)?)?,
            StatisticsV2::Uniform(UniformDistribution {
                interval: Interval::make_zero(&DataType::Int8)?,
            })
        );

        assert!(StatisticsV2::new_uniform(Interval::UNCERTAIN).is_err());
        Ok(())
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
            (
                StatisticsV2::new_bernoulli(ScalarValue::Int64(Some(0))),
                true,
            ),
            (
                StatisticsV2::new_bernoulli(ScalarValue::Int64(Some(1))),
                true,
            ),
            (
                StatisticsV2::new_bernoulli(ScalarValue::Int64(Some(11))),
                false,
            ),
            (
                StatisticsV2::new_bernoulli(ScalarValue::Int64(Some(-11))),
                false,
            ),
        ];
        for case in gaussian_stats {
            assert_eq!(case.0.is_ok(), case.1);
        }
    }

    #[test]
    fn unknown_stats_is_valid_test() -> Result<()> {
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
                    Interval::make_zero(&DataType::Float32)?,
                ),
                true,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Float32(Some(0.)),
                    ScalarValue::Float32(None),
                    ScalarValue::Null,
                    Interval::make_zero(&DataType::Float32)?,
                ),
                true,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Null,
                    ScalarValue::Float32(Some(0.)),
                    ScalarValue::Float64(None),
                    Interval::make_zero(&DataType::Float32)?,
                ),
                true,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Float32(Some(-10.)),
                    ScalarValue::Null,
                    ScalarValue::Null,
                    Interval::make_zero(&DataType::Float32)?,
                ),
                false,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Null,
                    ScalarValue::Float32(Some(10.)),
                    ScalarValue::Null,
                    Interval::make_zero(&DataType::Float32)?,
                ),
                false,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Null,
                    ScalarValue::Null,
                    ScalarValue::Null,
                    Interval::make_zero(&DataType::Float32)?,
                ),
                true,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Int32(Some(0)),
                    ScalarValue::Int32(Some(0)),
                    ScalarValue::Null,
                    Interval::make_zero(&DataType::Int32)?,
                ),
                true,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Float32(Some(0.)),
                    ScalarValue::Float32(Some(0.)),
                    ScalarValue::Null,
                    Interval::make_zero(&DataType::Float32)?,
                ),
                true,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Float64(Some(50.)),
                    ScalarValue::Float64(Some(50.)),
                    ScalarValue::Null,
                    Interval::make(Some(0.), Some(100.))?,
                ),
                true,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Float64(Some(50.)),
                    ScalarValue::Float64(Some(50.)),
                    ScalarValue::Null,
                    Interval::make(Some(-100.), Some(0.))?,
                ),
                false,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Null,
                    ScalarValue::Null,
                    ScalarValue::Float64(Some(1.)),
                    Interval::make_zero(&DataType::Float64)?,
                ),
                true,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Null,
                    ScalarValue::Null,
                    ScalarValue::Float64(Some(-1.)),
                    Interval::make_zero(&DataType::Float64)?,
                ),
                false,
            ),
        ];
        for case in unknown_stats {
            assert_eq!(case.0.is_ok(), case.1, "{:?}", case.0);
        }

        Ok(())
    }

    #[test]
    fn mean_extraction_test() -> Result<()> {
        // The test data is placed as follows : (stat -> expected answer)
        let stats = vec![
            (
                StatisticsV2::new_uniform(Interval::make_zero(&DataType::Int64)?),
                ScalarValue::Int64(Some(0)),
            ),
            (
                StatisticsV2::new_uniform(Interval::make_zero(&DataType::Float64)?),
                ScalarValue::Float64(Some(0.)),
            ),
            (
                StatisticsV2::new_uniform(Interval::make(Some(1), Some(100))?),
                ScalarValue::Int32(Some(50)),
            ),
            (
                StatisticsV2::new_uniform(Interval::make(Some(-100), Some(-1))?),
                ScalarValue::Int32(Some(-50)),
            ),
            (
                StatisticsV2::new_uniform(Interval::make(Some(-100), Some(100))?),
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
                    Interval::make(Some(25.), Some(50.))?,
                ),
                ScalarValue::Float64(Some(42.)),
            ),
        ];

        for case in stats {
            assert_eq!(case.0?.mean()?, case.1);
        }

        Ok(())
    }

    #[test]
    fn median_extraction_test() -> Result<()> {
        // The test data is placed as follows: (stat -> expected answer)
        let stats = vec![
            (
                StatisticsV2::new_uniform(Interval::make_zero(&DataType::Int64)?),
                ScalarValue::Int64(Some(0)),
            ),
            (
                StatisticsV2::new_uniform(Interval::make(Some(25.), Some(75.))?),
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
                    Interval::make(Some(0.), Some(25.))?,
                ),
                ScalarValue::Float64(Some(12.)),
            ),
        ];

        for case in stats {
            assert_eq!(case.0?.median()?, case.1);
        }

        Ok(())
    }

    #[test]
    fn variance_extraction_test() -> Result<()> {
        // The test data is placed as follows : (stat -> expected answer)
        let stats = vec![
            (
                StatisticsV2::new_uniform(Interval::make(Some(0.), Some(12.))?),
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
                    Interval::make_zero(&DataType::Float64)?,
                ),
                ScalarValue::Float64(Some(0.02)),
            ),
        ];

        for case in stats {
            assert_eq!(case.0?.variance()?, case.1);
        }

        Ok(())
    }
}
