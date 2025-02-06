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

static LN_TWO_LOCK: OnceLock<ScalarValue> = OnceLock::new();

/// Returns a ln(2) as a [`ScalarValue`]
fn get_ln_two() -> &'static ScalarValue {
    LN_TWO_LOCK.get_or_init(|| ScalarValue::Float64(Some(2_f64.ln())))
}

/// New, enhanced `Statistics` definition, represents five core statistical distributions. New variants will be added over time.
#[derive(Clone, Debug, PartialEq)]
pub enum StatisticsV2 {
    /// Uniform distribution, represented by its range. For a more in-depth discussion, see:
    ///
    /// <https://en.wikipedia.org/wiki/Continuous_uniform_distribution>
    Uniform { interval: Interval },
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
    Exponential {
        rate: ScalarValue,
        offset: ScalarValue,
        /// `true` if the exponential distribution has a positive tail (extending towards positive x),
        /// `false` if it has a negative tail (extending towards negative x).
        positive_tail: bool,
    },
    /// Gaussian (normal) distribution, represented by its mean and variance.
    /// For a more in-depth discussion, see:
    ///
    /// <https://en.wikipedia.org/wiki/Normal_distribution>
    Gaussian {
        mean: ScalarValue,
        variance: ScalarValue,
    },
    /// Bernoulli distribution with success probability `p`, which always has the
    /// data type [`DataType::Float64`]. If `p` is a null value, the success
    /// probability is unknown.
    ///
    /// For a more in-depth discussion, see:
    ///
    /// <https://en.wikipedia.org/wiki/Bernoulli_distribution>
    Bernoulli { p: ScalarValue },
    /// An unknown distribution, only containing some summary statistics.
    /// For a more in-depth discussion, see:
    ///
    /// <https://en.wikipedia.org/wiki/Summary_statistics>
    Unknown {
        mean: ScalarValue,
        median: ScalarValue,
        variance: ScalarValue,
        range: Interval,
    },
}

impl StatisticsV2 {
    /// Constructs a new [`StatisticsV2`] with [`Uniform`] distribution from the given [`Interval`].
    pub fn new_uniform(interval: Interval) -> Result<Self> {
        if interval.data_type().eq(&DataType::Boolean) {
            return internal_err!(
                "Construction of boolean Uniform statistic is prohibited.\
             Create Bernoulli statistic instead."
            );
        }

        Ok(Uniform { interval })
    }

    /// Constructs a new [`StatisticsV2`] with [`Exponential`] distribution from the given
    /// rate/offset pair, and checks the newly created statistic for validity.
    pub fn new_exponential(
        rate: ScalarValue,
        offset: ScalarValue,
        positive_tail: bool,
    ) -> Result<Self> {
        if offset.is_null() {
            internal_err!(
                "Offset argument of the Exponential distribution must be non-null"
            )
        } else if !is_valid_exponential(&rate)? {
            internal_err!("Tried to construct invalid Exponential statistic")
        } else {
            Ok(Exponential {
                rate,
                offset,
                positive_tail,
            })
        }
    }

    /// Constructs a new [`StatisticsV2`] with [`Gaussian`] distribution from the given
    /// mean/variance pair, and checks the newly created statistic for validity.
    pub fn new_gaussian(mean: ScalarValue, variance: ScalarValue) -> Result<Self> {
        is_valid_gaussian(&variance).and_then(|valid| {
            if valid {
                Ok(Gaussian { mean, variance })
            } else {
                internal_err!("Tried to construct invalid Gaussian statistic")
            }
        })
    }

    /// Constructs a new [`StatisticsV2`] with [`Bernoulli`] distribution from the given probability,
    /// and checks the newly created statistic for validity.
    pub fn new_bernoulli(p: ScalarValue) -> Result<Self> {
        is_valid_bernoulli(&p).and_then(|valid| {
            if valid {
                Ok(Bernoulli { p })
            } else {
                internal_err!("Tried to construct invalid Bernoulli statistic")
            }
        })
    }

    /// Constructs a new [`StatisticsV2`] with [`Unknown`] distribution from the given
    /// mean (optional), median (optional), variance (optional), and range values.
    /// Then, checks the newly created statistic for validity.
    pub fn new_unknown(
        mean: ScalarValue,
        median: ScalarValue,
        variance: ScalarValue,
        range: Interval,
    ) -> Result<Self> {
        if range.data_type().eq(&DataType::Boolean) {
            internal_err!(
                "Usage of boolean range during Unknown statistic construction \
            is prohibited. Create Bernoulli statistic instead."
            )
        } else if !is_valid_unknown(&mean, &median, &variance, &range)? {
            internal_err!("Tried to construct invalid Unknown statistic")
        } else {
            Ok(Unknown {
                mean,
                median,
                variance,
                range,
            })
        }
    }

    /// Constructs a new [`Unknown`] statistics instance from the given range.
    /// Other parameters; mean, median and variance are initialized with null values.
    pub fn new_unknown_from_interval(range: &Interval) -> Result<StatisticsV2> {
        let null = ScalarValue::try_from(range.data_type())?;
        StatisticsV2::new_unknown(null.clone(), null.clone(), null, range.clone())
    }

    /// Validates accumulated statistic for selected distribution methods:
    /// - For [`Exponential`], `rate` must be positive;
    /// - For [`Gaussian`], `variant` must be non-negative
    /// - For [`Bernoulli`], `p` must be in `[0, 1]`
    /// - For [`Unknown`],
    ///   - if `mean` and/or `median` are defined, the `range` must contain their values
    ///   - if `std_dev` is defined, it must be non-negative
    pub fn is_valid(&self) -> Result<bool> {
        match &self {
            Exponential { rate, .. } => is_valid_exponential(rate),
            Gaussian { variance, .. } => is_valid_gaussian(variance),
            Bernoulli { p } => is_valid_gaussian(p),
            Unknown {
                mean,
                median,
                variance,
                range,
            } => is_valid_unknown(mean, median, variance, range),
            Uniform { .. } => Ok(true),
        }
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
            Uniform { interval } => {
                let agg = interval
                    .lower()
                    .add_checked(interval.upper())?
                    .cast_to(&DataType::Float64)?;
                agg.div(ScalarValue::Float64(Some(2.)))
            }
            Exponential {
                rate,
                offset,
                positive_tail,
            } => {
                let one = ScalarValue::new_one(&offset.data_type())?;
                one.div(rate)?.add_checked(offset).map(|abs_mean| {
                    if *positive_tail {
                        Ok(abs_mean)
                    } else {
                        abs_mean.arithmetic_negate()
                    }
                })?
            }
            Gaussian { mean, .. } => Ok(mean.clone()),
            Bernoulli { p } => Ok(p.clone()),
            Unknown { mean, .. } => Ok(mean.clone()),
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
            Uniform { interval, .. } => {
                let agg = interval
                    .lower()
                    .add_checked(interval.upper())?
                    .cast_to(&DataType::Float64)?;
                agg.div(ScalarValue::Float64(Some(2.)))
            }
            Exponential {
                rate,
                offset,
                positive_tail,
            } => {
                let abs_median = get_ln_two().div(rate)?.add_checked(offset)?;
                if *positive_tail {
                    Ok(abs_median)
                } else {
                    abs_median.arithmetic_negate()
                }
            }
            Gaussian { mean, .. } => Ok(mean.clone()),
            Bernoulli { p } => {
                if p.gt(&ScalarValue::Float64(Some(0.5))) {
                    ScalarValue::new_one(&DataType::Float64)
                } else {
                    ScalarValue::new_zero(&DataType::Float64)
                }
            }
            Unknown { median, .. } => Ok(median.clone()),
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
            Uniform { interval, .. } => {
                let base_value_ref = interval
                    .upper()
                    .sub_checked(interval.lower())?
                    .cast_to(&DataType::Float64)?;
                let base_pow = base_value_ref
                    .mul_checked(&base_value_ref)?
                    .cast_to(&DataType::Float64)?;
                base_pow.div(ScalarValue::Float64(Some(12.)))
            }
            Exponential { rate, .. } => {
                let one = ScalarValue::new_one(&rate.data_type())?;
                let rate_squared = rate.mul(rate)?;
                one.div(rate_squared)
            }
            Gaussian { variance, .. } => Ok(variance.clone()),
            Bernoulli { p } => {
                let one = ScalarValue::new_one(&DataType::Float64)?;
                one.sub_checked(p)?.mul_checked(p)
            }
            Unknown { variance, .. } => Ok(variance.clone()),
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
            Uniform { interval, .. } => Ok(interval.clone()),
            Exponential { offset, .. } => {
                let offset_data_type = offset.data_type();
                let interval = Interval::try_new(
                    offset.clone(),
                    ScalarValue::try_from(offset_data_type)?,
                )?;
                Ok(interval)
            }
            Gaussian { .. } => Ok(Interval::make_unbounded(&DataType::Float64)?),
            Bernoulli { p } => {
                if p.eq(&ScalarValue::new_zero(&DataType::Float64)?) {
                    Ok(Interval::CERTAINLY_FALSE)
                } else if p.eq(&ScalarValue::new_one(&DataType::Float64)?) {
                    Ok(Interval::CERTAINLY_TRUE)
                } else {
                    Ok(Interval::UNCERTAIN)
                }
            }
            Unknown { range, .. } => Ok(range.clone()),
        }
    }

    /// Returns the data type of the statistics.
    pub fn data_type(&self) -> DataType {
        match &self {
            Uniform { interval, .. } => interval.data_type(),
            Exponential { offset, .. } => offset.data_type(),
            Gaussian { mean, .. } => mean.data_type(),
            Bernoulli { .. } => DataType::Boolean,
            Unknown { range, .. } => range.data_type(),
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

fn is_valid_exponential(rate: &ScalarValue) -> Result<bool> {
    if rate.is_null() {
        return internal_err!(
            "Rate argument of Exponential Distribution cannot be a null ScalarValue"
        );
    }
    let zero = ScalarValue::new_zero(&rate.data_type())?;
    Ok(rate.gt(&zero))
}

fn is_valid_gaussian(variance: &ScalarValue) -> Result<bool> {
    if variance.is_null() {
        return internal_err!(
            "Variance argument of Gaussian Distribution cannot be null ScalarValue"
        );
    }
    let zero = ScalarValue::new_zero(&variance.data_type())?;
    Ok(variance.ge(&zero))
}

fn is_valid_bernoulli(p: &ScalarValue) -> Result<bool> {
    if p.is_null() {
        Ok(true)
    } else if p.data_type().eq(&DataType::Float64) {
        let zero = ScalarValue::new_zero(&DataType::Float64)?;
        let one = ScalarValue::new_one(&DataType::Float64)?;
        Ok(p.ge(&zero) && p.le(&one))
    } else {
        internal_err!("Bernoulli distribution can hold only float64 probability")
    }
}

fn is_valid_unknown(
    mean: &ScalarValue,
    median: &ScalarValue,
    variance: &ScalarValue,
    range: &Interval,
) -> Result<bool> {
    let is_valid_mean_median = |m: &ScalarValue| -> Result<bool> {
        if m.is_null() {
            Ok(true)
        } else {
            range.contains_value(m)
        }
    };

    if !is_valid_mean_median(mean)? || !is_valid_mean_median(median)? {
        Ok(false)
    } else if !variance.is_null() {
        let zero = ScalarValue::new_zero(&variance.data_type())?;
        Ok(variance.ge(&zero))
    } else {
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use crate::stats_v2::StatisticsV2;

    use arrow::datatypes::DataType;
    use datafusion_common::ScalarValue;
    use datafusion_expr_common::interval_arithmetic::Interval;

    // The test data in the following tests are placed as follows: (stat -> expected answer)
    #[test]
    fn uniform_stats_is_valid_test() {
        assert_eq!(
            StatisticsV2::new_uniform(Interval::make_zero(&DataType::Int8).unwrap())
                .unwrap(),
            StatisticsV2::Uniform {
                interval: Interval::make_zero(&DataType::Int8).unwrap()
            }
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
        let mut stats = vec![
            (
                StatisticsV2::new_uniform(Interval::make_zero(&DataType::Int64).unwrap()),
                ScalarValue::Float64(Some(0.)),
            ),
            (
                StatisticsV2::new_uniform(
                    Interval::make_zero(&DataType::Float64).unwrap(),
                ),
                ScalarValue::Float64(Some(0.)),
            ),
            (
                StatisticsV2::new_uniform(Interval::make(Some(1), Some(100)).unwrap()),
                ScalarValue::Float64(Some(50.5)),
            ),
            (
                StatisticsV2::new_uniform(Interval::make(Some(-100), Some(-1)).unwrap()),
                ScalarValue::Float64(Some(-50.5)),
            ),
            (
                StatisticsV2::new_uniform(Interval::make(Some(-100), Some(100)).unwrap()),
                ScalarValue::Float64(Some(0.)),
            ),
        ];

        stats.push((
            StatisticsV2::new_exponential(
                ScalarValue::Float64(Some(2.)),
                ScalarValue::Float64(Some(0.)),
                true,
            ),
            ScalarValue::Float64(Some(0.5)),
        ));
        stats.push((
            StatisticsV2::new_exponential(
                ScalarValue::Float64(Some(2.)),
                ScalarValue::Float64(Some(1.)),
                true,
            ),
            ScalarValue::Float64(Some(1.5)),
        ));

        stats.push((
            StatisticsV2::new_gaussian(
                ScalarValue::Float64(Some(0.)),
                ScalarValue::Float64(Some(1.)),
            ),
            ScalarValue::Float64(Some(0.)),
        ));
        stats.push((
            StatisticsV2::new_gaussian(
                ScalarValue::Float64(Some(-2.)),
                ScalarValue::Float64(Some(0.5)),
            ),
            ScalarValue::Float64(Some(-2.)),
        ));

        stats.push((
            StatisticsV2::new_bernoulli(ScalarValue::Float64(Some(0.5))),
            ScalarValue::Float64(Some(0.5)),
        ));

        stats.push((
            StatisticsV2::new_unknown(
                ScalarValue::Float64(Some(42.)),
                ScalarValue::Float64(Some(42.)),
                ScalarValue::Null,
                Interval::make(Some(25.), Some(50.)).unwrap(),
            ),
            ScalarValue::Float64(Some(42.)),
        ));

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
                ScalarValue::Float64(Some(0.)),
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
            (
                StatisticsV2::new_unknown_from_interval(
                    &Interval::make(Some(0), Some(12)).unwrap(),
                ),
                ScalarValue::Float64(Some(12.)),
            ),
        ];

        for case in stats {
            assert_eq!(case.0.unwrap().variance().unwrap(), case.1);
        }
    }
}
