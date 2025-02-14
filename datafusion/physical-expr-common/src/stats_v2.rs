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

use arrow::array::ArrowNativeTypeOp;
use arrow::datatypes::DataType;
use datafusion_common::rounding::alter_fp_rounding_mode;
use datafusion_common::{internal_err, not_impl_err, Result, ScalarValue};
use datafusion_expr_common::interval_arithmetic::{apply_operator, Interval};
use datafusion_expr_common::operator::Operator;
use datafusion_expr_common::type_coercion::binary::binary_numeric_coercion;

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

use StatisticsV2::{Bernoulli, Exponential, Gaussian, Uniform, Unknown};

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
    pub fn new_from_interval(range: Interval) -> Result<Self> {
        let null = ScalarValue::try_from(range.data_type())?;
        StatisticsV2::new_unknown(null.clone(), null.clone(), null, range)
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

    /// Returns the data type of the statistical parameters comprising this
    /// distribution.
    pub fn data_type(&self) -> DataType {
        match &self {
            Uniform(u) => u.data_type(),
            Exponential(e) => e.data_type(),
            Gaussian(g) => g.data_type(),
            Bernoulli(b) => b.data_type(),
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

    /// Computes the mean value of this distribution. In case of improper
    /// distributions (i.e. when the range is unbounded), the function returns
    /// a `NULL` `ScalarValue`.
    pub fn mean(&self) -> Result<ScalarValue> {
        // TODO: Should we ensure that this always returns a real number data type?
        let dt = self.data_type();
        let two = ScalarValue::from(2).cast_to(&dt)?;
        let result = self
            .interval
            .lower()
            .add_checked(self.interval.upper())?
            .div(two);
        debug_assert!(
            !self.interval.is_unbounded() || result.as_ref().is_ok_and(|r| r.is_null())
        );
        result
    }

    pub fn median(&self) -> Result<ScalarValue> {
        self.mean()
    }

    /// Computes the variance value of this distribution. In case of improper
    /// distributions (i.e. when the range is unbounded), the function returns
    /// a `NULL` `ScalarValue`.
    pub fn variance(&self) -> Result<ScalarValue> {
        // TODO: Should we ensure that this always returns a real number data type?
        let width = self.interval.width()?;
        let dt = width.data_type();
        let twelve = ScalarValue::from(12).cast_to(&dt)?;
        let result = width.mul_checked(&width)?.div(twelve);
        debug_assert!(
            !self.interval.is_unbounded() || result.as_ref().is_ok_and(|r| r.is_null())
        );
        result
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
        // TODO: Should we ensure that this always returns a real number data type?
        let one = ScalarValue::new_one(&self.data_type())?;
        let tail_mean = one.div(&self.rate)?;
        if self.positive_tail {
            self.offset.add_checked(tail_mean)
        } else {
            self.offset.sub_checked(tail_mean)
        }
    }

    pub fn median(&self) -> Result<ScalarValue> {
        // TODO: Should we ensure that this always returns a real number data type?
        let ln_two = ScalarValue::from(LN_2).cast_to(&self.data_type())?;
        let tail_median = ln_two.div(&self.rate)?;
        if self.positive_tail {
            self.offset.add_checked(tail_median)
        } else {
            self.offset.sub_checked(tail_median)
        }
    }

    pub fn variance(&self) -> Result<ScalarValue> {
        // TODO: Should we ensure that this always returns a real number data type?
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

    pub fn data_type(&self) -> DataType {
        self.p.data_type()
    }

    pub fn p_value(&self) -> &ScalarValue {
        &self.p
    }

    pub fn mean(&self) -> &ScalarValue {
        &self.p
    }

    /// Computes the median value of this distribution. In case of an unknown
    /// success probability, the function returns a `NULL` `ScalarValue`.
    pub fn median(&self) -> Result<ScalarValue> {
        let dt = self.data_type();
        if self.p.is_null() {
            ScalarValue::try_from(&dt)
        } else {
            let one = ScalarValue::new_one(&dt)?;
            if one.sub_checked(&self.p)?.lt(&self.p) {
                ScalarValue::new_one(&dt)
            } else {
                ScalarValue::new_zero(&dt)
            }
        }
    }

    /// Computes the variance value of this distribution. In case of an unknown
    /// success probability, the function returns a `NULL` `ScalarValue`.
    pub fn variance(&self) -> Result<ScalarValue> {
        let dt = self.data_type();
        let one = ScalarValue::new_one(&dt)?;
        let result = one.sub_checked(&self.p)?.mul_checked(&self.p);
        debug_assert!(!self.p.is_null() || result.as_ref().is_ok_and(|r| r.is_null()));
        result
    }

    pub fn range(&self) -> Interval {
        let dt = self.data_type();
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
        if range.data_type().eq(&DataType::Boolean) {
            return internal_err!(
                "Construction of a boolean `Unknown` statistic is prohibited, create a `Bernoulli` statistic instead."
            );
        }

        let validate_location = |m: &ScalarValue| -> Result<bool> {
            // Checks whether the given location estimate is within the range.
            if m.is_null() {
                Ok(true)
            } else {
                range.contains_value(m)
            }
        };

        if !validate_location(&mean)?
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

/// This function takes a logical operator and two Bernoulli distributions,
/// and it returns a new Bernoulli distribution that represents the result of
/// the operation. Currently, only `AND` and `OR` operations are supported.
pub fn combine_bernoullis(
    op: &Operator,
    left: &BernoulliDistribution,
    right: &BernoulliDistribution,
) -> Result<BernoulliDistribution> {
    // TODO: Write tests for this function.
    let left_p = left.p_value();
    let right_p = right.p_value();
    match op {
        Operator::And => match (left_p.is_null(), right_p.is_null()) {
            (false, false) => {
                BernoulliDistribution::try_new(left_p.mul_checked(right_p)?)
            }
            (false, true) if left_p.eq(&ScalarValue::new_zero(&left_p.data_type())?) => {
                Ok(left.clone())
            }
            (true, false)
                if right_p.eq(&ScalarValue::new_zero(&right_p.data_type())?) =>
            {
                Ok(right.clone())
            }
            _ => {
                let dt = StatisticsV2::target_type(&[left_p, right_p])?;
                BernoulliDistribution::try_new(ScalarValue::try_from(&dt)?)
            }
        },
        Operator::Or => match (left_p.is_null(), right_p.is_null()) {
            (false, false) => {
                let sum = left_p.add_checked(right_p)?;
                let product = left_p.mul_checked(right_p)?;
                let or_success = sum.sub_checked(product)?;
                BernoulliDistribution::try_new(or_success)
            }
            (false, true) if left_p.eq(&ScalarValue::new_one(&left_p.data_type())?) => {
                Ok(left.clone())
            }
            (true, false) if right_p.eq(&ScalarValue::new_one(&right_p.data_type())?) => {
                Ok(right.clone())
            }
            _ => {
                let dt = StatisticsV2::target_type(&[left_p, right_p])?;
                BernoulliDistribution::try_new(ScalarValue::try_from(&dt)?)
            }
        },
        _ => {
            not_impl_err!("Statistical evaluation only supports AND and OR operators")
        }
    }
}

/// Applies the given operation to the given Gaussian distributions. Currently,
/// this function handles only addition and subtraction operations. If the
/// result is not a Gaussian random variable, it returns `None`. For details,
/// see:
///
/// <https://en.wikipedia.org/wiki/Sum_of_normally_distributed_random_variables>
pub fn combine_gaussians(
    op: &Operator,
    left: &GaussianDistribution,
    right: &GaussianDistribution,
) -> Result<Option<GaussianDistribution>> {
    // TODO: Write tests for this function.
    match op {
        Operator::Plus => GaussianDistribution::try_new(
            left.mean().add_checked(right.mean())?,
            left.variance().add_checked(right.variance())?,
        )
        .map(Some),
        Operator::Minus => GaussianDistribution::try_new(
            left.mean().sub_checked(right.mean())?,
            left.variance().add_checked(right.variance())?,
        )
        .map(Some),
        _ => Ok(None),
    }
}

/// Creates a new statistic with `Bernoulli` distribution by computing the
/// resulting probability. Expects `op` to be a comparison operator, with
/// `left` and `right` having numeric distributions. The resulting distribution
/// has the `Float64` data type.
pub fn create_bernoulli_from_comparison(
    op: &Operator,
    left: &StatisticsV2,
    right: &StatisticsV2,
) -> Result<StatisticsV2> {
    match (left, right) {
        (Uniform(left), Uniform(right)) => {
            match op {
                Operator::Eq | Operator::NotEq => {
                    let (li, ri) = (left.range(), right.range());
                    if let Some(intersection) = li.intersect(ri)? {
                        // If the ranges are not disjoint, calculate the probability
                        // of equality using cardinalities:
                        if let (Some(lc), Some(rc), Some(ic)) = (
                            li.cardinality(),
                            ri.cardinality(),
                            intersection.cardinality(),
                        ) {
                            // Avoid overflow by widening the type temporarily:
                            let pairs = ((lc as u128) * (rc as u128)) as f64;
                            let p = (ic as f64).div_checked(pairs)?;
                            // Alternative approach that may be more stable:
                            // let p = (ic as f64)
                            //     .div_checked(lc as f64)?
                            //     .div_checked(rc as f64)?;

                            let mut p_value = ScalarValue::from(p);
                            if op == &Operator::NotEq {
                                let one = ScalarValue::from(1.0);
                                p_value = alter_fp_rounding_mode::<false, _>(
                                    &one,
                                    &p_value,
                                    |lhs, rhs| lhs.sub_checked(rhs),
                                )?;
                            };
                            return StatisticsV2::new_bernoulli(p_value);
                        }
                    } else if op == &Operator::Eq {
                        // If the ranges are disjoint, probability of equality is 0.
                        return StatisticsV2::new_bernoulli(ScalarValue::from(0.0));
                    } else {
                        // If the ranges are disjoint, probability of not-equality is 1.
                        return StatisticsV2::new_bernoulli(ScalarValue::from(1.0));
                    }
                }
                Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
                    // TODO: We can handle inequality operators and calculate a
                    // `p` value instead of falling back to an unknown Bernoulli
                    // distribution. Note that the strict and non-strict inequalities
                    // may require slightly different logic in case of real vs.
                    // integral data types.
                }
                _ => {}
            }
        }
        (Gaussian(_), Gaussian(_)) => {
            // TODO: We can handle Gaussian comparisons and calculate a `p` value
            //       instead of falling back to an unknown Bernoulli distribution.
        }
        _ => {}
    }
    let (li, ri) = (left.range()?, right.range()?);
    let range_evaluation = apply_operator(op, &li, &ri)?;
    if range_evaluation.eq(&Interval::CERTAINLY_FALSE) {
        StatisticsV2::new_bernoulli(ScalarValue::from(0.0))
    } else if range_evaluation.eq(&Interval::CERTAINLY_TRUE) {
        StatisticsV2::new_bernoulli(ScalarValue::from(1.0))
    } else if range_evaluation.eq(&Interval::UNCERTAIN) {
        StatisticsV2::new_bernoulli(ScalarValue::try_from(&DataType::Float64)?)
    } else {
        internal_err!("This function must be called with a comparison operator")
    }
}

/// Creates a new statistic with `Unknown` distribution, and tries to compute
/// mean, median and variance if possible.
pub fn new_unknown_from_binary_op(
    op: &Operator,
    left: &StatisticsV2,
    right: &StatisticsV2,
) -> Result<StatisticsV2> {
    StatisticsV2::new_unknown(
        compute_mean(op, left, right)?,
        compute_median(op, left, right)?,
        compute_variance(op, left, right)?,
        apply_operator(op, &left.range()?, &right.range()?)?,
    )
}

/// Computes the mean value for the result of the given binary operation on
/// two statistics.
pub fn compute_mean(
    op: &Operator,
    left: &StatisticsV2,
    right: &StatisticsV2,
) -> Result<ScalarValue> {
    let (left_mean, right_mean) = (left.mean()?, right.mean()?);

    match op {
        Operator::Plus => return left_mean.add_checked(right_mean),
        Operator::Minus => return left_mean.sub_checked(right_mean),
        // Note the independence assumption below:
        Operator::Multiply => return left_mean.mul_checked(right_mean),
        // TODO: We can calculate the mean for division when we support reciprocals,
        // or know the distributions of the operands. For details, see:
        //
        // <https://en.wikipedia.org/wiki/Algebra_of_random_variables>
        // <https://stats.stackexchange.com/questions/185683/distribution-of-ratio-between-two-independent-uniform-random-variables>
        //
        // Fall back to an unknown mean value for division:
        Operator::Divide => {}
        // Fall back to an unknown mean value for other cases:
        _ => {}
    }
    let target_type = StatisticsV2::target_type(&[&left_mean, &right_mean])?;
    ScalarValue::try_from(target_type)
}

/// Computes the median value for the result of the given binary operation on
/// two statistics. Currently, the median is calculable only for addition and
/// subtraction operations on:
/// - [`Uniform`] and [`Uniform`] distributions, and
/// - [`Gaussian`] and [`Gaussian`] distributions.
pub fn compute_median(
    op: &Operator,
    left: &StatisticsV2,
    right: &StatisticsV2,
) -> Result<ScalarValue> {
    match (left, right) {
        (Uniform(lu), Uniform(ru)) => {
            let (left_median, right_median) = (lu.median()?, ru.median()?);
            // Under the independence assumption, the result is a symmetric
            // triangular distribution, so we can simply add/subtract the
            // median values:
            match op {
                Operator::Plus => return left_median.add_checked(right_median),
                Operator::Minus => return left_median.sub_checked(right_median),
                // Fall back to an unknown median value for other cases:
                _ => {}
            }
        }
        // Under the independence assumption, the result is another Gaussian
        // distribution, so we can simply add/subtract the median values:
        (Gaussian(lg), Gaussian(rg)) => match op {
            Operator::Plus => return lg.mean().add_checked(rg.mean()),
            Operator::Minus => return lg.mean().sub_checked(rg.mean()),
            // Fall back to an unknown median value for other cases:
            _ => {}
        },
        // Fall back to an unknown median value for other cases:
        _ => {}
    }

    let (left_median, right_median) = (left.median()?, right.median()?);
    let target_type = StatisticsV2::target_type(&[&left_median, &right_median])?;
    ScalarValue::try_from(target_type)
}

/// Computes the variance value for the result of the given binary operation on
/// two statistics.
pub fn compute_variance(
    op: &Operator,
    left: &StatisticsV2,
    right: &StatisticsV2,
) -> Result<ScalarValue> {
    let (left_variance, right_variance) = (left.variance()?, right.variance()?);

    match op {
        // Note the independence assumption below:
        Operator::Plus => return left_variance.add_checked(right_variance),
        // Note the independence assumption below:
        Operator::Minus => return left_variance.add_checked(right_variance),
        // Note the independence assumption below:
        Operator::Multiply => {
            // For more details, along with an explanation of the formula below, see:
            //
            // <https://en.wikipedia.org/wiki/Distribution_of_the_product_of_two_random_variables>
            let (left_mean, right_mean) = (left.mean()?, right.mean()?);
            let left_mean_sq = left_mean.mul_checked(&left_mean)?;
            let right_mean_sq = right_mean.mul_checked(&right_mean)?;
            let left_sos = left_variance.add_checked(&left_mean_sq)?;
            let right_sos = right_variance.add_checked(&right_mean_sq)?;
            let pos = left_mean_sq.mul_checked(right_mean_sq)?;
            return left_sos.mul_checked(right_sos)?.sub_checked(pos);
        }
        // TODO: We can calculate the variance for division when we support reciprocals,
        // or know the distributions of the operands. For details, see:
        //
        // <https://en.wikipedia.org/wiki/Algebra_of_random_variables>
        // <https://stats.stackexchange.com/questions/185683/distribution-of-ratio-between-two-independent-uniform-random-variables>
        //
        // Fall back to an unknown variance value for division:
        Operator::Divide => {}
        // Fall back to an unknown variance value for other cases:
        _ => {}
    }
    let target_type = StatisticsV2::target_type(&[&left_variance, &right_variance])?;
    ScalarValue::try_from(target_type)
}

#[cfg(test)]
mod tests {
    use super::{
        compute_mean, compute_median, compute_variance, create_bernoulli_from_comparison,
        new_unknown_from_binary_op, StatisticsV2, UniformDistribution,
    };

    use arrow::datatypes::DataType;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr_common::interval_arithmetic::{apply_operator, Interval};
    use datafusion_expr_common::operator::Operator;

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
                    ScalarValue::from(0_f32),
                    ScalarValue::from(1_f32),
                    true,
                ),
                false,
            ),
            (
                StatisticsV2::new_exponential(
                    ScalarValue::from(100_f32),
                    ScalarValue::from(1_f32),
                    true,
                ),
                true,
            ),
            (
                StatisticsV2::new_exponential(
                    ScalarValue::from(-100_f32),
                    ScalarValue::from(1_f32),
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
                    ScalarValue::from(0_f32),
                    ScalarValue::from(0_f32),
                ),
                true,
            ),
            (
                StatisticsV2::new_gaussian(
                    ScalarValue::from(0_f32),
                    ScalarValue::from(0.5_f32),
                ),
                true,
            ),
            (
                StatisticsV2::new_gaussian(
                    ScalarValue::from(0_f32),
                    ScalarValue::from(-0.5_f32),
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
            (StatisticsV2::new_bernoulli(ScalarValue::from(0.)), true),
            (StatisticsV2::new_bernoulli(ScalarValue::from(0.25)), true),
            (StatisticsV2::new_bernoulli(ScalarValue::from(1.)), true),
            (StatisticsV2::new_bernoulli(ScalarValue::from(11.)), false),
            (StatisticsV2::new_bernoulli(ScalarValue::from(-11.)), false),
            (StatisticsV2::new_bernoulli(ScalarValue::from(0_i64)), true),
            (StatisticsV2::new_bernoulli(ScalarValue::from(1_i64)), true),
            (
                StatisticsV2::new_bernoulli(ScalarValue::from(11_i64)),
                false,
            ),
            (
                StatisticsV2::new_bernoulli(ScalarValue::from(-11_i64)),
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
                    ScalarValue::from(0_f32),
                    ScalarValue::Float32(None),
                    ScalarValue::Float32(None),
                    Interval::make_zero(&DataType::Float32)?,
                ),
                true,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Float64(None),
                    ScalarValue::from(0.),
                    ScalarValue::Float64(None),
                    Interval::make_zero(&DataType::Float32)?,
                ),
                true,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::from(-10_f32),
                    ScalarValue::Float32(None),
                    ScalarValue::Float32(None),
                    Interval::make_zero(&DataType::Float32)?,
                ),
                false,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Float32(None),
                    ScalarValue::from(10_f32),
                    ScalarValue::Float32(None),
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
                    ScalarValue::from(0),
                    ScalarValue::from(0),
                    ScalarValue::Int32(None),
                    Interval::make_zero(&DataType::Int32)?,
                ),
                true,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::from(0_f32),
                    ScalarValue::from(0_f32),
                    ScalarValue::Float32(None),
                    Interval::make_zero(&DataType::Float32)?,
                ),
                true,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::from(50.),
                    ScalarValue::from(50.),
                    ScalarValue::Float64(None),
                    Interval::make(Some(0.), Some(100.))?,
                ),
                true,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::from(50.),
                    ScalarValue::from(50.),
                    ScalarValue::Float64(None),
                    Interval::make(Some(-100.), Some(0.))?,
                ),
                false,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Float64(None),
                    ScalarValue::Float64(None),
                    ScalarValue::from(1.),
                    Interval::make_zero(&DataType::Float64)?,
                ),
                true,
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Float64(None),
                    ScalarValue::Float64(None),
                    ScalarValue::from(-1.),
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
                ScalarValue::from(0_i64),
            ),
            (
                StatisticsV2::new_uniform(Interval::make_zero(&DataType::Float64)?),
                ScalarValue::from(0.),
            ),
            (
                StatisticsV2::new_uniform(Interval::make(Some(1), Some(100))?),
                ScalarValue::from(50),
            ),
            (
                StatisticsV2::new_uniform(Interval::make(Some(-100), Some(-1))?),
                ScalarValue::from(-50),
            ),
            (
                StatisticsV2::new_uniform(Interval::make(Some(-100), Some(100))?),
                ScalarValue::from(0),
            ),
            (
                StatisticsV2::new_exponential(
                    ScalarValue::from(2.),
                    ScalarValue::from(0.),
                    true,
                ),
                ScalarValue::from(0.5),
            ),
            (
                StatisticsV2::new_exponential(
                    ScalarValue::from(2.),
                    ScalarValue::from(1.),
                    true,
                ),
                ScalarValue::from(1.5),
            ),
            (
                StatisticsV2::new_gaussian(ScalarValue::from(0.), ScalarValue::from(1.)),
                ScalarValue::from(0.),
            ),
            (
                StatisticsV2::new_gaussian(
                    ScalarValue::from(-2.),
                    ScalarValue::from(0.5),
                ),
                ScalarValue::from(-2.),
            ),
            (
                StatisticsV2::new_bernoulli(ScalarValue::from(0.5)),
                ScalarValue::from(0.5),
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::from(42.),
                    ScalarValue::from(42.),
                    ScalarValue::Float64(None),
                    Interval::make(Some(25.), Some(50.))?,
                ),
                ScalarValue::from(42.),
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
                ScalarValue::from(0_i64),
            ),
            (
                StatisticsV2::new_uniform(Interval::make(Some(25.), Some(75.))?),
                ScalarValue::from(50.),
            ),
            (
                StatisticsV2::new_exponential(
                    ScalarValue::from(2_f64.ln()),
                    ScalarValue::from(0.),
                    true,
                ),
                ScalarValue::from(1.),
            ),
            (
                StatisticsV2::new_gaussian(ScalarValue::from(2.), ScalarValue::from(1.)),
                ScalarValue::from(2.),
            ),
            (
                StatisticsV2::new_bernoulli(ScalarValue::from(0.25)),
                ScalarValue::from(0.),
            ),
            (
                StatisticsV2::new_bernoulli(ScalarValue::from(0.75)),
                ScalarValue::from(1.),
            ),
            (
                StatisticsV2::new_gaussian(ScalarValue::from(2.), ScalarValue::from(1.)),
                ScalarValue::from(2.),
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::from(12.),
                    ScalarValue::from(12.),
                    ScalarValue::Float64(None),
                    Interval::make(Some(0.), Some(25.))?,
                ),
                ScalarValue::from(12.),
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
                ScalarValue::from(12.),
            ),
            (
                StatisticsV2::new_exponential(
                    ScalarValue::from(10.),
                    ScalarValue::from(0.),
                    true,
                ),
                ScalarValue::from(0.01),
            ),
            (
                StatisticsV2::new_gaussian(ScalarValue::from(0.), ScalarValue::from(1.)),
                ScalarValue::from(1.),
            ),
            (
                StatisticsV2::new_bernoulli(ScalarValue::from(0.5)),
                ScalarValue::from(0.25),
            ),
            (
                StatisticsV2::new_unknown(
                    ScalarValue::Float64(None),
                    ScalarValue::Float64(None),
                    ScalarValue::from(0.02),
                    Interval::make_zero(&DataType::Float64)?,
                ),
                ScalarValue::from(0.02),
            ),
        ];

        for case in stats {
            assert_eq!(case.0?.variance()?, case.1);
        }

        Ok(())
    }

    #[test]
    fn test_calculate_unknown_properties_gauss_gauss() -> Result<()> {
        let stat_a =
            StatisticsV2::new_gaussian(ScalarValue::from(10.), ScalarValue::from(0.0))?;
        let stat_b =
            StatisticsV2::new_gaussian(ScalarValue::from(20.), ScalarValue::from(0.0))?;

        let test_data = vec![
            // mean
            (
                compute_mean(&Operator::Plus, &stat_a, &stat_b)?,
                ScalarValue::from(30.),
            ),
            (
                compute_mean(&Operator::Minus, &stat_a, &stat_b)?,
                ScalarValue::from(-10.),
            ),
            // median
            (
                compute_median(&Operator::Plus, &stat_a, &stat_b)?,
                ScalarValue::from(30.),
            ),
            (
                compute_median(&Operator::Minus, &stat_a, &stat_b)?,
                ScalarValue::from(-10.),
            ),
        ];
        for (actual, expected) in test_data {
            assert_eq!(actual, expected);
        }

        Ok(())
    }

    // Expected test results were calculated in Wolfram Mathematica, by using
    // *METHOD_NAME*[TransformedDistribution[x op y, {x ~ *DISTRIBUTION_X*[..], y ~ *DISTRIBUTION_Y*[..]}]]
    #[test]
    fn test_calculate_unknown_properties_uniform_uniform() -> Result<()> {
        let stat_a = StatisticsV2::new_uniform(Interval::make(Some(0.), Some(12.))?)?;
        let stat_b = StatisticsV2::new_uniform(Interval::make(Some(12.), Some(36.))?)?;

        let test_data = vec![
            // mean
            (
                compute_mean(&Operator::Plus, &stat_a, &stat_b)?,
                ScalarValue::from(30.),
            ),
            (
                compute_mean(&Operator::Minus, &stat_a, &stat_b)?,
                ScalarValue::from(-18.),
            ),
            (
                compute_mean(&Operator::Multiply, &stat_a, &stat_b)?,
                ScalarValue::from(144.),
            ),
            // median
            (
                compute_median(&Operator::Plus, &stat_a, &stat_b)?,
                ScalarValue::from(30.),
            ),
            (
                compute_median(&Operator::Minus, &stat_a, &stat_b)?,
                ScalarValue::from(-18.),
            ),
            // FYI: median of combined distributions for mul, div and mod ops doesn't exist.

            // variance
            (
                compute_variance(&Operator::Plus, &stat_a, &stat_b)?,
                ScalarValue::from(60.),
            ),
            (
                compute_variance(&Operator::Minus, &stat_a, &stat_b)?,
                ScalarValue::from(60.),
            ),
            // (compute_variance(&Operator::Multiply, &stat_a, &stat_b), Some(Float64(Some(9216.)))),
        ];
        for (actual, expected) in test_data {
            assert_eq!(actual, expected);
        }

        Ok(())
    }

    /// Test for Uniform-Uniform, Uniform-Unknown, Unknown-Uniform, Unknown-Unknown pairs,
    /// where range is always present.
    #[test]
    fn test_compute_range_where_present() -> Result<()> {
        let a = &Interval::make(Some(0.), Some(12.0))?;
        let b = &Interval::make(Some(0.), Some(12.0))?;
        let mean = ScalarValue::from(6.0);
        for (stat_a, stat_b) in [
            (
                StatisticsV2::new_uniform(a.clone())?,
                StatisticsV2::new_uniform(b.clone())?,
            ),
            (
                StatisticsV2::new_unknown(
                    mean.clone(),
                    mean.clone(),
                    ScalarValue::Float64(None),
                    a.clone(),
                )?,
                StatisticsV2::new_uniform(b.clone())?,
            ),
            (
                StatisticsV2::new_uniform(a.clone())?,
                StatisticsV2::new_unknown(
                    mean.clone(),
                    mean.clone(),
                    ScalarValue::Float64(None),
                    b.clone(),
                )?,
            ),
            (
                StatisticsV2::new_unknown(
                    mean.clone(),
                    mean.clone(),
                    ScalarValue::Float64(None),
                    a.clone(),
                )?,
                StatisticsV2::new_unknown(
                    mean.clone(),
                    mean.clone(),
                    ScalarValue::Float64(None),
                    b.clone(),
                )?,
            ),
        ] {
            use super::Operator::{
                Divide, Eq, Gt, GtEq, Lt, LtEq, Minus, Multiply, NotEq, Plus,
            };
            for op in [Plus, Minus, Multiply, Divide] {
                assert_eq!(
                    new_unknown_from_binary_op(&op, &stat_a, &stat_b)?.range()?,
                    apply_operator(&op, a, b)?,
                    "Failed for {:?} {op} {:?}",
                    stat_a,
                    stat_b
                );
            }
            for op in [Gt, GtEq, Lt, LtEq, Eq, NotEq] {
                assert_eq!(
                    create_bernoulli_from_comparison(&op, &stat_a, &stat_b)?.range()?,
                    apply_operator(&op, a, b)?,
                    "Failed for {:?} {op} {:?}",
                    stat_a,
                    stat_b
                );
            }
        }

        Ok(())
    }
}
