use crate::stats::StatisticsV2::{Bernoulli, Exponential, Gaussian, Uniform, Unknown};

use arrow::datatypes::DataType;
use datafusion_common::{internal_err, Result, ScalarValue};
use datafusion_expr_common::interval_arithmetic::Interval;

/// New, enhanced `Statistics` definition, represents five core definitions
#[derive(Clone, Debug, PartialEq)]
pub enum StatisticsV2 {
    Uniform {
        interval: Interval,
    },
    /// f(x, λ) = (λe)^-λx, if x >= 0
    Exponential {
        rate: ScalarValue,
        offset: ScalarValue,
    },
    Gaussian {
        mean: ScalarValue,
        variance: ScalarValue,
    },
    Bernoulli {
        p: ScalarValue,
    },
    Unknown {
        mean: Option<ScalarValue>,
        median: Option<ScalarValue>,
        variance: Option<ScalarValue>,
        range: Interval,
    },
}

impl StatisticsV2 {
    /// Validates accumulated statistic for selected distribution methods:
    /// - For [`Exponential`], `rate` must be positive;
    /// - For [`Gaussian`], `variant` must be non-negative
    /// - For [`Bernoulli`], `p` must be in `[0, 1]`
    /// - For [`Unknown`],
    ///   - if `mean`, `median` are defined, the `range` must contain their values
    ///   - if `std_dev` is defined, it must be non-negative
    pub fn is_valid(&self) -> Result<bool> {
        match &self {
            Exponential { rate, .. } => {
                if rate.is_null() {
                    return internal_err!("Rate argument of Exponential Distribution cannot be ScalarValue::Null");
                }
                let zero = ScalarValue::new_zero(&rate.data_type())?;
                Ok(rate.gt(&zero))
            }
            Gaussian { variance, .. } => {
                if variance.is_null() {
                    return internal_err!("Variance argument of Gaussian Distribution cannot be ScalarValue::Null");
                }
                let zero = ScalarValue::new_zero(&variance.data_type())?;
                Ok(variance.ge(&zero))
            }
            Bernoulli { p } => {
                let zero = ScalarValue::new_zero(&DataType::Float64)?;
                let one = ScalarValue::new_one(&DataType::Float64)?;
                Ok(p.ge(&zero) && p.le(&one))
            }
            Unknown {
                mean,
                median,
                variance,
                range,
            } => {
                let mean_median_checker = |m: &ScalarValue| -> Result<bool> {
                    if m.is_null() {
                        return internal_err!("Mean/Median argument of Unknown Distribution cannot be ScalarValue::Null");
                    }
                    range.contains_value(m)
                };
                if let Some(mn) = mean {
                    if !mean_median_checker(mn)? {
                        return Ok(false);
                    }
                }
                if let Some(md) = median {
                    if !mean_median_checker(md)? {
                        return Ok(false);
                    }
                }

                if let Some(v) = variance {
                    if v.is_null() {
                        return internal_err!("Variance argument of Unknown Distribution cannot be ScalarValue::Null");
                    }
                    let zero = ScalarValue::new_zero(&v.data_type())?;
                    if !v.gt(&zero) {
                        return Ok(false);
                    }
                }

                if range.lower().is_null() && range.upper().is_null()
                    || !range.lower().is_null() && !range.upper().is_null()
                {
                    // To represent unbounded intervals with not supported datatypes,
                    // [ScalarValue::Null, ScalarValue::Null] can be used, and it is valid.
                    // We may have tried our best to evaluate and propagate distributions
                    // through the expression tree, but didn't manage to infer the precise
                    // distribution for the given expression.
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            _ => Ok(true),
        }
    }

    /// Extract the mean value of given statistic, available for given statistic kinds:
    /// - [`Uniform`]'s interval implicitly contains mean value, and it is calculable
    ///   by addition of upper and lower bound and dividing the result by 2.
    /// - [`Exponential`] distribution mean is calculable by formula: 1/λ. λ must be non-negative.
    /// - [`Gaussian`] distribution has it explicitly.
    /// - [`Bernoulli`] mean is `p`.
    /// - [`Unknown`] distribution _may_ have it explicitly.
    pub fn mean(&self) -> Result<Option<ScalarValue>> {
        if !self.is_valid()? {
            return internal_err!(
                "Cannot extract mean from invalid statistics: {:?}",
                self
            );
        }
        match &self {
            Uniform { interval, .. } => {
                let agg = interval
                    .lower()
                    .add_checked(interval.upper())?
                    .cast_to(&DataType::Float64)?;
                agg.div(ScalarValue::Float64(Some(2.))).map(Some)
            }
            Exponential { rate, .. } => {
                let one = ScalarValue::new_one(&rate.data_type())?;
                one.div(rate).map(Some)
            }
            Gaussian { mean, .. } => Ok(Some(mean.clone())),
            Bernoulli { p } => Ok(Some(p.clone())),
            Unknown { mean, .. } => Ok(mean.clone()),
        }
    }

    /// Extract the median value of given statistic, available for given statistic kinds:
    /// - [`Uniform`] distribution median is equals to mean: (a + b) / 2
    /// - [`Exponential`] distribution median is calculable by formula: ln2/λ. λ must be non-negative.
    /// - [`Gaussian`] distribution median is equals to mean, which is present explicitly.
    /// - [`Unknown`] distribution median _may_ be present explicitly.
    pub fn median(&self) -> Result<Option<ScalarValue>> {
        if !self.is_valid()? {
            return internal_err!(
                "Cannot extract median from invalid statistics: {:?}",
                self
            );
        }
        match &self {
            Uniform { interval, .. } => {
                let agg = interval
                    .lower()
                    .add_checked(interval.upper())?
                    .cast_to(&DataType::Float64)?;
                agg.div(ScalarValue::Float64(Some(2.))).map(Some)
            }
            Exponential { rate, .. } => {
                // Note: better to move it to constant with lazy_static! { ... },
                // but it does not present in the project.
                let ln_two = ScalarValue::Float64(Some(2_f64.ln()));
                ln_two.div(rate).map(Some)
            }
            Gaussian { mean, .. } => Ok(Some(mean.clone())),
            Bernoulli { p } => {
                if p.gt(&ScalarValue::Float64(Some(0.5))) {
                    Ok(Some(ScalarValue::new_one(&DataType::Float64)?))
                } else {
                    Ok(Some(ScalarValue::new_zero(&DataType::Float64)?))
                }
            }
            Unknown { median, .. } => Ok(median.clone()),
        }
    }

    /// Extract the variance of given statistic distribution:
    /// - [`Uniform`]'s variance is equal to (upper-lower)^2 / 12
    /// - [`Exponential`]'s variance is equal to mean value and calculable as 1/λ^2
    /// - [`Gaussian`]'s variance is available explicitly
    /// - [`Unknown`]'s distribution variance _may_ be present explicitly.
    pub fn variance(&self) -> Result<Option<ScalarValue>> {
        if !self.is_valid()? {
            return internal_err!(
                "Cannot extract variance from invalid statistics: {:?}",
                self
            );
        }
        match &self {
            Uniform { interval, .. } => {
                let base_value_ref = interval.upper().sub_checked(interval.lower())?;
                let base_pow = base_value_ref.mul_checked(&base_value_ref)?;
                base_pow.div(ScalarValue::Float64(Some(12.))).map(Some)
            }
            Exponential { rate, .. } => {
                let one = ScalarValue::new_one(&rate.data_type())?;
                let rate_squared = rate.mul(rate)?;
                one.div(rate_squared).map(Some)
            }
            Gaussian { variance, .. } => Ok(Some(variance.clone())),
            Bernoulli { p } => {
                let one = ScalarValue::new_one(&DataType::Float64)?;
                one.sub_checked(p)?.mul_checked(p).map(Some)
            }
            Unknown { variance, .. } => Ok(variance.clone()),
        }
    }

    /// Extract the range of given statistic distribution:
    /// - [`Uniform`]'s range is its interval
    /// - [`Bernoulli`]'s range is always [0, 1], but we return [`Interval::UNCERTAIN`]
    /// - [`Unknown`]'s range is unbounded by default, but
    pub fn range(&self) -> Result<Option<&Interval>> {
        if !self.is_valid()? {
            return Ok(None);
        }
        match &self {
            Uniform { interval, .. } => Ok(Some(interval)),
            Bernoulli { .. } => Ok(Some(&Interval::UNCERTAIN)),
            Unknown { range, .. } => Ok(Some(range)),
            _ => Ok(None),
        }
    }

    /// Returns a data type of the statistics, if present
    /// explicitly, or able to determine implicitly.
    pub fn data_type(&self) -> Option<DataType> {
        match &self {
            Uniform { interval, .. } => Some(interval.data_type()),
            Bernoulli { p } => Some(p.data_type()),
            Unknown { range, .. } => Some(range.data_type()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::stats::StatisticsV2;
    use arrow::datatypes::DataType;
    use datafusion_common::ScalarValue;
    use datafusion_expr_common::interval_arithmetic::Interval;

    //region is_valid tests
    // The test data in the following tests are placed as follows: (stat -> expected answer)
    #[test]
    fn uniform_stats_is_valid_test() {
        let uniform_stats = vec![
            (
                StatisticsV2::Uniform {
                    interval: Interval::make_zero(&DataType::Int8).unwrap(),
                },
                true,
            ),
            (
                StatisticsV2::Uniform {
                    interval: Interval::make(Some(1), Some(100)).unwrap(),
                },
                true,
            ),
            (
                StatisticsV2::Uniform {
                    interval: Interval::make(Some(-100), Some(-1)).unwrap(),
                },
                true,
            ),
        ];

        for case in uniform_stats {
            assert_eq!(case.0.is_valid().unwrap(), case.1);
        }
    }

    #[test]
    fn exponential_stats_is_valid_test() {
        let exp_stats = vec![
            (
                StatisticsV2::Exponential {
                    rate: ScalarValue::Null,
                    offset: ScalarValue::Null,
                },
                false,
            ),
            (
                StatisticsV2::Exponential {
                    rate: ScalarValue::Float32(Some(0.)),
                    offset: ScalarValue::Float32(Some(1.)),
                },
                false,
            ),
            (
                StatisticsV2::Exponential {
                    rate: ScalarValue::Float32(Some(100.)),
                    offset: ScalarValue::Float32(Some(1.)),
                },
                true,
            ),
            (
                StatisticsV2::Exponential {
                    rate: ScalarValue::Float32(Some(-100.)),
                    offset: ScalarValue::Float32(Some(1.)),
                },
                false,
            ),
        ];
        for case in exp_stats {
            assert_eq!(case.0.is_valid().unwrap(), case.1);
        }
    }

    #[test]
    fn gaussian_stats_is_valid_test() {
        let gaussian_stats = vec![
            (
                StatisticsV2::Gaussian {
                    mean: ScalarValue::Null,
                    variance: ScalarValue::Null,
                },
                false,
            ),
            (
                StatisticsV2::Gaussian {
                    mean: ScalarValue::Float32(Some(0.)),
                    variance: ScalarValue::Float32(Some(0.)),
                },
                true,
            ),
            (
                StatisticsV2::Gaussian {
                    mean: ScalarValue::Float32(Some(0.)),
                    variance: ScalarValue::Float32(Some(0.5)),
                },
                true,
            ),
            (
                StatisticsV2::Gaussian {
                    mean: ScalarValue::Float32(Some(0.)),
                    variance: ScalarValue::Float32(Some(-0.5)),
                },
                false,
            ),
        ];
        for case in gaussian_stats {
            assert_eq!(case.0.is_valid().unwrap(), case.1);
        }
    }

    #[test]
    fn bernoulli_stats_is_valid_test() {
        let gaussian_stats = vec![
            (
                StatisticsV2::Bernoulli {
                    p: ScalarValue::Null,
                },
                false,
            ),
            (
                StatisticsV2::Bernoulli {
                    p: ScalarValue::Float64(Some(0.25)),
                },
                true,
            ),
            (
                StatisticsV2::Bernoulli {
                    p: ScalarValue::Float64(Some(0.)),
                },
                true,
            ),
            (
                StatisticsV2::Bernoulli {
                    p: ScalarValue::Float64(Some(1.)),
                },
                true,
            ),
            (
                StatisticsV2::Bernoulli {
                    p: ScalarValue::Float64(Some(10.)),
                },
                false,
            ),
            (
                StatisticsV2::Bernoulli {
                    p: ScalarValue::Float64(Some(-10.)),
                },
                false,
            ),
        ];
        for case in gaussian_stats {
            assert_eq!(case.0.is_valid().unwrap(), case.1);
        }
    }

    #[test]
    fn unknown_stats_is_valid_test() {
        let unknown_stats = vec![
            (
                StatisticsV2::Unknown {
                    mean: None,
                    median: None,
                    variance: None,
                    range: Interval::make_zero(&DataType::Float32).unwrap(),
                },
                false,
            ),
            (
                StatisticsV2::Unknown {
                    mean: Some(ScalarValue::Float32(Some(0.))),
                    median: None,
                    variance: None,
                    range: Interval::make_zero(&DataType::Float32).unwrap(),
                },
                false,
            ),
            (
                StatisticsV2::Unknown {
                    mean: None,
                    median: Some(ScalarValue::Float32(Some(0.))),
                    variance: None,
                    range: Interval::make_zero(&DataType::Float32).unwrap(),
                },
                false,
            ),
            (
                StatisticsV2::Unknown {
                    mean: Some(ScalarValue::Null),
                    median: Some(ScalarValue::Null),
                    variance: Some(ScalarValue::Null),
                    range: Interval::make_zero(&DataType::Float32).unwrap(),
                },
                false,
            ),
            (
                StatisticsV2::Unknown {
                    mean: Some(ScalarValue::Float32(Some(0.))),
                    median: Some(ScalarValue::Float32(Some(0.))),
                    variance: None,
                    range: Interval::make_zero(&DataType::Float32).unwrap(),
                },
                true,
            ),
            (
                StatisticsV2::Unknown {
                    mean: Some(ScalarValue::Float64(Some(50.))),
                    median: Some(ScalarValue::Float64(Some(50.))),
                    variance: None,
                    range: Interval::make(Some(0.), Some(100.)).unwrap(),
                },
                true,
            ),
            (
                StatisticsV2::Unknown {
                    mean: Some(ScalarValue::Float64(Some(50.))),
                    median: Some(ScalarValue::Float64(Some(50.))),
                    variance: None,
                    range: Interval::make(Some(-100.), Some(0.)).unwrap(),
                },
                false,
            ),
            (
                StatisticsV2::Unknown {
                    mean: None,
                    median: None,
                    variance: Some(ScalarValue::Float64(Some(1.))),
                    range: Interval::make_zero(&DataType::Float64).unwrap(),
                },
                true,
            ),
            (
                StatisticsV2::Unknown {
                    mean: None,
                    median: None,
                    variance: Some(ScalarValue::Float64(Some(-1.))),
                    range: Interval::make_zero(&DataType::Float64).unwrap(),
                },
                false,
            ),
        ];
        for case in unknown_stats {
            assert_eq!(case.0.is_valid().unwrap(), case.1);
        }
    }
    //endregion

    #[test]
    fn mean_extraction_test() {
        // The test data is placed as follows : (stat -> expected answer)
        //region uniform
        let mut stats = vec![
            (
                StatisticsV2::Uniform {
                    interval: Interval::make_zero(&DataType::Int64).unwrap(),
                },
                Some(ScalarValue::Float64(Some(0.))),
            ),
            (
                StatisticsV2::Uniform {
                    interval: Interval::make_zero(&DataType::Float64).unwrap(),
                },
                Some(ScalarValue::Float64(Some(0.))),
            ),
            (
                StatisticsV2::Uniform {
                    interval: Interval::make(Some(1), Some(100)).unwrap(),
                },
                Some(ScalarValue::Float64(Some(50.5))),
            ),
            (
                StatisticsV2::Uniform {
                    interval: Interval::make(Some(-100), Some(-1)).unwrap(),
                },
                Some(ScalarValue::Float64(Some(-50.5))),
            ),
            (
                StatisticsV2::Uniform {
                    interval: Interval::make(Some(-100), Some(100)).unwrap(),
                },
                Some(ScalarValue::Float64(Some(0.))),
            ),
        ];
        //endregion

        //region exponential
        stats.push((
            StatisticsV2::Exponential {
                rate: ScalarValue::Float64(Some(2.)),
                offset: ScalarValue::Float64(Some(0.)),
            },
            Some(ScalarValue::Float64(Some(0.5))),
        ));
        //endregion

        // region gaussian
        stats.push((
            StatisticsV2::Gaussian {
                mean: ScalarValue::Float64(Some(0.)),
                variance: ScalarValue::Float64(Some(1.)),
            },
            Some(ScalarValue::Float64(Some(0.))),
        ));
        stats.push((
            StatisticsV2::Gaussian {
                mean: ScalarValue::Float64(Some(-2.)),
                variance: ScalarValue::Float64(Some(0.5)),
            },
            Some(ScalarValue::Float64(Some(-2.))),
        ));
        //endregion

        // region bernoulli
        stats.push((
            StatisticsV2::Bernoulli {
                p: ScalarValue::Null,
            },
            None,
        ));
        stats.push((
            StatisticsV2::Bernoulli {
                p: ScalarValue::Float64(Some(0.5)),
            },
            Some(ScalarValue::Float64(Some(0.5))),
        ));
        //endregion

        //region unknown
        stats.push((
            StatisticsV2::Unknown {
                mean: None,
                median: None,
                variance: None,
                range: Interval::make_zero(&DataType::Int8).unwrap(),
            },
            None,
        ));
        stats.push((
            // Median is None, the statistic is not valid, correct answer is None.
            StatisticsV2::Unknown {
                mean: Some(ScalarValue::Float64(Some(42.))),
                median: None,
                variance: None,
                range: Interval::make_zero(&DataType::Float64).unwrap(),
            },
            None,
        ));
        stats.push((
            // Range doesn't include mean and/or median, so - not valid
            StatisticsV2::Unknown {
                mean: Some(ScalarValue::Float64(Some(42.))),
                median: Some(ScalarValue::Float64(Some(42.))),
                variance: None,
                range: Interval::make_zero(&DataType::Float64).unwrap(),
            },
            None,
        ));
        stats.push((
            StatisticsV2::Unknown {
                mean: Some(ScalarValue::Float64(Some(42.))),
                median: Some(ScalarValue::Float64(Some(42.))),
                variance: None,
                range: Interval::make(Some(25.), Some(50.)).unwrap(),
            },
            Some(ScalarValue::Float64(Some(42.))),
        ));
        //endregion

        for case in stats {
            assert_eq!(case.0.mean().unwrap(), case.1);
        }
    }

    #[test]
    fn median_extraction_test() {
        // The test data is placed as follows: (stat -> expected answer)
        //region uniform
        let stats = vec![
            (
                StatisticsV2::Uniform {
                    interval: Interval::make_zero(&DataType::Int64).unwrap(),
                },
                Some(ScalarValue::Float64(Some(0.))),
            ),
            (
                StatisticsV2::Uniform {
                    interval: Interval::make(Some(25.), Some(75.)).unwrap(),
                },
                Some(ScalarValue::Float64(Some(50.))),
            ),
            (
                StatisticsV2::Exponential {
                    rate: ScalarValue::Float64(Some(2_f64.ln())),
                    offset: ScalarValue::Float64(Some(0.)),
                },
                Some(ScalarValue::Float64(Some(1.))),
            ),
            (
                StatisticsV2::Gaussian {
                    mean: ScalarValue::Float64(Some(2.)),
                    variance: ScalarValue::Float64(Some(1.)),
                },
                Some(ScalarValue::Float64(Some(2.))),
            ),
            (
                StatisticsV2::Bernoulli {
                    p: ScalarValue::Float64(Some(0.25)),
                },
                Some(ScalarValue::Float64(Some(0.))),
            ),
            (
                StatisticsV2::Bernoulli {
                    p: ScalarValue::Float64(Some(0.75)),
                },
                Some(ScalarValue::Float64(Some(1.))),
            ),
            (
                StatisticsV2::Gaussian {
                    mean: ScalarValue::Float64(Some(2.)),
                    variance: ScalarValue::Float64(Some(1.)),
                },
                Some(ScalarValue::Float64(Some(2.))),
            ),
            (
                StatisticsV2::Unknown {
                    mean: None,
                    median: None,
                    variance: None,
                    range: Interval::make_zero(&DataType::Int8).unwrap(),
                },
                None,
            ),
            (
                // Mean is None, statistics is not valid
                StatisticsV2::Unknown {
                    mean: None,
                    median: Some(ScalarValue::Float64(Some(12.))),
                    variance: None,
                    range: Interval::make_zero(&DataType::Float64).unwrap(),
                },
                None,
            ),
            (
                // Range doesn't include mean and/or median, so - not valid
                StatisticsV2::Unknown {
                    mean: None,
                    median: Some(ScalarValue::Float64(Some(12.))),
                    variance: None,
                    range: Interval::make_zero(&DataType::Float64).unwrap(),
                },
                None,
            ),
            (
                StatisticsV2::Unknown {
                    mean: Some(ScalarValue::Float64(Some(12.))),
                    median: Some(ScalarValue::Float64(Some(12.))),
                    variance: None,
                    range: Interval::make(Some(0.), Some(25.)).unwrap(),
                },
                Some(ScalarValue::Float64(Some(12.))),
            ),
        ];

        for case in stats {
            assert_eq!(case.0.median().unwrap(), case.1);
        }
    }

    #[test]
    fn variance_extraction_test() {
        // The test data is placed as follows : (stat -> expected answer)
        let stats = vec![
            (
                StatisticsV2::Uniform {
                    interval: Interval::make(Some(0.), Some(12.)).unwrap(),
                },
                Some(ScalarValue::Float64(Some(12.))),
            ),
            (
                StatisticsV2::Exponential {
                    rate: ScalarValue::Float64(Some(10.)),
                    offset: ScalarValue::Float64(Some(0.)),
                },
                Some(ScalarValue::Float64(Some(0.01))),
            ),
            (
                StatisticsV2::Gaussian {
                    mean: ScalarValue::Float64(Some(0.)),
                    variance: ScalarValue::Float64(Some(1.)),
                },
                Some(ScalarValue::Float64(Some(1.))),
            ),
            (
                StatisticsV2::Bernoulli {
                    p: ScalarValue::Float64(Some(0.5)),
                },
                Some(ScalarValue::Float64(Some(0.25))),
            ),
            (
                StatisticsV2::Unknown {
                    mean: None,
                    median: None,
                    variance: None,
                    range: Interval::make_zero(&DataType::Int8).unwrap(),
                },
                None,
            ),
            (
                StatisticsV2::Unknown {
                    mean: None,
                    median: None,
                    variance: Some(ScalarValue::Float64(Some(0.02))),
                    range: Interval::make_zero(&DataType::Float64).unwrap(),
                },
                Some(ScalarValue::Float64(Some(0.02))),
            ),
        ];

        //endregion

        for case in stats {
            assert_eq!(case.0.variance().unwrap(), case.1);
        }
    }
}
