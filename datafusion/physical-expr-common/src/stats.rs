use arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr_common::interval_arithmetic::Interval;
use crate::stats::StatisticsV2::{Uniform, Exponential, Gaussian, Unknown};

/// New, enhanced `Statistics` definition, represents three core definitions
#[derive(Clone, Debug)]
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
    Unknown {
        mean: Option<ScalarValue>,
        median: Option<ScalarValue>,
        variance: Option<ScalarValue>,
        range: Interval,
    },
}

impl Default for StatisticsV2 {
    fn default() -> Self {
        Unknown {
            mean: None,
            median: None,
            variance: None,
            range: Interval::make_unbounded(&DataType::Null).unwrap(),
        }
    }
}

impl StatisticsV2 {
    pub fn new_unknown(&self) -> Self {
        Unknown {
            mean: None,
            median: None,
            variance: None,
            range: Interval::make_zero(&DataType::Null).unwrap(),
        }
    }

    /// Validates accumulated statistic for selected distribution methods:
    /// - For [`Exponential`], `rate` must be positive;
    /// - For [`Gaussian`], `variant` must be non-negative
    /// - For [`Unknown`],
    ///   - if `mean`, `median` are defined, the `range` must contain their values
    ///   - if `std_dev` is defined, it must be non-negative
    pub fn is_valid(&self) -> bool {
        match &self {
            Exponential { rate, .. } => {
                if rate.is_null() {
                    return false;
                }
                let zero = &ScalarValue::new_zero(&rate.data_type()).unwrap();
                rate.gt(zero)
            }
            Gaussian { variance, .. } => {
                if variance.is_null() {
                    return false;
                }
                let zero = &ScalarValue::new_zero(&variance.data_type()).unwrap();
                variance.ge(zero)
            }
            Unknown {
                mean,
                median,
                variance: std_dev,
                range,
            } => {
                if let (Some(mn), Some(md)) = (mean, median) {
                    if mn.is_null() || md.is_null() {
                        return false;
                    }
                    range.contains_value(mn).unwrap() && range.contains_value(md).unwrap()
                } else if let Some(dev) = std_dev {
                    if dev.is_null() {
                        return false;
                    }
                    dev.gt(&ScalarValue::new_zero(&dev.data_type()).unwrap())
                } else {
                    false
                }
            }
            _ => true,
        }
    }

    /// Extract the mean value of given statistic, available for given statistic kinds:
    /// - [`Uniform`]'s interval implicitly contains mean value, and it is calculable
    ///   by addition of upper and lower bound and dividing the result by 2.
    /// - [`Exponential`] distribution mean is calculable by formula: 1/λ. λ must be non-negative.
    /// - [`Gaussian`] distribution has it explicitly
    /// - [`Unknown`] distribution _may_ have it explicitly
    pub fn mean(&self) -> Option<ScalarValue> {
        if !self.is_valid() {
            return None;
        }
        match &self {
            Uniform { interval, .. } => {
                let aggregate = interval.lower().add_checked(interval.upper());
                if aggregate.is_err() {
                    // TODO: logs
                    return None;
                }

                let float_aggregate = aggregate.unwrap().cast_to(&DataType::Float64);
                if float_aggregate.is_err() {
                    // TODO: logs
                    return None;
                }

                if let Ok(mean) =
                    float_aggregate.unwrap().div(ScalarValue::Float64(Some(2.)))
                {
                    return Some(mean);
                }
                None
            }
            Exponential { rate, .. } => {
                let one = &ScalarValue::new_one(&rate.data_type()).unwrap();
                if let Ok(mean) = one.div(rate) {
                    return Some(mean);
                }
                None
            }
            Gaussian { mean, .. } => Some(mean.clone()),
            Unknown { mean, .. } => mean.clone(),
        }
    }

    /// Extract the median value of given statistic, available for given statistic kinds:
    /// - [`Exponential`] distribution median is calculable by formula: ln2/λ. λ must be non-negative.
    /// - [`Gaussian`] distribution median is equals to mean, which is present explicitly.
    /// - [`Unknown`] distribution median _may_ be present explicitly.
    pub fn median(&self) -> Option<ScalarValue> {
        if !self.is_valid() {
            return None;
        }
        match &self {
            Exponential { rate, .. } => {
                let ln_two = ScalarValue::Float64(Some(2_f64.ln()));
                if let Ok(median) = ln_two.div(rate) {
                    return Some(median);
                }
                None
            }
            Gaussian { mean, .. } => Some(mean.clone()),
            Unknown { median, .. } => median.clone(),
            _ => None,
        }
    }

    /// Extract the variance of given statistic distribution:
    /// - [`Uniform`]'s variance is equal to (upper-lower)^2 / 12
    /// - [`Exponential`]'s variance is equal to mean value and calculable as 1/λ^2
    /// - [`Gaussian`]'s variance is available explicitly
    /// - [`Unknown`]'s distribution variance _may_ be present explicitly.
    pub fn variance(&self) -> Option<ScalarValue> {
        if !self.is_valid() {
            return None;
        }
        match &self {
            Uniform { interval, .. } => {
                let base = interval.upper().sub_checked(interval.lower());
                if base.is_err() {
                    return None;
                }
                let base_value = &base.unwrap();
                let base_pow = base_value.mul_checked(base_value);
                if let Ok(res) = base_pow.unwrap().div(ScalarValue::Float64(Some(12.))) {
                    Some(res)
                } else {
                    None
                }
            }
            Exponential { rate, .. } => {
                let one = &ScalarValue::new_one(&rate.data_type()).unwrap();
                let rate_squared = rate.mul(rate);
                if rate_squared.is_err() {
                    return None;
                }
                if let Ok(variance) = one.div(rate_squared.unwrap()) {
                    return Some(variance);
                }
                None
            }
            Gaussian { variance, .. } => Some(variance.clone()),
            Unknown { variance, .. } => variance.clone()
        }
    }

    /// Extract the range of given statistic distribution:
    /// - [`Uniform`]'s range is its interval
    /// - [`Unknown`]'s range is unbounded by default, but
    pub fn range(&self) -> Option<&Interval> {
        match &self {
            Uniform { interval, .. } => Some(interval),
            Unknown { range, .. } => Some(range),
            _ => None,
        }
    }

    /// Returns data type of the statistics, if present
    /// explicitly, or able to determine implicitly.
    pub fn data_type(&self) -> Option<DataType> {
        match &self {
            Uniform { interval, .. } => Some(interval.data_type()),
            Unknown { range, .. } => Some(range.data_type()),
            _ => None,
        }
    }
}

#[cfg(test)]
// #[cfg(all(test, feature = "stats_v2"))]
mod tests {
    use crate::stats::StatisticsV2;
    use arrow::datatypes::DataType;
    use datafusion_common::ScalarValue;
    use datafusion_expr_common::interval_arithmetic::Interval;

    //region is_valid tests
    // The test data in the following tests are placed as follows : (stat -> expected answer)
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
            assert_eq!(case.0.is_valid(), case.1);
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
            assert_eq!(case.0.is_valid(), case.1);
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
            assert_eq!(case.0.is_valid(), case.1);
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
            assert_eq!(case.0.is_valid(), case.1);
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
            assert_eq!(case.0.mean(), case.1);
        }
    }

    #[test]
    fn median_extraction_test() {
        // The test data is placed as follows : (stat -> expected answer)
        //region uniform
        let stats = vec![
            (
                StatisticsV2::Uniform {
                    interval: Interval::make_zero(&DataType::Int64).unwrap(),
                },
                None,
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
            assert_eq!(case.0.median(), case.1);
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
            assert_eq!(case.0.variance(), case.1);
        }
    }
}