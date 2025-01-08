use crate::interval_arithmetic::Interval;
use crate::stats::StatisticsV2::{Exponential, Gaussian, Unknown};
use datafusion_common::ScalarValue;

/// New, enhanced `Statistics` definition, represents three core definitions
pub enum StatisticsV2 {
    Uniform {
        interval: Interval,
    },
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
        std_dev: Option<ScalarValue>, // standard deviation
        range: Interval,
    },
}

impl StatisticsV2 {
    //! Validates accumulated statistic for selected distribution methods:
    //! - For [`Exponential`], `rate` must be positive;
    //! - For [`Gaussian`], `variant` must be non-negative
    //! - For [`Unknown`],
    //!   - if `mean`, `median` are defined, the `range` must contain their values
    //!   - if `std_dev` is defined, it must be non-negative
    pub fn is_valid(&self) -> bool {
        match &self {
            Exponential { rate, .. } => {
                if rate.is_null() { return false; }
                let zero = &ScalarValue::new_zero(&rate.data_type()).unwrap();
                rate.gt(zero)
            }
            Gaussian { variance, .. } => {
                if variance.is_null() { return false; }
                let zero = &ScalarValue::new_zero(&variance.data_type()).unwrap();
                variance.ge(zero)
            }
            Unknown {
                mean,
                median,
                std_dev,
                range,
            } => {
                if let (Some(mn), Some(md)) = (mean, median) {
                    if mn.is_null() || md.is_null() { return false; }
                    range.contains_value(mn).unwrap() && range.contains_value(md).unwrap()
                } else if let Some(dev) = std_dev {
                    if dev.is_null() { return false; }
                    dev.gt(&ScalarValue::new_zero(&dev.data_type()).unwrap())
                } else {
                    false
                }
            }
            _ => true,
        }
    }
}

// #[cfg(test)]
#[cfg(all(test, feature = "stats_v2"))]
mod tests {
    use crate::interval_arithmetic::Interval;
    use crate::stats::StatisticsV2;
    use arrow::datatypes::DataType;
    use datafusion_common::ScalarValue;

    #[test]
    fn uniform_stats_test() {
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
    fn exponential_stats_test() {
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
    fn gaussian_stats_test() {
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
    fn unknown_stats_test() {
        let unknown_stats = vec![
            (
                StatisticsV2::Unknown {
                    mean: None,
                    median: None,
                    std_dev: None,
                    range: Interval::make_zero(&DataType::Float32).unwrap(),
                },
                false,
            ),
            (
                StatisticsV2::Unknown {
                    mean: Some(ScalarValue::Float32(Some(0.))),
                    median: None,
                    std_dev: None,
                    range: Interval::make_zero(&DataType::Float32).unwrap(),
                },
                false,
            ),
            (
                StatisticsV2::Unknown {
                    mean: None,
                    median: Some(ScalarValue::Float32(Some(0.))),
                    std_dev: None,
                    range: Interval::make_zero(&DataType::Float32).unwrap(),
                },
                false,
            ),
            (
                StatisticsV2::Unknown {
                    mean: Some(ScalarValue::Null),
                    median: Some(ScalarValue::Null),
                    std_dev: Some(ScalarValue::Null),
                    range: Interval::make_zero(&DataType::Float32).unwrap(),
                },
                false,
            ),
            (
                StatisticsV2::Unknown {
                    mean: Some(ScalarValue::Float32(Some(0.))),
                    median: Some(ScalarValue::Float32(Some(0.))),
                    std_dev: None,
                    range: Interval::make_zero(&DataType::Float32).unwrap(),
                },
                true,
            ),
            (
                StatisticsV2::Unknown {
                    mean: Some(ScalarValue::Float64(Some(50.))),
                    median: Some(ScalarValue::Float64(Some(50.))),
                    std_dev: None,
                    range: Interval::make(Some(0.), Some(100.)).unwrap(),
                },
                true,
            ),
            (
                StatisticsV2::Unknown {
                    mean: Some(ScalarValue::Float64(Some(50.))),
                    median: Some(ScalarValue::Float64(Some(50.))),
                    std_dev: None,
                    range: Interval::make(Some(-100.), Some(0.)).unwrap(),
                },
                false,
            ),
            (
                StatisticsV2::Unknown {
                    mean: None,
                    median: None,
                    std_dev: Some(ScalarValue::Float64(Some(1.))),
                    range: Interval::make_zero(&DataType::Float64).unwrap()
                },
                true,
            ),
            (
                StatisticsV2::Unknown {
                    mean: None,
                    median: None,
                    std_dev: Some(ScalarValue::Float64(Some(-1.))),
                    range: Interval::make_zero(&DataType::Float64).unwrap()
                },
                false,
            ),
        ];
        for case in unknown_stats {
            assert_eq!(case.0.is_valid(), case.1);
        }
    }
}
