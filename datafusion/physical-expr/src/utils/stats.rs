use std::sync::Arc;
use arrow::datatypes::{DataType, Schema};
use petgraph::stable_graph::{NodeIndex, StableGraph};
use datafusion_expr_common::interval_arithmetic::Interval;
use crate::utils::stats::StatisticsV2::{Exponential, Gaussian, Uniform, Unknown};
use datafusion_common::ScalarValue;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_expr::Literal;

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
        std_dev: Option<ScalarValue>, // standard deviation
        range: Interval,
    },
}

impl StatisticsV2 {
    pub fn new_unknown(&self) -> Self {
        Unknown {
            mean: None,
            median: None,
            std_dev: None,
            range: Interval::make_zero(&DataType::Null).unwrap()
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
                std_dev,
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
            _ => unreachable!(),
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

    /// Extract the standard deviation of given statistic distribution:
    /// - [`Exponential`]'s standard deviation is equal to mean value and calculable as 1/λ
    /// - [`Gaussian`]'s standard deviation is a square root of variance.
    /// - [`Unknown`]'s distribution standard deviation _may_ be present explicitly.
    pub fn std_dev(&self) -> Option<ScalarValue> {
        if !self.is_valid() {
            return None;
        }
        match &self {
            Exponential { rate, .. } => {
                let one = &ScalarValue::new_one(&rate.data_type()).unwrap();
                if let Ok(std_dev) = one.div(rate) {
                    return Some(std_dev);
                }
                None
            }
            Gaussian {
                variance: _variance,
                ..
            } => {
                // TODO: sqrt() is not yet implemented for ScalarValue
                None
            }
            Unknown { std_dev, .. } => std_dev.clone(),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ExprStatisticGraphNode {
    expr: Arc<dyn PhysicalExpr>,
    statistics_v2: StatisticsV2
}

impl ExprStatisticGraphNode {
}

#[derive(Clone, Debug)]
pub struct ExprStatisticGraph {
    graph: StableGraph<ExprStatisticGraphNode, usize>,
    root: NodeIndex,
}

impl ExprStatisticGraph {
    pub fn try_new(expr: Arc<dyn PhysicalExpr>, schema: &Schema) /*-> Result<Self>*/ {}

    pub fn propagate_constraints(&mut self) {}

    pub fn evaluate(&mut self) {}
}

// #[cfg(test)]
#[cfg(all(test, feature = "stats_v2"))]
mod tests {
    use datafusion_expr_common::interval_arithmetic::Interval;
    use crate::utils::stats::StatisticsV2;
    use arrow::datatypes::DataType;
    use datafusion_common::ScalarValue;

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
                    range: Interval::make_zero(&DataType::Float64).unwrap(),
                },
                true,
            ),
            (
                StatisticsV2::Unknown {
                    mean: None,
                    median: None,
                    std_dev: Some(ScalarValue::Float64(Some(-1.))),
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
                std_dev: None,
                range: Interval::make_zero(&DataType::Int8).unwrap(),
            },
            None,
        ));
        stats.push((
            // Median is None, the statistic is not valid, correct answer is None.
            StatisticsV2::Unknown {
                mean: Some(ScalarValue::Float64(Some(42.))),
                median: None,
                std_dev: None,
                range: Interval::make_zero(&DataType::Float64).unwrap(),
            },
            None,
        ));
        stats.push((
            // Range doesn't include mean and/or median, so - not valid
            StatisticsV2::Unknown {
                mean: Some(ScalarValue::Float64(Some(42.))),
                median: Some(ScalarValue::Float64(Some(42.))),
                std_dev: None,
                range: Interval::make_zero(&DataType::Float64).unwrap(),
            },
            None,
        ));
        stats.push((
            StatisticsV2::Unknown {
                mean: Some(ScalarValue::Float64(Some(42.))),
                median: Some(ScalarValue::Float64(Some(42.))),
                std_dev: None,
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
                    std_dev: None,
                    range: Interval::make_zero(&DataType::Int8).unwrap(),
                },
                None,
            ),
            (
                // Mean is None, statistics is not valid
                StatisticsV2::Unknown {
                    mean: None,
                    median: Some(ScalarValue::Float64(Some(12.))),
                    std_dev: None,
                    range: Interval::make_zero(&DataType::Float64).unwrap(),
                },
                None,
            ),
            (
                // Range doesn't include mean and/or median, so - not valid
                StatisticsV2::Unknown {
                    mean: None,
                    median: Some(ScalarValue::Float64(Some(12.))),
                    std_dev: None,
                    range: Interval::make_zero(&DataType::Float64).unwrap(),
                },
                None,
            ),
            (
                StatisticsV2::Unknown {
                    mean: Some(ScalarValue::Float64(Some(12.))),
                    median: Some(ScalarValue::Float64(Some(12.))),
                    std_dev: None,
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
    fn std_dev_extraction_test() {
        // The test data is placed as follows : (stat -> expected answer)
        let stats = vec![
            (
                StatisticsV2::Uniform {
                    interval: Interval::make_zero(&DataType::Int64).unwrap(),
                },
                None,
            ),
            (
                StatisticsV2::Exponential {
                    rate: ScalarValue::Float64(Some(10.)),
                    offset: ScalarValue::Float64(Some(0.)),
                },
                Some(ScalarValue::Float64(Some(0.1))),
            ),
            (
                StatisticsV2::Gaussian {
                    mean: ScalarValue::Float64(Some(0.)),
                    variance: ScalarValue::Float64(Some(1.)),
                },
                None,
            ),
            (
                StatisticsV2::Unknown {
                    mean: None,
                    median: None,
                    std_dev: None,
                    range: Interval::make_zero(&DataType::Int8).unwrap(),
                },
                None,
            ),
            (
                StatisticsV2::Unknown {
                    mean: None,
                    median: None,
                    std_dev: Some(ScalarValue::Float64(Some(0.02))),
                    range: Interval::make_zero(&DataType::Float64).unwrap(),
                },
                Some(ScalarValue::Float64(Some(0.02))),
            ),
        ];

        //endregion

        for case in stats {
            assert_eq!(case.0.std_dev(), case.1);
        }
    }
}
