use datafusion_common::{plan_datafusion_err, plan_err, stats::Precision, Result};
use datafusion_expr::{Join, LogicalPlan};

pub trait JoinCostEstimator: std::fmt::Debug {
    fn cardinality(&self, plan: &LogicalPlan) -> Option<usize> {
        estimate_cardinality(plan).ok()
    }

    fn selectivity(&self, _join: &Join) -> f64 {
        0.1
    }

    fn cost(&self, selectivity: f64, cardinality: usize) -> f64 {
        selectivity * cardinality as f64
    }
}

/// Default implementation of JoinCostEstimator
#[derive(Debug, Clone, Copy)]
pub struct DefaultCostEstimator;

impl JoinCostEstimator for DefaultCostEstimator {}

fn estimate_cardinality(plan: &LogicalPlan) -> Result<usize> {
    match plan {
        LogicalPlan::Filter(filter) => {
            let input_cardinality = estimate_cardinality(&filter.input)?;
            Ok((0.1 * input_cardinality as f64) as usize)
        }
        LogicalPlan::Aggregate(agg) => {
            let input_cardinality = estimate_cardinality(&agg.input)?;
            Ok((0.1 * input_cardinality as f64) as usize)
        }
        LogicalPlan::TableScan(scan) => {
            let statistics = scan
                .source
                .statistics()
                .ok_or_else(|| plan_datafusion_err!("Table statistics not available"))?;
            if let Precision::Exact(num_rows) | Precision::Inexact(num_rows) =
                statistics.num_rows
            {
                Ok(num_rows)
            } else {
                plan_err!("Number of rows not available")
            }
        }
        x => {
            let inputs = x.inputs();
            if inputs.len() == 1 {
                estimate_cardinality(inputs[0])
            } else {
                plan_err!("Cannot estimate cardinality for plan with multiple inputs")
            }
        }
    }
}
