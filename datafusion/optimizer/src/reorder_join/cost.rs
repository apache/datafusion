use datafusion_common::{plan_datafusion_err, plan_err, stats::Precision, Result};
use datafusion_expr::{Join, JoinType, LogicalPlan};

pub trait JoinCostEstimator: std::fmt::Debug {
    fn cardinality(&self, plan: &LogicalPlan) -> Option<f64> {
        estimate_cardinality(plan).ok()
    }

    fn selectivity(&self, join: &Join) -> f64 {
        match join.join_type {
            JoinType::Inner => 0.1,
            _ => 1.0,
        }
    }

    fn cost(&self, selectivity: f64, cardinality: f64) -> f64 {
        selectivity * cardinality
    }
}

/// Default implementation of JoinCostEstimator
#[derive(Debug, Clone, Copy)]
pub struct DefaultCostEstimator;

impl JoinCostEstimator for DefaultCostEstimator {}

fn estimate_cardinality(plan: &LogicalPlan) -> Result<f64> {
    match plan {
        LogicalPlan::Filter(filter) => {
            let input_cardinality = estimate_cardinality(&filter.input)?;
            Ok(0.1 * input_cardinality)
        }
        LogicalPlan::Aggregate(agg) => {
            let input_cardinality = estimate_cardinality(&agg.input)?;
            Ok(0.1 * input_cardinality)
        }
        LogicalPlan::TableScan(scan) => {
            let statistics = scan
                .source
                .statistics()
                .ok_or_else(|| plan_datafusion_err!("Table statistics not available"))?;
            if let Precision::Exact(num_rows) | Precision::Inexact(num_rows) =
                statistics.num_rows
            {
                Ok(num_rows as f64)
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
