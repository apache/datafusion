use crate::metrics::{ExecutionPlanMetricsSet, MetricBuilder, Time};

pub struct GroupByMetrics {
    pub aggregate_arguments_time: Time,
    pub aggregation_time: Time,
    pub emitting_time: Time,
}

impl GroupByMetrics {
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            aggregate_arguments_time: MetricBuilder::new(metrics)
                .subset_time("aggregate_arguments_time", partition),
            aggregation_time: MetricBuilder::new(metrics)
                .subset_time("aggregation_time", partition),
            emitting_time: MetricBuilder::new(metrics)
                .subset_time("emitting_time", partition),
        }
    }
}
