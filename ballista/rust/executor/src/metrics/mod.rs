use ballista_core::execution_plans::ShuffleWriterExec;
use datafusion::physical_plan::display::DisplayableExecutionPlan;

pub trait ExecutorMetricsCollector: Send + Sync {
    fn record_stage(
        &self,
        job_id: &str,
        stage_id: usize,
        partition: usize,
        plan: ShuffleWriterExec,
    );
}

#[derive(Default)]
pub struct LoggingMetricsCollector {}

impl ExecutorMetricsCollector for LoggingMetricsCollector {
    fn record_stage(
        &self,
        job_id: &str,
        stage_id: usize,
        partition: usize,
        plan: ShuffleWriterExec,
    ) {
        println!(
            "=== [{}/{}/{}] Physical plan with metrics ===\n{}\n",
            job_id,
            stage_id,
            partition,
            DisplayableExecutionPlan::with_metrics(&plan).indent()
        );
    }
}
