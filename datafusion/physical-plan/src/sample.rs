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

//! Defines the SAMPLE operator

use rand_distr::{Distribution, Poisson};
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::{
    DisplayAs, ExecutionPlanProperties, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use crate::{
    metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
    DisplayFormatType, ExecutionPlan,
};

use arrow::array::UInt32Array;
use arrow::compute;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{internal_err, plan_datafusion_err, Result};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::EquivalenceProperties;

use futures::stream::{Stream, StreamExt};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

trait Sampler: Send + Sync {
    fn sample(&mut self, batch: &RecordBatch) -> Result<RecordBatch>;
}

struct BernoulliSampler {
    lower_bound: f64,
    upper_bound: f64,
    rng: StdRng,
}

impl BernoulliSampler {
    fn new(lower_bound: f64, upper_bound: f64, seed: u64) -> Self {
        Self {
            lower_bound,
            upper_bound,
            rng: StdRng::seed_from_u64(seed),
        }
    }
}

impl Sampler for BernoulliSampler {
    fn sample(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        if self.upper_bound <= self.lower_bound {
            return Ok(RecordBatch::new_empty(batch.schema()));
        }

        let mut indices = Vec::new();

        for i in 0..batch.num_rows() {
            let rnd: f64 = self.rng.random();

            if rnd >= self.lower_bound && rnd < self.upper_bound {
                indices.push(i as u32);
            }
        }

        if indices.is_empty() {
            return Ok(RecordBatch::new_empty(batch.schema()));
        }
        let indices = UInt32Array::from(indices);
        compute::take_record_batch(batch, &indices).map_err(|e| e.into())
    }
}

struct PoissonSampler {
    ratio: f64,
    poisson: Poisson<f64>,
    rng: StdRng,
}

impl PoissonSampler {
    fn try_new(ratio: f64, seed: u64) -> Result<Self> {
        let poisson = Poisson::new(ratio).map_err(|e| plan_datafusion_err!("{}", e))?;
        Ok(Self {
            ratio,
            poisson,
            rng: StdRng::seed_from_u64(seed),
        })
    }
}

impl Sampler for PoissonSampler {
    fn sample(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        if self.ratio <= 0.0 {
            return Ok(RecordBatch::new_empty(batch.schema()));
        }

        let mut indices = Vec::new();

        for i in 0..batch.num_rows() {
            let k = self.poisson.sample(&mut self.rng) as i32;
            for _ in 0..k {
                indices.push(i as u32);
            }
        }

        if indices.is_empty() {
            return Ok(RecordBatch::new_empty(batch.schema()));
        }

        let indices = UInt32Array::from(indices);
        compute::take_record_batch(batch, &indices).map_err(|e| e.into())
    }
}

/// SampleExec samples rows from its input based on a sampling method.
/// This is used to implement SQL `SAMPLE` clause.
#[derive(Debug, Clone)]
pub struct SampleExec {
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
    /// The lower bound of the sampling ratio
    lower_bound: f64,
    /// The upper bound of the sampling ratio
    upper_bound: f64,
    /// Whether to sample with replacement
    with_replacement: bool,
    /// Random seed for reproducible sampling
    seed: u64,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Properties equivalence properties, partitioning, etc.
    cache: PlanProperties,
}

impl SampleExec {
    /// Create a new SampleExec with a custom sampling method
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        lower_bound: f64,
        upper_bound: f64,
        with_replacement: bool,
        seed: u64,
    ) -> Result<Self> {
        if lower_bound < 0.0 || upper_bound > 1.0 || lower_bound > upper_bound {
            return internal_err!(
                "Sampling bounds must be between 0.0 and 1.0, and lower_bound <= upper_bound, got [{}, {}]",
                lower_bound, upper_bound
            );
        }

        let cache = Self::compute_properties(&input);

        Ok(Self {
            input,
            lower_bound,
            upper_bound,
            with_replacement,
            seed,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        })
    }

    fn create_sampler(&self, partition: usize) -> Result<Box<dyn Sampler>> {
        if self.with_replacement {
            Ok(Box::new(PoissonSampler::try_new(
                self.upper_bound - self.lower_bound,
                self.seed + partition as u64,
            )?))
        } else {
            Ok(Box::new(BernoulliSampler::new(
                self.lower_bound,
                self.upper_bound,
                self.seed + partition as u64,
            )))
        }
    }

    /// Whether to sample with replacement
    pub fn with_replacement(&self) -> bool {
        self.with_replacement
    }

    /// The lower bound of the sampling ratio
    pub fn lower_bound(&self) -> f64 {
        self.lower_bound
    }

    /// The upper bound of the sampling ratio
    pub fn upper_bound(&self) -> f64 {
        self.upper_bound
    }

    /// The random seed
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// The input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(input: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        )
    }
}

impl DisplayAs for SampleExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "SampleExec: lower_bound={}, upper_bound={}, with_replacement={}, seed={}",
                    self.lower_bound, self.upper_bound, self.with_replacement, self.seed
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "SampleExec: lower_bound={}, upper_bound={}, with_replacement={}, seed={}",
                    self.lower_bound, self.upper_bound, self.with_replacement, self.seed
                )
            }
        }
    }
}

impl ExecutionPlan for SampleExec {
    fn name(&self) -> &'static str {
        "SampleExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false] // Sampling does not maintain input order
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Get the ratio from the current method
        Ok(Arc::new(SampleExec::try_new(
            Arc::clone(&children[0]),
            self.lower_bound,
            self.upper_bound,
            self.with_replacement,
            self.seed,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        Ok(Box::pin(SampleExecStream {
            input: input_stream,
            sampler: self.create_sampler(partition)?,
            baseline_metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        let input_stats = self.input.partition_statistics(partition)?;

        // Apply sampling ratio to statistics
        let mut stats = input_stats;
        let ratio = self.upper_bound - self.lower_bound;

        stats.num_rows = stats
            .num_rows
            .map(|nr| (nr as f64 * ratio) as usize)
            .to_inexact();
        stats.total_byte_size = stats
            .total_byte_size
            .map(|tb| (tb as f64 * ratio) as usize)
            .to_inexact();

        Ok(stats)
    }
}

/// Stream for the SampleExec operator
struct SampleExecStream {
    /// The input stream
    input: SendableRecordBatchStream,
    /// The sampling method
    sampler: Box<dyn Sampler>,
    /// Runtime metrics recording
    baseline_metrics: BaselineMetrics,
}

impl Stream for SampleExecStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.input.poll_next_unpin(cx);
        let baseline_metrics = &mut self.baseline_metrics;

        match poll {
            Poll::Ready(Some(Ok(batch))) => {
                let start = baseline_metrics.elapsed_compute().clone();
                let result = self.sampler.sample(&batch);
                let _timer = start.timer();
                Poll::Ready(Some(result))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for SampleExecStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use datafusion_execution::TaskContext;
    use futures::TryStreamExt;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_sample_exec_bernoulli() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", arrow::datatypes::DataType::Int32, false),
            Field::new("name", arrow::datatypes::DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            ],
        )?;

        let input = Arc::new(crate::test::TestMemoryExec::try_new(
            &[vec![batch]],
            schema.clone(),
            None,
        )?);

        let sample_exec = SampleExec::try_new(input, 0.6, 1.0, false, 42)?;

        let context = Arc::new(TaskContext::default());
        let stream = sample_exec.execute(0, context)?;

        let batches = stream.try_collect::<Vec<_>>().await?;
        assert!(!batches.is_empty());

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // With 60% sampling ratio and 5 input rows, we expect around 3 rows
        assert!(total_rows >= 2 && total_rows <= 4);

        Ok(())
    }

    #[tokio::test]
    async fn test_sample_exec_poisson() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            arrow::datatypes::DataType::Int32,
            false,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )?;

        let input = Arc::new(crate::test::TestMemoryExec::try_new(
            &[vec![batch]],
            schema.clone(),
            None,
        )?);

        let sample_exec = SampleExec::try_new(input, 0.0, 0.5, true, 42)?;

        let context = Arc::new(TaskContext::default());
        let stream = sample_exec.execute(0, context)?;

        let batches = stream.try_collect::<Vec<_>>().await?;
        assert!(!batches.is_empty());

        Ok(())
    }

    #[test]
    fn test_sampler_trait() {
        let mut bernoulli_sampler = BernoulliSampler::new(0.0, 0.5, 42);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "id",
                arrow::datatypes::DataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();

        let result = bernoulli_sampler.sample(&batch);
        assert!(result.is_ok());

        let mut poisson_sampler = PoissonSampler::try_new(0.5, 42).unwrap();
        let result = poisson_sampler.sample(&batch);
        assert!(result.is_ok());
    }
}
