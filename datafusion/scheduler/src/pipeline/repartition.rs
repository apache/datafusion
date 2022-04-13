use std::collections::VecDeque;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use arrow::array::ArrayRef;
use parking_lot::Mutex;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::{Partitioning, PhysicalExpr};

use crate::ArrowResult;
use crate::pipeline::Pipeline;

/// A [`Pipeline`] that can repartition its input
#[derive(Debug)]
pub struct RepartitionPipeline {
    output: Partitioning,
    state: Mutex<RepartitionState>,
}

impl RepartitionPipeline {
    /// Create a new [`RepartitionPipeline`] with the given `input` and `output` partitioning
    pub fn new(input: Partitioning, output: Partitioning) -> Self {
        let input_count = input.partition_count();
        assert_ne!(input_count, 0);

        let num_partitions = match output {
            Partitioning::RoundRobinBatch(num_partitions) => num_partitions,
            Partitioning::Hash(_, num_partitions) => num_partitions,
            Partitioning::UnknownPartitioning(_) => unreachable!(),
        };
        assert_ne!(num_partitions, 0);

        let state = Mutex::new(RepartitionState {
            next_idx: 0,
            hash_buffer: vec![],
            partition_closed: vec![false; input_count],
            input_closed: false,
            output_buffers: (0..num_partitions).map(|_| Default::default()).collect(),
        });

        Self { output, state }
    }
}

// TODO: Explore per-partition state for better cache locality
#[derive(Debug)]
struct RepartitionState {
    // TODO: Split this into an enum based on output type
    next_idx: usize,
    hash_buffer: Vec<u64>,
    partition_closed: Vec<bool>,
    input_closed: bool,
    output_buffers: Vec<OutputBuffer>,
}

impl RepartitionState {
    fn push_batch(&mut self, partition: usize, batch: RecordBatch) {
        let buffer = &mut self.output_buffers[partition];

        buffer.batches.push_back(batch);

        for waker in buffer.wait_list.drain(..) {
            waker.wake()
        }
    }

    fn hash_batch(
        &mut self,
        exprs: &[Arc<dyn PhysicalExpr>],
        input: RecordBatch,
    ) -> Result<()> {
        let arrays = exprs
            .iter()
            .map(|expr| Ok(expr.evaluate(&input)?.into_array(input.num_rows())))
            .collect::<Result<Vec<_>>>()?;

        self.hash_buffer.clear();
        self.hash_buffer.resize(input.num_rows(), 0);

        // Use a fixed random state
        let random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);

        datafusion::physical_plan::hash_utils::create_hashes(
            &arrays,
            &random_state,
            &mut self.hash_buffer,
        )?;

        let num_output_partitions = self.output_buffers.len();

        let mut indices = vec![vec![]; num_output_partitions];
        for (index, hash) in self.hash_buffer.iter().enumerate() {
            indices[(*hash % num_output_partitions as u64) as usize].push(index as u64)
        }

        for (partition, indices) in indices.into_iter().enumerate() {
            if indices.is_empty() {
                continue;
            }

            let indices = indices.into();
            // Produce batches based on indices
            let columns = input
                .columns()
                .iter()
                .map(|c| {
                    arrow::compute::take(c.as_ref(), &indices, None)
                        .map_err(DataFusionError::ArrowError)
                })
                .collect::<Result<Vec<ArrayRef>>>()?;

            let batch = RecordBatch::try_new(input.schema(), columns).unwrap();
            self.push_batch(partition, batch);
        }

        Ok(())
    }
}

impl Pipeline for RepartitionPipeline {
    fn push(&self, input: RecordBatch, child: usize, partition: usize) {
        assert_eq!(child, 0);

        let mut state = self.state.lock();
        assert!(!state.partition_closed[partition]);

        match &self.output {
            Partitioning::RoundRobinBatch(_) => {
                let idx = state.next_idx;
                state.next_idx = (state.next_idx + 1) % state.output_buffers.len();
                state.push_batch(idx, input);
            }
            Partitioning::Hash(exprs, _) => {
                // TODO: Error handling
                state.hash_batch(exprs, input).unwrap();
            }
            Partitioning::UnknownPartitioning(_) => unreachable!(),
        }
    }

    fn close(&self, child: usize, partition: usize) {
        assert_eq!(child, 0);

        let mut state = self.state.lock();
        assert!(!state.partition_closed[partition]);
        state.partition_closed[partition] = true;

        // If all input streams exhausted, wake outputs
        if state.partition_closed.iter().all(|x| *x) {
            state.input_closed = true;
            for buffer in &mut state.output_buffers {
                for waker in buffer.wait_list.drain(..) {
                    waker.wake()
                }
            }
        }
    }

    fn output_partitions(&self) -> usize {
        self.output.partition_count()
    }

    fn poll_partition(
        &self,
        cx: &mut Context<'_>,
        partition: usize,
    ) -> Poll<Option<ArrowResult<RecordBatch>>> {
        let mut state = self.state.lock();
        let input_closed = state.input_closed;
        let buffer = &mut state.output_buffers[partition];

        match buffer.batches.pop_front() {
            Some(batch) => Poll::Ready(Some(Ok(batch))),
            None if input_closed => Poll::Ready(None),
            _ => {
                buffer.wait_list.push(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

#[derive(Debug, Default)]
struct OutputBuffer {
    batches: VecDeque<RecordBatch>,
    wait_list: Vec<Waker>,
}
