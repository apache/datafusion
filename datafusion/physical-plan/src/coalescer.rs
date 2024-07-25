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

use arrow::compute::concat_batches;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use std::sync::Arc;

/// Concatenate multiple record batches into larger batches
///
/// See [`CoalesceBatchesExec`] for more details.
///
/// Notes:
///
/// 1. The output rows is the same order as the input rows
///
/// 2. The output is a sequence of batches, with all but the last being at least
/// `target_batch_size` rows.
///
/// 3. This structure also handles other optimizations such as a
/// combined filter/coalesce operation.
#[derive(Debug)]
pub struct BatchCoalescer {
    /// The input schema
    schema: SchemaRef,
    /// Minimum number of rows for coalesces batches
    target_batch_size: usize,
    /// Buffered batches
    buffer: Vec<RecordBatch>,
    /// Buffered row count
    buffered_rows: usize,
}

impl BatchCoalescer {
    /// Create a new BatchCoalescer that produces batches of at least `target_batch_size` rows
    pub fn new(schema: SchemaRef, target_batch_size: usize) -> Self {
        Self {
            schema,
            target_batch_size,
            buffer: vec![],
            buffered_rows: 0,
        }
    }

    /// Return the schema of the output batches
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Add a batch to the coalescer, returning a batch if the target batch size is reached
    pub fn push_batch(
        &mut self,
        batch: RecordBatch,
    ) -> datafusion_common::Result<Option<RecordBatch>> {
        if batch.num_rows() >= self.target_batch_size && self.buffer.is_empty() {
            return Ok(Some(batch));
        }
        // discard empty batches
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        // add to the buffered batches
        self.buffered_rows += batch.num_rows();
        self.buffer.push(batch);
        // check to see if we have enough batches yet
        let batch = if self.buffered_rows >= self.target_batch_size {
            // combine the batches and return
            let batch = concat_batches(&self.schema, &self.buffer)?;
            // reset buffer state
            self.buffer.clear();
            self.buffered_rows = 0;
            // return batch
            Some(batch)
        } else {
            None
        };
        Ok(batch)
    }

    /// Finish the coalescing process, returning all buffered data as a final,
    /// single batch, if any
    pub fn finish(&mut self) -> datafusion_common::Result<Option<RecordBatch>> {
        if self.buffer.is_empty() {
            Ok(None)
        } else {
            // combine the batches and return
            let batch = concat_batches(&self.schema, &self.buffer)?;
            // reset buffer state
            self.buffer.clear();
            self.buffered_rows = 0;
            // return batch
            Ok(Some(batch))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::UInt32Array;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concat_batches() -> datafusion_common::Result<()> {
        let Scenario { schema, batch } = uint32_scenario();

        // input is 10 batches x 8 rows (80 rows)
        let input = std::iter::repeat(batch).take(10);

        // expected output is batches of at least 20 rows (except for the final batch)
        let batches = do_coalesce_batches(&schema, input, 21);
        assert_eq!(4, batches.len());
        assert_eq!(24, batches[0].num_rows());
        assert_eq!(24, batches[1].num_rows());
        assert_eq!(24, batches[2].num_rows());
        assert_eq!(8, batches[3].num_rows());

        Ok(())
    }

    // Coalesce the batches with a BatchCoalescer  function with the given input
    // and target batch size returning the resulting batches
    fn do_coalesce_batches(
        schema: &SchemaRef,
        input: impl IntoIterator<Item = RecordBatch>,
        target_batch_size: usize,
    ) -> Vec<RecordBatch> {
        // create physical plan
        let mut coalescer = BatchCoalescer::new(Arc::clone(schema), target_batch_size);
        let mut output_batches: Vec<_> = input
            .into_iter()
            .filter_map(|batch| coalescer.push_batch(batch).unwrap())
            .collect();
        if let Some(batch) = coalescer.finish().unwrap() {
            output_batches.push(batch);
        }
        output_batches
    }

    /// Test scenario
    #[derive(Debug)]
    struct Scenario {
        schema: Arc<Schema>,
        batch: RecordBatch,
    }

    /// a batch of 8 rows of UInt32
    fn uint32_scenario() -> Scenario {
        let schema =
            Arc::new(Schema::new(vec![Field::new("c0", DataType::UInt32, false)]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]))],
        )
        .unwrap();

        Scenario { schema, batch }
    }
}
