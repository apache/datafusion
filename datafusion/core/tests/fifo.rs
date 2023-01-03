// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! This test demonstrates the DataFusion FIFO capabilities.
//!
#[cfg(not(target_os = "windows"))]
mod unix_test {
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::{
        prelude::{CsvReadOptions, SessionConfig, SessionContext},
        test_util::{aggr_test_schema, arrow_test_data},
    };
    use datafusion_common::{DataFusionError, Result};
    use futures::StreamExt;
    use itertools::enumerate;
    use nix::sys::stat;
    use nix::unistd;
    use rstest::*;
    use std::fs::{File, OpenOptions};
    use std::io::Write;
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::mpsc;
    use std::sync::mpsc::{Receiver, Sender};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;
    // !  For the sake of the test, do not alter the numbers. !
    // Session batch size
    const TEST_BATCH_SIZE: usize = 20;
    // Number of lines written to FIFO
    const TEST_DATA_SIZE: usize = 20_000;
    // Number of lines what can be joined. Each joinable key produced 20 lines with
    // aggregate_test_100 dataset. We will use these joinable keys for understanding
    // incremental execution.
    const TEST_JOIN_RATIO: f64 = 0.01;

    fn create_fifo_file(tmp_dir: &TempDir, file_name: &str) -> Result<PathBuf> {
        let file_path = tmp_dir.path().join(file_name);
        // Simulate an infinite environment via a FIFO file
        if let Err(e) = unistd::mkfifo(&file_path, stat::Mode::S_IRWXU) {
            Err(DataFusionError::Execution(e.to_string()))
        } else {
            Ok(file_path)
        }
    }

    fn write_to_fifo(
        mut file: &File,
        line: &str,
        ref_time: Instant,
        broken_pipe_timeout: Duration,
    ) -> Result<usize> {
        // We need to handle broken pipe error until the reader is ready. This
        // is why we use a timeout to limit the wait duration for the reader.
        // If the error is different than broken pipe, we fail immediately.
        file.write(line.as_bytes()).or_else(|e| {
            if e.raw_os_error().unwrap() == 32 {
                let interval = Instant::now().duration_since(ref_time);
                if interval < broken_pipe_timeout {
                    thread::sleep(Duration::from_millis(100));
                    return Ok(0);
                }
            }
            Err(DataFusionError::Execution(e.to_string()))
        })
    }

    async fn create_ctx(
        fifo_path: &Path,
        with_unbounded_execution: bool,
    ) -> Result<SessionContext> {
        let config = SessionConfig::new().with_batch_size(TEST_BATCH_SIZE);
        let ctx = SessionContext::with_config(config);
        // Register left table
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("a1", DataType::Utf8, false),
            Field::new("a2", DataType::UInt32, false),
        ]));
        ctx.register_csv(
            "left",
            fifo_path.as_os_str().to_str().unwrap(),
            CsvReadOptions::new()
                .schema(left_schema.as_ref())
                .has_header(false)
                .mark_infinite(with_unbounded_execution),
        )
        .await?;
        // Register right table
        let schema = aggr_test_schema();
        let test_data = arrow_test_data();
        ctx.register_csv(
            "right",
            &format!("{test_data}/csv/aggregate_test_100.csv"),
            CsvReadOptions::new().schema(schema.as_ref()),
        )
        .await?;
        Ok(ctx)
    }

    #[derive(Debug, PartialEq)]
    enum Operation {
        Read,
        Write,
    }

    /// Checks if there is a [Operation::Read] between [Operation::Write]s.
    /// This indicates we did not wait for the file to finish before processing it.
    fn interleave(result: &[Operation]) -> bool {
        let first_read = result.iter().position(|op| op == &Operation::Read);
        let last_write = result.iter().rev().position(|op| op == &Operation::Write);
        match (first_read, last_write) {
            (Some(first_read), Some(last_write)) => {
                result.len() - 1 - last_write > first_read
            }
            (_, _) => false,
        }
    }

    // This test provides a relatively realistic end-to-end scenario where
    // we ensure that we swap join sides correctly to accommodate a FIFO source.
    #[rstest]
    #[timeout(std::time::Duration::from_secs(30))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
    async fn unbounded_file_with_swapped_join(
        #[values(true, false)] unbounded_file: bool,
    ) -> Result<()> {
        // To make unbounded deterministic
        let waiting = Arc::new(Mutex::new(unbounded_file));
        let waiting_thread = waiting.clone();
        // Create a channel
        let (tx, rx): (Sender<Operation>, Receiver<Operation>) = mpsc::channel();
        // Create a new temporary FIFO file
        let tmp_dir = TempDir::new()?;
        let fifo_path = create_fifo_file(&tmp_dir, "fisrt_fifo.csv")?;
        // Prevent move
        let fifo_path_thread = fifo_path.clone();
        // Timeout for a long period of BrokenPipe error
        let broken_pipe_timeout = Duration::from_secs(5);
        // The sender endpoint can be copied
        let thread_tx = tx.clone();
        // Spawn a new thread to write to the FIFO file
        let fifo_writer = thread::spawn(move || {
            let first_file = OpenOptions::new()
                .write(true)
                .open(fifo_path_thread)
                .unwrap();
            // Reference time to use when deciding to fail the test
            let execution_start = Instant::now();
            // Execution can calculated at least one RecordBatch after the number of
            // "joinable_lines_length" lines are read.
            let joinable_lines_length =
                (TEST_DATA_SIZE as f64 * TEST_JOIN_RATIO).round() as usize;
            // The row including "a" is joinable with aggregate_test_100.c1
            let joinable_iterator = (0..joinable_lines_length).map(|_| "a".to_string());
            let second_joinable_iterator =
                (0..joinable_lines_length).map(|_| "a".to_string());
            // The row including "zzz" is not joinable with aggregate_test_100.c1
            let non_joinable_iterator =
                (0..(TEST_DATA_SIZE - joinable_lines_length)).map(|_| "zzz".to_string());
            let string_array = joinable_iterator
                .chain(non_joinable_iterator)
                .chain(second_joinable_iterator);
            for (cnt, string_col) in enumerate(string_array) {
                // Wait a reading sign for unbounded execution
                // For unbounded execution:
                //  After joinable_lines_length FIFO reading, we MUST get a Operation::Read.
                // For bounded execution:
                //  Never goes into while loop since waiting_thread initiated as false.
                while *waiting_thread.lock().unwrap() && joinable_lines_length < cnt {
                    thread::sleep(Duration::from_millis(200));
                }
                // Each thread queues a message in the channel
                if cnt % TEST_BATCH_SIZE == 0 {
                    thread_tx.send(Operation::Write).unwrap();
                }
                let line = format!("{string_col},{cnt}\n").to_owned();
                write_to_fifo(&first_file, &line, execution_start, broken_pipe_timeout)
                    .unwrap();
            }
        });
        // Collects operations from both writer and executor.
        let result_collector = thread::spawn(move || {
            let mut results = vec![];
            while let Ok(res) = rx.recv() {
                results.push(res);
            }
            results
        });
        // Create an execution case with bounded or unbounded flag.
        let ctx = create_ctx(&fifo_path, unbounded_file).await?;
        // Execute the query
        let df = ctx.sql("SELECT t1.a2, t2.c1, t2.c4, t2.c5 FROM left as t1 JOIN right as t2 ON t1.a1 = t2.c1").await?;
        let mut stream = df.execute_stream().await?;
        while (stream.next().await).is_some() {
            *waiting.lock().unwrap() = false;
            tx.send(Operation::Read).unwrap();
        }
        fifo_writer.join().unwrap();
        drop(tx);
        let result = result_collector.join().unwrap();
        assert_eq!(interleave(&result), unbounded_file);
        Ok(())
    }
}
