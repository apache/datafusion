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
#[cfg(test)]
mod unix_test {
    use arrow::array::Array;
    use arrow::csv::ReaderBuilder;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::test_util::register_unbounded_file_with_ordering;
    use datafusion::{
        prelude::{CsvReadOptions, SessionConfig, SessionContext},
        test_util::{aggr_test_schema, arrow_test_data},
    };
    use datafusion_common::{exec_err, DataFusionError, Result};
    use futures::StreamExt;
    use itertools::enumerate;
    use nix::sys::stat;
    use nix::unistd;
    use rstest::*;
    use std::fs::{File, OpenOptions};
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::thread::JoinHandle;
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
            exec_err!("{}", e)
        } else {
            Ok(file_path)
        }
    }

    fn write_to_fifo(
        mut file: &File,
        line: &str,
        ref_time: Instant,
        broken_pipe_timeout: Duration,
    ) -> Result<()> {
        // We need to handle broken pipe error until the reader is ready. This
        // is why we use a timeout to limit the wait duration for the reader.
        // If the error is different than broken pipe, we fail immediately.
        while let Err(e) = file.write_all(line.as_bytes()) {
            if e.raw_os_error().unwrap() == 32 {
                let interval = Instant::now().duration_since(ref_time);
                if interval < broken_pipe_timeout {
                    thread::sleep(Duration::from_millis(100));
                    continue;
                }
            }
            return exec_err!("{}", e);
        }
        Ok(())
    }

    // This test provides a relatively realistic end-to-end scenario where
    // we swap join sides to accommodate a FIFO source.
    #[rstest]
    #[timeout(std::time::Duration::from_secs(30))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn unbounded_file_with_swapped_join(
        #[values(true, false)] unbounded_file: bool,
    ) -> Result<()> {
        // Create session context
        let config = SessionConfig::new()
            .with_batch_size(TEST_BATCH_SIZE)
            .with_collect_statistics(false)
            .with_target_partitions(1);
        let ctx = SessionContext::new_with_config(config);
        // To make unbounded deterministic
        let waiting = Arc::new(AtomicBool::new(unbounded_file));
        // Create a new temporary FIFO file
        let tmp_dir = TempDir::new()?;
        let fifo_path =
            create_fifo_file(&tmp_dir, &format!("fifo_{unbounded_file:?}.csv"))?;
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
        let lines = joinable_iterator
            .chain(non_joinable_iterator)
            .chain(second_joinable_iterator)
            .zip(0..TEST_DATA_SIZE)
            .map(|(a1, a2)| format!("{a1},{a2}\n"))
            .collect::<Vec<_>>();
        // Create writing threads for the left and right FIFO files
        let task = create_writing_thread(
            fifo_path.clone(),
            "a1,a2\n".to_owned(),
            lines,
            waiting.clone(),
            joinable_lines_length,
        );

        // Data Schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("a1", DataType::Utf8, false),
            Field::new("a2", DataType::UInt32, false),
        ]));
        // Create a file with bounded or unbounded flag.
        ctx.register_csv(
            "left",
            fifo_path.as_os_str().to_str().unwrap(),
            CsvReadOptions::new()
                .schema(schema.as_ref())
                .mark_infinite(unbounded_file),
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
        // Execute the query
        let df = ctx.sql("SELECT t1.a2, t2.c1, t2.c4, t2.c5 FROM left as t1 JOIN right as t2 ON t1.a1 = t2.c1").await?;
        let mut stream = df.execute_stream().await?;
        while (stream.next().await).is_some() {
            waiting.store(false, Ordering::SeqCst);
        }
        task.join().unwrap();
        Ok(())
    }

    #[derive(Debug, PartialEq)]
    enum JoinOperation {
        LeftUnmatched,
        RightUnmatched,
        Equal,
    }

    fn create_writing_thread(
        file_path: PathBuf,
        header: String,
        lines: Vec<String>,
        waiting_lock: Arc<AtomicBool>,
        wait_until: usize,
    ) -> JoinHandle<()> {
        // Timeout for a long period of BrokenPipe error
        let broken_pipe_timeout = Duration::from_secs(10);
        // Spawn a new thread to write to the FIFO file
        thread::spawn(move || {
            let file = OpenOptions::new().write(true).open(file_path).unwrap();
            // Reference time to use when deciding to fail the test
            let execution_start = Instant::now();
            write_to_fifo(&file, &header, execution_start, broken_pipe_timeout).unwrap();
            for (cnt, line) in enumerate(lines) {
                while waiting_lock.load(Ordering::SeqCst) && cnt > wait_until {
                    thread::sleep(Duration::from_millis(50));
                }
                write_to_fifo(&file, &line, execution_start, broken_pipe_timeout)
                    .unwrap();
            }
            drop(file);
        })
    }

    // This test provides a relatively realistic end-to-end scenario where
    // we change the join into a [SymmetricHashJoin] to accommodate two
    // unbounded (FIFO) sources.
    #[rstest]
    #[timeout(std::time::Duration::from_secs(30))]
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn unbounded_file_with_symmetric_join() -> Result<()> {
        // Create session context
        let config = SessionConfig::new()
            .with_batch_size(TEST_BATCH_SIZE)
            .set_bool("datafusion.execution.coalesce_batches", false)
            .with_target_partitions(1);
        let ctx = SessionContext::new_with_config(config);
        // Tasks
        let mut tasks: Vec<JoinHandle<()>> = vec![];

        // Join filter
        let a1_iter = 0..TEST_DATA_SIZE;
        // Join key
        let a2_iter = (0..TEST_DATA_SIZE).map(|x| x % 10);
        let lines = a1_iter
            .zip(a2_iter)
            .map(|(a1, a2)| format!("{a1},{a2}\n"))
            .collect::<Vec<_>>();

        // Create a new temporary FIFO file
        let tmp_dir = TempDir::new()?;
        // Create a FIFO file for the left input source.
        let left_fifo = create_fifo_file(&tmp_dir, "left.csv")?;
        // Create a FIFO file for the right input source.
        let right_fifo = create_fifo_file(&tmp_dir, "right.csv")?;
        // Create a mutex for tracking if the right input source is waiting for data.
        let waiting = Arc::new(AtomicBool::new(true));

        // Create writing threads for the left and right FIFO files
        tasks.push(create_writing_thread(
            left_fifo.clone(),
            "a1,a2\n".to_owned(),
            lines.clone(),
            waiting.clone(),
            TEST_BATCH_SIZE,
        ));
        tasks.push(create_writing_thread(
            right_fifo.clone(),
            "a1,a2\n".to_owned(),
            lines.clone(),
            waiting.clone(),
            TEST_BATCH_SIZE,
        ));

        // Create schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("a1", DataType::UInt32, false),
            Field::new("a2", DataType::UInt32, false),
        ]));
        // Specify the ordering:
        let file_sort_order = vec![[datafusion_expr::col("a1")]
            .into_iter()
            .map(|e| {
                let ascending = true;
                let nulls_first = false;
                e.sort(ascending, nulls_first)
            })
            .collect::<Vec<_>>()];
        // Set unbounded sorted files read configuration
        register_unbounded_file_with_ordering(
            &ctx,
            schema.clone(),
            &left_fifo,
            "left",
            file_sort_order.clone(),
            true,
        )
        .await?;
        register_unbounded_file_with_ordering(
            &ctx,
            schema,
            &right_fifo,
            "right",
            file_sort_order,
            true,
        )
        .await?;
        // Execute the query, with no matching rows. (since key is modulus 10)
        let df = ctx
            .sql(
                "SELECT
                                      t1.a1,
                                      t1.a2,
                                      t2.a1,
                                      t2.a2
                                    FROM
                                      left as t1 FULL
                                      JOIN right as t2 ON t1.a2 = t2.a2
                                      AND t1.a1 > t2.a1 + 4
                                      AND t1.a1 < t2.a1 + 9",
            )
            .await?;
        let mut stream = df.execute_stream().await?;
        let mut operations = vec![];
        // Partial.
        while let Some(Ok(batch)) = stream.next().await {
            waiting.store(false, Ordering::SeqCst);
            let left_unmatched = batch.column(2).null_count();
            let right_unmatched = batch.column(0).null_count();
            let op = if left_unmatched == 0 && right_unmatched == 0 {
                JoinOperation::Equal
            } else if right_unmatched > left_unmatched {
                JoinOperation::RightUnmatched
            } else {
                JoinOperation::LeftUnmatched
            };
            operations.push(op);
        }
        tasks.into_iter().for_each(|jh| jh.join().unwrap());
        // The SymmetricHashJoin executor produces FULL join results at every
        // pruning, which happens before it reaches the end of input and more
        // than once. In this test, we feed partially joinable data to both
        // sides in order to ensure that left or right unmatched results are
        // generated more than once during the test.
        assert!(
            operations
                .iter()
                .filter(|&n| JoinOperation::RightUnmatched.eq(n))
                .count()
                > 1
                && operations
                    .iter()
                    .filter(|&n| JoinOperation::LeftUnmatched.eq(n))
                    .count()
                    > 1
        );
        Ok(())
    }

    /// It tests the INSERT INTO functionality.
    #[tokio::test]
    #[ignore]
    async fn test_sql_insert_into_fifo() -> Result<()> {
        // To make unbounded deterministic
        let waiting = Arc::new(AtomicBool::new(true));
        let waiting_thread = waiting.clone();
        // create local execution context
        let config = SessionConfig::new().with_batch_size(TEST_BATCH_SIZE);
        let ctx = SessionContext::new_with_config(config);
        // Create a new temporary FIFO file
        let tmp_dir = TempDir::new()?;
        let source_fifo_path = create_fifo_file(&tmp_dir, "source.csv")?;
        // Prevent move
        let (source_fifo_path_thread, source_display_fifo_path) =
            (source_fifo_path.clone(), source_fifo_path.display());
        // Tasks
        let mut tasks: Vec<JoinHandle<()>> = vec![];
        // TEST_BATCH_SIZE + 1 rows will be provided. However, after processing precisely
        // TEST_BATCH_SIZE rows, the program will pause and wait for a batch to be read in another
        // thread. This approach ensures that the pipeline remains unbroken.
        tasks.push(create_writing_thread(
            source_fifo_path_thread,
            "a1,a2\n".to_owned(),
            (0..TEST_DATA_SIZE)
                .map(|_| "a,1\n".to_string())
                .collect::<Vec<_>>(),
            waiting,
            TEST_BATCH_SIZE,
        ));
        // Create a new temporary FIFO file
        let sink_fifo_path = create_fifo_file(&tmp_dir, "sink.csv")?;
        // Prevent move
        let (sink_fifo_path_thread, sink_display_fifo_path) =
            (sink_fifo_path.clone(), sink_fifo_path.display());
        // Spawn a new thread to read sink EXTERNAL TABLE.
        tasks.push(thread::spawn(move || {
            let file = File::open(sink_fifo_path_thread).unwrap();
            let schema = Arc::new(Schema::new(vec![
                Field::new("a1", DataType::Utf8, false),
                Field::new("a2", DataType::UInt32, false),
            ]));

            let mut reader = ReaderBuilder::new(schema)
                .with_header(true)
                .with_batch_size(TEST_BATCH_SIZE)
                .build(file)
                .map_err(|e| DataFusionError::Internal(e.to_string()))
                .unwrap();

            while let Some(Ok(_)) = reader.next() {
                waiting_thread.store(false, Ordering::SeqCst);
            }
        }));
        // register second csv file with the SQL (create an empty file if not found)
        ctx.sql(&format!(
            "CREATE EXTERNAL TABLE source_table (
                a1  VARCHAR NOT NULL,
                a2  INT NOT NULL
            )
            STORED AS CSV
            WITH HEADER ROW
            OPTIONS ('UNBOUNDED' 'TRUE')
            LOCATION '{source_display_fifo_path}'"
        ))
        .await?;

        // register csv file with the SQL
        ctx.sql(&format!(
            "CREATE EXTERNAL TABLE sink_table (
                a1  VARCHAR NOT NULL,
                a2  INT NOT NULL
            )
            STORED AS CSV
            WITH HEADER ROW
            OPTIONS ('UNBOUNDED' 'TRUE')
            LOCATION '{sink_display_fifo_path}'"
        ))
        .await?;

        let df = ctx
            .sql(
                "INSERT INTO sink_table
            SELECT a1, a2 FROM source_table",
            )
            .await?;
        df.collect().await?;
        tasks.into_iter().for_each(|jh| jh.join().unwrap());
        Ok(())
    }
}
