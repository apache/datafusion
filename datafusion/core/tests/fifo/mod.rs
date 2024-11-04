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

//! This test demonstrates the DataFusion FIFO capabilities.

#[cfg(target_family = "unix")]
#[cfg(test)]
mod unix_test {
    use std::fs::File;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::Array;
    use arrow::csv::ReaderBuilder;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_schema::SchemaRef;
    use datafusion::datasource::stream::{FileStreamProvider, StreamConfig, StreamTable};
    use datafusion::datasource::TableProvider;
    use datafusion::{
        prelude::{CsvReadOptions, SessionConfig, SessionContext},
        test_util::{aggr_test_schema, arrow_test_data},
    };
    use datafusion_common::instant::Instant;
    use datafusion_common::{exec_err, Result};
    use datafusion_expr::SortExpr;

    use futures::StreamExt;
    use nix::sys::stat;
    use nix::unistd;
    use tempfile::TempDir;
    use tokio::io::AsyncWriteExt;
    use tokio::task::{spawn_blocking, JoinHandle};

    /// Makes a TableProvider for a fifo file
    fn fifo_table(
        schema: SchemaRef,
        path: impl Into<PathBuf>,
        sort: Vec<Vec<SortExpr>>,
    ) -> Arc<dyn TableProvider> {
        let source = FileStreamProvider::new_file(schema, path.into())
            .with_batch_size(TEST_BATCH_SIZE)
            .with_header(true);
        let config = StreamConfig::new(Arc::new(source)).with_order(sort);
        Arc::new(StreamTable::new(Arc::new(config)))
    }

    fn create_fifo_file(tmp_dir: &TempDir, file_name: &str) -> Result<PathBuf> {
        let file_path = tmp_dir.path().join(file_name);
        // Simulate an infinite environment via a FIFO file
        if let Err(e) = unistd::mkfifo(&file_path, stat::Mode::S_IRWXU) {
            exec_err!("{}", e)
        } else {
            Ok(file_path)
        }
    }

    async fn write_to_fifo(
        file: &mut tokio::fs::File,
        line: &str,
        ref_time: Instant,
        broken_pipe_timeout: Duration,
    ) -> Result<()> {
        // We need to handle broken pipe error until the reader is ready. This
        // is why we use a timeout to limit the wait duration for the reader.
        // If the error is different than broken pipe, we fail immediately.
        while let Err(e) = file.write_all(line.as_bytes()).await {
            if e.raw_os_error().unwrap() == 32 {
                let interval = Instant::now().duration_since(ref_time);
                if interval < broken_pipe_timeout {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            }
            return exec_err!("{}", e);
        }
        Ok(())
    }

    /// This function creates a writing task for the FIFO file. To verify
    /// incremental processing, it waits for a signal to continue writing after
    /// a certain number of lines are written.
    #[allow(clippy::disallowed_methods)]
    fn create_writing_task(
        file_path: PathBuf,
        header: String,
        lines: Vec<String>,
        waiting_signal: Arc<AtomicBool>,
        send_before_waiting: usize,
    ) -> JoinHandle<()> {
        // Timeout for a long period of BrokenPipe error
        let broken_pipe_timeout = Duration::from_secs(10);
        // Spawn a new task to write to the FIFO file
        tokio::spawn(async move {
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .open(file_path)
                .await
                .unwrap();
            // Reference time to use when deciding to fail the test
            let execution_start = Instant::now();
            write_to_fifo(&mut file, &header, execution_start, broken_pipe_timeout)
                .await
                .unwrap();
            for (cnt, line) in lines.iter().enumerate() {
                while waiting_signal.load(Ordering::SeqCst) && cnt > send_before_waiting {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                write_to_fifo(&mut file, line, execution_start, broken_pipe_timeout)
                    .await
                    .unwrap();
            }
            drop(file);
        })
    }

    // !  For the sake of the test, do not alter the numbers. !
    // Session batch size
    const TEST_BATCH_SIZE: usize = 20;
    // Number of lines written to FIFO
    const TEST_DATA_SIZE: usize = 20_000;
    // Number of lines to write before waiting to verify incremental processing
    const SEND_BEFORE_WAITING: usize = 2 * TEST_BATCH_SIZE;
    // Number of lines what can be joined. Each joinable key produced 20 lines with
    // aggregate_test_100 dataset. We will use these joinable keys for understanding
    // incremental execution.
    const TEST_JOIN_RATIO: f64 = 0.01;

    // This test provides a relatively realistic end-to-end scenario where
    // we swap join sides to accommodate a FIFO source.
    #[tokio::test]
    async fn unbounded_file_with_swapped_join() -> Result<()> {
        // Create session context
        let config = SessionConfig::new()
            .with_batch_size(TEST_BATCH_SIZE)
            .with_collect_statistics(false)
            .with_target_partitions(1);
        let ctx = SessionContext::new_with_config(config);
        // To make unbounded deterministic
        let waiting = Arc::new(AtomicBool::new(true));
        // Create a new temporary FIFO file
        let tmp_dir = TempDir::new()?;
        let fifo_path = create_fifo_file(&tmp_dir, "fifo_unbounded.csv")?;
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
        // Create writing tasks for the left and right FIFO files
        let task = create_writing_task(
            fifo_path.clone(),
            "a1,a2\n".to_owned(),
            lines,
            waiting.clone(),
            joinable_lines_length * 2,
        );

        // Data Schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("a1", DataType::Utf8, false),
            Field::new("a2", DataType::UInt32, false),
        ]));

        let provider = fifo_table(schema, fifo_path, vec![]);
        ctx.register_table("left", provider).unwrap();

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
        let df = ctx
            .sql(
                "SELECT
                  t1.a2, t2.c1, t2.c4, t2.c5
                FROM
                  left as t1, right as t2
                WHERE
                  t1.a1 = t2.c1",
            )
            .await?;
        let mut stream = df.execute_stream().await?;
        while (stream.next().await).is_some() {
            waiting.store(false, Ordering::SeqCst);
        }
        task.await.unwrap();
        Ok(())
    }

    /// This test provides a relatively realistic end-to-end scenario where
    /// we change the join into a `SymmetricHashJoinExec` to accommodate two
    /// unbounded (FIFO) sources.
    #[tokio::test]
    async fn unbounded_file_with_symmetric_join() -> Result<()> {
        // Create session context
        let config = SessionConfig::new()
            .with_batch_size(TEST_BATCH_SIZE)
            .set_bool("datafusion.execution.coalesce_batches", false)
            .with_target_partitions(1);
        let ctx = SessionContext::new_with_config(config);

        // Create a new temporary FIFO file
        let tmp_dir = TempDir::new()?;
        // Create a FIFO file for the left input source.
        let left_fifo = create_fifo_file(&tmp_dir, "left.csv")?;
        // Create a FIFO file for the right input source.
        let right_fifo = create_fifo_file(&tmp_dir, "right.csv")?;
        // Create a mutex for tracking if the right input source is waiting for data.
        let waiting = Arc::new(AtomicBool::new(true));

        // Create schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("a1", DataType::UInt32, false),
            Field::new("a2", DataType::UInt32, false),
        ]));

        // Specify the ordering:
        let order = vec![vec![datafusion_expr::col("a1").sort(true, false)]];

        // Set unbounded sorted files read configuration
        let provider = fifo_table(schema.clone(), left_fifo.clone(), order.clone());
        ctx.register_table("left", provider)?;

        let provider = fifo_table(schema.clone(), right_fifo.clone(), order);
        ctx.register_table("right", provider)?;

        // Execute the query, with no matching rows. (since key is modulus 10)
        let df = ctx
            .sql(
                "SELECT
                  t1.a1, t1.a2, t2.a1, t2.a2
                FROM
                  left as t1
                FULL JOIN
                  right as t2
                ON
                  t1.a2 = t2.a2 AND
                  t1.a1 > t2.a1 + 4 AND
                  t1.a1 < t2.a1 + 9",
            )
            .await?;
        let mut stream = df.execute_stream().await?;

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

        // Create writing tasks for the left and right FIFO files
        tasks.push(create_writing_task(
            left_fifo,
            "a1,a2\n".to_owned(),
            lines.clone(),
            waiting.clone(),
            SEND_BEFORE_WAITING,
        ));
        tasks.push(create_writing_task(
            right_fifo,
            "a1,a2\n".to_owned(),
            lines,
            waiting.clone(),
            SEND_BEFORE_WAITING,
        ));
        // Collect output data:
        let (mut equal, mut left, mut right) = (0, 0, 0);
        while let Some(Ok(batch)) = stream.next().await {
            waiting.store(false, Ordering::SeqCst);
            let left_unmatched = batch.column(2).null_count();
            let right_unmatched = batch.column(0).null_count();
            if left_unmatched == 0 && right_unmatched == 0 {
                equal += 1;
            } else if right_unmatched <= left_unmatched {
                left += 1;
            } else {
                right += 1;
            };
        }
        futures::future::try_join_all(tasks).await.unwrap();

        // The symmetric hash join algorithm produces FULL join results at
        // every pruning, which happens before it reaches the end of input and
        // more than once. In this test, we feed partially joinable data to
        // both sides in order to ensure that left or right unmatched results
        // are generated as expected.
        assert!(equal >= 0 && left > 1 && right > 1);
        Ok(())
    }

    /// It tests the INSERT INTO functionality.
    #[tokio::test]
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
        tasks.push(create_writing_task(
            source_fifo_path_thread,
            "a1,a2\n".to_owned(),
            (0..TEST_DATA_SIZE)
                .map(|_| "a,1\n".to_string())
                .collect::<Vec<_>>(),
            waiting,
            SEND_BEFORE_WAITING,
        ));
        // Create a new temporary FIFO file
        let sink_fifo_path = create_fifo_file(&tmp_dir, "sink.csv")?;
        // Prevent move
        let (sink_fifo_path_thread, sink_display_fifo_path) =
            (sink_fifo_path.clone(), sink_fifo_path.display());

        // Spawn a new thread to read sink EXTERNAL TABLE.
        #[allow(clippy::disallowed_methods)] // spawn allowed only in tests
        tasks.push(spawn_blocking(move || {
            let file = File::open(sink_fifo_path_thread).unwrap();
            let schema = Arc::new(Schema::new(vec![
                Field::new("a1", DataType::Utf8, false),
                Field::new("a2", DataType::UInt32, false),
            ]));

            let mut reader = ReaderBuilder::new(schema)
                .with_batch_size(TEST_BATCH_SIZE)
                .with_header(true)
                .build(file)
                .unwrap();

            while let Some(Ok(_)) = reader.next() {
                waiting_thread.store(false, Ordering::SeqCst);
            }
        }));
        // register second csv file with the SQL (create an empty file if not found)
        ctx.sql(&format!(
            "CREATE UNBOUNDED EXTERNAL TABLE source_table (
                a1  VARCHAR NOT NULL,
                a2  INT NOT NULL
            )
            STORED AS CSV
            LOCATION '{source_display_fifo_path}'
            OPTIONS ('format.has_header' 'true')"
        ))
        .await?;

        // register csv file with the SQL
        ctx.sql(&format!(
            "CREATE UNBOUNDED EXTERNAL TABLE sink_table (
                a1  VARCHAR NOT NULL,
                a2  INT NOT NULL
            )
            STORED AS CSV
            LOCATION '{sink_display_fifo_path}'
            OPTIONS ('format.has_header' 'true')"
        ))
        .await?;

        let df = ctx
            .sql("INSERT INTO sink_table SELECT a1, a2 FROM source_table")
            .await?;

        // Start execution
        df.collect().await?;
        futures::future::try_join_all(tasks).await.unwrap();
        Ok(())
    }
}
