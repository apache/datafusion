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

use std::fs::{self, File};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Int64Array, Int64Builder, ListBuilder, RecordBatch, StringArray,
};
use arrow::compute::concat_batches;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::MemTable;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext, col};
use datafusion_common::Result;
use datafusion_common::config::ConfigNonZeroUsize;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use rstest::rstest;
use tempfile::TempDir;

#[rstest]
#[case(10_000, None, false)]
#[case(128, None, false)]
#[case(10_000, Some(1), false)]
#[case(300, Some(1), false)]
#[case(10_000, Some(4096), false)]
#[case(300, Some(4096), false)]
#[case::growing_strings(200, Some(12_288), true)]
#[tokio::test]
async fn parallel_copy_matches_serial_row_groups(
    #[case] max_rows: usize,
    #[case] max_bytes: Option<usize>,
    #[case] growing_strings: bool,
) -> Result<()> {
    // Each partition becomes a separate file. List lengths differ from root row
    // counts, and string widths change between batches to exercise prediction.
    let mut input = vec![];
    let mut expected = [vec![], vec![]];
    for batch_index in 0..4 {
        for (partition, expected) in expected.iter_mut().enumerate() {
            let ids = (batch_index * 128..(batch_index + 1) * 128).collect::<Vec<i64>>();
            let mut lists = ListBuilder::new(Int64Builder::new());
            for id in &ids {
                for _ in 0..id % 4 {
                    lists.values().append_value(*id);
                }
                lists.append(true);
            }
            let width = match (growing_strings, batch_index == 0) {
                (true, true) => 64,
                (true, false) | (false, true) => 512,
                (false, false) => 8,
            };
            let batch = RecordBatch::try_from_iter(vec![
                (
                    "partition",
                    Arc::new(Int64Array::from(vec![partition as i64; 128])) as ArrayRef,
                ),
                ("id", Arc::new(Int64Array::from(ids.clone())) as ArrayRef),
                ("items", Arc::new(lists.finish()) as ArrayRef),
                (
                    "payload",
                    Arc::new(StringArray::from_iter_values(
                        ids.iter().map(|id| format!("{id}-{}", "x".repeat(width))),
                    )) as ArrayRef,
                ),
            ])?;
            expected.push(batch.project(&[1, 2, 3])?);
            input.push(batch);
        }
    }
    let source = Arc::new(MemTable::try_new(input[0].schema(), vec![input])?);
    let mut layouts = vec![];
    for parallel in [false, true] {
        let mut config = SessionConfig::new()
            .with_batch_size(128)
            .with_target_partitions(1);
        let options = &mut config.options_mut().execution;
        options.minimum_parallel_output_files = ConfigNonZeroUsize::try_new(1)?;
        options.parquet.allow_single_file_parallelism = parallel;
        options.parquet.schema_force_view_types = false;
        options.parquet.max_row_group_size = max_rows;
        options.parquet.maximum_parallel_row_group_writers = 2;
        if let Some(limit) = max_bytes {
            config.options_mut().set(
                "datafusion.execution.parquet.max_row_group_bytes",
                &limit.to_string(),
            )?;
        }
        let ctx = SessionContext::new_with_config(config);
        let output = TempDir::new()?;
        ctx.read_table(source.clone())?
            .write_parquet(
                output.path().to_str().unwrap(),
                DataFrameWriteOptions::new().with_partition_by(vec!["partition".into()]),
                None,
            )
            .await?;

        let mut layout = vec![];
        for (partition, expected) in expected.iter().enumerate() {
            let directory = output.path().join(format!("partition={partition}"));
            let files = fs::read_dir(&directory)?.collect::<std::io::Result<Vec<_>>>()?;
            assert_eq!(files.len(), 1, "expected one file per partition");
            let path = files[0].path();
            let metadata =
                ArrowReaderMetadata::load(&File::open(&path)?, Default::default())?;
            let groups = metadata.metadata().row_groups();
            assert_eq!(
                groups.iter().map(|group| group.num_rows()).sum::<i64>(),
                512
            );
            assert!(
                groups
                    .iter()
                    .all(|group| group.num_rows() > 0
                        && group.num_rows() <= max_rows as i64)
            );
            if max_bytes == Some(1) {
                assert!(
                    groups.len() >= 4,
                    "the byte target must split each input batch"
                );
            }
            layout.push(
                groups
                    .iter()
                    .map(|group| {
                        (
                            group.num_rows(),
                            group.total_byte_size(),
                            group.compressed_size(),
                        )
                    })
                    .collect::<Vec<_>>(),
            );

            let actual = ctx
                .read_parquet(path.to_str().unwrap(), ParquetReadOptions::default())
                .await?
                .sort(vec![col("id").sort(true, true)])?
                .collect()
                .await?;
            let actual = concat_batches(&actual[0].schema(), &actual)?;
            let expected = concat_batches(&expected[0].schema(), expected)?;
            assert_eq!(actual.columns(), expected.columns());
        }
        layouts.push(layout);
    }
    assert_eq!(layouts[0], layouts[1], "serial and parallel layouts differ");
    Ok(())
}
