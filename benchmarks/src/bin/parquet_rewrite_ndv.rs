// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Rewrites benchmark Parquet data with row-group NDV statistics.

use std::collections::VecDeque;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;

use clap::Parser;
use datafusion::error::{DataFusionError, Result};
use datafusion_common::exec_datafusion_err;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, Type as PhysicalType, ZstdLevel};
use parquet::file::properties::WriterProperties;

#[derive(Debug, Parser)]
#[command(
    name = "parquet_rewrite_ndv",
    about = "Rewrite Parquet benchmark data with row-group NDV statistics"
)]
struct Cli {
    /// Source Parquet file or directory.
    #[arg(long)]
    source: PathBuf,

    /// Destination file or directory. Existing files are replaced atomically.
    #[arg(long)]
    destination: PathBuf,

    /// Number of files to rewrite concurrently.
    #[arg(long, default_value_t = 1)]
    jobs: usize,

    /// Record batch size used while copying each row group.
    #[arg(long, default_value_t = 8192)]
    batch_size: usize,
}

fn io_error(context: impl Into<String>, error: &std::io::Error) -> DataFusionError {
    exec_datafusion_err!("{}: {error}", context.into())
}

fn parquet_error(
    context: impl Into<String>,
    error: &parquet::errors::ParquetError,
) -> DataFusionError {
    exec_datafusion_err!("{}: {error}", context.into())
}

fn collect_parquet_files(
    root: &Path,
    current: &Path,
    files: &mut Vec<PathBuf>,
) -> Result<()> {
    let entries = fs::read_dir(current)
        .map_err(|e| io_error(format!("reading {}", current.display()), &e))?;
    for entry in entries {
        let entry =
            entry.map_err(|e| io_error(format!("reading {}", current.display()), &e))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|e| io_error(format!("reading type of {}", path.display()), &e))?;
        if file_type.is_dir() {
            collect_parquet_files(root, &path, files)?;
        } else if file_type.is_file()
            && path
                .extension()
                .is_some_and(|extension| extension == "parquet")
        {
            files.push(
                path.strip_prefix(root)
                    .map_err(|e| exec_datafusion_err!("computing relative path: {e}"))?
                    .to_path_buf(),
            );
        }
    }
    Ok(())
}

fn input_files(source: &Path, destination: &Path) -> Result<Vec<(PathBuf, PathBuf)>> {
    if source.is_file() {
        return Ok(vec![(source.to_path_buf(), destination.to_path_buf())]);
    }
    if !source.is_dir() {
        return Err(exec_datafusion_err!(
            "source does not exist: {}",
            source.display()
        ));
    }

    let mut relative = Vec::new();
    collect_parquet_files(source, source, &mut relative)?;
    relative.sort_unstable();
    Ok(relative
        .into_iter()
        .map(|path| (source.join(&path), destination.join(path)))
        .collect())
}

fn rewrite_file(source: &Path, destination: &Path, batch_size: usize) -> Result<()> {
    let source_file = File::open(source)
        .map_err(|e| io_error(format!("opening {}", source.display()), &e))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(source_file).map_err(|e| {
        parquet_error(format!("reading metadata for {}", source.display()), &e)
    })?;
    let schema = Arc::clone(builder.schema());
    let row_group_rows: Vec<usize> = builder
        .metadata()
        .row_groups()
        .iter()
        .map(|row_group| row_group.num_rows() as usize)
        .collect();
    let max_row_group_rows = row_group_rows.iter().copied().max().unwrap_or(1);
    let mut reader = builder.with_batch_size(batch_size).build().map_err(|e| {
        parquet_error(format!("opening reader for {}", source.display()), &e)
    })?;

    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| io_error(format!("creating {}", parent.display()), &e))?;
    }
    let temporary = destination.with_extension("parquet.tmp");
    let output = File::create(&temporary)
        .map_err(|e| io_error(format!("creating {}", temporary.display()), &e))?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(1).unwrap()))
        .set_dictionary_enabled(true)
        .set_max_row_group_row_count(Some(max_row_group_rows))
        .set_write_row_group_number_distinct_values(true)
        .build();
    let mut writer =
        ArrowWriter::try_new(output, schema, Some(properties)).map_err(|e| {
            parquet_error(format!("opening writer for {}", destination.display()), &e)
        })?;

    let mut row_group_index = 0;
    let mut rows_remaining = row_group_rows.first().copied().unwrap_or(0);
    for batch in &mut reader {
        let batch = batch
            .map_err(|e| exec_datafusion_err!("reading {}: {e}", source.display()))?;
        let mut offset = 0;
        while offset < batch.num_rows() {
            if row_group_index == row_group_rows.len() {
                return Err(exec_datafusion_err!(
                    "reader returned more rows than metadata for {}",
                    source.display()
                ));
            }
            let rows = rows_remaining.min(batch.num_rows() - offset);
            writer.write(&batch.slice(offset, rows)).map_err(|e| {
                parquet_error(format!("writing {}", destination.display()), &e)
            })?;
            offset += rows;
            rows_remaining -= rows;
            if rows_remaining == 0 {
                writer.flush().map_err(|e| {
                    parquet_error(format!("flushing {}", destination.display()), &e)
                })?;
                row_group_index += 1;
                rows_remaining =
                    row_group_rows.get(row_group_index).copied().unwrap_or(0);
            }
        }
    }
    if row_group_index != row_group_rows.len() || rows_remaining != 0 {
        return Err(exec_datafusion_err!(
            "reader returned fewer rows than metadata for {}",
            source.display()
        ));
    }
    writer
        .close()
        .map_err(|e| parquet_error(format!("closing {}", destination.display()), &e))?;

    let check = ParquetRecordBatchReaderBuilder::try_new(
        File::open(&temporary)
            .map_err(|e| io_error(format!("opening {}", temporary.display()), &e))?,
    )
    .map_err(|e| parquet_error(format!("validating {}", temporary.display()), &e))?;
    for row_group in check.metadata().row_groups() {
        for (index, column) in row_group.columns().iter().enumerate() {
            if check.parquet_schema().column(index).physical_type()
                == PhysicalType::BYTE_ARRAY
                && column
                    .statistics()
                    .and_then(|statistics| statistics.distinct_count_opt())
                    .is_none()
            {
                return Err(exec_datafusion_err!(
                    "missing NDV for byte-array column {} in {}",
                    check.parquet_schema().column(index).path(),
                    temporary.display()
                ));
            }
        }
    }
    fs::rename(&temporary, destination).map_err(|e| {
        io_error(
            format!(
                "renaming {} to {}",
                temporary.display(),
                destination.display()
            ),
            &e,
        )
    })?;
    println!("{} -> {}", source.display(), destination.display());
    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    if cli.jobs == 0 {
        return Err(exec_datafusion_err!("--jobs must be >= 1"));
    }
    if cli.batch_size == 0 {
        return Err(exec_datafusion_err!("--batch-size must be >= 1"));
    }

    let files = input_files(&cli.source, &cli.destination)?;
    if files.is_empty() {
        return Err(exec_datafusion_err!(
            "no Parquet files found under {}",
            cli.source.display()
        ));
    }
    let queue = Arc::new(Mutex::new(VecDeque::from(files)));
    let mut workers = Vec::new();
    for _ in 0..cli.jobs.min(queue.lock().unwrap().len()) {
        let queue = Arc::clone(&queue);
        let batch_size = cli.batch_size;
        workers.push(thread::spawn(move || -> Result<()> {
            loop {
                let Some((source, destination)) = queue.lock().unwrap().pop_front()
                else {
                    break;
                };
                rewrite_file(&source, &destination, batch_size)?;
            }
            Ok(())
        }));
    }
    for worker in workers {
        worker
            .join()
            .map_err(|_| exec_datafusion_err!("NDV rewrite worker panicked"))??;
    }
    Ok(())
}
