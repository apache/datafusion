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

use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use bytes::{BufMut, Bytes, BytesMut};
use datafusion::{
    datasource::{
        listing::PartitionedFile,
        physical_plan::{parquet::ParquetExecBuilder, FileScanConfig},
    },
    prelude::*,
};
use datafusion_common::DFSchema;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::{collect, filter::FilterExec, ExecutionPlan};
use itertools::Itertools;
use object_store::{memory::InMemory, path::Path, ObjectStore, PutPayload};
use parquet::{
    arrow::ArrowWriter,
    file::properties::{EnabledStatistics, WriterProperties},
};
use rand::seq::SliceRandom;
use url::Url;

#[tokio::test]
async fn test_fuzz_utf8() {
    // Fuzz testing for UTF8 predicate pruning
    // The basic idea is that query results should always be the same with or without stats/pruning
    // If we get this right we at least guarantee that there are no incorrect results
    // There may still be suboptimal pruning or stats but that's something we can try to catch
    // with more targeted tests.

    // Since we know where the edge cases might be we don't do random black box fuzzing.
    // Instead we fuzz on specific pre-defined axis:
    //
    // - Which characters are in each value. We want to make sure to include characters that when
    //   incremented, truncated or otherwise manipulated might cause issues.
    // - The values in each row group. This impacts which min/max stats are generated for each rg.
    //   We'll generate combinations of the characters with lengths ranging from 1 to 4.
    // - Truncation of statistics to 1, 2 or 3 characters as well as no truncation.

    let mut rng = rand::thread_rng();

    let characters = [
        "z",
        "0",
        "~",
        "ß",
        "℣",
        "%", // this one is useful for like/not like tests since it will result in randomly inserted wildcards
        "_", // this one is useful for like/not like tests since it will result in randomly inserted wildcards
        "\u{7F}",
        "\u{7FF}",
        "\u{FF}",
        "\u{10FFFF}",
        "\u{D7FF}",
        "\u{FDCF}",
        // null character
        "\u{0}",
    ];

    let value_lengths = [1, 2, 3];

    // generate all combinations of characters with lengths ranging from 1 to 4
    let mut values = vec![];
    for length in &value_lengths {
        values.extend(
            characters
                .iter()
                .cloned()
                .combinations(*length)
                // now get all permutations of each combination
                .flat_map(|c| c.into_iter().permutations(*length))
                // and join them into strings
                .map(|c| c.join("")),
        );
    }

    println!("Generated {} values", values.len());

    // randomly pick 100 values
    values.shuffle(&mut rng);
    values.truncate(100);

    let mut row_groups = vec![];
    // generate all combinations of values for row groups (1 or 2 values per rg, more is unessecarry since we only get min/max stats out)
    for rg_length in [1, 2] {
        row_groups.extend(values.iter().cloned().combinations(rg_length));
    }

    println!("Generated {} row groups", row_groups.len());

    // Randomly pick 100 row groups (combinations of said values)
    row_groups.shuffle(&mut rng);
    row_groups.truncate(100);

    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));
    let df_schema = DFSchema::try_from(schema.clone()).unwrap();

    let store = InMemory::new();
    let mut files = vec![];
    for (idx, truncation_length) in [Some(1), Some(2), None].iter().enumerate() {
        // parquet files only support 32767 row groups per file, so chunk up into multiple files so we don't error if running on a large number of row groups
        for (rg_idx, row_groups) in row_groups.chunks(32766).enumerate() {
            let buf = write_parquet_file(
                *truncation_length,
                schema.clone(),
                row_groups.to_vec(),
            )
            .await;
            let filename = format!("test_fuzz_utf8_{idx}_{rg_idx}.parquet");
            files.push((filename.clone(), buf.len()));
            let payload = PutPayload::from(buf);
            let path = Path::from(filename);
            store.put(&path, payload).await.unwrap();
        }
    }

    println!("Generated {} parquet files", files.len());

    let ctx = SessionContext::new();

    ctx.register_object_store(&Url::parse("memory://").unwrap(), Arc::new(store));

    let mut predicates = vec![];
    for value in values {
        predicates.push(col("a").eq(lit(value.clone())));
        predicates.push(col("a").not_eq(lit(value.clone())));
        predicates.push(col("a").lt(lit(value.clone())));
        predicates.push(col("a").lt_eq(lit(value.clone())));
        predicates.push(col("a").gt(lit(value.clone())));
        predicates.push(col("a").gt_eq(lit(value.clone())));
        predicates.push(col("a").like(lit(value.clone())));
        predicates.push(col("a").not_like(lit(value.clone())));
        predicates.push(col("a").like(lit(format!("%{}", value.clone()))));
        predicates.push(col("a").like(lit(format!("{}%", value.clone()))));
        predicates.push(col("a").not_like(lit(format!("%{}", value.clone()))));
        predicates.push(col("a").not_like(lit(format!("{}%", value.clone()))));
    }

    for predicate in predicates {
        println!("Testing predicate {:?}", predicate);
        let phys_expr_predicate = ctx
            .create_physical_expr(predicate.clone(), &df_schema)
            .unwrap();
        let expected = execute_with_predicate(
            &files,
            phys_expr_predicate.clone(),
            false,
            schema.clone(),
            &ctx,
        )
        .await;
        let with_pruning = execute_with_predicate(
            &files,
            phys_expr_predicate,
            true,
            schema.clone(),
            &ctx,
        )
        .await;
        assert_eq!(expected, with_pruning);
    }
}

async fn execute_with_predicate(
    files: &[(String, usize)],
    predicate: Arc<dyn PhysicalExpr>,
    prune_stats: bool,
    schema: Arc<Schema>,
    ctx: &SessionContext,
) -> Vec<String> {
    let scan =
        FileScanConfig::new(ObjectStoreUrl::parse("memory://").unwrap(), schema.clone())
            .with_file_group(
                files
                    .iter()
                    .map(|(path, size)| PartitionedFile::new(path.clone(), *size as u64))
                    .collect(),
            );
    let mut builder = ParquetExecBuilder::new(scan);
    if prune_stats {
        builder = builder.with_predicate(predicate.clone())
    }
    let exec = Arc::new(builder.build()) as Arc<dyn ExecutionPlan>;
    let exec =
        Arc::new(FilterExec::try_new(predicate, exec).unwrap()) as Arc<dyn ExecutionPlan>;

    let batches = collect(exec, ctx.task_ctx()).await.unwrap();
    let mut values = vec![];
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..column.len() {
            values.push(column.value(i).to_string());
        }
    }
    values
}

async fn write_parquet_file(
    truncation_length: Option<usize>,
    schema: Arc<Schema>,
    row_groups: Vec<Vec<String>>,
) -> Bytes {
    let mut buf = BytesMut::new().writer();
    let mut props = WriterProperties::builder();
    if let Some(truncation_length) = truncation_length {
        props = props.set_max_statistics_size(truncation_length);
    }
    props = props.set_statistics_enabled(EnabledStatistics::Chunk); // row group level
    let props = props.build();
    {
        let mut writer =
            ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).unwrap();
        for rg_values in row_groups.iter() {
            let arr = StringArray::from_iter_values(rg_values.iter());
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
            writer.write(&batch).unwrap();
            writer.flush().unwrap(); // finishes the current row group and starts a new one
        }
        writer.finish().unwrap();
    }
    buf.into_inner().freeze()
}
