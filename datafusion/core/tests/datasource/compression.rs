// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Tests for reading compressed CSV and JSON files

use std::fmt::Write as _;
use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::Int64Type;
use bytes::Bytes;
use datafusion::common::file_options::file_type::{
    DEFAULT_CSV_EXTENSION, DEFAULT_JSON_EXTENSION, GetExt,
};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::error::Result;
use datafusion::functions_aggregate::expr_fn::{count, sum};
use datafusion::prelude::*;
use futures::{StreamExt, TryStreamExt};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use tempfile::TempDir;
use url::Url;

const CODECS: [FileCompressionType; 4] = [
    FileCompressionType::GZIP,
    FileCompressionType::BZIP2,
    FileCompressionType::XZ,
    FileCompressionType::ZSTD,
];

#[derive(Debug, Clone, Copy)]
enum Format {
    Csv,
    NdJson,
    JsonArray,
}

/// A compressed file made of two concatenated members, such as
/// `cat a.csv.zst b.csv.zst` produces, returns every row whether it is read
/// from a local file or from an object store.
#[tokio::test]
async fn concatenated_members_read_in_full() -> Result<()> {
    const ROWS: i64 = 20_000;
    let expected = (ROWS, ROWS * (ROWS - 1) / 2);

    let ctx = SessionContext::new();
    let store = Arc::new(InMemory::new());
    ctx.register_object_store(
        &Url::parse("memory://").unwrap(),
        Arc::clone(&store) as Arc<dyn ObjectStore>,
    );
    let dir = TempDir::new()?;

    let mut wrong = Vec::new();
    for format in [Format::Csv, Format::NdJson, Format::JsonArray] {
        let (text, ext) = match format {
            Format::Csv => {
                let mut text = String::from("a\n");
                for i in 0..ROWS {
                    writeln!(text, "{i}").unwrap();
                }
                (text, DEFAULT_CSV_EXTENSION)
            }
            Format::NdJson => {
                let mut text = String::new();
                for i in 0..ROWS {
                    writeln!(text, "{{\"a\":{i}}}").unwrap();
                }
                (text, DEFAULT_JSON_EXTENSION)
            }
            Format::JsonArray => (
                format!(
                    "[{}]",
                    (0..ROWS)
                        .map(|i| format!("{{\"a\":{i}}}"))
                        .collect::<Vec<_>>()
                        .join(",\n")
                ),
                DEFAULT_JSON_EXTENSION,
            ),
        };
        // Split inside a row, so that one row spans both members.
        let split = text.len() / 2;
        assert!(!text[..split].ends_with('\n'));
        let (first, second) = text.as_bytes().split_at(split);

        for codec in CODECS {
            let bytes = [
                compress(codec, first).await?,
                compress(codec, second).await?,
            ]
            .concat();

            let file_ext = format!("{ext}{}", codec.get_ext());
            let name = format!("{format:?}{file_ext}");
            for (source, location) in write_file(&dir, &store, &name, bytes).await? {
                match count_and_sum(&ctx, format, location, &file_ext, codec).await {
                    Ok(got) if got == expected => {}
                    Ok(got) => wrong.push(format!("{name} from {source}: {got:?}")),
                    Err(e) => wrong.push(format!("{name} from {source}: {e}")),
                }
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "expected (count, sum) = {expected:?}, got: {wrong:#?}"
    );

    Ok(())
}

/// Bytes after the last member that do not start another member are an
/// error, whether the file is read from a local file or from an object store.
#[tokio::test]
async fn bytes_after_the_last_member_are_an_error() -> Result<()> {
    let ctx = SessionContext::new();
    let store = Arc::new(InMemory::new());
    ctx.register_object_store(
        &Url::parse("memory://").unwrap(),
        Arc::clone(&store) as Arc<dyn ObjectStore>,
    );
    let dir = TempDir::new()?;

    let mut wrong = Vec::new();
    for codec in CODECS {
        let bytes = [compress(codec, b"a\n1\n2\n").await?, b"garbage".to_vec()].concat();

        let file_ext = format!("{DEFAULT_CSV_EXTENSION}{}", codec.get_ext());
        let name = format!("garbage{file_ext}");
        for (source, location) in write_file(&dir, &store, &name, bytes).await? {
            if let Ok(got) =
                count_and_sum(&ctx, Format::Csv, location, &file_ext, codec).await
            {
                wrong.push(format!("{name} from {source}: {got:?}"));
            }
        }
    }
    assert!(wrong.is_empty(), "read without an error: {wrong:#?}");

    Ok(())
}

/// Compresses `data` as one member.
async fn compress(codec: FileCompressionType, data: &[u8]) -> Result<Vec<u8>> {
    let input = futures::stream::iter([Ok(Bytes::copy_from_slice(data))]);
    let compressed: Vec<Bytes> = codec
        .convert_to_compress_stream(input.boxed())?
        .try_collect()
        .await?;
    Ok(compressed.concat())
}

/// Writes `bytes` as `name` to a local file in `dir` and to `store`, and
/// returns a `(description, location)` for each copy.
async fn write_file(
    dir: &TempDir,
    store: &InMemory,
    name: &str,
    bytes: Vec<u8>,
) -> Result<[(&'static str, String); 2]> {
    let local = dir.path().join(name);
    std::fs::write(&local, &bytes)?;
    store.put(&Path::from(name), bytes.into()).await?;
    Ok([
        ("local file", local.to_str().unwrap().to_string()),
        ("object store", format!("memory:///{name}")),
    ])
}

/// Returns `(count(a), sum(a))` over the file at `location`.
async fn count_and_sum(
    ctx: &SessionContext,
    format: Format,
    location: String,
    file_ext: &str,
    codec: FileCompressionType,
) -> Result<(i64, i64)> {
    let df = match format {
        Format::Csv => {
            let options = CsvReadOptions::new()
                .file_extension(file_ext)
                .file_compression_type(codec);
            ctx.read_csv(location, options).await?
        }
        Format::NdJson | Format::JsonArray => {
            let options = JsonReadOptions::default()
                .file_extension(file_ext)
                .file_compression_type(codec)
                .newline_delimited(matches!(format, Format::NdJson));
            ctx.read_json(location, options).await?
        }
    };
    let batches = df
        .aggregate(vec![], vec![count(col("a")), sum(col("a"))])?
        .collect()
        .await?;
    let value = |i: usize| batches[0].column(i).as_primitive::<Int64Type>().value(0);
    Ok((value(0), value(1)))
}
