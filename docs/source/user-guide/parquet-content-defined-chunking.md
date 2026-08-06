<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Parquet Content-Defined Chunking

Content-defined chunking (CDC) is an experimental Parquet writer feature that
makes data page boundaries depend on column values rather than fixed row or byte
counts. This makes unchanged regions more likely to produce identical pages when
closely related versions of a dataset are written with the same settings.

CDC is useful when the resulting files are stored or transferred through a
content-addressable or block-deduplicating system. Such a system can reuse the
identical pages instead of storing or transferring them again. For example, a
small insertion near the beginning of a dataset can change one page while later
page boundaries converge back to those of the previous version.

CDC does not itself deduplicate data or provide a page store. On a conventional
filesystem or object store, each Parquet file is still stored in full. The output
is a normal Parquet file and requires no CDC-specific reader support.

## When to enable CDC

Consider CDC when all of the following apply:

- You regularly write similar versions of the same dataset.
- Your storage or transfer layer detects and reuses duplicate byte ranges.
- Reducing storage or network transfer is more important than maximizing write
  parallelism for an individual file.

Leave CDC disabled for ordinary Parquet output unless you have measured a benefit
in the system that stores or transfers the files. CDC is disabled by default.

When CDC is enabled, DataFusion uses the sequential Arrow writer for each output
file because the chunker's state must persist across row groups. This can reduce
write throughput compared with DataFusion's parallel writer path. Writing
different output files can still proceed concurrently.

## Enable CDC with SQL

Set CDC for one [`COPY`](sql/dml.md#copy) operation with Parquet format options:

```sql
COPY (
    SELECT
        value AS id,
        CONCAT('event-', CAST(value AS VARCHAR)) AS event
    FROM generate_series(1, 100000)
) TO 'cdc-output'
STORED AS PARQUET
OPTIONS (
    'format.content_defined_chunking.enabled' 'true'
);
```

The default chunking parameters are a good starting point. To tune them for one
write:

```sql
COPY source_table TO 'cdc-output'
STORED AS PARQUET
OPTIONS (
    'format.content_defined_chunking.enabled' 'true',
    'format.content_defined_chunking.min_chunk_size' '262144',
    'format.content_defined_chunking.max_chunk_size' '1048576',
    'format.content_defined_chunking.norm_level' '0'
);
```

You can instead enable CDC for subsequent Parquet writes in the session:

```sql
SET datafusion.execution.parquet.content_defined_chunking.enabled = true;
```

The corresponding environment variable is
`DATAFUSION_EXECUTION_PARQUET_CONTENT_DEFINED_CHUNKING_ENABLED`. See
[Configuration Settings](configs.md#setting-configuration-options) for all ways
to set session options.

## Enable CDC with the Rust API

Pass [`TableParquetOptions`] to [`DataFrame::write_parquet`]:

```rust
use datafusion::config::{ParquetCdcOptions, TableParquetOptions};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let df = ctx
        .sql("SELECT value AS id FROM generate_series(1, 100000)")
        .await?;

    let mut parquet_options = TableParquetOptions::default();
    parquet_options.global.content_defined_chunking = ParquetCdcOptions::enabled();

    df.write_parquet(
        "cdc-output",
        DataFrameWriteOptions::new(),
        Some(parquet_options),
    )
    .await?;

    Ok(())
}
```

Set the fields of `ParquetCdcOptions` directly to use non-default chunk sizes or
normalization.

## Tuning

| Option           | Default | Effect                                                                                                                                                         |
| ---------------- | ------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `min_chunk_size` | 256 KiB | Minimum logical size before the rolling hash can select a boundary.                                                                                            |
| `max_chunk_size` | 1 MiB   | Maximum logical size before the writer forces a boundary. It must be greater than `min_chunk_size`.                                                            |
| `norm_level`     | `0`     | Controls how aggressively boundaries are selected. Higher values can improve deduplication but create more small pages; recommended range is `-3` through `3`. |

Chunk sizes are measured from logical column data before encoding and
compression. Definition and repetition levels for nested data also count toward
the size.

Use the same CDC, encoding, compression, and schema settings when comparing
dataset versions. Changing writer settings can change the page bytes and reduce
deduplication even when the logical data is unchanged. Measure the deduplication
ratio, output size, network transfer, and write time with representative data
before changing the defaults.

[`dataframe::write_parquet`]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.write_parquet
[`tableparquetoptions`]: https://docs.rs/datafusion/latest/datafusion/common/config/struct.TableParquetOptions.html
