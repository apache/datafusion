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

use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::listing::ListingTableInsertMode;
use datafusion::error::Result;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::prelude::*;

use object_store::local::LocalFileSystem;
use std::sync::Arc;
use url::Url;

#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();

    let local = Arc::new(LocalFileSystem::new());
    let local_url = Url::parse("file://local").unwrap();
    ctx.runtime_env().register_object_store(&local_url, local);

    ctx.register_parquet(
        "source",
        "file://local/home/dev/dftest/test5.parquet",
        ParquetReadOptions::default(),
    )
    .await?;
    let _path = "file://local/home/dev/dftest/testdata/";
    //let file_format = ParquetFormat::default().with_enable_pruning(Some(true));
    //let listing_options = ListingOptions::new(Arc::new(file_format))
    //    .with_file_extension(FileType::PARQUET.get_ext());
    //ctx.register_listing_table("source", &path, listing_options, None, None)
    //    .await?;
    let df = ctx
        .sql("select * from source")
        .await?
        .limit(0, Some(2))?
        .repartition(Partitioning::RoundRobinBatch(1))?;

    let in_path = "file://local/home/dev/dftest/parquet_data/";
    df.clone()
        .write_parquet(in_path, Some(WriterProperties::default()))
        .await?;
    ctx.register_parquet("parquet_in", in_path, ParquetReadOptions::default())
        .await?;

    let out_path = "file://local/home/dev/dftest/parquet_data_sink/";
    df.clone()
        .write_parquet(out_path, Some(WriterProperties::default()))
        .await?;
    ctx.register_parquet(
        "parquet_table_sink",
        out_path,
        ParquetReadOptions::default().insert_mode(ListingTableInsertMode::AppendNewFiles),
    )
    .await?;

    println!("writing table!");
    // execute the query
    let df = ctx
        .sql("SELECT * FROM parquet_in")
        .await?
        .repartition(Partitioning::RoundRobinBatch(1))?;

    //df.write_csv(out_path).await?;
    let _cnt = df
        .clone()
        .write_table("parquet_table_sink", DataFrameWriteOptions::new())
        .await?;

    let df = ctx.sql("select count(*) from parquet_table_sink").await?;
    df.show().await?;
    //let out_path2 = "file://local/home/dev/dftest/csv_data_sink2/";
    //df.write_csv(&out_path2).await?;
    //println!("Got cnt {:?}", cnt);
    //df.write_parquet(&out_path, None).await?;

    //write as JSON to s3 instead
    //df.write_json(&out_path).await?;

    //write as csv to s3 instead
    //df.write_csv(&out_path).await?;

    // println!("Write complete! Now reading...");

    // let file_format = ParquetFormat::default();
    // let listing_options = ListingOptions::new(Arc::new(file_format))
    //     .with_file_extension(FileType::PARQUET.get_ext());
    // ctx.register_listing_table("test2", &out_path, listing_options, None, None)
    //     .await?;

    // let df = ctx
    //     .sql(
    //         "SELECT count(1) \
    //     FROM test2 \
    //     ",
    //     )
    //     .await?;

    // df.show_limit(20).await?;

    Ok(())
}
