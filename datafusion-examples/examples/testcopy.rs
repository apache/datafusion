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

use datafusion::error::Result;
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

    let copy_to_path = "/home/dev/test/copy.parquet";
    let r = ctx.sql(&format!("COPY source TO '{copy_to_path}'")).await?;

    r.show().await?;

    Ok(())
}
