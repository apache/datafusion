// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! See `main.rs` for how to run it.

use datafusion::error::Result;
use datafusion::prelude::*;
use object_store::http::HttpBuilder;
use std::sync::Arc;
use url::Url;

/// Configure `object_store` and run a query against files via HTTP
pub async fn query_http_csv() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();

    // setup http object store
    let base_url = Url::parse("https://github.com").unwrap();
    let http_store = HttpBuilder::new()
        .with_url(base_url.clone())
        .build()
        .unwrap();
    ctx.register_object_store(&base_url, Arc::new(http_store));

    // register csv file with the execution context
    ctx.register_csv(
        "aggregate_test_100",
        "https://github.com/apache/arrow-testing/raw/master/data/csv/aggregate_test_100.csv",
        CsvReadOptions::new(),
    )
    .await?;

    // execute the query
    let df = ctx
        .sql("SELECT c1,c2,c3 FROM aggregate_test_100 LIMIT 5")
        .await?;

    // print the results
    df.show().await?;

    Ok(())
}
