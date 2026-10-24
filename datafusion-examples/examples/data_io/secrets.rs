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

//! Share a named secret between two table provider connections.
//! Run with `cargo run --example data_io -- secrets`.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::record_batch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider, TableProviderFactory};
use datafusion::common::secret::Secret;
use datafusion::common::{Result, assert_batches_eq, exec_err};
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::secrets::SecretCreateMode;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::prelude::SessionContext;

// An intentionally public synthetic credential. No external service is used.
const EXAMPLE_TOKEN: &str = "example-session-token";

pub async fn secrets() -> Result<()> {
    let ctx: SessionContext = SessionStateBuilder::new()
        .with_default_features()
        .with_table_factory("SOURCE_A", Arc::new(ExampleTableFactory { id: 1 }))
        .with_table_factory("SOURCE_B", Arc::new(ExampleTableFactory { id: 2 }))
        .build()
        .into();

    // Applications can inject credentials outside SQL. The SQL below only
    // contains the name, so connection plans do not need credential values.
    ctx.secrets_store()
        .register(
            "shared_connection",
            Secret::new(BTreeMap::from([(
                "token".to_string(),
                EXAMPLE_TOKEN.to_string(),
            )])),
            SecretCreateMode::ErrorIfExists,
        )
        .await?;

    for (table, provider) in [("first_source", "SOURCE_A"), ("second_source", "SOURCE_B")]
    {
        ctx.sql(&format!(
            "CREATE EXTERNAL TABLE {table} STORED AS {provider} \
             LOCATION 'synthetic://table' \
             OPTIONS ('connection.secret' 'shared_connection')"
        ))
        .await?;
    }

    let batches = ctx
        .sql("SELECT id FROM first_source UNION ALL SELECT id FROM second_source ORDER BY id")
        .await?
        .collect()
        .await?;
    assert_batches_eq!(
        ["+----+", "| id |", "+----+", "| 1  |", "| 2  |", "+----+"],
        &batches
    );

    // SQL uses the same store. Removing a name prevents future lookups, but
    // does not invalidate connections or snapshots already held by providers.
    ctx.sql("DROP SECRET shared_connection").await?;
    assert!(
        ctx.secrets_store()
            .get("shared_connection")
            .await?
            .is_none()
    );

    // When supplying a value in SQL, take care with the caller's SQL history
    // and logging. DataFusion cannot control copies of raw SQL outside itself.
    ctx.sql("CREATE SECRET from_sql (TOKEN 'example-session-token')")
        .await?;
    assert_eq!(
        ctx.secrets_store()
            .get("from_sql")
            .await?
            .unwrap()
            .get("token"),
        Some(EXAMPLE_TOKEN)
    );
    ctx.sql("DROP SECRET from_sql").await?;
    Ok(())
}

/// A provider defines how to interpret its reference option and secret fields.
/// The engine does not automatically apply credentials to any URL or service.
#[derive(Debug)]
struct ExampleTableFactory {
    id: i32,
}

#[async_trait]
impl TableProviderFactory for ExampleTableFactory {
    async fn create(
        &self,
        session: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let Some(reference) = cmd.options.get("connection.secret") else {
            return exec_err!("This provider requires a named secret reference");
        };
        let Some(secret) = session.secrets_store()?.get(reference).await? else {
            return exec_err!("The named connection secret is unavailable");
        };

        // A real integration would use these fields to establish a connection.
        // Validate the synthetic credential to show both providers consume it;
        // never include credentials in query data or error messages.
        if secret.get("token") != Some(EXAMPLE_TOKEN) {
            return exec_err!("Connection credential was rejected");
        }

        let batch = record_batch!(("id", Int32, [self.id]))?;
        Ok(Arc::new(MemTable::try_new(
            batch.schema(),
            vec![vec![batch]],
        )?))
    }
}
