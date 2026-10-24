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

//! Exercise named secrets through public session, SQL, and provider APIs.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::record_batch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider, TableProviderFactory};
use datafusion::common::secret::Secret;
use datafusion::common::{Result, assert_batches_eq, exec_err};
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::secrets::{
    MemorySecretsStore, SecretCreateMode, SecretsStore,
};
use datafusion::logical_expr::CreateExternalTable;
use datafusion::prelude::{SQLOptions, SessionContext};

const CANARY: &str = "session-secret-private-value-canary";

fn secret(value: &str) -> Secret {
    Secret::new(BTreeMap::from([("token".to_string(), value.to_string())]))
}

fn assert_redacted(output: impl AsRef<str>) {
    assert!(
        !output.as_ref().contains(CANARY),
        "secret value was exposed"
    );
}

/// Treat the token as a connection credential, never as query data.
#[derive(Debug)]
struct CredentialTableFactory {
    id: i32,
}

#[async_trait]
impl TableProviderFactory for CredentialTableFactory {
    async fn create(
        &self,
        state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let Some(name) = cmd.options.get("connection.secret") else {
            return exec_err!("Missing named secret reference");
        };
        let Some(secret) = state.secrets_store()?.get(name).await? else {
            return exec_err!("Named secret is unavailable");
        };
        // This positive control ensures the test actually uses the secret.
        if secret.get("token") != Some(CANARY) {
            return exec_err!("Connection credential was rejected");
        }
        let batch = record_batch!(("id", Int32, [self.id]))?;
        Ok(Arc::new(MemTable::try_new(
            batch.schema(),
            vec![vec![batch]],
        )?))
    }
}

fn context(store: Arc<dyn SecretsStore>) -> SessionContext {
    SessionStateBuilder::new()
        .with_default_features()
        .with_secrets_store(store)
        .with_table_factory("CREDENTIAL_A", Arc::new(CredentialTableFactory { id: 7 }))
        .with_table_factory("CREDENTIAL_B", Arc::new(CredentialTableFactory { id: 11 }))
        .build()
        .into()
}

async fn connect(
    ctx: &SessionContext,
    table: &str,
    provider: &str,
    reference: &str,
) -> Result<()> {
    ctx.sql(&format!(
        "CREATE EXTERNAL TABLE {table} STORED AS {provider} \
         LOCATION 'synthetic://table' OPTIONS ('connection.secret' '{reference}')"
    ))
    .await?;
    Ok(())
}

#[tokio::test]
async fn two_providers_share_an_out_of_band_secret() -> Result<()> {
    let ctx = context(Arc::new(MemorySecretsStore::new()));
    ctx.secrets_store()
        .register("shared", secret(CANARY), SecretCreateMode::ErrorIfExists)
        .await?;

    let connection_plan = ctx
        .state()
        .create_logical_plan(
            "CREATE EXTERNAL TABLE planned STORED AS CREDENTIAL_A \
         LOCATION 'synthetic://table' OPTIONS ('connection.secret' 'shared')",
        )
        .await?;
    let connection_debug = format!("{connection_plan:?}");
    assert!(connection_debug.contains("shared"));
    assert_redacted(connection_debug);

    connect(&ctx, "left_table", "CREDENTIAL_A", "shared").await?;
    connect(&ctx, "right_table", "CREDENTIAL_B", "shared").await?;
    let query = ctx
        .sql("SELECT id FROM left_table UNION ALL SELECT id FROM right_table ORDER BY id")
        .await?;
    assert_redacted(format!("{:?}", query.logical_plan()));
    assert_redacted(query.logical_plan().display_indent().to_string());
    assert_redacted(format!("{:?}", query.clone().into_optimized_plan()?));
    assert_batches_eq!(
        ["+----+", "| id |", "+----+", "| 7  |", "| 11 |", "+----+"],
        &query.collect().await?
    );

    // Dropping a stored value affects future connections, not those already
    // established by a provider with a snapshot of the credential.
    ctx.sql("DROP SECRET shared").await?;
    let error = connect(&ctx, "missing", "CREDENTIAL_A", "shared")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Named secret is unavailable"));
    assert_redacted(error.to_string());
    let error = ctx
        .sql(
            "CREATE EXTERNAL TABLE no_reference STORED AS CREDENTIAL_A \
             LOCATION 'synthetic://table'",
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Missing named secret reference"));
    assert_redacted(error.to_string());
    assert_eq!(
        ctx.sql("SELECT * FROM left_table").await?.collect().await?[0].num_rows(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn sql_and_programmatic_updates_reach_the_same_provider_store() -> Result<()> {
    let ctx = context(Arc::new(MemorySecretsStore::new()));
    ctx.sql(&format!("CREATE SECRET shared (TOKEN '{CANARY}')"))
        .await?;
    assert_eq!(
        ctx.secrets_store()
            .get("shared")
            .await?
            .unwrap()
            .get("token"),
        Some(CANARY)
    );
    connect(&ctx, "from_sql", "CREDENTIAL_A", "shared").await?;

    // The provider must reject an incorrect value rather than ignoring it.
    ctx.secrets_store()
        .register(
            "shared",
            secret("incorrect-token"),
            SecretCreateMode::Replace,
        )
        .await?;
    let error = connect(&ctx, "rejected", "CREDENTIAL_B", "shared")
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Connection credential was rejected")
    );
    assert_redacted(error.to_string());
    assert!(!ctx.table_exist("rejected")?);

    ctx.sql(&format!(
        "CREATE OR REPLACE SECRET shared (TOKEN '{CANARY}')"
    ))
    .await?;
    connect(&ctx, "after_rotation", "CREDENTIAL_B", "shared").await?;
    assert_batches_eq!(
        ["+----+", "| id |", "+----+", "| 11 |", "+----+"],
        &ctx.sql("SELECT * FROM after_rotation")
            .await?
            .collect()
            .await?
    );
    Ok(())
}

#[tokio::test]
async fn cloned_and_rebuilt_states_share_secrets_but_new_sessions_do_not() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.sql(&format!("CREATE SECRET shared (TOKEN '{CANARY}')"))
        .await?;
    let cloned = ctx.clone();
    let rebuilt: SessionContext = SessionStateBuilder::new_from_existing(ctx.state())
        .build()
        .into();
    let independent = SessionContext::new();
    assert!(independent.secrets_store().get("shared").await?.is_none());
    for sibling in [&cloned, &rebuilt] {
        assert!(Arc::ptr_eq(&ctx.secrets_store(), &sibling.secrets_store()));
        assert_eq!(
            sibling
                .secrets_store()
                .get("shared")
                .await?
                .unwrap()
                .get("token"),
            Some(CANARY)
        );
    }
    cloned.sql("DROP SECRET shared").await?;
    assert!(ctx.secrets_store().get("shared").await?.is_none());
    assert!(rebuilt.secrets_store().get("shared").await?.is_none());
    Ok(())
}

/// A minimal application-provided store demonstrating the public extension
/// point without a real credentials service or network dependency.
#[derive(Default)]
struct ApplicationSecretsStore {
    inner: MemorySecretsStore,
    lookups: AtomicUsize,
}

impl std::fmt::Debug for ApplicationSecretsStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Deliberately misbehaving application code: SessionState and its
        // builder must not delegate their Debug output to a custom store.
        f.debug_struct("ApplicationSecretsStore")
            .field("misbehaving_provider_debug", &CANARY)
            .finish()
    }
}

#[async_trait]
impl SecretsStore for ApplicationSecretsStore {
    async fn get(&self, name: &str) -> Result<Option<Arc<Secret>>> {
        self.lookups.fetch_add(1, Ordering::Relaxed);
        if name == "service_unavailable" {
            return exec_err!("Credentials service is unavailable");
        }
        self.inner.get(name).await
    }

    async fn register(
        &self,
        name: &str,
        secret: Secret,
        mode: SecretCreateMode,
    ) -> Result<()> {
        self.inner.register(name, secret, mode).await
    }

    async fn remove(&self, name: &str, if_exists: bool) -> Result<()> {
        self.inner.remove(name, if_exists).await
    }
}

#[tokio::test]
async fn custom_store_is_used_by_sql_and_providers_and_propagates_lookup_errors()
-> Result<()> {
    let store = Arc::new(ApplicationSecretsStore::default());
    assert!(format!("{store:?}").contains(CANARY));
    let store_for_builder: Arc<dyn SecretsStore> = store.clone();
    let builder = SessionStateBuilder::new().with_secrets_store(store_for_builder);
    assert_redacted(format!("{builder:?}"));
    let ctx = context(Arc::clone(&store) as Arc<dyn SecretsStore>);
    assert_redacted(format!("{:?}", ctx.state()));
    ctx.sql(&format!("CREATE SECRET shared (TOKEN '{CANARY}')"))
        .await?;
    assert_eq!(
        store.inner.get("shared").await?.unwrap().get("token"),
        Some(CANARY)
    );
    connect(&ctx, "left_table", "CREDENTIAL_A", "shared").await?;
    connect(&ctx, "right_table", "CREDENTIAL_B", "shared").await?;
    assert_eq!(store.lookups.load(Ordering::Relaxed), 2);

    let error = connect(&ctx, "unavailable", "CREDENTIAL_A", "service_unavailable")
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Credentials service is unavailable")
    );
    assert_redacted(error.to_string());
    assert!(!ctx.table_exist("unavailable")?);
    assert_eq!(store.lookups.load(Ordering::Relaxed), 3);
    ctx.sql("DROP SECRET shared").await?;
    assert!(store.inner.get("shared").await?.is_none());
    Ok(())
}

#[tokio::test]
async fn secret_statements_plans_and_explain_redact_values_without_mutation() -> Result<()>
{
    let ctx = SessionContext::new();
    let sql = format!("CREATE SECRET shared (TOKEN '{CANARY}')");
    let state = ctx.state();
    let dialect = state.config().options().sql_parser.dialect;
    let statement = state.sql_to_statement(&sql, &dialect)?;
    assert_redacted(statement.to_string());
    assert_redacted(format!("{statement:?}"));
    let plan = state.statement_to_plan(statement).await?;
    assert_redacted(format!("{plan:?}"));
    assert_redacted(plan.display_indent().to_string());
    assert_redacted(format!("{:?}", state.optimize(&plan)?));
    assert!(ctx.secrets_store().get("shared").await?.is_none());

    let batches = ctx.sql(&format!("EXPLAIN {sql}")).await?.collect().await?;
    assert_redacted(format!("{batches:?}"));
    assert!(ctx.secrets_store().get("shared").await?.is_none());

    ctx.sql(&sql).await?;
    assert_redacted(format!("{:?}", ctx.state()));
    assert_redacted(format!("{:?}", ctx.secrets_store()));
    let duplicate = ctx.sql(&sql).await.unwrap_err();
    assert_redacted(duplicate.to_string());
    assert!(duplicate.to_string().contains("already exists"));
    Ok(())
}

#[tokio::test]
async fn disallowed_ddl_does_not_create_replace_or_drop_secrets() -> Result<()> {
    let ctx = SessionContext::new();
    let options = SQLOptions::new().with_allow_ddl(false);
    let sql = format!("CREATE SECRET shared (TOKEN '{CANARY}')");
    let error = ctx.sql_with_options(&sql, options).await.unwrap_err();
    assert!(error.to_string().contains("DDL not supported"));
    assert_redacted(error.to_string());
    assert!(ctx.secrets_store().get("shared").await?.is_none());

    ctx.secrets_store()
        .register("shared", secret(CANARY), SecretCreateMode::ErrorIfExists)
        .await?;
    for sql in [
        "CREATE OR REPLACE SECRET shared (TOKEN 'replacement')",
        "DROP SECRET shared",
    ] {
        let error = ctx.sql_with_options(sql, options).await.unwrap_err();
        assert!(error.to_string().contains("DDL not supported"));
        assert_redacted(error.to_string());
        assert_eq!(
            ctx.secrets_store()
                .get("shared")
                .await?
                .unwrap()
                .get("token"),
            Some(CANARY)
        );
    }
    Ok(())
}
