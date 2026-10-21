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

//! Connection plans carry secret names; credentials belong to the receiving session.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider, TableProviderFactory};
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::secrets::SecretCreateMode;
use datafusion::logical_expr::{CreateExternalTable, LogicalPlan};
use datafusion::prelude::SessionContext;
use datafusion_common::secret::Secret;
use datafusion_common::{Result, assert_batches_eq, exec_err};
use datafusion_proto::bytes::{logical_plan_from_bytes, logical_plan_to_bytes};
use datafusion_proto::protobuf;
use prost::Message;

// Public synthetic fixtures. Using different tokens proves that the receiving
// session resolves the reference instead of reusing the producer's credential.
const PRODUCER_TOKEN: &str = "producer-credential-canary-6521";
const EXECUTOR_TOKEN: &str = "executor-credential-canary-8934";

#[derive(Debug)]
struct ReferenceTableFactory;

#[async_trait]
impl TableProviderFactory for ReferenceTableFactory {
    async fn create(
        &self,
        session: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let Some(reference) = cmd.options.get("connection.secret") else {
            return exec_err!("A connection secret reference is required");
        };
        let Some(secret) = session.secrets_store()?.get(reference).await? else {
            return exec_err!("The receiving session has no connection secret");
        };
        if secret.get("token") != Some(EXECUTOR_TOKEN) {
            return exec_err!("The receiving connection credential was rejected");
        }
        let batch = RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int32Array::from(vec![17])) as ArrayRef,
        )])?;
        Ok(Arc::new(MemTable::try_new(
            batch.schema(),
            vec![vec![batch]],
        )?))
    }
}

fn context() -> SessionContext {
    SessionStateBuilder::new()
        .with_default_features()
        .with_table_factory("SECRET_REFERENCE", Arc::new(ReferenceTableFactory))
        .build()
        .into()
}

fn secret(token: &str) -> Secret {
    Secret::new(BTreeMap::from([("token".to_string(), token.to_string())]))
}

fn assert_no_credentials(bytes: &[u8]) {
    for token in [PRODUCER_TOKEN, EXECUTOR_TOKEN] {
        assert!(
            !bytes
                .windows(token.len())
                .any(|window| window == token.as_bytes()),
            "credential appeared in serialized output"
        );
    }
}

async fn assert_receiving_session_resolves_reference(
    executor: &SessionContext,
    plan: LogicalPlan,
) -> Result<()> {
    assert_no_credentials(format!("{plan:?}").as_bytes());
    assert_no_credentials(plan.display_indent().to_string().as_bytes());

    // Decoding a connection plan must not import the producer's secret.
    assert!(executor.secrets_store().get("shared").await?.is_none());
    let missing = executor
        .execute_logical_plan(plan.clone())
        .await
        .unwrap_err();
    assert!(missing.to_string().contains("has no connection secret"));
    assert_no_credentials(missing.to_string().as_bytes());
    assert!(!executor.table_exist("remote_table")?);

    // A named reference alone is insufficient: the provider validates the
    // receiver's credential, rather than merely accepting any stored value.
    executor
        .secrets_store()
        .register(
            "shared",
            secret(PRODUCER_TOKEN),
            SecretCreateMode::ErrorIfExists,
        )
        .await?;
    let rejected = executor
        .execute_logical_plan(plan.clone())
        .await
        .unwrap_err();
    assert!(rejected.to_string().contains("credential was rejected"));
    assert_no_credentials(rejected.to_string().as_bytes());
    assert!(!executor.table_exist("remote_table")?);

    // Supply a valid credential out of band, then execute the same decoded plan.
    executor
        .secrets_store()
        .register("shared", secret(EXECUTOR_TOKEN), SecretCreateMode::Replace)
        .await?;
    executor.execute_logical_plan(plan).await?.collect().await?;
    assert_batches_eq!(
        ["+----+", "| id |", "+----+", "| 17 |", "+----+"],
        &executor
            .sql("SELECT * FROM remote_table")
            .await?
            .collect()
            .await?
    );
    Ok(())
}

#[tokio::test]
async fn connection_plan_roundtrip_transfers_reference_without_credentials() -> Result<()>
{
    let producer = context();
    producer
        .secrets_store()
        .register(
            "shared",
            secret(PRODUCER_TOKEN),
            SecretCreateMode::ErrorIfExists,
        )
        .await?;
    let plan = producer
        .state()
        .create_logical_plan(
            "CREATE EXTERNAL TABLE remote_table STORED AS SECRET_REFERENCE \
             LOCATION 'synthetic://table' OPTIONS ('connection.secret' 'shared')",
        )
        .await?;
    assert!(!producer.table_exist("remote_table")?);
    let bytes = logical_plan_to_bytes(&plan)?;
    assert_no_credentials(&bytes);
    let proto = protobuf::LogicalPlanNode::decode(bytes.as_ref())
        .expect("encoded connection plan must decode");
    let Some(protobuf::logical_plan_node::LogicalPlanType::CreateExternalTable(table)) =
        &proto.logical_plan_type
    else {
        panic!("expected an external table connection plan")
    };
    assert_eq!(
        table.options.get("connection.secret").map(String::as_str),
        Some("shared")
    );
    let debug = format!("{proto:?}");
    assert!(debug.contains("shared"));
    assert_no_credentials(debug.as_bytes());
    let executor = context();
    let restored = logical_plan_from_bytes(&bytes, &executor.task_ctx())?;
    assert_receiving_session_resolves_reference(&executor, restored).await?;

    #[cfg(feature = "json")]
    {
        let json = datafusion_proto::bytes::logical_plan_to_json(&plan)?;
        assert!(json.contains("shared"));
        assert_no_credentials(json.as_bytes());
        let executor = context();
        let restored =
            datafusion_proto::bytes::logical_plan_from_json(&json, &executor.task_ctx())?;
        assert_receiving_session_resolves_reference(&executor, restored).await?;
    }

    assert_eq!(
        producer
            .secrets_store()
            .get("shared")
            .await?
            .unwrap()
            .get("token"),
        Some(PRODUCER_TOKEN)
    );
    Ok(())
}
