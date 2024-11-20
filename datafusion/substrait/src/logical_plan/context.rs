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

use async_trait::async_trait;
use datafusion::{
    catalog::TableProvider,
    error::{DataFusionError, Result},
    execution::{registry::SerializerRegistry, FunctionRegistry, SessionState},
    sql::TableReference,
};

/// This trait provides the context needed to transform a substrait plan into a
/// [`datafusion::logical_expr::LogicalPlan`] (via [`super::consumer::from_substrait_plan`])
/// and back again into a substrait plan (via [`super::producer::to_substrait_plan`]).
///
/// The context is declared as a trait to decouple the substrait plan encoder /
/// decoder from the [`SessionState`], potentially allowing users to define
/// their own slimmer context just for serializing and deserializing substrait.
///
/// [`SessionState`] implements this trait.
#[async_trait]
pub trait SubstraitPlanningContext: Sync + Send + FunctionRegistry {
    /// Return [SerializerRegistry] for extensions
    fn serializer_registry(&self) -> &Arc<dyn SerializerRegistry>;

    async fn table(
        &self,
        reference: &TableReference,
    ) -> Result<Option<Arc<dyn TableProvider>>>;
}

#[async_trait]
impl SubstraitPlanningContext for SessionState {
    fn serializer_registry(&self) -> &Arc<dyn SerializerRegistry> {
        self.serializer_registry()
    }

    async fn table(
        &self,
        reference: &TableReference,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let table = reference.table().to_string();
        let schema = self.schema_for_ref(reference.clone())?;
        let table_provider = schema.table(&table).await?;
        Ok(table_provider)
    }
}
