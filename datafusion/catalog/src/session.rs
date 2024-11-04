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

use async_trait::async_trait;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{DFSchema, Result};
use datafusion_execution::config::SessionConfig;
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_execution::TaskContext;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::{AggregateUDF, Expr, LogicalPlan, ScalarUDF, WindowUDF};
use datafusion_physical_plan::{ExecutionPlan, PhysicalExpr};
use parking_lot::{Mutex, RwLock};
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Weak};

/// Interface for accessing [`SessionState`] from the catalog.
///
/// This trait provides access to the information needed to plan and execute
/// queries, such as configuration, functions, and runtime environment. See the
/// documentation on [`SessionState`] for more information.
///
/// Historically, the `SessionState` struct was passed directly to catalog
/// traits such as [`TableProvider`], which required a direct dependency on the
/// DataFusion core. The interface required is now defined by this trait. See
/// [#10782] for more details.
///
/// [#10782]: https://github.com/apache/datafusion/issues/10782
///
/// # Migration from `SessionState`
///
/// Using trait methods is preferred, as the implementation may change in future
/// versions. However, you can downcast a `Session` to a `SessionState` as shown
/// in the example below. If you find yourself needing to do this, please open
/// an issue on the DataFusion repository so we can extend the trait to provide
/// the required information.
///
/// ```
/// # use datafusion_catalog::Session;
/// # use datafusion_common::{Result, exec_datafusion_err};
/// # struct SessionState {}
/// // Given a `Session` reference, get the concrete `SessionState` reference
/// // Note: this may stop working in future versions,
/// fn session_state_from_session(session: &dyn Session) -> Result<&SessionState> {
///    session.as_any()
///     .downcast_ref::<SessionState>()
///     .ok_or_else(|| exec_datafusion_err!("Failed to downcast Session to SessionState"))
/// }
/// ```
///
/// [`SessionState`]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html
/// [`TableProvider`]: crate::TableProvider
#[async_trait]
pub trait Session: Send + Sync {
    /// Return the session ID
    fn session_id(&self) -> &str;

    /// Return the [`SessionConfig`]
    fn config(&self) -> &SessionConfig;

    /// return the [`ConfigOptions`]
    fn config_options(&self) -> &ConfigOptions {
        self.config().options()
    }

    /// Creates a physical [`ExecutionPlan`] plan from a [`LogicalPlan`].
    ///
    /// Note: this will optimize the provided plan first.
    ///
    /// This function will error for [`LogicalPlan`]s such as catalog DDL like
    /// `CREATE TABLE`, which do not have corresponding physical plans and must
    /// be handled by another layer, typically the `SessionContext`.
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Create a [`PhysicalExpr`] from an [`Expr`] after applying type
    /// coercion, and function rewrites.
    ///
    /// Note: The expression is not simplified or otherwise optimized:  `a = 1
    /// + 2` will not be simplified to `a = 3` as this is a more involved process.
    /// See the [expr_api] example for how to simplify expressions.
    ///
    /// [expr_api]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/expr_api.rs
    fn create_physical_expr(
        &self,
        expr: Expr,
        df_schema: &DFSchema,
    ) -> Result<Arc<dyn PhysicalExpr>>;

    /// Return reference to scalar_functions
    fn scalar_functions(&self) -> &HashMap<String, Arc<ScalarUDF>>;

    /// Return reference to aggregate_functions
    fn aggregate_functions(&self) -> &HashMap<String, Arc<AggregateUDF>>;

    /// Return reference to window functions
    fn window_functions(&self) -> &HashMap<String, Arc<WindowUDF>>;

    /// Return the runtime env
    fn runtime_env(&self) -> &Arc<RuntimeEnv>;

    /// Return the execution properties
    fn execution_props(&self) -> &ExecutionProps;

    fn as_any(&self) -> &dyn Any;
}

/// Create a new task context instance from Session
impl From<&dyn Session> for TaskContext {
    fn from(state: &dyn Session) -> Self {
        let task_id = None;
        TaskContext::new(
            task_id,
            state.session_id().to_string(),
            state.config().clone(),
            state.scalar_functions().clone(),
            state.aggregate_functions().clone(),
            state.window_functions().clone(),
            state.runtime_env().clone(),
        )
    }
}
type SessionRefLock = Arc<Mutex<Option<Weak<RwLock<dyn Session>>>>>;
/// The state store that stores the reference of the runtime session state.
#[derive(Debug)]
pub struct SessionStore {
    session: SessionRefLock,
}

impl SessionStore {
    /// Create a new [SessionStore]
    pub fn new() -> Self {
        Self {
            session: Arc::new(Mutex::new(None)),
        }
    }

    /// Set the session state of the store
    pub fn with_state(&self, state: Weak<RwLock<dyn Session>>) {
        let mut lock = self.session.lock();
        *lock = Some(state);
    }

    /// Get the current session of the store
    pub fn get_session(&self) -> Weak<RwLock<dyn Session>> {
        self.session.lock().clone().unwrap()
    }
}

impl Default for SessionStore {
    fn default() -> Self {
        Self::new()
    }
}
