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

//! Session plumbing for the extension [`PhysicalExpr`] decoder registry.
//!
//! The registry itself lives in `datafusion-physical-expr-common`, next to the
//! encode/decode contexts it is expressed in terms of, so that a crate defining
//! an extension expression need not depend on `datafusion-proto`. What lives
//! here is the session half: attaching a registry to a [`SessionConfig`] and
//! looking one up while decoding.
//!
//! [`PhysicalExpr`]: datafusion_physical_plan::PhysicalExpr

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use datafusion_physical_expr_common::physical_expr::proto_decode::{
    ExtensionPhysicalExpr, PhysicalExprDecoderFn, PhysicalExprDecoderRegistry,
};

/// Registers extension [`PhysicalExpr`]s on a [`SessionConfig`], so that
/// expressions naming themselves on the wire decode through
/// [`ExtensionPhysicalExpr::try_from_proto`] instead of through a
/// [`PhysicalExtensionCodec`].
///
/// ```ignore
/// use datafusion_proto::physical_plan::PhysicalExprRegistration;
///
/// let config = SessionConfig::new().with_physical_expr::<MyExpr>()?;
/// let ctx = SessionContext::new_with_config(config);
/// ```
///
/// Registration is additive: each call merges into the registry already on the
/// config, so a library registering its own expressions cannot silently drop
/// another's. Both methods error if the name is already taken.
///
/// [`PhysicalExpr`]: datafusion_physical_plan::PhysicalExpr
/// [`PhysicalExtensionCodec`]: super::PhysicalExtensionCodec
pub trait PhysicalExprRegistration {
    /// Register `T`, keyed by [`ExtensionPhysicalExpr::EXPR_NAME`].
    fn register_physical_expr<T: ExtensionPhysicalExpr>(&mut self) -> Result<()>;

    /// Register `T`, returning `self` for chaining.
    fn with_physical_expr<T: ExtensionPhysicalExpr>(self) -> Result<Self>
    where
        Self: Sized;
}

impl PhysicalExprRegistration for SessionConfig {
    fn register_physical_expr<T: ExtensionPhysicalExpr>(&mut self) -> Result<()> {
        let mut registry = self
            .get_extension::<PhysicalExprDecoderRegistry>()
            .map(|registry| registry.as_ref().clone())
            .unwrap_or_default();
        registry.register::<T>()?;
        self.set_extension(Arc::new(registry));
        Ok(())
    }

    fn with_physical_expr<T: ExtensionPhysicalExpr>(mut self) -> Result<Self> {
        self.register_physical_expr::<T>()?;
        Ok(self)
    }
}

/// The decoder registered for `expr_name` in this session, if any.
///
/// `None` — no registry on the session, or no such name in it — means the node
/// decodes through [`PhysicalExtensionCodec::try_decode_expr`] as before.
///
/// [`PhysicalExtensionCodec::try_decode_expr`]: super::PhysicalExtensionCodec::try_decode_expr
pub(crate) fn lookup_physical_expr_decoder(
    task_ctx: &TaskContext,
    expr_name: &str,
) -> Option<PhysicalExprDecoderFn> {
    task_ctx
        .session_config()
        .get_extension::<PhysicalExprDecoderRegistry>()?
        .get(expr_name)
}
