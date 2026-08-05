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

//! The complete serialization environment used at an FFI boundary.
//!
//! Serializing DataFusion plans across a library boundary requires three values
//! that must agree with one another: a [`FFI_TaskContextProvider`] that resolves
//! the current [`TaskContext`], a logical extension codec, and a physical
//! extension codec. [`FFI_ExtensionCodecBundle`] carries them as one unit so
//! that a wrapper cannot be built from a codec and a provider that were never
//! configured together, and so that a wrapper which only needs to serialize
//! logical plans still has a physical codec to hand to any session it exports.
//!
//! # Structure
//!
//! The bundle is exactly its three members. Each one already carries its own
//! `clone` / `release` function pointers, `version` extern, and
//! `library_marker_id`, so the bundle holds no private data and needs no
//! foreign adapter of its own; `Clone` and `Drop` come from the members.
//!
//! The dependency direction is bundle to codecs to task context provider. A
//! bundle must never be stored inside a codec: cloning a bundle clones its
//! logical codec, so a bundle field on a codec would make cloning recurse until
//! the stack is exhausted.

use std::sync::Arc;

use datafusion_common::error::Result;
use datafusion_execution::TaskContext;
use datafusion_proto::logical_plan::{
    DefaultLogicalExtensionCodec, LogicalExtensionCodec,
};
use datafusion_proto::physical_plan::{
    DefaultPhysicalExtensionCodec, PhysicalExtensionCodec,
};
use tokio::runtime::Handle;

use crate::execution::FFI_TaskContextProvider;
use crate::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use crate::proto::physical_extension_codec::FFI_PhysicalExtensionCodec;

/// A stable struct describing one complete serialization environment.
///
/// The fields are private so that the constructors are the only way to pair a
/// task context provider with the codecs that use it. Read access is through
/// [`Self::task_ctx_provider`], [`Self::logical_codec`], and
/// [`Self::physical_codec`].
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_ExtensionCodecBundle {
    task_ctx_provider: FFI_TaskContextProvider,

    logical_codec: FFI_LogicalExtensionCodec,

    physical_codec: FFI_PhysicalExtensionCodec,
}

impl FFI_ExtensionCodecBundle {
    /// Creates a bundle from one task context provider and native codecs.
    ///
    /// Both codecs are exported using `task_ctx_provider`. `runtime` is attached
    /// to both codecs for callbacks that must enter the exporting library's Tokio
    /// runtime.
    ///
    /// The provider is held weakly. It must outlive every wrapper built from
    /// this bundle; otherwise codec callbacks fail with a clear error rather
    /// than resolving a stale [`TaskContext`]. See [`Self::task_ctx`].
    pub fn new(
        task_ctx_provider: impl Into<FFI_TaskContextProvider>,
        runtime: Option<Handle>,
        logical_codec: Arc<dyn LogicalExtensionCodec>,
        physical_codec: Arc<dyn PhysicalExtensionCodec>,
    ) -> Self {
        let task_ctx_provider = task_ctx_provider.into();
        let logical_codec = FFI_LogicalExtensionCodec::new(
            logical_codec,
            runtime.clone(),
            task_ctx_provider.clone(),
        );
        let physical_codec = FFI_PhysicalExtensionCodec::new(
            physical_codec,
            runtime,
            task_ctx_provider.clone(),
        );

        Self {
            task_ctx_provider,
            logical_codec,
            physical_codec,
        }
    }

    /// Creates a bundle whose codecs support built-in nodes only.
    ///
    /// Use this when no custom logical or physical extension nodes cross the
    /// boundary. A wrapper built from this bundle cannot round-trip custom
    /// extension nodes; attempting it fails during encoding or decoding rather
    /// than silently dropping the node.
    pub fn new_default(
        task_ctx_provider: impl Into<FFI_TaskContextProvider>,
        runtime: Option<Handle>,
    ) -> Self {
        Self::new(
            task_ctx_provider,
            runtime,
            Arc::new(DefaultLogicalExtensionCodec {}),
            Arc::new(DefaultPhysicalExtensionCodec {}),
        )
    }

    /// Creates a bundle from codecs that already crossed an FFI boundary.
    ///
    /// Cloning or re-exporting FFI codecs does not nest foreign wrappers: each
    /// codec's `clone` returns a handle owned by its original library.
    ///
    /// This constructor cannot verify that the three arguments belong together,
    /// so the caller asserts that both codecs were exported with
    /// `task_ctx_provider` and that they can round-trip every extension node
    /// the resulting wrappers expose. Prefer [`Self::new`] whenever the native
    /// codecs are available.
    pub fn new_with_ffi_codecs(
        task_ctx_provider: FFI_TaskContextProvider,
        logical_codec: FFI_LogicalExtensionCodec,
        physical_codec: FFI_PhysicalExtensionCodec,
    ) -> Self {
        Self {
            task_ctx_provider,
            logical_codec,
            physical_codec,
        }
    }

    /// Pairs an existing logical codec with an explicit default physical codec.
    ///
    /// Used by the paths inside [`FFI_LogicalExtensionCodec`] that build a nested
    /// [`FFI_TableProvider`](crate::table_provider::FFI_TableProvider) from a
    /// function pointer receiving the codec alone, where a codec cannot carry a
    /// bundle (see the [module documentation](self)). The task context provider is
    /// cloned out of `logical_codec`.
    ///
    /// A table provider built from this bundle cannot carry custom physical
    /// extension nodes through a session callback. If the library that decoded
    /// the provider scans it with a session local to that library, and the
    /// provider reaches back through [`Session::query_planner`] for a custom
    /// physical node, the call fails with `PhysicalExtensionCodec is not
    /// provided`. When the provider is instead scanned with the exporting
    /// library's own session handle, exporting that session short-circuits to
    /// the original handle and its complete bundle, so that topology is
    /// unaffected.
    ///
    /// [`Session::query_planner`]: datafusion_session::Session::query_planner
    pub(crate) fn new_logical_with_default_physical(
        logical_codec: FFI_LogicalExtensionCodec,
        runtime: Option<Handle>,
    ) -> Self {
        let task_ctx_provider = logical_codec.task_ctx_provider.clone();
        let physical_codec = FFI_PhysicalExtensionCodec::new(
            Arc::new(DefaultPhysicalExtensionCodec {}),
            runtime,
            task_ctx_provider.clone(),
        );

        Self {
            task_ctx_provider,
            logical_codec,
            physical_codec,
        }
    }

    /// The task context provider shared by both codecs.
    pub fn task_ctx_provider(&self) -> &FFI_TaskContextProvider {
        &self.task_ctx_provider
    }

    /// The logical extension codec.
    pub fn logical_codec(&self) -> &FFI_LogicalExtensionCodec {
        &self.logical_codec
    }

    /// The physical extension codec.
    pub fn physical_codec(&self) -> &FFI_PhysicalExtensionCodec {
        &self.physical_codec
    }

    /// Resolves the current [`TaskContext`].
    ///
    /// Returns an error if the [`TaskContextProvider`] this bundle was built
    /// from has been dropped. The context is resolved on every call rather than
    /// captured at construction, so functions and other session state
    /// registered after the bundle was created are visible to codec callbacks.
    ///
    /// [`TaskContextProvider`]: datafusion_execution::TaskContextProvider
    pub fn task_ctx(&self) -> Result<Arc<TaskContext>> {
        (&self.task_ctx_provider).try_into()
    }

    /// The logical codec as a native trait object.
    ///
    /// Returns the underlying codec directly when it is owned by this library,
    /// and a foreign adapter otherwise.
    pub fn to_logical_codec(&self) -> Arc<dyn LogicalExtensionCodec> {
        (&self.logical_codec).into()
    }

    /// The physical codec as a native trait object.
    ///
    /// Returns the underlying codec directly when it is owned by this library,
    /// and a foreign adapter otherwise.
    pub fn to_physical_codec(&self) -> Arc<dyn PhysicalExtensionCodec> {
        (&self.physical_codec).into()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::prelude::SessionContext;
    use datafusion_common::{DataFusionError, Result};
    use datafusion_execution::TaskContextProvider;
    use datafusion_expr::ptr_eq::arc_ptr_eq;
    use datafusion_proto::logical_plan::{
        DefaultLogicalExtensionCodec, LogicalExtensionCodec,
    };
    use datafusion_proto::physical_plan::{
        DefaultPhysicalExtensionCodec, PhysicalExtensionCodec,
    };

    use crate::execution::FFI_TaskContextProvider;
    use crate::proto::extension_codec_bundle::FFI_ExtensionCodecBundle;
    use crate::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
    use crate::proto::physical_extension_codec::FFI_PhysicalExtensionCodec;
    use crate::proto::physical_extension_codec::tests::TestExtensionCodec;

    /// Both codec traits require [`Any`](std::any::Any), so a codec trait object
    /// can be re-borrowed as one to check the concrete type behind it.
    fn logical_is<T: 'static>(codec: &Arc<dyn LogicalExtensionCodec>) -> bool {
        let any_ref: &dyn std::any::Any = codec.as_ref();
        any_ref.is::<T>()
    }

    fn physical_is<T: 'static>(codec: &Arc<dyn PhysicalExtensionCodec>) -> bool {
        let any_ref: &dyn std::any::Any = codec.as_ref();
        any_ref.is::<T>()
    }

    #[test]
    fn bundle_new_retains_both_codecs() -> Result<()> {
        let (_ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();
        let logical = Arc::new(TestExtensionCodec {}) as Arc<dyn LogicalExtensionCodec>;
        let physical = Arc::new(TestExtensionCodec {}) as Arc<dyn PhysicalExtensionCodec>;

        let bundle = FFI_ExtensionCodecBundle::new(
            task_ctx_provider,
            None,
            Arc::clone(&logical),
            Arc::clone(&physical),
        );

        // Both codecs are local, so the conversions hand back the originals.
        assert!(arc_ptr_eq(&bundle.to_logical_codec(), &logical));
        assert!(arc_ptr_eq(&bundle.to_physical_codec(), &physical));
        bundle.task_ctx()?;

        Ok(())
    }

    #[test]
    fn bundle_new_default_uses_default_codecs() {
        let (_ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();
        let bundle = FFI_ExtensionCodecBundle::new_default(task_ctx_provider, None);

        assert!(logical_is::<DefaultLogicalExtensionCodec>(
            &bundle.to_logical_codec()
        ));
        assert!(physical_is::<DefaultPhysicalExtensionCodec>(
            &bundle.to_physical_codec()
        ));
    }

    #[test]
    fn bundle_clone_preserves_codec_identity() -> Result<()> {
        let (_ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();
        let logical = Arc::new(TestExtensionCodec {}) as Arc<dyn LogicalExtensionCodec>;
        let physical = Arc::new(TestExtensionCodec {}) as Arc<dyn PhysicalExtensionCodec>;

        let bundle = FFI_ExtensionCodecBundle::new(
            task_ctx_provider,
            None,
            Arc::clone(&logical),
            Arc::clone(&physical),
        );
        let cloned = bundle.clone();

        // Cloning must not wrap the codecs in another foreign layer.
        assert!(arc_ptr_eq(&cloned.to_logical_codec(), &logical));
        assert!(arc_ptr_eq(&cloned.to_physical_codec(), &physical));
        cloned.task_ctx()?;

        Ok(())
    }

    #[test]
    fn bundle_new_with_ffi_codecs_retains_supplied_codecs() {
        let (_ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();
        let logical = Arc::new(TestExtensionCodec {}) as Arc<dyn LogicalExtensionCodec>;
        let physical = Arc::new(TestExtensionCodec {}) as Arc<dyn PhysicalExtensionCodec>;
        let ffi_logical = FFI_LogicalExtensionCodec::new(
            Arc::clone(&logical),
            None,
            task_ctx_provider.clone(),
        );
        let ffi_physical = FFI_PhysicalExtensionCodec::new(
            Arc::clone(&physical),
            None,
            task_ctx_provider.clone(),
        );

        let bundle = FFI_ExtensionCodecBundle::new_with_ffi_codecs(
            task_ctx_provider,
            ffi_logical,
            ffi_physical,
        );

        assert!(arc_ptr_eq(&bundle.to_logical_codec(), &logical));
        assert!(arc_ptr_eq(&bundle.to_physical_codec(), &physical));
    }

    #[test]
    fn bundle_logical_with_default_physical_is_explicit() {
        let (_ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();
        let logical = Arc::new(TestExtensionCodec {}) as Arc<dyn LogicalExtensionCodec>;
        let ffi_logical =
            FFI_LogicalExtensionCodec::new(Arc::clone(&logical), None, task_ctx_provider);

        let bundle = FFI_ExtensionCodecBundle::new_logical_with_default_physical(
            ffi_logical,
            None,
        );

        assert!(arc_ptr_eq(&bundle.to_logical_codec(), &logical));
        assert!(physical_is::<DefaultPhysicalExtensionCodec>(
            &bundle.to_physical_codec()
        ));
        // The provider is inherited from the logical codec, so it still resolves.
        assert!(bundle.task_ctx().is_ok());
    }

    #[test]
    fn bundle_reports_expired_task_context_provider() {
        fn bundle_with_dropped_provider() -> FFI_ExtensionCodecBundle {
            let ctx = Arc::new(SessionContext::new());
            let task_ctx_provider = Arc::clone(&ctx) as Arc<dyn TaskContextProvider>;
            FFI_ExtensionCodecBundle::new_default(
                FFI_TaskContextProvider::from(&task_ctx_provider),
                None,
            )
        }

        let bundle = bundle_with_dropped_provider();
        let Err(DataFusionError::Ffi(message)) = bundle.task_ctx() else {
            panic!("expected an out of scope error from an expired provider")
        };
        assert!(message.contains("went out of scope"), "{message}");
    }
}
