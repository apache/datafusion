use datafusion::prelude::SessionContext;
use datafusion_execution::TaskContextProvider;
use datafusion_ffi::execution::FFI_TaskContextProvider;
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_proto::logical_plan::DefaultLogicalExtensionCodec;
use std::sync::Arc;

pub fn ctx_and_codec() -> (Arc<SessionContext>, FFI_LogicalExtensionCodec) {
    let ctx = Arc::new(SessionContext::default());
    let task_ctx_provider = Arc::clone(&ctx) as Arc<dyn TaskContextProvider>;
    let task_ctx_provider = FFI_TaskContextProvider::from(&task_ctx_provider);
    let codec = FFI_LogicalExtensionCodec::new(
        Arc::new(DefaultLogicalExtensionCodec {}),
        None,
        task_ctx_provider,
    );

    (ctx, codec)
}
