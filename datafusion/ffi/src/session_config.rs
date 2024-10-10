use std::ffi::c_void;

use datafusion::{catalog::Session, prelude::SessionConfig};

#[repr(C)]
#[derive(Debug)]
#[allow(missing_docs)]
#[allow(non_camel_case_types)]
pub struct FFI_SessionConfig {
    pub version: i64,

    pub private_data: *mut c_void,
}

unsafe impl Send for FFI_SessionConfig {}

struct SessionConfigPrivateData {
    pub config: SessionConfig,
}

pub struct ExportedSessionConfig(pub *const FFI_SessionConfig);

impl ExportedSessionConfig {
    fn get_private_data(&self) -> &SessionConfigPrivateData {
        unsafe { &*((*self.0).private_data as *const SessionConfigPrivateData) }
    }

    pub fn session_config(&self) -> &SessionConfig {
        &self.get_private_data().config
    }
}

impl FFI_SessionConfig {
    /// Creates a new [`FFI_TableProvider`].
    pub fn new(session: &dyn Session) -> Self {
        let config = session.config().clone();
        let private_data = Box::new(SessionConfigPrivateData {
            config,
        });

        Self {
            version: 2,
            private_data: Box::into_raw(private_data) as *mut c_void,
        }
    }
}
