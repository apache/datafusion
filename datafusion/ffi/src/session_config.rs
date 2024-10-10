use std::ffi::{c_void, CString};

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

pub struct SessionConfigPrivateData {
    pub config: SessionConfig,
    pub last_error: Option<CString>,
}

pub struct ExportedSessionConfig {
    session: *mut FFI_SessionConfig,
}

impl ExportedSessionConfig {
    fn get_private_data(&mut self) -> &mut SessionConfigPrivateData {
        unsafe { &mut *((*self.session).private_data as *mut SessionConfigPrivateData) }
    }
}

impl FFI_SessionConfig {
    /// Creates a new [`FFI_TableProvider`].
    pub fn new(session: &dyn Session) -> Self {
        let config = session.config().clone();
        let private_data = Box::new(SessionConfigPrivateData {
            config,
            last_error: None,
        });

        Self {
            version: 2,
            private_data: Box::into_raw(private_data) as *mut c_void,
        }
    }
}
