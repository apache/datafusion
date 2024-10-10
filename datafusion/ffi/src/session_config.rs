use std::ffi::c_void;

use datafusion::{catalog::Session, prelude::SessionConfig};

#[repr(C)]
#[derive(Debug)]
#[allow(missing_docs)]
#[allow(non_camel_case_types)]
pub struct FFI_SessionConfig {
    pub private_data: *mut c_void,
    pub release: Option<unsafe extern "C" fn(arg: *mut Self)>,
}

unsafe impl Send for FFI_SessionConfig {}

unsafe extern "C" fn release_fn_wrapper(config: *mut FFI_SessionConfig) {
    if config.is_null() {
        return;
    }
    let config = &mut *config;

    let private_data = Box::from_raw(config.private_data as *mut SessionConfigPrivateData);
    drop(private_data);

    config.release = None;
}


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
    /// Creates a new [`FFI_SessionConfig`].
    pub fn new(session: &dyn Session) -> Self {
        let config = session.config().clone();
        let private_data = Box::new(SessionConfigPrivateData {
            config,
        });

        Self {
            private_data: Box::into_raw(private_data) as *mut c_void,
            release: Some(release_fn_wrapper),
        }
    }
}

impl Drop for FFI_SessionConfig {
    fn drop(&mut self) {
        match self.release {
            None => (),
            Some(release) => unsafe { release(self) },
        };
    }
}