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