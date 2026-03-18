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

//! FFI-safe Option and Result types that do not require `IStable` bounds.
//!
//! stabby's `Option<T>` and `Result<T, E>` require `T: IStable` for niche
//! optimization. Many of our FFI structs contain self-referential function
//! pointers and cannot implement `IStable`. These simple `#[repr(C)]` types
//! provide the same FFI-safe semantics without that constraint.

/// An FFI-safe option type.
#[repr(C, u8)]
#[derive(Debug, Clone)]
pub enum FfiOption<T> {
    Some(T),
    None,
}

impl<T> From<Option<T>> for FfiOption<T> {
    fn from(opt: Option<T>) -> Self {
        match opt {
            Some(v) => FfiOption::Some(v),
            None => FfiOption::None,
        }
    }
}

impl<T> From<FfiOption<T>> for Option<T> {
    fn from(opt: FfiOption<T>) -> Self {
        match opt {
            FfiOption::Some(v) => Some(v),
            FfiOption::None => None,
        }
    }
}

impl<T> FfiOption<T> {
    pub fn as_ref(&self) -> Option<&T> {
        match self {
            FfiOption::Some(v) => Some(v),
            FfiOption::None => None,
        }
    }

    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> FfiOption<U> {
        match self {
            FfiOption::Some(v) => FfiOption::Some(f(v)),
            FfiOption::None => FfiOption::None,
        }
    }

    pub fn into_option(self) -> Option<T> {
        self.into()
    }
}

/// An FFI-safe result type.
#[repr(C, u8)]
#[derive(Debug, Clone)]
pub enum FfiResult<T, E> {
    Ok(T),
    Err(E),
}

impl<T, E> From<Result<T, E>> for FfiResult<T, E> {
    fn from(res: Result<T, E>) -> Self {
        match res {
            Ok(v) => FfiResult::Ok(v),
            Err(e) => FfiResult::Err(e),
        }
    }
}

impl<T, E> From<FfiResult<T, E>> for Result<T, E> {
    fn from(res: FfiResult<T, E>) -> Self {
        match res {
            FfiResult::Ok(v) => Ok(v),
            FfiResult::Err(e) => Err(e),
        }
    }
}

impl<T, E> FfiResult<T, E> {
    pub fn is_ok(&self) -> bool {
        matches!(self, FfiResult::Ok(_))
    }

    pub fn is_err(&self) -> bool {
        matches!(self, FfiResult::Err(_))
    }

    pub fn unwrap_err(self) -> E {
        match self {
            FfiResult::Err(e) => e,
            FfiResult::Ok(_) => panic!("called unwrap_err on Ok"),
        }
    }

    pub fn into_result(self) -> Result<T, E> {
        self.into()
    }
}

impl<T: PartialEq, E: PartialEq> PartialEq for FfiResult<T, E> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (FfiResult::Ok(a), FfiResult::Ok(b)) => a == b,
            (FfiResult::Err(a), FfiResult::Err(b)) => a == b,
            _ => false,
        }
    }
}
