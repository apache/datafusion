// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! FFI-safe Option and Result types that do not require `IStable` bounds.
//!
//! stabby's `Option<T>` and `Result<T, E>` require `T: IStable` for niche
//! optimization. Many of our FFI structs contain self-referential function
//! pointers and cannot implement `IStable`. These simple `#[repr(C)]` types
//! provide the same FFI-safe semantics without that constraint.

use stabby::string::String as SString;

/// An FFI-safe option type.
#[repr(C, u8)]
#[derive(Debug, Clone)]
pub enum FFI_Option<T> {
    Some(T),
    None,
}

impl<T> From<Option<T>> for FFI_Option<T> {
    fn from(opt: Option<T>) -> Self {
        match opt {
            Some(v) => FFI_Option::Some(v),
            None => FFI_Option::None,
        }
    }
}

impl<T> From<FFI_Option<T>> for Option<T> {
    fn from(opt: FFI_Option<T>) -> Self {
        match opt {
            FFI_Option::Some(v) => Some(v),
            FFI_Option::None => None,
        }
    }
}

impl<T> FFI_Option<T> {
    pub fn as_ref(&self) -> Option<&T> {
        match self {
            FFI_Option::Some(v) => Some(v),
            FFI_Option::None => None,
        }
    }

    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> FFI_Option<U> {
        match self {
            FFI_Option::Some(v) => FFI_Option::Some(f(v)),
            FFI_Option::None => FFI_Option::None,
        }
    }

    pub fn into_option(self) -> Option<T> {
        self.into()
    }
}

/// An FFI-safe result type with SString as the error type.
#[repr(C, u8)]
#[derive(Debug, Clone)]
pub enum FFI_Result<T> {
    Ok(T),
    Err(SString),
}

impl<T> FFI_Result<T> {
    pub fn is_ok(&self) -> bool {
        matches!(self, FFI_Result::Ok(_))
    }

    pub fn is_err(&self) -> bool {
        matches!(self, FFI_Result::Err(_))
    }

    pub fn unwrap_err(self) -> SString {
        match self {
            FFI_Result::Err(e) => e,
            FFI_Result::Ok(_) => panic!("called unwrap_err on Ok"),
        }
    }

    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> FFI_Result<U> {
        match self {
            FFI_Result::Ok(v) => FFI_Result::Ok(f(v)),
            FFI_Result::Err(e) => FFI_Result::Err(e),
        }
    }

    pub fn into_result(self) -> Result<T, SString> {
        self.into()
    }
}

impl<T> From<FFI_Result<T>> for Result<T, SString> {
    fn from(res: FFI_Result<T>) -> Self {
        match res {
            FFI_Result::Ok(v) => Ok(v),
            FFI_Result::Err(e) => Err(e),
        }
    }
}

impl<T, E: ToString> From<Result<T, E>> for FFI_Result<T> {
    fn from(res: Result<T, E>) -> Self {
        match res {
            Ok(v) => FFI_Result::Ok(v),
            Err(e) => FFI_Result::Err(SString::from(e.to_string().as_str())),
        }
    }
}

impl<T: PartialEq> PartialEq for FFI_Result<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (FFI_Result::Ok(a), FFI_Result::Ok(b)) => a == b,
            (FFI_Result::Err(a), FFI_Result::Err(b)) => a == b,
            _ => false,
        }
    }
}
