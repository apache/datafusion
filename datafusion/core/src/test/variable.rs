// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! System variable provider

use crate::error::Result;
use crate::scalar::ScalarValue;
use crate::variable::VarProvider;
use arrow::datatypes::DataType;

/// System variable
#[derive(Default, Debug)]
pub struct SystemVar {}

impl SystemVar {
    /// new system variable
    pub fn new() -> Self {
        Self {}
    }
}

impl VarProvider for SystemVar {
    /// get system variable value
    fn get_value(&self, var_names: Vec<String>) -> Result<ScalarValue> {
        let s = format!("{}-{}", "system-var", var_names.concat());
        Ok(ScalarValue::from(s))
    }

    fn get_type(&self, _: &[String]) -> Option<DataType> {
        Some(DataType::Utf8)
    }
}

/// user defined variable
#[derive(Default, Debug)]
pub struct UserDefinedVar {}

impl UserDefinedVar {
    /// new user defined variable
    pub fn new() -> Self {
        Self {}
    }
}

impl VarProvider for UserDefinedVar {
    /// Get user defined variable value
    fn get_value(&self, var_names: Vec<String>) -> Result<ScalarValue> {
        if var_names[0] != "@integer" {
            let s = format!("{}-{}", "user-defined-var", var_names.concat());
            Ok(ScalarValue::from(s))
        } else {
            Ok(ScalarValue::Int32(Some(41)))
        }
    }

    fn get_type(&self, var_names: &[String]) -> Option<DataType> {
        if var_names[0] != "@integer" {
            Some(DataType::Utf8)
        } else {
            Some(DataType::Int32)
        }
    }
}
