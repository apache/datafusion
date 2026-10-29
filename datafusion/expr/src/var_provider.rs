// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Variable provider

use arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};

/// Variable type, system/user defined
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum VarType {
    /// System variable, like @@version
    System,
    /// User defined variable, like @name
    UserDefined,
}

/// A var provider for `@variable` and `@@variable` runtime values.
pub trait VarProvider: std::fmt::Debug {
    /// Get variable value
    fn get_value(&self, var_names: Vec<String>) -> Result<ScalarValue>;

    /// Return the type of the given variable
    fn get_type(&self, var_names: &[String]) -> Option<DataType>;
}

/// Returns true if the specified string is a "system" variable such as
/// `@@version`
///
/// See [`SessionContext::register_variable`] for more details
///
/// [`SessionContext::register_variable`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_variable
pub fn is_system_variables(variable_names: &[String]) -> bool {
    !variable_names.is_empty() && variable_names[0].get(0..2) == Some("@@")
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_is_system_variables() {
        assert!(!is_system_variables(&["N".into(), "\"\"".into()]));
        assert!(is_system_variables(&["@@D".into(), "F".into(), "J".into()]));
        assert!(!is_system_variables(&[
            "J".into(),
            "@@F".into(),
            "K".into()
        ]));
        assert!(!is_system_variables(&[]));
        assert!(is_system_variables(&[
            "@@longvariablenamethatIdontknowhwyanyonewoulduse".into()
        ]));
        assert!(!is_system_variables(&["@F".into()]));
    }
}
