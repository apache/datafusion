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

use std::fmt::{self, Display};

use datafusion_common::DFSchemaRef;

/// Various types of Statements.
///
/// # Transactions:
///
/// While DataFusion does not offer support transactions, it provides
/// [`LogicalPlan`](crate::LogicalPlan) support to assist building
/// database systems using DataFusion
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum Statement {
    // Begin a transaction
    TransactionStart(TransactionStart),
    // Commit or rollback a transaction
    TransactionEnd(TransactionEnd),
    /// Set a Variable
    SetVariable(SetVariable),
}

impl Statement {
    /// Get a reference to the logical plan's schema
    pub fn schema(&self) -> &DFSchemaRef {
        match self {
            Statement::TransactionStart(TransactionStart { schema, .. }) => schema,
            Statement::TransactionEnd(TransactionEnd { schema, .. }) => schema,
            Statement::SetVariable(SetVariable { schema, .. }) => schema,
        }
    }

    /// Return a descriptive string describing the type of this
    /// [`Statement`]
    pub fn name(&self) -> &str {
        match self {
            Statement::TransactionStart(_) => "TransactionStart",
            Statement::TransactionEnd(_) => "TransactionEnd",
            Statement::SetVariable(_) => "SetVariable",
        }
    }

    /// Return a `format`able structure with the a human readable
    /// description of this LogicalPlan node per node, not including
    /// children.
    ///
    /// See [crate::LogicalPlan::display] for an example
    pub fn display(&self) -> impl fmt::Display + '_ {
        struct Wrapper<'a>(&'a Statement);
        impl<'a> Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                match self.0 {
                    Statement::TransactionStart(TransactionStart {
                        access_mode,
                        isolation_level,
                        ..
                    }) => {
                        write!(f, "TransactionStart: {access_mode:?} {isolation_level:?}")
                    }
                    Statement::TransactionEnd(TransactionEnd {
                        conclusion,
                        chain,
                        ..
                    }) => {
                        write!(f, "TransactionEnd: {conclusion:?} chain:={chain}")
                    }
                    Statement::SetVariable(SetVariable {
                        variable, value, ..
                    }) => {
                        write!(f, "SetVariable: set {variable:?} to {value:?}")
                    }
                }
            }
        }
        Wrapper(self)
    }
}

/// Indicates if a transaction was committed or aborted
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum TransactionConclusion {
    Commit,
    Rollback,
}

/// Indicates if this transaction is allowed to write
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum TransactionAccessMode {
    ReadOnly,
    ReadWrite,
}

/// Indicates ANSI transaction isolation level
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum TransactionIsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

/// Indicator that the following statements should be committed or rolled back atomically
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct TransactionStart {
    /// indicates if transaction is allowed to write
    pub access_mode: TransactionAccessMode,
    // indicates ANSI isolation level
    pub isolation_level: TransactionIsolationLevel,
    /// Empty schema
    pub schema: DFSchemaRef,
}

/// Indicator that any current transaction should be terminated
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct TransactionEnd {
    /// whether the transaction committed or aborted
    pub conclusion: TransactionConclusion,
    /// if specified a new transaction is immediately started with same characteristics
    pub chain: bool,
    /// Empty schema
    pub schema: DFSchemaRef,
}

/// Set a Variable's value -- value in
/// [`ConfigOptions`](datafusion_common::config::ConfigOptions)
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct SetVariable {
    /// The variable name
    pub variable: String,
    /// The value to set
    pub value: String,
    /// Dummy schema
    pub schema: DFSchemaRef,
}
