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

use arrow::datatypes::DataType;
use datafusion_common::{DFSchema, DFSchemaRef};
use std::fmt::{self, Display};
use std::sync::{Arc, LazyLock};

use crate::{expr_vec_fmt, Expr, LogicalPlan};

/// Various types of Statements.
///
/// # Transactions:
///
/// While DataFusion does not offer support transactions, it provides
/// [`LogicalPlan`] support to assist building database systems
/// using DataFusion
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum Statement {
    // Begin a transaction
    TransactionStart(TransactionStart),
    // Commit or rollback a transaction
    TransactionEnd(TransactionEnd),
    /// Set a Variable
    SetVariable(SetVariable),
    /// Prepare a statement and find any bind parameters
    /// (e.g. `?`). This is used to implement SQL-prepared statements.
    Prepare(Prepare),
    /// Execute a prepared statement. This is used to implement SQL 'EXECUTE'.
    Execute(Execute),
    /// Deallocate a prepared statement.
    /// This is used to implement SQL 'DEALLOCATE'.
    Deallocate(Deallocate),
}

impl Statement {
    /// Get a reference to the logical plan's schema
    pub fn schema(&self) -> &DFSchemaRef {
        // Statements have an unchanging empty schema.
        static STATEMENT_EMPTY_SCHEMA: LazyLock<DFSchemaRef> =
            LazyLock::new(|| Arc::new(DFSchema::empty()));

        &STATEMENT_EMPTY_SCHEMA
    }

    /// Return a descriptive string describing the type of this
    /// [`Statement`]
    pub fn name(&self) -> &str {
        match self {
            Statement::TransactionStart(_) => "TransactionStart",
            Statement::TransactionEnd(_) => "TransactionEnd",
            Statement::SetVariable(_) => "SetVariable",
            Statement::Prepare(_) => "Prepare",
            Statement::Execute(_) => "Execute",
            Statement::Deallocate(_) => "Deallocate",
        }
    }

    /// Returns input LogicalPlans in the current `Statement`.
    pub(super) fn inputs(&self) -> Vec<&LogicalPlan> {
        match self {
            Statement::Prepare(Prepare { input, .. }) => vec![input.as_ref()],
            _ => vec![],
        }
    }

    /// Return a `format`able structure with the a human readable
    /// description of this LogicalPlan node per node, not including
    /// children.
    ///
    /// See [crate::LogicalPlan::display] for an example
    pub fn display(&self) -> impl Display + '_ {
        struct Wrapper<'a>(&'a Statement);
        impl Display for Wrapper<'_> {
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
                    Statement::Prepare(Prepare {
                        name, data_types, ..
                    }) => {
                        write!(f, "Prepare: {name:?} {data_types:?} ")
                    }
                    Statement::Execute(Execute {
                        name, parameters, ..
                    }) => {
                        write!(
                            f,
                            "Execute: {} params=[{}]",
                            name,
                            expr_vec_fmt!(parameters)
                        )
                    }
                    Statement::Deallocate(Deallocate { name }) => {
                        write!(f, "Deallocate: {}", name)
                    }
                }
            }
        }
        Wrapper(self)
    }
}

/// Indicates if a transaction was committed or aborted
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub enum TransactionConclusion {
    Commit,
    Rollback,
}

/// Indicates if this transaction is allowed to write
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub enum TransactionAccessMode {
    ReadOnly,
    ReadWrite,
}

/// Indicates ANSI transaction isolation level
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub enum TransactionIsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
    Snapshot,
}

/// Indicator that the following statements should be committed or rolled back atomically
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash)]
pub struct TransactionStart {
    /// indicates if transaction is allowed to write
    pub access_mode: TransactionAccessMode,
    // indicates ANSI isolation level
    pub isolation_level: TransactionIsolationLevel,
}

/// Indicator that any current transaction should be terminated
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash)]
pub struct TransactionEnd {
    /// whether the transaction committed or aborted
    pub conclusion: TransactionConclusion,
    /// if specified a new transaction is immediately started with same characteristics
    pub chain: bool,
}

/// Set a Variable's value -- value in
/// [`ConfigOptions`](datafusion_common::config::ConfigOptions)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash)]
pub struct SetVariable {
    /// The variable name
    pub variable: String,
    /// The value to set
    pub value: String,
}

/// Prepare a statement but do not execute it. Prepare statements can have 0 or more
/// `Expr::Placeholder` expressions that are filled in during execution
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct Prepare {
    /// The name of the statement
    pub name: String,
    /// Data types of the parameters ([`Expr::Placeholder`])
    pub data_types: Vec<DataType>,
    /// The logical plan of the statements
    pub input: Arc<LogicalPlan>,
}

/// Execute a prepared statement.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash)]
pub struct Execute {
    /// The name of the prepared statement to execute
    pub name: String,
    /// The execute parameters
    pub parameters: Vec<Expr>,
}

/// Deallocate a prepared statement.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash)]
pub struct Deallocate {
    /// The name of the prepared statement to deallocate
    pub name: String,
}
