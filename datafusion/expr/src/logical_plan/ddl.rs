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

use crate::{Expr, LogicalPlan, SortExpr, Volatility};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;
use std::{
    fmt::{self, Display},
    hash::{Hash, Hasher},
};

use crate::expr::Sort;
use arrow::datatypes::DataType;
use datafusion_common::{Constraints, DFSchemaRef, SchemaReference, TableReference};
use sqlparser::ast::Ident;

/// Various types of DDL  (CREATE / DROP) catalog manipulation
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum DdlStatement {
    /// Creates an external table.
    CreateExternalTable(CreateExternalTable),
    /// Creates an in memory table.
    CreateMemoryTable(CreateMemoryTable),
    /// Creates a new view.
    CreateView(CreateView),
    /// Creates a new catalog schema.
    CreateCatalogSchema(CreateCatalogSchema),
    /// Creates a new catalog (aka "Database").
    CreateCatalog(CreateCatalog),
    /// Creates a new index.
    CreateIndex(CreateIndex),
    /// Drops a table.
    DropTable(DropTable),
    /// Drops a view.
    DropView(DropView),
    /// Drops a catalog schema
    DropCatalogSchema(DropCatalogSchema),
    /// Create function statement
    CreateFunction(CreateFunction),
    /// Drop function statement
    DropFunction(DropFunction),
}

impl DdlStatement {
    /// Get a reference to the logical plan's schema
    pub fn schema(&self) -> &DFSchemaRef {
        match self {
            DdlStatement::CreateExternalTable(CreateExternalTable { schema, .. }) => {
                schema
            }
            DdlStatement::CreateMemoryTable(CreateMemoryTable { input, .. })
            | DdlStatement::CreateView(CreateView { input, .. }) => input.schema(),
            DdlStatement::CreateCatalogSchema(CreateCatalogSchema { schema, .. }) => {
                schema
            }
            DdlStatement::CreateCatalog(CreateCatalog { schema, .. }) => schema,
            DdlStatement::CreateIndex(CreateIndex { schema, .. }) => schema,
            DdlStatement::DropTable(DropTable { schema, .. }) => schema,
            DdlStatement::DropView(DropView { schema, .. }) => schema,
            DdlStatement::DropCatalogSchema(DropCatalogSchema { schema, .. }) => schema,
            DdlStatement::CreateFunction(CreateFunction { schema, .. }) => schema,
            DdlStatement::DropFunction(DropFunction { schema, .. }) => schema,
        }
    }

    /// Return a descriptive string describing the type of this
    /// [`DdlStatement`]
    pub fn name(&self) -> &str {
        match self {
            DdlStatement::CreateExternalTable(_) => "CreateExternalTable",
            DdlStatement::CreateMemoryTable(_) => "CreateMemoryTable",
            DdlStatement::CreateView(_) => "CreateView",
            DdlStatement::CreateCatalogSchema(_) => "CreateCatalogSchema",
            DdlStatement::CreateCatalog(_) => "CreateCatalog",
            DdlStatement::CreateIndex(_) => "CreateIndex",
            DdlStatement::DropTable(_) => "DropTable",
            DdlStatement::DropView(_) => "DropView",
            DdlStatement::DropCatalogSchema(_) => "DropCatalogSchema",
            DdlStatement::CreateFunction(_) => "CreateFunction",
            DdlStatement::DropFunction(_) => "DropFunction",
        }
    }

    /// Return all inputs for this plan
    pub fn inputs(&self) -> Vec<&LogicalPlan> {
        match self {
            DdlStatement::CreateExternalTable(_) => vec![],
            DdlStatement::CreateCatalogSchema(_) => vec![],
            DdlStatement::CreateCatalog(_) => vec![],
            DdlStatement::CreateMemoryTable(CreateMemoryTable { input, .. }) => {
                vec![input]
            }
            DdlStatement::CreateView(CreateView { input, .. }) => vec![input],
            DdlStatement::CreateIndex(_) => vec![],
            DdlStatement::DropTable(_) => vec![],
            DdlStatement::DropView(_) => vec![],
            DdlStatement::DropCatalogSchema(_) => vec![],
            DdlStatement::CreateFunction(_) => vec![],
            DdlStatement::DropFunction(_) => vec![],
        }
    }

    /// Return a `format`able structure with the a human readable
    /// description of this LogicalPlan node per node, not including
    /// children.
    ///
    /// See [crate::LogicalPlan::display] for an example
    pub fn display(&self) -> impl fmt::Display + '_ {
        struct Wrapper<'a>(&'a DdlStatement);
        impl<'a> Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                match self.0 {
                    DdlStatement::CreateExternalTable(CreateExternalTable {
                        ref name,
                        constraints,
                        ..
                    }) => {
                        write!(f, "CreateExternalTable: {name:?}{constraints}")
                    }
                    DdlStatement::CreateMemoryTable(CreateMemoryTable {
                        name,
                        constraints,
                        ..
                    }) => {
                        write!(f, "CreateMemoryTable: {name:?}{constraints}")
                    }
                    DdlStatement::CreateView(CreateView { name, .. }) => {
                        write!(f, "CreateView: {name:?}")
                    }
                    DdlStatement::CreateCatalogSchema(CreateCatalogSchema {
                        schema_name,
                        ..
                    }) => {
                        write!(f, "CreateCatalogSchema: {schema_name:?}")
                    }
                    DdlStatement::CreateCatalog(CreateCatalog {
                        catalog_name, ..
                    }) => {
                        write!(f, "CreateCatalog: {catalog_name:?}")
                    }
                    DdlStatement::CreateIndex(CreateIndex { name, .. }) => {
                        write!(f, "CreateIndex: {name:?}")
                    }
                    DdlStatement::DropTable(DropTable {
                        name, if_exists, ..
                    }) => {
                        write!(f, "DropTable: {name:?} if not exist:={if_exists}")
                    }
                    DdlStatement::DropView(DropView {
                        name, if_exists, ..
                    }) => {
                        write!(f, "DropView: {name:?} if not exist:={if_exists}")
                    }
                    DdlStatement::DropCatalogSchema(DropCatalogSchema {
                        name,
                        if_exists,
                        cascade,
                        ..
                    }) => {
                        write!(f, "DropCatalogSchema: {name:?} if not exist:={if_exists} cascade:={cascade}")
                    }
                    DdlStatement::CreateFunction(CreateFunction { name, .. }) => {
                        write!(f, "CreateFunction: name {name:?}")
                    }
                    DdlStatement::DropFunction(DropFunction { name, .. }) => {
                        write!(f, "CreateFunction: name {name:?}")
                    }
                }
            }
        }
        Wrapper(self)
    }
}

/// Creates an external table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateExternalTable {
    /// The table schema
    pub schema: DFSchemaRef,
    /// The table name
    pub name: TableReference,
    /// The physical location
    pub location: String,
    /// The file type of physical file
    pub file_type: String,
    /// Partition Columns
    pub table_partition_cols: Vec<String>,
    /// Option to not error if table already exists
    pub if_not_exists: bool,
    /// SQL used to create the table, if available
    pub definition: Option<String>,
    /// Order expressions supplied by user
    pub order_exprs: Vec<Vec<Sort>>,
    /// Whether the table is an infinite streams
    pub unbounded: bool,
    /// Table(provider) specific options
    pub options: HashMap<String, String>,
    /// The list of constraints in the schema, such as primary key, unique, etc.
    pub constraints: Constraints,
    /// Default values for columns
    pub column_defaults: HashMap<String, Expr>,
}

// Hashing refers to a subset of fields considered in PartialEq.
impl Hash for CreateExternalTable {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.schema.hash(state);
        self.name.hash(state);
        self.location.hash(state);
        self.file_type.hash(state);
        self.table_partition_cols.hash(state);
        self.if_not_exists.hash(state);
        self.definition.hash(state);
        self.order_exprs.hash(state);
        self.unbounded.hash(state);
        self.options.len().hash(state); // HashMap is not hashable
    }
}

// Manual implementation needed because of `schema`, `options`, and `column_defaults` fields.
// Comparison excludes these fields.
impl PartialOrd for CreateExternalTable {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        #[derive(PartialEq, PartialOrd)]
        struct ComparableCreateExternalTable<'a> {
            /// The table name
            pub name: &'a TableReference,
            /// The physical location
            pub location: &'a String,
            /// The file type of physical file
            pub file_type: &'a String,
            /// Partition Columns
            pub table_partition_cols: &'a Vec<String>,
            /// Option to not error if table already exists
            pub if_not_exists: &'a bool,
            /// SQL used to create the table, if available
            pub definition: &'a Option<String>,
            /// Order expressions supplied by user
            pub order_exprs: &'a Vec<Vec<Sort>>,
            /// Whether the table is an infinite streams
            pub unbounded: &'a bool,
            /// The list of constraints in the schema, such as primary key, unique, etc.
            pub constraints: &'a Constraints,
        }
        let comparable_self = ComparableCreateExternalTable {
            name: &self.name,
            location: &self.location,
            file_type: &self.file_type,
            table_partition_cols: &self.table_partition_cols,
            if_not_exists: &self.if_not_exists,
            definition: &self.definition,
            order_exprs: &self.order_exprs,
            unbounded: &self.unbounded,
            constraints: &self.constraints,
        };
        let comparable_other = ComparableCreateExternalTable {
            name: &other.name,
            location: &other.location,
            file_type: &other.file_type,
            table_partition_cols: &other.table_partition_cols,
            if_not_exists: &other.if_not_exists,
            definition: &other.definition,
            order_exprs: &other.order_exprs,
            unbounded: &other.unbounded,
            constraints: &other.constraints,
        };
        comparable_self.partial_cmp(&comparable_other)
    }
}

/// Creates an in memory table.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct CreateMemoryTable {
    /// The table name
    pub name: TableReference,
    /// The list of constraints in the schema, such as primary key, unique, etc.
    pub constraints: Constraints,
    /// The logical plan
    pub input: Arc<LogicalPlan>,
    /// Option to not error if table already exists
    pub if_not_exists: bool,
    /// Option to replace table content if table already exists
    pub or_replace: bool,
    /// Default values for columns
    pub column_defaults: Vec<(String, Expr)>,
}

/// Creates a view.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash)]
pub struct CreateView {
    /// The table name
    pub name: TableReference,
    /// The logical plan
    pub input: Arc<LogicalPlan>,
    /// Option to not error if table already exists
    pub or_replace: bool,
    /// SQL used to create the view, if available
    pub definition: Option<String>,
}

/// Creates a catalog (aka "Database").
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateCatalog {
    /// The catalog name
    pub catalog_name: String,
    /// Do nothing (except issuing a notice) if a schema with the same name already exists
    pub if_not_exists: bool,
    /// Empty schema
    pub schema: DFSchemaRef,
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for CreateCatalog {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.catalog_name.partial_cmp(&other.catalog_name) {
            Some(Ordering::Equal) => self.if_not_exists.partial_cmp(&other.if_not_exists),
            cmp => cmp,
        }
    }
}

/// Creates a schema.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateCatalogSchema {
    /// The table schema
    pub schema_name: String,
    /// Do nothing (except issuing a notice) if a schema with the same name already exists
    pub if_not_exists: bool,
    /// Empty schema
    pub schema: DFSchemaRef,
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for CreateCatalogSchema {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.schema_name.partial_cmp(&other.schema_name) {
            Some(Ordering::Equal) => self.if_not_exists.partial_cmp(&other.if_not_exists),
            cmp => cmp,
        }
    }
}

/// Drops a table.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DropTable {
    /// The table name
    pub name: TableReference,
    /// If the table exists
    pub if_exists: bool,
    /// Dummy schema
    pub schema: DFSchemaRef,
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for DropTable {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.name.partial_cmp(&other.name) {
            Some(Ordering::Equal) => self.if_exists.partial_cmp(&other.if_exists),
            cmp => cmp,
        }
    }
}

/// Drops a view.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DropView {
    /// The view name
    pub name: TableReference,
    /// If the view exists
    pub if_exists: bool,
    /// Dummy schema
    pub schema: DFSchemaRef,
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for DropView {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.name.partial_cmp(&other.name) {
            Some(Ordering::Equal) => self.if_exists.partial_cmp(&other.if_exists),
            cmp => cmp,
        }
    }
}

/// Drops a schema
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DropCatalogSchema {
    /// The schema name
    pub name: SchemaReference,
    /// If the schema exists
    pub if_exists: bool,
    /// Whether drop should cascade
    pub cascade: bool,
    /// Dummy schema
    pub schema: DFSchemaRef,
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for DropCatalogSchema {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.name.partial_cmp(&other.name) {
            Some(Ordering::Equal) => match self.if_exists.partial_cmp(&other.if_exists) {
                Some(Ordering::Equal) => self.cascade.partial_cmp(&other.cascade),
                cmp => cmp,
            },
            cmp => cmp,
        }
    }
}

/// Arguments passed to `CREATE FUNCTION`
///
/// Note this meant to be the same as from sqlparser's [`sqlparser::ast::Statement::CreateFunction`]
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct CreateFunction {
    // TODO: There is open question should we expose sqlparser types or redefine them here?
    //       At the moment it make more sense to expose sqlparser types and leave
    //       user to convert them as needed
    pub or_replace: bool,
    pub temporary: bool,
    pub name: String,
    pub args: Option<Vec<OperateFunctionArg>>,
    pub return_type: Option<DataType>,
    pub params: CreateFunctionBody,
    /// Dummy schema
    pub schema: DFSchemaRef,
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for CreateFunction {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        #[derive(PartialEq, PartialOrd)]
        struct ComparableCreateFunction<'a> {
            pub or_replace: &'a bool,
            pub temporary: &'a bool,
            pub name: &'a String,
            pub args: &'a Option<Vec<OperateFunctionArg>>,
            pub return_type: &'a Option<DataType>,
            pub params: &'a CreateFunctionBody,
        }
        let comparable_self = ComparableCreateFunction {
            or_replace: &self.or_replace,
            temporary: &self.temporary,
            name: &self.name,
            args: &self.args,
            return_type: &self.return_type,
            params: &self.params,
        };
        let comparable_other = ComparableCreateFunction {
            or_replace: &other.or_replace,
            temporary: &other.temporary,
            name: &other.name,
            args: &other.args,
            return_type: &other.return_type,
            params: &other.params,
        };
        comparable_self.partial_cmp(&comparable_other)
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct OperateFunctionArg {
    // TODO: figure out how to support mode
    // pub mode: Option<ArgMode>,
    pub name: Option<Ident>,
    pub data_type: DataType,
    pub default_expr: Option<Expr>,
}
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct CreateFunctionBody {
    /// LANGUAGE lang_name
    pub language: Option<Ident>,
    /// IMMUTABLE | STABLE | VOLATILE
    pub behavior: Option<Volatility>,
    /// RETURN or AS function body
    pub function_body: Option<Expr>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct DropFunction {
    pub name: String,
    pub if_exists: bool,
    pub schema: DFSchemaRef,
}

impl PartialOrd for DropFunction {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.name.partial_cmp(&other.name) {
            Some(Ordering::Equal) => self.if_exists.partial_cmp(&other.if_exists),
            cmp => cmp,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct CreateIndex {
    pub name: Option<String>,
    pub table: TableReference,
    pub using: Option<String>,
    pub columns: Vec<SortExpr>,
    pub unique: bool,
    pub if_not_exists: bool,
    pub schema: DFSchemaRef,
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for CreateIndex {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        #[derive(PartialEq, PartialOrd)]
        struct ComparableCreateIndex<'a> {
            pub name: &'a Option<String>,
            pub table: &'a TableReference,
            pub using: &'a Option<String>,
            pub columns: &'a Vec<SortExpr>,
            pub unique: &'a bool,
            pub if_not_exists: &'a bool,
        }
        let comparable_self = ComparableCreateIndex {
            name: &self.name,
            table: &self.table,
            using: &self.using,
            columns: &self.columns,
            unique: &self.unique,
            if_not_exists: &self.if_not_exists,
        };
        let comparable_other = ComparableCreateIndex {
            name: &other.name,
            table: &other.table,
            using: &other.using,
            columns: &other.columns,
            unique: &other.unique,
            if_not_exists: &other.if_not_exists,
        };
        comparable_self.partial_cmp(&comparable_other)
    }
}

#[cfg(test)]
mod test {
    use crate::{CreateCatalog, DdlStatement, DropView};
    use datafusion_common::{DFSchema, DFSchemaRef, TableReference};
    use std::cmp::Ordering;

    #[test]
    fn test_partial_ord() {
        let catalog = DdlStatement::CreateCatalog(CreateCatalog {
            catalog_name: "name".to_string(),
            if_not_exists: false,
            schema: DFSchemaRef::new(DFSchema::empty()),
        });
        let catalog_2 = DdlStatement::CreateCatalog(CreateCatalog {
            catalog_name: "name".to_string(),
            if_not_exists: true,
            schema: DFSchemaRef::new(DFSchema::empty()),
        });

        assert_eq!(catalog.partial_cmp(&catalog_2), Some(Ordering::Less));

        let drop_view = DdlStatement::DropView(DropView {
            name: TableReference::from("table"),
            if_exists: false,
            schema: DFSchemaRef::new(DFSchema::empty()),
        });

        assert_eq!(drop_view.partial_cmp(&catalog), Some(Ordering::Greater));
    }
}
