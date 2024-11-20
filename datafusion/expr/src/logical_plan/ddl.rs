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
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::{
    fmt::{self, Display},
    hash::{Hash, Hasher},
};

use crate::expr::Sort;
use arrow::datatypes::DataType;
use datafusion_common::tree_node::{Transformed, TreeNodeContainer, TreeNodeRecursion};
use datafusion_common::{
    schema_err, Column, Constraints, DFSchema, DFSchemaRef, Result,
    SchemaError, SchemaReference, TableReference,
};
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
    pub fn display(&self) -> impl Display + '_ {
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
#[non_exhaustive]
pub struct CreateExternalTable {
    /// The table name
    pub name: TableReference,
    /// The table schema
    pub schema: DFSchemaRef,
    /// The physical location
    pub location: String,
    /// The file type of physical file
    pub file_type: String,
    /// Partition Columns
    pub table_partition_cols: Vec<String>,
    /// Option to not error if table already exists
    pub if_not_exists: bool,
    /// Whether the table is a temporary table
    pub temporary: bool,
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
        self.name.hash(state);
        self.schema.hash(state);
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

impl CreateExternalTable {
    pub fn new(fields: CreateExternalTableFields) -> Result<Self> {
        let CreateExternalTableFields {
            name,
            schema,
            location,
            file_type,
            table_partition_cols,
            if_not_exists,
            temporary,
            definition,
            order_exprs,
            unbounded,
            options,
            constraints,
            column_defaults,
        } = fields;
        check_fields_unique(&schema)?;
        Ok(Self {
            name,
            schema,
            location,
            file_type,
            table_partition_cols,
            if_not_exists,
            temporary,
            definition,
            order_exprs,
            unbounded,
            options,
            constraints,
            column_defaults,
        })
    }

    pub fn into_fields(self) -> CreateExternalTableFields {
        let Self {
            name,
            schema,
            location,
            file_type,
            table_partition_cols,
            if_not_exists,
            temporary,
            definition,
            order_exprs,
            unbounded,
            options,
            constraints,
            column_defaults,
        } = self;
        CreateExternalTableFields {
            name,
            schema,
            location,
            file_type,
            table_partition_cols,
            if_not_exists,
            temporary,
            definition,
            order_exprs,
            unbounded,
            options,
            constraints,
            column_defaults,
        }
    }

    pub fn builder() -> CreateExternalTableBuilder {
        CreateExternalTableBuilder::new()
    }
}

/// A struct with same fields as [`CreateExternalTable`] struct so that the DDL can be conveniently
/// destructed with validation that each field is handled, while still requiring that all
/// construction goes through the [`CreateExternalTable::new`] constructor or the builder.
pub struct CreateExternalTableFields {
    /// The table name
    pub name: TableReference,
    /// The table schema
    pub schema: DFSchemaRef,
    /// The physical location
    pub location: String,
    /// The file type of physical file
    pub file_type: String,
    /// Partition Columns
    pub table_partition_cols: Vec<String>,
    /// Option to not error if table already exists
    pub if_not_exists: bool,
    /// Whether the table is a temporary table
    pub temporary: bool,
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

/// A builder or [`CreateExternalTable`]. Use [`CreateExternalTable::builder`] to obtain a new builder instance.
pub struct CreateExternalTableBuilder {
    name: Option<TableReference>,
    schema: Option<DFSchemaRef>,
    location: Option<String>,
    file_type: Option<String>,
    table_partition_cols: Vec<String>,
    if_not_exists: bool,
    temporary: bool,
    definition: Option<String>,
    order_exprs: Vec<Vec<Sort>>,
    unbounded: bool,
    options: HashMap<String, String>,
    constraints: Constraints,
    column_defaults: HashMap<String, Expr>,
}

impl CreateExternalTableBuilder {
    fn new() -> Self {
        Self {
            name: None,
            schema: None,
            location: None,
            file_type: None,
            table_partition_cols: vec![],
            if_not_exists: false,
            temporary: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints: Constraints::empty(),
            column_defaults: HashMap::new(),
        }
    }

    pub fn name(mut self, name: TableReference) -> Self {
        self.name = Some(name);
        self
    }

    pub fn schema(mut self, schema: DFSchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn location(mut self, location: String) -> Self {
        self.location = Some(location);
        self
    }

    pub fn file_type(mut self, file_type: String) -> Self {
        self.file_type = Some(file_type);
        self
    }

    pub fn table_partition_cols(mut self, table_partition_cols: Vec<String>) -> Self {
        self.table_partition_cols = table_partition_cols;
        self
    }

    pub fn if_not_exists(mut self, if_not_exists: bool) -> Self {
        self.if_not_exists = if_not_exists;
        self
    }

    pub fn temporary(mut self, temporary: bool) -> Self {
        self.temporary = temporary;
        self
    }

    pub fn definition(mut self, definition: Option<String>) -> Self {
        self.definition = definition;
        self
    }

    pub fn order_exprs(mut self, order_exprs: Vec<Vec<Sort>>) -> Self {
        self.order_exprs = order_exprs;
        self
    }

    pub fn unbounded(mut self, unbounded: bool) -> Self {
        self.unbounded = unbounded;
        self
    }

    pub fn options(mut self, options: HashMap<String, String>) -> Self {
        self.options = options;
        self
    }

    pub fn constraints(mut self, constraints: Constraints) -> Self {
        self.constraints = constraints;
        self
    }

    pub fn column_defaults(mut self, column_defaults: HashMap<String, Expr>) -> Self {
        self.column_defaults = column_defaults;
        self
    }

    pub fn build(self) -> Result<CreateExternalTable> {
        CreateExternalTable::new(CreateExternalTableFields {
            name: self.name.expect("name is required"),
            schema: self.schema.expect("schema is required"),
            location: self.location.expect("location is required"),
            file_type: self.file_type.expect("file_type is required"),
            table_partition_cols: self.table_partition_cols,
            if_not_exists: self.if_not_exists,
            temporary: self.temporary,
            definition: self.definition,
            order_exprs: self.order_exprs,
            unbounded: self.unbounded,
            options: self.options,
            constraints: self.constraints,
            column_defaults: self.column_defaults,
        })
    }
}

/// Creates an in memory table.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
#[non_exhaustive]
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
    /// Whether the table is `TableType::Temporary`
    pub temporary: bool,
}

impl CreateMemoryTable {
    pub fn new(fields: CreateMemoryTableFields) -> Result<Self> {
        let CreateMemoryTableFields {
            name,
            constraints,
            input,
            if_not_exists,
            or_replace,
            column_defaults,
            temporary,
        } = fields;
        check_fields_unique(input.schema())?;
        Ok(Self {
            name,
            constraints,
            input,
            if_not_exists,
            or_replace,
            column_defaults,
            temporary,
        })
    }

    pub fn into_fields(self) -> CreateMemoryTableFields {
        let Self {
            name,
            constraints,
            input,
            if_not_exists,
            or_replace,
            column_defaults,
            temporary,
        } = self;
        CreateMemoryTableFields {
            name,
            constraints,
            input,
            if_not_exists,
            or_replace,
            column_defaults,
            temporary,
        }
    }

    pub fn builder() -> CreateMemoryTableBuilder {
        CreateMemoryTableBuilder::new()
    }
}

/// A struct with same fields as [`CreateMemoryTable`] struct so that the DDL can be conveniently
/// destructed with validation that each field is handled, while still requiring that all
/// construction goes through the [`CreateMemoryTable::new`] constructor or the builder.
pub struct CreateMemoryTableFields {
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
    /// Whether the table is `TableType::Temporary`
    pub temporary: bool,
}

/// A builder or [`CreateMemoryTable`]. Use [`CreateMemoryTable::builder`] to obtain a new builder instance.
pub struct CreateMemoryTableBuilder {
    name: Option<TableReference>,
    constraints: Constraints,
    input: Option<Arc<LogicalPlan>>,
    if_not_exists: bool,
    or_replace: bool,
    column_defaults: Vec<(String, Expr)>,
    temporary: bool,
}

impl CreateMemoryTableBuilder {
    fn new() -> Self {
        Self {
            name: None,
            constraints: Constraints::empty(),
            input: None,
            if_not_exists: false,
            or_replace: false,
            column_defaults: vec![],
            temporary: false,
        }
    }

    pub fn name(mut self, name: TableReference) -> Self {
        self.name = Some(name);
        self
    }

    pub fn constraints(mut self, constraints: Constraints) -> Self {
        self.constraints = constraints;
        self
    }

    pub fn input(mut self, input: Arc<LogicalPlan>) -> Self {
        self.input = Some(input);
        self
    }

    pub fn if_not_exists(mut self, if_not_exists: bool) -> Self {
        self.if_not_exists = if_not_exists;
        self
    }

    pub fn or_replace(mut self, or_replace: bool) -> Self {
        self.or_replace = or_replace;
        self
    }

    pub fn column_defaults(mut self, column_defaults: Vec<(String, Expr)>) -> Self {
        self.column_defaults = column_defaults;
        self
    }

    pub fn temporary(mut self, temporary: bool) -> Self {
        self.temporary = temporary;
        self
    }

    pub fn build(self) -> Result<CreateMemoryTable> {
        CreateMemoryTable::new(CreateMemoryTableFields {
            name: self.name.expect("name is required"),
            constraints: self.constraints,
            input: self.input.expect("input is required"),
            if_not_exists: self.if_not_exists,
            or_replace: self.or_replace,
            column_defaults: self.column_defaults,
            temporary: self.temporary,
        })
    }
}

/// Creates a view.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash)]
#[non_exhaustive]
pub struct CreateView {
    /// The table name
    pub name: TableReference,
    /// The logical plan
    pub input: Arc<LogicalPlan>,
    /// Option to not error if table already exists
    pub or_replace: bool,
    /// SQL used to create the view, if available
    pub definition: Option<String>,
    /// Whether the view is ephemeral
    pub temporary: bool,
}

impl CreateView {
    pub fn new(fields: CreateViewFields) -> Result<Self> {
        let CreateViewFields {
            name,
            input,
            or_replace,
            definition,
            temporary,
        } = fields;
        check_fields_unique(input.schema())?;
        Ok(Self {
            name,
            input,
            or_replace,
            definition,
            temporary,
        })
    }

    pub fn into_fields(self) -> CreateViewFields {
        let Self {
            name,
            input,
            or_replace,
            definition,
            temporary,
        } = self;
        CreateViewFields {
            name,
            input,
            or_replace,
            definition,
            temporary,
        }
    }

    pub fn builder() -> CreateViewBuilder {
        CreateViewBuilder::new()
    }
}

/// A struct with same fields as [`CreateView`] struct so that the DDL can be conveniently
/// destructed with validation that each field is handled, while still requiring that all
/// construction goes through the [`CreateView::new`] constructor or the builder.
pub struct CreateViewFields {
    /// The table name
    pub name: TableReference,
    /// The logical plan
    pub input: Arc<LogicalPlan>,
    /// Option to not error if table already exists
    pub or_replace: bool,
    /// SQL used to create the view, if available
    pub definition: Option<String>,
    /// Whether the view is ephemeral
    pub temporary: bool,
}

/// A builder or [`CreateView`]. Use [`CreateView::builder`] to obtain a new builder instance.
pub struct CreateViewBuilder {
    name: Option<TableReference>,
    input: Option<Arc<LogicalPlan>>,
    or_replace: bool,
    definition: Option<String>,
    temporary: bool,
}

impl CreateViewBuilder {
    fn new() -> Self {
        Self {
            name: None,
            input: None,
            or_replace: false,
            definition: None,
            temporary: false,
        }
    }

    pub fn name(mut self, name: TableReference) -> Self {
        self.name = Some(name);
        self
    }

    pub fn input(mut self, input: Arc<LogicalPlan>) -> Self {
        self.input = Some(input);
        self
    }

    pub fn or_replace(mut self, or_replace: bool) -> Self {
        self.or_replace = or_replace;
        self
    }

    pub fn definition(mut self, definition: Option<String>) -> Self {
        self.definition = definition;
        self
    }

    pub fn temporary(mut self, temporary: bool) -> Self {
        self.temporary = temporary;
        self
    }

    pub fn build(self) -> Result<CreateView> {
        CreateView::new(CreateViewFields {
            name: self.name.expect("name is required"),
            input: self.input.expect("input is required"),
            or_replace: self.or_replace,
            definition: self.definition,
            temporary: self.temporary,
        })
    }
}
fn check_fields_unique(schema: &DFSchema) -> Result<()> {
    // Use tree set for deterministic error messages
    let mut qualified_names = BTreeSet::new();
    let mut unqualified_names = HashSet::new();
    let mut name_occurrences: HashMap<&String, usize> = HashMap::new();

    for (qualifier, field) in schema.iter() {
        if let Some(qualifier) = qualifier {
            // Check for duplicate qualified field names
            if !qualified_names.insert((qualifier, field.name())) {
                return schema_err!(SchemaError::DuplicateQualifiedField {
                    qualifier: Box::new(qualifier.clone()),
                    name: field.name().to_string(),
                });
            }
        // Check for duplicate unqualified field names
        } else if !unqualified_names.insert(field.name()) {
            return schema_err!(SchemaError::DuplicateUnqualifiedField {
                name: field.name().to_string()
            });
        }
        *name_occurrences.entry(field.name()).or_default() += 1;
    }

    for (qualifier, name) in qualified_names {
        // Check for duplicate between qualified and unqualified field names
        if unqualified_names.contains(name) {
            return schema_err!(SchemaError::AmbiguousReference {
                field: Column::new(Some(qualifier.clone()), name)
            });
        }
        // Check for duplicates between qualified names as the qualification will be stripped off
        if name_occurrences[name] > 1 {
            return schema_err!(SchemaError::QualifiedFieldWithDuplicateName {
                qualifier: Box::new(qualifier.clone()),
                name: name.to_owned(),
            });
        }
    }

    Ok(())
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

impl<'a> TreeNodeContainer<'a, Expr> for OperateFunctionArg {
    fn apply_elements<F: FnMut(&'a Expr) -> Result<TreeNodeRecursion>>(
        &'a self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        self.default_expr.apply_elements(f)
    }

    fn map_elements<F: FnMut(Expr) -> Result<Transformed<Expr>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        self.default_expr.map_elements(f)?.map_data(|default_expr| {
            Ok(Self {
                default_expr,
                ..self
            })
        })
    }
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

impl<'a> TreeNodeContainer<'a, Expr> for CreateFunctionBody {
    fn apply_elements<F: FnMut(&'a Expr) -> Result<TreeNodeRecursion>>(
        &'a self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        self.function_body.apply_elements(f)
    }

    fn map_elements<F: FnMut(Expr) -> Result<Transformed<Expr>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        self.function_body
            .map_elements(f)?
            .map_data(|function_body| {
                Ok(Self {
                    function_body,
                    ..self
                })
            })
    }
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
    use super::*;
    use crate::{CreateCatalog, DdlStatement, DropView};
    use arrow::datatypes::{DataType, Field, Schema};
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

    #[test]
    fn test_check_fields_unique() -> Result<()> {
        // no duplicate fields, unqualified schema
        check_fields_unique(&DFSchema::try_from(Schema::new(vec![
            Field::new("c100", DataType::Boolean, true),
            Field::new("c101", DataType::Boolean, true),
        ]))?)?;

        // no duplicate fields, qualified schema
        check_fields_unique(&DFSchema::try_from_qualified_schema(
            "t1",
            &Schema::new(vec![
                Field::new("c100", DataType::Boolean, true),
                Field::new("c101", DataType::Boolean, true),
            ]),
        )?)?;

        // duplicate unqualified field name
        assert_eq!(
            check_fields_unique(&DFSchema::try_from(Schema::new(vec![
                Field::new("c0", DataType::Boolean, true),
                Field::new("c1", DataType::Boolean, true),
                Field::new("c1", DataType::Boolean, true),
                Field::new("c2", DataType::Boolean, true),
            ]))?)
            .unwrap_err()
            .strip_backtrace()
            .to_string(),
            "Schema error: Schema contains duplicate unqualified field name c1"
        );

        // duplicate qualified field with same qualifier
        assert_eq!(
            DFSchema::try_from_qualified_schema(
                "t1",
                &Schema::new(vec![
                    Field::new("c1", DataType::Boolean, true),
                    Field::new("c1", DataType::Boolean, true),
                ])
            )
            // if schema construction succeeds (due to future changes in DFSchema), call check_fields_unique on it
            .unwrap_err()
            .strip_backtrace()
            .to_string(),
            "Schema error: Schema contains duplicate qualified field name t1.c1"
        );

        // duplicate qualified and unqualified field
        assert_eq!(
            DFSchema::from_field_specific_qualified_schema(
                vec![
                    None,
                    Some(TableReference::from("t1")),
                ],
                &Arc::new(Schema::new(vec![
                    Field::new("c1", DataType::Boolean, true),
                    Field::new("c1", DataType::Boolean, true),
                ]))
            )
                 // if schema construction succeeds (due to future changes in DFSchema), call check_fields_unique on it
                .unwrap_err().strip_backtrace().to_string(),
            "Schema error: Schema contains qualified field name t1.c1 and unqualified field name c1 which would be ambiguous"
        );

        // qualified fields with duplicate unqualified names
        assert_eq!(
            check_fields_unique(&DFSchema::from_field_specific_qualified_schema(
                vec![
                    Some(TableReference::from("t1")),
                    Some(TableReference::from("t2")),
                ],
                &Arc::new(Schema::new(vec![
                    Field::new("c1", DataType::Boolean, true),
                    Field::new("c1", DataType::Boolean, true),
                ]))
            )?)
                .unwrap_err().strip_backtrace().to_string(),
             "Schema error: Schema contains qualified fields with duplicate unqualified names t1.c1"
        );

        Ok(())
    }
}
