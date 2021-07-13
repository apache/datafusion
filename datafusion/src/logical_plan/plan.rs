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
//! This module contains the  `LogicalPlan` enum that describes queries
//! via a logical query plan.

use super::display::{GraphvizVisitor, IndentVisitor};
use super::expr::{Column, Expr};
use super::extension::UserDefinedLogicalNode;
use crate::datasource::TableProvider;
use crate::error::DataFusionError;
use crate::logical_plan::dfschema::DFSchemaRef;
use crate::sql::parser::FileType;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::{
    collections::HashSet,
    fmt::{self, Display},
    sync::Arc,
};

/// Join type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// Inner Join
    Inner,
    /// Left Join
    Left,
    /// Right Join
    Right,
    /// Full Join
    Full,
    /// Semi Join
    Semi,
    /// Anti Join
    Anti,
}

/// Join constraint
#[derive(Debug, Clone, Copy)]
pub enum JoinConstraint {
    /// Join ON
    On,
    /// Join USING
    Using,
}

/// A LogicalPlan represents the different types of relational
/// operators (such as Projection, Filter, etc) and can be created by
/// the SQL query planner and the DataFrame API.
///
/// A LogicalPlan represents transforming an input relation (table) to
/// an output relation (table) with a (potentially) different
/// schema. A plan represents a dataflow tree where data flows
/// from leaves up to the root to produce the query result.
#[derive(Clone)]
pub enum LogicalPlan {
    /// Evaluates an arbitrary list of expressions (essentially a
    /// SELECT with an expression list) on its input.
    Projection {
        /// The list of expressions
        expr: Vec<Expr>,
        /// The incoming logical plan
        input: Arc<LogicalPlan>,
        /// The schema description of the output
        schema: DFSchemaRef,
    },
    /// Filters rows from its input that do not match an
    /// expression (essentially a WHERE clause with a predicate
    /// expression).
    ///
    /// Semantically, `<predicate>` is evaluated for each row of the input;
    /// If the value of `<predicate>` is true, the input row is passed to
    /// the output. If the value of `<predicate>` is false, the row is
    /// discarded.
    Filter {
        /// The predicate expression, which must have Boolean type.
        predicate: Expr,
        /// The incoming logical plan
        input: Arc<LogicalPlan>,
    },
    /// Window its input based on a set of window spec and window function (e.g. SUM or RANK)
    Window {
        /// The incoming logical plan
        input: Arc<LogicalPlan>,
        /// The window function expression
        window_expr: Vec<Expr>,
        /// The schema description of the window output
        schema: DFSchemaRef,
    },
    /// Aggregates its input based on a set of grouping and aggregate
    /// expressions (e.g. SUM).
    Aggregate {
        /// The incoming logical plan
        input: Arc<LogicalPlan>,
        /// Grouping expressions
        group_expr: Vec<Expr>,
        /// Aggregate expressions
        aggr_expr: Vec<Expr>,
        /// The schema description of the aggregate output
        schema: DFSchemaRef,
    },
    /// Sorts its input according to a list of sort expressions.
    Sort {
        /// The sort expressions
        expr: Vec<Expr>,
        /// The incoming logical plan
        input: Arc<LogicalPlan>,
    },
    /// Join two logical plans on one or more join columns
    Join {
        /// Left input
        left: Arc<LogicalPlan>,
        /// Right input
        right: Arc<LogicalPlan>,
        /// Equijoin clause expressed as pairs of (left, right) join columns
        on: Vec<(Column, Column)>,
        /// Join type
        join_type: JoinType,
        /// Join constraint
        join_constraint: JoinConstraint,
        /// The output schema, containing fields from the left and right inputs
        schema: DFSchemaRef,
    },
    /// Apply Cross Join to two logical plans
    CrossJoin {
        /// Left input
        left: Arc<LogicalPlan>,
        /// Right input
        right: Arc<LogicalPlan>,
        /// The output schema, containing fields from the left and right inputs
        schema: DFSchemaRef,
    },
    /// Repartition the plan based on a partitioning scheme.
    Repartition {
        /// The incoming logical plan
        input: Arc<LogicalPlan>,
        /// The partitioning scheme
        partitioning_scheme: Partitioning,
    },
    /// Union multiple inputs
    Union {
        /// Inputs to merge
        inputs: Vec<LogicalPlan>,
        /// Union schema. Should be the same for all inputs.
        schema: DFSchemaRef,
        /// Union output relation alias
        alias: Option<String>,
    },
    /// Produces rows from a table provider by reference or from the context
    TableScan {
        /// The name of the table
        table_name: String,
        /// The source of the table
        source: Arc<dyn TableProvider>,
        /// Optional column indices to use as a projection
        projection: Option<Vec<usize>>,
        /// The schema description of the output
        projected_schema: DFSchemaRef,
        /// Optional expressions to be used as filters by the table provider
        filters: Vec<Expr>,
        /// Optional limit to skip reading
        limit: Option<usize>,
    },
    /// Produces no rows: An empty relation with an empty schema
    EmptyRelation {
        /// Whether to produce a placeholder row
        produce_one_row: bool,
        /// The schema description of the output
        schema: DFSchemaRef,
    },
    /// Produces the first `n` tuples from its input and discards the rest.
    Limit {
        /// The limit
        n: usize,
        /// The logical plan
        input: Arc<LogicalPlan>,
    },
    /// Creates an external table.
    CreateExternalTable {
        /// The table schema
        schema: DFSchemaRef,
        /// The table name
        name: String,
        /// The physical location
        location: String,
        /// The file type of physical file
        file_type: FileType,
        /// Whether the CSV file contains a header
        has_header: bool,
    },
    /// Produces a relation with string representations of
    /// various parts of the plan
    Explain {
        /// Should extra (detailed, intermediate plans) be included?
        verbose: bool,
        /// The logical plan that is being EXPLAIN'd
        plan: Arc<LogicalPlan>,
        /// Represent the various stages plans have gone through
        stringified_plans: Vec<StringifiedPlan>,
        /// The output schema of the explain (2 columns of text)
        schema: DFSchemaRef,
    },
    /// Extension operator defined outside of DataFusion
    Extension {
        /// The runtime extension operator
        node: Arc<dyn UserDefinedLogicalNode + Send + Sync>,
    },
}

impl LogicalPlan {
    /// Get a reference to the logical plan's schema
    pub fn schema(&self) -> &DFSchemaRef {
        match self {
            LogicalPlan::EmptyRelation { schema, .. } => schema,
            LogicalPlan::TableScan {
                projected_schema, ..
            } => projected_schema,
            LogicalPlan::Projection { schema, .. } => schema,
            LogicalPlan::Filter { input, .. } => input.schema(),
            LogicalPlan::Window { schema, .. } => schema,
            LogicalPlan::Aggregate { schema, .. } => schema,
            LogicalPlan::Sort { input, .. } => input.schema(),
            LogicalPlan::Join { schema, .. } => schema,
            LogicalPlan::CrossJoin { schema, .. } => schema,
            LogicalPlan::Repartition { input, .. } => input.schema(),
            LogicalPlan::Limit { input, .. } => input.schema(),
            LogicalPlan::CreateExternalTable { schema, .. } => schema,
            LogicalPlan::Explain { schema, .. } => schema,
            LogicalPlan::Extension { node } => node.schema(),
            LogicalPlan::Union { schema, .. } => schema,
        }
    }

    /// Get a vector of references to all schemas in every node of the logical plan
    pub fn all_schemas(&self) -> Vec<&DFSchemaRef> {
        match self {
            LogicalPlan::TableScan {
                projected_schema, ..
            } => vec![projected_schema],
            LogicalPlan::Window { input, schema, .. }
            | LogicalPlan::Aggregate { input, schema, .. }
            | LogicalPlan::Projection { input, schema, .. } => {
                let mut schemas = input.all_schemas();
                schemas.insert(0, schema);
                schemas
            }
            LogicalPlan::Join {
                left,
                right,
                schema,
                ..
            }
            | LogicalPlan::CrossJoin {
                left,
                right,
                schema,
            } => {
                let mut schemas = left.all_schemas();
                schemas.extend(right.all_schemas());
                schemas.insert(0, schema);
                schemas
            }
            LogicalPlan::Union { schema, .. } => {
                vec![schema]
            }
            LogicalPlan::Extension { node } => vec![node.schema()],
            LogicalPlan::Explain { schema, .. }
            | LogicalPlan::EmptyRelation { schema, .. }
            | LogicalPlan::CreateExternalTable { schema, .. } => vec![schema],
            LogicalPlan::Limit { input, .. }
            | LogicalPlan::Repartition { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Filter { input, .. } => input.all_schemas(),
        }
    }

    /// Returns the (fixed) output schema for explain plans
    pub fn explain_schema() -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("plan_type", DataType::Utf8, false),
            Field::new("plan", DataType::Utf8, false),
        ]))
    }

    /// returns all expressions (non-recursively) in the current
    /// logical plan node. This does not include expressions in any
    /// children
    pub fn expressions(self: &LogicalPlan) -> Vec<Expr> {
        match self {
            LogicalPlan::Projection { expr, .. } => expr.clone(),
            LogicalPlan::Filter { predicate, .. } => vec![predicate.clone()],
            LogicalPlan::Repartition {
                partitioning_scheme,
                ..
            } => match partitioning_scheme {
                Partitioning::Hash(expr, _) => expr.clone(),
                _ => vec![],
            },
            LogicalPlan::Window { window_expr, .. } => window_expr.clone(),
            LogicalPlan::Aggregate {
                group_expr,
                aggr_expr,
                ..
            } => group_expr.iter().chain(aggr_expr.iter()).cloned().collect(),
            LogicalPlan::Join { on, .. } => on
                .iter()
                .flat_map(|(l, r)| vec![Expr::Column(l.clone()), Expr::Column(r.clone())])
                .collect(),
            LogicalPlan::Sort { expr, .. } => expr.clone(),
            LogicalPlan::Extension { node } => node.expressions(),
            // plans without expressions
            LogicalPlan::TableScan { .. }
            | LogicalPlan::EmptyRelation { .. }
            | LogicalPlan::Limit { .. }
            | LogicalPlan::CreateExternalTable { .. }
            | LogicalPlan::CrossJoin { .. }
            | LogicalPlan::Explain { .. }
            | LogicalPlan::Union { .. } => {
                vec![]
            }
        }
    }

    /// returns all inputs of this `LogicalPlan` node. Does not
    /// include inputs to inputs.
    pub fn inputs(self: &LogicalPlan) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::Projection { input, .. } => vec![input],
            LogicalPlan::Filter { input, .. } => vec![input],
            LogicalPlan::Repartition { input, .. } => vec![input],
            LogicalPlan::Window { input, .. } => vec![input],
            LogicalPlan::Aggregate { input, .. } => vec![input],
            LogicalPlan::Sort { input, .. } => vec![input],
            LogicalPlan::Join { left, right, .. } => vec![left, right],
            LogicalPlan::CrossJoin { left, right, .. } => vec![left, right],
            LogicalPlan::Limit { input, .. } => vec![input],
            LogicalPlan::Extension { node } => node.inputs(),
            LogicalPlan::Union { inputs, .. } => inputs.iter().collect(),
            LogicalPlan::Explain { plan, .. } => vec![plan],
            // plans without inputs
            LogicalPlan::TableScan { .. }
            | LogicalPlan::EmptyRelation { .. }
            | LogicalPlan::CreateExternalTable { .. } => vec![],
        }
    }

    /// returns all `Using` join columns in a logical plan
    pub fn using_columns(&self) -> Result<Vec<HashSet<Column>>, DataFusionError> {
        struct UsingJoinColumnVisitor {
            using_columns: Vec<HashSet<Column>>,
        }

        impl PlanVisitor for UsingJoinColumnVisitor {
            type Error = DataFusionError;

            fn pre_visit(&mut self, plan: &LogicalPlan) -> Result<bool, Self::Error> {
                if let LogicalPlan::Join {
                    join_constraint: JoinConstraint::Using,
                    on,
                    ..
                } = plan
                {
                    self.using_columns.push(
                        on.iter()
                            .map(|entry| [&entry.0, &entry.1])
                            .flatten()
                            .cloned()
                            .collect::<HashSet<Column>>(),
                    );
                }
                Ok(true)
            }
        }

        let mut visitor = UsingJoinColumnVisitor {
            using_columns: vec![],
        };
        self.accept(&mut visitor)?;
        Ok(visitor.using_columns)
    }
}

/// Logical partitioning schemes supported by the repartition operator.
#[derive(Debug, Clone)]
pub enum Partitioning {
    /// Allocate batches using a round-robin algorithm and the specified number of partitions
    RoundRobinBatch(usize),
    /// Allocate rows based on a hash of one of more expressions and the specified number
    /// of partitions.
    /// This partitioning scheme is not yet fully supported. See <https://issues.apache.org/jira/browse/ARROW-11011>
    Hash(Vec<Expr>, usize),
}

/// Trait that implements the [Visitor
/// pattern](https://en.wikipedia.org/wiki/Visitor_pattern) for a
/// depth first walk of `LogicalPlan` nodes. `pre_visit` is called
/// before any children are visited, and then `post_visit` is called
/// after all children have been visited.
////
/// To use, define a struct that implements this trait and then invoke
/// [`LogicalPlan::accept`].
///
/// For example, for a logical plan like:
///
/// ```text
/// Projection: #id
///    Filter: #state Eq Utf8(\"CO\")\
///       CsvScan: employee.csv projection=Some([0, 3])";
/// ```
///
/// The sequence of visit operations would be:
/// ```text
/// visitor.pre_visit(Projection)
/// visitor.pre_visit(Filter)
/// visitor.pre_visit(CsvScan)
/// visitor.post_visit(CsvScan)
/// visitor.post_visit(Filter)
/// visitor.post_visit(Projection)
/// ```
pub trait PlanVisitor {
    /// The type of error returned by this visitor
    type Error;

    /// Invoked on a logical plan before any of its child inputs have been
    /// visited. If Ok(true) is returned, the recursion continues. If
    /// Err(..) or Ok(false) are returned, the recursion stops
    /// immediately and the error, if any, is returned to `accept`
    fn pre_visit(&mut self, plan: &LogicalPlan)
        -> std::result::Result<bool, Self::Error>;

    /// Invoked on a logical plan after all of its child inputs have
    /// been visited. The return value is handled the same as the
    /// return value of `pre_visit`. The provided default implementation
    /// returns `Ok(true)`.
    fn post_visit(
        &mut self,
        _plan: &LogicalPlan,
    ) -> std::result::Result<bool, Self::Error> {
        Ok(true)
    }
}

impl LogicalPlan {
    /// returns all inputs in the logical plan. Returns Ok(true) if
    /// all nodes were visited, and Ok(false) if any call to
    /// `pre_visit` or `post_visit` returned Ok(false) and may have
    /// cut short the recursion
    pub fn accept<V>(&self, visitor: &mut V) -> std::result::Result<bool, V::Error>
    where
        V: PlanVisitor,
    {
        if !visitor.pre_visit(self)? {
            return Ok(false);
        }

        let recurse = match self {
            LogicalPlan::Projection { input, .. } => input.accept(visitor)?,
            LogicalPlan::Filter { input, .. } => input.accept(visitor)?,
            LogicalPlan::Repartition { input, .. } => input.accept(visitor)?,
            LogicalPlan::Window { input, .. } => input.accept(visitor)?,
            LogicalPlan::Aggregate { input, .. } => input.accept(visitor)?,
            LogicalPlan::Sort { input, .. } => input.accept(visitor)?,
            LogicalPlan::Join { left, right, .. }
            | LogicalPlan::CrossJoin { left, right, .. } => {
                left.accept(visitor)? && right.accept(visitor)?
            }
            LogicalPlan::Union { inputs, .. } => {
                for input in inputs {
                    if !input.accept(visitor)? {
                        return Ok(false);
                    }
                }
                true
            }
            LogicalPlan::Limit { input, .. } => input.accept(visitor)?,
            LogicalPlan::Extension { node } => {
                for input in node.inputs() {
                    if !input.accept(visitor)? {
                        return Ok(false);
                    }
                }
                true
            }
            LogicalPlan::Explain { plan, .. } => plan.accept(visitor)?,
            // plans without inputs
            LogicalPlan::TableScan { .. }
            | LogicalPlan::EmptyRelation { .. }
            | LogicalPlan::CreateExternalTable { .. } => true,
        };
        if !recurse {
            return Ok(false);
        }

        if !visitor.post_visit(self)? {
            return Ok(false);
        }

        Ok(true)
    }
}

// Various implementations for printing out LogicalPlans
impl LogicalPlan {
    /// Return a `format`able structure that produces a single line
    /// per node. For example:
    ///
    /// ```text
    /// Projection: #employee.id
    ///    Filter: #employee.state Eq Utf8(\"CO\")\
    ///       CsvScan: employee projection=Some([0, 3])
    /// ```
    ///
    /// ```
    /// use arrow::datatypes::{Field, Schema, DataType};
    /// use datafusion::logical_plan::{lit, col, LogicalPlanBuilder};
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    /// ]);
    /// let plan = LogicalPlanBuilder::scan_empty(Some("foo_csv"), &schema, None).unwrap()
    ///     .filter(col("id").eq(lit(5))).unwrap()
    ///     .build().unwrap();
    ///
    /// // Format using display_indent
    /// let display_string = format!("{}", plan.display_indent());
    ///
    /// assert_eq!("Filter: #foo_csv.id Eq Int32(5)\
    ///              \n  TableScan: foo_csv projection=None",
    ///             display_string);
    /// ```
    pub fn display_indent(&self) -> impl fmt::Display + '_ {
        // Boilerplate structure to wrap LogicalPlan with something
        // that that can be formatted
        struct Wrapper<'a>(&'a LogicalPlan);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let with_schema = false;
                let mut visitor = IndentVisitor::new(f, with_schema);
                self.0.accept(&mut visitor).unwrap();
                Ok(())
            }
        }
        Wrapper(self)
    }

    /// Return a `format`able structure that produces a single line
    /// per node that includes the output schema. For example:
    ///
    /// ```text
    /// Projection: #employee.id [id:Int32]\
    ///    Filter: #employee.state Eq Utf8(\"CO\") [id:Int32, state:Utf8]\
    ///      TableScan: employee projection=Some([0, 3]) [id:Int32, state:Utf8]";
    /// ```
    ///
    /// ```
    /// use arrow::datatypes::{Field, Schema, DataType};
    /// use datafusion::logical_plan::{lit, col, LogicalPlanBuilder};
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    /// ]);
    /// let plan = LogicalPlanBuilder::scan_empty(Some("foo_csv"), &schema, None).unwrap()
    ///     .filter(col("id").eq(lit(5))).unwrap()
    ///     .build().unwrap();
    ///
    /// // Format using display_indent_schema
    /// let display_string = format!("{}", plan.display_indent_schema());
    ///
    /// assert_eq!("Filter: #foo_csv.id Eq Int32(5) [id:Int32]\
    ///             \n  TableScan: foo_csv projection=None [id:Int32]",
    ///             display_string);
    /// ```
    pub fn display_indent_schema(&self) -> impl fmt::Display + '_ {
        // Boilerplate structure to wrap LogicalPlan with something
        // that that can be formatted
        struct Wrapper<'a>(&'a LogicalPlan);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let with_schema = true;
                let mut visitor = IndentVisitor::new(f, with_schema);
                self.0.accept(&mut visitor).unwrap();
                Ok(())
            }
        }
        Wrapper(self)
    }

    /// Return a `format`able structure that produces lines meant for
    /// graphical display using the `DOT` language. This format can be
    /// visualized using software from
    /// [`graphviz`](https://graphviz.org/)
    ///
    /// This currently produces two graphs -- one with the basic
    /// structure, and one with additional details such as schema.
    ///
    /// ```
    /// use arrow::datatypes::{Field, Schema, DataType};
    /// use datafusion::logical_plan::{lit, col, LogicalPlanBuilder};
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    /// ]);
    /// let plan = LogicalPlanBuilder::scan_empty(Some("foo.csv"), &schema, None).unwrap()
    ///     .filter(col("id").eq(lit(5))).unwrap()
    ///     .build().unwrap();
    ///
    /// // Format using display_graphviz
    /// let graphviz_string = format!("{}", plan.display_graphviz());
    /// ```
    ///
    /// If graphviz string is saved to a file such as `/tmp/example.dot`, the following
    /// commands can be used to render it as a pdf:
    ///
    /// ```bash
    ///   dot -Tpdf < /tmp/example.dot  > /tmp/example.pdf
    /// ```
    ///
    pub fn display_graphviz(&self) -> impl fmt::Display + '_ {
        // Boilerplate structure to wrap LogicalPlan with something
        // that that can be formatted
        struct Wrapper<'a>(&'a LogicalPlan);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                writeln!(
                    f,
                    "// Begin DataFusion GraphViz Plan (see https://graphviz.org)"
                )?;
                writeln!(f, "digraph {{")?;

                let mut visitor = GraphvizVisitor::new(f);

                visitor.pre_visit_plan("LogicalPlan")?;
                self.0.accept(&mut visitor).unwrap();
                visitor.post_visit_plan()?;

                visitor.set_with_schema(true);
                visitor.pre_visit_plan("Detailed LogicalPlan")?;
                self.0.accept(&mut visitor).unwrap();
                visitor.post_visit_plan()?;

                writeln!(f, "}}")?;
                writeln!(f, "// End DataFusion GraphViz Plan")?;
                Ok(())
            }
        }
        Wrapper(self)
    }

    /// Return a `format`able structure with the a human readable
    /// description of this LogicalPlan node per node, not including
    /// children. For example:
    ///
    /// ```text
    /// Projection: #id
    /// ```
    /// ```
    /// use arrow::datatypes::{Field, Schema, DataType};
    /// use datafusion::logical_plan::{lit, col, LogicalPlanBuilder};
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    /// ]);
    /// let plan = LogicalPlanBuilder::scan_empty(Some("foo.csv"), &schema, None).unwrap()
    ///     .build().unwrap();
    ///
    /// // Format using display
    /// let display_string = format!("{}", plan.display());
    ///
    /// assert_eq!("TableScan: foo.csv projection=None", display_string);
    /// ```
    pub fn display(&self) -> impl fmt::Display + '_ {
        // Boilerplate structure to wrap LogicalPlan with something
        // that that can be formatted
        struct Wrapper<'a>(&'a LogicalPlan);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                match &*self.0 {
                    LogicalPlan::EmptyRelation { .. } => write!(f, "EmptyRelation"),
                    LogicalPlan::TableScan {
                        ref table_name,
                        ref projection,
                        ref filters,
                        ref limit,
                        ..
                    } => {
                        write!(
                            f,
                            "TableScan: {} projection={:?}",
                            table_name, projection
                        )?;

                        if !filters.is_empty() {
                            write!(f, ", filters={:?}", filters)?;
                        }

                        if let Some(n) = limit {
                            write!(f, ", limit={}", n)?;
                        }

                        Ok(())
                    }
                    LogicalPlan::Projection { ref expr, .. } => {
                        write!(f, "Projection: ")?;
                        for (i, expr_item) in expr.iter().enumerate() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{:?}", expr_item)?;
                        }
                        Ok(())
                    }
                    LogicalPlan::Filter {
                        predicate: ref expr,
                        ..
                    } => write!(f, "Filter: {:?}", expr),
                    LogicalPlan::Window {
                        ref window_expr, ..
                    } => {
                        write!(f, "WindowAggr: windowExpr=[{:?}]", window_expr)
                    }
                    LogicalPlan::Aggregate {
                        ref group_expr,
                        ref aggr_expr,
                        ..
                    } => write!(
                        f,
                        "Aggregate: groupBy=[{:?}], aggr=[{:?}]",
                        group_expr, aggr_expr
                    ),
                    LogicalPlan::Sort { ref expr, .. } => {
                        write!(f, "Sort: ")?;
                        for (i, expr_item) in expr.iter().enumerate() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{:?}", expr_item)?;
                        }
                        Ok(())
                    }
                    LogicalPlan::Join {
                        on: ref keys,
                        join_constraint,
                        ..
                    } => {
                        let join_expr: Vec<String> =
                            keys.iter().map(|(l, r)| format!("{} = {}", l, r)).collect();
                        match join_constraint {
                            JoinConstraint::On => {
                                write!(f, "Join: {}", join_expr.join(", "))
                            }
                            JoinConstraint::Using => {
                                write!(f, "Join: Using {}", join_expr.join(", "))
                            }
                        }
                    }
                    LogicalPlan::CrossJoin { .. } => {
                        write!(f, "CrossJoin:")
                    }
                    LogicalPlan::Repartition {
                        partitioning_scheme,
                        ..
                    } => match partitioning_scheme {
                        Partitioning::RoundRobinBatch(n) => write!(
                            f,
                            "Repartition: RoundRobinBatch partition_count={}",
                            n
                        ),
                        Partitioning::Hash(expr, n) => {
                            let hash_expr: Vec<String> =
                                expr.iter().map(|e| format!("{:?}", e)).collect();
                            write!(
                                f,
                                "Repartition: Hash({}) partition_count={}",
                                hash_expr.join(", "),
                                n
                            )
                        }
                    },
                    LogicalPlan::Limit { ref n, .. } => write!(f, "Limit: {}", n),
                    LogicalPlan::CreateExternalTable { ref name, .. } => {
                        write!(f, "CreateExternalTable: {:?}", name)
                    }
                    LogicalPlan::Explain { .. } => write!(f, "Explain"),
                    LogicalPlan::Union { .. } => write!(f, "Union"),
                    LogicalPlan::Extension { ref node } => node.fmt_for_explain(f),
                }
            }
        }
        Wrapper(self)
    }
}

impl fmt::Debug for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.display_indent().fmt(f)
    }
}

/// Represents which type of plan
#[derive(Debug, Clone, PartialEq)]
pub enum PlanType {
    /// The initial LogicalPlan provided to DataFusion
    LogicalPlan,
    /// The LogicalPlan which results from applying an optimizer pass
    OptimizedLogicalPlan {
        /// The name of the optimizer which produced this plan
        optimizer_name: String,
    },
    /// The physical plan, prepared for execution
    PhysicalPlan,
}

impl From<&PlanType> for String {
    fn from(t: &PlanType) -> Self {
        match t {
            PlanType::LogicalPlan => "logical_plan".into(),
            PlanType::OptimizedLogicalPlan { optimizer_name } => {
                format!("logical_plan after {}", optimizer_name)
            }
            PlanType::PhysicalPlan => "physical_plan".into(),
        }
    }
}

/// Represents some sort of execution plan, in String form
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::rc_buffer)]
pub struct StringifiedPlan {
    /// An identifier of what type of plan this string represents
    pub plan_type: PlanType,
    /// The string representation of the plan
    pub plan: Arc<String>,
}

impl StringifiedPlan {
    /// Create a new Stringified plan of `plan_type` with string
    /// representation `plan`
    pub fn new(plan_type: PlanType, plan: impl Into<String>) -> Self {
        StringifiedPlan {
            plan_type,
            plan: Arc::new(plan.into()),
        }
    }

    /// returns true if this plan should be displayed. Generally
    /// `verbose_mode = true` will display all available plans
    pub fn should_display(&self, verbose_mode: bool) -> bool {
        self.plan_type == PlanType::LogicalPlan || verbose_mode
    }
}

#[cfg(test)]
mod tests {
    use super::super::{col, lit, LogicalPlanBuilder};
    use super::*;

    fn employee_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ])
    }

    fn display_plan() -> LogicalPlan {
        LogicalPlanBuilder::scan_empty(
            Some("employee_csv"),
            &employee_schema(),
            Some(vec![0, 3]),
        )
        .unwrap()
        .filter(col("state").eq(lit("CO")))
        .unwrap()
        .project(vec![col("id")])
        .unwrap()
        .build()
        .unwrap()
    }

    #[test]
    fn test_display_indent() {
        let plan = display_plan();

        let expected = "Projection: #employee_csv.id\
        \n  Filter: #employee_csv.state Eq Utf8(\"CO\")\
        \n    TableScan: employee_csv projection=Some([0, 3])";

        assert_eq!(expected, format!("{}", plan.display_indent()));
    }

    #[test]
    fn test_display_indent_schema() {
        let plan = display_plan();

        let expected = "Projection: #employee_csv.id [id:Int32]\
                        \n  Filter: #employee_csv.state Eq Utf8(\"CO\") [id:Int32, state:Utf8]\
                        \n    TableScan: employee_csv projection=Some([0, 3]) [id:Int32, state:Utf8]";

        assert_eq!(expected, format!("{}", plan.display_indent_schema()));
    }

    #[test]
    fn test_display_graphviz() {
        let plan = display_plan();

        // just test for a few key lines in the output rather than the
        // whole thing to make test mainteance easier.
        let graphviz = format!("{}", plan.display_graphviz());

        assert!(
            graphviz.contains(
                r#"// Begin DataFusion GraphViz Plan (see https://graphviz.org)"#
            ),
            "\n{}",
            plan.display_graphviz()
        );
        assert!(
            graphviz.contains(
                r#"[shape=box label="TableScan: employee_csv projection=Some([0, 3])"]"#
            ),
            "\n{}",
            plan.display_graphviz()
        );
        assert!(graphviz.contains(r#"[shape=box label="TableScan: employee_csv projection=Some([0, 3])\nSchema: [id:Int32, state:Utf8]"]"#),
                "\n{}", plan.display_graphviz());
        assert!(
            graphviz.contains(r#"// End DataFusion GraphViz Plan"#),
            "\n{}",
            plan.display_graphviz()
        );
    }

    /// Tests for the Visitor trait and walking logical plan nodes

    #[derive(Debug, Default)]
    struct OkVisitor {
        strings: Vec<String>,
    }
    impl PlanVisitor for OkVisitor {
        type Error = String;

        fn pre_visit(
            &mut self,
            plan: &LogicalPlan,
        ) -> std::result::Result<bool, Self::Error> {
            let s = match plan {
                LogicalPlan::Projection { .. } => "pre_visit Projection",
                LogicalPlan::Filter { .. } => "pre_visit Filter",
                LogicalPlan::TableScan { .. } => "pre_visit TableScan",
                _ => unimplemented!("unknown plan type"),
            };

            self.strings.push(s.into());
            Ok(true)
        }

        fn post_visit(
            &mut self,
            plan: &LogicalPlan,
        ) -> std::result::Result<bool, Self::Error> {
            let s = match plan {
                LogicalPlan::Projection { .. } => "post_visit Projection",
                LogicalPlan::Filter { .. } => "post_visit Filter",
                LogicalPlan::TableScan { .. } => "post_visit TableScan",
                _ => unimplemented!("unknown plan type"),
            };

            self.strings.push(s.into());
            Ok(true)
        }
    }

    #[test]
    fn visit_order() {
        let mut visitor = OkVisitor::default();
        let plan = test_plan();
        let res = plan.accept(&mut visitor);
        assert!(res.is_ok());

        assert_eq!(
            visitor.strings,
            vec![
                "pre_visit Projection",
                "pre_visit Filter",
                "pre_visit TableScan",
                "post_visit TableScan",
                "post_visit Filter",
                "post_visit Projection"
            ]
        );
    }

    #[derive(Debug, Default)]
    /// Counter than counts to zero and returns true when it gets there
    struct OptionalCounter {
        val: Option<usize>,
    }
    impl OptionalCounter {
        fn new(val: usize) -> Self {
            Self { val: Some(val) }
        }
        // Decrements the counter by 1, if any, returning true if it hits zero
        fn dec(&mut self) -> bool {
            if Some(0) == self.val {
                true
            } else {
                self.val = self.val.take().map(|i| i - 1);
                false
            }
        }
    }

    #[derive(Debug, Default)]
    /// Visitor that returns false after some number of visits
    struct StoppingVisitor {
        inner: OkVisitor,
        /// When Some(0) returns false from pre_visit
        return_false_from_pre_in: OptionalCounter,
        /// When Some(0) returns false from post_visit
        return_false_from_post_in: OptionalCounter,
    }

    impl PlanVisitor for StoppingVisitor {
        type Error = String;

        fn pre_visit(
            &mut self,
            plan: &LogicalPlan,
        ) -> std::result::Result<bool, Self::Error> {
            if self.return_false_from_pre_in.dec() {
                return Ok(false);
            }
            self.inner.pre_visit(plan)
        }

        fn post_visit(
            &mut self,
            plan: &LogicalPlan,
        ) -> std::result::Result<bool, Self::Error> {
            if self.return_false_from_post_in.dec() {
                return Ok(false);
            }

            self.inner.post_visit(plan)
        }
    }

    /// test early stopping in pre-visit
    #[test]
    fn early_stopping_pre_visit() {
        let mut visitor = StoppingVisitor {
            return_false_from_pre_in: OptionalCounter::new(2),
            ..Default::default()
        };
        let plan = test_plan();
        let res = plan.accept(&mut visitor);
        assert!(res.is_ok());

        assert_eq!(
            visitor.inner.strings,
            vec!["pre_visit Projection", "pre_visit Filter",]
        );
    }

    #[test]
    fn early_stopping_post_visit() {
        let mut visitor = StoppingVisitor {
            return_false_from_post_in: OptionalCounter::new(1),
            ..Default::default()
        };
        let plan = test_plan();
        let res = plan.accept(&mut visitor);
        assert!(res.is_ok());

        assert_eq!(
            visitor.inner.strings,
            vec![
                "pre_visit Projection",
                "pre_visit Filter",
                "pre_visit TableScan",
                "post_visit TableScan",
            ]
        );
    }

    #[derive(Debug, Default)]
    /// Visitor that returns an error after some number of visits
    struct ErrorVisitor {
        inner: OkVisitor,
        /// When Some(0) returns false from pre_visit
        return_error_from_pre_in: OptionalCounter,
        /// When Some(0) returns false from post_visit
        return_error_from_post_in: OptionalCounter,
    }

    impl PlanVisitor for ErrorVisitor {
        type Error = String;

        fn pre_visit(
            &mut self,
            plan: &LogicalPlan,
        ) -> std::result::Result<bool, Self::Error> {
            if self.return_error_from_pre_in.dec() {
                return Err("Error in pre_visit".into());
            }

            self.inner.pre_visit(plan)
        }

        fn post_visit(
            &mut self,
            plan: &LogicalPlan,
        ) -> std::result::Result<bool, Self::Error> {
            if self.return_error_from_post_in.dec() {
                return Err("Error in post_visit".into());
            }

            self.inner.post_visit(plan)
        }
    }

    #[test]
    fn error_pre_visit() {
        let mut visitor = ErrorVisitor {
            return_error_from_pre_in: OptionalCounter::new(2),
            ..Default::default()
        };
        let plan = test_plan();
        let res = plan.accept(&mut visitor);

        if let Err(e) = res {
            assert_eq!("Error in pre_visit", e);
        } else {
            panic!("Expected an error");
        }

        assert_eq!(
            visitor.inner.strings,
            vec!["pre_visit Projection", "pre_visit Filter",]
        );
    }

    #[test]
    fn error_post_visit() {
        let mut visitor = ErrorVisitor {
            return_error_from_post_in: OptionalCounter::new(1),
            ..Default::default()
        };
        let plan = test_plan();
        let res = plan.accept(&mut visitor);
        if let Err(e) = res {
            assert_eq!("Error in post_visit", e);
        } else {
            panic!("Expected an error");
        }

        assert_eq!(
            visitor.inner.strings,
            vec![
                "pre_visit Projection",
                "pre_visit Filter",
                "pre_visit TableScan",
                "post_visit TableScan",
            ]
        );
    }

    fn test_plan() -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("state", DataType::Utf8, false),
        ]);

        LogicalPlanBuilder::scan_empty(None, &schema, Some(vec![0, 1]))
            .unwrap()
            .filter(col("state").eq(lit("CO")))
            .unwrap()
            .project(vec![col("id")])
            .unwrap()
            .build()
            .unwrap()
    }
}
