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

use crate::logical_plan::display::{GraphvizVisitor, IndentVisitor};
use crate::logical_plan::extension::UserDefinedLogicalNode;
use crate::utils::exprlist_to_fields;
use crate::{Expr, TableProviderFilterPushDown, TableSource};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::{plan_err, Column, DFSchema, DFSchemaRef, DataFusionError};
use std::collections::HashSet;
///! Logical plan types
use std::fmt::{self, Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

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
    Projection(Projection),
    /// Filters rows from its input that do not match an
    /// expression (essentially a WHERE clause with a predicate
    /// expression).
    ///
    /// Semantically, `<predicate>` is evaluated for each row of the input;
    /// If the value of `<predicate>` is true, the input row is passed to
    /// the output. If the value of `<predicate>` is false, the row is
    /// discarded.
    Filter(Filter),
    /// Window its input based on a set of window spec and window function (e.g. SUM or RANK)
    Window(Window),
    /// Aggregates its input based on a set of grouping and aggregate
    /// expressions (e.g. SUM).
    Aggregate(Aggregate),
    /// Sorts its input according to a list of sort expressions.
    Sort(Sort),
    /// Join two logical plans on one or more join columns
    Join(Join),
    /// Apply Cross Join to two logical plans
    CrossJoin(CrossJoin),
    /// Repartition the plan based on a partitioning scheme.
    Repartition(Repartition),
    /// Union multiple inputs
    Union(Union),
    /// Produces rows from a table provider by reference or from the context
    TableScan(TableScan),
    /// Produces no rows: An empty relation with an empty schema
    EmptyRelation(EmptyRelation),
    /// Subquery
    Subquery(Subquery),
    /// Aliased relation provides, or changes, the name of a relation.
    SubqueryAlias(SubqueryAlias),
    /// Skip some number of rows, and then fetch some number of rows.
    Limit(Limit),
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
    /// Drops a table.
    DropTable(DropTable),
    /// Values expression. See
    /// [Postgres VALUES](https://www.postgresql.org/docs/current/queries-values.html)
    /// documentation for more details.
    Values(Values),
    /// Produces a relation with string representations of
    /// various parts of the plan
    Explain(Explain),
    /// Runs the actual plan, and then prints the physical plan with
    /// with execution metrics.
    Analyze(Analyze),
    /// Extension operator defined outside of DataFusion
    Extension(Extension),
    /// Remove duplicate rows from the input
    Distinct(Distinct),
}

impl LogicalPlan {
    /// Get a reference to the logical plan's schema
    pub fn schema(&self) -> &DFSchemaRef {
        match self {
            LogicalPlan::EmptyRelation(EmptyRelation { schema, .. }) => schema,
            LogicalPlan::Values(Values { schema, .. }) => schema,
            LogicalPlan::TableScan(TableScan {
                projected_schema, ..
            }) => projected_schema,
            LogicalPlan::Projection(Projection { schema, .. }) => schema,
            LogicalPlan::Filter(Filter { input, .. }) => input.schema(),
            LogicalPlan::Distinct(Distinct { input }) => input.schema(),
            LogicalPlan::Window(Window { schema, .. }) => schema,
            LogicalPlan::Aggregate(Aggregate { schema, .. }) => schema,
            LogicalPlan::Sort(Sort { input, .. }) => input.schema(),
            LogicalPlan::Join(Join { schema, .. }) => schema,
            LogicalPlan::CrossJoin(CrossJoin { schema, .. }) => schema,
            LogicalPlan::Repartition(Repartition { input, .. }) => input.schema(),
            LogicalPlan::Limit(Limit { input, .. }) => input.schema(),
            LogicalPlan::Subquery(Subquery { subquery, .. }) => subquery.schema(),
            LogicalPlan::SubqueryAlias(SubqueryAlias { schema, .. }) => schema,
            LogicalPlan::CreateExternalTable(CreateExternalTable { schema, .. }) => {
                schema
            }
            LogicalPlan::Explain(explain) => &explain.schema,
            LogicalPlan::Analyze(analyze) => &analyze.schema,
            LogicalPlan::Extension(extension) => extension.node.schema(),
            LogicalPlan::Union(Union { schema, .. }) => schema,
            LogicalPlan::CreateMemoryTable(CreateMemoryTable { input, .. })
            | LogicalPlan::CreateView(CreateView { input, .. }) => input.schema(),
            LogicalPlan::CreateCatalogSchema(CreateCatalogSchema { schema, .. }) => {
                schema
            }
            LogicalPlan::CreateCatalog(CreateCatalog { schema, .. }) => schema,
            LogicalPlan::DropTable(DropTable { schema, .. }) => schema,
        }
    }

    /// Get a vector of references to all schemas in every node of the logical plan
    pub fn all_schemas(&self) -> Vec<&DFSchemaRef> {
        match self {
            LogicalPlan::TableScan(TableScan {
                projected_schema, ..
            }) => vec![projected_schema],
            LogicalPlan::Values(Values { schema, .. }) => vec![schema],
            LogicalPlan::Window(Window { input, schema, .. })
            | LogicalPlan::Projection(Projection { input, schema, .. })
            | LogicalPlan::Aggregate(Aggregate { input, schema, .. }) => {
                let mut schemas = input.all_schemas();
                schemas.insert(0, schema);
                schemas
            }
            LogicalPlan::Join(Join {
                left,
                right,
                schema,
                ..
            })
            | LogicalPlan::CrossJoin(CrossJoin {
                left,
                right,
                schema,
            }) => {
                let mut schemas = left.all_schemas();
                schemas.extend(right.all_schemas());
                schemas.insert(0, schema);
                schemas
            }
            LogicalPlan::Subquery(Subquery { subquery, .. }) => subquery.all_schemas(),
            LogicalPlan::SubqueryAlias(SubqueryAlias { schema, .. }) => {
                vec![schema]
            }
            LogicalPlan::Union(Union { schema, .. }) => {
                vec![schema]
            }
            LogicalPlan::Extension(extension) => vec![extension.node.schema()],
            LogicalPlan::Explain(Explain { schema, .. })
            | LogicalPlan::Analyze(Analyze { schema, .. })
            | LogicalPlan::EmptyRelation(EmptyRelation { schema, .. })
            | LogicalPlan::CreateExternalTable(CreateExternalTable { schema, .. })
            | LogicalPlan::CreateCatalogSchema(CreateCatalogSchema { schema, .. })
            | LogicalPlan::CreateCatalog(CreateCatalog { schema, .. }) => {
                vec![schema]
            }
            LogicalPlan::Limit(Limit { input, .. })
            | LogicalPlan::Repartition(Repartition { input, .. })
            | LogicalPlan::Sort(Sort { input, .. })
            | LogicalPlan::CreateMemoryTable(CreateMemoryTable { input, .. })
            | LogicalPlan::CreateView(CreateView { input, .. })
            | LogicalPlan::Filter(Filter { input, .. }) => input.all_schemas(),
            LogicalPlan::Distinct(Distinct { input, .. }) => input.all_schemas(),
            LogicalPlan::DropTable(_) => vec![],
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
            LogicalPlan::Projection(Projection { expr, .. }) => expr.clone(),
            LogicalPlan::Values(Values { values, .. }) => {
                values.iter().flatten().cloned().collect()
            }
            LogicalPlan::Filter(Filter { predicate, .. }) => vec![predicate.clone()],
            LogicalPlan::Repartition(Repartition {
                partitioning_scheme,
                ..
            }) => match partitioning_scheme {
                Partitioning::Hash(expr, _) => expr.clone(),
                _ => vec![],
            },
            LogicalPlan::Window(Window { window_expr, .. }) => window_expr.clone(),
            LogicalPlan::Aggregate(Aggregate {
                group_expr,
                aggr_expr,
                ..
            }) => group_expr.iter().chain(aggr_expr.iter()).cloned().collect(),
            LogicalPlan::Join(Join { on, filter, .. }) => on
                .iter()
                .flat_map(|(l, r)| vec![Expr::Column(l.clone()), Expr::Column(r.clone())])
                .chain(
                    filter
                        .as_ref()
                        .map(|expr| vec![expr.clone()])
                        .unwrap_or_default(),
                )
                .collect(),
            LogicalPlan::Sort(Sort { expr, .. }) => expr.clone(),
            LogicalPlan::Extension(extension) => extension.node.expressions(),
            // plans without expressions
            LogicalPlan::TableScan { .. }
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Subquery(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::CreateExternalTable(_)
            | LogicalPlan::CreateMemoryTable(_)
            | LogicalPlan::CreateView(_)
            | LogicalPlan::CreateCatalogSchema(_)
            | LogicalPlan::CreateCatalog(_)
            | LogicalPlan::DropTable(_)
            | LogicalPlan::CrossJoin(_)
            | LogicalPlan::Analyze { .. }
            | LogicalPlan::Explain { .. }
            | LogicalPlan::Union(_)
            | LogicalPlan::Distinct(_) => {
                vec![]
            }
        }
    }

    /// returns all inputs of this `LogicalPlan` node. Does not
    /// include inputs to inputs, or subqueries.
    pub fn inputs(self: &LogicalPlan) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::Projection(Projection { input, .. }) => vec![input],
            LogicalPlan::Filter(Filter { input, .. }) => vec![input],
            LogicalPlan::Repartition(Repartition { input, .. }) => vec![input],
            LogicalPlan::Window(Window { input, .. }) => vec![input],
            LogicalPlan::Aggregate(Aggregate { input, .. }) => vec![input],
            LogicalPlan::Sort(Sort { input, .. }) => vec![input],
            LogicalPlan::Join(Join { left, right, .. }) => vec![left, right],
            LogicalPlan::CrossJoin(CrossJoin { left, right, .. }) => vec![left, right],
            LogicalPlan::Limit(Limit { input, .. }) => vec![input],
            LogicalPlan::Subquery(Subquery { subquery, .. }) => vec![subquery],
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) => vec![input],
            LogicalPlan::Extension(extension) => extension.node.inputs(),
            LogicalPlan::Union(Union { inputs, .. }) => {
                inputs.iter().map(|arc| arc.as_ref()).collect()
            }
            LogicalPlan::Distinct(Distinct { input }) => vec![input],
            LogicalPlan::Explain(explain) => vec![&explain.plan],
            LogicalPlan::Analyze(analyze) => vec![&analyze.input],
            LogicalPlan::CreateMemoryTable(CreateMemoryTable { input, .. })
            | LogicalPlan::CreateView(CreateView { input, .. }) => {
                vec![input]
            }
            // plans without inputs
            LogicalPlan::TableScan { .. }
            | LogicalPlan::EmptyRelation { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::CreateExternalTable(_)
            | LogicalPlan::CreateCatalogSchema(_)
            | LogicalPlan::CreateCatalog(_)
            | LogicalPlan::DropTable(_) => vec![],
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
                if let LogicalPlan::Join(Join {
                    join_constraint: JoinConstraint::Using,
                    on,
                    ..
                }) = plan
                {
                    self.using_columns.push(
                        on.iter()
                            .flat_map(|entry| [&entry.0, &entry.1])
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
            LogicalPlan::Projection(Projection { .. }) => {
                self.visit_all_inputs(visitor)?
            }
            LogicalPlan::Filter(Filter { .. }) => self.visit_all_inputs(visitor)?,
            LogicalPlan::Repartition(Repartition { input, .. }) => {
                input.accept(visitor)?
            }
            LogicalPlan::Window(Window { input, .. }) => input.accept(visitor)?,
            LogicalPlan::Aggregate(Aggregate { input, .. }) => input.accept(visitor)?,
            LogicalPlan::Sort(Sort { input, .. }) => input.accept(visitor)?,
            LogicalPlan::Join(Join { left, right, .. })
            | LogicalPlan::CrossJoin(CrossJoin { left, right, .. }) => {
                left.accept(visitor)? && right.accept(visitor)?
            }
            LogicalPlan::Union(Union { inputs, .. }) => {
                for input in inputs {
                    if !input.accept(visitor)? {
                        return Ok(false);
                    }
                }
                true
            }
            LogicalPlan::Distinct(Distinct { input }) => input.accept(visitor)?,
            LogicalPlan::Limit(Limit { input, .. }) => input.accept(visitor)?,
            LogicalPlan::Subquery(Subquery { subquery, .. }) => {
                subquery.accept(visitor)?
            }
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) => {
                input.accept(visitor)?
            }
            LogicalPlan::CreateMemoryTable(CreateMemoryTable { input, .. })
            | LogicalPlan::CreateView(CreateView { input, .. }) => {
                input.accept(visitor)?
            }
            LogicalPlan::Extension(extension) => {
                for input in extension.node.inputs() {
                    if !input.accept(visitor)? {
                        return Ok(false);
                    }
                }
                true
            }
            LogicalPlan::Explain(explain) => explain.plan.accept(visitor)?,
            LogicalPlan::Analyze(analyze) => analyze.input.accept(visitor)?,
            // plans without inputs
            LogicalPlan::TableScan { .. }
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::CreateExternalTable(_)
            | LogicalPlan::CreateCatalogSchema(_)
            | LogicalPlan::CreateCatalog(_)
            | LogicalPlan::DropTable(_) => true,
        };
        if !recurse {
            return Ok(false);
        }

        if !visitor.post_visit(self)? {
            return Ok(false);
        }

        Ok(true)
    }

    /// Visit all inputs, including subqueries
    pub fn visit_all_inputs<V>(&self, visitor: &mut V) -> Result<bool, V::Error>
    where
        V: PlanVisitor,
    {
        for input in self.all_inputs() {
            if !input.accept(visitor)? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Get all plan inputs, including subqueries from expressions
    fn all_inputs(&self) -> Vec<Arc<LogicalPlan>> {
        let mut inputs = vec![];
        for expr in self.expressions() {
            self.collect_subqueries(&expr, &mut inputs);
        }
        for input in self.inputs() {
            inputs.push(Arc::new(input.clone()));
        }
        inputs
    }

    fn collect_subqueries(&self, expr: &Expr, sub: &mut Vec<Arc<LogicalPlan>>) {
        match expr {
            Expr::BinaryExpr { left, right, .. } => {
                self.collect_subqueries(left, sub);
                self.collect_subqueries(right, sub);
            }
            Expr::Exists { subquery, .. } => {
                sub.push(Arc::new(LogicalPlan::Subquery(subquery.clone())));
            }
            Expr::InSubquery { subquery, .. } => {
                sub.push(Arc::new(LogicalPlan::Subquery(subquery.clone())));
            }
            Expr::ScalarSubquery(subquery) => {
                sub.push(Arc::new(LogicalPlan::Subquery(subquery.clone())));
            }
            _ => {}
        }
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
    /// use datafusion_expr::{lit, col, LogicalPlanBuilder, logical_plan::table_scan};
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    /// ]);
    /// let plan = table_scan(Some("t1"), &schema, None).unwrap()
    ///     .filter(col("id").eq(lit(5))).unwrap()
    ///     .build().unwrap();
    ///
    /// // Format using display_indent
    /// let display_string = format!("{}", plan.display_indent());
    ///
    /// assert_eq!("Filter: #t1.id = Int32(5)\n  TableScan: t1",
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
    ///    Filter: #employee.state = Utf8(\"CO\") [id:Int32, state:Utf8]\
    ///      TableScan: employee projection=[0, 3] [id:Int32, state:Utf8]";
    /// ```
    ///
    /// ```
    /// use arrow::datatypes::{Field, Schema, DataType};
    /// use datafusion_expr::{lit, col, LogicalPlanBuilder, logical_plan::table_scan};
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    /// ]);
    /// let plan = table_scan(Some("t1"), &schema, None).unwrap()
    ///     .filter(col("id").eq(lit(5))).unwrap()
    ///     .build().unwrap();
    ///
    /// // Format using display_indent_schema
    /// let display_string = format!("{}", plan.display_indent_schema());
    ///
    /// assert_eq!("Filter: #t1.id = Int32(5) [id:Int32]\
    ///             \n  TableScan: t1 [id:Int32]",
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
    /// use datafusion_expr::{lit, col, LogicalPlanBuilder, logical_plan::table_scan};
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    /// ]);
    /// let plan = table_scan(Some("t1"), &schema, None).unwrap()
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
    /// use datafusion_expr::{lit, col, LogicalPlanBuilder, logical_plan::table_scan};
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    /// ]);
    /// let plan = table_scan(Some("t1"), &schema, None).unwrap()
    ///     .build().unwrap();
    ///
    /// // Format using display
    /// let display_string = format!("{}", plan.display());
    ///
    /// assert_eq!("TableScan: t1", display_string);
    /// ```
    pub fn display(&self) -> impl fmt::Display + '_ {
        // Boilerplate structure to wrap LogicalPlan with something
        // that that can be formatted
        struct Wrapper<'a>(&'a LogicalPlan);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                match self.0 {
                    LogicalPlan::EmptyRelation(_) => write!(f, "EmptyRelation"),
                    LogicalPlan::Values(Values { ref values, .. }) => {
                        let str_values: Vec<_> = values
                            .iter()
                            // limit to only 5 values to avoid horrible display
                            .take(5)
                            .map(|row| {
                                let item = row
                                    .iter()
                                    .map(|expr| expr.to_string())
                                    .collect::<Vec<_>>()
                                    .join(", ");
                                format!("({})", item)
                            })
                            .collect();

                        let elipse = if values.len() > 5 { "..." } else { "" };
                        write!(f, "Values: {}{}", str_values.join(", "), elipse)
                    }

                    LogicalPlan::TableScan(TableScan {
                        ref source,
                        ref table_name,
                        ref projection,
                        ref filters,
                        ref fetch,
                        ..
                    }) => {
                        let projected_fields = match projection {
                            Some(indices) => {
                                let schema = source.schema();
                                let names: Vec<&str> = indices
                                    .iter()
                                    .map(|i| schema.field(*i).name().as_str())
                                    .collect();
                                format!(" projection=[{}]", names.join(", "))
                            }
                            _ => "".to_string(),
                        };

                        write!(f, "TableScan: {}{}", table_name, projected_fields)?;

                        if !filters.is_empty() {
                            let mut full_filter = vec![];
                            let mut partial_filter = vec![];
                            let mut unsupported_filters = vec![];

                            filters.iter().for_each(|x| {
                                if let Ok(t) = source.supports_filter_pushdown(x) {
                                    match t {
                                        TableProviderFilterPushDown::Exact => {
                                            full_filter.push(x)
                                        }
                                        TableProviderFilterPushDown::Inexact => {
                                            partial_filter.push(x)
                                        }
                                        TableProviderFilterPushDown::Unsupported => {
                                            unsupported_filters.push(x)
                                        }
                                    }
                                }
                            });

                            if !full_filter.is_empty() {
                                write!(f, ", full_filters={:?}", full_filter)?;
                            };
                            if !partial_filter.is_empty() {
                                write!(f, ", partial_filters={:?}", partial_filter)?;
                            }
                            if !unsupported_filters.is_empty() {
                                write!(
                                    f,
                                    ", unsupported_filters={:?}",
                                    unsupported_filters
                                )?;
                            }
                        }

                        if let Some(n) = fetch {
                            write!(f, ", fetch={}", n)?;
                        }

                        Ok(())
                    }
                    LogicalPlan::Projection(Projection {
                        ref expr, alias, ..
                    }) => {
                        write!(f, "Projection: ")?;
                        for (i, expr_item) in expr.iter().enumerate() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{:?}", expr_item)?;
                        }
                        if let Some(a) = alias {
                            write!(f, ", alias={}", a)?;
                        }
                        Ok(())
                    }
                    LogicalPlan::Filter(Filter {
                        predicate: ref expr,
                        ..
                    }) => write!(f, "Filter: {:?}", expr),
                    LogicalPlan::Window(Window {
                        ref window_expr, ..
                    }) => {
                        write!(f, "WindowAggr: windowExpr=[{:?}]", window_expr)
                    }
                    LogicalPlan::Aggregate(Aggregate {
                        ref group_expr,
                        ref aggr_expr,
                        ..
                    }) => write!(
                        f,
                        "Aggregate: groupBy=[{:?}], aggr=[{:?}]",
                        group_expr, aggr_expr
                    ),
                    LogicalPlan::Sort(Sort { expr, .. }) => {
                        write!(f, "Sort: ")?;
                        for (i, expr_item) in expr.iter().enumerate() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{:?}", expr_item)?;
                        }
                        Ok(())
                    }
                    LogicalPlan::Join(Join {
                        on: ref keys,
                        filter,
                        join_constraint,
                        join_type,
                        ..
                    }) => {
                        let join_expr: Vec<String> =
                            keys.iter().map(|(l, r)| format!("{} = {}", l, r)).collect();
                        let filter_expr = filter
                            .as_ref()
                            .map(|expr| format!(" Filter: {}", expr))
                            .unwrap_or_else(|| "".to_string());
                        match join_constraint {
                            JoinConstraint::On => {
                                write!(
                                    f,
                                    "{} Join: {}{}",
                                    join_type,
                                    join_expr.join(", "),
                                    filter_expr
                                )
                            }
                            JoinConstraint::Using => {
                                write!(
                                    f,
                                    "{} Join: Using {}{}",
                                    join_type,
                                    join_expr.join(", "),
                                    filter_expr,
                                )
                            }
                        }
                    }
                    LogicalPlan::CrossJoin(_) => {
                        write!(f, "CrossJoin:")
                    }
                    LogicalPlan::Repartition(Repartition {
                        partitioning_scheme,
                        ..
                    }) => match partitioning_scheme {
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
                        Partitioning::DistributeBy(expr) => {
                            let dist_by_expr: Vec<String> =
                                expr.iter().map(|e| format!("{:?}", e)).collect();
                            write!(
                                f,
                                "Repartition: DistributeBy({})",
                                dist_by_expr.join(", "),
                            )
                        }
                    },
                    LogicalPlan::Limit(Limit {
                        ref skip,
                        ref fetch,
                        ..
                    }) => {
                        write!(
                            f,
                            "Limit: skip={}, fetch={}",
                            skip.map_or("None".to_string(), |x| x.to_string()),
                            fetch.map_or_else(|| "None".to_string(), |x| x.to_string())
                        )
                    }
                    LogicalPlan::Subquery(Subquery { .. }) => {
                        write!(f, "Subquery:")
                    }
                    LogicalPlan::SubqueryAlias(SubqueryAlias { ref alias, .. }) => {
                        write!(f, "SubqueryAlias: {}", alias)
                    }
                    LogicalPlan::CreateExternalTable(CreateExternalTable {
                        ref name,
                        ..
                    }) => {
                        write!(f, "CreateExternalTable: {:?}", name)
                    }
                    LogicalPlan::CreateMemoryTable(CreateMemoryTable {
                        name, ..
                    }) => {
                        write!(f, "CreateMemoryTable: {:?}", name)
                    }
                    LogicalPlan::CreateView(CreateView { name, .. }) => {
                        write!(f, "CreateView: {:?}", name)
                    }
                    LogicalPlan::CreateCatalogSchema(CreateCatalogSchema {
                        schema_name,
                        ..
                    }) => {
                        write!(f, "CreateCatalogSchema: {:?}", schema_name)
                    }
                    LogicalPlan::CreateCatalog(CreateCatalog {
                        catalog_name, ..
                    }) => {
                        write!(f, "CreateCatalog: {:?}", catalog_name)
                    }
                    LogicalPlan::DropTable(DropTable {
                        name, if_exists, ..
                    }) => {
                        write!(f, "DropTable: {:?} if not exist:={}", name, if_exists)
                    }
                    LogicalPlan::Distinct(Distinct { .. }) => {
                        write!(f, "Distinct:")
                    }
                    LogicalPlan::Explain { .. } => write!(f, "Explain"),
                    LogicalPlan::Analyze { .. } => write!(f, "Analyze"),
                    LogicalPlan::Union(_) => write!(f, "Union"),
                    LogicalPlan::Extension(e) => e.node.fmt_for_explain(f),
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

impl ToStringifiedPlan for LogicalPlan {
    fn to_stringified(&self, plan_type: PlanType) -> StringifiedPlan {
        StringifiedPlan::new(plan_type, self.display_indent().to_string())
    }
}

/// Join type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

impl Display for JoinType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let join_type = match self {
            JoinType::Inner => "Inner",
            JoinType::Left => "Left",
            JoinType::Right => "Right",
            JoinType::Full => "Full",
            JoinType::Semi => "Semi",
            JoinType::Anti => "Anti",
        };
        write!(f, "{}", join_type)
    }
}

/// Join constraint
#[derive(Debug, Clone, Copy)]
pub enum JoinConstraint {
    /// Join ON
    On,
    /// Join USING
    Using,
}

/// Creates a catalog (aka "Database").
#[derive(Clone)]
pub struct CreateCatalog {
    /// The catalog name
    pub catalog_name: String,
    /// Do nothing (except issuing a notice) if a schema with the same name already exists
    pub if_not_exists: bool,
    /// Empty schema
    pub schema: DFSchemaRef,
}

/// Creates a schema.
#[derive(Clone)]
pub struct CreateCatalogSchema {
    /// The table schema
    pub schema_name: String,
    /// Do nothing (except issuing a notice) if a schema with the same name already exists
    pub if_not_exists: bool,
    /// Empty schema
    pub schema: DFSchemaRef,
}

/// Drops a table.
#[derive(Clone)]
pub struct DropTable {
    /// The table name
    pub name: String,
    /// If the table exists
    pub if_exists: bool,
    /// Dummy schema
    pub schema: DFSchemaRef,
}

/// Produces no rows: An empty relation with an empty schema
#[derive(Clone)]
pub struct EmptyRelation {
    /// Whether to produce a placeholder row
    pub produce_one_row: bool,
    /// The schema description of the output
    pub schema: DFSchemaRef,
}

/// Values expression. See
/// [Postgres VALUES](https://www.postgresql.org/docs/current/queries-values.html)
/// documentation for more details.
#[derive(Clone)]
pub struct Values {
    /// The table schema
    pub schema: DFSchemaRef,
    /// Values
    pub values: Vec<Vec<Expr>>,
}

/// Evaluates an arbitrary list of expressions (essentially a
/// SELECT with an expression list) on its input.
#[derive(Clone)]
pub struct Projection {
    /// The list of expressions
    pub expr: Vec<Expr>,
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// The schema description of the output
    pub schema: DFSchemaRef,
    /// Projection output relation alias
    pub alias: Option<String>,
}

impl Projection {
    /// Create a new Projection
    pub fn try_new(
        expr: Vec<Expr>,
        input: Arc<LogicalPlan>,
        alias: Option<String>,
    ) -> Result<Self, DataFusionError> {
        let schema = Arc::new(DFSchema::new_with_metadata(
            exprlist_to_fields(&expr, &input)?,
            input.schema().metadata().clone(),
        )?);
        Self::try_new_with_schema(expr, input, schema, alias)
    }

    /// Create a new Projection using the specified output schema
    pub fn try_new_with_schema(
        expr: Vec<Expr>,
        input: Arc<LogicalPlan>,
        schema: DFSchemaRef,
        alias: Option<String>,
    ) -> Result<Self, DataFusionError> {
        if expr.len() != schema.fields().len() {
            return Err(DataFusionError::Plan(format!("Projection has mismatch between number of expressions ({}) and number of fields in schema ({})", expr.len(), schema.fields().len())));
        }
        Ok(Self {
            expr,
            input,
            schema,
            alias,
        })
    }

    pub fn try_from_plan(plan: &LogicalPlan) -> datafusion_common::Result<&Projection> {
        match plan {
            LogicalPlan::Projection(it) => Ok(it),
            _ => plan_err!("Could not coerce into Projection!"),
        }
    }
}

/// Aliased subquery
#[derive(Clone)]
pub struct SubqueryAlias {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// The alias for the input relation
    pub alias: String,
    /// The schema with qualified field names
    pub schema: DFSchemaRef,
}

/// Filters rows from its input that do not match an
/// expression (essentially a WHERE clause with a predicate
/// expression).
///
/// Semantically, `<predicate>` is evaluated for each row of the input;
/// If the value of `<predicate>` is true, the input row is passed to
/// the output. If the value of `<predicate>` is false, the row is
/// discarded.
#[derive(Clone)]
pub struct Filter {
    /// The predicate expression, which must have Boolean type.
    pub predicate: Expr,
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
}

impl Filter {
    pub fn try_from_plan(plan: &LogicalPlan) -> datafusion_common::Result<&Filter> {
        match plan {
            LogicalPlan::Filter(it) => Ok(it),
            _ => plan_err!("Could not coerce into Filter!"),
        }
    }
}

/// Window its input based on a set of window spec and window function (e.g. SUM or RANK)
#[derive(Clone)]
pub struct Window {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// The window function expression
    pub window_expr: Vec<Expr>,
    /// The schema description of the window output
    pub schema: DFSchemaRef,
}

/// Produces rows from a table provider by reference or from the context
#[derive(Clone)]
pub struct TableScan {
    /// The name of the table
    pub table_name: String,
    /// The source of the table
    pub source: Arc<dyn TableSource>,
    /// Optional column indices to use as a projection
    pub projection: Option<Vec<usize>>,
    /// The schema description of the output
    pub projected_schema: DFSchemaRef,
    /// Optional expressions to be used as filters by the table provider
    pub filters: Vec<Expr>,
    /// Optional number of rows to read
    pub fetch: Option<usize>,
}

/// Apply Cross Join to two logical plans
#[derive(Clone)]
pub struct CrossJoin {
    /// Left input
    pub left: Arc<LogicalPlan>,
    /// Right input
    pub right: Arc<LogicalPlan>,
    /// The output schema, containing fields from the left and right inputs
    pub schema: DFSchemaRef,
}

/// Repartition the plan based on a partitioning scheme.
#[derive(Clone)]
pub struct Repartition {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// The partitioning scheme
    pub partitioning_scheme: Partitioning,
}

/// Union multiple inputs
#[derive(Clone)]
pub struct Union {
    /// Inputs to merge
    pub inputs: Vec<Arc<LogicalPlan>>,
    /// Union schema. Should be the same for all inputs.
    pub schema: DFSchemaRef,
    /// Union output relation alias
    pub alias: Option<String>,
}

/// Creates an in memory table.
#[derive(Clone)]
pub struct CreateMemoryTable {
    /// The table name
    pub name: String,
    /// The logical plan
    pub input: Arc<LogicalPlan>,
    /// Option to not error if table already exists
    pub if_not_exists: bool,
    /// Option to replace table content if table already exists
    pub or_replace: bool,
}

/// Creates a view.
#[derive(Clone)]
pub struct CreateView {
    /// The table name
    pub name: String,
    /// The logical plan
    pub input: Arc<LogicalPlan>,
    /// Option to not error if table already exists
    pub or_replace: bool,
    /// SQL used to create the view, if available
    pub definition: Option<String>,
}

/// Types of files to parse as DataFrames
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    /// Newline-delimited JSON
    NdJson,
    /// Apache Parquet columnar storage
    Parquet,
    /// Comma separated values
    CSV,
    /// Avro binary records
    Avro,
}

/// Creates an external table.
#[derive(Clone)]
pub struct CreateExternalTable {
    /// The table schema
    pub schema: DFSchemaRef,
    /// The table name
    pub name: String,
    /// The physical location
    pub location: String,
    /// The file type of physical file
    pub file_type: FileType,
    /// Whether the CSV file contains a header
    pub has_header: bool,
    /// Delimiter for CSV
    pub delimiter: char,
    /// Partition Columns
    pub table_partition_cols: Vec<String>,
    /// Option to not error if table already exists
    pub if_not_exists: bool,
}

/// Produces a relation with string representations of
/// various parts of the plan
#[derive(Clone)]
pub struct Explain {
    /// Should extra (detailed, intermediate plans) be included?
    pub verbose: bool,
    /// The logical plan that is being EXPLAIN'd
    pub plan: Arc<LogicalPlan>,
    /// Represent the various stages plans have gone through
    pub stringified_plans: Vec<StringifiedPlan>,
    /// The output schema of the explain (2 columns of text)
    pub schema: DFSchemaRef,
}

/// Runs the actual plan, and then prints the physical plan with
/// with execution metrics.
#[derive(Clone)]
pub struct Analyze {
    /// Should extra detail be included?
    pub verbose: bool,
    /// The logical plan that is being EXPLAIN ANALYZE'd
    pub input: Arc<LogicalPlan>,
    /// The output schema of the explain (2 columns of text)
    pub schema: DFSchemaRef,
}

/// Extension operator defined outside of DataFusion
#[derive(Clone)]
pub struct Extension {
    /// The runtime extension operator
    pub node: Arc<dyn UserDefinedLogicalNode>,
}

/// Produces the first `n` tuples from its input and discards the rest.
#[derive(Clone)]
pub struct Limit {
    /// Number of rows to skip before fetch
    pub skip: Option<usize>,
    /// Maximum number of rows to fetch
    pub fetch: Option<usize>,
    /// The logical plan
    pub input: Arc<LogicalPlan>,
}

/// Removes duplicate rows from the input
#[derive(Clone)]
pub struct Distinct {
    /// The logical plan that is being DISTINCT'd
    pub input: Arc<LogicalPlan>,
}

/// Aggregates its input based on a set of grouping and aggregate
/// expressions (e.g. SUM).
#[derive(Clone)]
pub struct Aggregate {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// Grouping expressions
    pub group_expr: Vec<Expr>,
    /// Aggregate expressions
    pub aggr_expr: Vec<Expr>,
    /// The schema description of the aggregate output
    pub schema: DFSchemaRef,
}

impl Aggregate {
    pub fn try_from_plan(plan: &LogicalPlan) -> datafusion_common::Result<&Aggregate> {
        match plan {
            LogicalPlan::Aggregate(it) => Ok(it),
            _ => plan_err!("Could not coerce into Aggregate!"),
        }
    }
}

/// Sorts its input according to a list of sort expressions.
#[derive(Clone)]
pub struct Sort {
    /// The sort expressions
    pub expr: Vec<Expr>,
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
}

/// Join two logical plans on one or more join columns
#[derive(Clone)]
pub struct Join {
    /// Left input
    pub left: Arc<LogicalPlan>,
    /// Right input
    pub right: Arc<LogicalPlan>,
    /// Equijoin clause expressed as pairs of (left, right) join columns
    pub on: Vec<(Column, Column)>,
    /// Filters applied during join (non-equi conditions)
    pub filter: Option<Expr>,
    /// Join type
    pub join_type: JoinType,
    /// Join constraint
    pub join_constraint: JoinConstraint,
    /// The output schema, containing fields from the left and right inputs
    pub schema: DFSchemaRef,
    /// If null_equals_null is true, null == null else null != null
    pub null_equals_null: bool,
}

/// Subquery
#[derive(Clone)]
pub struct Subquery {
    /// The subquery
    pub subquery: Arc<LogicalPlan>,
}

impl Subquery {
    pub fn try_from_expr(plan: &Expr) -> datafusion_common::Result<&Subquery> {
        match plan {
            Expr::ScalarSubquery(it) => Ok(it),
            _ => plan_err!("Could not coerce into ScalarSubquery!"),
        }
    }
}

impl Debug for Subquery {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "<subquery>")
    }
}

impl Hash for Subquery {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.finish();
    }

    fn hash_slice<H: Hasher>(_data: &[Self], state: &mut H)
    where
        Self: Sized,
    {
        state.finish();
    }
}

impl PartialEq for Subquery {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

/// Logical partitioning schemes supported by the repartition operator.
#[derive(Debug, Clone)]
pub enum Partitioning {
    /// Allocate batches using a round-robin algorithm and the specified number of partitions
    RoundRobinBatch(usize),
    /// Allocate rows based on a hash of one of more expressions and the specified number
    /// of partitions.
    Hash(Vec<Expr>, usize),
    /// The DISTRIBUTE BY clause is used to repartition the data based on the input expressions
    DistributeBy(Vec<Expr>),
}

/// Represents which type of plan, when storing multiple
/// for use in EXPLAIN plans
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanType {
    /// The initial LogicalPlan provided to DataFusion
    InitialLogicalPlan,
    /// The LogicalPlan which results from applying an optimizer pass
    OptimizedLogicalPlan {
        /// The name of the optimizer which produced this plan
        optimizer_name: String,
    },
    /// The final, fully optimized LogicalPlan that was converted to a physical plan
    FinalLogicalPlan,
    /// The initial physical plan, prepared for execution
    InitialPhysicalPlan,
    /// The ExecutionPlan which results from applying an optimizer pass
    OptimizedPhysicalPlan {
        /// The name of the optimizer which produced this plan
        optimizer_name: String,
    },
    /// The final, fully optimized physical which would be executed
    FinalPhysicalPlan,
}

impl fmt::Display for PlanType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PlanType::InitialLogicalPlan => write!(f, "initial_logical_plan"),
            PlanType::OptimizedLogicalPlan { optimizer_name } => {
                write!(f, "logical_plan after {}", optimizer_name)
            }
            PlanType::FinalLogicalPlan => write!(f, "logical_plan"),
            PlanType::InitialPhysicalPlan => write!(f, "initial_physical_plan"),
            PlanType::OptimizedPhysicalPlan { optimizer_name } => {
                write!(f, "physical_plan after {}", optimizer_name)
            }
            PlanType::FinalPhysicalPlan => write!(f, "physical_plan"),
        }
    }
}

/// Represents some sort of execution plan, in String form
#[derive(Debug, Clone, PartialEq, Eq)]
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
        match self.plan_type {
            PlanType::FinalLogicalPlan | PlanType::FinalPhysicalPlan => true,
            _ => verbose_mode,
        }
    }
}

/// Trait for something that can be formatted as a stringified plan
pub trait ToStringifiedPlan {
    /// Create a stringified plan with the specified type
    fn to_stringified(&self, plan_type: PlanType) -> StringifiedPlan;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::table_scan;
    use crate::{col, in_subquery, lit};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::DFSchema;
    use datafusion_common::Result;
    use std::collections::HashMap;

    fn employee_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ])
    }

    fn display_plan() -> Result<LogicalPlan> {
        let plan1 = table_scan(Some("employee_csv"), &employee_schema(), Some(vec![3]))?
            .build()?;

        table_scan(Some("employee_csv"), &employee_schema(), Some(vec![0, 3]))?
            .filter(in_subquery(col("state"), Arc::new(plan1)))?
            .project(vec![col("id")])?
            .build()
    }

    #[test]
    fn test_display_indent() -> Result<()> {
        let plan = display_plan()?;

        let expected = "Projection: #employee_csv.id\
        \n  Filter: #employee_csv.state IN (<subquery>)\
        \n    Subquery:\
        \n      TableScan: employee_csv projection=[state]\
        \n    TableScan: employee_csv projection=[id, state]";

        assert_eq!(expected, format!("{}", plan.display_indent()));
        Ok(())
    }

    #[test]
    fn test_display_indent_schema() -> Result<()> {
        let plan = display_plan()?;

        let expected = "Projection: #employee_csv.id [id:Int32]\
        \n  Filter: #employee_csv.state IN (<subquery>) [id:Int32, state:Utf8]\
        \n    Subquery: [state:Utf8]\
        \n      TableScan: employee_csv projection=[state] [state:Utf8]\
        \n    TableScan: employee_csv projection=[id, state] [id:Int32, state:Utf8]";

        assert_eq!(expected, format!("{}", plan.display_indent_schema()));
        Ok(())
    }

    #[test]
    fn test_display_graphviz() -> Result<()> {
        let plan = display_plan()?;

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
                r#"[shape=box label="TableScan: employee_csv projection=[id, state]"]"#
            ),
            "\n{}",
            plan.display_graphviz()
        );
        assert!(graphviz.contains(r#"[shape=box label="TableScan: employee_csv projection=[id, state]\nSchema: [id:Int32, state:Utf8]"]"#),
                "\n{}", plan.display_graphviz());
        assert!(
            graphviz.contains(r#"// End DataFusion GraphViz Plan"#),
            "\n{}",
            plan.display_graphviz()
        );
        Ok(())
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
                "post_visit Projection",
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
            vec!["pre_visit Projection", "pre_visit Filter"]
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
            vec!["pre_visit Projection", "pre_visit Filter"]
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

    #[test]
    fn projection_expr_schema_mismatch() -> Result<()> {
        let empty_schema = Arc::new(DFSchema::new_with_metadata(vec![], HashMap::new())?);
        let p = Projection::try_new_with_schema(
            vec![col("a")],
            Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: empty_schema.clone(),
            })),
            empty_schema,
            None,
        );
        assert_eq!("Error during planning: Projection has mismatch between number of expressions (1) and number of fields in schema (0)", format!("{}", p.err().unwrap()));
        Ok(())
    }

    fn test_plan() -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("state", DataType::Utf8, false),
        ]);

        table_scan(None, &schema, Some(vec![0, 1]))
            .unwrap()
            .filter(col("state").eq(lit("CO")))
            .unwrap()
            .project(vec![col("id")])
            .unwrap()
            .build()
            .unwrap()
    }
}
