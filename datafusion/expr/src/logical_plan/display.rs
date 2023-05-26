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
//! This module provides logic for displaying LogicalPlans in various styles

use crate::LogicalPlan;
use arrow::datatypes::Schema;
use datafusion_common::tree_node::{TreeNodeVisitor, VisitRecursion};
use datafusion_common::DataFusionError;
use std::fmt;

/// Formats plans with a single line per node. For example:
///
/// Projection: id
///    Filter: state Eq Utf8(\"CO\")\
///       CsvScan: employee.csv projection=Some([0, 3])";
pub struct IndentVisitor<'a, 'b> {
    f: &'a mut fmt::Formatter<'b>,
    /// If true, includes summarized schema information
    with_schema: bool,
    /// The current indent
    indent: usize,
}

impl<'a, 'b> IndentVisitor<'a, 'b> {
    /// Create a visitor that will write a formatted LogicalPlan to f. If `with_schema` is
    /// true, includes schema information on each line.
    pub fn new(f: &'a mut fmt::Formatter<'b>, with_schema: bool) -> Self {
        Self {
            f,
            with_schema,
            indent: 0,
        }
    }
}

impl<'a, 'b> TreeNodeVisitor for IndentVisitor<'a, 'b> {
    type N = LogicalPlan;

    fn pre_visit(
        &mut self,
        plan: &LogicalPlan,
    ) -> datafusion_common::Result<VisitRecursion> {
        if self.indent > 0 {
            writeln!(self.f)?;
        }
        write!(self.f, "{:indent$}", "", indent = self.indent * 2)?;
        write!(self.f, "{}", plan.display())?;
        if self.with_schema {
            write!(
                self.f,
                " {}",
                display_schema(&plan.schema().as_ref().to_owned().into())
            )?;
        }

        self.indent += 1;
        Ok(VisitRecursion::Continue)
    }

    fn post_visit(
        &mut self,
        _plan: &LogicalPlan,
    ) -> datafusion_common::Result<VisitRecursion> {
        self.indent -= 1;
        Ok(VisitRecursion::Continue)
    }
}

/// Print the schema in a compact representation to `buf`
///
/// For example: `foo:Utf8` if `foo` can not be null, and
/// `foo:Utf8;N` if `foo` is nullable.
///
/// ```
/// use arrow::datatypes::{Field, Schema, DataType};
/// # use datafusion_expr::logical_plan::display_schema;
/// let schema = Schema::new(vec![
///     Field::new("id", DataType::Int32, false),
///     Field::new("first_name", DataType::Utf8, true),
///  ]);
///
///  assert_eq!(
///      "[id:Int32, first_name:Utf8;N]",
///      format!("{}", display_schema(&schema))
///  );
/// ```
pub fn display_schema(schema: &Schema) -> impl fmt::Display + '_ {
    struct Wrapper<'a>(&'a Schema);

    impl<'a> fmt::Display for Wrapper<'a> {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "[")?;
            for (idx, field) in self.0.fields().iter().enumerate() {
                if idx > 0 {
                    write!(f, ", ")?;
                }
                let nullable_str = if field.is_nullable() { ";N" } else { "" };
                write!(
                    f,
                    "{}:{:?}{}",
                    field.name(),
                    field.data_type(),
                    nullable_str
                )?;
            }
            write!(f, "]")
        }
    }
    Wrapper(schema)
}

/// Logic related to creating DOT language graphs.
#[derive(Default)]
struct GraphvizBuilder {
    id_gen: usize,
}

impl GraphvizBuilder {
    fn next_id(&mut self) -> usize {
        self.id_gen += 1;
        self.id_gen
    }

    // write out the start of the subgraph cluster
    fn start_cluster(&mut self, f: &mut fmt::Formatter, title: &str) -> fmt::Result {
        writeln!(f, "  subgraph cluster_{}", self.next_id())?;
        writeln!(f, "  {{")?;
        writeln!(f, "    graph[label={}]", Self::quoted(title))
    }

    // write out the end of the subgraph cluster
    fn end_cluster(&mut self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "  }}")
    }

    /// makes a quoted string suitable for inclusion in a graphviz chart
    fn quoted(label: &str) -> String {
        let label = label.replace('"', "_");
        format!("\"{label}\"")
    }
}

/// Formats plans for graphical display using the `DOT` language. This
/// format can be visualized using software from
/// [`graphviz`](https://graphviz.org/)
pub struct GraphvizVisitor<'a, 'b> {
    f: &'a mut fmt::Formatter<'b>,
    graphviz_builder: GraphvizBuilder,
    /// If true, includes summarized schema information
    with_schema: bool,

    /// Holds the ids (as generated from `graphviz_builder` of all
    /// parent nodes
    parent_ids: Vec<usize>,
}

impl<'a, 'b> GraphvizVisitor<'a, 'b> {
    pub fn new(f: &'a mut fmt::Formatter<'b>) -> Self {
        Self {
            f,
            graphviz_builder: GraphvizBuilder::default(),
            with_schema: false,
            parent_ids: Vec::new(),
        }
    }

    /// Sets a flag which controls if the output schema is displayed
    pub fn set_with_schema(&mut self, with_schema: bool) {
        self.with_schema = with_schema;
    }

    pub fn pre_visit_plan(&mut self, label: &str) -> fmt::Result {
        self.graphviz_builder.start_cluster(self.f, label)
    }

    pub fn post_visit_plan(&mut self) -> fmt::Result {
        self.graphviz_builder.end_cluster(self.f)
    }
}

impl<'a, 'b> TreeNodeVisitor for GraphvizVisitor<'a, 'b> {
    type N = LogicalPlan;

    fn pre_visit(
        &mut self,
        plan: &LogicalPlan,
    ) -> datafusion_common::Result<VisitRecursion> {
        let id = self.graphviz_builder.next_id();

        // Create a new graph node for `plan` such as
        // id [label="foo"]
        let label = if self.with_schema {
            format!(
                r"{}\nSchema: {}",
                plan.display(),
                display_schema(&plan.schema().as_ref().to_owned().into())
            )
        } else {
            format!("{}", plan.display())
        };

        writeln!(
            self.f,
            "    {}[shape=box label={}]",
            id,
            GraphvizBuilder::quoted(&label)
        )
        .map_err(|_e| DataFusionError::Internal("Fail to format".to_string()))?;

        // Create an edge to our parent node, if any
        //  parent_id -> id
        if let Some(parent_id) = self.parent_ids.last() {
            writeln!(
                self.f,
                "    {parent_id} -> {id} [arrowhead=none, arrowtail=normal, dir=back]"
            )
            .map_err(|_e| DataFusionError::Internal("Fail to format".to_string()))?;
        }

        self.parent_ids.push(id);
        Ok(VisitRecursion::Continue)
    }

    fn post_visit(
        &mut self,
        _plan: &LogicalPlan,
    ) -> datafusion_common::Result<VisitRecursion> {
        // always be non-empty as pre_visit always pushes
        // So it should always be Ok(true)
        let res = self.parent_ids.pop();
        res.ok_or(DataFusionError::Internal("Fail to format".to_string()))
            .map(|_| VisitRecursion::Continue)
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field};

    use super::*;

    #[test]
    fn test_display_empty_schema() {
        let schema = Schema::empty();
        assert_eq!("[]", format!("{}", display_schema(&schema)));
    }

    #[test]
    fn test_display_schema() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, true),
        ]);

        assert_eq!(
            "[id:Int32, first_name:Utf8;N]",
            format!("{}", display_schema(&schema))
        );
    }
}
