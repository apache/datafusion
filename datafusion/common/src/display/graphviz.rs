// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Logic related to creating DOT language graphs.

use std::fmt;

#[derive(Default)]
pub struct GraphvizBuilder {
    id_gen: usize,
}

impl GraphvizBuilder {
    // Generate next id in graphviz.
    pub fn next_id(&mut self) -> usize {
        self.id_gen += 1;
        self.id_gen
    }

    // Write out the start of whole graph.
    pub fn start_graph(&mut self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            r#"
// Begin DataFusion GraphViz Plan,
// display it online here: https://dreampuf.github.io/GraphvizOnline
"#
        )?;
        writeln!(f, "digraph {{")
    }

    pub fn end_graph(&mut self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "}}")?;
        writeln!(f, "// End DataFusion GraphViz Plan")
    }

    // write out the start of the subgraph cluster
    pub fn start_cluster(&mut self, f: &mut fmt::Formatter, title: &str) -> fmt::Result {
        writeln!(f, "  subgraph cluster_{}", self.next_id())?;
        writeln!(f, "  {{")?;
        writeln!(f, "    graph[label={}]", Self::quoted(title))
    }

    // write out the end of the subgraph cluster
    pub fn end_cluster(&mut self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "  }}")
    }

    /// makes a quoted string suitable for inclusion in a graphviz chart
    pub fn quoted(label: &str) -> String {
        let label = label.replace('"', "_");
        format!("\"{label}\"")
    }

    pub fn add_node(
        &self,
        f: &mut fmt::Formatter,
        id: usize,
        label: &str,
        tooltip: Option<&str>,
    ) -> fmt::Result {
        let tooltip = tooltip
            .map(|tooltip| format!(", tooltip={}", GraphvizBuilder::quoted(tooltip)))
            .unwrap_or_default();

        writeln!(
            f,
            "    {}[shape=box label={}{}]",
            id,
            GraphvizBuilder::quoted(label),
            tooltip,
        )
    }

    pub fn add_edge(
        &self,
        f: &mut fmt::Formatter,
        from_id: usize,
        to_id: usize,
    ) -> fmt::Result {
        writeln!(
            f,
            "    {from_id} -> {to_id} [arrowhead=none, arrowtail=normal, dir=back]"
        )
    }
}
