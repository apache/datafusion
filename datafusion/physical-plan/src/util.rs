use std::sync::Arc;

use datafusion_common::{
    HashMap,
    tree_node::{Transformed, TreeNodeRewriter},
};
use datafusion_physical_expr::{
    PhysicalExpr,
    expressions::{Column, UnKnownColumn},
};

/// Rewrite column references in a physical expr according to a mapping.
/// TODO: instead just add a ProjectionExec as a new child of leaf node
pub struct PhysicalColumnRewriter {
    /// Mapping from original column to new column.
    pub column_map: HashMap<Column, Column>,
}

impl PhysicalColumnRewriter {
    /// Create a new PhysicalColumnRewriter with the given column mapping.
    pub fn new(column_map: HashMap<Column, Column>) -> Self {
        Self { column_map }
    }
}

impl TreeNodeRewriter for PhysicalColumnRewriter {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_down(
        &mut self,
        node: Self::Node,
    ) -> datafusion_common::Result<Transformed<Self::Node>> {
        if let Some(column) = node.as_any().downcast_ref::<Column>() {
            if let Some(new_column) = self.column_map.get(column) {
                return Ok(Transformed::yes(Arc::new(new_column.clone())));
            } else {
                return Ok(Transformed::yes(Arc::new(UnKnownColumn::new(
                    column.name(),
                ))));
            }
        }
        Ok(Transformed::no(node))
    }
}
