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

use std::fmt::Display;
use std::ops::Deref;
use std::sync::Arc;
use std::vec::IntoIter;

use crate::PhysicalExpr;
use crate::expressions::with_new_schema;

use arrow::datatypes::SchemaRef;
use datafusion_common::{HashSet, Result};
use datafusion_physical_expr_common::physical_expr::format_physical_expr_list;

/// A set of grouping tuples that are known to describe contiguous rows.
///
/// Each entry is a complete grouping tuple. For example, `[a, b]` means that,
/// within each output partition, all rows having the same values for both `a`
/// and `b` occur in one contiguous run. The runs themselves may occur in any
/// order. Contiguity applies to the complete partition stream, not separately
/// to each record batch.
///
/// Expression order within an entry is not significant: `[a, b]` and `[b, a]`
/// describe the same grouping. Entries do not imply properties for subsets;
/// `[a, b]` alone says nothing about whether all rows with the same `a` are
/// contiguous.
#[derive(Clone, Debug, Default)]
pub struct GroupingEquivalenceClass {
    groupings: Vec<Vec<Arc<dyn PhysicalExpr>>>,
}

impl GroupingEquivalenceClass {
    /// Clears all groupings in this equivalence class.
    pub fn clear(&mut self) {
        self.groupings.clear();
    }

    /// Creates a grouping equivalence class, discarding empty and duplicate
    /// entries and duplicate expressions within each entry.
    pub fn new(
        groupings: impl IntoIterator<Item = impl IntoIterator<Item = Arc<dyn PhysicalExpr>>>,
    ) -> Self {
        let mut result = Self::default();
        result.add_groupings(groupings);
        result
    }

    /// Adds grouping tuples to this equivalence class.
    pub fn add_groupings(
        &mut self,
        groupings: impl IntoIterator<Item = impl IntoIterator<Item = Arc<dyn PhysicalExpr>>>,
    ) {
        for grouping in groupings {
            let mut seen = HashSet::new();
            let grouping = grouping
                .into_iter()
                .filter(|expr| seen.insert(Arc::clone(expr)))
                .collect::<Vec<_>>();
            if !grouping.is_empty() && !self.contains(&grouping) {
                self.groupings.push(grouping);
            }
        }
    }

    /// Returns whether this class contains the complete grouping tuple.
    pub fn contains(&self, grouping: &[Arc<dyn PhysicalExpr>]) -> bool {
        self.groupings
            .iter()
            .any(|candidate| same_grouping(candidate, grouping))
    }

    /// Rewrites all expressions to reference an aligned schema.
    pub fn with_new_schema(self, schema: &SchemaRef) -> Result<Self> {
        let groupings = self.groupings.into_iter().map(|grouping| {
            grouping
                .into_iter()
                .map(|expr| with_new_schema(expr, schema))
                .collect::<Result<Vec<_>>>()
        });
        Ok(Self::new(groupings.collect::<Result<Vec<_>>>()?))
    }
}

fn same_grouping(lhs: &[Arc<dyn PhysicalExpr>], rhs: &[Arc<dyn PhysicalExpr>]) -> bool {
    lhs.iter()
        .all(|lhs_expr| rhs.iter().any(|rhs_expr| lhs_expr.eq(rhs_expr)))
        && rhs
            .iter()
            .all(|rhs_expr| lhs.iter().any(|lhs_expr| rhs_expr.eq(lhs_expr)))
}

impl PartialEq for GroupingEquivalenceClass {
    fn eq(&self, other: &Self) -> bool {
        self.groupings.len() == other.groupings.len()
            && self
                .groupings
                .iter()
                .all(|grouping| other.contains(grouping))
    }
}

impl Eq for GroupingEquivalenceClass {}

impl Deref for GroupingEquivalenceClass {
    type Target = [Vec<Arc<dyn PhysicalExpr>>];

    fn deref(&self) -> &Self::Target {
        self.groupings.as_slice()
    }
}

impl From<Vec<Vec<Arc<dyn PhysicalExpr>>>> for GroupingEquivalenceClass {
    fn from(groupings: Vec<Vec<Arc<dyn PhysicalExpr>>>) -> Self {
        Self::new(groupings)
    }
}

/// Converts the grouping equivalence class into an iterator of complete
/// grouping tuples.
impl IntoIterator for GroupingEquivalenceClass {
    type Item = Vec<Arc<dyn PhysicalExpr>>;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.groupings.into_iter()
    }
}

impl From<GroupingEquivalenceClass> for Vec<Vec<Arc<dyn PhysicalExpr>>> {
    fn from(geq_class: GroupingEquivalenceClass) -> Self {
        geq_class.groupings
    }
}

impl Display for GroupingEquivalenceClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        let mut groupings = self.groupings.iter();
        if let Some(grouping) = groupings.next() {
            write!(f, "{}", format_physical_expr_list(grouping))?;
        }
        for grouping in groupings {
            write!(f, ", {}", format_physical_expr_list(grouping))?;
        }
        write!(f, "]")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::Column;

    #[test]
    fn grouping_equality_ignores_expression_and_entry_order() {
        let a = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let b = Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>;
        let c = Arc::new(Column::new("c", 2)) as Arc<dyn PhysicalExpr>;

        let lhs = GroupingEquivalenceClass::new([
            vec![Arc::clone(&a), Arc::clone(&b)],
            vec![Arc::clone(&c)],
        ]);
        let rhs = GroupingEquivalenceClass::new([
            vec![Arc::clone(&c)],
            vec![Arc::clone(&b), Arc::clone(&a)],
        ]);

        assert_eq!(lhs, rhs);
    }

    #[test]
    fn grouping_deduplicates_entries_and_expressions() {
        let a = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let b = Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>;

        let groupings = GroupingEquivalenceClass::new([
            vec![Arc::clone(&a), Arc::clone(&a), Arc::clone(&b)],
            vec![Arc::clone(&b), Arc::clone(&a)],
            vec![],
        ]);

        assert_eq!(groupings.len(), 1);
        assert_eq!(groupings[0].len(), 2);
        assert!(groupings.contains(&[Arc::clone(&a), Arc::clone(&a), Arc::clone(&b),]));
        assert_eq!(groupings.to_string(), "[[a@0, b@1]]");
    }
}
