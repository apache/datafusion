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

use crate::expressions::{BinaryExpr, Column};
use crate::{
    normalize_expr_with_equivalence_properties, LexOrdering, PhysicalExpr,
    PhysicalSortExpr,
};

use arrow::datatypes::SchemaRef;

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;

/// Represents a collection of [`EquivalentClass`] (equivalences
/// between columns in relations)
///
/// This is used to represent both:
///
/// 1. Equality conditions (like `A=B`), when `T` = [`Column`]
/// 2. Ordering (like `A ASC = B ASC`), when `T` = [`PhysicalSortExpr`]
#[derive(Debug, Clone)]
pub struct EquivalenceProperties<T = Column> {
    classes: Vec<EquivalentClass<T>>,
    schema: SchemaRef,
}

impl<T: Eq + Clone + Hash> EquivalenceProperties<T> {
    pub fn new(schema: SchemaRef) -> Self {
        EquivalenceProperties {
            classes: vec![],
            schema,
        }
    }

    /// return the set of equivalences
    pub fn classes(&self) -> &[EquivalentClass<T>] {
        &self.classes
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Add the [`EquivalentClass`] from `iter` to this list
    pub fn extend<I: IntoIterator<Item = EquivalentClass<T>>>(&mut self, iter: I) {
        for ec in iter {
            self.classes.push(ec)
        }
    }

    /// Adds new equal conditions into the EquivalenceProperties. New equal
    /// conditions usually come from equality predicates in a join/filter.
    pub fn add_equal_conditions(&mut self, new_conditions: (&T, &T)) {
        let mut idx1: Option<usize> = None;
        let mut idx2: Option<usize> = None;
        for (idx, class) in self.classes.iter_mut().enumerate() {
            let contains_first = class.contains(new_conditions.0);
            let contains_second = class.contains(new_conditions.1);
            match (contains_first, contains_second) {
                (true, false) => {
                    class.insert(new_conditions.1.clone());
                    idx1 = Some(idx);
                }
                (false, true) => {
                    class.insert(new_conditions.0.clone());
                    idx2 = Some(idx);
                }
                (true, true) => {
                    idx1 = Some(idx);
                    idx2 = Some(idx);
                    break;
                }
                (false, false) => {}
            }
        }

        match (idx1, idx2) {
            (Some(idx_1), Some(idx_2)) if idx_1 != idx_2 => {
                // need to merge the two existing EquivalentClasses
                let second_eq_class = self.classes.get(idx_2).unwrap().clone();
                let first_eq_class = self.classes.get_mut(idx_1).unwrap();
                for prop in second_eq_class.iter() {
                    if !first_eq_class.contains(prop) {
                        first_eq_class.insert(prop.clone());
                    }
                }
                self.classes.remove(idx_2);
            }
            (None, None) => {
                // adding new pairs
                self.classes.push(EquivalentClass::<T>::new(
                    new_conditions.0.clone(),
                    vec![new_conditions.1.clone()],
                ));
            }
            _ => {}
        }
    }
}

// Helper function to calculate column info recursively
fn get_column_indices_helper(
    indices: &mut Vec<(usize, String)>,
    expr: &Arc<dyn PhysicalExpr>,
) {
    if let Some(col) = expr.as_any().downcast_ref::<Column>() {
        indices.push((col.index(), col.name().to_string()))
    } else if let Some(binary_expr) = expr.as_any().downcast_ref::<BinaryExpr>() {
        get_column_indices_helper(indices, binary_expr.left());
        get_column_indices_helper(indices, binary_expr.right());
    };
}

/// Get index and name of each column that is in the expression (Can return multiple entries for `BinaryExpr`s)
fn get_column_indices(expr: &Arc<dyn PhysicalExpr>) -> Vec<(usize, String)> {
    let mut result = vec![];
    get_column_indices_helper(&mut result, expr);
    result
}

/// `OrderingEquivalenceProperties` keeps track of columns that describe the
/// global ordering of the schema. These columns are not necessarily same; e.g.
/// ```text
/// ┌-------┐
/// | a | b |
/// |---|---|
/// | 1 | 9 |
/// | 2 | 8 |
/// | 3 | 7 |
/// | 5 | 5 |
/// └---┴---┘
/// ```
/// where both `a ASC` and `b DESC` can describe the table ordering. With
/// `OrderingEquivalenceProperties`, we can keep track of these equivalences
/// and treat `a ASC` and `b DESC` as the same ordering requirement.
pub type OrderingEquivalenceProperties = EquivalenceProperties<LexOrdering>;

/// EquivalentClass is a set of [`Column`]s or [`PhysicalSortExpr`]s that are known
/// to have the same value in all tuples in a relation. `EquivalentClass<Column>`
/// is generated by equality predicates, typically equijoin conditions and equality
/// conditions in filters. `EquivalentClass<PhysicalSortExpr>` is generated by the
/// `ROW_NUMBER` window function.
#[derive(Debug, Clone)]
pub struct EquivalentClass<T = Column> {
    /// First element in the EquivalentClass
    head: T,
    /// Other equal columns
    others: HashSet<T>,
}

impl<T: Eq + Hash + Clone> EquivalentClass<T> {
    pub fn new(head: T, others: Vec<T>) -> EquivalentClass<T> {
        EquivalentClass {
            head,
            others: HashSet::from_iter(others),
        }
    }

    pub fn head(&self) -> &T {
        &self.head
    }

    pub fn others(&self) -> &HashSet<T> {
        &self.others
    }

    pub fn contains(&self, col: &T) -> bool {
        self.head == *col || self.others.contains(col)
    }

    pub fn insert(&mut self, col: T) -> bool {
        self.head != col && self.others.insert(col)
    }

    pub fn remove(&mut self, col: &T) -> bool {
        let removed = self.others.remove(col);
        // If we are removing the head, adjust others so that its first entry becomes the new head.
        if !removed && *col == self.head {
            if let Some(col) = self.others.iter().next().cloned() {
                let removed = self.others.remove(&col);
                self.head = col;
                removed
            } else {
                // We don't allow empty equivalence classes, reject removal if one tries removing
                // the only element in an equivalence class.
                false
            }
        } else {
            removed
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &'_ T> {
        std::iter::once(&self.head).chain(self.others.iter())
    }

    pub fn len(&self) -> usize {
        self.others.len() + 1
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// `LexOrdering` stores the lexicographical ordering for a schema.
/// OrderingEquivalentClass keeps track of different alternative orderings than can
/// describe the schema.
/// For instance, for the table below
/// |a|b|c|d|
/// |1|4|3|1|
/// |2|3|3|2|
/// |3|1|2|2|
/// |3|2|1|3|
/// both `vec![a ASC, b ASC]` and `vec![c DESC, d ASC]` describe the ordering of the table.
/// For this case, we say that `vec![a ASC, b ASC]`, and `vec![c DESC, d ASC]` are ordering equivalent.
pub type OrderingEquivalentClass = EquivalentClass<LexOrdering>;

/// Update each expression in the `ordering`, with alias expressions.
/// Assume `ordering` is `a ASC, b ASC` and `c` is alias of `b`. Result will
/// be `a ASC, c ASC`.
fn update_with_alias(
    mut ordering: LexOrdering,
    columns_map: &HashMap<Column, Vec<Column>>,
) -> LexOrdering {
    for (column, columns) in columns_map {
        let col_expr = Arc::new(column.clone()) as Arc<dyn PhysicalExpr>;
        // By convention we replace with first alias expression. Since during equivalence normalization
        // same aliases are normalized, we do not lose any capability by only using first alias.
        let target_col = Arc::new(columns[0].clone()) as Arc<dyn PhysicalExpr>;
        for item in ordering.iter_mut() {
            if item.expr.eq(&col_expr) {
                // Change the corresponding entry with alias expression
                item.expr = target_col.clone();
            }
        }
    }
    ordering
}

impl OrderingEquivalentClass {
    /// This function updates ordering equivalences with alias information.
    /// For instance, assume column a and b are aliases (a as b),
    /// and column (a ASC), (c DESC) are ordering equivalent. We replace column a with b
    /// in ordering equivalence expressions. After this function, (a ASC), (c DESC) will be
    /// converted to the (b ASC), (c DESC).
    fn update_with_aliases(&mut self, columns_map: &HashMap<Column, Vec<Column>>) {
        self.head = update_with_alias(self.head.clone(), columns_map);
        self.others = self
            .others
            .iter()
            .map(|item| update_with_alias(item.clone(), columns_map))
            .collect::<HashSet<_>>();
    }
}

/// This is a builder object facilitating incremental construction
/// for ordering equivalences.
pub struct OrderingEquivalenceBuilder {
    eq_properties: EquivalenceProperties,
    ordering_eq_properties: OrderingEquivalenceProperties,
    existing_ordering: Vec<PhysicalSortExpr>,
    schema: SchemaRef,
}

impl OrderingEquivalenceBuilder {
    pub fn new(schema: SchemaRef) -> Self {
        let eq_properties = EquivalenceProperties::new(schema.clone());
        let ordering_eq_properties = OrderingEquivalenceProperties::new(schema.clone());
        Self {
            eq_properties,
            ordering_eq_properties,
            existing_ordering: vec![],
            schema,
        }
    }

    pub fn extend(
        mut self,
        new_ordering_eq_properties: OrderingEquivalenceProperties,
    ) -> Self {
        self.ordering_eq_properties
            .extend(new_ordering_eq_properties.classes().iter().cloned());
        self
    }

    pub fn with_existing_ordering(
        mut self,
        existing_ordering: Option<Vec<PhysicalSortExpr>>,
    ) -> Self {
        if let Some(existing_ordering) = existing_ordering {
            self.existing_ordering = existing_ordering;
        }
        self
    }

    pub fn with_equivalences(mut self, new_eq_properties: EquivalenceProperties) -> Self {
        self.eq_properties = new_eq_properties;
        self
    }

    pub fn add_equal_conditions(
        &mut self,
        new_equivalent_ordering: Vec<PhysicalSortExpr>,
    ) {
        let mut normalized_out_ordering = vec![];
        for item in &self.existing_ordering {
            // To account for ordering equivalences, first normalize the expression:
            let normalized = normalize_expr_with_equivalence_properties(
                item.expr.clone(),
                self.eq_properties.classes(),
            );
            normalized_out_ordering.push(PhysicalSortExpr {
                expr: normalized,
                options: item.options,
            });
        }
        // If there is an existing ordering, add new ordering as an equivalence:
        if !normalized_out_ordering.is_empty() {
            self.ordering_eq_properties.add_equal_conditions((
                &normalized_out_ordering,
                &new_equivalent_ordering,
            ));
        }
    }

    /// Return a reference to the schema with which this builder was constructed with
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Return a reference to the existing ordering
    pub fn existing_ordering(&self) -> &LexOrdering {
        &self.existing_ordering
    }

    pub fn build(self) -> OrderingEquivalenceProperties {
        self.ordering_eq_properties
    }
}

/// This function applies the given projection to the given equivalence
/// properties to compute the resulting (projected) equivalence properties; e.g.
/// 1) Adding an alias, which can introduce additional equivalence properties,
///    as in Projection(a, a as a1, a as a2).
/// 2) Truncate the [`EquivalentClass`]es that are not in the output schema.
pub fn project_equivalence_properties(
    input_eq: EquivalenceProperties,
    alias_map: &HashMap<Column, Vec<Column>>,
    output_eq: &mut EquivalenceProperties,
) {
    let mut eq_classes = input_eq.classes().to_vec();
    for (column, columns) in alias_map {
        let mut find_match = false;
        for class in eq_classes.iter_mut() {
            if class.contains(column) {
                for col in columns {
                    class.insert(col.clone());
                }
                find_match = true;
                break;
            }
        }
        if !find_match {
            eq_classes.push(EquivalentClass::new(column.clone(), columns.clone()));
        }
    }

    // Prune columns that are no longer in the schema from equivalences.
    let schema = output_eq.schema();
    let fields = schema.fields();
    for class in eq_classes.iter_mut() {
        let columns_to_remove = class
            .iter()
            .filter(|column| {
                let idx = column.index();
                idx >= fields.len() || fields[idx].name() != column.name()
            })
            .cloned()
            .collect::<Vec<_>>();
        for column in columns_to_remove {
            class.remove(&column);
        }
    }
    eq_classes.retain(|props| props.len() > 1);

    output_eq.extend(eq_classes);
}

/// This function applies the given projection to the given ordering
/// equivalence properties to compute the resulting (projected) ordering
/// equivalence properties; e.g.
/// 1) Adding an alias, which can introduce additional ordering equivalence
///    properties, as in Projection(a, a as a1, a as a2) extends global ordering
///    of a to a1 and a2.
/// 2) Truncate the [`OrderingEquivalentClass`]es that are not in the output schema.
pub fn project_ordering_equivalence_properties(
    input_eq: OrderingEquivalenceProperties,
    columns_map: &HashMap<Column, Vec<Column>>,
    output_eq: &mut OrderingEquivalenceProperties,
) {
    let mut eq_classes = input_eq.classes().to_vec();
    for class in eq_classes.iter_mut() {
        class.update_with_aliases(columns_map);
    }

    // Prune columns that no longer is in the schema from from the OrderingEquivalenceProperties.
    let schema = output_eq.schema();
    let fields = schema.fields();
    for class in eq_classes.iter_mut() {
        let sort_exprs_to_remove = class
            .iter()
            .filter(|sort_exprs| {
                sort_exprs.iter().any(|sort_expr| {
                    let col_infos = get_column_indices(&sort_expr.expr);
                    // If any one of the columns, used in Expression is invalid, remove expression
                    // from ordering equivalences
                    col_infos.into_iter().any(|(idx, name)| {
                        idx >= fields.len() || fields[idx].name() != &name
                    })
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        for sort_exprs in sort_exprs_to_remove {
            class.remove(&sort_exprs);
        }
    }
    eq_classes.retain(|props| props.len() > 1);

    output_eq.extend(eq_classes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::Column;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::Result;

    use datafusion_expr::Operator;
    use std::sync::Arc;

    #[test]
    fn add_equal_conditions_test() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
            Field::new("x", DataType::Int64, true),
            Field::new("y", DataType::Int64, true),
        ]));

        let mut eq_properties = EquivalenceProperties::new(schema);
        let new_condition = (&Column::new("a", 0), &Column::new("b", 1));
        eq_properties.add_equal_conditions(new_condition);
        assert_eq!(eq_properties.classes().len(), 1);

        let new_condition = (&Column::new("b", 1), &Column::new("a", 0));
        eq_properties.add_equal_conditions(new_condition);
        assert_eq!(eq_properties.classes().len(), 1);
        assert_eq!(eq_properties.classes()[0].len(), 2);
        assert!(eq_properties.classes()[0].contains(&Column::new("a", 0)));
        assert!(eq_properties.classes()[0].contains(&Column::new("b", 1)));

        let new_condition = (&Column::new("b", 1), &Column::new("c", 2));
        eq_properties.add_equal_conditions(new_condition);
        assert_eq!(eq_properties.classes().len(), 1);
        assert_eq!(eq_properties.classes()[0].len(), 3);
        assert!(eq_properties.classes()[0].contains(&Column::new("a", 0)));
        assert!(eq_properties.classes()[0].contains(&Column::new("b", 1)));
        assert!(eq_properties.classes()[0].contains(&Column::new("c", 2)));

        let new_condition = (&Column::new("x", 3), &Column::new("y", 4));
        eq_properties.add_equal_conditions(new_condition);
        assert_eq!(eq_properties.classes().len(), 2);

        let new_condition = (&Column::new("x", 3), &Column::new("a", 0));
        eq_properties.add_equal_conditions(new_condition);
        assert_eq!(eq_properties.classes().len(), 1);
        assert_eq!(eq_properties.classes()[0].len(), 5);
        assert!(eq_properties.classes()[0].contains(&Column::new("a", 0)));
        assert!(eq_properties.classes()[0].contains(&Column::new("b", 1)));
        assert!(eq_properties.classes()[0].contains(&Column::new("c", 2)));
        assert!(eq_properties.classes()[0].contains(&Column::new("x", 3)));
        assert!(eq_properties.classes()[0].contains(&Column::new("y", 4)));

        Ok(())
    }

    #[test]
    fn project_equivalence_properties_test() -> Result<()> {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]));

        let mut input_properties = EquivalenceProperties::new(input_schema);
        let new_condition = (&Column::new("a", 0), &Column::new("b", 1));
        input_properties.add_equal_conditions(new_condition);
        let new_condition = (&Column::new("b", 1), &Column::new("c", 2));
        input_properties.add_equal_conditions(new_condition);

        let out_schema = Arc::new(Schema::new(vec![
            Field::new("a1", DataType::Int64, true),
            Field::new("a2", DataType::Int64, true),
            Field::new("a3", DataType::Int64, true),
            Field::new("a4", DataType::Int64, true),
        ]));

        let mut alias_map = HashMap::new();
        alias_map.insert(
            Column::new("a", 0),
            vec![
                Column::new("a1", 0),
                Column::new("a2", 1),
                Column::new("a3", 2),
                Column::new("a4", 3),
            ],
        );
        let mut out_properties = EquivalenceProperties::new(out_schema);

        project_equivalence_properties(input_properties, &alias_map, &mut out_properties);
        assert_eq!(out_properties.classes().len(), 1);
        assert_eq!(out_properties.classes()[0].len(), 4);
        assert!(out_properties.classes()[0].contains(&Column::new("a1", 0)));
        assert!(out_properties.classes()[0].contains(&Column::new("a2", 1)));
        assert!(out_properties.classes()[0].contains(&Column::new("a3", 2)));
        assert!(out_properties.classes()[0].contains(&Column::new("a4", 3)));

        Ok(())
    }

    #[test]
    fn test_get_column_infos() -> Result<()> {
        let expr1 = Arc::new(Column::new("col1", 2)) as _;
        assert_eq!(get_column_indices(&expr1), vec![(2, "col1".to_string())]);
        let expr2 = Arc::new(Column::new("col2", 5)) as _;
        assert_eq!(get_column_indices(&expr2), vec![(5, "col2".to_string())]);
        let expr3 = Arc::new(BinaryExpr::new(expr1, Operator::Plus, expr2)) as _;
        assert_eq!(
            get_column_indices(&expr3),
            vec![(2, "col1".to_string()), (5, "col2".to_string())]
        );
        Ok(())
    }
}
