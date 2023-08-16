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

use crate::expressions::Column;
use crate::{
    normalize_expr_with_equivalence_properties, LexOrdering, PhysicalExpr,
    PhysicalSortExpr,
};

use arrow::datatypes::SchemaRef;
use arrow_schema::Fields;

use crate::utils::collect_columns;
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

impl OrderingEquivalenceProperties {
    /// Check whether leading_ordering is contained in either of the ordering equivalent classes.
    pub fn satisfies_leading_ordering(
        &self,
        leading_ordering: &PhysicalSortExpr,
    ) -> bool {
        for cls in &self.classes {
            for ordering in cls.others.iter().chain(std::iter::once(&cls.head)) {
                if ordering[0].eq(leading_ordering) {
                    return true;
                }
            }
        }
        false
    }
}

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

/// Update each expression in `ordering` with alias expressions. Assume
/// `ordering` is `a ASC, b ASC` and `c` is alias of `b`. Then, the result
/// will be `a ASC, c ASC`.
fn update_with_alias(
    mut ordering: LexOrdering,
    oeq_alias_map: &[(Column, Column)],
) -> LexOrdering {
    for (source_col, target_col) in oeq_alias_map {
        let source_col: Arc<dyn PhysicalExpr> = Arc::new(source_col.clone());
        // Replace invalidated columns with its alias in the ordering expression.
        let target_col: Arc<dyn PhysicalExpr> = Arc::new(target_col.clone());
        for item in ordering.iter_mut() {
            if item.expr.eq(&source_col) {
                // Change the corresponding entry with alias expression
                item.expr = target_col.clone();
            }
        }
    }
    ordering
}

impl OrderingEquivalentClass {
    /// This function updates ordering equivalences with alias information.
    /// For instance, assume columns `a` and `b` are aliases (a as b), and
    /// orderings `a ASC` and `c DESC` are equivalent. Here, we replace column
    /// `a` with `b` in ordering equivalence expressions. After this function,
    /// `a ASC`, `c DESC` will be converted to the `b ASC`, `c DESC`.
    fn update_with_aliases(
        &mut self,
        oeq_alias_map: &[(Column, Column)],
        fields: &Fields,
    ) {
        let is_head_invalid = self.head.iter().any(|sort_expr| {
            collect_columns(&sort_expr.expr)
                .iter()
                .any(|col| is_column_invalid_in_new_schema(col, fields))
        });
        // If head is invalidated, update head with alias expressions
        if is_head_invalid {
            self.head = update_with_alias(self.head.clone(), oeq_alias_map);
        } else {
            let new_oeq_expr = update_with_alias(self.head.clone(), oeq_alias_map);
            self.insert(new_oeq_expr);
        }
        for ordering in self.others.clone().into_iter() {
            self.insert(update_with_alias(ordering, oeq_alias_map));
        }
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

/// Checks whether column is still valid after projection.
fn is_column_invalid_in_new_schema(column: &Column, fields: &Fields) -> bool {
    let idx = column.index();
    idx >= fields.len() || fields[idx].name() != column.name()
}

/// Gets first aliased version of `col` found in `alias_map`.
fn get_alias_column(
    col: &Column,
    alias_map: &HashMap<Column, Vec<Column>>,
) -> Option<Column> {
    alias_map
        .iter()
        .find_map(|(column, columns)| column.eq(col).then(|| columns[0].clone()))
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
    // Get schema and fields of projection output
    let schema = output_eq.schema();
    let fields = schema.fields();

    let mut eq_classes = input_eq.classes().to_vec();
    for (column, columns) in alias_map {
        let mut find_match = false;
        for class in eq_classes.iter_mut() {
            // If `self.head` is invalidated in the new schema, update head
            // with this change `self.head` is not randomly assigned by one of the entries from `self.others`
            if is_column_invalid_in_new_schema(&class.head, fields) {
                if let Some(alias_col) = get_alias_column(&class.head, alias_map) {
                    class.head = alias_col;
                }
            }
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
    for class in eq_classes.iter_mut() {
        let columns_to_remove = class
            .iter()
            .filter(|column| is_column_invalid_in_new_schema(column, fields))
            .cloned()
            .collect::<Vec<_>>();
        for column in columns_to_remove {
            class.remove(&column);
        }
    }

    eq_classes.retain(|props| {
        props.len() > 1
            && !(props.len() == 2 && props.head.eq(props.others().iter().next().unwrap()))
    });

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
    // Get schema and fields of projection output
    let schema = output_eq.schema();
    let fields = schema.fields();

    let mut eq_classes = input_eq.classes().to_vec();
    let mut oeq_alias_map = vec![];
    for (column, columns) in columns_map {
        if is_column_invalid_in_new_schema(column, fields) {
            oeq_alias_map.push((column.clone(), columns[0].clone()));
        }
    }
    for class in eq_classes.iter_mut() {
        class.update_with_aliases(&oeq_alias_map, fields);
    }

    // Prune columns that no longer is in the schema from the OrderingEquivalenceProperties.
    for class in eq_classes.iter_mut() {
        let sort_exprs_to_remove = class
            .iter()
            .filter(|sort_exprs| {
                sort_exprs.iter().any(|sort_expr| {
                    let cols_in_expr = collect_columns(&sort_expr.expr);
                    // If any one of the columns, used in Expression is invalid, remove expression
                    // from ordering equivalences
                    cols_in_expr
                        .iter()
                        .any(|col| is_column_invalid_in_new_schema(col, fields))
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

/// Retrieves the ordering equivalence properties for a given schema and output ordering.
pub fn ordering_equivalence_properties_helper(
    schema: SchemaRef,
    eq_orderings: &[LexOrdering],
) -> OrderingEquivalenceProperties {
    let mut oep = OrderingEquivalenceProperties::new(schema);
    let first_ordering = if let Some(first) = eq_orderings.first() {
        first
    } else {
        // Return an empty OrderingEquivalenceProperties:
        return oep;
    };
    // First entry among eq_orderings is the head, skip it:
    for ordering in eq_orderings.iter().skip(1) {
        if !ordering.is_empty() {
            oep.add_equal_conditions((first_ordering, ordering))
        }
    }
    oep
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::Column;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::Result;

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
}
