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

use crate::expressions::{CastExpr, Column};
use crate::{LexOrdering, PhysicalExpr, PhysicalSortExpr, PhysicalSortRequirement};

use arrow::datatypes::SchemaRef;
use arrow_schema::Fields;

use crate::utils::collect_columns;
use datafusion_common::tree_node::{Transformed, TreeNode};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::ops::Range;
use std::sync::Arc;

/// Represents a collection of [`EquivalentClass`] (equivalences
/// between columns in relations)
///
/// This is used to represent both:
///
/// 1. Equality conditions (like `A=B`), when `T` = [`Column`]
/// 2. Ordering (like `A ASC = B ASC`), when `T` = [`PhysicalSortExpr`]
#[derive(Debug, Clone)]
pub struct EquivalenceProperties {
    classes: Vec<EquivalentClass<Column>>,
    schema: SchemaRef,
}

impl EquivalenceProperties {
    pub fn new(schema: SchemaRef) -> Self {
        EquivalenceProperties {
            classes: vec![],
            schema,
        }
    }

    /// return the set of equivalences
    pub fn classes(&self) -> &[EquivalentClass<Column>] {
        &self.classes
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Add the [`EquivalentClass`] from `iter` to this list
    pub fn extend<I: IntoIterator<Item = EquivalentClass<Column>>>(&mut self, iter: I) {
        for ec in iter {
            self.classes.push(ec)
        }
    }

    /// Adds new equal conditions into the EquivalenceProperties. New equal
    /// conditions usually come from equality predicates in a join/filter.
    pub fn add_equal_conditions(&mut self, new_conditions: (&Column, &Column)) {
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
                self.classes.push(EquivalentClass::<Column>::new(
                    new_conditions.0.clone(),
                    vec![new_conditions.1.clone()],
                ));
            }
            _ => {}
        }
    }

    pub fn normalize_expr(&self, expr: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        expr.clone()
            .transform(&|expr| {
                let normalized_form =
                    expr.as_any().downcast_ref::<Column>().and_then(|column| {
                        for class in &self.classes {
                            if class.contains(column) {
                                return Some(Arc::new(class.head().clone()) as _);
                            }
                        }
                        None
                    });
                Ok(if let Some(normalized_form) = normalized_form {
                    Transformed::Yes(normalized_form)
                } else {
                    Transformed::No(expr)
                })
            })
            .unwrap_or(expr)
    }

    pub fn normalize_exprs(
        &self,
        exprs: &[Arc<dyn PhysicalExpr>],
    ) -> Vec<Arc<dyn PhysicalExpr>> {
        exprs
            .iter()
            .map(|expr| self.normalize_expr(expr.clone()))
            .collect::<Vec<_>>()
    }

    pub fn normalize_sort_requirement(
        &self,
        mut sort_requirement: PhysicalSortRequirement,
    ) -> PhysicalSortRequirement {
        sort_requirement.expr = self.normalize_expr(sort_requirement.expr);
        sort_requirement
    }

    pub fn normalize_sort_requirements(
        &self,
        sort_reqs: &[PhysicalSortRequirement],
    ) -> Vec<PhysicalSortRequirement> {
        let normalized_sort_reqs = sort_reqs
            .iter()
            .map(|sort_req| self.normalize_sort_requirement(sort_req.clone()))
            .collect::<Vec<_>>();
        collapse_vec(normalized_sort_reqs)
    }

    pub fn normalize_sort_exprs(
        &self,
        sort_exprs: &[PhysicalSortExpr],
    ) -> Vec<PhysicalSortExpr> {
        let sort_requirements =
            PhysicalSortRequirement::from_sort_exprs(sort_exprs.iter());
        let normalized_sort_requirement =
            self.normalize_sort_requirements(&sort_requirements);
        PhysicalSortRequirement::to_sort_exprs(normalized_sort_requirement)
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
#[derive(Debug, Clone)]
pub struct OrderingEquivalenceProperties {
    oeq_class: Option<OrderingEquivalentClass>,
    schema: SchemaRef,
}

impl OrderingEquivalenceProperties {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            oeq_class: None,
            schema,
        }
    }
}

impl OrderingEquivalenceProperties {
    pub fn extend(&mut self, other: Option<OrderingEquivalentClass>) {
        if let Some(other) = other {
            if let Some(class) = &mut self.oeq_class {
                class.others.insert(other.head);
                class.others.extend(other.others);
            } else {
                self.oeq_class = Some(other);
            }
        }
    }

    pub fn oeq_class(&self) -> Option<&OrderingEquivalentClass> {
        self.oeq_class.as_ref()
    }

    /// Adds new equal conditions into the EquivalenceProperties. New equal
    /// conditions usually come from equality predicates in a join/filter.
    pub fn add_equal_conditions(&mut self, new_conditions: (&LexOrdering, &LexOrdering)) {
        if let Some(class) = &mut self.oeq_class {
            class.insert(new_conditions.0.clone());
            class.insert(new_conditions.1.clone());
        } else {
            let head = new_conditions.0.clone();
            let others = vec![new_conditions.1.clone()];
            self.oeq_class = Some(OrderingEquivalentClass::new(head, others))
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn normalize_sort_requirements(
        &self,
        sort_reqs: &[PhysicalSortRequirement],
        is_aggressive: bool,
    ) -> Vec<PhysicalSortRequirement> {
        let mut normalized_sort_reqs = sort_reqs.to_vec();
        if let Some(oeq_class) = &self.oeq_class {
            for item in oeq_class.others() {
                let item = PhysicalSortRequirement::from_sort_exprs(item);
                let ranges =
                    get_compatible_ranges(&normalized_sort_reqs, &item, is_aggressive);
                let mut offset: i64 = 0;
                for Range { start, end } in ranges {
                    let mut head = oeq_class
                        .head()
                        .clone()
                        .into_iter()
                        .map(|elem| elem.into())
                        .collect::<Vec<PhysicalSortRequirement>>();
                    let updated_start = (start as i64 + offset) as usize;
                    let updated_end = (end as i64 + offset) as usize;
                    let range = end - start;
                    offset += head.len() as i64 - range as i64;
                    let all_none = normalized_sort_reqs[updated_start..updated_end]
                        .iter()
                        .all(|req| req.options.is_none());
                    if all_none {
                        for req in head.iter_mut() {
                            req.options = None;
                        }
                    }
                    normalized_sort_reqs.splice(updated_start..updated_end, head);
                }
            }
        }
        collapse_vec(normalized_sort_reqs)
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
            .extend(new_ordering_eq_properties.oeq_class().cloned());
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
            let normalized = self.eq_properties.normalize_expr(item.expr.clone());
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
    // Get schema and fields of projection output
    let schema = output_eq.schema();
    let fields = schema.fields();

    let oeq_class = input_eq.oeq_class();
    let mut oeq_class = if let Some(oeq_class) = oeq_class {
        oeq_class.clone()
    } else {
        return;
    };
    let mut oeq_alias_map = vec![];
    for (column, columns) in columns_map {
        if is_column_invalid_in_new_schema(column, fields) {
            oeq_alias_map.push((column.clone(), columns[0].clone()));
        }
    }
    oeq_class.update_with_aliases(&oeq_alias_map, fields);

    // Prune columns that no longer is in the schema from from the OrderingEquivalenceProperties.
    let sort_exprs_to_remove = oeq_class
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
        oeq_class.remove(&sort_exprs);
    }
    if oeq_class.len() > 1 {
        output_eq.extend(Some(oeq_class));
    }
}

/// Update `ordering` if it contains cast expression with target column
/// after projection, if there is no cast expression among `ordering` expressions,
/// returns `None`.
fn update_with_cast_exprs(
    cast_exprs: &[(CastExpr, Column)],
    mut ordering: LexOrdering,
) -> Option<LexOrdering> {
    let mut is_changed = false;
    for sort_expr in ordering.iter_mut() {
        for (cast_expr, target_col) in cast_exprs.iter() {
            if sort_expr.expr.eq(cast_expr.expr()) {
                sort_expr.expr = Arc::new(target_col.clone()) as _;
                is_changed = true;
            }
        }
    }
    is_changed.then_some(ordering)
}

/// Update cast expressions inside ordering equivalence
/// properties with its target column after projection
pub fn update_ordering_equivalence_with_cast(
    cast_exprs: &[(CastExpr, Column)],
    input_oeq: &mut OrderingEquivalenceProperties,
) {
    if let Some(cls) = &mut input_oeq.oeq_class {
        for ordering in
            std::iter::once(cls.head().clone()).chain(cls.others().clone().into_iter())
        {
            if let Some(updated_ordering) = update_with_cast_exprs(cast_exprs, ordering) {
                cls.insert(updated_ordering);
            }
        }
    }
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

/// This function constructs a duplicate-free vector by filtering out duplicate
/// entries inside the given vector `input`.
fn collapse_vec<T: PartialEq>(input: Vec<T>) -> Vec<T> {
    let mut output = vec![];
    for item in input {
        if !output.contains(&item) {
            output.push(item);
        }
    }
    output
}

/// This function searches for the slice `section` inside the slice `given`.
/// It returns each range where `section` is compatible with the corresponding
/// slice in `given`.
fn get_compatible_ranges(
    given: &[PhysicalSortRequirement],
    section: &[PhysicalSortRequirement],
    is_aggressive: bool,
) -> Vec<Range<usize>> {
    if is_aggressive {
        // println!("given: {:?}", given);
        // println!("section: {:?}", section);
        let mut res = vec![];
        for i in 0..given.len() {
            let mut count = 0;
            while i + count < given.len() && count < section.len() && given[i + count] == section[count] {
                count += 1;
            }
            if count > 0 {
                res.push(Range {
                    start: i,
                    end: i + count,
                })
            }
        }
        res
    } else {
        let n_section = section.len();
        let n_end = if given.len() >= n_section {
            given.len() - n_section + 1
        } else {
            0
        };
        (0..n_end)
            .filter_map(|idx| {
                let end = idx + n_section;
                given[idx..end]
                    .iter()
                    .zip(section)
                    .all(|(req, given)| given.compatible(req))
                    .then_some(Range { start: idx, end })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::Column;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::Result;

    use arrow_schema::SortOptions;
    use std::sync::Arc;

    fn convert_to_requirement(
        in_data: &[(&Column, Option<SortOptions>)],
    ) -> Vec<PhysicalSortRequirement> {
        in_data
            .iter()
            .map(|(col, options)| {
                PhysicalSortRequirement::new(Arc::new((*col).clone()) as _, *options)
            })
            .collect::<Vec<_>>()
    }

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
    fn test_collapse_vec() -> Result<()> {
        assert_eq!(collapse_vec(vec![1, 2, 3]), vec![1, 2, 3]);
        assert_eq!(collapse_vec(vec![1, 2, 3, 2, 3]), vec![1, 2, 3]);
        assert_eq!(collapse_vec(vec![3, 1, 2, 3, 2, 3]), vec![3, 1, 2]);
        Ok(())
    }

    #[test]
    fn test_get_compatible_ranges() -> Result<()> {
        let col_a = &Column::new("a", 0);
        let col_b = &Column::new("b", 1);
        let option1 = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let test_data = vec![
            (
                vec![(col_a, Some(option1)), (col_b, Some(option1))],
                vec![(col_a, Some(option1))],
                vec![(0, 1)],
            ),
            (
                vec![(col_a, None), (col_b, Some(option1))],
                vec![(col_a, Some(option1))],
                vec![(0, 1)],
            ),
            (
                vec![
                    (col_a, None),
                    (col_b, Some(option1)),
                    (col_a, Some(option1)),
                ],
                vec![(col_a, Some(option1))],
                vec![(0, 1), (2, 3)],
            ),
        ];
        for (searched, to_search, expected) in test_data {
            let searched = convert_to_requirement(&searched);
            let to_search = convert_to_requirement(&to_search);
            let expected = expected
                .into_iter()
                .map(|(start, end)| Range { start, end })
                .collect::<Vec<_>>();
            assert_eq!(
                get_compatible_ranges(&searched, &to_search, false),
                expected
            );
        }
        Ok(())
    }
}
