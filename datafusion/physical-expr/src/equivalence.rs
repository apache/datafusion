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
use crate::utils::{collect_columns, merge_vectors};
use crate::{
    LexOrdering, LexOrderingRef, LexOrderingReq, PhysicalExpr, PhysicalSortExpr,
    PhysicalSortRequirement,
};

use arrow::datatypes::SchemaRef;
use arrow_schema::Fields;

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{plan_err, DataFusionError, JoinSide, JoinType, Result};
use datafusion_expr::UserDefinedLogicalNode;
use itertools::izip;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::ops::Range;
use std::sync::Arc;

/// Represents a collection of [`EquivalentClass`] (equivalences
/// between columns in relations)
///
/// This is used to represent:
///
/// 1. Equality conditions (like `A=B`), when `T` = [`Column`]
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

    /// Normalizes physical expression according to `EquivalentClass`es inside `self.classes`.
    /// expression is replaced with `EquivalentClass::head` expression if it is among `EquivalentClass::others`.
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

    /// This function applies the \[`normalize_expr`]
    /// function for all expression in `exprs` and returns a vector of
    /// normalized physical expressions.
    pub fn normalize_exprs(
        &self,
        exprs: &[Arc<dyn PhysicalExpr>],
    ) -> Vec<Arc<dyn PhysicalExpr>> {
        exprs
            .iter()
            .map(|expr| self.normalize_expr(expr.clone()))
            .collect::<Vec<_>>()
    }

    /// This function normalizes `sort_requirement` according to `EquivalenceClasses` in the `self`.
    /// If the given sort requirement doesn't belong to equivalence set inside
    /// `self`, it returns `sort_requirement` as is.
    pub fn normalize_sort_requirement(
        &self,
        mut sort_requirement: PhysicalSortRequirement,
    ) -> PhysicalSortRequirement {
        sort_requirement.expr = self.normalize_expr(sort_requirement.expr);
        sort_requirement
    }

    /// This function applies the \[`normalize_sort_requirement`]
    /// function for all sort requirements in `sort_reqs` and returns a vector of
    /// normalized sort expressions.
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

    /// Similar to the \[`normalize_sort_requirements`] this function normalizes
    /// sort expressions in `sort_exprs` and returns a vector of
    /// normalized sort expressions.
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
    /// Keeps track of expressions that have constant value.
    constants: Vec<Arc<dyn PhysicalExpr>>,
    schema: SchemaRef,
}

impl OrderingEquivalenceProperties {
    /// Create an empty `OrderingEquivalenceProperties`
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            oeq_class: None,
            constants: vec![],
            schema,
        }
    }

    /// Extends `OrderingEquivalenceProperties` by adding ordering inside the `other`
    /// to the `self.oeq_class`.
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

    /// Add physical expression that have constant value to the `self.constants`
    pub fn with_constants(mut self, constants: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        constants.into_iter().for_each(|constant| {
            if !physical_exprs_contains(&self.constants, &constant) {
                self.constants.push(constant);
            }
        });
        self
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// This function normalizes `sort_reqs` by
    /// - removing expressions that have constant value from requirement
    /// - replacing sections that are in the `self.oeq_class.others` with `self.oeq_class.head`
    /// - removing sections that satisfies global ordering that are in the post fix of requirement
    pub fn normalize_sort_requirements(
        &self,
        sort_reqs: &[PhysicalSortRequirement],
    ) -> Vec<PhysicalSortRequirement> {
        let normalized_sort_reqs =
            prune_sort_reqs_with_constants(sort_reqs, &self.constants);
        let mut normalized_sort_reqs = collapse_lex_req(normalized_sort_reqs);
        if let Some(oeq_class) = &self.oeq_class {
            for item in oeq_class.others() {
                let item = PhysicalSortRequirement::from_sort_exprs(item);
                let item = prune_sort_reqs_with_constants(&item, &self.constants);
                let ranges = get_compatible_ranges(&normalized_sort_reqs, &item);
                let mut offset: i64 = 0;
                for Range { start, end } in ranges {
                    let head = PhysicalSortRequirement::from_sort_exprs(oeq_class.head());
                    let mut head = prune_sort_reqs_with_constants(&head, &self.constants);
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
            normalized_sort_reqs = simplify_lex_req(normalized_sort_reqs, oeq_class);
        }
        collapse_lex_req(normalized_sort_reqs)
    }

    /// Checks whether `leading_ordering` is contained in any of the ordering
    /// equivalence classes.
    pub fn satisfies_leading_ordering(
        &self,
        leading_ordering: &PhysicalSortExpr,
    ) -> bool {
        if let Some(oeq_class) = &self.oeq_class {
            for ordering in oeq_class
                .others
                .iter()
                .chain(std::iter::once(&oeq_class.head))
            {
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

    pub fn into_iter(self) -> impl Iterator<Item = T> {
        std::iter::once(self.head).chain(self.others.into_iter())
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

    /// Adds `offset` value to the index of each expression inside `self.head` and `self.others`.
    pub fn add_offset(&self, offset: usize) -> Result<OrderingEquivalentClass> {
        let head = add_offset_to_lex_ordering(self.head(), offset)?;
        let others = self
            .others()
            .iter()
            .map(|ordering| add_offset_to_lex_ordering(ordering, offset))
            .collect::<Result<Vec<_>>>()?;
        Ok(OrderingEquivalentClass::new(head, others))
    }

    /// This function normalizes `OrderingEquivalenceProperties` according to `eq_properties`.
    /// More explicitly, it makes sure that expressions in `oeq_class` are head entries
    /// in `eq_properties`, replacing any non-head entries with head entries if necessary.
    pub fn normalize_with_equivalence_properties(
        &self,
        eq_properties: &EquivalenceProperties,
    ) -> OrderingEquivalentClass {
        let head = eq_properties.normalize_sort_exprs(self.head());

        let others = self
            .others()
            .iter()
            .map(|other| eq_properties.normalize_sort_exprs(other))
            .collect();

        EquivalentClass::new(head, others)
    }

    /// Prefix with existing ordering.
    pub fn prefix_ordering_equivalent_class_with_existing_ordering(
        &self,
        existing_ordering: &[PhysicalSortExpr],
        eq_properties: &EquivalenceProperties,
    ) -> OrderingEquivalentClass {
        let existing_ordering = eq_properties.normalize_sort_exprs(existing_ordering);
        let normalized_head = eq_properties.normalize_sort_exprs(self.head());
        let updated_head = merge_vectors(&existing_ordering, &normalized_head);
        let updated_others = self
            .others()
            .iter()
            .map(|ordering| {
                let normalized_ordering = eq_properties.normalize_sort_exprs(ordering);
                merge_vectors(&existing_ordering, &normalized_ordering)
            })
            .collect();
        OrderingEquivalentClass::new(updated_head, updated_others)
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

    eq_classes.retain(|props| {
        props.len() > 1
            &&
            // A column should not give an equivalence with itself.
             !(props.len() == 2 && props.head.eq(props.others().iter().next().unwrap()))
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

/// This function constructs a duplicate-free `LexOrderingReq` by filtering out duplicate
/// entries that have same physical expression inside the given vector `input`.
/// `vec![a Some(Asc), a Some(Desc)]` is collapsed to the `vec![a Some(Asc)]`. Since
/// when same expression is already seen before, following expressions are redundant.
fn collapse_lex_req(input: LexOrderingReq) -> LexOrderingReq {
    let mut output = vec![];
    for item in input {
        if !lex_req_contains(&output, &item) {
            output.push(item);
        }
    }
    output
}

/// Check whether `sort_req.expr` is among the expressions of `lex_req`.
fn lex_req_contains(
    lex_req: &[PhysicalSortRequirement],
    sort_req: &PhysicalSortRequirement,
) -> bool {
    for constant in lex_req {
        if constant.expr.eq(&sort_req.expr) {
            return true;
        }
    }
    false
}

/// This function simplifies lexicographical ordering requirement
/// inside `input` by removing postfix lexicographical requirements
/// that satisfy global ordering (occurs inside the ordering equivalent class)
fn simplify_lex_req(
    input: LexOrderingReq,
    oeq_class: &OrderingEquivalentClass,
) -> LexOrderingReq {
    let mut section = &input[..];
    loop {
        let n_prune = prune_last_n_that_is_in_oeq(section, oeq_class);
        // Cannot prune entries from the end of requirement
        if n_prune == 0 {
            break;
        }
        section = &section[0..section.len() - n_prune];
    }
    if section.is_empty() {
        PhysicalSortRequirement::from_sort_exprs(oeq_class.head())
    } else {
        section.to_vec()
    }
}

/// Determines how many entries from the end can be deleted.
/// Last n entry satisfies global ordering, hence having them
/// as postfix in the lexicographical requirement is unnecessary.
/// Assume requirement is [a ASC, b ASC, c ASC], also assume that
/// existing ordering is [c ASC, d ASC]. In this case, since [c ASC]
/// is satisfied by the existing ordering (e.g corresponding section is global ordering),
/// [c ASC] can be pruned from the requirement: [a ASC, b ASC, c ASC]. In this case,
/// this function will return 1, to indicate last element can be removed from the requirement
fn prune_last_n_that_is_in_oeq(
    input: &[PhysicalSortRequirement],
    oeq_class: &OrderingEquivalentClass,
) -> usize {
    let input_len = input.len();
    for ordering in std::iter::once(oeq_class.head()).chain(oeq_class.others().iter()) {
        let mut search_range = std::cmp::min(ordering.len(), input_len);
        while search_range > 0 {
            let req_section = &input[input_len - search_range..];
            // let given_section = &ordering[0..search_range];
            if req_satisfied(ordering, req_section) {
                return search_range;
            } else {
                search_range -= 1;
            }
        }
    }
    0
}

/// Checks whether given section satisfies req.
fn req_satisfied(given: LexOrderingRef, req: &[PhysicalSortRequirement]) -> bool {
    for (given, req) in izip!(given.iter(), req.iter()) {
        let PhysicalSortRequirement { expr, options } = req;
        if let Some(options) = options {
            if options != &given.options || !expr.eq(&given.expr) {
                return false;
            }
        } else if !expr.eq(&given.expr) {
            return false;
        }
    }
    true
}

/// Combine equivalence properties of the given join inputs.
pub fn combine_join_equivalence_properties(
    join_type: JoinType,
    left_properties: EquivalenceProperties,
    right_properties: EquivalenceProperties,
    left_columns_len: usize,
    on: &[(Column, Column)],
    schema: SchemaRef,
) -> EquivalenceProperties {
    let mut new_properties = EquivalenceProperties::new(schema);
    match join_type {
        JoinType::Inner | JoinType::Left | JoinType::Full | JoinType::Right => {
            new_properties.extend(left_properties.classes().to_vec());
            let new_right_properties = right_properties
                .classes()
                .iter()
                .map(|prop| {
                    let new_head = Column::new(
                        prop.head().name(),
                        left_columns_len + prop.head().index(),
                    );
                    let new_others = prop
                        .others()
                        .iter()
                        .map(|col| {
                            Column::new(col.name(), left_columns_len + col.index())
                        })
                        .collect::<Vec<_>>();
                    EquivalentClass::new(new_head, new_others)
                })
                .collect::<Vec<_>>();

            new_properties.extend(new_right_properties);
        }
        JoinType::LeftSemi | JoinType::LeftAnti => {
            new_properties.extend(left_properties.classes().to_vec())
        }
        JoinType::RightSemi | JoinType::RightAnti => {
            new_properties.extend(right_properties.classes().to_vec())
        }
    }

    if join_type == JoinType::Inner {
        on.iter().for_each(|(column1, column2)| {
            let new_column2 =
                Column::new(column2.name(), left_columns_len + column2.index());
            new_properties.add_equal_conditions((column1, &new_column2))
        })
    }
    new_properties
}

/// Calculate equivalence properties for the given cross join operation.
pub fn cross_join_equivalence_properties(
    left_properties: EquivalenceProperties,
    right_properties: EquivalenceProperties,
    left_columns_len: usize,
    schema: SchemaRef,
) -> EquivalenceProperties {
    let mut new_properties = EquivalenceProperties::new(schema);
    new_properties.extend(left_properties.classes().to_vec());
    let new_right_properties = right_properties
        .classes()
        .iter()
        .map(|prop| {
            let new_head =
                Column::new(prop.head().name(), left_columns_len + prop.head().index());
            let new_others = prop
                .others()
                .iter()
                .map(|col| Column::new(col.name(), left_columns_len + col.index()))
                .collect::<Vec<_>>();
            EquivalentClass::new(new_head, new_others)
        })
        .collect::<Vec<_>>();
    new_properties.extend(new_right_properties);
    new_properties
}

/// Update right table ordering equivalences so that:
/// - They point to valid indices at the output of the join schema, and
/// - They are normalized with respect to equivalence columns.
///
/// To do so, we increment column indices by the size of the left table when
/// join schema consists of a combination of left and right schema (Inner,
/// Left, Full, Right joins). Then, we normalize the sort expressions of
/// ordering equivalences one by one. We make sure that each expression in the
/// ordering equivalence is either:
/// - The head of the one of the equivalent classes, or
/// - Doesn't have an equivalent column.
///
/// This way; once we normalize an expression according to equivalence properties,
/// it can thereafter safely be used for ordering equivalence normalization.
fn get_updated_right_ordering_equivalent_class(
    join_type: &JoinType,
    right_oeq_class: &OrderingEquivalentClass,
    left_columns_len: usize,
    join_eq_properties: &EquivalenceProperties,
) -> Result<OrderingEquivalentClass> {
    match join_type {
        // In these modes, indices of the right schema should be offset by
        // the left table size.
        JoinType::Inner | JoinType::Left | JoinType::Full | JoinType::Right => {
            let right_oeq_class = right_oeq_class.add_offset(left_columns_len)?;
            return Ok(
                right_oeq_class.normalize_with_equivalence_properties(join_eq_properties)
            );
        }
        _ => {}
    };
    Ok(right_oeq_class.normalize_with_equivalence_properties(join_eq_properties))
}

/// Calculate ordering equivalence properties for the given join operation.
pub fn combine_join_ordering_equivalence_properties(
    join_type: &JoinType,
    left_oeq_properties: &OrderingEquivalenceProperties,
    right_oeq_properties: &OrderingEquivalenceProperties,
    schema: SchemaRef,
    maintains_input_order: &[bool],
    probe_side: Option<JoinSide>,
    join_eq_properties: EquivalenceProperties,
) -> Result<OrderingEquivalenceProperties> {
    let mut new_properties = OrderingEquivalenceProperties::new(schema);
    let left_columns_len = left_oeq_properties.schema().fields().len();
    // All joins have 2 children
    assert_eq!(maintains_input_order.len(), 2);
    let left_maintains = maintains_input_order[0];
    let right_maintains = maintains_input_order[1];
    match (left_maintains, right_maintains) {
        (true, true) => return plan_err!("Cannot maintain ordering of both sides"),
        (true, false) => {
            // In this special case, right side ordering can be prefixed with left side ordering.
            if let (
                Some(JoinSide::Left),
                JoinType::Inner,
                Some(left_oeq_class),
                Some(right_oeq_class),
            ) = (
                probe_side,
                join_type,
                left_oeq_properties.oeq_class(),
                right_oeq_properties.oeq_class(),
            ) {
                let updated_right_oeq = get_updated_right_ordering_equivalent_class(
                    join_type,
                    right_oeq_class,
                    left_columns_len,
                    &join_eq_properties,
                )?;

                // Right side ordering equivalence properties should be prepended with
                // those of the left side while constructing output ordering equivalence
                // properties since stream side is the left side.
                //
                // If the right table ordering equivalences contain `b ASC`, and the output
                // ordering of the left table is `a ASC`, then the ordering equivalence `b ASC`
                // for the right table should be converted to `a ASC, b ASC` before it is added
                // to the ordering equivalences of the join.
                let mut orderings = vec![];
                for left_ordering in left_oeq_class.iter() {
                    for right_ordering in updated_right_oeq.iter() {
                        let mut ordering = left_ordering.to_vec();
                        ordering.extend(right_ordering.to_vec());
                        let ordering_normalized =
                            join_eq_properties.normalize_sort_exprs(&ordering);
                        orderings.push(ordering_normalized);
                    }
                }
                if !orderings.is_empty() {
                    let head = orderings.swap_remove(0);
                    let new_oeq_class = OrderingEquivalentClass::new(head, orderings);
                    new_properties.extend(Some(new_oeq_class));
                }
            } else {
                new_properties.extend(left_oeq_properties.oeq_class().cloned());
            }
        }
        (false, true) => {
            let updated_right_oeq = right_oeq_properties
                .oeq_class()
                .map(|right_oeq_class| {
                    get_updated_right_ordering_equivalent_class(
                        join_type,
                        right_oeq_class,
                        left_columns_len,
                        &join_eq_properties,
                    )
                })
                .transpose()?;
            // In this special case, left side ordering can be prefixed with right side ordering.
            if let (
                Some(JoinSide::Right),
                JoinType::Inner,
                Some(left_oeq_class),
                Some(right_oeg_class),
            ) = (
                probe_side,
                join_type,
                left_oeq_properties.oeq_class(),
                &updated_right_oeq,
            ) {
                // Left side ordering equivalence properties should be prepended with
                // those of the right side while constructing output ordering equivalence
                // properties since stream side is the right side.
                //
                // If the right table ordering equivalences contain `b ASC`, and the output
                // ordering of the left table is `a ASC`, then the ordering equivalence `b ASC`
                // for the right table should be converted to `a ASC, b ASC` before it is added
                // to the ordering equivalences of the join.
                let mut orderings = vec![];
                for right_ordering in right_oeg_class.iter() {
                    for left_ordering in left_oeq_class.iter() {
                        let mut ordering = right_ordering.to_vec();
                        ordering.extend(left_ordering.to_vec());
                        let ordering_normalized =
                            join_eq_properties.normalize_sort_exprs(&ordering);
                        orderings.push(ordering_normalized);
                    }
                }
                if !orderings.is_empty() {
                    let head = orderings.swap_remove(0);
                    let new_oeq_class = OrderingEquivalentClass::new(head, orderings);
                    new_properties.extend(Some(new_oeq_class));
                }
            } else {
                new_properties.extend(updated_right_oeq);
            }
        }
        (false, false) => {}
    }
    Ok(new_properties)
}

/// This function searches for the slice `section` inside the slice `given`.
/// It returns each range where `section` is compatible with the corresponding
/// slice in `given`.
fn get_compatible_ranges(
    given: &[PhysicalSortRequirement],
    section: &[PhysicalSortRequirement],
) -> Vec<Range<usize>> {
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

/// It is similar to contains method of vector.
/// Finds whether `expr` is among `physical_exprs`.
pub fn physical_exprs_contains(
    physical_exprs: &[Arc<dyn PhysicalExpr>],
    expr: &Arc<dyn PhysicalExpr>,
) -> bool {
    physical_exprs
        .iter()
        .any(|physical_expr| physical_expr.eq(expr))
}

/// Remove ordering requirements that have constant value
fn prune_sort_reqs_with_constants(
    ordering: &[PhysicalSortRequirement],
    constants: &[Arc<dyn PhysicalExpr>],
) -> Vec<PhysicalSortRequirement> {
    ordering
        .iter()
        .filter(|&order| !physical_exprs_contains(constants, &order.expr))
        .cloned()
        .collect()
}

/// Adds the `offset` value to `Column` indices inside `expr`. This function is
/// generally used during the update of the right table schema in join operations.
pub(crate) fn add_offset_to_expr(
    expr: Arc<dyn PhysicalExpr>,
    offset: usize,
) -> Result<Arc<dyn PhysicalExpr>> {
    expr.transform_down(&|e| match e.as_any().downcast_ref::<Column>() {
        Some(col) => Ok(Transformed::Yes(Arc::new(Column::new(
            col.name(),
            offset + col.index(),
        )))),
        None => Ok(Transformed::No(e)),
    })
}

/// Adds the `offset` value to `Column` indices inside `sort_expr.expr`.
pub(crate) fn add_offset_to_sort_expr(
    sort_expr: &PhysicalSortExpr,
    offset: usize,
) -> Result<PhysicalSortExpr> {
    Ok(PhysicalSortExpr {
        expr: add_offset_to_expr(sort_expr.expr.clone(), offset)?,
        options: sort_expr.options,
    })
}

/// Adds the `offset` value to `Column` indices for each `sort_expr.expr`
/// inside `sort_exprs`.
pub fn add_offset_to_lex_ordering(
    sort_exprs: LexOrderingRef,
    offset: usize,
) -> Result<LexOrdering> {
    sort_exprs
        .iter()
        .map(|sort_expr| add_offset_to_sort_expr(sort_expr, offset))
        .collect()
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
            assert_eq!(get_compatible_ranges(&searched, &to_search), expected);
        }
        Ok(())
    }

    #[test]
    fn test_get_updated_right_ordering_equivalence_properties() -> Result<()> {
        let join_type = JoinType::Inner;

        let options = SortOptions::default();
        let right_oeq_class = OrderingEquivalentClass::new(
            vec![
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("x", 0)),
                    options,
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("y", 1)),
                    options,
                },
            ],
            vec![vec![
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("z", 2)),
                    options,
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("w", 3)),
                    options,
                },
            ]],
        );

        let left_columns_len = 4;

        let fields: Fields = ["a", "b", "c", "d", "x", "y", "z", "w"]
            .into_iter()
            .map(|name| Field::new(name, DataType::Int32, true))
            .collect();

        let mut join_eq_properties =
            EquivalenceProperties::new(Arc::new(Schema::new(fields)));
        join_eq_properties
            .add_equal_conditions((&Column::new("a", 0), &Column::new("x", 4)));
        join_eq_properties
            .add_equal_conditions((&Column::new("d", 3), &Column::new("w", 7)));

        let result = get_updated_right_ordering_equivalent_class(
            &join_type,
            &right_oeq_class,
            left_columns_len,
            &join_eq_properties,
        )?;

        let expected = OrderingEquivalentClass::new(
            vec![
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("a", 0)),
                    options,
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("y", 5)),
                    options,
                },
            ],
            vec![vec![
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("z", 6)),
                    options,
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("d", 3)),
                    options,
                },
            ]],
        );

        assert_eq!(result.head(), expected.head());
        assert_eq!(result.others(), expected.others());

        Ok(())
    }
}
