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
use crate::utils::{collect_columns, get_indices_of_matching_exprs, merge_vectors};
use crate::{
    LexOrdering, LexOrderingRef, LexOrderingReq, PhysicalExpr, PhysicalSortExpr,
    PhysicalSortRequirement,
};

use arrow::datatypes::SchemaRef;
use arrow_schema::{Fields, SortOptions};

use crate::sort_properties::{ExprOrdering, SortProperties};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::utils::longest_consecutive_prefix;
use datafusion_common::{JoinType, Result};
use itertools::izip;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::ops::Range;
use std::sync::Arc;

// /// Represents a collection of [`EquivalentClass`] (equivalences
// /// between columns in relations)
// ///
// /// This is used to represent:
// ///
// /// 1. Equality conditions (like `A=B`), when `T` = [`Column`]
// #[derive(Debug, Clone)]
// pub struct EquivalenceProperties {
//     classes: Vec<EquivalentClass<Column>>,
//     schema: SchemaRef,
// }
//
// impl EquivalenceProperties {
//     pub fn new(schema: SchemaRef) -> Self {
//         EquivalenceProperties {
//             classes: vec![],
//             schema,
//         }
//     }
//
//     /// return the set of equivalences
//     pub fn classes(&self) -> &[EquivalentClass<Column>] {
//         &self.classes
//     }
//
//     pub fn schema(&self) -> SchemaRef {
//         self.schema.clone()
//     }
//
//     /// Add the [`EquivalentClass`] from `iter` to this list
//     pub fn extend<I: IntoIterator<Item = EquivalentClass<Column>>>(&mut self, iter: I) {
//         for ec in iter {
//             self.classes.push(ec)
//         }
//     }
//
//     /// Adds new equal conditions into the EquivalenceProperties. New equal
//     /// conditions usually come from equality predicates in a join/filter.
//     pub fn add_equal_conditions(&mut self, new_conditions: (&Column, &Column)) {
//         let mut idx1: Option<usize> = None;
//         let mut idx2: Option<usize> = None;
//         for (idx, class) in self.classes.iter_mut().enumerate() {
//             let contains_first = class.contains(new_conditions.0);
//             let contains_second = class.contains(new_conditions.1);
//             match (contains_first, contains_second) {
//                 (true, false) => {
//                     class.insert(new_conditions.1.clone());
//                     idx1 = Some(idx);
//                 }
//                 (false, true) => {
//                     class.insert(new_conditions.0.clone());
//                     idx2 = Some(idx);
//                 }
//                 (true, true) => {
//                     idx1 = Some(idx);
//                     idx2 = Some(idx);
//                     break;
//                 }
//                 (false, false) => {}
//             }
//         }
//
//         match (idx1, idx2) {
//             (Some(idx_1), Some(idx_2)) if idx_1 != idx_2 => {
//                 // need to merge the two existing EquivalentClasses
//                 let second_eq_class = self.classes.get(idx_2).unwrap().clone();
//                 let first_eq_class = self.classes.get_mut(idx_1).unwrap();
//                 for prop in second_eq_class.iter() {
//                     if !first_eq_class.contains(prop) {
//                         first_eq_class.insert(prop.clone());
//                     }
//                 }
//                 self.classes.remove(idx_2);
//             }
//             (None, None) => {
//                 // adding new pairs
//                 self.classes.push(EquivalentClass::<Column>::new(
//                     new_conditions.0.clone(),
//                     vec![new_conditions.1.clone()],
//                 ));
//             }
//             _ => {}
//         }
//     }
//
//     /// Normalizes physical expression according to `EquivalentClass`es inside `self.classes`.
//     /// expression is replaced with `EquivalentClass::head` expression if it is among `EquivalentClass::others`.
//     pub fn normalize_expr(&self, expr: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
//         expr.clone()
//             .transform(&|expr| {
//                 let normalized_form =
//                     expr.as_any().downcast_ref::<Column>().and_then(|column| {
//                         for class in &self.classes {
//                             if class.contains(column) {
//                                 return Some(Arc::new(class.head().clone()) as _);
//                             }
//                         }
//                         None
//                     });
//                 Ok(if let Some(normalized_form) = normalized_form {
//                     Transformed::Yes(normalized_form)
//                 } else {
//                     Transformed::No(expr)
//                 })
//             })
//             .unwrap_or(expr)
//     }
//
//     /// This function applies the \[`normalize_expr`]
//     /// function for all expression in `exprs` and returns a vector of
//     /// normalized physical expressions.
//     pub fn normalize_exprs(
//         &self,
//         exprs: &[Arc<dyn PhysicalExpr>],
//     ) -> Vec<Arc<dyn PhysicalExpr>> {
//         exprs
//             .iter()
//             .map(|expr| self.normalize_expr(expr.clone()))
//             .collect::<Vec<_>>()
//     }
//
//     /// This function normalizes `sort_requirement` according to `EquivalenceClasses` in the `self`.
//     /// If the given sort requirement doesn't belong to equivalence set inside
//     /// `self`, it returns `sort_requirement` as is.
//     pub fn normalize_sort_requirement(
//         &self,
//         mut sort_requirement: PhysicalSortRequirement,
//     ) -> PhysicalSortRequirement {
//         sort_requirement.expr = self.normalize_expr(sort_requirement.expr);
//         sort_requirement
//     }
//
//     /// This function applies the \[`normalize_sort_requirement`]
//     /// function for all sort requirements in `sort_reqs` and returns a vector of
//     /// normalized sort expressions.
//     pub fn normalize_sort_requirements(
//         &self,
//         sort_reqs: &[PhysicalSortRequirement],
//     ) -> Vec<PhysicalSortRequirement> {
//         let normalized_sort_reqs = sort_reqs
//             .iter()
//             .map(|sort_req| self.normalize_sort_requirement(sort_req.clone()))
//             .collect::<Vec<_>>();
//         collapse_vec(normalized_sort_reqs)
//     }
//
//     /// Similar to the \[`normalize_sort_requirements`] this function normalizes
//     /// sort expressions in `sort_exprs` and returns a vector of
//     /// normalized sort expressions.
//     pub fn normalize_sort_exprs(
//         &self,
//         sort_exprs: &[PhysicalSortExpr],
//     ) -> Vec<PhysicalSortExpr> {
//         let sort_requirements =
//             PhysicalSortRequirement::from_sort_exprs(sort_exprs.iter());
//         let normalized_sort_requirement =
//             self.normalize_sort_requirements(&sort_requirements);
//         PhysicalSortRequirement::to_sort_exprs(normalized_sort_requirement)
//     }
// }

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
    eq_classes: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    oeq_class: Option<OrderingEquivalentClass>,
    /// Keeps track of expressions that have constant value.
    constants: Vec<Arc<dyn PhysicalExpr>>,
    schema: SchemaRef,
}

impl OrderingEquivalenceProperties {
    /// Create an empty `OrderingEquivalenceProperties`
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            eq_classes: vec![],
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

    pub fn eq_classes(&self) -> &[Vec<Arc<dyn PhysicalExpr>>] {
        &self.eq_classes
    }

    /// Adds new equal conditions into the EquivalenceProperties. New equal
    /// conditions usually come from equality predicates in a join/filter.
    pub fn add_ordering_equal_conditions(
        &mut self,
        new_conditions: (&LexOrdering, &LexOrdering),
    ) {
        if let Some(class) = &mut self.oeq_class {
            class.insert(new_conditions.0.clone());
            class.insert(new_conditions.1.clone());
        } else {
            let head = new_conditions.0.clone();
            let others = vec![new_conditions.1.clone()];
            self.oeq_class = Some(OrderingEquivalentClass::new(head, others))
        }
    }

    pub fn add_equal_conditions(
        &mut self,
        new_conditions: (&Arc<dyn PhysicalExpr>, &Arc<dyn PhysicalExpr>),
    ) {
        let (first, second) = new_conditions;
        let mut added_to_existing_equalities = false;
        self.eq_classes.iter_mut().for_each(|eq_class| {
            if physical_exprs_contains(eq_class, first)
                && !physical_exprs_contains(eq_class, second)
            {
                eq_class.push(second.clone());
                added_to_existing_equalities = true;
            }
        });
        if !added_to_existing_equalities && !first.eq(second) {
            self.eq_classes.push(vec![first.clone(), second.clone()]);
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

    fn normalize_with_eq_classes(
        &self,
        sort_reqs: &[PhysicalSortRequirement],
    ) -> Vec<PhysicalSortRequirement> {
        // println!("sort_reqs: {:?}", sort_reqs);
        // println!("self.eq_classes: {:?}", self.eq_classes);
        let normalized_sort_reqs = sort_reqs
            .iter()
            .map(|sort_req| {
                for eq_class in &self.eq_classes {
                    if physical_exprs_contains(eq_class, &sort_req.expr) {
                        return PhysicalSortRequirement {
                            expr: eq_class[0].clone(),
                            options: sort_req.options,
                        };
                    }
                }
                sort_req.clone()
            })
            .collect::<Vec<_>>();
        let normalized_sort_reqs =
            prune_sort_reqs_with_constants(&normalized_sort_reqs, &self.constants);
        normalized_sort_reqs
    }

    /// This function normalizes `sort_reqs` by
    /// - removing expressions that have constant value from requirement
    /// - replacing sections that are in the `self.oeq_class.others` with `self.oeq_class.head`
    /// - removing sections that satisfies global ordering that are in the post fix of requirement
    pub fn normalize_sort_requirements(
        &self,
        sort_reqs: &[PhysicalSortRequirement],
    ) -> Vec<PhysicalSortRequirement> {
        let normalized_sort_reqs = self.normalize_with_eq_classes(sort_reqs);
        let mut normalized_sort_reqs = collapse_lex_req(normalized_sort_reqs);
        if let Some(oeq_class) = &self.oeq_class {
            for item in oeq_class.others() {
                let item = PhysicalSortRequirement::from_sort_exprs(item);
                let item = self.normalize_with_eq_classes(&item);
                let item = prune_sort_reqs_with_constants(&item, &self.constants);
                let ranges = get_compatible_ranges(&normalized_sort_reqs, &item);
                let mut offset: i64 = 0;
                for Range { start, end } in ranges {
                    let head = PhysicalSortRequirement::from_sort_exprs(oeq_class.head());
                    let head = self.normalize_with_eq_classes(&head);
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

    /// Normalizes physical expression according to `EquivalentClass`es inside `self.classes`.
    /// expression is replaced with `EquivalentClass::head` expression if it is among `EquivalentClass::others`.
    pub fn normalize_expr(&self, expr: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        expr.clone()
            .transform(&|expr| {
                for class in self.eq_classes() {
                    if physical_exprs_contains(class, &expr) {
                        return Ok(Transformed::Yes(class[0].clone()));
                    }
                }
                Ok(Transformed::No(expr))
            })
            .unwrap_or(expr)
    }

    pub fn normalize_sort_expr(&self, sort_expr: PhysicalSortExpr) -> PhysicalSortExpr {
        let PhysicalSortExpr { expr, options } = sort_expr;
        let new_expr = self.normalize_expr(expr);
        PhysicalSortExpr {
            expr: new_expr,
            options,
        }
    }

    pub fn normalize_exprs(
        &self,
        exprs: &[Arc<dyn PhysicalExpr>],
    ) -> Vec<Arc<dyn PhysicalExpr>> {
        let res = exprs
            .iter()
            .map(|expr| self.normalize_expr(expr.clone()))
            .collect::<Vec<_>>();
        // TODO: Add deduplication check here after normalization
        res
    }

    pub fn normalize_sort_exprs(
        &self,
        sort_exprs: &[PhysicalSortExpr],
    ) -> Vec<PhysicalSortExpr> {
        let res = sort_exprs
            .iter()
            .map(|sort_expr| self.normalize_sort_expr(sort_expr.clone()))
            .collect::<Vec<_>>();
        // TODO: Add deduplication check here after normalization
        res
    }

    fn get_aliased_expr(
        alias_map: &HashMap<Column, Vec<Column>>,
        source_to_target_mapping: &[(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)],
        expr: &Arc<dyn PhysicalExpr>,
    ) -> Option<Arc<dyn PhysicalExpr>> {
        for (source, target) in source_to_target_mapping {
            if expr.eq(source) {
                return Some(target.clone());
            }
        }
        // for (column, columns) in alias_map {
        //     let column_expr = Arc::new(column.clone()) as Arc<dyn PhysicalExpr>;
        //     // println!("column_expr:{:?}, expr:{:?}",column_expr, expr);
        //     if column_expr.eq(expr) {
        //         // println!("return some");
        //         return Some(Arc::new(columns[0].clone()));
        //     }
        // }
        None
    }

    fn get_eq_class_group(
        old_eq_class: &[Vec<Arc<dyn PhysicalExpr>>],
        expr: &Arc<dyn PhysicalExpr>,
    ) -> Option<Vec<Arc<dyn PhysicalExpr>>> {
        for eq_class in old_eq_class {
            if physical_exprs_contains(eq_class, expr) {
                return Some(eq_class.to_vec());
            }
        }
        None
    }

    fn get_corresponding_expr(
        old_eq_class: &[Vec<Arc<dyn PhysicalExpr>>],
        new_eq_class: &[Option<Vec<Arc<dyn PhysicalExpr>>>],
        alias_map: &HashMap<Column, Vec<Column>>,
        source_to_target_mapping: &[(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)],
        expr: &Arc<dyn PhysicalExpr>,
    ) -> Option<Arc<dyn PhysicalExpr>> {
        assert_eq!(old_eq_class.len(), new_eq_class.len());
        // for (old, new) in izip!(old_eq_class.iter(), new_eq_class.iter()) {
        //     if let (true, Some(new_eq)) = (physical_exprs_contains(old, expr), new) {
        //         return Some(new_eq[0].clone());
        //     }
        // }
        // for (column, columns) in alias_map {
        //     let column_expr = Arc::new(column.clone()) as Arc<dyn PhysicalExpr>;
        //     if column_expr.eq(expr) {
        //         return Some(Arc::new(columns[0].clone()));
        //     }
        // }
        let children = expr.children();
        if children.is_empty() {
            for (source, target) in source_to_target_mapping.iter() {
                if source.eq(expr)
                    || old_eq_class
                        .iter()
                        .any(|eq_class| eq_class.iter().any(|item| item.eq(source)))
                {
                    return Some(target.clone());
                } else if let Some(group) = Self::get_eq_class_group(old_eq_class, source)
                {
                    if physical_exprs_contains(&group, expr) {
                        return Some(target.clone());
                    }
                }
            }
            None
        } else if let Some(children) = children
            .into_iter()
            .map(|child| {
                Self::get_corresponding_expr(
                    old_eq_class,
                    new_eq_class,
                    alias_map,
                    source_to_target_mapping,
                    &child,
                )
            })
            .collect::<Option<Vec<_>>>()
        {
            Some(expr.clone().with_new_children(children).unwrap())
        } else {
            None
        }

        // for (source, target) in source_to_target_mapping.iter(){
        //
        //     if source.eq(expr) || old_eq_class.iter().any(|eq_class| eq_class.iter().any(|item| item.eq(source))){
        //         return Some(target.clone())
        //     } else if let Some(group) = Self::get_eq_class_group(old_eq_class, source){
        //         if physical_exprs_contains(&group, expr){
        //             return Some(target.clone())
        //         }
        //     }
        // }
        // None
    }

    fn get_projected_ordering(
        old_eq_class: &[Vec<Arc<dyn PhysicalExpr>>],
        new_eq_class: &[Option<Vec<Arc<dyn PhysicalExpr>>>],
        alias_map: &HashMap<Column, Vec<Column>>,
        source_to_target_mapping: &[(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)],
        ordering: &[PhysicalSortExpr],
    ) -> Option<Vec<PhysicalSortExpr>> {
        // println!("old_eq_class: {:?}", old_eq_class);
        // println!("new_eq_class: {:?}", new_eq_class);
        // println!("ordering: {:?}", ordering);
        let mut res = vec![];
        for order in ordering {
            // println!("order.expr:{:?}", order.expr);
            if let Some(new_expr) = Self::get_corresponding_expr(
                old_eq_class,
                new_eq_class,
                alias_map,
                source_to_target_mapping,
                &order.expr,
            ) {
                // println!("new_expr:{:?}", new_expr);
                res.push(PhysicalSortExpr {
                    expr: new_expr,
                    options: order.options,
                })
            } else {
                break;
            }
        }
        if res.is_empty() {
            None
        } else {
            Some(res)
        }
    }

    fn get_equivalent_groups(
        source_to_target_mapping: &[(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)],
    ) -> Vec<Vec<Arc<dyn PhysicalExpr>>> {
        let mut res = vec![];
        for (source, target) in source_to_target_mapping {
            if res.is_empty() {
                res.push((source, vec![target.clone()]));
            }
            if let Some(idx) = res.iter_mut().position(|(key, _values)| key.eq(source)) {
                let (_, values) = &mut res[idx];
                if !physical_exprs_contains(values, target) {
                    values.push(target.clone());
                }
            }
        }
        res.into_iter()
            .filter_map(
                |(_, values)| {
                    if values.len() > 1 {
                        Some(values)
                    } else {
                        None
                    }
                },
            )
            .collect()
        // vec![]
    }

    pub fn project(
        &self,
        alias_map: &HashMap<Column, Vec<Column>>,
        source_to_target_mapping: &[(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)],
        output_schema: SchemaRef,
    ) -> OrderingEquivalenceProperties {
        // println!("alias_map: {:?}", alias_map);
        // println!("source_to_target_mapping: {:?}", source_to_target_mapping);
        let mut res = OrderingEquivalenceProperties::new(output_schema);

        let mut new_eq_classes = vec![];
        let mut new_eq_classes2 = vec![];
        for eq_class in &self.eq_classes {
            let new_eq_class = eq_class
                .iter()
                .filter_map(|expr| {
                    Self::get_aliased_expr(alias_map, source_to_target_mapping, expr)
                })
                .collect::<Vec<_>>();
            // println!("new_eq_class:{:?}", new_eq_class);
            if new_eq_class.len() > 1 {
                new_eq_classes.push(new_eq_class.clone());
            }
            if new_eq_class.is_empty() {
                new_eq_classes2.push(None);
            } else {
                new_eq_classes2.push(Some(new_eq_class));
            }
        }
        let new_classes = Self::get_equivalent_groups(source_to_target_mapping);
        // println!("new_classes alias group:{:?}", new_classes);
        // TODO: Add check for redundant group
        // combine groups with common entries
        new_eq_classes.extend(new_classes);
        res.eq_classes = new_eq_classes;

        if let Some(oeq_class) = &self.oeq_class {
            // println!("old oeq class: {:?}", oeq_class);
            let new_ordering = oeq_class
                .iter()
                .filter_map(|order| {
                    Self::get_projected_ordering(
                        &self.eq_classes,
                        &new_eq_classes2,
                        alias_map,
                        source_to_target_mapping,
                        order,
                    )
                })
                .collect::<Vec<_>>();
            // println!("new_ordering: {:?}", new_ordering);
            if !new_ordering.is_empty() {
                let head = new_ordering[0].clone();
                let others = new_ordering[1..].to_vec();
                res.oeq_class = Some(OrderingEquivalentClass::new(head, others));
            }
        }
        for (source, target) in source_to_target_mapping {
            let initial_expr = ExprOrdering::new(source.clone());
            let transformed = initial_expr
                .transform_up(&|expr| update_ordering(expr, &self))
                .unwrap();
            if let Some(SortProperties::Ordered(sort_options)) = transformed.state {
                let sort_expr = PhysicalSortExpr {
                    expr: target.clone(),
                    options: sort_options,
                };
                if let Some(oeq_class) = &mut res.oeq_class {
                    // println!("oeq_class before: {:?}", oeq_class);
                    oeq_class.add_new_ordering(&[sort_expr]);
                    // println!("oeq_class after: {:?}", oeq_class);
                    // oeq_class.others.insert(vec![sort_expr]);
                } else {
                    let head = vec![sort_expr];
                    res.oeq_class = Some(OrderingEquivalentClass::new(head, vec![]));
                }
            }
        }

        res
    }

    pub fn with_reorder(
        mut self,
        sort_expr: Vec<PhysicalSortExpr>,
    ) -> OrderingEquivalenceProperties {
        // TODO: In some cases, existing ordering equivalences may still be valid add this analysis
        // Equivalences and constants are still valid after reorder
        self.oeq_class = Some(OrderingEquivalentClass::new(sort_expr, vec![]));
        self
    }

    pub fn set_satisfy(
        &self,
        exprs: &[Arc<dyn PhysicalExpr>],
    ) -> Option<Vec<(usize, SortOptions)>> {
        let exprs_normalized = self.normalize_exprs(exprs);
        // println!("exprs: {:?}", exprs);
        // println!("exprs_normalized: {:?}", exprs_normalized);
        // println!("self.eq_classes: {:?}", self.eq_classes);
        // println!("self.oeq_class: {:?}", self.oeq_class);
        let mut best = vec![];
        if let Some(oeq_class) = &self.oeq_class {
            for ordering in oeq_class.iter() {
                let ordering = self.normalize_sort_exprs(ordering);
                let ordering_exprs = ordering
                    .iter()
                    .map(|sort_expr| sort_expr.expr.clone())
                    .collect::<Vec<_>>();
                // let ordering_exprs = self.normalize_exprs(&ordering);
                // println!("exprs_normalized: {:?}, normalized_ordering_exprs:{:?}", exprs_normalized, ordering_exprs);
                let mut ordered_indices =
                    get_indices_of_matching_exprs(&exprs_normalized, &ordering_exprs);
                // println!("ordered_indices: {:?}", ordered_indices);
                ordered_indices.sort();
                // Find out how many expressions of the existing ordering define ordering
                // for expressions in the GROUP BY clause. For example, if the input is
                // ordered by a, b, c, d and we group by b, a, d; the result below would be.
                // 2, meaning 2 elements (a, b) among the GROUP BY columns define ordering.
                let first_n = longest_consecutive_prefix(ordered_indices);
                if first_n > best.len() {
                    let ordered_exprs = ordering_exprs[0..first_n].to_vec();
                    // Find indices for the GROUP BY expressions such that when we iterate with
                    // these indices, we would match existing ordering. For the example above,
                    // this would produce 1, 0; meaning 1st and 0th entries (a, b) among the
                    // GROUP BY expressions b, a, d match input ordering.
                    let indices =
                        get_indices_of_matching_exprs(&ordered_exprs, &exprs_normalized);
                    // println!("indices:{:?}, ordered_exprs: {:?}, exprs_normalized:{:?}", indices, ordered_exprs, exprs_normalized);
                    best = indices
                        .iter()
                        .enumerate()
                        .map(|(order_idx, &match_idx)| {
                            (match_idx, ordering[order_idx].options)
                        })
                        .collect();
                }
            }
        }
        if best.is_empty() {
            None
        } else {
            Some(best)
        }
    }

    pub fn with_empty_ordering_equivalence(mut self) -> OrderingEquivalenceProperties {
        self.oeq_class = None;
        self
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

    // /// This function normalizes `OrderingEquivalenceProperties` according to `eq_properties`.
    // /// More explicitly, it makes sure that expressions in `oeq_class` are head entries
    // /// in `eq_properties`, replacing any non-head entries with head entries if necessary.
    // pub fn normalize_with_equivalence_properties(
    //     &self,
    //     eq_properties: &EquivalenceProperties,
    // ) -> OrderingEquivalentClass {
    //     let head = eq_properties.normalize_sort_exprs(self.head());
    //
    //     let others = self
    //         .others()
    //         .iter()
    //         .map(|other| eq_properties.normalize_sort_exprs(other))
    //         .collect();
    //
    //     EquivalentClass::new(head, others)
    // }

    /// Prefix with existing ordering.
    pub fn prefix_ordering_equivalent_class_with_existing_ordering(
        &self,
        existing_ordering: &[PhysicalSortExpr],
    ) -> OrderingEquivalentClass {
        // let existing_ordering = eq_properties.normalize_sort_exprs(existing_ordering);
        // let normalized_head = eq_properties.normalize_sort_exprs(self.head());
        let normalized_head = self.head();
        let updated_head = merge_vectors(&existing_ordering, &normalized_head);
        let updated_others = self
            .others()
            .iter()
            .map(|ordering| {
                // let normalized_ordering = eq_properties.normalize_sort_exprs(ordering);
                let normalized_ordering = ordering;
                merge_vectors(&existing_ordering, &normalized_ordering)
            })
            .collect();
        OrderingEquivalentClass::new(updated_head, updated_others)
    }

    fn get_finer(
        lhs: &[PhysicalSortExpr],
        rhs: &[PhysicalSortExpr],
    ) -> Option<Vec<PhysicalSortExpr>> {
        if izip!(lhs.iter(), rhs.iter()).all(|(lhs, rhs)| lhs.eq(rhs)) {
            if lhs.len() > rhs.len() {
                return Some(lhs.to_vec());
            } else {
                return Some(rhs.to_vec());
            }
        }
        None
    }

    fn add_new_ordering(&mut self, ordering: &[PhysicalSortExpr]) {
        let mut is_redundant = false;
        let mut new_res = vec![];
        for existing_ordering in self.iter() {
            if let Some(finer) = Self::get_finer(existing_ordering, ordering) {
                // existing_ordering = finer;
                new_res.push(finer);
                is_redundant = true;
            } else {
                new_res.push(existing_ordering.to_vec());
            }
        }
        if !is_redundant {
            new_res.push(ordering.to_vec());
        }
        let head = new_res[0].clone();
        let others = new_res[1..].to_vec();
        *self = OrderingEquivalentClass::new(head, others);
    }
}

/// This is a builder object facilitating incremental construction
/// for ordering equivalences.
pub struct OrderingEquivalenceBuilder {
    // eq_properties: EquivalenceProperties,
    ordering_eq_properties: OrderingEquivalenceProperties,
    existing_ordering: Vec<PhysicalSortExpr>,
    schema: SchemaRef,
}

impl OrderingEquivalenceBuilder {
    pub fn new(schema: SchemaRef) -> Self {
        // let eq_properties = EquivalenceProperties::new(schema.clone());
        let ordering_eq_properties = OrderingEquivalenceProperties::new(schema.clone());
        Self {
            // eq_properties,
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

    // pub fn with_equivalences(mut self, new_eq_properties: EquivalenceProperties) -> Self {
    //     self.eq_properties = new_eq_properties;
    //     self
    // }

    pub fn add_equal_conditions(
        &mut self,
        new_equivalent_ordering: Vec<PhysicalSortExpr>,
    ) {
        let mut normalized_out_ordering = vec![];
        for item in &self.existing_ordering {
            // To account for ordering equivalences, first normalize the expression:
            // let normalized = self.eq_properties.normalize_expr(item.expr.clone());
            normalized_out_ordering.push(PhysicalSortExpr {
                expr: item.expr.clone(),
                options: item.options,
            });
        }
        // If there is an existing ordering, add new ordering as an equivalence:
        if !normalized_out_ordering.is_empty() {
            self.ordering_eq_properties.add_ordering_equal_conditions((
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

// /// This function applies the given projection to the given equivalence
// /// properties to compute the resulting (projected) equivalence properties; e.g.
// /// 1) Adding an alias, which can introduce additional equivalence properties,
// ///    as in Projection(a, a as a1, a as a2).
// /// 2) Truncate the [`EquivalentClass`]es that are not in the output schema.
// pub fn project_equivalence_properties(
//     input_eq: EquivalenceProperties,
//     alias_map: &HashMap<Column, Vec<Column>>,
//     output_eq: &mut EquivalenceProperties,
// ) {
//     // Get schema and fields of projection output
//     let schema = output_eq.schema();
//     let fields = schema.fields();
//
//     let mut eq_classes = input_eq.classes().to_vec();
//     for (column, columns) in alias_map {
//         let mut find_match = false;
//         for class in eq_classes.iter_mut() {
//             // If `self.head` is invalidated in the new schema, update head
//             // with this change `self.head` is not randomly assigned by one of the entries from `self.others`
//             if is_column_invalid_in_new_schema(&class.head, fields) {
//                 if let Some(alias_col) = get_alias_column(&class.head, alias_map) {
//                     class.head = alias_col;
//                 }
//             }
//             if class.contains(column) {
//                 for col in columns {
//                     class.insert(col.clone());
//                 }
//                 find_match = true;
//                 break;
//             }
//         }
//         if !find_match {
//             eq_classes.push(EquivalentClass::new(column.clone(), columns.clone()));
//         }
//     }
//
//     // Prune columns that are no longer in the schema from equivalences.
//     for class in eq_classes.iter_mut() {
//         let columns_to_remove = class
//             .iter()
//             .filter(|column| is_column_invalid_in_new_schema(column, fields))
//             .cloned()
//             .collect::<Vec<_>>();
//         for column in columns_to_remove {
//             class.remove(&column);
//         }
//     }
//
//     eq_classes.retain(|props| {
//         props.len() > 1
//             &&
//             // A column should not give an equivalence with itself.
//              !(props.len() == 2 && props.head.eq(props.others().iter().next().unwrap()))
//     });
//
//     output_eq.extend(eq_classes);
// }

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
    if eq_orderings.is_empty() {
        // Return an empty OrderingEquivalenceProperties:
        return oep;
    } else {
        let head = eq_orderings[0].clone();
        let others = eq_orderings[1..].to_vec();
        oep.extend(Some(OrderingEquivalentClass::new(head, others)));
        return oep;
    }
    // oep.extend(Some(OrderingEquivalentClass::new()))
    // let first_ordering = if let Some(first) = eq_orderings.first() {
    //     first
    // } else {
    //     // Return an empty OrderingEquivalenceProperties:
    //     return oep;
    // };
    // // First entry among eq_orderings is the head, skip it:
    // for ordering in eq_orderings.iter().skip(1) {
    //     if !ordering.is_empty() {
    //         oep.add_ordering_equal_conditions((first_ordering, ordering))
    //     }
    // }
    // oep
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
    // println!("physical_exprs:{:?}, expr:{:?}", physical_exprs, expr);
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
pub(crate) fn add_offset_to_exprs(
    exprs: Vec<Arc<dyn PhysicalExpr>>,
    offset: usize,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    exprs
        .into_iter()
        .map(|item| add_offset_to_expr(item, offset))
        .collect::<Result<Vec<_>>>()
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

/// Calculates the [`SortProperties`] of a given [`ExprOrdering`] node.
/// The node is either a leaf node, or an intermediate node:
/// - If it is a leaf node, the children states are `None`. We directly find
/// the order of the node by looking at the given sort expression and equivalence
/// properties if it is a `Column` leaf, or we mark it as unordered. In the case
/// of a `Literal` leaf, we mark it as singleton so that it can cooperate with
/// some ordered columns at the upper steps.
/// - If it is an intermediate node, the children states matter. Each `PhysicalExpr`
/// and operator has its own rules about how to propagate the children orderings.
/// However, before the children order propagation, it is checked that whether
/// the intermediate node can be directly matched with the sort expression. If there
/// is a match, the sort expression emerges at that node immediately, discarding
/// the order coming from the children.
pub fn update_ordering(
    mut node: ExprOrdering,
    ordering_equal_properties: &OrderingEquivalenceProperties,
) -> Result<Transformed<ExprOrdering>> {
    if let Some(children_sort_options) = &node.children_states {
        // We have an intermediate (non-leaf) node, account for its children:
        node.state = Some(node.expr.get_ordering(children_sort_options));
        Ok(Transformed::Yes(node))
    } else if let Some(column) = node.expr.as_any().downcast_ref::<Column>() {
        // We have a Column, which is one of the two possible leaf node types:
        // TODO: Make this a method of ordering equivalence
        if let Some(oeq_class) = ordering_equal_properties.oeq_class() {
            for ordering in oeq_class.iter() {
                let global_ordering = &ordering[0];
                if node.expr.eq(&global_ordering.expr) {
                    node.state = Some(SortProperties::Ordered(global_ordering.options));
                    return Ok(Transformed::Yes(node));
                }
            }
        }
        node.state = None;
        Ok(Transformed::No(node))
    } else {
        // We have a Literal, which is the other possible leaf node type:
        node.state = Some(node.expr.get_ordering(&[]));
        Ok(Transformed::Yes(node))
    }
    // Ok(Transformed::Yes(node))
}

/// Combine equivalence properties of the given join inputs.
pub fn combine_join_equivalence_properties2(
    join_type: &JoinType,
    left_eq_classes: &[Vec<Arc<dyn PhysicalExpr>>],
    right_eq_classes: &[Vec<Arc<dyn PhysicalExpr>>],
    left_columns_len: usize,
    on: &[(Column, Column)],
    out_properties: &mut OrderingEquivalenceProperties,
) -> Result<()> {
    let mut res = vec![];
    match join_type {
        JoinType::Inner | JoinType::Left | JoinType::Full | JoinType::Right => {
            res.extend(left_eq_classes.to_vec());
            let updated_eq_classes = right_eq_classes
                .iter()
                .map(|eq_class| add_offset_to_exprs(eq_class.to_vec(), left_columns_len))
                .collect::<Result<Vec<_>>>()?;

            res.extend(updated_eq_classes);
        }
        JoinType::LeftSemi | JoinType::LeftAnti => {
            res.extend(left_eq_classes.to_vec());
        }
        JoinType::RightSemi | JoinType::RightAnti => {
            res.extend(right_eq_classes.to_vec());
        }
    }
    out_properties.eq_classes = res;
    if *join_type == JoinType::Inner {
        on.iter().for_each(|(lhs, rhs)| {
            let new_lhs = Arc::new(lhs.clone()) as _;
            let new_rhs =
                Arc::new(Column::new(rhs.name(), rhs.index() + left_columns_len)) as _;
            // (new_lhs, new_rhs)
            // println!("new_lhs: {:?}, new_rhs: {:?}", new_lhs, new_rhs);
            out_properties.add_equal_conditions((&new_lhs, &new_rhs));
        });
    }
    Ok(())

    // if join_type == JoinType::Inner {
    //     on.iter().for_each(|(column1, column2)| {
    //         let new_column2 =
    //             Column::new(column2.name(), left_columns_len + column2.index());
    //         new_properties.add_equal_conditions((column1, &new_column2))
    //     })
    // }
    // new_properties
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
}
