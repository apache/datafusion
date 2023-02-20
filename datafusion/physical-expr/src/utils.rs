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

use crate::equivalence::EquivalentClass;
use crate::expressions::BinaryExpr;
use crate::expressions::Column;
use crate::expressions::UnKnownColumn;
use crate::rewrite::TreeNodeRewritable;
use crate::PhysicalSortExpr;
use crate::{EquivalenceProperties, PhysicalExpr, PhysicalSortRequirements};
use datafusion_expr::Operator;

use arrow::datatypes::SchemaRef;

use arrow_schema::SortOptions;
use std::collections::HashMap;
use std::sync::Arc;

/// Compare the two expr lists are equal no matter the order.
/// For example two InListExpr can be considered to be equals no matter the order:
///
/// In('a','b','c') == In('c','b','a')
pub fn expr_list_eq_any_order(
    list1: &[Arc<dyn PhysicalExpr>],
    list2: &[Arc<dyn PhysicalExpr>],
) -> bool {
    if list1.len() == list2.len() {
        let mut expr_vec1 = list1.to_vec();
        let mut expr_vec2 = list2.to_vec();
        while let Some(expr1) = expr_vec1.pop() {
            if let Some(idx) = expr_vec2.iter().position(|expr2| expr1.eq(expr2)) {
                expr_vec2.swap_remove(idx);
            } else {
                break;
            }
        }
        expr_vec1.is_empty() && expr_vec2.is_empty()
    } else {
        false
    }
}

/// Strictly compare the two expr lists are equal in the given order.
pub fn expr_list_eq_strict_order(
    list1: &[Arc<dyn PhysicalExpr>],
    list2: &[Arc<dyn PhysicalExpr>],
) -> bool {
    list1.len() == list2.len() && list1.iter().zip(list2.iter()).all(|(e1, e2)| e1.eq(e2))
}

/// Strictly compare the two sort expr lists in the given order.
///
/// For Physical Sort Exprs, the order matters:
///
/// SortExpr('a','b','c') != SortExpr('c','b','a')
#[allow(dead_code)]
pub fn sort_expr_list_eq_strict_order(
    list1: &[PhysicalSortExpr],
    list2: &[PhysicalSortExpr],
) -> bool {
    list1.len() == list2.len() && list1.iter().zip(list2.iter()).all(|(e1, e2)| e1.eq(e2))
}

/// Assume the predicate is in the form of CNF, split the predicate to a Vec of PhysicalExprs.
///
/// For example, split "a1 = a2 AND b1 <= b2 AND c1 != c2" into ["a1 = a2", "b1 <= b2", "c1 != c2"]
pub fn split_conjunction(
    predicate: &Arc<dyn PhysicalExpr>,
) -> Vec<&Arc<dyn PhysicalExpr>> {
    split_conjunction_impl(predicate, vec![])
}

fn split_conjunction_impl<'a>(
    predicate: &'a Arc<dyn PhysicalExpr>,
    mut exprs: Vec<&'a Arc<dyn PhysicalExpr>>,
) -> Vec<&'a Arc<dyn PhysicalExpr>> {
    match predicate.as_any().downcast_ref::<BinaryExpr>() {
        Some(binary) => match binary.op() {
            Operator::And => {
                let exprs = split_conjunction_impl(binary.left(), exprs);
                split_conjunction_impl(binary.right(), exprs)
            }
            _ => {
                exprs.push(predicate);
                exprs
            }
        },
        None => {
            exprs.push(predicate);
            exprs
        }
    }
}

/// Normalize the output expressions based on Alias Map and SchemaRef.
///
/// 1) If there is mapping in Alias Map, replace the Column in the output expressions with the 1st Column in Alias Map
/// 2) If the Column is invalid for the current Schema, replace the Column with a place holder UnKnownColumn
///
pub fn normalize_out_expr_with_alias_schema(
    expr: Arc<dyn PhysicalExpr>,
    alias_map: &HashMap<Column, Vec<Column>>,
    schema: &SchemaRef,
) -> Arc<dyn PhysicalExpr> {
    let expr_clone = expr.clone();
    expr_clone
        .transform(&|expr| {
            let normalized_form: Option<Arc<dyn PhysicalExpr>> =
                match expr.as_any().downcast_ref::<Column>() {
                    Some(column) => {
                        let out = alias_map
                            .get(column)
                            .map(|c| {
                                let out_col: Arc<dyn PhysicalExpr> =
                                    Arc::new(c[0].clone());
                                out_col
                            })
                            .or_else(|| match schema.index_of(column.name()) {
                                // Exactly matching, return None, no need to do the transform
                                Ok(idx) if column.index() == idx => None,
                                _ => {
                                    let out_col: Arc<dyn PhysicalExpr> =
                                        Arc::new(UnKnownColumn::new(column.name()));
                                    Some(out_col)
                                }
                            });
                        out
                    }
                    None => None,
                };
            Ok(normalized_form)
        })
        .unwrap_or(expr)
}

pub fn normalize_expr_with_equivalence_properties(
    expr: Arc<dyn PhysicalExpr>,
    eq_properties: &[EquivalentClass],
) -> Arc<dyn PhysicalExpr> {
    let expr_clone = expr.clone();
    expr_clone
        .transform(&|expr| match expr.as_any().downcast_ref::<Column>() {
            Some(column) => {
                let mut normalized: Option<Arc<dyn PhysicalExpr>> = None;
                for class in eq_properties {
                    if class.contains(column) {
                        normalized = Some(Arc::new(class.head().clone()));
                        break;
                    }
                }
                Ok(normalized)
            }
            None => Ok(None),
        })
        .unwrap_or(expr)
}

pub fn new_sort_requirements(
    sort_keys: Option<&[PhysicalSortExpr]>,
) -> Option<Vec<PhysicalSortRequirements>> {
    sort_keys.map(|ordering| {
        ordering
            .iter()
            .map(|o| PhysicalSortRequirements {
                expr: o.expr.clone(),
                sort_options: Some(o.options),
            })
            .collect::<Vec<_>>()
    })
}

pub fn normalize_sort_expr_with_equivalence_properties(
    sort_expr: PhysicalSortExpr,
    eq_properties: &[EquivalentClass],
) -> PhysicalSortExpr {
    let normalized_expr =
        normalize_expr_with_equivalence_properties(sort_expr.expr.clone(), eq_properties);

    if sort_expr.expr.ne(&normalized_expr) {
        PhysicalSortExpr {
            expr: normalized_expr,
            options: sort_expr.options,
        }
    } else {
        sort_expr
    }
}

pub fn normalize_sort_requirement_with_equivalence_properties(
    sort_requirement: PhysicalSortRequirements,
    eq_properties: &[EquivalentClass],
) -> PhysicalSortRequirements {
    let normalized_expr = normalize_expr_with_equivalence_properties(
        sort_requirement.expr.clone(),
        eq_properties,
    );
    if sort_requirement.expr.ne(&normalized_expr) {
        PhysicalSortRequirements {
            expr: normalized_expr,
            sort_options: sort_requirement.sort_options,
        }
    } else {
        sort_requirement
    }
}

/// Checks whether the required [PhysicalSortExpr]s are satisfied by the provided [PhysicalSortExpr]s.
pub fn ordering_satisfy<F: FnOnce() -> EquivalenceProperties>(
    provided: Option<&[PhysicalSortExpr]>,
    required: Option<&[PhysicalSortExpr]>,
    equal_properties: F,
) -> bool {
    match (provided, required) {
        (_, None) => true,
        (None, Some(_)) => false,
        (Some(provided), Some(required)) => {
            ordering_satisfy_concrete(provided, required, equal_properties)
        }
    }
}

fn ordering_satisfy_concrete<F: FnOnce() -> EquivalenceProperties>(
    provided: &[PhysicalSortExpr],
    required: &[PhysicalSortExpr],
    equal_properties: F,
) -> bool {
    if required.len() > provided.len() {
        false
    } else if required
        .iter()
        .zip(provided.iter())
        .all(|(order1, order2)| order1.eq(order2))
    {
        true
    } else if let eq_classes @ [_, ..] = equal_properties().classes() {
        let normalized_required_exprs = required
            .iter()
            .map(|e| {
                normalize_sort_expr_with_equivalence_properties(e.clone(), eq_classes)
            })
            .collect::<Vec<_>>();
        let normalized_provided_exprs = provided
            .iter()
            .map(|e| {
                normalize_sort_expr_with_equivalence_properties(e.clone(), eq_classes)
            })
            .collect::<Vec<_>>();
        normalized_required_exprs
            .iter()
            .zip(normalized_provided_exprs.iter())
            .all(|(order1, order2)| order1.eq(order2))
    } else {
        false
    }
}

/// Checks whether the required ordering requirements are satisfied by the provided [PhysicalSortExpr]s.
pub fn ordering_satisfy_requirement<F: FnOnce() -> EquivalenceProperties>(
    provided: Option<&[PhysicalSortExpr]>,
    required: Option<&[PhysicalSortRequirements]>,
    equal_properties: F,
) -> bool {
    match (provided, required) {
        (_, None) => true,
        (None, Some(_)) => false,
        (Some(provided), Some(required)) => {
            ordering_satisfy_requirement_concrete(provided, required, equal_properties)
        }
    }
}

pub fn ordering_satisfy_requirement_concrete<F: FnOnce() -> EquivalenceProperties>(
    provided: &[PhysicalSortExpr],
    required: &[PhysicalSortRequirements],
    equal_properties: F,
) -> bool {
    if required.len() > provided.len() {
        false
    } else if required
        .iter()
        .zip(provided.iter())
        .all(|(order1, order2)| order2.satisfy(order1))
    {
        true
    } else if let eq_classes @ [_, ..] = equal_properties().classes() {
        let normalized_requirements = required
            .iter()
            .map(|e| {
                normalize_sort_requirement_with_equivalence_properties(
                    e.clone(),
                    eq_classes,
                )
            })
            .collect::<Vec<_>>();
        let normalized_provided_exprs = provided
            .iter()
            .map(|e| {
                normalize_sort_expr_with_equivalence_properties(e.clone(), eq_classes)
            })
            .collect::<Vec<_>>();
        normalized_requirements
            .iter()
            .zip(normalized_provided_exprs.iter())
            .all(|(order1, order2)| order2.satisfy(order1))
    } else {
        false
    }
}

/// Provided requirements are compatible with the required, which means the provided requirements are equal or more specific than the required
pub fn requirements_compatible<F: FnOnce() -> EquivalenceProperties>(
    provided: Option<&[PhysicalSortRequirements]>,
    required: Option<&[PhysicalSortRequirements]>,
    equal_properties: F,
) -> bool {
    match (provided, required) {
        (_, None) => true,
        (None, Some(_)) => false,
        (Some(provided), Some(required)) => {
            if required.len() > provided.len() {
                false
            } else if required
                .iter()
                .zip(provided.iter())
                .all(|(req, pro)| pro.compatible(req))
            {
                true
            } else if let eq_classes @ [_, ..] = equal_properties().classes() {
                let normalized_required = required
                    .iter()
                    .map(|e| {
                        normalize_sort_requirement_with_equivalence_properties(
                            e.clone(),
                            eq_classes,
                        )
                    })
                    .collect::<Vec<_>>();
                let normalized_provided = provided
                    .iter()
                    .map(|e| {
                        normalize_sort_requirement_with_equivalence_properties(
                            e.clone(),
                            eq_classes,
                        )
                    })
                    .collect::<Vec<_>>();
                normalized_required
                    .iter()
                    .zip(normalized_provided.iter())
                    .all(|(req, pro)| pro.compatible(req))
            } else {
                false
            }
        }
    }
}

pub fn map_columns_before_projection(
    parent_required: &[Arc<dyn PhysicalExpr>],
    proj_exprs: &[(Arc<dyn PhysicalExpr>, String)],
) -> Vec<Arc<dyn PhysicalExpr>> {
    let mut column_mapping = HashMap::new();
    for (expression, name) in proj_exprs.iter() {
        if let Some(column) = expression.as_any().downcast_ref::<Column>() {
            column_mapping.insert(name.clone(), column.clone());
        };
    }
    let new_required: Vec<Arc<dyn PhysicalExpr>> = parent_required
        .iter()
        .filter_map(|r| {
            if let Some(column) = r.as_any().downcast_ref::<Column>() {
                column_mapping.get(column.name())
            } else {
                None
            }
        })
        .map(|e| Arc::new(e.clone()) as Arc<dyn PhysicalExpr>)
        .collect::<Vec<_>>();
    new_required
}

pub fn map_requirement_before_projection(
    parent_required: Option<&[PhysicalSortRequirements]>,
    proj_exprs: &[(Arc<dyn PhysicalExpr>, String)],
) -> Option<Vec<PhysicalSortRequirements>> {
    println!("parent_required: {:?}", parent_required);
    println!("proj_exprs: {:?}", proj_exprs);
    if let Some(requirement) = parent_required {
        let required_expr = create_sort_expr_from_requirement(requirement)
            .iter()
            .map(|sort_expr| sort_expr.expr.clone())
            .collect::<Vec<_>>();
        println!("required_expr:{:?}", required_expr);
        let new_exprs = map_columns_before_projection(&required_expr, proj_exprs);
        println!("new_exprs:{:?}", new_exprs);
        if new_exprs.len() == requirement.len() {
            let new_request = new_exprs
                .iter()
                .zip(requirement.iter())
                .map(|(new, old)| PhysicalSortRequirements {
                    expr: new.clone(),
                    sort_options: old.sort_options,
                })
                .collect::<Vec<_>>();
            Some(new_request)
        } else {
            None
        }
    } else {
        None
    }
}

pub fn create_sort_expr_from_requirement(
    required: &[PhysicalSortRequirements],
) -> Vec<PhysicalSortExpr> {
    let parent_required_expr = required
        .iter()
        .map(|prop| {
            if prop.sort_options.is_some() {
                PhysicalSortExpr {
                    expr: prop.expr.clone(),
                    options: prop.sort_options.unwrap(),
                }
            } else {
                PhysicalSortExpr {
                    expr: prop.expr.clone(),
                    options: SortOptions {
                        // By default, create sort key with ASC is true and NULLS LAST to be consistent with
                        // PostgreSQL's rule: https://www.postgresql.org/docs/current/queries-order.html
                        descending: false,
                        nulls_first: false,
                    },
                }
            }
        })
        .collect::<Vec<_>>();
    parent_required_expr
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::expressions::Column;
    use crate::PhysicalSortExpr;
    use arrow::compute::SortOptions;
    use datafusion_common::Result;

    use arrow_schema::Schema;
    use std::sync::Arc;

    #[test]
    fn expr_list_eq_test() -> Result<()> {
        let list1: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("b", 1)),
        ];
        let list2: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("a", 0)),
        ];
        assert!(!expr_list_eq_any_order(list1.as_slice(), list2.as_slice()));
        assert!(!expr_list_eq_any_order(list2.as_slice(), list1.as_slice()));

        assert!(!expr_list_eq_strict_order(
            list1.as_slice(),
            list2.as_slice()
        ));
        assert!(!expr_list_eq_strict_order(
            list2.as_slice(),
            list1.as_slice()
        ));

        let list3: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("c", 2)),
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("b", 1)),
        ];
        let list4: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("c", 2)),
            Arc::new(Column::new("a", 0)),
        ];
        assert!(expr_list_eq_any_order(list3.as_slice(), list4.as_slice()));
        assert!(expr_list_eq_any_order(list4.as_slice(), list3.as_slice()));
        assert!(expr_list_eq_any_order(list3.as_slice(), list3.as_slice()));
        assert!(expr_list_eq_any_order(list4.as_slice(), list4.as_slice()));

        assert!(!expr_list_eq_strict_order(
            list3.as_slice(),
            list4.as_slice()
        ));
        assert!(!expr_list_eq_strict_order(
            list4.as_slice(),
            list3.as_slice()
        ));
        assert!(expr_list_eq_any_order(list3.as_slice(), list3.as_slice()));
        assert!(expr_list_eq_any_order(list4.as_slice(), list4.as_slice()));

        Ok(())
    }

    #[test]
    fn sort_expr_list_eq_strict_order_test() -> Result<()> {
        let list1: Vec<PhysicalSortExpr> = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: SortOptions::default(),
            },
        ];

        let list2: Vec<PhysicalSortExpr> = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: SortOptions::default(),
            },
        ];

        assert!(!sort_expr_list_eq_strict_order(
            list1.as_slice(),
            list2.as_slice()
        ));
        assert!(!sort_expr_list_eq_strict_order(
            list2.as_slice(),
            list1.as_slice()
        ));

        let list3: Vec<PhysicalSortExpr> = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("c", 2)),
                options: SortOptions::default(),
            },
        ];
        let list4: Vec<PhysicalSortExpr> = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("c", 2)),
                options: SortOptions::default(),
            },
        ];

        assert!(sort_expr_list_eq_strict_order(
            list3.as_slice(),
            list4.as_slice()
        ));
        assert!(sort_expr_list_eq_strict_order(
            list4.as_slice(),
            list3.as_slice()
        ));
        assert!(sort_expr_list_eq_strict_order(
            list3.as_slice(),
            list3.as_slice()
        ));
        assert!(sort_expr_list_eq_strict_order(
            list4.as_slice(),
            list4.as_slice()
        ));

        Ok(())
    }

    #[test]
    fn test_ordering_satisfy() -> Result<()> {
        let crude = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("a", 0)),
            options: SortOptions::default(),
        }];
        let crude = Some(&crude[..]);
        let finer = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: SortOptions::default(),
            },
        ];
        let finer = Some(&finer[..]);
        let empty_schema = &Arc::new(Schema {
            fields: vec![],
            metadata: Default::default(),
        });
        assert!(ordering_satisfy(finer, crude, || {
            EquivalenceProperties::new(empty_schema.clone())
        }));
        assert!(!ordering_satisfy(crude, finer, || {
            EquivalenceProperties::new(empty_schema.clone())
        }));
        Ok(())
    }
}
