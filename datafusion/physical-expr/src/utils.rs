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

use crate::expressions::BinaryExpr;
use crate::expressions::Column;
use crate::expressions::UnKnownColumn;
use crate::physical_expr::EquivalenceProperties;
use crate::rewrite::TreeNodeRewritable;
use crate::PhysicalExpr;
use crate::PhysicalSortExpr;
use datafusion_expr::Operator;

use arrow::datatypes::SchemaRef;

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
///
pub fn split_predicate(predicate: &Arc<dyn PhysicalExpr>) -> Vec<&Arc<dyn PhysicalExpr>> {
    match predicate.as_any().downcast_ref::<BinaryExpr>() {
        Some(binary) => match binary.op() {
            Operator::And => {
                let mut vec1 = split_predicate(binary.left());
                let vec2 = split_predicate(binary.right());
                vec1.extend(vec2);
                vec1
            }
            _ => vec![predicate],
        },
        None => vec![],
    }
}

/// Combine the new equal condition with the existing equivalence properties.
pub fn combine_equivalence_properties(
    eq_properties: &mut Vec<EquivalenceProperties>,
    new_condition: (&Column, &Column),
) {
    let mut idx1 = -1i32;
    let mut idx2 = -1i32;
    for (idx, prop) in eq_properties.iter_mut().enumerate() {
        let contains_first = prop.contains(new_condition.0);
        let contains_second = prop.contains(new_condition.1);
        if contains_first && !contains_second {
            prop.insert(new_condition.1.clone());
            idx1 = idx as i32;
        } else if !contains_first && contains_second {
            prop.insert(new_condition.0.clone());
            idx2 = idx as i32;
        } else if contains_first && contains_second {
            idx1 = idx as i32;
            idx2 = idx as i32;
            break;
        }
    }

    if idx1 != -1 && idx2 != -1 && idx1 != idx2 {
        // need to merge the two existing properties
        let second_properties = eq_properties.get(idx2 as usize).unwrap().clone();
        let first_properties = eq_properties.get_mut(idx1 as usize).unwrap();
        for prop in second_properties.iter() {
            if !first_properties.contains(prop) {
                first_properties.insert(prop.clone());
            }
        }
        eq_properties.remove(idx2 as usize);
    } else if idx1 == -1 && idx2 == -1 {
        // adding new pairs
        eq_properties.push(EquivalenceProperties::new(
            new_condition.0.clone(),
            vec![new_condition.1.clone()],
        ))
    }
}

pub fn remove_equivalence_properties(
    eq_properties: &mut Vec<EquivalenceProperties>,
    remove_condition: (&Column, &Column),
) {
    let mut match_idx = -1i32;
    for (idx, prop) in eq_properties.iter_mut().enumerate() {
        let contains_first = prop.contains(remove_condition.0);
        let contains_second = prop.contains(remove_condition.1);
        if contains_first && contains_second {
            match_idx = idx as i32;
            break;
        }
    }
    if match_idx >= 0 {
        let matches = eq_properties.get_mut(match_idx as usize).unwrap();
        matches.remove(remove_condition.0);
        matches.remove(remove_condition.1);
        if matches.len() <= 1 {
            eq_properties.remove(match_idx as usize);
        }
    }
}

pub fn merge_equivalence_properties_with_alias(
    eq_properties: &mut Vec<EquivalenceProperties>,
    alias_map: &HashMap<Column, Vec<Column>>,
) {
    for (column, columns) in alias_map {
        let mut find_match = false;
        for (_idx, prop) in eq_properties.iter_mut().enumerate() {
            if prop.contains(column) {
                for col in columns {
                    prop.insert(col.clone());
                }
                find_match = true;
                break;
            }
        }
        if !find_match {
            eq_properties
                .push(EquivalenceProperties::new(column.clone(), columns.clone()));
        }
    }
}

pub fn truncate_equivalence_properties_not_in_schema(
    eq_properties: &mut Vec<EquivalenceProperties>,
    schema: &SchemaRef,
) {
    for props in eq_properties.iter_mut() {
        let mut columns_to_remove = vec![];
        for column in props.iter() {
            if let Ok(idx) = schema.index_of(column.name()) {
                if idx != column.index() {
                    columns_to_remove.push(column.clone());
                }
            } else {
                columns_to_remove.push(column.clone());
            }
        }
        for column in columns_to_remove {
            props.remove(&column);
        }
    }
    eq_properties.retain(|props| props.len() > 1);
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
            normalized_form
        })
        .unwrap_or(expr)
}

pub fn normalize_expr_with_equivalence_properties(
    expr: Arc<dyn PhysicalExpr>,
    eq_properties: &[EquivalenceProperties],
) -> Arc<dyn PhysicalExpr> {
    let mut normalized = expr.clone();
    if let Some(column) = expr.as_any().downcast_ref::<Column>() {
        for prop in eq_properties {
            if prop.contains(column) {
                normalized = Arc::new(prop.head().clone());
                break;
            }
        }
    }
    normalized
}

pub fn normalize_sort_expr_with_equivalence_properties(
    sort_expr: PhysicalSortExpr,
    eq_properties: &[EquivalenceProperties],
) -> PhysicalSortExpr {
    let mut normalized = sort_expr.clone();
    if let Some(column) = sort_expr.expr.as_any().downcast_ref::<Column>() {
        for prop in eq_properties {
            if prop.contains(column) {
                normalized = PhysicalSortExpr {
                    expr: Arc::new(prop.head().clone()),
                    options: sort_expr.options,
                };
                break;
            }
        }
    }
    normalized
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::expressions::Column;
    use crate::PhysicalSortExpr;
    use arrow::compute::SortOptions;
    use datafusion_common::Result;

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
    fn combine_equivalence_properties_test() -> Result<()> {
        let mut eq_properties: Vec<EquivalenceProperties> = vec![];
        let new_condition = (&Column::new("a", 0), &Column::new("b", 1));
        combine_equivalence_properties(&mut eq_properties, new_condition);
        assert_eq!(eq_properties.len(), 1);

        let new_condition = (&Column::new("b", 1), &Column::new("a", 0));
        combine_equivalence_properties(&mut eq_properties, new_condition);
        assert_eq!(eq_properties.len(), 1);
        assert_eq!(eq_properties[0].len(), 2);

        let new_condition = (&Column::new("b", 1), &Column::new("c", 2));
        combine_equivalence_properties(&mut eq_properties, new_condition);
        assert_eq!(eq_properties.len(), 1);
        assert_eq!(eq_properties[0].len(), 3);

        let new_condition = (&Column::new("x", 99), &Column::new("y", 100));
        combine_equivalence_properties(&mut eq_properties, new_condition);
        assert_eq!(eq_properties.len(), 2);

        let new_condition = (&Column::new("x", 99), &Column::new("a", 0));
        combine_equivalence_properties(&mut eq_properties, new_condition);
        assert_eq!(eq_properties.len(), 1);
        assert_eq!(eq_properties[0].len(), 5);

        Ok(())
    }

    #[test]
    fn remove_equivalence_properties_test() -> Result<()> {
        let mut eq_properties: Vec<EquivalenceProperties> = vec![];
        let remove_condition = (&Column::new("a", 0), &Column::new("b", 1));
        remove_equivalence_properties(&mut eq_properties, remove_condition);
        assert_eq!(eq_properties.len(), 0);

        let new_condition = (&Column::new("a", 0), &Column::new("b", 1));
        combine_equivalence_properties(&mut eq_properties, new_condition);
        let new_condition = (&Column::new("a", 0), &Column::new("c", 2));
        combine_equivalence_properties(&mut eq_properties, new_condition);
        let new_condition = (&Column::new("c", 2), &Column::new("d", 3));
        combine_equivalence_properties(&mut eq_properties, new_condition);
        assert_eq!(eq_properties.len(), 1);

        let remove_condition = (&Column::new("a", 0), &Column::new("b", 1));
        remove_equivalence_properties(&mut eq_properties, remove_condition);
        assert_eq!(eq_properties.len(), 1);
        assert_eq!(eq_properties[0].len(), 2);

        Ok(())
    }

    #[test]
    fn merge_equivalence_properties_with_alias_test() -> Result<()> {
        let mut eq_properties: Vec<EquivalenceProperties> = vec![];
        let mut alias_map = HashMap::new();
        alias_map.insert(
            Column::new("a", 0),
            vec![Column::new("a1", 1), Column::new("a2", 2)],
        );

        merge_equivalence_properties_with_alias(&mut eq_properties, &alias_map);
        assert_eq!(eq_properties.len(), 1);
        assert_eq!(eq_properties[0].len(), 3);

        let mut alias_map = HashMap::new();
        alias_map.insert(
            Column::new("a", 0),
            vec![Column::new("a3", 1), Column::new("a4", 2)],
        );
        merge_equivalence_properties_with_alias(&mut eq_properties, &alias_map);
        assert_eq!(eq_properties.len(), 1);
        assert_eq!(eq_properties[0].len(), 5);
        Ok(())
    }
}
