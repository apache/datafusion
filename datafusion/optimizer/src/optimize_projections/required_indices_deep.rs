use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::hash::Hash;
use datafusion_common::{Column, ScalarValue};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_expr::Expr;

// pub fn append_column(acc: &mut HashMap<Column, Vec<String>>, column: &Column, rest: Vec<String>) {
//     info!("APPEND: {:?} = {:?}", column, rest);
//     match acc.get_mut(column) {
//         None => {
//             let column_clone = column.clone();
//             if rest.len() > 0 {
//                 acc.insert(column_clone, vec![rest.join(".")]);
//             } else {
//                 acc.insert(column_clone, vec![]);
//             }
//         }
//         Some(cc) => {
//             if rest.len() > 0 {
//                 cc.push(rest.join("."));
//             }
//         }
//     }
// }

pub fn append_column<T>(acc: &mut HashMap<T, Vec<String>>, column: &T, rest: Vec<String>)
where T: Debug + Clone + Eq + Hash {
    match acc.get_mut(column) {
        None => {
            let column_clone = column.clone();
            if rest.len() > 0 {
                acc.insert(column_clone, vec![rest.join(".")]);
            } else {
                acc.insert(column_clone, vec![]);
            }
        }
        Some(cc) => {
            if cc.len() == 0 {
                // we already had this column in full
            } else {
                if rest.len() > 0 {
                    cc.push(rest.join("."));
                } else {
                    // we are getting the entire column, and we already had something
                    // we should delete everything
                    cc.clear();
                }
            }
        }
    }
}

pub fn expr_to_deep_columns(expr: &Expr) -> HashMap<Column, Vec<String>> {
    let mut accum: HashMap<Column, Vec<String>> = HashMap::new();
    let mut field_accum: VecDeque<String> = VecDeque::new();
    let mut in_make_struct_call: bool = false;
    let _ = expr.apply(|expr| {
        match expr {
            Expr::Column(qc) => {
                // @HStack FIXME: ADR: we should have a test case
                // ignore deep columns if we have a in_make_struct_call
                // case: struct(a, b, c)['col'] - we were getting 'col' in the accum stack
                // FIXME Will this work for struct(get_field(a, 'substruct'))['col'] ?????
                if in_make_struct_call {
                    field_accum.clear()
                }
                // at the end, unwind the field_accum and push all to accum
                let mut tmp: Vec<String> = vec![];
                // if we did't just save a "*" - which means the entire column
                if !(field_accum.len() == 1 && field_accum.get(0).unwrap() == "*") {
                    for f in field_accum.iter().rev() {
                        tmp.push(f.to_owned());
                    }
                }
                field_accum.clear();
                append_column::<Column>(&mut accum, qc, tmp);
            }
            Expr::ScalarFunction(sf) => {
                // TODO what about maps ? what's the operator
                match sf.name() {
                    "get_field" => {
                        // get field, append the second argument to the stack and continue
                        let literal_expr: String = match sf.args[1].clone() {
                            Expr::Literal(lit_expr) => match lit_expr {
                                ScalarValue::Utf8(str) => str.unwrap(),
                                _ => panic!()
                            }
                            _ => {panic!()}
                        };
                        field_accum.push_back(literal_expr);
                    }
                    "array_element" => {
                        // We don't have the schema, but when splatting the column, we need to actually push the list inner field name here
                        field_accum.push_back("*".to_owned());
                    }
                    "struct" => {
                        in_make_struct_call = true;
                    }
                    _ => {}
                }

            }
            // Use explicit pattern match instead of a default
            // implementation, so that in the future if someone adds
            // new Expr types, they will check here as well
            Expr::Unnest(_)
            | Expr::ScalarVariable(_, _)
            | Expr::Alias(_)
            | Expr::Literal(_)
            | Expr::BinaryExpr { .. }
            | Expr::Like { .. }
            | Expr::SimilarTo { .. }
            | Expr::Not(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotUnknown(_)
            | Expr::Negative(_)
            | Expr::Between { .. }
            | Expr::Case { .. }
            | Expr::Cast { .. }
            | Expr::TryCast { .. }
            | Expr::Sort { .. }
            | Expr::WindowFunction { .. }
            | Expr::AggregateFunction { .. }
            | Expr::GroupingSet(_)
            | Expr::InList { .. }
            | Expr::Exists { .. }
            | Expr::InSubquery(_)
            | Expr::ScalarSubquery(_)
            | Expr::Wildcard { .. }
            | Expr::Placeholder(_)
            | Expr::OuterReferenceColumn { .. } => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })
        .map(|_| ());
    accum
}
