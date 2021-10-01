use crate::{
    error::DataFusionError,
    logical_plan::Operator,
    optimizer::tokomak::scalar::TokomakScalar,
    physical_plan::expressions::{common_binary_type, BinaryExpr, CastExpr},
    scalar::ScalarValue,
};
use arrow::datatypes::DataType;
use egg::*;

use super::super::TokomakExpr;
use std::convert::TryInto;

pub fn convert_to_scalar_value(
    var: Var,
    egraph: &mut EGraph<TokomakExpr, ()>,
    _id: Id,
    subst: &Subst,
) -> Result<ScalarValue, DataFusionError> {
    let expr = egraph[subst[var]]
        .nodes
        .iter()
        .find(|e| e.can_convert_to_scalar_value())
        .ok_or_else(|| {
            DataFusionError::Internal(
                "Could not find a node that was convertable to scalar value".to_string(),
            )
        })?;
    expr.try_into()
}

pub fn scalar_binop(
    lhs: ScalarValue,
    rhs: ScalarValue,
    op: &Operator,
) -> Result<ScalarValue, DataFusionError> {
    let (lhs, rhs) = binop_type_coercion(lhs, rhs, &op)?;
    let res = BinaryExpr::evaluate_values(
        crate::physical_plan::ColumnarValue::Scalar(lhs),
        crate::physical_plan::ColumnarValue::Scalar(rhs),
        &op,
        1,
    )?;
    match res {
        crate::physical_plan::ColumnarValue::Scalar(s) => Ok(s),
        crate::physical_plan::ColumnarValue::Array(arr) => {
            if arr.len() == 1 {
                ScalarValue::try_from_array(&arr, 0)
            } else {
                Err(DataFusionError::Internal(format!(
                    "Could not convert an array of length {} to a scalar",
                    arr.len()
                )))
            }
        }
    }
}

pub fn cast(
    val: ScalarValue,
    cast_type: &DataType,
) -> Result<ScalarValue, DataFusionError> {
    CastExpr::cast_scalar_default_options(val, cast_type)
}

pub fn binop_type_coercion(
    mut lhs: ScalarValue,
    mut rhs: ScalarValue,
    op: &Operator,
) -> Result<(ScalarValue, ScalarValue), DataFusionError> {
    let lhs_dt = lhs.get_datatype();
    let rhs_dt = rhs.get_datatype();
    let common_datatype = common_binary_type(&lhs_dt, &op, &rhs_dt)?;
    if lhs_dt != common_datatype {
        lhs = cast(lhs, &common_datatype)?;
    }
    if rhs_dt != common_datatype {
        rhs = cast(rhs, &common_datatype)?;
    }
    Ok((lhs, rhs))
}

pub fn min(x: ScalarValue, y: ScalarValue) -> Result<ScalarValue, DataFusionError> {
    let ret_val = if lt(x.clone(), y.clone())? { x } else { y };
    Ok(ret_val)
}

pub fn max(x: ScalarValue, y: ScalarValue) -> Result<ScalarValue, DataFusionError> {
    let ret_val = if gt(x.clone(), y.clone())? { x } else { y };
    Ok(ret_val)
}

pub fn lt(lhs: ScalarValue, rhs: ScalarValue) -> Result<bool, DataFusionError> {
    let res = scalar_binop(lhs, rhs, &Operator::Lt)?;
    res.try_into()
}

pub fn lte(lhs: ScalarValue, rhs: ScalarValue) -> Result<bool, DataFusionError> {
    let res = scalar_binop(lhs, rhs, &Operator::LtEq)?;
    res.try_into()
}

pub fn gt(lhs: ScalarValue, rhs: ScalarValue) -> Result<bool, DataFusionError> {
    let res = scalar_binop(lhs, rhs, &Operator::Gt)?;
    res.try_into()
}

pub fn gte(lhs: ScalarValue, rhs: ScalarValue) -> Result<bool, DataFusionError> {
    let res = scalar_binop(lhs, rhs, &Operator::GtEq)?;
    res.try_into()
}

pub fn var_gt(
    lhs: Var,
    rhs: Var,
) -> impl Fn(&mut EGraph<TokomakExpr, ()>, Id, &Subst) -> bool {
    move |egraph: &mut EGraph<TokomakExpr, ()>, id: Id, subst: &Subst| -> bool {
        if !can_convert_both_to_scalar_value(lhs, rhs)(egraph, id, subst) {
            return false;
        }
        let lt = const_binop(lhs, rhs, Operator::Gt);
        let t_expr = match lt.evaluate(egraph, id, subst) {
            Some(s) => s,
            None => return false,
        };
        match t_expr {
            TokomakExpr::Scalar(TokomakScalar::Boolean(Some(v))) => return v,
            _ => return false,
        }
    }
}

pub fn var_lt(
    lhs: Var,
    rhs: Var,
) -> impl Fn(&mut EGraph<TokomakExpr, ()>, Id, &Subst) -> bool {
    move |egraph: &mut EGraph<TokomakExpr, ()>, id: Id, subst: &Subst| -> bool {
        if !can_convert_both_to_scalar_value(lhs, rhs)(egraph, id, subst) {
            return false;
        }
        let lt = const_binop(lhs, rhs, Operator::Lt);
        let t_expr = match lt.evaluate(egraph, id, subst) {
            Some(s) => s,
            None => return false,
        };
        match t_expr {
            TokomakExpr::Scalar(TokomakScalar::Boolean(Some(v))) => return v,
            _ => return false,
        }
    }
}

pub fn var_gte(
    lhs: Var,
    rhs: Var,
) -> impl Fn(&mut EGraph<TokomakExpr, ()>, Id, &Subst) -> bool {
    move |egraph: &mut EGraph<TokomakExpr, ()>, id: Id, subst: &Subst| -> bool {
        if !can_convert_both_to_scalar_value(lhs, rhs)(egraph, id, subst) {
            return false;
        }
        let lt = const_binop(lhs, rhs, Operator::GtEq);
        let t_expr = match lt.evaluate(egraph, id, subst) {
            Some(s) => s,
            None => return false,
        };
        match t_expr {
            TokomakExpr::Scalar(TokomakScalar::Boolean(Some(v))) => return v,
            _ => return false,
        }
    }
}

pub fn var_lte(
    lhs: Var,
    rhs: Var,
) -> impl Fn(&mut EGraph<TokomakExpr, ()>, Id, &Subst) -> bool {
    move |egraph: &mut EGraph<TokomakExpr, ()>, id: Id, subst: &Subst| -> bool {
        if !can_convert_both_to_scalar_value(lhs, rhs)(egraph, id, subst) {
            return false;
        }
        let lt = const_binop(lhs, rhs, Operator::LtEq);
        let t_expr = match lt.evaluate(egraph, id, subst) {
            Some(s) => s,
            None => return false,
        };
        match t_expr {
            TokomakExpr::Scalar(TokomakScalar::Boolean(Some(v))) => return v,
            _ => return false,
        }
    }
}

pub fn var_eq(
    lhs: Var,
    rhs: Var,
) -> impl Fn(&mut EGraph<TokomakExpr, ()>, Id, &Subst) -> bool {
    move |egraph: &mut EGraph<TokomakExpr, ()>, id: Id, subst: &Subst| -> bool {
        if !can_convert_both_to_scalar_value(lhs, rhs)(egraph, id, subst) {
            return false;
        }
        let lt = const_binop(lhs, rhs, Operator::Eq);
        let t_expr = match lt.evaluate(egraph, id, subst) {
            Some(s) => s,
            None => return false,
        };
        match t_expr {
            TokomakExpr::Scalar(TokomakScalar::Boolean(Some(v))) => return v,
            _ => return false,
        }
    }
}

pub fn var_neq(
    lhs: Var,
    rhs: Var,
) -> impl Fn(&mut EGraph<TokomakExpr, ()>, Id, &Subst) -> bool {
    move |egraph: &mut EGraph<TokomakExpr, ()>, id: Id, subst: &Subst| -> bool {
        if !can_convert_both_to_scalar_value(lhs, rhs)(egraph, id, subst) {
            return false;
        }
        let lt = const_binop(lhs, rhs, Operator::NotEq);
        let t_expr = match lt.evaluate(egraph, id, subst) {
            Some(s) => s,
            None => return false,
        };
        match t_expr {
            TokomakExpr::Scalar(TokomakScalar::Boolean(Some(v))) => return v,
            _ => return false,
        }
    }
}

pub fn can_convert_both_to_scalar_value(
    lhs: Var,
    rhs: Var,
) -> impl Fn(&mut EGraph<TokomakExpr, ()>, Id, &Subst) -> bool {
    move |egraph: &mut EGraph<TokomakExpr, ()>, _id: Id, subst: &Subst| -> bool {
        let lexpr = egraph[subst[lhs]]
            .nodes
            .iter()
            .find(|e| e.can_convert_to_scalar_value());
        let rexpr = egraph[subst[rhs]]
            .nodes
            .iter()
            .find(|e| e.can_convert_to_scalar_value());
        lexpr.is_some() && rexpr.is_some()
    }
}

pub struct ConstBinop<
    F: Fn(TokomakExpr, TokomakExpr) -> Option<TokomakExpr>,
    M: Fn(&&TokomakExpr) -> bool + 'static,
>(pub Var, pub Var, pub M, pub F);

impl<
        F: Fn(TokomakExpr, TokomakExpr) -> Option<TokomakExpr>,
        M: Fn(&&TokomakExpr) -> bool,
    > ConstBinop<F, M>
{
    fn evaluate(
        &self,
        egraph: &mut EGraph<TokomakExpr, ()>,
        _: Id,
        subst: &Subst,
    ) -> Option<TokomakExpr> {
        let lhs = egraph[subst[self.0]].nodes.iter().find(|expr| self.2(expr));
        let lhs = match lhs {
            Some(v) => v,
            None => return None,
        }
        .clone();
        let rhs = egraph[subst[self.1]].nodes.iter().find(|expr| self.2(expr));
        let rhs = match rhs {
            Some(v) => v,
            None => return None,
        }
        .clone();
        self.3(lhs, rhs)
    }
}

impl<
        F: Fn(TokomakExpr, TokomakExpr) -> Option<TokomakExpr>,
        M: Fn(&&TokomakExpr) -> bool,
    > Applier<TokomakExpr, ()> for ConstBinop<F, M>
{
    fn apply_one(
        &self,
        egraph: &mut EGraph<TokomakExpr, ()>,
        id: Id,
        subst: &Subst,
    ) -> Vec<Id> {
        match self.evaluate(egraph, id, subst) {
            Some(e) => vec![egraph.add(e)],
            None => Vec::new(),
        }
    }
}

pub fn const_binop(
    lhs: Var,
    rhs: Var,
    op: Operator,
) -> ConstBinop<
    impl Fn(TokomakExpr, TokomakExpr) -> Option<TokomakExpr>,
    impl Fn(&&TokomakExpr) -> bool,
> {
    ConstBinop(
        lhs,
        rhs,
        |e| e.can_convert_to_scalar_value(),
        move |l, r| -> Option<TokomakExpr> {
            let mut lhs: ScalarValue = (&l).try_into().ok()?;
            let mut rhs: ScalarValue = (&r).try_into().ok()?;
            let ldt = lhs.get_datatype();
            let rdt = rhs.get_datatype();
            if ldt != rdt {
                let common_dt = common_binary_type(&ldt, &op, &rdt).ok()?;
                if ldt != common_dt {
                    lhs = CastExpr::cast_scalar_default_options(lhs, &common_dt).ok()?;
                }
                if rdt != common_dt {
                    rhs = CastExpr::cast_scalar_default_options(rhs, &common_dt).ok()?;
                }
            }

            let res = BinaryExpr::evaluate_values(
                crate::physical_plan::ColumnarValue::Scalar(lhs),
                crate::physical_plan::ColumnarValue::Scalar(rhs),
                &op,
                1,
            )
            .ok()?;
            let scalar = ScalarValue::try_from_columnar_value(res).ok()?;

            let t_expr: TokomakExpr = scalar.try_into().unwrap();
            Some(t_expr)
        },
    )
}

pub fn is_literal(
    expr: Id,
    egraph: &EGraph<TokomakExpr, ()>,
    _id: Id,
    _subst: &Subst,
) -> bool {
    let res = egraph[expr]
        .nodes
        .iter()
        .flat_map(|expr| -> Result<ScalarValue, DataFusionError> { expr.try_into() })
        .nth(0);
    if res.is_none() {
        return false;
    }
    true
}

pub fn to_literal_list<'a>(
    var: Var,
    egraph: &'a EGraph<TokomakExpr, ()>,
    id: Id,
    subst: &Subst,
) -> Option<&'a [Id]> {
    for expr in egraph[subst[var]].nodes.iter() {
        match expr {
            TokomakExpr::List(ref l) => {
                if l.iter().all(|expr| is_literal(*expr, egraph, id, subst)) {
                    return Some(l.as_slice());
                }
            }
            _ => (),
        }
    }
    None
}
