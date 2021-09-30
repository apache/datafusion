use std::convert::TryInto;
use super::super::TokomakDataType;
use crate::{error::DataFusionError, logical_plan::Operator, optimizer::tokomak::{Tokomak, TokomakExpr, scalar::TokomakScalar}, physical_plan::{ColumnarValue, expressions::{BinaryExpr, CastExpr, common_binary_type}, functions::{BuiltinScalarFunction, FunctionVolatility, Signature}}, scalar::ScalarValue};
use arrow::datatypes::DataType;
use crate::physical_plan::type_coercion::data_types;

use egg::{rewrite as rw, *};


fn rules()->Vec<Rewrite<TokomakExpr,()>>{
    let a:Var = "?a".parse().unwrap();
    let b: Var = "?b".parse().unwrap();
    let c: Var = "?c".parse().unwrap();
    let d: Var = "?d".parse().unwrap();
    let x: Var ="?x".parse().unwrap();
    let func: Var = "?func".parse().unwrap();
    let args: Var = "?args".parse().unwrap();
    vec![rw!("const-prop-between"; "(and (>= ?e ?a) (<= ?e ?b))"=> "false" if gt_var(a,b)),
    rw!("const-prop-between_inverted"; "(and (< ?e ?a) (> ?e ?b))" => "true" if gt_var(a,b)),

    
    
    rw!("const-prop-binop-col-eq"; "(= ?a ?a)" => "(is_not_null ?a)"), //May not be true with nullable columns because NULL != NULL
    rw!("const-prop-binop-col-neq"; "(<> ?a ?a)" => "false"), //May not be true with nullable columns because NULL != NULL
    rw!("const-prop-binop-eq"; "(= ?a ?b)"=>{ const_binop(a,b, Operator::Eq)} if can_perform_const_binary_op(a,b, Operator::Eq)),
    rw!("const-prop-binop-neq"; "(<> ?a ?b)"=>{ const_binop(a,b, Operator::NotEq)} if can_perform_const_binary_op(a,b, Operator::NotEq)),

    rw!("const-prop-binop-gt"; "(> ?a ?b)"=>{ const_binop(a,b, Operator::Gt)} if can_perform_const_binary_op(a,b, Operator::Gt)),
    rw!("const-prop-binop-gte"; "(>= ?a ?b)"=>{ const_binop(a,b, Operator::GtEq)} if can_perform_const_binary_op(a,b, Operator::GtEq)),

    rw!("const-prop-binop-lt"; "(< ?a ?b)"=>{ const_binop(a,b, Operator::Lt)} if can_perform_const_binary_op(a,b, Operator::Lt)),
    rw!("const-prop-binop-lte"; "(<= ?a ?b)"=>{ const_binop(a,b, Operator::LtEq)} if can_perform_const_binary_op(a,b, Operator::LtEq)),


    rw!("const-prop-binop-add"; "(+ ?a ?b)"=>{ const_binop(a,b, Operator::Plus) } if can_convert_both_to_scalar_value(a,b) ),
    rw!("const-prop-binop-sub"; "(- ?a ?b)" => { const_binop(a, b, Operator::Minus)} if can_convert_both_to_scalar_value(a,b)),
    rw!("const-prop-binop-mul"; "(* ?a ?b)" => { const_binop(a,b, Operator::Multiply) } if can_convert_both_to_scalar_value(a,b)),
    rw!("const-prop-binop-div"; "(/ ?a ?b)" =>{  const_binop(a,b, Operator::Divide)} if can_convert_both_to_scalar_value(a,b)),

    rw!("const-cast"; "(cast ?a ?b)" =>{ ConstCastApplier{value: a, cast_type: b}} if can_convert_to_scalar_value(a) ),
    rw!("const-call-scalar"; "(call ?func ?args)"=>{ ConstCallApplier{func, args}} if is_immutable_scalar_builtin_function(func))]
}


fn add_constant_folding_rules(optimizer: &mut Tokomak){
    for rule in rules(){
        optimizer.add_rule(rule);
    }
}

fn can_convert_both_to_scalar_value(lhs: Var, rhs: Var)->impl Fn(&mut EGraph<TokomakExpr,()>, Id, &Subst) -> bool{
    move |egraph: &mut EGraph<TokomakExpr,()>, _id: Id, subst: &Subst| -> bool{
        let lexpr = egraph[subst[lhs]].nodes.iter().find(|e| e.can_convert_to_scalar_value());
        let rexpr = egraph[subst[rhs]].nodes.iter().find(|e| e.can_convert_to_scalar_value());
        lexpr.is_some() && rexpr.is_some()
    }
}

fn fetch_as_immutable_builtin_scalar_func(var: Var, egraph: &mut EGraph<TokomakExpr, ()>, id: Id, subst: &Subst)-> Option<BuiltinScalarFunction>{
    egraph[subst[var]].nodes.iter().flat_map(|expr| match expr{
        TokomakExpr::ScalarBuiltin(f)=> {
            if f.function_volatility() == FunctionVolatility::Immutable{
                Some(f.clone())
            }else{
                None
            }
        },
        _ => None
    }).nth(0)
}

fn is_immutable_scalar_builtin_function(func: Var)->impl Fn(&mut EGraph<TokomakExpr, ()>,  Id,  &Subst)->bool{
    move |egraph: &mut EGraph<TokomakExpr, ()>, id: Id, subst: &Subst|->bool{
        fetch_as_immutable_builtin_scalar_func(func, egraph, id, subst).is_some()
    }
 }

fn can_convert_to_scalar_value(var: Var)-> impl Fn(&mut EGraph<TokomakExpr, ()>, Id, &Subst)-> bool{
    move |egraph: &mut EGraph<TokomakExpr, ()>, _id: Id, subst: &Subst| -> bool{
        let expr = egraph[subst[var]].nodes.iter().find(|e| e.can_convert_to_scalar_value());
        expr.is_some()
    }
}



fn gt_var(lhs: Var, rhs: Var)->impl Fn(&mut EGraph<TokomakExpr,()>, Id, &Subst) -> bool{
    move |egraph: &mut EGraph<TokomakExpr,()>, id: Id, subst: &Subst| -> bool{
        if !can_convert_both_to_scalar_value(lhs, rhs)(egraph, id, subst){
            return false;
        }
        let lt = const_binop(lhs, rhs, Operator::Gt);
        let t_expr = match lt.evaluate(egraph,id, subst){
            Some(s) => s,
            None => return false,
        };
        match t_expr{
            TokomakExpr::Scalar(TokomakScalar::Boolean(Some(v)))=> return v,
            _ => return false,
        }

    }
}

fn convert_to_scalar_value(var: Var, egraph: &mut EGraph<TokomakExpr, ()>, _id: Id, subst: &Subst)->Result<ScalarValue, DataFusionError>{
    let expr = egraph[subst[var]].nodes.iter().find(|e| e.can_convert_to_scalar_value()).ok_or_else(|| DataFusionError::Internal("Could not find a node that was convertable to scalar value".to_string()))?;
    expr.try_into()
}



fn binop_type_coercion(mut lhs: ScalarValue,mut rhs:ScalarValue, op: &Operator)->Result<(ScalarValue, ScalarValue), DataFusionError>{
    let lhs_dt = lhs.get_datatype();
    let rhs_dt = rhs.get_datatype();
    let common_datatype = common_binary_type(&lhs_dt, &op, &rhs_dt)?;
    
    if lhs_dt != common_datatype{
        lhs = cast(lhs, &common_datatype)?;
    }
    if rhs_dt != common_datatype{
        rhs = cast(rhs, &common_datatype)?;
    }
    Ok((lhs, rhs))
}

fn cast(val: ScalarValue, cast_type: &DataType)->Result<ScalarValue, DataFusionError>{
    CastExpr::cast_scalar_default_options(val, cast_type)
}

fn can_perform_const_binary_op(lhs: Var, rhs: Var, op: Operator)->impl Fn(&mut EGraph<TokomakExpr, ()>, Id, &Subst)-> bool{
    move |egraph:  &mut EGraph<TokomakExpr, ()>, id: Id, subst: &Subst| -> bool{
        let lhs_lit = convert_to_scalar_value(lhs, egraph, id, subst);
        if lhs_lit.is_err(){
            return false;
        }
        let lhs_lit = lhs_lit.unwrap();

        let rhs_lit = convert_to_scalar_value(rhs, egraph, id, subst);
        if rhs_lit.is_err(){
            return false;
        }
        let rhs_lit = rhs_lit.unwrap();
        let res  = binop_type_coercion(lhs_lit, rhs_lit, &op);
        if res.is_err(){
            return false;
        }
        return true;
    }
}

fn const_binop(lhs: Var, rhs: Var, op: Operator)->ConstBinop<impl  Fn (TokomakExpr, TokomakExpr)->Option<TokomakExpr>, impl Fn(&&TokomakExpr)->bool>{
    ConstBinop(lhs, rhs, |e| e.can_convert_to_scalar_value(), move |l, r|->Option<TokomakExpr>{
        let mut lhs : ScalarValue = (&l).try_into().ok()?;
        let mut rhs: ScalarValue = (&r).try_into().ok()?;
        let ldt = lhs.get_datatype();
        let rdt = rhs.get_datatype();
        if ldt != rdt{
            let common_dt = common_binary_type(&ldt, &op, &rdt).ok()?;
            if ldt != common_dt {
                lhs = CastExpr::cast_scalar_default_options(lhs, &common_dt).ok()?;
            }
            if rdt != common_dt{
                rhs = CastExpr::cast_scalar_default_options(rhs, &common_dt).ok()?;
            }
        }
        
        
        let res = BinaryExpr::evaluate_values( crate::physical_plan::ColumnarValue::Scalar(lhs), crate::physical_plan::ColumnarValue::Scalar(rhs), &op,1).ok()?;
        let scalar =ScalarValue::try_from_columnar_value(res).ok()?;
        
        let t_expr: TokomakExpr = scalar.try_into().unwrap();
        Some(t_expr)
    })
}



struct ConstBinop<F: Fn (TokomakExpr, TokomakExpr)->Option<TokomakExpr>, M: Fn(&&TokomakExpr)->bool + 'static>(pub Var, pub Var, pub M, pub F);

impl<F: Fn (TokomakExpr, TokomakExpr)->Option<TokomakExpr>, M: Fn(&&TokomakExpr)->bool> ConstBinop<F, M>{
    fn evaluate(&self, egraph: &mut EGraph<TokomakExpr, ()>, _: Id, subst: &Subst)->Option<TokomakExpr>{
        let lhs = egraph[subst[self.0]].nodes.iter().find(|expr| self.2(expr));
        let lhs = match lhs{
            Some(v)=>v,
            None => return None,
        }.clone();
        let rhs =  egraph[subst[self.1]].nodes.iter().find(|expr| self.2(expr));
        let rhs = match rhs{
            Some(v)=>v,
            None => return None,
        }.clone();
        self.3(lhs, rhs)

    }
}

impl<F: Fn (TokomakExpr, TokomakExpr)->Option<TokomakExpr>, M: Fn(&&TokomakExpr)->bool> Applier<TokomakExpr, ()> for ConstBinop<F, M>{
    fn apply_one(&self, egraph: &mut EGraph<TokomakExpr, ()>, id: Id, subst: &Subst) -> Vec<Id> {
        match self.evaluate(egraph, id, subst){
            Some(e) => vec![egraph.add(e)],
            None => Vec::new(),
        }
    }
}



struct ConstCallApplier{
    pub func: Var,
    pub args: Var,
}

impl ConstCallApplier{
    fn apply_one_opt(&self, egraph: &mut EGraph<TokomakExpr, ()>, eclass: Id, subst: &Subst)->Option<Vec<Id>>{
        let fun = fetch_as_immutable_builtin_scalar_func(self.func, egraph, eclass,subst)?;
        let args = to_literal_list(self.args,egraph, eclass, subst)?;
        let num_args = args.len();

        let mut func_args = Vec::with_capacity(num_args);
        for id in args{
            let scalar = egraph[*id].nodes.iter()
            .flat_map(|expr|->Result<ScalarValue, DataFusionError>{ expr.try_into()} )
            .nth(0)
            .map(|s| s)?;
            func_args.push(scalar);
        }
        let func_args = coerce_func_args(&func_args, &crate::physical_plan::functions::signature(&fun)).ok()?;
        let func_args = func_args.into_iter().map(|s| ColumnarValue::Scalar(s)).collect::<Vec<ColumnarValue>>();
        let arg_types = func_args.iter().map(|arg| arg.data_type()).collect::<Vec<DataType>>();
        let return_ty = crate::physical_plan::functions::return_type(&fun, arg_types.as_slice()).ok()?;
        let fun_impl = crate::physical_plan::functions::create_immutable_impl(&fun).ok()?;
        let result:ColumnarValue = (&fun_impl)(&func_args).ok()?;
        let val: ScalarValue = ScalarValue::try_from_columnar_value(result).ok()?;

        let expr: TokomakExpr = val.try_into().ok()?;

        Some(vec![egraph.add(expr)]) 
    }
}


fn coerce_func_args(
    values: &[ScalarValue],
    signature: &Signature,
) -> Result<Vec<ScalarValue>, DataFusionError> {
    if values.is_empty() {
        return Ok(vec![]);
    }

    let current_types = (values)
        .iter()
        .map(|e| e.get_datatype() )
        .collect::<Vec<DataType>>();

    let new_types = data_types(&current_types, signature)?;

    (&values)
        .iter()
        .enumerate()
        .map(|(i, value)| cast(value.clone(), &new_types[i]))
        .collect::<Result<Vec<_>, DataFusionError>>()
}




impl Applier<TokomakExpr, ()> for ConstCallApplier{
    fn apply_one(&self, egraph: &mut EGraph<TokomakExpr, ()>, eclass: Id, subst: &Subst) -> Vec<Id> {
        match self.apply_one_opt(egraph, eclass, subst){
            Some(s) =>s,
            None => Vec::new(),
        }
    }
}


struct ConstCastApplier{
    value: Var,
    cast_type: Var,
}

fn convert_to_tokomak_type(var: Var, egraph: &mut EGraph<TokomakExpr, ()>, _id: Id, subst: &Subst)->Result<TokomakDataType, DataFusionError>{
    let expr: Option<&TokomakExpr> = egraph[subst[var]].nodes.iter().find(|e| matches!(e, TokomakExpr::Type(_)));
    let expr = expr.map(|e| match e{
        TokomakExpr::Type(t) => t.clone(),
        _ => panic!("Found TokomakExpr which was not TokomakExpr::Type, found {:?}", e),
    });
    expr.ok_or_else(|| DataFusionError::Internal("Could not convert to TokomakType".to_string()))
}

impl ConstCastApplier{
    fn apply_res(&self, egraph: &mut EGraph<TokomakExpr, ()>, id: Id, subst: &Subst)-> Option<TokomakExpr>{
        let value = convert_to_scalar_value(self.value, egraph, id, subst).ok()?;
        let ty = convert_to_tokomak_type(self.cast_type, egraph, id, subst).ok()?;
        let ty: DataType = ty.into();
        let val = CastExpr::cast_scalar_default_options(value,&ty).ok()?;
        val.try_into().ok()
    }
}
impl Applier<TokomakExpr, ()> for ConstCastApplier{
    fn apply_one(&self, egraph: &mut EGraph<TokomakExpr, ()>, eclass: Id, subst: &Subst) -> Vec<Id> {
        let cast_const_res = self.apply_res(egraph, eclass, subst);
        let cast_const_val = match cast_const_res{
            Some(v)=> v,
            None=> return Vec::new()
        };
        vec![egraph.add(cast_const_val)]
    }
}


fn is_literal(expr: Id, egraph: & EGraph<TokomakExpr, ()>, id: Id, subst: &Subst)->bool{
    let res = egraph[expr].nodes.iter().flat_map(|expr|->Result<ScalarValue, DataFusionError>{ expr.try_into()} ).nth(0);
    if res.is_none(){
        return false;
    }
    true
}

fn to_literal_list<'a>(var: Var, egraph: &'a EGraph<TokomakExpr, ()>, id: Id, subst: &Subst)->Option<&'a [Id]>{
    for expr in egraph[subst[var]].nodes.iter(){
        match expr{
            TokomakExpr::List(ref l)=>{
                if l.iter().all(|expr|  is_literal(*expr,egraph, id, subst)){
                    return Some(l.as_slice());
                }
            }
            _=>(),
        } 
    }
    None
}




#[cfg(test)]
mod test{
    use super::*;
    use std::{rc::Rc, sync::Arc};

    use arrow::{array::{ArrayRef, Float64Array}, datatypes::DataType};
    use egg::RecExpr;

    use crate::physical_plan::{functions::FunctionVolatility, udaf::AggregateUDF, udf::ScalarUDF};

    





    fn rewrite_expr(expr: &str)->Result<String, Box<dyn std::error::Error>>{
        let expr: RecExpr<TokomakExpr> = expr.parse()?;
        println!("unoptomized expr {:?}", expr);
        let runner = Runner::<TokomakExpr, (), ()>::default()
        .with_expr(&expr)
        .run(&rules());
        let mut extractor = Extractor::new(&runner.egraph, AstSize);
        let (_, best_expr) = extractor.find_best(runner.roots[0]);
        println!("optimized expr {:?}", best_expr);
        Ok(format!("{}", best_expr))
    }
    #[test]
    fn test_const_prop()->Result<(), Box<dyn std::error::Error>>{

     
    let expr_expected: Vec<(&'static str, &'static str)> =  vec![ 
        ("(cast (cast 2  utf8) float32)", "2"),
        ("(* 4 (call abs (list -1)))", "4"),
        ("(call_udf pow (list (+ 2.5 1.5) (- 1 1.5)))","(call_udf pow (list 4 -0.5))"), //Test udf inlining
        ("(call_udf pow_vol (list (+ 2.5 1.5) (- 1 1.5)))", "(call_udf pow_vol (list 4 -0.5))")
    ];

    for (expr, expected) in expr_expected{
        let rewritten_expr = rewrite_expr(  expr)?;
        assert_eq!(rewritten_expr, expected); 
    } 
    Ok(())
    }
}