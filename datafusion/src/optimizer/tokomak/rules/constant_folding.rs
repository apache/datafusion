use std::convert::TryInto;
use super::super::TokomakDataType;
use crate::{error::DataFusionError, logical_plan::Operator, optimizer::tokomak::{Tokomak, TokomakExpr, scalar::TokomakScalar}, physical_plan::{ColumnarValue, expressions::{CastExpr, TryCastExpr}, functions::{BuiltinScalarFunction, FunctionVolatility, Signature}}, scalar::ScalarValue};
use arrow::datatypes::DataType;
use crate::physical_plan::type_coercion::data_types;

use super::utils::*;
use egg::{rewrite as rw, *};


fn rules()->Vec<Rewrite<TokomakExpr,()>>{ 
    let a:Var = "?a".parse().unwrap();
    let b: Var = "?b".parse().unwrap();
    //let c: Var = "?c".parse().unwrap();
    //let d: Var = "?d".parse().unwrap();
    //let x: Var ="?x".parse().unwrap();
    let func: Var = "?func".parse().unwrap();
    let args: Var = "?args".parse().unwrap();
    vec![
    //Move these 4 to simplification    
    rw!("const-prop-between"; "(and (>= ?e ?a) (<= ?e ?b))"=> "false" if var_gt(a,b)),
    rw!("const-prop-between_inverted"; "(and (< ?e ?a)(> ?e ?b))" => "true" if var_gt(a,b)),
    rw!("const-prop-binop-col-eq"; "(= ?a ?a)" => "(is_not_null ?a)"), 
    rw!("const-prop-binop-col-neq"; "(<> ?a ?a)" => "false"), 

    rw!("const-prop-add"; "(+ ?a ?b)"=>{ const_binop(a,b, Operator::Plus) } ),
    rw!("const-prop-sub"; "(- ?a ?b)" => { const_binop(a, b, Operator::Minus)}),
    rw!("const-prop-mul"; "(* ?a ?b)" => { const_binop(a,b, Operator::Multiply) } ),
    rw!("const-prop-div"; "(/ ?a ?b)" =>{  const_binop(a,b, Operator::Divide)} ),
    rw!("const-prop-mod"; "(% ?a ?b)" =>{const_binop(a,b, Operator::Modulo)}),

    rw!("const-prop-or"; "(or ?a ?b)" =>{const_binop(a,b, Operator::Or)}),
    rw!("const-prop-and"; "(and ?a ?b)" =>{const_binop(a,b, Operator::And)}),


    rw!("const-prop-binop-eq"; "(= ?a ?b)"=>{ const_binop(a,b, Operator::Eq)} ),
    rw!("const-prop-binop-neq"; "(<> ?a ?b)"=>{ const_binop(a,b, Operator::NotEq)}),

    rw!("const-prop-binop-gt"; "(> ?a ?b)"=>{ const_binop(a,b, Operator::Gt)} ),
    rw!("const-prop-binop-gte"; "(>= ?a ?b)"=>{ const_binop(a,b, Operator::GtEq)} ),

    rw!("const-prop-binop-lt"; "(< ?a ?b)"=>{ const_binop(a,b, Operator::Lt)} ),
    rw!("const-prop-binop-lte"; "(<= ?a ?b)"=>{ const_binop(a,b, Operator::LtEq)} ),

    rw!("const-prop-regex_match"; "(regex_match ?a ?b)"=>{ const_binop(a,b, Operator::RegexMatch)} ),
    rw!("const-prop-regex_imatch"; "(regex_imatch ?a ?b)"=>{ const_binop(a,b, Operator::RegexIMatch)} ),
    rw!("const-prop-regex_not_match"; "(regex_not_match ?a ?b)"=>{ const_binop(a,b, Operator::RegexNotMatch)} ),
    rw!("const-prop-regex_not_imatch"; "(regex_not_imatch ?a ?b)"=>{ const_binop(a,b, Operator::RegexNotIMatch)} ),

    rw!("const-prop-like"; "(like ?a ?b)"=>{ const_binop(a,b, Operator::Like)} ),
    rw!("const-prop-not_like"; "(not_like ?a ?b)"=>{ const_binop(a,b, Operator::NotLike)} ),



    rw!("const-prop-cast"; "(cast ?a ?b)" =>{ ConstCastApplier{value: a, cast_type: b}} ),
    rw!("const-prop-try_cast"; "(try_cast ?a ?b)" => {ConstTryCastApplier{value: a, cast_type: b}}),
    rw!("const-prop-call-scalar"; "(call ?func ?args)"=>{ ConstCallApplier{func, args}} if is_immutable_scalar_builtin_function(func))]
    
}


pub fn add_constant_folding_rules(optimizer: &mut Tokomak){
    for rule in rules(){
        optimizer.add_rule(rule);
    }
}

fn is_immutable_scalar_builtin_function(func: Var)->impl Fn(&mut EGraph<TokomakExpr, ()>,  Id,  &Subst)->bool{
    move |egraph: &mut EGraph<TokomakExpr, ()>, id: Id, subst: &Subst|->bool{
        fetch_as_immutable_builtin_scalar_func(func, egraph, id, subst).is_some()
    }
 }


 fn fetch_as_immutable_builtin_scalar_func(var: Var, egraph: &mut EGraph<TokomakExpr, ()>, _id: Id, subst: &Subst)-> Option<BuiltinScalarFunction>{
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
        let _return_ty = crate::physical_plan::functions::return_type(&fun, arg_types.as_slice()).ok()?;
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


struct ConstTryCastApplier{
    value: Var,
    cast_type: Var,
}


impl ConstTryCastApplier{
    fn apply_opt(&self, egraph: &mut EGraph<TokomakExpr, ()>, id: Id, subst: &Subst)-> Option<TokomakScalar>{
        let value = convert_to_scalar_value(self.value, egraph, id, subst).ok()?;
        let ty = convert_to_tokomak_type(self.cast_type, egraph, id, subst).ok()?;
        let ty: DataType = ty.into();
        let val = TryCastExpr::evaluate(ColumnarValue::Scalar(value), &ty).ok()?;// CastExpr::cast_scalar_default_options(value,&ty).ok()?;
        let v = ScalarValue::try_from_columnar_value(val).ok()?.into();
        Some(v)
        
    }
}

impl Applier<TokomakExpr, ()> for ConstTryCastApplier{
    fn apply_one(&self, egraph: &mut EGraph<TokomakExpr, ()>, eclass: Id, subst: &Subst) -> Vec<Id> {
        match self.apply_opt(egraph, eclass, subst){
            Some(v)=> vec![egraph.add(TokomakExpr::Scalar(v))],
            None => vec![]
        }
    }
}


#[cfg(test)]
mod test{
    use super::*;
    use egg::RecExpr;


    





    fn rewrite_expr(expr: &str)->Result<String, Box<dyn std::error::Error>>{
        let expr: RecExpr<TokomakExpr> = expr.parse()?;
        //println!("unoptomized expr {:?}", expr);
        let runner = Runner::<TokomakExpr, (), ()>::default()
        .with_expr(&expr)
        .run(&rules());
        let mut extractor = Extractor::new(&runner.egraph, AstSize);
        let (_, best_expr) = extractor.find_best(runner.roots[0]);
        //println!("optimized expr {:?}", best_expr);
        Ok(format!("{}", best_expr))
    }
    #[test]
    fn test_const_prop()->Result<(), Box<dyn std::error::Error>>{

     
    let expr_expected: Vec<(&'static str, &'static str)> =  vec![ 
        ("(cast (cast 2  utf8) float32)", "2"),
        ("(try_cast 'gibberish' float32)", "NULL"), //Test try cast
        ("(try_cast 4.5 utf8)", "4.5"),
        ("(* 4 (call abs (list -1)))", "4"), //Test function inlining 
        ("(call_udf udf[pow] (list (cast (+ 2.5 1.5) uint8) (- 1 1.5)))","(call_udf udf[pow] (list 4 -0.5))"), //Test const propagation within udf arguments
        //("(call_udf pow_vol (list (+ 2.5 1.5) (- 1 1.5)))", "(call_udf pow_vol (list 4 -0.5))") //FOr testing volatile udf argument optimzation without inlining. requires UDF registry to be availble at the optimize call
    ];

    for (expr, expected) in expr_expected{
        let rewritten_expr = rewrite_expr(  expr)?;
        assert_eq!(rewritten_expr, expected); 
    } 
    Ok(())
    }
}