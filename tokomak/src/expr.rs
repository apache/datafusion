use datafusion::{arrow::datatypes::DataType, error::DataFusionError, logical_plan::{Column, window_frames::WindowFrame}, physical_plan::{aggregates::AggregateFunction, functions::BuiltinScalarFunction, sort::SortExec, window_functions::WindowFunction}, scalar::ScalarValue};
use crate::{ScalarUDFName, SortSpec, UDAFName};
use egg::*;
use std::{fmt::Display, str::FromStr, sync::Arc};
use super::datatype::TokomakDataType;
use super::scalar::TokomakScalar;
use std::convert::TryInto;
use datafusion::error::Result as DFResult;



define_language! {
    pub enum TokomakExpr {
        "+" = Plus([Id;2]),
        "-" = Minus([Id;2]),
        "*" = Multiply([Id;2]),
        "/" = Divide([Id;2]),
        "%" = Modulus([Id;2]),
        
        "or" = Or([Id;2]),
        "and" = And([Id;2]),
        "=" = Eq([Id;2]),
        "<>" = NotEq([Id;2]),
        "<" = Lt([Id;2]),
        "<=" = LtEq([Id;2]),
        ">" = Gt([Id;2]),
        ">=" = GtEq([Id;2]),
        "regex_match"=RegexMatch([Id;2]),
        "regex_imatch"=RegexIMatch([Id;2]),
        "regex_not_match"=RegexNotMatch([Id;2]),
        "regex_not_imatch"=RegexNotIMatch([Id;2]),
        "is_distinct"=IsDistinctFrom([Id;2]),
        "is_not_distinct"=IsNotDistinctFrom([Id;2]),
    
        "not" = Not(Id),
    
    
        "is_not_null" = IsNotNull(Id),
        "is_null" = IsNull(Id),
        "negative" = Negative(Id),
        //expr low high 
        "between" = Between([Id;3]),
        "between_inverted" = BetweenInverted([Id;3]),

        "like" = Like([Id;2]),
        "not_like" = NotLike([Id;2]),
        "in_list" = InList([Id; 2]),
        "not_in_list" = NotInList([Id; 2]),
        "list" = List(Vec<Id>),
    
        //ScalarValue types
        ScalarBuiltin(BuiltinScalarFunction),
        AggregateBuiltin(AggregateFunction),
        WindowBuiltin(WindowFunction),
        Scalar(TokomakScalar),
    
        //THe fist expression for all of the function call types must be the corresponding function type
        //For UDFs this is a string, which is looked up in the ExecutionProps
        //The last expression must be a List and is the arguments for the function.
        "call" = ScalarBuiltinCall(FunctionCall),
        "call_udf"=ScalarUDFCall(FunctionCall),
        "call_agg" = AggregateBuiltinCall(FunctionCall),
        "call_agg_distinct"=AggregateBuiltinDistinctCall(FunctionCall),
        "call_udaf" = AggregateUDFCall(FunctionCall),
        //For window fuctions index 1 is the window partition
        //index 2 is the window order
        "call_win" = WindowBuiltinCallUnframed(WindowBuiltinCallUnframed),
        //For a framed window function index 3 is the frame parameters
        "call_win_framed" = WindowBuiltinCallFramed(WindowBuiltinCallFramed),
        //Last Id of the Sort node MUST be a SortSpec
        "sort_expr" = Sort(SortExpr),
        SortSpec(SortSpec),
        ScalarUDF(ScalarUDFName),
        AggregateUDF(UDAFName),
        
        Column(TokomakColumn),
        WindowFrame(WindowFrame),
        // cast id as expr. Type is encoded as symbol
        "cast" = Cast([Id;2]),
        "try_cast" = TryCast([Id;2]),
    }
    }
    


#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct BetweenExpr([Id;3]);
impl BetweenExpr{
    pub fn new(expr: Id, low: Id, high: Id)->Self{
        Self([expr, low, high])
    }
    pub fn expr(&self)->Id{
        self.0[0]
    }
    pub fn low(&self)->Id{
        self.0[1]
    }
    pub fn high(&self)->Id{
        self.0[2]
    }
}

#[rustfmt::skip]
impl LanguageChildren for BetweenExpr{
    fn len(&self) -> usize { 3 }
    fn can_be_length(n: usize) -> bool {n == 3}
    fn from_vec(v: Vec<Id>) -> Self { Self(v.try_into().unwrap())}
    fn as_slice(&self) -> &[Id] {&self.0[..]}
    fn as_mut_slice(&mut self) -> &mut [Id] {&mut self.0[..]}
}


#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]

pub struct FunctionCall([Id;2]);
impl FunctionCall{
    pub fn new(func: Id, args: Id)->Self{
        Self([func, args])
    }
    pub fn fun(&self)->Id{
        self.0[0]
    }
    pub fn args(&self)->Id{
        self.0[1]
    }
}
#[rustfmt::skip]
impl LanguageChildren for FunctionCall{
    fn len(&self) -> usize { self.0.len() }
    fn can_be_length(n: usize) -> bool { n == 2 }
    fn from_vec(v: Vec<Id>) -> Self { Self( [v[0], v[1]] ) }
    fn as_slice(&self) -> &[Id] { &self.0[..] }
    fn as_mut_slice(&mut self) -> &mut [Id] { &mut self.0[..] }
}


#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Debug)]
pub struct TokomakColumn{
    pub relation: Option<Symbol>,
    pub name: Symbol
}
impl Display for TokomakColumn{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.relation{
            Some(r) => write!(f, "#{}.{}", r, self.name),
            None => write!(f, "#{}", self.name),
        }
    }
}


impl TokomakColumn{
    pub fn new(relation: Option<impl Into<Symbol>>, name: impl Into<Symbol>)->Self{
        TokomakColumn{relation: relation.map(|r| r.into()),name: name.into()}
    }
}


impl FromStr for TokomakColumn{
    type Err= <Column as FromStr>::Err;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let c: Column = s.parse()?;
        Ok(c.into())
    }
}

impl From<Column> for TokomakColumn{
    fn from(c: Column) -> Self {
        TokomakColumn::new(c.relation, c.name)
    }
}
impl From<&Column> for TokomakColumn{
    fn from(c: &Column) -> Self {
        TokomakColumn::new(c.relation.as_ref().map(|s| s.as_str()), c.name.as_str())
    }
}

impl From<&TokomakColumn> for Column{
    fn from(tc: &TokomakColumn) -> Self {
        let relation = tc.relation.map(|s| s.as_str().to_owned());
        let name = tc.name.as_str().to_owned();
        Column { relation, name }
    }
}



/// Represents case expression with the form
///
/// CASE expression
///     WHEN value THEN result
///     [WHEN ...]
///     [ELSE result]
/// END
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]

pub struct CaseExprLiteral(Box<[Id]>);

impl CaseExprLiteral{
    pub fn new(expr: Id, when: Id, then: Id, r#else: Option<Id>)->Self{
        Self(Vec::into_boxed_slice(match r#else{
            Some(else_id)=>vec![expr,when,then,else_id],
            None => vec![expr, when, then]
        }))
    }

    pub fn expr(&self)->Id{
        self.0[0]
    }
    pub fn when(&self)->Id{
        self.0[1]
    }
    pub fn then(&self)->Id{
        self.0[2]
    }
    pub fn r#else(&self)->Option<Id>{
        if self.0.len() == 3{
            None
        }else{
            Some(self.0[3])
        }
    }

}
#[rustfmt::skip]
impl LanguageChildren for CaseExprLiteral{
    fn len(&self) -> usize { self.0.len() }
    fn can_be_length(n: usize) -> bool { n == 3 || n == 4 }
    fn from_vec(v: Vec<Id>) -> Self { Self(v.into_boxed_slice()) }
    fn as_slice(&self) -> &[Id] { &self.0[..] }
    fn as_mut_slice(&mut self) -> &mut [Id] { &mut self.0[..] }
}

///The CaseExprIfElse represents a case expression of the form
/// CASE WHEN condition THEN result
///      [WHEN ...]
///      [ELSE result]
/// END
/// When and then must always be the same length and 
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]

pub struct CaseExprIfElse(Box<[Id]>);

impl CaseExprIfElse{
    pub fn new(when: Id, then: Id, r#else: Option<Id>)->Self{
        let ids = match r#else{
            Some(else_id) => vec![when, then, else_id],
            None => vec![when, then],
        };
        Self(ids.into_boxed_slice())
    }
    pub fn when(&self)->Id{
        self.0[0]
    }
    pub fn then(&self)->Id{
        let test = Self::new(0.into(), 0.into() ,None).r#else();
        self.0[1]
    }
    pub fn r#else(&self)->Option<Id>{
        if self.0.len() == 3{
            Some(self.0[2])
        }else{
            None
        }
    }
}

#[rustfmt::skip]
impl LanguageChildren for CaseExprIfElse{
    fn len(&self) -> usize { self.0.len() }
    fn can_be_length(n: usize) -> bool { n == 2 || n == 3 }
    fn from_vec(v: Vec<Id>) -> Self { Self(v.into_boxed_slice()) }
    fn as_slice(&self) -> &[Id] { &self.0[..] }
    fn as_mut_slice(&mut self) -> &mut [Id] { &mut self.0[..] }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct InListExpr([Id;2]);
impl InListExpr{
    pub fn new(expr: Id,  list: Id)->Self{
        Self([expr, list])
    }
    pub fn expr(&self)->Id{
        self.0[0]
    }
    pub fn list(&self)->Id{
        self.0[1]
    }
}
#[rustfmt::skip]
impl LanguageChildren for InListExpr{
    fn len(&self) -> usize { self.0.len() }
    fn can_be_length(n: usize) -> bool { n == 2 }
    fn from_vec(v: Vec<Id>) -> Self { Self([v[0], v[1]]) }
    fn as_slice(&self) -> &[Id] { &self.0[..] }
    fn as_mut_slice(&mut self) -> &mut [Id] { &mut self.0[..] }
}
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct WindowBuiltinCallUnframed([Id;4]);
impl WindowBuiltinCallUnframed{
    pub fn new(fun: Id, args: Id, partition: Id, order_by: Id)->Self{
        Self([fun, args, partition, order_by])
    }
    pub fn fun(&self)->Id{
        self.0[0]
    }
    pub fn args_list(&self)->Id{
        self.0[1]
    }
    pub fn partition_list(&self)->Id{
        self.0[2]
    }
    pub fn order_by_list(&self)->Id{
        self.0[3]
    }
}
#[rustfmt::skip]
impl LanguageChildren for WindowBuiltinCallUnframed{
    fn len(&self) -> usize {5}
    fn can_be_length(n: usize) -> bool {n == 5}
    fn from_vec(v: Vec<Id>) -> Self { Self([v[0],v[1],v[2],v[3]]) }
    fn as_slice(&self) -> &[Id] { &self.0[..] }
    fn as_mut_slice(&mut self) -> &mut [Id] { &mut self.0[..] }
}


#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct WindowBuiltinCallFramed([Id;5]);
impl WindowBuiltinCallFramed{
    pub fn new(fun: Id, args: Id, partition: Id, order_by: Id, frame: Id)->Self{
        Self([fun, args, partition, order_by, frame])
    }
    pub fn fun(&self)->Id{
        self.0[0]
    }
    pub fn args_list(&self)->Id{
        self.0[1]
    }

    pub fn partition_list(&self)->Id{
        self.0[2]
    }
    pub fn order_by_list(&self)->Id{
        self.0[3]
    }
    pub fn frame(&self)->Id{
        self.0[4]
    }
}
#[rustfmt::skip]
impl LanguageChildren for WindowBuiltinCallFramed{
    fn len(&self) -> usize { 5 }
    fn can_be_length(n: usize) -> bool{ n == 5 }
    fn from_vec(v: Vec<Id>) -> Self { Self([v[0],v[1],v[2],v[3],v[4]]) }
    fn as_slice(&self) -> &[Id] { &self.0[..] }
    fn as_mut_slice(&mut self) -> &mut [Id] { &mut self.0[..] }
}
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct CastExpr([Id;2]);
impl CastExpr{
    pub fn new(expr: Id, cast_type: Id)->Self{
        Self([expr,cast_type])
    }
    pub fn expr(&self)->Id{
        self.0[0]
    }
    pub fn cast_type(&self)->Id{
        self.0[1]
    }
}
#[rustfmt::skip]
impl LanguageChildren for CastExpr{
    fn len(&self) -> usize {2}
    fn can_be_length(n: usize) -> bool { n == 2 }
    fn from_vec(v: Vec<Id>) -> Self { Self([v[0],v[1]]) }
    fn as_slice(&self) -> &[Id] { &self.0[..] }
    fn as_mut_slice(&mut self) -> &mut [Id] { &mut self.0[..] }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct SortExpr([Id; 2]);

impl SortExpr{
    pub fn new(expr: Id, sort_spec: Id)->Self{
        Self([expr, sort_spec])
    }
    pub fn expr(&self)->Id{
        self.0[0]
    }

    pub fn sort_spec(&self)->Id{
        self.0[1]
    }
}

#[rustfmt::skip]
impl LanguageChildren for SortExpr{
    fn len(&self) -> usize {2}
    fn can_be_length(n: usize) -> bool {n == 2}

    fn from_vec(v: Vec<Id>) -> Self {
        Self([v[0], v[1]])
    }

    fn as_slice(&self) -> &[Id] {
        &self.0[..]
    }

    fn as_mut_slice(&mut self) -> &mut [Id] {
        &mut self.0[..]
    }
}


impl TokomakExpr {
    #[allow(dead_code)]
    pub(crate) fn can_convert_to_scalar_value(&self) -> bool {
        matches!(self, TokomakExpr::Scalar(_))
    }
}

impl TryInto<ScalarValue> for &TokomakExpr {
    type Error = DataFusionError;

    fn try_into(self) -> Result<ScalarValue, Self::Error> {
        match self {
            TokomakExpr::Scalar(s) => Ok(s.clone().into()),
            e => Err(DataFusionError::Internal(format!(
                "Could not convert {:?} to scalar",
                e
            ))),
        }
    }
}

impl From<ScalarValue> for TokomakExpr {
    fn from(v: ScalarValue) -> Self {
        TokomakExpr::Scalar(v.into())
    }
}
