

use std::cmp::Ordering;
use std::fmt;
use std::fmt::Write;
use std::hash::Hash;
use std::io::Empty;
use std::str::FromStr;
use std::sync::Arc;
use std::collections::HashMap;

use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::Schema;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_plan::Column;
use datafusion::logical_plan::DFSchema;
use datafusion::logical_plan::DFSchemaRef;
use datafusion::logical_plan::Expr;
use datafusion::logical_plan::JoinType;
use datafusion::logical_plan::LogicalPlan;
use datafusion::logical_plan::LogicalPlanBuilder;
use datafusion::logical_plan::Operator;
use datafusion::logical_plan::UserDefinedLogicalNode;
use datafusion::logical_plan::union_with_alias;
use datafusion::logical_plan::window_frames::WindowFrame;
use egg::Analysis;
use egg::EGraph;
use egg::FromOp;
use egg::Language;
use egg::LanguageChildren;
use egg::RecExpr;
use egg::Symbol;
use egg::define_language;
use datafusion::error::Result as DFResult;

use egg::Id;
use log::Log;
use tokio::runtime::Handle;
use crate::ScalarUDFName;
use crate::SortSpec;
use crate::Tokomak;
use crate::TokomakAnalysis;
use crate::UDAFName;
use crate::UDF;
use crate::expr::BetweenExpr;
use crate::expr::CastExpr;
use crate::expr::FunctionCall;
use crate::expr::InListExpr;
use crate::expr::SortExpr;
use crate::expr::TokomakColumn;
use crate::expr::TokomakExpr;
use crate::expr::WindowBuiltinCallFramed;
use crate::expr::WindowBuiltinCallUnframed;
use crate::scalar::TokomakScalar;
use crate::unexpected_tokomak_expr;

use datafusion::physical_plan::functions::BuiltinScalarFunction;
use datafusion::physical_plan::aggregates::AggregateFunction;
use datafusion::physical_plan::window_functions::{BuiltInWindowFunction, WindowFunction};

define_language! {
    pub enum TokomakLogicalPlan{
        //Logical plan nodes
        //input predicate
        "filter"=Filter([Id; 2]),
        //input, expressions, alias
        "project"=Projection([Id; 3]),
        //left right on
        "inner_join"=InnerJoin([Id;3]),
        //left right on
        "left_join"=LeftJoin([Id;3]),
        //left right on
        "right_join"=RightJoin([Id;3]),
        //left right on
        "full_join"=FullJoin([Id;3]),
        //left right on
        "semi_join"=SemiJoin([Id;3]),
        //left right on
        "anti_join"=AntiJoin([Id;3]),
        "ext"=Extension(Id),
        //left right
        "cross_join"=CrossJoin([Id;2]),
        //input count
        "limit"=Limit([Id; 2]),
        //TODO: Empty relation is tricky without putting schema in egraph which I'm not a huge fan of
        "empty_relation"=EmptyRelation(EmptyRelation),
        //TODO: figure out how to handle repartition plan
        "repartition"=Repartition(Id),
        "scan"=TableScan(TableScan),
        //plans alias
        "union"=Union([Id;2]),
        //input window_exprs
        "window"=Window([Id;2]),
        //input  aggr_exprs group_exprs
        "aggregate"=Aggregate([Id;3]),
        //input sort_exprs
        "sort"=Sort([Id;2]),
        "plist"=PList(Box<[Id]>),
        //expressions
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
        "elist" = EList(Box<[Id]>),
    
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
        "sort_expr" = SortExpr(SortExpr),
        //input alias
        "as"=ExprAlias([Id;2]),
        SortSpec(SortSpec),
        ScalarUDF(ScalarUDFName),
        AggregateUDF(UDAFName),
        
        Column(TokomakColumn),
        WindowFrame(WindowFrame),
        // cast id as expr. Type is encoded as symbol
        "cast" = Cast([Id;2]),
        "try_cast" = TryCast([Id;2]),
        PartitioningScheme(Partitioning),

        Table(Table),
        Values(SharedValues),
        TableProject(TableProject),
        LimitCount(usize),
        Schema(TokomakSchema),
        "keys"=JoinKeys(JoinKeys),
        
        Str(Symbol),
        "None"=None,
    }
}


#[derive(Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct TokomakSchema(Arc<DFSchema>);
impl fmt::Display for TokomakSchema{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0.as_ref())
    }
}
impl FromStr for TokomakSchema{
    type Err = DataFusionError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        return Err(DataFusionError::Internal(String::new()))
    }
}
#[derive(Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct TableProject(Arc<Vec<usize>>);
impl fmt::Display for TableProject{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0.as_ref())
    }
}
impl FromStr for TableProject{
    type Err=DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Err(DataFusionError::Internal(String::new()))
    }
}
fn try_create_language_child<L: LanguageChildren>(children: Vec<Id>)->Result<L, DataFusionError>{
    if L::can_be_length(children.len()){
        Ok(L::from_vec(children))
    }else{
        Err(DataFusionError::Internal(format!("Could not create {} node, {} number of children is unsupported", std::any::type_name::<L>(), children.len())))
    }

}


impl TokomakLogicalPlan{
    fn is_expr(&self)->bool{
        use TokomakLogicalPlan::*;
        matches!(&self, Plus(_) | Minus(_) | Multiply(_) | Divide(_) | Modulus(_) | Or(_) | And(_) | Eq(_) | NotEq(_) | Lt(_) | LtEq(_) | Gt(_) | GtEq(_) | RegexMatch(_) | RegexIMatch(_) | RegexNotMatch(_) | RegexNotIMatch(_) | IsDistinctFrom(_) | IsNotDistinctFrom(_) | Not(_) | IsNotNull(_) | IsNull(_) | Negative(_) | Between(_) | BetweenInverted(_) | Like(_) | NotLike(_) | InList(_) | NotInList(_) | ScalarBuiltinCall(_) | ScalarUDFCall(_) | AggregateBuiltinCall(_) | AggregateBuiltinDistinctCall(_) | AggregateUDFCall(_) | WindowBuiltinCallUnframed(_) | WindowBuiltinCallFramed(_) | SortExpr(_) | Column(_) | Cast(_) | TryCast(_)  |Scalar(_) )
    }
    fn is_plan(&self)->bool{
        use TokomakLogicalPlan::*;
        matches!(&self, Filter(_) | Projection(_) | InnerJoin(_) | LeftJoin(_) | RightJoin(_) | FullJoin(_) | SemiJoin(_) | AntiJoin(_) | Extension(_) | CrossJoin(_) | Limit(_) | EmptyRelation(_) | Repartition(_) | TableScan(_) | Union(_) | Window(_) | Aggregate(_) | Sort(_)  )
    }
}





#[derive(Clone)]
pub struct Table(pub Arc<dyn TableProvider>);

impl FromStr for Table{
    type Err=DataFusionError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Err(DataFusionError::Internal(String::new()))
    }
}
impl std::fmt::Debug for Table{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Table").finish()
    }
}
impl std::fmt::Display for Table{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f,"{:?}", self.0.table_type())
    }
}

impl PartialEq for Table{
    fn eq(&self, other: &Self) -> bool {
        let self_ptr = Arc::as_ptr(&self.0) as *const u8 as usize;
        let other_ptr = Arc::as_ptr(&other.0) as *const u8 as usize;
        self_ptr == other_ptr
    }
}

impl Eq for Table{}
impl Ord for Table{
    fn cmp(&self, other: &Self) -> Ordering {
        let self_ptr = Arc::as_ptr(&self.0) as *const u8 as usize;
        let other_ptr = Arc::as_ptr(&other.0) as *const u8 as usize;
        self_ptr.cmp(&other_ptr)
    }
}
impl PartialOrd for Table{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let self_ptr = (Arc::as_ptr(&self.0) as *const u8) as usize;
        let other_ptr = Arc::as_ptr(&other.0) as *const u8 as usize;
        Some(self_ptr.cmp(&other_ptr))
    }
}
impl Hash for Table{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let fat_ptr = Arc::as_ptr(&self.0);
        let ptr = (fat_ptr as *const u8) as usize;
        state.write_usize(ptr);
    }
}


#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Union(Box<[Id]>);
impl LanguageChildren for Union{
    fn len(&self) -> usize {
        self.0.len()
    }

    fn can_be_length(n: usize) -> bool {
        n == 2 || n == 3
    }

    fn from_vec(v: Vec<Id>) -> Self {
        Self(v.into_boxed_slice())
    }

    fn as_slice(&self) -> &[Id] {
        &self.0[..]
    }

    fn as_mut_slice(&mut self) -> &mut [Id] {
        &mut self.0[..]
    }
}


impl Union{
    pub fn new(inputs: Id, schema: Id, alias: Option<Id>)->Self{
        let union = match alias{
            Some(a)=> vec![inputs, schema, a],
            None => vec![inputs,schema],
        };
        Self(union.into_boxed_slice())
    }

    pub fn inputs(&self)->Id{
        self.0[0]
    }

    pub fn schema(&self)->Id{
        *self.0.last().unwrap()
    }
    pub fn alias(&self)->Option<Id>{
        if self.0.len() == 3{
            Some(self.0[1])
        }else{
            None
        }
    }
}

pub struct ExtensionNode([Id; 4]);
impl ExtensionNode{
    pub fn new(exprs: Id, input_plans: Id, ext_node: Id, schema: Id)->Self{
        Self([exprs, input_plans, ext_node,schema])
}
}


#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Partitioning{
    RoundRobinBatch(usize),
    Hash(usize),
}
impl std::fmt::Display for Partitioning{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self{
            Partitioning::RoundRobinBatch(batch_size) => write!(f, "Partitioning::RoundRobinBatch({})", batch_size),
            Partitioning::Hash(batch_size) => write!(f, "Partitioning::Hash({})", batch_size),
        }
    }
}

impl FromStr for Partitioning{
    type Err=DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Err(DataFusionError::Internal(String::new()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Projection(Box<[Id]>);
impl Projection{
    pub fn new(exprs: Id, input: Id, alias: Option<Id>, schema: Id)->Self{
        let s = match alias{
            Some(alias)=> vec![exprs, input, alias, schema],
            None => vec![exprs, input, schema],
        }.into_boxed_slice();
        Self(s)
    }
    pub fn exprs(&self)->Id{
        self.0[0]
    }
    pub fn input(&self)->Id{
        self.0[1]
    }
    pub fn schema(&self)->Id{
        *self.0.last().unwrap()
    }
    pub fn alias(&self)->Option<Id>{
        if self.0.len()!=4{
            None
        }else{
            Some(self.0[2])
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Filter([Id;3]);
impl Filter{
    pub fn new(predicate: Id, input: Id, schema: Id)->Self{
        Self([predicate, input, schema])
    }
    pub fn predicate(&self)->Id{
        self.0[0]
    }
    pub fn input(&self)->Id{
        self.0[1]
    }
    pub fn schema(&self)->Id{
        self.0[2]
    }
}

impl LanguageChildren for Filter{
    fn len(&self) -> usize {
        3
    }

    fn can_be_length(n: usize) -> bool {
        n == 3
    }

    fn from_vec(v: Vec<Id>) -> Self {
        Self([v[0],v[1],v[2]])
    }

    fn as_slice(&self) -> &[Id] {
        &self.0[..]
    }

    fn as_mut_slice(&mut self) -> &mut [Id] {
        &mut self.0[..]
    }
}
impl LanguageChildren for Projection{
    fn len(&self) -> usize {
        self.0.len()
    }

    fn can_be_length(n: usize) -> bool {
        n == 3 || n ==4
    }

    fn from_vec(v: Vec<Id>) -> Self {
        Self(v.into_boxed_slice())
    }

    fn as_slice(&self) -> &[Id] {
        &self.0[..]
    }

    fn as_mut_slice(&mut self) -> &mut [Id] {
        &mut self.0[..]
    }
}
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Hash, Clone)]
pub struct TableScan([Id; 5]);
impl TableScan{
    pub fn new(table_name: Id, source: Id, projection: Id, filters: Id, limit: Id)->Self{
        Self([table_name, source, projection, filters, limit])
    }
    pub fn name(&self)->Id{
        self.0[0]
    }
    pub fn source(&self)->Id{
        self.0[1]
    }
    pub fn projection(&self)->Id{
        self.0[2]
    }

    pub fn filters(&self)->Id{
        self.0[3]
    }
    pub fn limit(&self)->Id{
        self.0[4]
    }
}

impl LanguageChildren for TableScan{
    fn len(&self) -> usize {
        self.0.len()
    }

    fn can_be_length(n: usize) -> bool {
        n == 6
    }

    fn from_vec(v: Vec<Id>) -> Self {
        //egg requires that can_be_length is called before from_vec so this should be okay
        Self(v.try_into().expect("Incorrect length for <TableScan as LanguageChildren>::from_vec"))
    }

    fn as_slice(&self) -> &[Id] {
        &self.0[..]
    }

    fn as_mut_slice(&mut self) -> &mut [Id] {
        &mut self.0[..]
    }
}
#[derive(Debug,Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JoinKeys(Box<[Id]>);
impl LanguageChildren for JoinKeys{
    fn len(&self) -> usize {
        self.0.len()
    }

    fn can_be_length(n: usize) -> bool {
        //JoinKeys must be even in length
        n & 0x1 == 0
    }

    fn from_vec(v: Vec<Id>) -> Self {
        Self(v.into_boxed_slice())
    }

    fn as_slice(&self) -> &[Id] {
        &self.0[..]
    }

    fn as_mut_slice(&mut self) -> &mut [Id] {
        &mut self.0[..]
    }
}
impl fmt::Display for JoinKeys{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JoinKeys({:?})", self.0)
    }
}



pub struct PlanConverter<'a>{
    egraph: &'a mut EGraph<TokomakLogicalPlan, TokomakAnalysis>,
    
}
impl<'a> PlanConverter<'a>{
    /* 
    fn add_tokomak_datatype(&mut self, expr:&Expr, schema: &DFSchema)->DFResult<Id>{
        let dt = expr.get_type(schema)?;
        let nullable = expr.nullable(schema)?;
        let t_dt = dt.try_into()?;
        Ok(self.rec_expr.add(TokomakLogicalPlan::Expr(TokomakExpr::FieldType(FieldType{ty: t_dt, nullable}))))
    }*/
    #[allow(clippy::wrong_self_convention)]
    fn to_tokomak_expr(&mut self, expr: &Expr, schema:&DFSchema) -> Result<Id, DataFusionError> {
        let datatype = expr.get_type(schema)?;
        let nullable = expr.nullable(schema)?;
        let texpr: TokomakLogicalPlan = match expr {
            Expr::Alias(e, alias)=>{
                let id = self.to_tokomak_expr(e, schema)?;
                self.egraph.analysis.add_alias(e, id, alias.as_str())?;
                return Ok(id);
            }
            Expr::BinaryExpr { left, op, right } => {
                let left = self.to_tokomak_expr(left, schema)?;
                let right = self.to_tokomak_expr(right, schema)?;
                (match op {
                    Operator::Eq => TokomakLogicalPlan::Eq,
                    Operator::NotEq => TokomakLogicalPlan::NotEq,
                    Operator::Lt => TokomakLogicalPlan::Lt,
                    Operator::LtEq => TokomakLogicalPlan::LtEq,
                    Operator::Gt => TokomakLogicalPlan::Gt,
                    Operator::GtEq => TokomakLogicalPlan::GtEq,
                    Operator::Plus => TokomakLogicalPlan::Plus,
                    Operator::Minus => TokomakLogicalPlan::Minus,
                    Operator::Multiply => TokomakLogicalPlan::Multiply,
                    Operator::Divide => TokomakLogicalPlan::Divide,
                    Operator::Modulo => TokomakLogicalPlan::Modulus,
                    Operator::And => TokomakLogicalPlan::And,
                    Operator::Or => TokomakLogicalPlan::Or,
                    Operator::Like => TokomakLogicalPlan::Like,
                    Operator::NotLike => TokomakLogicalPlan::NotLike,
                    Operator::RegexMatch => TokomakLogicalPlan::RegexMatch,
                    Operator::RegexIMatch => TokomakLogicalPlan::RegexIMatch,
                    Operator::RegexNotMatch => TokomakLogicalPlan::RegexNotMatch,
                    Operator::RegexNotIMatch => TokomakLogicalPlan::RegexNotIMatch,
                    Operator::IsDistinctFrom => TokomakLogicalPlan::IsDistinctFrom,
                    Operator::IsNotDistinctFrom => TokomakLogicalPlan::IsNotDistinctFrom,
                })([left, right])
                
            }
            Expr::Column(c) => TokomakLogicalPlan::Column(c.clone().into()),
            Expr::Literal(s) => TokomakLogicalPlan::Scalar(s.clone().into()),
            Expr::Not(expr) => {
                let e = self.to_tokomak_expr(expr, schema)?;
                TokomakLogicalPlan::Not(e)
            }
            Expr::IsNull(expr) => {
                let e = self.to_tokomak_expr(expr, schema)?;
                TokomakLogicalPlan::IsNull(e)
            }
            Expr::IsNotNull(expr) => {
                let e = self.to_tokomak_expr(expr, schema)?;
                TokomakLogicalPlan::IsNotNull(e)
            }
            Expr::Negative(expr) => {
                let e = self.to_tokomak_expr(expr,schema)?;
                TokomakLogicalPlan::Negative(e)
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let e = self.to_tokomak_expr(expr, schema)?;
                let low = self.to_tokomak_expr(low, schema)?;
                let high = self.to_tokomak_expr(high, schema)?;
                if *negated {
                    TokomakLogicalPlan::BetweenInverted([e, low, high])
                } else {
                    TokomakLogicalPlan::Between([e, low, high])
                }
            }

            Expr::Cast { expr, .. } => {
                let e = self.to_tokomak_expr(expr, schema)?;
                //TODO: implment datatype conversion
                let datatype = e;
                TokomakLogicalPlan::Cast([e, datatype])
            }
            Expr::TryCast { expr, .. } => {
                //TODO: implment datatype conversion
                let e = self.to_tokomak_expr(expr, schema)?;
                let datatype = e;

                TokomakLogicalPlan::TryCast([e, datatype])
            }
            Expr::ScalarFunction { fun, args } => {
                let func = self.egraph.add(TokomakLogicalPlan::ScalarBuiltin(fun.clone()));
                let args = self.as_tokomak_expr_list(args, schema)?;
                TokomakLogicalPlan::ScalarBuiltinCall(FunctionCall::new(func, args ))
            }
            Expr::Alias(expr, _) => return self.to_tokomak_expr(expr, schema),
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let expr = self.to_tokomak_expr(expr, schema)?;
                let list = self.as_tokomak_expr_list(list, schema)?;
                let inlist = [expr, list];
                match negated {
                    false => TokomakLogicalPlan::InList(inlist),
                    true => TokomakLogicalPlan::NotInList(inlist),
                }
            }
            Expr::AggregateFunction {
                fun,
                args,
                distinct,
            } => {
                let func = self.egraph.add(TokomakLogicalPlan::AggregateBuiltin(fun.clone()));
                let args = self.as_tokomak_expr_list(args, schema)?;
                let fun_call = FunctionCall::new(func, args);
                match distinct {
                    true => {
                        TokomakLogicalPlan::AggregateBuiltinDistinctCall(fun_call)
                    }
                    false => TokomakLogicalPlan::AggregateBuiltinCall(fun_call),
                }
            }
            Expr::Case { expr, when_then_expr, else_expr } => todo!("Representation of Case expressions has not been decided yet"),
            Expr::Sort {
                expr,
                asc,
                nulls_first,
            } => {
                let sort_spec = match (asc, nulls_first) {
                    (true, true) => SortSpec::AscNullsFirst,
                    (true, false) => SortSpec::Asc,
                    (false, true) => SortSpec::Desc,
                    (false, false) => SortSpec::DescNullsFirst,
                };
                let expr_id = self.to_tokomak_expr(expr, schema)?;
                let spec_id = self.egraph.add(TokomakLogicalPlan::SortSpec(sort_spec));
                TokomakLogicalPlan::SortExpr(SortExpr::new(expr_id, spec_id))
            }
            Expr::ScalarUDF { fun, args } => {
                //TODO: Function registry
                //self.udf_registry.add_sudf(fun.clone());
                let fun_name: Symbol = fun.name.clone().into();
                let func = self
                    .egraph
                    .add(TokomakLogicalPlan::ScalarUDF(ScalarUDFName(fun_name)));
                let args = self.as_tokomak_expr_list(args, schema)?;
                let func_call = FunctionCall::new(func, args);
                TokomakLogicalPlan::ScalarUDFCall(func_call)
            }
            Expr::WindowFunction {
                fun,
                args,
                partition_by,
                order_by,
                window_frame,
            } => {
                let args = self.as_tokomak_expr_list(args, schema)?;
                let partition = self.as_tokomak_expr_list(partition_by,schema)?;
                let order_by = self.as_tokomak_expr_list(order_by, schema)?;
                let fun_id = self.egraph.add(TokomakLogicalPlan::WindowBuiltin(fun.clone()));
                
                match window_frame {
                    Some(frame) => {
                        let frame_id = self.egraph.add(TokomakLogicalPlan::WindowFrame(*frame));
                        TokomakLogicalPlan::WindowBuiltinCallFramed(WindowBuiltinCallFramed::new(fun_id, args, partition, order_by, frame_id))
                    }
                    None => TokomakLogicalPlan::WindowBuiltinCallUnframed(WindowBuiltinCallUnframed::new(fun_id, args, partition, order_by)),
                }
            }
            Expr::AggregateUDF { fun, args } => {
                //self.udf_registry.add_uadf(fun.clone());
                let fun_name: Symbol = fun.name.clone().into(); //Symbols are leaked at this point in time. Maybe different solution is required.
                let func = self
                    .egraph
                    .add(TokomakLogicalPlan::AggregateUDF(UDAFName(fun_name)));
                let args = self.as_tokomak_expr_list(args,schema)?;
                let func_call = FunctionCall::new(func, args);
                TokomakLogicalPlan::AggregateUDFCall(func_call)
            }
            //Expr::Wildcard => todo!(),
            //Expr::ScalarVariable(_) => todo!(),

            // not yet supported
            e => {
                return Err(DataFusionError::Internal(format!(
                    "Expression not yet supported in tokomak optimizer {:?}",
                    e
                )))
            }
        };
        //Ensure that the datatypes for all inserted nodes is consistent
        
        let id = self.egraph.add(texpr);
        self.egraph.analysis.add_datatype(expr, id, datatype, nullable)?;
        Ok(id)
    }
    
    fn as_tokomak_expr_list(&mut self, exprs: &[Expr], schema: &DFSchema)->Result<Id, DataFusionError>{
        let ids = exprs.iter().map(|e| self.to_tokomak_expr(e, schema)).collect::<Result<Vec<_>, _>>()?;
        Ok(self.egraph.add(TokomakLogicalPlan::EList(ids.into_boxed_slice())))
    }

    fn as_tokomak_plan_list(&mut self, plans: &[LogicalPlan])->Result<Id, DataFusionError>{
        let ids = Vec::with_capacity(plans.len());
        for plan in plans{
            let id = self.as_tokomak_plan(plan)?;
            self.add_schema(id, plan.schema());
        }
        Ok(self.egraph.add(TokomakLogicalPlan::PList(ids.into_boxed_slice())))
    }
    
    fn add_schema(&mut self, plan: Id, schema: &Arc<DFSchema>)->Result<(), DataFusionError>{
        todo!()
    }
    
    fn as_tokomak_str<'b>(&mut self, s: impl Into<Option<&'b str>>)->Id{
        let s = s.into();
        match s{
            Some(s)=>self.egraph.add(TokomakLogicalPlan::Str(s.into())),
            None => self.egraph.add(TokomakLogicalPlan::None),
        }
    }

    fn as_join_keys(&mut self, on: &[(Column, Column)], lschema: &Arc<DFSchema>, rschema: &Arc<DFSchema>)->Result<Id, DataFusionError>{
        let mut ids = Vec::with_capacity(on.len()*2);
        for (l, r) in on{
            let lid = self.egraph.add(TokomakLogicalPlan::Column(l.into()));
            let rid = self.egraph.add(TokomakLogicalPlan::Column(r.into()));
            let lfield = lschema.field_from_column(l)?;
            let rfield = rschema.field_from_column(r)?;
            self.egraph.analysis.add_datatype(&Expr::Column(l.clone()), lid, lfield.data_type().clone(), lfield.is_nullable())?;
            self.egraph.analysis.add_datatype(&Expr::Column(r.clone()), rid, rfield.data_type().clone(), rfield.is_nullable())?;
            ids.push(lid);
            ids.push(rid);
        }
        assert!(ids.len()%2 ==0, "JoinKeys length must be even");
        Ok(self.egraph.add(TokomakLogicalPlan::JoinKeys(JoinKeys::from_vec(ids))))
    }
    fn as_tokomak_plan(&mut self, plan: &LogicalPlan)->Result<Id, DataFusionError>{
        let tplan = match plan{
            LogicalPlan::Projection { expr, input, schema, alias } => {
                let exprs = self.as_tokomak_expr_list(expr, input.schema())?;
                let input = self.as_tokomak_plan(input)?;
                let alias = self.as_tokomak_str(alias.as_ref().map(|s| s.as_str()));
                TokomakLogicalPlan::Projection([input, exprs, alias])
            },
            LogicalPlan::Filter { predicate, input } => {
                let predicate = self.to_tokomak_expr(predicate, input.schema())?;
                
                let input = self.as_tokomak_plan(input)?;
                TokomakLogicalPlan::Filter([input, predicate])
            },
            LogicalPlan::Window { input, window_expr, schema } => {
                let window_expr = self.as_tokomak_expr_list(window_expr, input.schema())?;
                let input = self.as_tokomak_plan(input)?;
                TokomakLogicalPlan::Window([input, window_expr])
            },
            LogicalPlan::Aggregate { input, group_expr, aggr_expr, schema } => {
                let group_expr = self.as_tokomak_expr_list(group_expr, input.schema())?;
                let aggr_expr = self.as_tokomak_expr_list(aggr_expr,  input.schema())?;

                let input = self.as_tokomak_plan(input)?;
                TokomakLogicalPlan::Aggregate([input, aggr_expr, group_expr])
            },
            LogicalPlan::Sort { expr, input } => {
                let exprs = self.as_tokomak_expr_list(expr, input.schema())?;
                let input = self.as_tokomak_plan(input)?;
                TokomakLogicalPlan::Sort([input, exprs])
            },
            LogicalPlan::Join { left, right, on, join_type, schema,.. } =>{
                let on = self.as_join_keys(on, left.schema(), right.schema())?;
                let left = self.as_tokomak_plan(left)?;
                let right = self.as_tokomak_plan(right)?;
                
                
                (match join_type{
                    datafusion::logical_plan::JoinType::Inner => TokomakLogicalPlan::InnerJoin,
                    datafusion::logical_plan::JoinType::Left => TokomakLogicalPlan::LeftJoin,
                    datafusion::logical_plan::JoinType::Right => TokomakLogicalPlan::RightJoin,
                    datafusion::logical_plan::JoinType::Full => TokomakLogicalPlan::FullJoin,
                    datafusion::logical_plan::JoinType::Semi => TokomakLogicalPlan::SemiJoin,
                    datafusion::logical_plan::JoinType::Anti => TokomakLogicalPlan::AntiJoin,
                })([left, right, on])
            },
            LogicalPlan::CrossJoin { left, right, schema } => {
                let left = self.as_tokomak_plan(left)?;
                let right = self.as_tokomak_plan(right)?;
                TokomakLogicalPlan::CrossJoin([left, right])
            },
            LogicalPlan::Repartition { input, partitioning_scheme } => {
                let input = self.as_tokomak_plan(input)?;
                //TODO: Perform conversion
                let partitioning_scheme = input;
                todo!()
            },
            LogicalPlan::Union { inputs, schema, alias } => {
                let plans = self.as_tokomak_plan_list(inputs)?;
                let alias = self.as_tokomak_str(alias.as_ref().map(|s| s.as_str()));
                TokomakLogicalPlan::Union([plans,alias])
            },
            LogicalPlan::TableScan { table_name, source, projection, projected_schema, filters, limit } =>{
                let schema= source.schema().as_ref().clone();
                let dfschema = DFSchema::try_from_qualified_schema(table_name.as_str(), &schema) ?;
                
                let filters = self.as_tokomak_expr_list(filters, &dfschema)?;
                let source= self.egraph.add(TokomakLogicalPlan::Table(Table(source.clone())));
                let table_name = self.as_tokomak_str(table_name.as_str());
                let projection = match projection{
                    Some(proj) => self.egraph.add(TokomakLogicalPlan::TableProject(TableProject(Arc::new(proj.clone())))),
                    None => self.egraph.add(TokomakLogicalPlan::None),
                };
                let limit =  self.egraph.add(match limit{
                    Some(lim) => TokomakLogicalPlan::LimitCount(*lim),
                    None => TokomakLogicalPlan::None,
                });

                TokomakLogicalPlan::TableScan(TableScan::new(table_name, source, projection, filters, limit))
            },
            LogicalPlan::EmptyRelation { produce_one_row, schema } => {
                let produce_one_row = TokomakScalar::Boolean(Some(*produce_one_row));
                let produce_one_row = self.egraph.add(TokomakLogicalPlan::Scalar(produce_one_row));
                todo!();
                //TokomakLogicalPlan::EmptyRelation(produce_one_row)
            }
            LogicalPlan::Limit { n, input } => {
                let n = self.egraph.add(TokomakLogicalPlan::LimitCount(*n));
                let input = self.as_tokomak_plan(input)?;
                todo!()
                //TokomakLogicalPlan::Limit([input, n])  
            },
            plan @ LogicalPlan::CreateExternalTable { .. } |
            plan @ LogicalPlan::Values { .. } |
            plan @ LogicalPlan::Explain { .. } |
            plan @ LogicalPlan::Analyze { .. } |
            plan @ LogicalPlan::Extension { .. } =>  return Err(DataFusionError::NotImplemented(format!("The logical plan {:?} will not be implemented for the Tokomak optimizer", plan))),
        };
        let id = self.egraph.add(tplan);
        self.egraph.analysis.add_schema(plan,id )?;
        Ok(id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Limit([Id;3]);
impl Limit{
    pub fn new(input: Id, limit: Id, schema: Id)->Self{
        Self([input,limit, schema])
    }

    pub fn input(&self)->Id{
        self.0[0]
    }
    pub fn limit(&self)->Id{
        self.0[1]
    }
    pub fn schema(&self)->Id{
        self.0[2]
    }
}

impl LanguageChildren for Limit{
    fn len(&self) -> usize {
        self.0.len()
    }

    fn can_be_length(n: usize) -> bool {
        n == 3
    }

    fn from_vec(v: Vec<Id>) -> Self {
        Self(v.try_into().unwrap())
    }

    fn as_slice(&self) -> &[Id] {
        &self.0[..]
    }

    fn as_mut_slice(&mut self) -> &mut [Id] {
        &mut self.0[..]
    }
}

pub fn to_tokomak_plan(plan: &LogicalPlan, egraph: &mut EGraph<TokomakLogicalPlan, TokomakAnalysis>)->Result<Id, DataFusionError>{
    let mut converter = PlanConverter{egraph};
    converter.as_tokomak_plan(plan)
}

fn unexpected_node(curr_node: &'static str, unexpected_plan: &TokomakLogicalPlan, expected: &'static str)->DataFusionError{
    DataFusionError::Internal(format!("{} expected a node of type {} found {:?}", curr_node, expected, unexpected_plan))
}

fn unexpected_expr(node: &TokomakLogicalPlan, parent: &'static str, expected: &'static str)->DataFusionError{
        DataFusionError::Internal(format!("Expected node of {}, found {:?}", expected, node))
}


struct TokomakPlanConverter<'a> {
    udf_reg: &'a HashMap<String, UDF>,
    refs: &'a [TokomakLogicalPlan],
    analysis: &'a TokomakAnalysis,
    eclasses: &'a [Id]
}
impl<'a> TokomakPlanConverter<'a>{
    fn new(
        udf_reg: &'a HashMap<String, UDF>,
        rec_expr: &'a RecExpr<TokomakLogicalPlan>,
        analysis: &'a TokomakAnalysis,
        eclasses: &'a [Id]
    ) -> Self {
        Self {
            udf_reg,
            refs: rec_expr.as_ref(),
            analysis,
            eclasses
        }
    }
    fn get_ref(&self, id: Id)->&TokomakLogicalPlan{
        let idx: usize = id.into();
        &self.refs[idx]
    }
    fn convert_inlist(&self, [expr, list]: &[Id; 2], negated: bool)->Result<Expr, DataFusionError>{
        let name = if negated{ "NotInList"} else{"InList"};
        let elist = match self.get_ref(*list){
            TokomakLogicalPlan::EList(l) => l,
            node => return Err(unexpected_expr(node, name, "List")),
        };
        let expr = self.convert_to_expr(*expr)?.into();

        let list = elist.iter().map(|i| self.convert_to_expr(*i)).collect::<DFResult<Vec<_>>>()?;
        Ok(Expr::InList{ expr, list, negated })
    }


    fn to_binary_op(&self, [left, right]: &[Id;2], op: Operator)->Result<Expr, DataFusionError>{
        let left = self.convert_to_expr(*left)?;
        let right = self.convert_to_expr(*right)?;
        //println!("{:?} {:?} {:?}", left, op, right);
        Ok(datafusion::logical_plan::binary_expr(left, op, right))
    }

    fn extract_datatype(&self, id: Id, parent: &'static str)->Result<DataType, DataFusionError>{
        todo!();
        //let field_ty = match self.get_ref(id) {
        //    TokomakLogicalPlan::Expr(TokomakExpr::FieldType(s)) => s,
        //    e => return Err(unexpected_expr(e, parent, "TokmakExpr::FieldType")),
        //};
        //Ok(field_ty.ty.into())
    }

    fn convert_to_expr(&self, id: Id)->Result<Expr, DataFusionError>{
        Ok(self.convert_to_expr_inner(id).unwrap())
    }
    fn convert_to_expr_inner(&self, id: Id)->Result<Expr, DataFusionError>{
            
            let alias = self.get_alias(id);
            let expr = match self.get_ref(id) {
                TokomakLogicalPlan::Plus(ids) => self.to_binary_op(ids, Operator::Plus)?,
                TokomakLogicalPlan::Minus(ids) => self.to_binary_op(ids, Operator::Minus)?,
                TokomakLogicalPlan::Divide(ids) =>self.to_binary_op(ids, Operator::Divide)?,
                TokomakLogicalPlan::Modulus(ids) => self.to_binary_op(ids, Operator::Modulo)?,
                TokomakLogicalPlan::Multiply(ids) => self.to_binary_op(ids, Operator::Multiply)?,
                TokomakLogicalPlan::Or(ids) => self.to_binary_op(ids, Operator::Or)?,
                TokomakLogicalPlan::And(ids) => self.to_binary_op(ids, Operator::And)?,
                TokomakLogicalPlan::Eq(ids) => self.to_binary_op(ids, Operator::Eq)?,
                TokomakLogicalPlan::NotEq(ids) => self.to_binary_op(ids, Operator::NotEq)?,
                TokomakLogicalPlan::Lt(ids) => self.to_binary_op(ids, Operator::Lt)?,
                TokomakLogicalPlan::LtEq(ids) =>self.to_binary_op(ids, Operator::LtEq)?,
                TokomakLogicalPlan::Gt(ids) => self.to_binary_op(ids, Operator::Gt)?,
                TokomakLogicalPlan::GtEq(ids) => self.to_binary_op(ids, Operator::GtEq)?,
                TokomakLogicalPlan::Like(ids) => self.to_binary_op(ids, Operator::Like)?,
                TokomakLogicalPlan::NotLike(ids) => self.to_binary_op(ids, Operator::NotLike)?,
                TokomakLogicalPlan::RegexMatch(ids) => self.to_binary_op(ids, Operator::RegexMatch)?,
                TokomakLogicalPlan::RegexIMatch(ids) => self.to_binary_op(ids, Operator::RegexIMatch)?,
                TokomakLogicalPlan::RegexNotMatch(ids) => self.to_binary_op(ids, Operator::RegexNotMatch)?,
                TokomakLogicalPlan::RegexNotIMatch(ids) => self.to_binary_op(ids, Operator::RegexNotIMatch)?,
                TokomakLogicalPlan::IsDistinctFrom(ids)=>self.to_binary_op(ids, Operator::IsNotDistinctFrom)?,
                TokomakLogicalPlan::IsNotDistinctFrom(ids)=>self.to_binary_op(ids, Operator::IsNotDistinctFrom)?,
                TokomakLogicalPlan::Not(expr) => {
                    let l = self.convert_to_expr(*expr)?;
                    Expr::Not(Box::new(l))
                }
                TokomakLogicalPlan::IsNotNull(expr) => {
                    let l = self.convert_to_expr(*expr)?;
                    Expr::IsNotNull(Box::new(l))
                }
                TokomakLogicalPlan::IsNull(expr) => {
                    let l = self.convert_to_expr(*expr)?;
                    Expr::IsNull(Box::new(l))
                }
                TokomakLogicalPlan::Negative(expr) => {
                    let l = self.convert_to_expr(*expr)?;
                    Expr::Negative(Box::new(l))
                }
                TokomakLogicalPlan::Between([expr, low, high]) => {
                    let expr = self.convert_to_expr(*expr)?;
                    let low_expr = self.convert_to_expr(*low)?;
                    let high_expr = self.convert_to_expr(*high)?;
                    Expr::Between {
                        expr: Box::new(expr),
                        negated: false,
                        low: Box::new(low_expr),
                        high: Box::new(high_expr),
                    }
                }
                TokomakLogicalPlan::BetweenInverted([expr, low, high]) => {
                    let expr = self.convert_to_expr(*expr)?;
                    let low_expr = self.convert_to_expr(*low)?;
                    let high_expr = self.convert_to_expr(*high)?;
                    Expr::Between {
                        expr: Box::new(expr),
                        negated: false,
                        low: Box::new(low_expr),
                        high: Box::new(high_expr),
                    }
                }
                TokomakLogicalPlan::Column(col) => Expr::Column(col.into()),
                TokomakLogicalPlan::Cast([expr, dt]) => {
    
                    let l = self.convert_to_expr(*expr)?;
                    let dt = match self.get_ref(*dt) {
                        node => return Err(unexpected_expr(node, "TypedColumn", "FieldType")),
                    };
                    //let dt: DataType = dt.ty.into();
                    //TODO: implment conversion
                    Expr::Cast { expr: Box::new(l), data_type: DataType::Int8}
                }
                TokomakLogicalPlan::InList(inlist)=>self.convert_inlist(inlist, false)?,
                TokomakLogicalPlan::NotInList(inlist)=>self.convert_inlist(inlist, true)?,
                TokomakLogicalPlan::Scalar(s) => Expr::Literal(s.clone().into()),
                
                TokomakLogicalPlan::ScalarBuiltinCall(func_call) => {
                    let fun = match self.get_ref(func_call.fun()){
                        TokomakLogicalPlan::ScalarBuiltin(f)=>f,
                        f => return Err(unexpected_expr(f, "ScalarBuiltinCall", "TokomakExpr::ScalarBuiltin")),
                    };
                    let args = self.extract_expr_list(func_call.args(), "ScalarBuiltinCall.args")?;
                    Expr::ScalarFunction{
                        fun:fun.clone(),
                        args,
                    }
                },
                TokomakLogicalPlan::TryCast([expr,dt]) => {
                    let l = self.convert_to_expr(*expr)?;
                    let dt = self.extract_datatype(*dt , "TryCast")?;
                    Expr::TryCast { expr: Box::new(l), data_type: dt}
                },
                TokomakLogicalPlan::AggregateBuiltin(_) => todo!(),
                TokomakLogicalPlan::ScalarUDFCall(func_call) => {
                    let args = self.extract_expr_list(func_call.args(), "ScalarUDFCall.args")?;
                    let name = self.get_ref(func_call.fun());
                    let name = self.extract_alias(func_call.fun(), "ScalarUDFCall.name")?.ok_or_else(|| DataFusionError::Internal("Could not find the name of the scalar UDF".to_string()))?;
                    let fun = match self.udf_reg.get(&name){
                        Some(s) => s,
                        None => return Err(DataFusionError::Internal(format!("Did not find the scalar UDF {} in the registry", name))),
                    };
                    let fun = match fun{
                        UDF::Scalar(s) => s.clone(),
                        UDF::Aggregate(_) => return Err(DataFusionError::Internal(format!("Did not find scalar UDF named {}. Found an aggregate UDF instead.", name))),
                    };
                    Expr::ScalarUDF{
                        fun,
                        args
                    }
                },
                TokomakLogicalPlan::AggregateBuiltinCall(func_call) =>{
                    let args = self.extract_expr_list(func_call.args(), "AggregateBuiltinCall.args")?;
                    let fun = match self.get_ref(func_call.fun()){
                        TokomakLogicalPlan::AggregateBuiltin(f)=>f.clone(),
                        e => return Err(unexpected_expr(e, "AggregateBuiltinCall", "AggregateBuiltin")),
                    };
                    Expr::AggregateFunction{
                        fun,
                        args,
                        distinct: false
                    }
                },
                TokomakLogicalPlan::AggregateBuiltinDistinctCall(func_call) => {
                    let args = self.extract_expr_list(func_call.args(), "AggregateBuiltinDistinctCall.args")?;
                    let fun = match self.get_ref(func_call.fun()){
                        TokomakLogicalPlan::AggregateBuiltin(f)=>f.clone(),
                        e => return Err(unexpected_expr(e, "AggregateBuiltinDistinctCall.args", "TokomakExpr::AggregateBuiltin")),
                    };
                    Expr::AggregateFunction{
                        fun,
                        args,
                        distinct: true
                    }
                },
                
                TokomakLogicalPlan::AggregateUDFCall(func_call) => {
                    let udf_name = match self.get_ref(func_call.fun()){
                        TokomakLogicalPlan::AggregateUDF(name)=> name.to_string(),
                        e =>    return Err(DataFusionError::Internal(format!("Expected an AggregateUDF node at index 0 of AggregateUDFCall, found: {:?}",e)))
                    };
                    let fun = match self.udf_reg.get(&udf_name){
                        Some(s) => s,
                        None => return Err(DataFusionError::Internal(format!("Did not find the aggregate UDF '{}' in the registry", udf_name))),
                    };
                    let fun = match fun{
                        UDF::Aggregate(a) =>a.clone() ,
                        UDF::Scalar(_) => return Err(DataFusionError::Internal(format!("Did not find aggregate UDF named {}. Found a scalar UDF instead.", udf_name))),
                    };
                    let args = self.extract_expr_list(func_call.args(), "AggregateUDF.args")?;
                    Expr::AggregateUDF{
                        fun,
                        args
                    }
                },
                TokomakLogicalPlan::WindowBuiltinCallUnframed(call) => {
                    let fun = match self.get_ref(call.fun()){
                        TokomakLogicalPlan::WindowBuiltin(f)=>f.clone(),
                        node => return Err(unexpected_expr(node, "WindowBuiltinCallUnframed.fun", "TokomakExpr::WindowBuiltin")),
                    };
                    let partition_by = self.extract_expr_list(call.partition_list(), "WindowBuiltinCallUnframed")?;
                    let order_by = self.extract_expr_list(call.order_by_list(), "WindowBuiltinCallUnframed")?;
                    let args = self.extract_expr_list(call.args_list(), "WindowBuiltinCallUnframed")?;
                    Expr::WindowFunction{
                        fun,
                        partition_by,
                        order_by,
                        args,
                        window_frame: None
                    }
                },
                TokomakLogicalPlan::WindowBuiltinCallFramed(call) => {
                    let fun = match self.get_ref(call.fun()){
                        TokomakLogicalPlan::WindowBuiltin(f)=>f.clone(),
                        e=>return Err(unexpected_expr(e, "WindowBuiltinCallFramed", "TokomakExpr::WindowBuiltin")),
                    };
                    let partition_by = self.extract_expr_list(call.partition_list(), "WindowBuiltinCallFramed")?;
                    let order_by = self.extract_expr_list(call.order_by_list(), "WindowBuiltinCallFramed")?;
                    let args = self.extract_expr_list(call.args_list(), "WindowBuiltinCallFramed")?;

                    let window_frame = match self.get_ref(call.frame()){
                        TokomakLogicalPlan::WindowFrame(f)=>*f,
                        e=> return Err(unexpected_expr(e, "WindowBuiltinCallUnframed.frame", "TokomakExpr::WindowFrame")),
                    };
                    Expr::WindowFunction{
                        fun,
                        partition_by,
                        order_by,
                        args,
                        window_frame: Some(window_frame)
                    }
                },
                TokomakLogicalPlan::SortExpr(sort) => {
                    let expr = self.convert_to_expr(sort.expr())?;
                    let sort_spec = match self.get_ref(sort.sort_spec()){
                        TokomakLogicalPlan::SortSpec(s)=>*s,
                        e => return Err(unexpected_expr(e, "Sort.sort_spec", "TokomakExpr::SortSpec")),
                    };
                    let (asc, nulls_first)= match sort_spec{
                        SortSpec::Asc => (true,false),
                        SortSpec::Desc => (false, false),
                        SortSpec::AscNullsFirst => (true, true),
                        SortSpec::DescNullsFirst => (false,true),
                    };
                    Expr::Sort{
                        expr: Box::new(expr),
                        asc,
                        nulls_first
                    }
                },
                unconvertible_expr@ (
                TokomakLogicalPlan::ScalarBuiltin(_) |
                TokomakLogicalPlan::AggregateUDF(_) |
                TokomakLogicalPlan::ScalarUDF(_) |
                 TokomakLogicalPlan::SortSpec(_) |
                 TokomakLogicalPlan::WindowFrame(_)|
                 TokomakLogicalPlan::WindowBuiltin(_)|
                  TokomakLogicalPlan::EList(_)|
                TokomakLogicalPlan::ExprAlias(_))=>return Err(DataFusionError::Internal(format!("The Expr {:?} should only ever be converted within a parent expression conversion. E.g. SortSpec conversion should be handled when converting Sort", unconvertible_expr))),
                plan @ (
                TokomakLogicalPlan::Schema(_)|
                TokomakLogicalPlan::Filter(_)|
                TokomakLogicalPlan::Projection(_)|
                TokomakLogicalPlan::InnerJoin(_)|
                TokomakLogicalPlan::LeftJoin(_)|
                TokomakLogicalPlan::RightJoin(_)|
                TokomakLogicalPlan::FullJoin(_)|
                TokomakLogicalPlan::SemiJoin(_)|
                TokomakLogicalPlan::AntiJoin(_)|
                TokomakLogicalPlan::Extension(_)|
                TokomakLogicalPlan::CrossJoin(_)|
                TokomakLogicalPlan::Limit(_)|
                TokomakLogicalPlan::EmptyRelation(_)|
                TokomakLogicalPlan::Repartition(_)|
                TokomakLogicalPlan::TableScan(_)|
                TokomakLogicalPlan::Union(_)|
                TokomakLogicalPlan::Window(_)|
                TokomakLogicalPlan::Aggregate(_)|
                TokomakLogicalPlan::Sort(_)|
                TokomakLogicalPlan::PList(_)|
                TokomakLogicalPlan::ScalarBuiltin(_)|
                TokomakLogicalPlan::WindowBuiltin(_)|
                TokomakLogicalPlan::SortSpec(_)|
                TokomakLogicalPlan::ScalarUDF(_)|
                TokomakLogicalPlan::AggregateUDF(_)|
                TokomakLogicalPlan::WindowFrame(_)|
                TokomakLogicalPlan::PartitioningScheme(_)|
                TokomakLogicalPlan::Table(_)|
                TokomakLogicalPlan::Values(_)|
                TokomakLogicalPlan::TableProject(_)|
                TokomakLogicalPlan::LimitCount(_)|
                TokomakLogicalPlan::JoinKeys(_)|
                TokomakLogicalPlan::Str(_)|
                TokomakLogicalPlan::None )=> return Err(DataFusionError::Internal(format!("Expected expression found plan: {}", plan)))
            };
           
        Ok(match alias{
            Some(s)=> {
                println!("Found alias {} for {:?}", s, expr);
                Expr::Alias(expr.into(), s.clone())
            },
            None => {
                //println!("Did not ")
                expr
            },
        })
    }

    fn extract_plan_list(&self, id: Id, parent: &'static str)->Result<Vec<LogicalPlanBuilder> , DataFusionError>{
        let list = match self.get_ref(id){
            TokomakLogicalPlan::PList(plans)=>plans,
            plan => return Err(unexpected_node(parent, plan, "PList")),
        };
        if list.is_empty(){
            return Ok(Vec::new())
        }
        let mut plans = Vec::with_capacity(list.len());
        for id in list.iter(){
            plans.push(self.convert_to_builder(*id)?);
        }
        Ok(plans)
    }

    fn extract_expr_list(&self, id: Id, parent: &'static str)->Result<Vec<Expr>, DataFusionError>{
        
        let exprs = match self.get_ref(id){
            TokomakLogicalPlan::EList(list)=>{
                if parent == "Projection.expr"{
                    let mut conv_list = Vec::with_capacity(list.len());
                    for i in list.iter(){
                        let idx :usize= (*i).into();
                        let eclass_id=self.eclasses[idx];
                        conv_list.push(eclass_id);
                    }
                    println!("{:?}", conv_list);
                }   
                list.iter().map(|i| self.convert_to_expr(*i)).collect::<Result<Vec<_>,_>>()?
            },
            plan =>return Err( unexpected_node(parent, plan, "EList"))
        };
        Ok(exprs)
    }
    fn extract_column(&self,id: Id, parent: &'static str)->Result<Column, DataFusionError>{
        let col = match self.get_ref(id){
            TokomakLogicalPlan::Column(c)=>{
                Column{
                    relation: c.relation.as_ref().map(ToString::to_string),
                    name: c.name.to_string(),
                }
            }
            plan => return Err(unexpected_node(parent, plan, "TokomakExpr::Column"))
        };
        Ok(col)
    }
    fn convert_to_join(&self, join_type: JoinType, [left, right, on]: &[Id;3])->Result<LogicalPlanBuilder, DataFusionError>{
        let plan_name = match join_type{
            JoinType::Inner => "JoinType::Inner",
            JoinType::Left => "JoinType::Left",
            JoinType::Right => "JoinType::Right",
            JoinType::Full => "JoinType::Full",
            JoinType::Semi => "JoinType::Semi",
            JoinType::Anti => "JoinType::Anti",
        };
        let left = self.convert_to_builder(*left)?;
        let right = self.convert_to_builder(*right)?;
        let join_keys = match self.get_ref(*on){
            TokomakLogicalPlan::JoinKeys(keys)=>keys,
            plan => return Err(DataFusionError::Internal(format!("The join of type {:?} expected a node of type JoinKeys, found {:?}", join_type, plan))),
        };
        let mut left_join_keys = Vec::with_capacity(join_keys.len()/2);
        let mut right_join_keys = Vec::with_capacity(join_keys.len()/2);
        assert!(join_keys.len() %2 ==0, "Found JoinKeys with odd number of keys, this should never happen"); 
        for keys in join_keys.as_slice().chunks_exact(2){
            let left_key = keys[0];
            let right_key = keys[1];
            left_join_keys.push(self.extract_column(left_key, plan_name)?);
            right_join_keys.push(self.extract_column(right_key, plan_name)?)
        }
        left.join(&right.build()?, join_type, (left_join_keys, right_join_keys))
    }
    fn extract_alias(&self, id: Id, parent: &'static str)->Result<Option<String>, DataFusionError>{
        let alias = match self.get_ref(id){
            TokomakLogicalPlan::Str(sym) => Some(sym.to_string()),
            TokomakLogicalPlan::None => None,
            plan => return Err(unexpected_node(parent, plan, "Str or None")),
        };
        Ok(alias)
    }
    fn extract_schema(&self, id: Id, parent: &'static str)->Result<DFSchema, DataFusionError>{
        todo!()
    }
    fn convert_to_builder(&self, id: Id)->Result<LogicalPlanBuilder, DataFusionError>{
        let res = Ok(self.convert_to_builder_inner(id).unwrap());
        res
    }

    fn get_alias(&self, id: Id)->Option<&String>{
        let eclass_idx: usize = id.into();
        let eclass_id = self.eclasses[eclass_idx];
       let alias= self.analysis.expr_alias_map.get(&eclass_id);
       println!("id:{}, eclass_id: {}, alias: {:?}", id, eclass_id, alias);
       alias
    }

    fn convert_to_builder_inner(&self, id: Id)->Result<LogicalPlanBuilder, DataFusionError>{
        let plan = self.get_ref(id);
        let builder: LogicalPlanBuilder = match plan{
            TokomakLogicalPlan::Filter([input, predicate]) => {

                let predicate = self.convert_to_expr(*predicate)?;
                let input = self.convert_to_builder(*input)?;
                input.filter(predicate)?
            },
            TokomakLogicalPlan::Projection([input, exprs, alias]) =>{
                let input = self.convert_to_builder(*input)?;
                let alias = self.extract_alias(*alias, "Projection")?;
                let exprs = self.extract_expr_list(*exprs, "Projection.expr")?;
               let builder = input.project_with_alias(exprs, alias)? ;
               let proj = builder.build().unwrap();
               println!("proj: {:?}", proj);
               println!("Alias map: {:?}", self.analysis.expr_alias_map);
               builder
            },
            TokomakLogicalPlan::InnerJoin(join) => self.convert_to_join(JoinType::Inner, join)?,
            TokomakLogicalPlan::LeftJoin(join) => self.convert_to_join(JoinType::Left, join)?,
            TokomakLogicalPlan::RightJoin(join) => self.convert_to_join(JoinType::Right, join)?,
            TokomakLogicalPlan::FullJoin(join) => self.convert_to_join(JoinType::Full, join)?,
            TokomakLogicalPlan::SemiJoin(join) => self.convert_to_join(JoinType::Semi, join)?,
            TokomakLogicalPlan::AntiJoin(join) => self.convert_to_join(JoinType::Anti, join)?,
            TokomakLogicalPlan::Extension(_) => todo!(),
            TokomakLogicalPlan::CrossJoin([left, right]) => {
                let left = self.convert_to_builder(*left)?;
                let right = self.convert_to_builder(*right)?.build()?;
                left.cross_join(&right)?
            },
            TokomakLogicalPlan::Limit(_) => todo!(),
            TokomakLogicalPlan::EmptyRelation(empty_relation) => {
                let produce_one_row = match self.get_ref(empty_relation.produce_one_row()){
                    TokomakLogicalPlan::Scalar(TokomakScalar::Boolean(Some(val)))=>* val,
                    TokomakLogicalPlan::Scalar(non_boolean) => return Err(DataFusionError::Internal(format!("Expected a boolean scalar for EmptyRelation.produce_one_row found {:?}", non_boolean))),
                    plan =>return Err(unexpected_node("EmptyRelation.produce_one_row", plan, "Scalar"))
                };
                let schema = Arc::new(self.extract_schema(empty_relation.schema(), "EmptyRelation.schema")?);
                LogicalPlanBuilder::from(LogicalPlan::EmptyRelation{ produce_one_row, schema})
            },
            TokomakLogicalPlan::Repartition(_) => todo!(),
            TokomakLogicalPlan::TableScan(s) => {
                let name = match self.get_ref(s.name()){
                    TokomakLogicalPlan::Str(sym) => sym.to_string(),
                    unexpected_plan=> return Err(unexpected_node("TableScan.name", unexpected_plan, "Str")),
                };
                let limit = match self.get_ref(s.limit()){
                    TokomakLogicalPlan::LimitCount(lim)=>Some(*lim),
                    TokomakLogicalPlan::None => None,
                    unexpected_plan => return Err(unexpected_node("TableScan.limit", unexpected_plan, "LimitCount or None ")),
                };
                let source = match self.get_ref(s.source()){
                    TokomakLogicalPlan::Table(t)=> t.0.clone(),
                    unexpected_plan => return Err(unexpected_node("TableScan.provider", unexpected_plan,"Table")),
                };
                let projection = match self.get_ref(s.projection()){
                    TokomakLogicalPlan::None => None,
                    TokomakLogicalPlan::TableProject(proj)=>Some(proj.0.as_ref().clone()),
                    unexpected_plan => return Err(unexpected_node("TableScan.projection", unexpected_plan, "TableProject or None")),
                };
                let filters = self.extract_expr_list(s.filters(), "TableScan.filters")?;
                let builder = LogicalPlanBuilder::scan_with_limit_and_filters(name, source, projection, limit, filters)?;
                builder
            },
            TokomakLogicalPlan::Union([inputs, alias]) => {
                let mut builders = self.extract_plan_list(*inputs, "Union.inputs")?;
                if builders.is_empty(){
                    return Err(DataFusionError::Internal(format!("Union requires at least a single child plan, found 0")));
                }else if builders.len() == 1{
                    return Ok(builders.pop().unwrap())
                }
                let alias = self.extract_alias(*alias, "Union.alias")?;
                let mut builder = builders.pop().unwrap();
                let apply_alias =builders.pop().unwrap().build()?;
                for other in builders{
                    builder = builder.union(other.build()?)?;                    
                }
                builder.union_with_alias(apply_alias, alias)?
            },
            TokomakLogicalPlan::Window([input, window_exprs]) => {
                let input = self.convert_to_builder(*input)?;
                let window_expr = self.extract_expr_list(*window_exprs, "Window.window_expr")?;
                input.window(window_expr)?
            },
            TokomakLogicalPlan::Aggregate([input, aggr_expr, group_expr]) =>{
                let aggr_expr = self.extract_expr_list(*aggr_expr, "Aggregate.aggr_expr")?;
                let group_expr = self.extract_expr_list(*group_expr, "Aggregate.group_expr")?;
                let input = self.convert_to_builder(*input)?;
                input.aggregate(group_expr, aggr_expr)?
            },
            TokomakLogicalPlan::Sort([input, sort_exprs]) => {
                let input = self.convert_to_builder(*input)?;
                let exprs = self.extract_expr_list(*sort_exprs, "Sort.exprs")?;
                input.sort(exprs)?                
            },
            TokomakLogicalPlan::PartitioningScheme(_) => todo!(),
            TokomakLogicalPlan::Table(_) => todo!(),
            TokomakLogicalPlan::Values(_) => todo!(),
            TokomakLogicalPlan::TableProject(_) => todo!(),
            TokomakLogicalPlan::LimitCount(_) => todo!(),
            TokomakLogicalPlan::Str(_) => todo!(),
            TokomakLogicalPlan::None => todo!(),
            TokomakLogicalPlan::JoinKeys(_) => todo!(),
            TokomakLogicalPlan::PList(_)|
            TokomakLogicalPlan::Schema(_) => todo!(),

        expr @(
            TokomakLogicalPlan::ExprAlias(_)|
            TokomakLogicalPlan::Plus(_)|
            TokomakLogicalPlan::Minus(_)|
            TokomakLogicalPlan::Multiply(_)|
            TokomakLogicalPlan::Divide(_)|
            TokomakLogicalPlan::Modulus(_)|
            TokomakLogicalPlan::Or(_)|
            TokomakLogicalPlan::And(_)|
            TokomakLogicalPlan::Eq(_)|
            TokomakLogicalPlan::NotEq(_)|
            TokomakLogicalPlan::Lt(_)|
            TokomakLogicalPlan::LtEq(_)|
            TokomakLogicalPlan::Gt(_)|
            TokomakLogicalPlan::GtEq(_)|
            TokomakLogicalPlan::RegexMatch(_)|
            TokomakLogicalPlan::RegexIMatch(_)|
            TokomakLogicalPlan::RegexNotMatch(_)|
            TokomakLogicalPlan::RegexNotIMatch(_)|
            TokomakLogicalPlan::IsDistinctFrom(_)|
            TokomakLogicalPlan::IsNotDistinctFrom(_)|
            TokomakLogicalPlan::Not(_)|
            TokomakLogicalPlan::IsNotNull(_)|
            TokomakLogicalPlan::IsNull(_)|
            TokomakLogicalPlan::Negative(_)|
            TokomakLogicalPlan::Between(_)|
            TokomakLogicalPlan::BetweenInverted(_)|
            TokomakLogicalPlan::Like(_)|
            TokomakLogicalPlan::NotLike(_)|
            TokomakLogicalPlan::InList(_)|
            TokomakLogicalPlan::NotInList(_)|
            TokomakLogicalPlan::EList(_)|
            TokomakLogicalPlan::ScalarBuiltin(_)|
            TokomakLogicalPlan::AggregateBuiltin(_)|
            TokomakLogicalPlan::WindowBuiltin(_)|
            TokomakLogicalPlan::Scalar(_)|
            TokomakLogicalPlan::ScalarBuiltinCall(_)|
            TokomakLogicalPlan::ScalarUDFCall(_)|
            TokomakLogicalPlan::AggregateBuiltinCall(_)|
            TokomakLogicalPlan::AggregateBuiltinDistinctCall(_)|
            TokomakLogicalPlan::AggregateUDFCall(_)|
            TokomakLogicalPlan::WindowBuiltinCallUnframed(_)|
            TokomakLogicalPlan::WindowBuiltinCallFramed(_)|
            TokomakLogicalPlan::SortExpr(_)|
            TokomakLogicalPlan::SortSpec(_)|
            TokomakLogicalPlan::ScalarUDF(_)|
            TokomakLogicalPlan::AggregateUDF(_)|
            TokomakLogicalPlan::Column(_)|
            TokomakLogicalPlan::WindowFrame(_)|
            TokomakLogicalPlan::Cast(_)|
            TokomakLogicalPlan::TryCast(_))=> return Err(DataFusionError::Internal(format!("Expected plan found expression: {}", expr)))
            
        };
        Ok(builder)
    }
}


pub fn convert_to_df_plan(plan: &RecExpr<TokomakLogicalPlan>, udf_reg: &std::collections::HashMap<String, UDF>, analysis: &TokomakAnalysis, eclasses: &[Id])->Result<LogicalPlan, DataFusionError>{
    let converter = TokomakPlanConverter::new(udf_reg, plan, analysis, eclasses);
    let start = plan.as_ref().len() - 1;
    converter.convert_to_builder(start.into())?.build()
}

impl FromStr for TokomakExpr{
    type Err=DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        todo!()
    }
}


#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Window([Id;3]);
impl Window{
    pub fn new(input: Id, window_expr: Id, schema: Id)->Self{
        Self([input, window_expr, schema])
    }

    pub fn input(&self)->Id{
        self.0[0]
    }
    pub fn window_expr(&self)->Id{
        self.0[1]
    }
    pub fn schema(&self)->Id{
        self.0[2]
    }
}
impl LanguageChildren for Window{
    fn len(&self) -> usize {
        3
    }

    fn can_be_length(n: usize) -> bool {
        n == 3
    }

    fn from_vec(v: Vec<Id>) -> Self {
        Self([v[0], v[1],v[2]])
    }

    fn as_slice(&self) -> &[Id] {
        &self.0[..]
    }

    fn as_mut_slice(&mut self) -> &mut [Id] {
        &mut self.0[..]
    }
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Aggregate([Id;4]);

impl Aggregate{
    pub fn new(input: Id, group_expr: Id, aggr_expr: Id, schema: Id)->Self{
        Self([input,group_expr, aggr_expr, schema])
    }
    pub fn input(&self)->Id{
        self.0[0]
    }
    pub fn group_expr(&self)->Id{
        self.0[1]
    }
    pub fn aggr_expr(&self)->Id{
        self.0[2]
    }
    pub fn schema(&self)->Id{
        self.0[3]
    }
}

impl LanguageChildren for Aggregate{
    fn len(&self) -> usize {
        self.0.len()
    }

    fn can_be_length(n: usize) -> bool {
        n == 4
    }

    fn from_vec(v: Vec<Id>) -> Self {
        Self(v.try_into().unwrap())
    }

    fn as_slice(&self) -> &[Id] {
        &self.0[..]
    }

    fn as_mut_slice(&mut self) -> &mut [Id] {
        &mut self.0[..]
    }
}




#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Debug)]
pub struct EmptyRelation([Id; 2]);

impl EmptyRelation{
    pub fn new( produce_row: Id,schema: Id)->Self{
        Self([produce_row, schema])
    }

    pub fn produce_one_row(&self)->Id{
        self.0[0]
    }
    pub fn schema(&self)->Id{
        self.0[1]
    }
}
impl LanguageChildren for EmptyRelation{
    fn len(&self) -> usize {
        0
    }

    fn can_be_length(n: usize) -> bool {
        n == 0
    }

    fn from_vec(v: Vec<Id>) -> Self {
        todo!()
    }

    fn as_slice(&self) -> &[Id] {
        todo!()
    }

    fn as_mut_slice(&mut self) -> &mut [Id] {
        todo!()
    }
}


#[derive(Debug, Clone)]
pub struct SharedValues(Arc<Vec<Vec<Expr>>>);
impl Eq for SharedValues{}
impl PartialEq for SharedValues{
    fn eq(&self, other: &Self) -> bool {
        for (srow, orow) in self.0.iter().zip(other.0.as_slice()){
            for (sc, oc) in srow.iter().zip(orow){
                if sc != oc{
                    return false;
                }
            }
        }
        true
    }
}

impl Hash for SharedValues{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let mut buf = String::with_capacity(1024);
        for e in self.0.iter().flatten(){
            buf.clear();
            write!(buf, "{:?}", e).unwrap();
            buf.hash(state);
        }
    }
}

impl FromStr for SharedValues{
    type Err=DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Err(DataFusionError::NotImplemented("Parsing shared values is not supported for SharedValues".to_owned()))
    }
}
impl Ord for SharedValues{
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}
impl PartialOrd for SharedValues{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        for (srow, orow) in self.0.iter().zip(other.0.as_slice()){
            for (sc, oc) in srow.iter().zip(orow){
                if let Some(ord) = sc.partial_cmp(oc){
                    if  Ordering::Equal != ord{    
                        return Some(ord);
                    }
                }
            }
        }
        Some(Ordering::Equal)
    }
}

impl std::fmt::Display for SharedValues{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "VALUES({})", self.0.len())
    }
}
pub struct ExtNode(Arc<dyn UserDefinedLogicalNode + Send + Sync>);


//"(filter (and (= ?a:col ?b:col) ?x) (cross_join ?table_a ?table_b))" => "(filter ?x (inner_join ?table_a ?table_b) )"


/* 
#[cfg(test)]
mod tests{
    use datafusion::{arrow::datatypes::{DataType, Field, Schema}, datasource::empty::EmptyTable, logical_plan::{LogicalPlanBuilder, col, lit}};

    use super::*;
    fn test_schema()->Schema{
        Schema::new(vec![Field::new("a", DataType::Float32, false),Field::new("b", DataType::Float32, true), Field::new("string_ver", DataType::Utf8, false)])
    }

    fn test_tables()->(Arc<dyn TableProvider>, Arc<dyn TableProvider>){
        (
            Arc::new(EmptyTable::new(Arc::new(Schema::new(
                vec![Field::new("a", DataType::Int32, false), Field::new("b", DataType::Utf8, false), Field::new("c", DataType::Int32, false)]
            )))),
            Arc::new(EmptyTable::new(Arc::new(Schema::new(
                vec![Field::new("d", DataType::Int32, false), Field::new("e", DataType::Utf8, false), Field::new("f", DataType::Int32, false)]
            ))))
        )
    }

    #[test]
    fn round_trip_plan()->Result<(), Box<dyn std::error::Error>>{
        let schema = test_schema();
        let (table_one, table_two) = test_tables();
        let input_plans = vec![
            LogicalPlanBuilder::scan("one", table_one.clone(), None)?.build()?,
            LogicalPlanBuilder::scan_with_limit_and_filters("one", table_one.clone(), None, Some(200), vec![ col("a").eq(lit(1))])?.build()?,

            LogicalPlanBuilder::scan_empty(Some("input"), &schema, None)?.build()?,
            LogicalPlanBuilder::scan_empty(Some("input"), &schema, Some(vec![1]))?.build()?
        ];

        let dummy_reg = HashMap::new();
        for plan in input_plans {
            let tokomak_plan = to_tokomak_plan(&plan, )?;
            let round_tripped_plan = convert_to_df_plan(&tokomak_plan, &dummy_reg)?;
            let input_plan_str = format!("{:?}", plan);
            let round_tripped_plan_str = format!("{:?}", round_tripped_plan);
            assert_eq!(input_plan_str, round_tripped_plan_str);
        }
        Ok(())
    }
}
*/