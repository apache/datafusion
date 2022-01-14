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

//! Contains structs which represent more complex expressions in the TokomakLogicalPlan
use datafusion::logical_plan::{Column, DFSchema, Expr};
use egg::*;
use std::convert::TryInto;
use std::{fmt::Display, str::FromStr};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
///Represents a Between expr. 3 members in order expr:Expr, low:expr, high:Expr
pub struct BetweenExpr([Id; 3]);
impl BetweenExpr {
    ///Create new BetweenExpr
    pub fn new(expr: Id, low: Id, high: Id) -> Self {
        Self([expr, low, high])
    }
    ///Return the input expr id
    pub fn expr(&self) -> Id {
        self.0[0]
    }
    ///Return the id of low
    pub fn low(&self) -> Id {
        self.0[1]
    }
    ///Return the id of high
    pub fn high(&self) -> Id {
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
///Represents a ScalarBuiltin, AggregateBuiltin, ScalarUDF, and AggregateUDF call: [func_name, args:elist]
pub struct FunctionCall([Id; 2]);
impl FunctionCall {
    ///Create FunctionCall
    pub fn new(func: Id, args: Id) -> Self {
        Self([func, args])
    }
    ///Id of the name
    pub fn fun(&self) -> Id {
        self.0[0]
    }
    ///Id of the argument list
    pub fn args(&self) -> Id {
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

///Counterpart to datafusion's column type with egg::Symbols to decrease size of Column and reduce heap allocations.
pub struct TokomakColumn {
    ///Relation name
    pub relation: Option<Symbol>,
    ///Column name
    pub name: Symbol,
}
impl Display for TokomakColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.relation {
            Some(r) => write!(f, "#{}.{}", r, self.name),
            None => write!(f, "#{}", self.name),
        }
    }
}

impl TokomakColumn {
    ///Create new TokomakColumn
    pub fn new(relation: Option<impl Into<Symbol>>, name: impl Into<Symbol>) -> Self {
        let relation = relation.map(|r| r.into());
        let name = name.into();
        TokomakColumn { relation, name }
    }
}

impl FromStr for TokomakColumn {
    type Err = <Column as FromStr>::Err;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let c: Column = s.parse()?;
        Ok(c.into())
    }
}

impl From<Column> for TokomakColumn {
    fn from(c: Column) -> Self {
        TokomakColumn::new(c.relation, c.name)
    }
}
impl From<&Column> for TokomakColumn {
    fn from(c: &Column) -> Self {
        TokomakColumn::new(c.relation.as_deref(), c.name.as_str())
    }
}

impl From<&TokomakColumn> for Column {
    fn from(tc: &TokomakColumn) -> Self {
        let relation = tc.relation.map(|s| s.as_str().to_owned());
        let name = tc.name.as_str().to_owned();
        Column { relation, name }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
///Unframed windowed function call: [fun:WindowBuiltin, args:elist, partition_by:elist, order_by:elist]
pub struct WindowBuiltinCallUnframed([Id; 4]);
impl WindowBuiltinCallUnframed {
    ///Create new unframed window call
    pub fn new(fun: Id, args: Id, partition: Id, order_by: Id) -> Self {
        Self([fun, args, partition, order_by])
    }
    ///Builtin window function id
    pub fn fun(&self) -> Id {
        self.0[0]
    }
    ///Argument list id
    pub fn args_list(&self) -> Id {
        self.0[1]
    }
    ///Partition list id
    pub fn partition_list(&self) -> Id {
        self.0[2]
    }
    ///Order list id
    pub fn order_by_list(&self) -> Id {
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
///Framed windowed function call: [fun:WindowBuiltin, args:elist, partition_by:elist, order_by:elist]

pub struct WindowBuiltinCallFramed([Id; 5]);
impl WindowBuiltinCallFramed {
    ///Create new framed window call
    pub fn new(fun: Id, args: Id, partition: Id, order_by: Id, frame: Id) -> Self {
        Self([fun, args, partition, order_by, frame])
    }
    ///Builtin window function
    pub fn fun(&self) -> Id {
        self.0[0]
    }
    ///Argument elist id
    pub fn args_list(&self) -> Id {
        self.0[1]
    }
    ///Partition elist id
    pub fn partition_list(&self) -> Id {
        self.0[2]
    }
    ///Order by elist id
    pub fn order_by_list(&self) -> Id {
        self.0[3]
    }
    ///Window frame id
    pub fn frame(&self) -> Id {
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
///Represents a CastExpression: [expr:Expr, dt:TokomakDataType]
pub struct CastExpr([Id; 2]);
impl CastExpr {
    ///Creates a new CastExpr
    pub fn new(expr: Id, cast_type: Id) -> Self {
        Self([expr, cast_type])
    }
    ///Input expr id
    pub fn expr(&self) -> Id {
        self.0[0]
    }
    ///Cast to type id
    pub fn cast_type(&self) -> Id {
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
///SortExpr
pub struct SortExpr([Id; 2]);

impl SortExpr {
    ///Create new SortExpr
    pub fn new(expr: Id, sort_spec: Id) -> Self {
        Self([expr, sort_spec])
    }
    ///Input expr id
    pub fn expr(&self) -> Id {
        self.0[0]
    }
    ///Expresion SortSpec
    pub fn sort_spec(&self) -> Id {
        self.0[1]
    }
}

#[rustfmt::skip]
impl LanguageChildren for SortExpr{
    fn len(&self) -> usize {2}
    fn can_be_length(n: usize) -> bool {n == 2}
    fn from_vec(v: Vec<Id>) -> Self { Self([v[0], v[1]]) }
    fn as_slice(&self) -> &[Id] { &self.0[..] }
    fn as_mut_slice(&mut self) -> &mut [Id] { &mut self.0[..] }
}

#[allow(dead_code)]
pub(crate) fn to_tokomak_expr(
    expr: &Expr,
    egraph: &mut EGraph<crate::plan::TokomakLogicalPlan, crate::TokomakAnalysis>,
    schema: &DFSchema,
) -> Result<Id, datafusion::error::DataFusionError> {
    let mut converter = crate::plan::PlanConverter { egraph };
    converter.as_tokomak_expr(expr, schema)
}
