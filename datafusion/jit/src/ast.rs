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

use arrow::datatypes::DataType;
use cranelift::codegen::ir;
use datafusion_common::{DFSchemaRef, DataFusionError, ScalarValue};
use std::fmt::{Display, Formatter};

#[derive(Clone, Debug)]
/// Statement
pub enum Stmt {
    /// if-then-else
    IfElse(Box<Expr>, Vec<Stmt>, Vec<Stmt>),
    /// while
    WhileLoop(Box<Expr>, Vec<Stmt>),
    /// assignment
    Assign(String, Box<Expr>),
    /// call function for side effect
    Call(String, Vec<Expr>),
    /// declare a new variable of type
    Declare(String, JITType),
    /// store value (the first expr) to an address (the second expr)
    Store(Box<Expr>, Box<Expr>),
}

#[derive(Copy, Clone, Debug, PartialEq)]
/// Shorthand typed literals
pub enum TypedLit {
    Bool(bool),
    Int(i64),
    Float(f32),
    Double(f64),
}

#[derive(Clone, Debug, PartialEq)]
/// Expression
pub enum Expr {
    /// literal
    Literal(Literal),
    /// variable
    Identifier(String, JITType),
    /// binary expression
    Binary(BinaryExpr),
    /// call function expression
    Call(String, Vec<Expr>, JITType),
    /// Load a value from pointer
    Load(Box<Expr>, JITType),
}

impl Expr {
    pub fn get_type(&self) -> JITType {
        match self {
            Expr::Literal(lit) => lit.get_type(),
            Expr::Identifier(_, ty) => *ty,
            Expr::Binary(bin) => bin.get_type(),
            Expr::Call(_, _, ty) => *ty,
            Expr::Load(_, ty) => *ty,
        }
    }
}

impl Literal {
    fn get_type(&self) -> JITType {
        match self {
            Literal::Parsing(_, ty) => *ty,
            Literal::Typed(tl) => tl.get_type(),
        }
    }
}

impl TypedLit {
    fn get_type(&self) -> JITType {
        match self {
            TypedLit::Bool(_) => BOOL,
            TypedLit::Int(_) => I64,
            TypedLit::Float(_) => F32,
            TypedLit::Double(_) => F64,
        }
    }
}

impl BinaryExpr {
    fn get_type(&self) -> JITType {
        match self {
            BinaryExpr::Eq(_, _) => BOOL,
            BinaryExpr::Ne(_, _) => BOOL,
            BinaryExpr::Lt(_, _) => BOOL,
            BinaryExpr::Le(_, _) => BOOL,
            BinaryExpr::Gt(_, _) => BOOL,
            BinaryExpr::Ge(_, _) => BOOL,
            BinaryExpr::Add(lhs, _) => lhs.get_type(),
            BinaryExpr::Sub(lhs, _) => lhs.get_type(),
            BinaryExpr::Mul(lhs, _) => lhs.get_type(),
            BinaryExpr::Div(lhs, _) => lhs.get_type(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
/// Binary expression
pub enum BinaryExpr {
    /// ==
    Eq(Box<Expr>, Box<Expr>),
    /// !=
    Ne(Box<Expr>, Box<Expr>),
    /// <
    Lt(Box<Expr>, Box<Expr>),
    /// <=
    Le(Box<Expr>, Box<Expr>),
    /// >
    Gt(Box<Expr>, Box<Expr>),
    /// >=
    Ge(Box<Expr>, Box<Expr>),
    /// add
    Add(Box<Expr>, Box<Expr>),
    /// subtract
    Sub(Box<Expr>, Box<Expr>),
    /// multiply
    Mul(Box<Expr>, Box<Expr>),
    /// divide
    Div(Box<Expr>, Box<Expr>),
}

#[derive(Clone, Debug, PartialEq)]
/// Literal
pub enum Literal {
    /// Parsable literal with type
    Parsing(String, JITType),
    /// Shorthand literals of common types
    Typed(TypedLit),
}

impl TryFrom<(datafusion_expr::Expr, DFSchemaRef)> for Expr {
    type Error = DataFusionError;

    // Try to JIT compile the Expr for faster evaluation
    fn try_from(
        (value, schema): (datafusion_expr::Expr, DFSchemaRef),
    ) -> Result<Self, Self::Error> {
        match &value {
            datafusion_expr::Expr::BinaryExpr(datafusion_expr::expr::BinaryExpr {
                left,
                op,
                right,
            }) => {
                let op = match op {
                    datafusion_expr::Operator::Eq => BinaryExpr::Eq,
                    datafusion_expr::Operator::NotEq => BinaryExpr::Ne,
                    datafusion_expr::Operator::Lt => BinaryExpr::Lt,
                    datafusion_expr::Operator::LtEq => BinaryExpr::Le,
                    datafusion_expr::Operator::Gt => BinaryExpr::Gt,
                    datafusion_expr::Operator::GtEq => BinaryExpr::Ge,
                    datafusion_expr::Operator::Plus => BinaryExpr::Add,
                    datafusion_expr::Operator::Minus => BinaryExpr::Sub,
                    datafusion_expr::Operator::Multiply => BinaryExpr::Mul,
                    datafusion_expr::Operator::Divide => BinaryExpr::Div,
                    _ => {
                        return Err(DataFusionError::NotImplemented(format!(
                            "Compiling binary expression {} not yet supported",
                            value
                        )))
                    }
                };
                Ok(Expr::Binary(op(
                    Box::new((*left.clone(), schema.clone()).try_into()?),
                    Box::new((*right.clone(), schema).try_into()?),
                )))
            }
            datafusion_expr::Expr::Column(col) => {
                let field = schema.field_from_column(col)?;
                let ty = field.data_type();

                let jit_type = JITType::try_from(ty)?;

                Ok(Expr::Identifier(field.qualified_name(), jit_type))
            }
            datafusion_expr::Expr::Literal(s) => {
                let lit = match s {
                    ScalarValue::Boolean(Some(b)) => TypedLit::Bool(*b),
                    ScalarValue::Float32(Some(f)) => TypedLit::Float(*f),
                    ScalarValue::Float64(Some(f)) => TypedLit::Double(*f),
                    ScalarValue::Int64(Some(i)) => TypedLit::Int(*i),
                    _ => {
                        return Err(DataFusionError::NotImplemented(format!(
                            "Compiling Scalar {} not yet supported in JIT mode",
                            s
                        )))
                    }
                };
                Ok(Expr::Literal(Literal::Typed(lit)))
            }
            _ => Err(DataFusionError::NotImplemented(format!(
                "Compiling {} not yet supported",
                value
            ))),
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
/// Type to be used in JIT
pub struct JITType {
    /// The cranelift type
    pub(crate) native: ir::Type,
    /// re-expose inner field of `ir::Type` out for easier pattern matching
    pub(crate) code: u8,
}

/// null type as placeholder
pub const NIL: JITType = JITType {
    native: ir::types::INVALID,
    code: 0,
};
/// bool
pub const BOOL: JITType = JITType {
    native: ir::types::B1,
    code: 0x70,
};
/// integer of 1 byte
pub const I8: JITType = JITType {
    native: ir::types::I8,
    code: 0x76,
};
/// integer of 2 bytes
pub const I16: JITType = JITType {
    native: ir::types::I16,
    code: 0x77,
};
/// integer of 4 bytes
pub const I32: JITType = JITType {
    native: ir::types::I32,
    code: 0x78,
};
/// integer of 8 bytes
pub const I64: JITType = JITType {
    native: ir::types::I64,
    code: 0x79,
};
/// Ieee float of 32 bits
pub const F32: JITType = JITType {
    native: ir::types::F32,
    code: 0x7b,
};
/// Ieee float of 64 bits
pub const F64: JITType = JITType {
    native: ir::types::F64,
    code: 0x7c,
};
/// Pointer type of 32 bits
pub const R32: JITType = JITType {
    native: ir::types::R32,
    code: 0x7e,
};
/// Pointer type of 64 bits
pub const R64: JITType = JITType {
    native: ir::types::R64,
    code: 0x7f,
};
pub const PTR_SIZE: usize = std::mem::size_of::<usize>();
/// The pointer type to use based on our currently target.
pub const PTR: JITType = if PTR_SIZE == 8 { R64 } else { R32 };

impl TryFrom<&DataType> for JITType {
    type Error = DataFusionError;

    /// Try to convert DataFusion's [DataType] to [JITType]
    fn try_from(df_type: &DataType) -> Result<Self, Self::Error> {
        match df_type {
            DataType::Int64 => Ok(I64),
            DataType::Float32 => Ok(F32),
            DataType::Float64 => Ok(F64),
            DataType::Boolean => Ok(BOOL),

            _ => Err(DataFusionError::NotImplemented(format!(
                "Compiling Expression with type {} not yet supported in JIT mode",
                df_type
            ))),
        }
    }
}

impl Stmt {
    /// print the statement with indentation
    pub fn fmt_ident(&self, ident: usize, f: &mut Formatter) -> std::fmt::Result {
        let mut ident_str = String::new();
        for _ in 0..ident {
            ident_str.push(' ');
        }
        match self {
            Stmt::IfElse(cond, then_stmts, else_stmts) => {
                writeln!(f, "{}if {} {{", ident_str, cond)?;
                for stmt in then_stmts {
                    stmt.fmt_ident(ident + 4, f)?;
                }
                writeln!(f, "{}}} else {{", ident_str)?;
                for stmt in else_stmts {
                    stmt.fmt_ident(ident + 4, f)?;
                }
                writeln!(f, "{}}}", ident_str)
            }
            Stmt::WhileLoop(cond, stmts) => {
                writeln!(f, "{}while {} {{", ident_str, cond)?;
                for stmt in stmts {
                    stmt.fmt_ident(ident + 4, f)?;
                }
                writeln!(f, "{}}}", ident_str)
            }
            Stmt::Assign(name, expr) => {
                writeln!(f, "{}{} = {};", ident_str, name, expr)
            }
            Stmt::Call(name, args) => {
                writeln!(
                    f,
                    "{}{}({});",
                    ident_str,
                    name,
                    args.iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            Stmt::Declare(name, ty) => {
                writeln!(f, "{}let {}: {};", ident_str, name, ty)
            }
            Stmt::Store(value, ptr) => {
                writeln!(f, "{}*({}) = {}", ident_str, ptr, value)
            }
        }
    }
}

impl Display for Stmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_ident(0, f)?;
        Ok(())
    }
}

impl Display for Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Literal(l) => write!(f, "{}", l),
            Expr::Identifier(name, _) => write!(f, "{}", name),
            Expr::Binary(be) => write!(f, "{}", be),
            Expr::Call(name, exprs, _) => {
                write!(
                    f,
                    "{}({})",
                    name,
                    exprs
                        .iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            Expr::Load(ptr, _) => write!(f, "*({})", ptr,),
        }
    }
}

impl Display for Literal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::Parsing(str, _) => write!(f, "{}", str),
            Literal::Typed(tl) => write!(f, "{}", tl),
        }
    }
}

impl Display for TypedLit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TypedLit::Bool(b) => write!(f, "{}", b),
            TypedLit::Int(i) => write!(f, "{}", i),
            TypedLit::Float(fl) => write!(f, "{}", fl),
            TypedLit::Double(d) => write!(f, "{}", d),
        }
    }
}

impl Display for BinaryExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BinaryExpr::Eq(lhs, rhs) => write!(f, "{} == {}", lhs, rhs),
            BinaryExpr::Ne(lhs, rhs) => write!(f, "{} != {}", lhs, rhs),
            BinaryExpr::Lt(lhs, rhs) => write!(f, "{} < {}", lhs, rhs),
            BinaryExpr::Le(lhs, rhs) => write!(f, "{} <= {}", lhs, rhs),
            BinaryExpr::Gt(lhs, rhs) => write!(f, "{} > {}", lhs, rhs),
            BinaryExpr::Ge(lhs, rhs) => write!(f, "{} >= {}", lhs, rhs),
            BinaryExpr::Add(lhs, rhs) => write!(f, "{} + {}", lhs, rhs),
            BinaryExpr::Sub(lhs, rhs) => write!(f, "{} - {}", lhs, rhs),
            BinaryExpr::Mul(lhs, rhs) => write!(f, "{} * {}", lhs, rhs),
            BinaryExpr::Div(lhs, rhs) => write!(f, "{} / {}", lhs, rhs),
        }
    }
}

impl std::fmt::Display for JITType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::fmt::Debug for JITType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.code {
            0 => write!(f, "nil"),
            0x70 => write!(f, "bool"),
            0x76 => write!(f, "i8"),
            0x77 => write!(f, "i16"),
            0x78 => write!(f, "i32"),
            0x79 => write!(f, "i64"),
            0x7b => write!(f, "f32"),
            0x7c => write!(f, "f64"),
            0x7e => write!(f, "small_ptr"),
            0x7f => write!(f, "ptr"),
            _ => write!(f, "unknown"),
        }
    }
}

impl From<&str> for JITType {
    fn from(x: &str) -> Self {
        match x {
            "bool" => BOOL,
            "i8" => I8,
            "i16" => I16,
            "i32" => I32,
            "i64" => I64,
            "f32" => F32,
            "f64" => F64,
            "small_ptr" => R32,
            "ptr" => R64,
            _ => panic!("unknown type: {}", x),
        }
    }
}
