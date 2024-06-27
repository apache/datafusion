use std::fmt::Debug;

use datafusion_common::Result;
use sqlparser::ast::BinaryOperator;

use crate::Operator;

pub trait ParseCustomOperator: Debug + Send + Sync {
    /// Return a human-readable name for this parser
    fn name(&self) -> &str;

    /// potentially parse a custom operator.
    ///
    /// Return `None` if the operator is not recognized
    fn op_from_ast(&self, op: &BinaryOperator) -> Result<Option<Operator>>;

    fn op_from_name(&self, raw_op: &str) -> Result<Option<Operator>>;
}
