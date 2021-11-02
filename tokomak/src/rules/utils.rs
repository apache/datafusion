use datafusion::{error::DataFusionError, execution::context::ExecutionProps, logical_plan::{Expr, Operator}, physical_plan::expressions::{ BinaryExpr, CastExpr}, scalar::ScalarValue};
use datafusion::arrow::datatypes::DataType;
use egg::*;
use datafusion::optimizer::utils::ConstEvaluator;
use crate::{CustomTokomakAnalysis, TokomakAnalysis, scalar::TokomakScalar};

use super::super::TokomakExpr;
use std::convert::TryInto;

