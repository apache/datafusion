use crate::expressions::{
    BinaryExpr, CastExpr, Column, DateTimeIntervalExpr, GetIndexedFieldExpr, InListExpr,
};
use crate::PhysicalExpr;

use datafusion_common::Result;

use std::sync::Arc;

/// Controls how the visitor recursion should proceed.
pub enum Recursion<V: PhysicalExpressionVisitor> {
    /// Attempt to visit all the children, recursively, of this expression.
    Continue(V),
    /// Do not visit the children of this expression, though the walk
    /// of parents of this expression will not be affected
    Stop(V),
}

/// Encode the traversal of an expression tree. When passed to
/// `Expr::accept`, `PhysicalExpressionVisitor::visit` is invoked
/// recursively on all nodes of an expression tree. See the comments
/// on `Expr::accept` for details on its use
pub trait PhysicalExpressionVisitor<E: ExprVisitable = Arc<dyn PhysicalExpr>>:
    Sized
{
    /// Invoked before any children of `expr` are visited.
    fn pre_visit(self, expr: E) -> Result<Recursion<Self>>
    where
        Self: PhysicalExpressionVisitor;

    /// Invoked after all children of `expr` are visited. Default
    /// implementation does nothing.
    fn post_visit(self, _expr: E) -> Result<Self> {
        Ok(self)
    }
}

/// trait for types that can be visited by [`ExpressionVisitor`]
pub trait ExprVisitable: Sized {
    /// accept a visitor, calling `visit` on all children of this
    fn accept<V: PhysicalExpressionVisitor<Self>>(self, visitor: V) -> Result<V>;
}

// TODO: Widen the options.
impl ExprVisitable for Arc<dyn PhysicalExpr> {
    fn accept<V: PhysicalExpressionVisitor<Self>>(self, visitor: V) -> Result<V> {
        let visitor = match visitor.pre_visit(self.clone())? {
            Recursion::Continue(visitor) => visitor,
            // If the recursion should stop, do not visit children
            Recursion::Stop(visitor) => return Ok(visitor),
        };
        let visitor =
            if let Some(binary_expr) = self.as_any().downcast_ref::<BinaryExpr>() {
                let left_expr = binary_expr.left().clone();
                let right_expr = binary_expr.right().clone();
                let visitor = left_expr.accept(visitor)?;
                right_expr.accept(visitor)
            } else if let Some(datatime_expr) =
                self.as_any().downcast_ref::<DateTimeIntervalExpr>()
            {
                let lhs = datatime_expr.lhs().clone();
                let rhs = datatime_expr.rhs().clone();
                let visitor = lhs.accept(visitor)?;
                rhs.accept(visitor)
            } else if let Some(cast) = self.as_any().downcast_ref::<CastExpr>() {
                let expr = cast.expr().clone();
                expr.accept(visitor)
            } else if let Some(get_index) =
                self.as_any().downcast_ref::<GetIndexedFieldExpr>()
            {
                let arg = get_index.arg().clone();
                arg.accept(visitor)
            } else if let Some(inlist_expr) = self.as_any().downcast_ref::<InListExpr>() {
                let expr = inlist_expr.expr().clone();
                let list = inlist_expr.list();
                let visitor = expr.clone().accept(visitor)?;
                list.iter()
                    .try_fold(visitor, |visitor, arg| arg.clone().accept(visitor))
            } else {
                Ok(visitor)
            }?;
        visitor.post_visit(self.clone())
    }
}

#[derive(Default, Debug)]
pub struct PostOrderPhysicalColumnCollector {
    pub columns: Vec<Column>,
}

impl PhysicalExpressionVisitor for PostOrderPhysicalColumnCollector {
    fn pre_visit(mut self, expr: Arc<dyn PhysicalExpr>) -> Result<Recursion<Self>> {
        if let Some(column) = expr.as_any().downcast_ref::<Column>() {
            self.columns.push(column.clone())
        }
        Ok(Recursion::Continue(self))
    }
}
