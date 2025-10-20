use arrow::array::ArrayRef;
use arrow::datatypes::Schema;
use std::ops::Deref;
use std::sync::Arc;
use datafusion_common::{internal_datafusion_err, plan_datafusion_err, ScalarValue};
use datafusion_expr::expr::FieldMetadata;
use datafusion_expr_common::operator::Operator;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use crate::expressions::{BinaryExpr, Literal};

/// All the optimizations in this module are based on the assumption of this `CASE WHEN` pattern:
/// ```sql
/// CASE
///     WHEN (<expr_a> = <literal_a>) THEN <literal_e>
///     WHEN (<expr_a> = <literal_b>) THEN <literal_f>
///     WHEN (<expr_a> = <literal_c>) THEN <literal_g>
///     WHEN (<expr_a> = <literal_d>) THEN <literal_h>
///     ELSE <fallback_literal>
/// END
/// ```
///
/// all the `WHEN` expressions are equality comparisons on the same expression against literals,
/// and all the `THEN` expressions are literals
/// the expression `<expr_a>` can be any expression as long as it does not have any state (e.g. random number generator, current timestamp, etc.)
pub(super) struct CaseWhenLiteralMapping {
    /// The expression that is being compared against the literals in the when clauses
    /// This expression must be deterministic
    /// In the example above this is `<expr_a>`
    pub(super) expression_to_match_on: Arc<dyn PhysicalExpr>,

    /// The literals that are being compared against the expression in the when clauses
    /// In the example above this is `<literal_a>`, `<literal_b>`, `<literal_c>`, `<literal_d>`
    /// These literals must all be of the same data type as the expression_to_match_on
    pub(super) when_equality_literals: Vec<Arc<Literal>>,

    /// The literals that are being returned in the then clauses
    /// In the example above this is `<literal_e>`, `<literal_f>`, `<literal_g>`, `<literal_h>`
    /// These literals must all be of the same data type
    pub(super) then_literals: Vec<Arc<Literal>>,

    /// The literal that is being returned in the else clause
    /// In the example above this is `<fallback_literal>`
    /// This literal must be of the same data type as the then_literals
    ///
    /// If no else clause is provided, this will be a null literal of the same data type as the then_literals
    pub(super) else_expr: Arc<Literal>,
}

impl CaseWhenLiteralMapping {
    /// Will return None if the optimization cannot be used
    /// Otherwise will return the optimized expression
    pub fn map_case_when(
        when_then_pairs: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
        else_expr: Option<Arc<dyn PhysicalExpr>>,
        input_schema: &Schema,
    ) -> Option<Self> {
        // let expression_to_match_on: Arc<dyn PhysicalExpr>;
        // let when_equality_literals: Vec<Arc<Literal>>;

        // We can't use the optimization if we don't have any when then pairs
        if when_then_pairs.is_empty() {
            return None;
        }

        // If we only have 1 than this optimization is not useful
        if when_then_pairs.len() == 1 {
            return None;
        }

        let when_exprs = when_then_pairs
            .iter()
            .map(|(when, _)| Arc::clone(when))
            .collect::<Vec<_>>();

        // If any of the when expressions is not a binary expression we cannot use this optimization

        let when_binary_exprs = when_exprs
            .iter()
            .map(|when| {
                let binary = when
                    .as_any()
                    .downcast_ref::<BinaryExpr>();
                binary.cloned()
            })
            .collect::<Option<Vec<_>>>()?;
        let when_exprs = when_binary_exprs;

        // If not all the binary expression are equality we cannot use this optimization
        if when_exprs
            .iter()
            .any(|when| !matches!(when.op(), Operator::Eq))
        {
            return None;
        }

        let expressions_to_match_on = when_exprs
            .iter()
            .map(|when| Arc::clone(when.left()))
            .collect::<Vec<_>>();

        let first_expression_to_match_on = &expressions_to_match_on[0];

        // Check if all expressions are the same
        if expressions_to_match_on
            .iter()
            .any(|expr| !expr.dyn_eq(first_expression_to_match_on.deref().as_any()))
        {
            return None;
        }
        // TODO - Test that the expression is deterministic
        let expression_to_match_on: Arc<dyn PhysicalExpr> =
            Arc::clone(first_expression_to_match_on);

        let equality_value_exprs = when_exprs
            .iter()
            .map(|when| when.right())
            .collect::<Vec<_>>();

        let when_equality_literals: Vec<Arc<Literal>> = {
            // TODO - spark should do constant folding but we should support expression on literal anyway
            // Test that all of the expressions are literals
            if equality_value_exprs
                .iter()
                .any(|expr| expr.as_any().downcast_ref::<Literal>().is_none())
            {
                return None;
            }

            equality_value_exprs
                .iter()
                .map(|expr| {
                    let literal = expr.as_any().downcast_ref::<Literal>().unwrap();
                    let literal = Literal::new_with_metadata(
                        literal.value().clone(),
                        // Empty schema as it is not used by literal
                        Some(FieldMetadata::from(
                            literal.return_field(&Schema::empty()).unwrap().deref(),
                        )),
                    );

                    Arc::new(literal)
                })
                .collect::<Vec<_>>()
        };

        {
            let Ok(data_type) = expression_to_match_on.data_type(input_schema) else {
                return None;
            };

            if data_type != when_equality_literals[0].value().data_type() {
                return None;
            }
        }

        let then_literals: Vec<Arc<Literal>> = {
            let then_literal_values = when_then_pairs
                .iter()
                .map(|(_, then)| then)
                .collect::<Vec<_>>();

            // TODO - spark should do constant folding but we should support expression on literal anyway
            // Test that all of the expressions are literals
            if then_literal_values
                .iter()
                .any(|expr| expr.as_any().downcast_ref::<Literal>().is_none())
            {
                return None;
            }

            then_literal_values
                .iter()
                .map(|expr| {
                    let literal = expr.as_any().downcast_ref::<Literal>().unwrap();
                    let literal = Literal::new_with_metadata(
                        literal.value().clone(),
                        // Empty schema as it is not used by literal
                        Some(FieldMetadata::from(
                            literal.return_field(&Schema::empty()).unwrap().deref(),
                        )),
                    );

                    Arc::new(literal)
                })
                .collect::<Vec<_>>()
        };

        let else_expr: Arc<Literal> = if let Some(else_expr) = else_expr {
            // TODO - spark should do constant folding but we should support expression on literal anyway

            let literal = else_expr.as_any().downcast_ref::<Literal>()?;
            let literal = Literal::new_with_metadata(
                literal.value().clone(),
                // Empty schema as it is not used by literal
                Some(FieldMetadata::from(
                    literal.return_field(&Schema::empty()).unwrap().deref(),
                )),
            );

            Arc::new(literal)
        } else {
            let Ok(null_scalar) = ScalarValue::try_new_null(&then_literals[0].value().data_type())
            else {
                return None;
            };
            Arc::new(Literal::new(null_scalar))
        };

        let this = Self {
            expression_to_match_on,
            when_equality_literals,
            then_literals,
            else_expr,
        };

        this.assert_requirements_are_met().ok()?;

        Some(this)
    }

    /// Assert that the requirements for the optimization are met so we can use it
    pub fn assert_requirements_are_met(&self) -> datafusion_common::Result<()> {
        // If expression_to_match_on is not deterministic we cannot use this optimization
        // TODO - we need a way to check if an expression is deterministic

        if self.when_equality_literals.len() != self.then_literals.len() {
            return Err(plan_datafusion_err!(
                "when_equality_literals and then_literals must be the same length"
            ));
        }

        if self.when_equality_literals.is_empty() {
            return Err(plan_datafusion_err!(
                "when_equality_literals and then_literals cannot be empty"
            ));
        }

        // Assert that all when equality literals are the same type and no nulls
        {
            let data_type = self.when_equality_literals[0].value().data_type();

            for when_lit in &self.when_equality_literals {
                if when_lit.value().data_type() != data_type {
                    return Err(plan_datafusion_err!(
                        "All when_equality_literals must have the same data type, found {} and {}",
                        when_lit.value().data_type(),
                        data_type
                    ));
                }
            }
        }

        // Assert that all output values are the same type
        {
            let data_type = self.then_literals[0].value().data_type();

            for then_lit in &self.then_literals {
                if then_lit.value().data_type() != data_type {
                    return Err(plan_datafusion_err!(
                        "All then_literals must have the same data type, found {} and {}",
                        then_lit.value().data_type(),
                        data_type
                    ));
                }
            }

            if self.else_expr.value().data_type() != data_type {
                return Err(plan_datafusion_err!(
                    "else_expr must have the same data type as then_literals, found {} and {}",
                    self.else_expr.value().data_type(),
                    data_type
                ));
            }
        }

        Ok(())
    }

    /// Return ArrayRef where array[i] = then_literals[i]
    /// the last value in the array is the else_expr
    pub fn build_dense_output_values(&self) -> datafusion_common::Result<ArrayRef> {
        // Create the dictionary values array filled with the else value
        let mut dictionary_values = vec![];

        // Fill the dictionary values array with the then literals
        for then_lit in self.then_literals.iter() {
            dictionary_values.push(then_lit.value().clone());
        }

        // Add the else
        dictionary_values.push(self.else_expr.value().clone());

        let dictionary_values = ScalarValue::iter_to_array(dictionary_values)?;

        Ok(dictionary_values)
    }

    /// Normalized all literal values to i128 to ease one-time computations
    ///
    /// this is i128 as we don't know if the input is signed or unsigned
    /// as it can be used to validate the requirements of no negative,
    /// and we don't want to lose information
    pub fn get_when_literals_values_normalized_for_non_nullable_integer_literals(
        &self,
    ) -> datafusion_common::Result<Vec<i128>> {
        self.when_equality_literals
            .iter()
            .map(|lit| lit.value())
            .map(|lit| {
                if !lit.data_type().is_integer() {
                    return Err(plan_datafusion_err!(
                        "All when_equality_literals must be integer type, found {}",
                        lit.data_type()
                    ));
                }

                if !lit.data_type().is_dictionary_key_type() {
                    return Err(plan_datafusion_err!(
                        "All when_equality_literals must be valid dictionary key type, found {}",
                        lit.data_type()
                    ));
                }

                if lit.is_null() {
                    return Err(plan_datafusion_err!(
                        "All when_equality_literals must be non-null numeric types, found null"
                    ));
                }

                match lit {
                    ScalarValue::Int8(Some(v)) => Ok(*v as i128),
                    ScalarValue::Int16(Some(v)) => Ok(*v as i128),
                    ScalarValue::Int32(Some(v)) => Ok(*v as i128),
                    ScalarValue::Int64(Some(v)) => Ok(*v as i128),
                    ScalarValue::UInt8(Some(v)) => Ok(*v as i128),
                    ScalarValue::UInt16(Some(v)) => Ok(*v as i128),
                    ScalarValue::UInt32(Some(v)) => Ok(*v as i128),
                    ScalarValue::UInt64(Some(v)) => Ok(*v as i128),
                    _ => Err(internal_datafusion_err!(
                        "dictionary key type is not supported {}, value: {}",
                        lit.data_type(),
                        lit
                    )),
                }
            })
            .collect::<datafusion_common::Result<Vec<i128>>>()
    }
}
