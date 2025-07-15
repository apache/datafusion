use super::*;

// Common test macros
macro_rules! test_coercion_binary_rule {
    ($LHS_TYPE:expr, $RHS_TYPE:expr, $OP:expr, $RESULT_TYPE:expr) => {{
        let (lhs, rhs) =
            BinaryTypeCoercer::new(&$LHS_TYPE, &$OP, &$RHS_TYPE).get_input_types()?;
        assert_eq!(lhs, $RESULT_TYPE);
        assert_eq!(rhs, $RESULT_TYPE);
    }};
}

macro_rules! test_coercion_binary_rule_multiple {
    ($LHS_TYPE:expr, $RHS_TYPES:expr, $OP:expr, $RESULT_TYPE:expr) => {{
        for rh_type in $RHS_TYPES {
            let (lhs, rhs) =
                BinaryTypeCoercer::new(&$LHS_TYPE, &$OP, &rh_type).get_input_types()?;
            assert_eq!(lhs, $RESULT_TYPE);
            assert_eq!(rhs, $RESULT_TYPE);

            BinaryTypeCoercer::new(&rh_type, &$OP, &$LHS_TYPE).get_input_types()?;
            assert_eq!(lhs, $RESULT_TYPE);
            assert_eq!(rhs, $RESULT_TYPE);
        }
    }};
}

macro_rules! test_like_rule {
    ($LHS_TYPE:expr, $RHS_TYPE:expr, $RESULT_TYPE:expr) => {{
        let result = like_coercion(&$LHS_TYPE, &$RHS_TYPE);
        assert_eq!(result, $RESULT_TYPE);
        let result = like_coercion(&$RHS_TYPE, &$LHS_TYPE);
        assert_eq!(result, $RESULT_TYPE);
    }};
}

mod arithmetic;
mod comparison;
mod dictionary;
mod null_coercion;
