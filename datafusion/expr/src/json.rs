use arrow::datatypes::DataType;
use arrow::datatypes::{Field, Fields};
use datafusion_common::{DataFusionError, Result};
use sqlparser::ast::JsonOperator as SQLJsonOperator;
use std::fmt;

/// We represent the JSON type as a custom arrow struct, with one field containing the
/// JSON as text
pub fn json_type() -> DataType {
    let json_struct = Fields::from(vec![Field::new("json", DataType::Utf8, false)]);
    DataType::Struct(json_struct)
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum JsonAcessOperator {
    /// Access operand as json (`->`)
    Arrow,
}

pub fn arrow_result_type(left: &DataType, right: &DataType) -> Result<DataType> {
    if left != &json_type() {
        Err(DataFusionError::Plan(format!(
            "Cannot use arrow access operator on non-json {left}!"
        )))
    } else if !right.is_integer() && right != &DataType::Utf8 {
        Err(DataFusionError::Plan(format!(
            "Right side of of access operator must integer or text (not {right})!"
        )))
    } else {
        Ok(json_type())
    }
}

impl JsonAcessOperator {
    pub fn result_type(&self, left: &DataType, right: &DataType) -> Result<DataType> {
        match self {
            Self::Arrow => {
                if left != &json_type() {
                    Err(DataFusionError::Plan(format!(
                        "Left side of '{self}' used on non-json {left}!"
                    )))
                } else if !right.is_integer() && right != &DataType::Utf8 {
                    Err(DataFusionError::Plan(format!(
                        "Right side of '{self}' used on {right}, not interger or text!"
                    )))
                } else {
                    Ok(json_type())
                }
            }
        }
    }
}

impl TryFrom<SQLJsonOperator> for JsonAcessOperator {
    type Error = DataFusionError;

    fn try_from(operator: SQLJsonOperator) -> Result<Self> {
        match operator {
            SQLJsonOperator::Arrow => Ok(Self::Arrow),
            operator => Err(DataFusionError::NotImplemented(format!(
                "Json Operator '{operator}' is not supported."
            ))),
        }
    }
}

impl fmt::Display for JsonAcessOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let display = match &self {
            Self::Arrow => "->",
        };
        write!(f, "{display}")
    }
}
