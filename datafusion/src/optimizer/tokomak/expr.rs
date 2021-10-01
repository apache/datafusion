use crate::{
    error::DataFusionError,
    logical_plan::window_frames::WindowFrame,
    optimizer::tokomak::{ScalarUDFName, SortSpec, UDAFName, UDFName},
    physical_plan::{
        aggregates::AggregateFunction, functions::BuiltinScalarFunction,
        window_functions::WindowFunction,
    },
    scalar::ScalarValue,
};
use egg::*;

use super::datatype::TokomakDataType;
use super::scalar::TokomakScalar;
use std::convert::TryInto;
use std::str::FromStr;
define_language! {
pub enum TokomakExpr {
    "+" = Plus([Id; 2]),
    "-" = Minus([Id; 2]),
    "*" = Multiply([Id; 2]),
    "/" = Divide([Id; 2]),
    "%" = Modulus([Id; 2]),
    "not" = Not(Id),
    "or" = Or([Id; 2]),
    "and" = And([Id; 2]),
    "=" = Eq([Id; 2]),
    "<>" = NotEq([Id; 2]),
    "<" = Lt([Id; 2]),
    "<=" = LtEq([Id; 2]),
    ">" = Gt([Id; 2]),
    ">=" = GtEq([Id; 2]),
    "regex_match"=RegexMatch([Id;2]),
    "regex_imatch"=RegexIMatch([Id;2]),
    "regex_not_match"=RegexNotMatch([Id;2]),
    "regex_not_imatch"=RegexNotIMatch([Id;2]),


    "is_not_null" = IsNotNull(Id),
    "is_null" = IsNull(Id),
    "negative" = Negative(Id),
    "between" = Between([Id; 3]),
    "between_inverted" = BetweenInverted([Id; 3]),
    "like" = Like([Id; 2]),
    "not_like" = NotLike([Id; 2]),
    "in_list" = InList([Id; 2]),
    "not_in_list" = NotInList([Id; 2]),
    "list" = List(Vec<Id>),

    //ScalarValue types

    Type(TokomakDataType),
    ScalarBuiltin(BuiltinScalarFunction),
    AggregateBuiltin(AggregateFunction),
    WindowBuiltin(WindowFunction),
    Scalar(TokomakScalar),

    //THe fist expression for all of the function call types must be the corresponding function type
    //For UDFs this is a string, which is looked up in the ExecutionProps
    //The last expression must be a List and is the arguments for the function.
    "call" = ScalarBuiltinCall([Id; 2]),
    "call_udf"=ScalarUDFCall([Id; 2]),
    "call_agg" = AggregateBuiltinCall([Id; 2]),
    "call_agg_distinct"=AggregateBuiltinDistinctCall([Id;2]),
    "call_udaf" = AggregateUDFCall([Id; 2]),
    //For window fuctions index 1 is the window partition
    //index 2 is the window order
    "call_win" = WindowBuiltinCallUnframed([Id;4 ]),
    //For a framed window function index 3 is the frame parameters
    "call_win_framed" = WindowBuiltinCallFramed([Id;5 ]),
    //Last Id of the Sort node MUST be a SortSpec
    "sort" = Sort([Id;2]),
    SortSpec(SortSpec),
    ScalarUDF(ScalarUDFName),
    AggregateUDF(UDAFName),
    Column(Symbol),
    WindowFrame(WindowFrame),

    // cast id as expr. Type is encoded as symbol
    "cast" = Cast([Id; 2]),
    "try_cast" = TryCast([Id;2]),
}
}

impl TokomakExpr {
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
impl FromStr for WindowFrame {
    type Err = DataFusionError;
    #[allow(unused_variables)]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Err(DataFusionError::NotImplemented(
            "WindowFrame's aren't parsed yet".to_string(),
        ))
    }
}
