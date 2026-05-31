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

use arrow::array::{ArrayRef, AsArray};
use std::sync::Arc;

use arrow::datatypes::DataType::{Decimal256, Int64};
use arrow::datatypes::{DECIMAL256_MAX_PRECISION, DataType, Decimal256Type, Int64Type};
use arrow_buffer::i256;

use datafusion_common::{
    Result, ScalarValue, exec_err, internal_err, utils::take_function_args,
};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Math Functions"),
    description = "Factorial of a non-negative integer. Errors if the argument is negative or the result overflows.",
    syntax_example = "factorial(numeric_expression)",
    sql_example = r#"```sql
> SELECT factorial(5);
+---------------+
| factorial(5)  |
+---------------+
| 120           |
+---------------+
```"#,
    standard_argument(name = "numeric_expression", prefix = "Numeric")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct FactorialFunc {
    signature: Signature,
}

impl Default for FactorialFunc {
    fn default() -> Self {
        FactorialFunc::new()
    }
}

impl FactorialFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![Int64], Volatility::Immutable),
        }
    }
}

const FACTORIAL_RETURN_TYPE: DataType = Decimal256(DECIMAL256_MAX_PRECISION, 0);

impl ScalarUDFImpl for FactorialFunc {
    fn name(&self) -> &str {
        "factorial"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(FACTORIAL_RETURN_TYPE)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = take_function_args(self.name(), args.args)?;

        match arg {
            ColumnarValue::Scalar(scalar) => {
                if scalar.is_null() {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Decimal256(
                        None,
                        DECIMAL256_MAX_PRECISION,
                        0,
                    )));
                }

                match scalar {
                    ScalarValue::Int64(Some(v)) => {
                        let result = compute_factorial(v)?;
                        Ok(ColumnarValue::Scalar(ScalarValue::Decimal256(
                            Some(result),
                            DECIMAL256_MAX_PRECISION,
                            0,
                        )))
                    }
                    _ => {
                        internal_err!(
                            "Unexpected data type {:?} for function factorial",
                            scalar.data_type()
                        )
                    }
                }
            }
            ColumnarValue::Array(array) => match array.data_type() {
                Int64 => {
                    let result = array
                        .as_primitive::<Int64Type>()
                        .try_unary::<_, Decimal256Type, _>(compute_factorial)?
                        .with_precision_and_scale(DECIMAL256_MAX_PRECISION, 0)?;
                    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
                }
                other => {
                    internal_err!("Unexpected data type {other:?} for function factorial")
                }
            },
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

const FACTORIALS: [i256; 57] = [
    i256::from_parts(1, 0),
    i256::from_parts(1, 0),
    i256::from_parts(2, 0),
    i256::from_parts(6, 0),
    i256::from_parts(24, 0),
    i256::from_parts(120, 0),
    i256::from_parts(720, 0),
    i256::from_parts(5040, 0),
    i256::from_parts(40320, 0),
    i256::from_parts(362880, 0),
    i256::from_parts(3628800, 0),
    i256::from_parts(39916800, 0),
    i256::from_parts(479001600, 0),
    i256::from_parts(6227020800, 0),
    i256::from_parts(87178291200, 0),
    i256::from_parts(1307674368000, 0),
    i256::from_parts(20922789888000, 0),
    i256::from_parts(355687428096000, 0),
    i256::from_parts(6402373705728000, 0),
    i256::from_parts(121645100408832000, 0),
    i256::from_parts(2432902008176640000, 0),
    i256::from_parts(51090942171709440000, 0),
    i256::from_parts(1124000727777607680000, 0),
    i256::from_parts(25852016738884976640000, 0),
    i256::from_parts(620448401733239439360000, 0),
    i256::from_parts(15511210043330985984000000, 0),
    i256::from_parts(403291461126605635584000000, 0),
    i256::from_parts(10888869450418352160768000000, 0),
    i256::from_parts(304888344611713860501504000000, 0),
    i256::from_parts(8841761993739701954543616000000, 0),
    i256::from_parts(265252859812191058636308480000000, 0),
    i256::from_parts(8222838654177922817725562880000000, 0),
    i256::from_parts(263130836933693530167218012160000000, 0),
    i256::from_parts(8683317618811886495518194401280000000, 0),
    i256::from_parts(295232799039604140847618609643520000000, 0),
    i256::from_parts(124676958757991025765413114570153656320, 30),
    i256::from_parts(64699745315476902531002227912544878592, 1093),
    i256::from_parts(11914008226076149403460180741783027712, 40448),
    i256::from_parts(112449945669955213868112260755986841600, 1537025),
    i256::from_parts(302159478076991779295882880302268284928, 59943987),
    i256::from_parts(176496280846824950617203951978843996160, 2397759515),
    i256::from_parts(90417809380115242574495275065471401984, 98308140136),
    i256::from_parts(54441957834517090031680871000348557312, 4128941885723),
    i256::from_parts(299309985358604090582029808424378695680, 177544501086095),
    i256::from_parts(238909412782918374001076488265470574592, 7811958047788218),
    i256::from_parts(202170200682234462683829141561361301504, 351538112150469841),
    i256::from_parts(
        112205324517446769945026111164878159872,
        16170753158921612713,
    ),
    i256::from_parts(
        169414748505921235465608113272750342144,
        760025398469315797526,
    ),
    i256::from_parts(
        305413489102634642691573466161347559424,
        36481219126527158281271,
    ),
    i256::from_parts(
        333119188428743562961991722339997319168,
        1787579737199830755782322,
    ),
    i256::from_parts(
        322405809232131901857604960274991808512,
        89378986859991537789116148,
    ),
    i256::from_parts(
        109142658633680748495871817299708084224,
        4558328329859568427244923596,
    ),
    i256::from_parts(
        230900378216383506371340780676528996352,
        237033073152697558216736027008,
    ),
    i256::from_parts(
        327837203235479616462950115744149405696,
        12562752877092970585487009431459,
    ),
    i256::from_parts(
        8525894827099188903826663732120911872,
        678388655363020411616298509298838,
    ),
    i256::from_parts(
        128641848569516926247091897834881941504,
        37311376044966122638896418011436091,
    ),
    i256::from_parts(
        58013814553240137106279522686256283648,
        2089437058518102867778199408640421117,
    ),
];

fn compute_factorial(n: i64) -> Result<i256> {
    if n < 0 {
        return exec_err!("factorial of a negative number is undefined");
    }

    if let Some(value) = FACTORIALS.get(n as usize) {
        return Ok(*value);
    }

    exec_err!("Overflow happened on FACTORIAL({n})")
}
