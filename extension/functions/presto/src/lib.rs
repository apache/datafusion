use arrow::array::{ArrayRef, Float64Array, StringArray, Array};
use arrow::datatypes::{DataType, Int32Type};
use datafusion::error::Result;
use datafusion::logical_expr::Volatility;
use datafusion_common::cast::{as_float64_array, as_string_array, as_primitive_array};
use datafusion_expr::{
    ReturnTypeFunction, ScalarFunctionDef, ScalarFunctionPackage, Signature,
};
use std::sync::Arc;

#[derive(Debug)]
pub struct AddOneFunction;

impl ScalarFunctionDef for AddOneFunction {
    fn name(&self) -> &str {
        "add_one"
    }

    fn signature(&self) -> Signature {
        Signature::exact(vec![DataType::Float64], Volatility::Immutable)
    }

    fn return_type(&self) -> ReturnTypeFunction {
        let return_type = Arc::new(DataType::Float64);
        Arc::new(move |_| Ok(return_type.clone()))
    }

    fn execute(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        assert_eq!(args.len(), 1);
        let input = as_float64_array(&args[0]).expect("cast failed");
        let array = input
            .iter()
            .map(|value| match value {
                Some(value) => Some(value + 1.0),
                _ => None,
            })
            .collect::<Float64Array>();
        Ok(Arc::new(array) as ArrayRef)
    }
}

#[derive(Debug)]
pub struct MultiplyTwoFunction;

impl ScalarFunctionDef for MultiplyTwoFunction {
    fn name(&self) -> &str {
        "multiply_two"
    }

    fn signature(&self) -> Signature {
        Signature::exact(vec![DataType::Float64], Volatility::Immutable)
    }

    fn return_type(&self) -> ReturnTypeFunction {
        let return_type = Arc::new(DataType::Float64);
        Arc::new(move |_| Ok(return_type.clone()))
    }

    fn execute(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        assert_eq!(args.len(), 1);
        let input = as_float64_array(&args[0]).expect("cast failed");
        let array = input
            .iter()
            .map(|value| match value {
                Some(value) => Some(value * 2.0),
                _ => None,
            })
            .collect::<Float64Array>();
        Ok(Arc::new(array) as ArrayRef)
    }
}

#[derive(Debug)]
pub struct LowerFunction;

impl ScalarFunctionDef for LowerFunction{
    fn name(&self) -> &str {
        "lower"
    }

    fn signature(&self) -> Signature {
        Signature::exact(vec![DataType::Utf8], Volatility::Immutable)
    }

    fn return_type(&self) -> ReturnTypeFunction {
        let return_type = Arc::new(DataType::Utf8);
        Arc::new(move |_| Ok(return_type.clone()))
    }

    fn execute(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        assert_eq!(args.len(), 1);
        let input = as_string_array(&args[0]).expect("cast failed");
        let array = input
            .iter()
            .map(|value| match value {
                Some(value) => Some(value.to_lowercase()),
                _ => None,
            })
            .collect::<StringArray>();
        Ok(Arc::new(array) as ArrayRef)
    }
}

#[derive(Debug)]
pub struct LpadFunction;

impl ScalarFunctionDef for LpadFunction{
    fn name(&self)->&str{
        "lpad"
    }

    fn signature(&self)->Signature{
        Signature::exact(vec![DataType::Utf8,DataType::Int64,DataType::Utf8], Volatility::Immutable)
    }

    fn return_type(&self) -> ReturnTypeFunction {
        let return_type = Arc::new(DataType::Utf8);
        Arc::new(move |_| Ok(return_type.clone()))
    }

    fn execute(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        assert_eq!(args.len(), 3);

        let string_array = as_string_array(&args[0]).expect("cast failed");
        let size_array = as_primitive_array::<Int32Type>(&args[1]).expect("cast failed");
        let padstring_array = as_string_array(&args[2]).expect("cast failed");

        let string_values = string_array.values();
        let size_values = size_array.values();
        let padstring_values = padstring_array.values();

        let array = (0..string_array.len()).map(|i| {
            let string = string_values.get(i).map(|s| s.to_string());
            let size = size_values.get(i).map(|&size| size as usize);
            let padstring = padstring_values.get(i).map(|s| s.to_string());

            match (string, size, padstring) {
                (Some(string), Some(size), Some(padstring)) => {
                    let padded_string = if string.len() < size {
                        let pad_count = size - string.len();
                        let pads = padstring.repeat(pad_count);
                        let truncated_pads = &pads[..pad_count];
                        format!("{}{}", truncated_pads, string)
                    } else {
                        string[..size].to_string()
                    };
                    Some(padded_string)
                }
                _ => None,
            }
        }).collect::<StringArray>();

        Ok(Arc::new(array) as ArrayRef)
    }
}


// Function package declaration
pub struct TestFunctionPackage;

impl ScalarFunctionPackage for TestFunctionPackage {
    fn functions(&self) -> Vec<Box<dyn ScalarFunctionDef>> {
        vec![Box::new(AddOneFunction), Box::new(MultiplyTwoFunction),Box::new(LowerFunction)]
    }
}

#[cfg(test)]
mod test {
    use arrow::{
        array::ArrayRef, record_batch::RecordBatch, util::display::array_value_to_string,
    };
    use datafusion::prelude::SessionContext;
    use tokio;
    use datafusion::error::Result;

    use crate::TestFunctionPackage;

    /// Execute query and return result set as 2-d table of Vecs
    /// `result[row][column]`
    async fn execute(ctx: &SessionContext, sql: &str) -> Vec<Vec<String>> {
        result_vec(&execute_to_batches(ctx, sql).await)
    }

    /// Specialised String representation
    fn col_str(column: &ArrayRef, row_index: usize) -> String {
        if column.is_null(row_index) {
            return "NULL".to_string();
        }

        array_value_to_string(column, row_index)
            .ok()
            .unwrap_or_else(|| "???".to_string())
    }
    /// Converts the results into a 2d array of strings, `result[row][column]`
    /// Special cases nulls to NULL for testing
    fn result_vec(results: &[RecordBatch]) -> Vec<Vec<String>> {
        let mut result = vec![];
        for batch in results {
            for row_index in 0..batch.num_rows() {
                let row_vec = batch
                    .columns()
                    .iter()
                    .map(|column| col_str(column, row_index))
                    .collect();
                result.push(row_vec);
            }
        }
        result
    }

    /// Execute query and return results as a Vec of RecordBatches
    async fn execute_to_batches(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
        let df = ctx.sql(sql).await.unwrap();

        // optimize just for check schema don't change during optimization.
        df.clone().into_optimized_plan().unwrap();

        df.collect().await.unwrap()
    }

    macro_rules! test_expression {
        ($SQL:expr, $EXPECTED:expr) => {
            let ctx = SessionContext::new();
            ctx.register_scalar_function_package(Box::new(TestFunctionPackage));
            let sql = format!("SELECT {}", $SQL);
            let actual = execute(&ctx, sql.as_str()).await;
            assert_eq!(actual[0][0], $EXPECTED);
        };
    }

    #[tokio::test]
    async fn test_add_one() -> Result<()> {
        test_expression!("add_one(1)", "2.0");
        test_expression!("add_one(-1)", "0.0");
        Ok(())
    }
     #[tokio::test]
    async fn test_multiply_two() -> Result<()> {
        test_expression!("multiply_two(1)", "2.0");
        test_expression!("multiply_two(-1)", "-2.0");
        Ok(())
    }
    #[tokio::test]
    async fn test_lower() -> Result<()> {
        test_expression!("lower('ABc')", "abc");
        test_expression!("lower('QWE')", "qwe");
        Ok(())
    }   
    #[tokio::test]
    async fn test_lpad() ->Result<()>{
        test_expression!("lpad('hello',4,'rust')","hell");
        test_expression!("lpad('bc',5,'a')","aaabc");
        Ok(())
    }
}
