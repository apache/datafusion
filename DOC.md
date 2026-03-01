This PR adds support for lambdas with column capture and the `array_transform` function used to test the lambda implementation. Example usage:

```sql
CREATE TABLE t as SELECT 2 as n;

SELECT array_transform([2, 3], v -> v != t.n) from t;

[false, true]

-- arbitrally nested lambdas are also supported
SELECT array_transform([[[2, 3]]], m -> array_transform(m, l -> array_transform(l, v -> v*2)));

[[[4, 6]]]
```

Some comments on code snippets of this doc show what value each struct, variant or field would hold after planning the first example above. Some literals are simplified pseudo code

3 new `Expr` variants are added, `LambdaFunction`, owing a new trait `LambdaUDF`, which is like a `ScalarFunction`/`ScalarUDFImpl` with support for lambdas, `Lambda`, for the lambda body and it's parameters names, and `LambdaVariable`, which is like `Column` but for lambdas parameters. The reasoning why not using `Column` instead is later on this doc.

Their logical representations:

```rust
enum Expr {
    LambdaFunction(LambdaFunction), // array_transform([2, 3], v -> v != t.n)
    Lambda(Lambda), // v -> v != t.n
    LambdaVariable(LambdaVariable), // v, of the lambda body: v != t.n
   ...
}

// array_transform([2, 3], v -> v != t.n)
struct LambdaFunction {
    pub func: Arc<dyn LambdaUDF>, // global instance of array_transform
    pub args: Vec<Expr>, // [Expr::ScalarValue([2, 3]), Expr::Lambda(v -> v != n)]
}

// v -> v != t.n
struct Lambda {
    pub params: Vec<String>, // ["v"]
    pub body: Box<Expr>, // v != n
}

// v, of the lambda body: v != t.n
struct LambdaVariable {
    pub name: String, // "v"
    pub field: Option<FieldRef>, // Some(Field::new("", DataType::Int32, false))
    pub spans: Spans,
}

```

The example would be planned into a tree like this:

```
LambdaFunctionExpression
  name: array_transform
  children:
    1. ListExpression [2,3]
    2. LambdaExpression
         parameters: ["v"]
         body:
            ComparisonExpression (!=)
              left:
                 LambdaVariableExpression("v", Some(Field::new("", Int32, false)))
              right:
                 ColumnExpression("t.n")
```

The physical counterparts definition:

```rust

struct LambdaFunctionExpr {
    fun: Arc<dyn LambdaUDF>, // global instance of array_transform
    name: String, // "array_transform"
    args: Vec<Arc<dyn PhysicalExpr>>, // [LiteralExpr([2, 3], LambdaExpr("v -> v != t.n"))]
    return_field: FieldRef, // Field::new("", DataType::new_list(DataType::Boolean, false), false)
    config_options: Arc<ConfigOptions>, 
}


struct LambdaExpr {
    params: Vec<String>, // ["v"]
    body: Arc<dyn PhysicalExpr>, // v -> v != t.n
}

struct LambdaVariable {
    name: String, // "v", of the lambda body: v != t.n
    field: FieldRef, // Field::new("", DataType::Int32, false)
    value: Option<ColumnarValue>, // reasoning later on
}
```

Note: For those who primarly wants to check if this lambda implementation supports their usecase and don't want to spend much time here, it's okay to skip most collapsed blocks, as those serve mostly to help code reviewers, with the exception of `LambdaUDF` and the `array_transform` implementation of `LambdaUDF` relevant methods, collapsed due to their size

<details><summary>Physical planning implementation is trivial:</summary>

```rust
fn create_physical_expr(
    e: &Expr,
    input_dfschema: &DFSchema,
    execution_props: &ExecutionProps,
) -> Result<Arc<dyn PhysicalExpr>> {
    let input_schema = input_dfschema.as_arrow();

    match e {
        ...
        Expr::LambdaFunction(LambdaFunction { func, args}) => {
            let physical_args =
                create_physical_exprs(args, input_dfschema, execution_props)?;

            Ok(Arc::new(LambdaFunctionExpr::try_new(
                Arc::clone(func),
                physical_args,
                input_schema,
                config_options: ... // irrelevant
            )?))
        }
        Expr::Lambda(Lambda { params, body }) => Ok(Arc::new(LambdaExpr::new(
            params.clone(),
            create_physical_expr(body, input_dfschema, execution_props)?,
        ))),
        Expr::LambdaVariable(LambdaVariable {
            name,
            field,
            spans: _,
        }) => lambda_variable(
            name,
            Arc::clone(field),
        ),
    }
}
```

</details>
<br>

The added `LambdaUDF` trait is almost a clone of `ScalarUDFImpl`, with the exception of: 
1. `return_field_from_args` and `invoke_with_args`, where now `args.args` is a list of enums with two variants: `Value` or `Lambda` instead of a list of values
2. the addition of `lambdas_parameters`, which return a `Field` for each parameter supported for every lambda argument based on the `Field` of the non lambda arguments
3. the removal of `return_field` and the deprecated ones `is_nullable` and `display_name`.

<details><summary>LambdaUDF</summary>

```rust

trait LambdaUDF {
    /// Returns a list of the same size as args where each value is the logic below applied to value at the correspondent position in args:
    /// 
    /// If it's a value, return None
    /// If it's a lambda, return the list of all parameters that that lambda supports
    /// based on the Field of the non-lambda arguments
    /// 
    /// Example for array_transform:
    /// 
    /// `array_transform([2, 8], v -> v > 4)`
    /// 
    /// let lambdas_parameters = array_transform.lambdas_parameters(&[
    ///      ValueOrLambdaParameter::Value(Field::new("", DataType::new_list(DataType::Int32, false)))]), // the Field associated with the literal `[2, 8]`
    ///      ValueOrLambdaParameter::Lambda, // A lambda
    /// ]?;
    ///
    /// assert_eq!(
    ///      lambdas_parameters,
    ///      vec![
    ///         None, // it's a value, return None
    ///         // it's a lambda, return it's supported parameters, regardless of how many are actually used
    ///         Some(vec![
    ///             Field::new("", DataType::Int32, false), // the value being transformed, 
    ///             Field::new("", DataType::Int32, false), // the 1-based index being transformed, not used on the example above, but implementations doesn't need to care about it
    ///         ])
    ///      ]
    /// )
    fn lambdas_parameters(
        &self,
        args: &[ValueOrLambdaParameter],
    ) -> Result<Vec<Option<Vec<Field>>>>;
    fn return_field_from_args(&self, args: LambdaReturnFieldArgs) -> Result<FieldRef>;
    fn invoke_with_args(&self, args: LambdaFunctionArgs) -> Result<ColumnarValue>;
   // ... omitted methods that are similar in ScalarUDFImpl
}

pub enum ValueOrLambdaParameter {
    /// A columnar value with the given field
    Value(FieldRef),
    /// A lambda
    Lambda,
}

/// Information about arguments passed to the function
///
/// This structure contains metadata about how the function was called
/// such as the type of the arguments, any scalar arguments and if the
/// arguments can (ever) be null
///
/// See [`LambdaUDF::return_field_from_args`] for more information
pub struct LambdaReturnFieldArgs<'a> {
    /// The data types of the arguments to the function
    ///
    /// If argument `i` to the function is a lambda, it will be the field returned by the
    /// lambda when executed with the arguments returned from `LambdaUDF::lambdas_parameters`
    ///
    /// For example, with `array_transform([1], v -> v == 5)`
    /// this field will be `[
    //      ValueOrLambdaField::Value(Field::new("", DataType::new_list(DataType::Int32, false), false)),
    //      ValueOrLambdaField::Lambda(Field::new("", DataType::Boolean, false))
    //  ]`
    pub arg_fields: &'a [ValueOrLambdaField],
    /// Is argument `i` to the function a scalar (constant)?
    ///
    /// If the argument `i` is not a scalar, it will be None
    ///
    /// For example, if a function is called like `my_function(column_a, 5)`
    /// this field will be `[None, Some(ScalarValue::Int32(Some(5)))]`
    pub scalar_arguments: &'a [Option<&'a ScalarValue>],
}

/// A tagged FieldRef indicating whether it correspond the field of a value or the field of the output of a lambda argument
pub enum ValueOrLambdaField {
    /// The FieldRef of a ColumnarValue argument
    Value(FieldRef),
    /// The return FieldRef of the lambda body when evaluated with the parameters from LambdaUDF::lambda_parameters
    Lambda(FieldRef),
}

/// Arguments passed to [`LambdaUDF::invoke_with_args`] when invoking a
/// lambda function.
pub struct LambdaFunctionArgs {
    /// The evaluated arguments to the function
    pub args: Vec<ValueOrLambda>,
    /// Field associated with each arg, if it exists
    pub arg_fields: Vec<ValueOrLambdaField>,
    /// The number of rows in record batch being evaluated
    pub number_rows: usize,
    /// The return field of the lambda function returned (from `return_type`
    /// or `return_field_from_args`) when creating the physical expression
    /// from the logical expression
    pub return_field: FieldRef,
    /// The config options at execution time
    pub config_options: Arc<ConfigOptions>,
}

/// A lambda argument to a LambdaFunction
pub struct LambdaFunctionLambdaArg {
    /// The parameters defined in this lambda
    ///
    /// For example, for `array_transform([2], v -> -v)`,
    /// this will be `vec![Field::new("v", DataType::Int32, true)]`
    pub params: Vec<FieldRef>,
    /// The body of the lambda
    ///
    /// For example, for `array_transform([2], v -> -v)`,
    /// this will be the physical expression of `-v`
    pub body: Arc<dyn PhysicalExpr>,
    /// A RecordBatch containing at least the captured columns inside this lambda body, if any
    /// Note that it may contain additional, non-specified columns,
    /// but that's implementation detail and should not be relied upon
    ///
    /// For example, for `array_transform([2], v -> v + t.a + t.b)`,
    /// this will be a `RecordBatch` with at least two columns, `t.a` and `t.b`
    pub captures: Option<RecordBatch>,
}

// An argument to a LambdaUDF
pub enum ValueOrLambda {
    Value(ColumnarValue),
    Lambda(LambdaFunctionLambdaArg),
}
```


</details>

<details><summary>array_transform lambdas_parameters implementation</summary>

```rust
impl LambdaUDF for ArrayTransform {
    fn lambdas_parameters(
        &self,
        args: &[ValueOrLambdaParameter],
    ) -> Result<Vec<Option<Vec<Field>>>> {
        // list is the field of [2, 3]: Field::new("", DataType::new_list(DataType::Int32, false), false)
        let [ValueOrLambdaParameter::Value(list), ValueOrLambdaParameter::Lambda] = args
        else {
            return exec_err!(
                "{} expects a value follewed by a lambda, got {:?}",
                self.name(),
                args
            );
        };

        // the field of [2, 3] inner values: Field::new("", DataType::Int32, false)
        let (field, index_type) = match list.data_type() {
            DataType::List(field) => (field, DataType::Int32),
            DataType::LargeList(field) => (field, DataType::Int64),
            DataType::FixedSizeList(field, _) => (field, DataType::Int32),
            _ => return exec_err!("expected list, got {list}"),
        };

        // we don't need to omit the index in the case the lambda don't specify, e.g. array_transform([], v -> v*2),
        // nor check whether the lambda contains more than two parameters, e.g. array_transform([], (v, i, j) -> v+i+j),
        // as datafusion will do that for us
        let value = Field::new("", field.data_type().clone(), field.is_nullable())
            .with_metadata(field.metadata().clone());
        let index = Field::new("", index_type, false);

        Ok(vec![None, Some(vec![value, index])])
    }
}
```

</details>

<details><summary>array_transform return_field_from_args implementation</summary>

```rust
impl LambdaUDF for ArrayTransform {
    fn return_field_from_args(
        &self,
        args: datafusion_expr::LambdaReturnFieldArgs,
    ) -> Result<Arc<Field>> {
        // [
        //    Field::new("", DataType::new_list(DataType::Int32, false), false),
        //    Field::new("", DataType::Boolean, false),
        // ]
        let [ValueOrLambdaField::Value(list), ValueOrLambdaField::Lambda(lambda)] =
            take_function_args(self.name(), args.arg_fields)?
        else {
            return exec_err!(
                "{} expects a value follewed by a lambda, got {:?}",
                self.name(),
                args
            );
        };

        // lambda is the return_field of the lambda body
        // when evaluated with the parameters from lambdas_parameters
        let field = Arc::new(Field::new(
            Field::LIST_FIELD_DEFAULT_NAME,
            lambda.data_type().clone(),
            lambda.is_nullable(),
        ));

        let return_type = match list.data_type() {
            DataType::List(_) => DataType::List(field),
            DataType::LargeList(_) => DataType::LargeList(field),
            DataType::FixedSizeList(_, size) => DataType::FixedSizeList(field, *size),
            other => plan_err!("expected list, got {other}"),
        };

        Ok(Arc::new(Field::new("", return_type, list.is_nullable())))
    }
}
```

</details>

<details><summary>array_transform invoke_with_args implementation</summary>


```rust
impl LambdaUDF for ArrayTransform {
    fn invoke_with_args(&self, args: LambdaFunctionArgs) -> Result<ColumnarValue> {
        let [list_value, lambda] = take_function_args(self.name(), &args.args)?;

        // list = [2, 3]
        // lambda = LambdaFunctionLambdaArg {
        //    params: vec![Field::new("v", DataType::Int32, false)],
        //    body: PhysicalExpr("v != t.n"),// the physical expression of the lambda *body*, and not the lambda itself: this is not a LambdaExpr. 
        //    captures: Some(record_batch!("t.n", Int32, [2]))
        // }
        let (ValueOrLambda::Value(list_value), ValueOrLambda::Lambda(lambda)) =
            (list_value, lambda)
        else {
            return exec_err!(
                "{} expects a value followed by a lambda, got {} and {}",
                self.name(),
                list_value,
                lambda,
            );
        };

        let list_array = list_value.to_array(args.number_rows)?;
        let list_values = match list_array.data_type() {
            DataType::List(_) => list_array.as_list::<i32>().values(),
            DataType::LargeList(_) => list_array.as_list::<i64>().values(),
            DataType::FixedSizeList(_, _) => list_array.as_fixed_size_list().values(),
            other => exec_err!("expected list, got {other}")
        }

        // if any column got captured, we need to adjust it to the values arrays,
        // duplicating values of list with mulitple values and removing values of empty lists
        // list_indices is not cheap so is important to avoid it when no column is captured
        let adjusted_captures = lambda
            .captures
            .as_ref()
            //list_indices return the row_number for each sublist element: [[1, 2], [3], [4]] => [0,0,1,2], not included here
            .map(|captures| take_record_batch(captures, &list_indices(&list_array)?))
            .transpose()?
            .unwrap_or_else(|| {
                RecordBatch::try_new_with_options(
                    Arc::new(Schema::empty()),
                    vec![],
                    &RecordBatchOptions::new().with_row_count(Some(list_values.len())),
                )
                .unwrap()
            });

        // by using closures, bind_lambda_variables can evaluate only the needed ones avoiding unnecessary computations
        let values_param = || Ok(Arc::clone(list_values));
        //elements_indices return the index of each element within its sublist: [[5, 3], [7, 1, 1]] => [1, 2, 1, 2, 3], not included here
        let indices_param = || elements_indices(&list_array);

        let binded_body = bind_lambda_variables(
            Arc::clone(&lambda.body),
            &lambda.params,
            &[&values_param, &indices_param],
        )?;

        // call the transforming expression with the record batch
        let transformed_values = binded_body
            .evaluate(&adjusted_captures)?
            .into_array(list_values.len())?;

        let field = match args.return_field.data_type() {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _) => Arc::clone(field),
            _ => {
                return exec_err!(
                    "{} expected ScalarFunctionArgs.return_field to be a list, got {}",
                    self.name(),
                    args.return_field
                )
            }
        };

        let transformed_list = match list_array.data_type() {
            DataType::List(_) => {
                let list = list_array.as_list();

                Arc::new(ListArray::new(
                    field,
                    list.offsets().clone(),
                    transformed_values,
                    list.nulls().cloned(),
                )) as ArrayRef
            }
            DataType::LargeList(_) => {
                let large_list = list_array.as_list();

                Arc::new(LargeListArray::new(
                    field,
                    large_list.offsets().clone(),
                    transformed_values,
                    large_list.nulls().cloned(),
                ))
            }
            DataType::FixedSizeList(_, value_length) => {
                Arc::new(FixedSizeListArray::new(
                    field,
                    *value_length,
                    transformed_values,
                    list_array.as_fixed_size_list().nulls().cloned(),
                ))
            }
            other => exec_err!("expected list, got {other}")?,
        };

        Ok(ColumnarValue::Array(transformed_list))
    }
}
```

</details>

<details><summary>How relevant LambdaUDF methods would be called and what they would return during planning and evaluation of the example</summary>


```rust
// this is called at sql planning
let lambdas_parameters = lambda_udf.lambdas_parameters(&[
    ValueOrLambdaParameter::Value(Field::new("", DataType::new_list(DataType::Int32, false), false)), // the Field of the [2, 3] literal
    ValueOrLambdaParameter::Lambda, // A unspecified lambda. On the example, v -> v != t.n
])?;

assert_eq!(
    lambdas_parameters,
    vec![
            // the [2, 3] argument, not a lambda so no parameters
            None,
            // the parameters that *can* be declared on the lambda, and not only 
            // those actually declared: the implementation doesn't need to care 
            // about it
            Some(vec![
                Field::new("", DataType::Int32, false), // the list inner value
                Field::new("", DataType::Int32, false), // the 1-based index of the element being transformed
            ])]
);



// this is called every time ExprSchemable is called on a LambdaFunction
let return_field = array_transform.return_field_from_args(&LambdaReturnFieldArgs {
    arg_fields: &[
        ValueOrLambdaField::Value(Field::new("", DataType::new_list(DataType::Int32, false), false)),
        ValueOrLambdaField::Lambda(Field::new("", DataType::Boolean, false)), // the return_field of the expression "v != t.n" when "v" is of the type returned in lambdas_parameters
    ],
    scalar_arguments // irrelevant
})?;

assert_eq!(return_field, Field::new("", DataType::new_list(DataType::Boolean, false), false));



let value = array_transform.evaluate(&LambdaFunctionArgs {
    args: vec![
        ValueOrLambda::Value(List([2, 3])),
        ValueOrLambda::Lambda(LambdaFunctionLambdaArg {
            params: vec![Field::new("v", DataType::Int32, false)],
            body: PhysicalExpr("v != t.n"),// the physical expression of the lambda *body*, and not the lambda itself: this is not a LambdaExpr. 
            captures: Some(record_batch!("t.n", Int32, [2]))
        }),
    ],
    arg_fields, // same as above
    number_rows: 1,
    return_field, // same as above
    config_options, // irrelevant
})?;

assert_eq!(value, BooleanArray::from([false, true]))
```

</details>
<br>
<br>

A pair LambdaUDF/LambdaUDFImpl like ScalarFunction was not used because those exist only [to maintain backwards compatibility with the older API](https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.ScalarUDF.html#api-note) #8045

LambdaFunction invocation:

Instead of evaluating all it's arguments as ScalarFunction, LambdaFunction does the following:

1. If it's a non lambda argument, evaluate as usual, and provide the resulting `ColumnarValue` to `LambdaUDF::evaluate` as a `ValueOrLambda::Value`
2. If it's a lambda, construct a `LambdaFunctionLambdaArg` containing the lambda body physical expression and a record batch containing any captured columns as a `ValueOrLambda::Lambda` and provide it to `LambdaUDF::evaluate`. To avoid costly copies of uncaptured columns, we swap them with a `NullArray` while keeping the number of columns on the batch the same so captured columns indices are kept stable across the whole tree. The recent #18329 instead projects-out uncaptured columns and rewrites the expr adjusting columns indexes. If that is preferrable we can generalize that implementation and use it here too.

<details><summary>LambdaFunction evalution</summary>

```rust

impl PhysicalExpr for LambdaFunctionExpr {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let args = self.args
            .map(|arg| {
                match arg.as_any().downcast_ref::<LambdaExpr>() {
                    Some(lambda) => {
                        // helper method that returns the indices of the captured columns. In the example, the only column available (index 0) is captured, so this would be HashSet(0)
                        let captures = lambda.captures();

                        let captures = if !captures.is_empty() {
                            let (fields, columns): (Vec<_>, _) = std::iter::zip(
                                batch.schema_ref().fields(),
                                batch.columns(),
                            )
                            .enumerate()
                            .map(|(column_index, (field, column))| {
                                if captures.contains(&column_index) {
                                    (Arc::clone(field), Arc::clone(column))
                                } else {
                                    (
                                        Arc::new(Field::new(
                                            field.name(),
                                            DataType::Null,
                                            false,
                                        )),
                                        Arc::new(NullArray::new(column.len())) as _,
                                    )
                                }
                            })
                            .unzip();

                            let schema = Arc::new(Schema::new(fields));

                            Some(RecordBatch::try_new(schema, columns)?)
                        } else {
                            None
                        };

                        Ok(ValueOrLambda::Lambda(LambdaFunctionLambdaArg {
                            params, // irrelevant,
                            body: Arc::clone(lambda.body()), // use the lambda body and not the lambda itself
                            captures,
                        }))
                    }
                    None => Ok(ValueOrLambda::Value(arg.evaluate(batch)?)),
                }
            })
            .collect::<Result<Vec<_>>>()?;

        // evaluate the function
        let output = self.fun.invoke_with_args(LambdaFunctionArgs {
            args,
            arg_fields, // irrelevant
            number_rows: batch.num_rows(),
            return_field: Arc::clone(&self.return_field),
            config_options: Arc::clone(&self.config_options),
        })?;

        Ok(output)
    }
}

```

</details>
<br>

Why `LambdaVariable` and not `Column`:

Existing tree traversals that operate on columns would break if some column nodes referenced to a lambda parameter and not a real column. In the example query, projection pushdown would try to push the lambda parameter "v", which won't exist in table "t".

Example of code of another traversal that would break:

```rust
fn minimize_join_filter(expr: Arc<dyn PhysicalExpr>, ...) -> JoinFilter {
    let mut used_columns = HashSet::new();
    expr.apply(|expr| {
        if let Some(col) = expr.as_any().downcast_ref::<Column>() {
            // if this is a lambda column, this function will break
            used_columns.insert(col.index());
        }
        Ok(TreeNodeRecursion::Continue)
    });
    ...
}
```

Furthermore, the implemention of `ExprSchemable` and `PhysicalExpr::return_field` for `Column` expects that the schema it receives as a argument contains an entry for its name, which is not the case for lambda parameters. 

By including a `FieldRef` on `LambdaVariable` that should be resolved either during construction time, as in the sql planner, or later by the an `AnalyzerRule`, `ExprSchemable` and `PhysicalExpr::return_field` simply return it's own Field:

<details><summary>LambdaVariable ExprSchemable and PhysicalExpr::return_field implementation </summary>

```rust
impl ExprSchemable for Expr {
   fn to_field(
        &self,
        schema: &dyn ExprSchema,
    ) -> Result<(Option<TableReference>, Arc<Field>)> {
        let (relation, schema_name) = self.qualified_name();
        let field = match self {
           Expr::LambdaVariable(l) => Ok(Arc::clone(&l.field.ok_or_else(|| plan_err!("Unresolved LambdaVariable {}", l.name)))),
           ...
        }?;

        Ok((
            relation,
            Arc::new(field.as_ref().clone().with_name(schema_name)),
        ))
    }
    ...
}

impl PhysicalExpr for LambdaVariable {
    fn return_field(&self, _input_schema: &Schema) -> Result<FieldRef> {
        Ok(Arc::clone(&self.field))
    }
    ...
}
```

</details>
<br>

For reference, [Spark](https://github.com/apache/spark/blob/8b68a172d34d2ed9bd0a2deefcae1840a78143b6/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/higherOrderFunctions.scala#L77) and [Substrait](https://substrait.io/expressions/lambda_expressions/#parameter-references) also use a specialized node instead of a regular column

There's also discussions on making every expr own it's type: #18845, #12604

<details><summary>Possible fixes discarded due to complexity, requiring downstream changes and implementation size:</summary>

1. Add a new set of TreeNode methods that provides the set of lambdas parameters names seen during the traversal, so column nodes can be tested if they refer to a regular column or to a lambda parameter. Any downstream user that wants to support lambdas would need use those methods instead of the existing ones. This also would add 1k+ lines to the PR.

```rust
impl Expr {
    pub fn transform_with_lambdas_params<
        F: FnMut(Self, &HashSet<String>) -> Result<Transformed<Self>>,
    >(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {}
}
```

How minimize_join_filter would looks like:


```rust
fn minimize_join_filter(expr: Arc<dyn PhysicalExpr>, ...) -> JoinFilter {
    let mut used_columns = HashSet::new();
    expr.apply_with_lambdas_params(|expr, lambdas_params| {
        if let Some(col) = expr.as_any().downcast_ref::<Column>() {
            // dont include lambdas parameters
            if !lambdas_params.contains(col.name()) {
                used_columns.insert(col.index());
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    ...
}
```

2. Add a flag to the Column node indicating if it refers to a lambda parameter. Still requires checking for it on existing tree traversals that works on Columns (30+) and also downstream.

```rust
//logical
struct Column {
    pub relation: Option<TableReference>,
    pub name: String,
    pub spans: Spans,
    pub is_lambda_parameter: bool,
}

//physical
struct Column {
    name: String,
    index: usize,
    is_lambda_parameter: bool,
}
```


How minimize_join_filter would look like:

```rust
fn minimize_join_filter(expr: Arc<dyn PhysicalExpr>, ...) -> JoinFilter {
    let mut used_columns = HashSet::new();
    expr.apply(|expr| {
        if let Some(col) = expr.as_any().downcast_ref::<Column>() {
            // dont include lambdas parameters
            if !col.is_lambda_parameter {
                used_columns.insert(col.index());
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    ...
}
```


1. Add a new set of TreeNode methods that provides a schema that includes the lambdas parameters for the scope of the node being visited/transformed:

```rust
impl Expr {
    pub fn transform_with_schema<
        F: FnMut(Self, &DFSchema) -> Result<Transformed<Self>>,
    >(
        self,
        schema: &DFSchema,
        f: F,
    ) -> Result<Transformed<Self>> { ... }
    ... other methods
}
```

For any given LambdaFunction found during the traversal, a new schema is created for each lambda argument that contains it's parameter, returned from LambdaUDF::lambdas_parameters
How it would look like:

```rust

pub fn infer_placeholder_types(self, schema: &DFSchema) -> Result<(Expr, bool)> {
        let mut has_placeholder = false;
        // Provide the schema as the first argument. 
        // Transforming closure receive an adjusted_schema as argument
        self.transform_with_schema(schema, |mut expr, adjusted_schema| {
            match &mut expr {
                // Default to assuming the arguments are the same type
                Expr::BinaryExpr(BinaryExpr { left, op: _, right }) => {
                    // use adjusted_schema and not schema. Those expressions may contain 
                    // columns referring to a lambda parameter, which Field would only be
                    // available in adjusted_schema and not in schema
                    rewrite_placeholder(left.as_mut(), right.as_ref(), adjusted_schema)?;
                    rewrite_placeholder(right.as_mut(), left.as_ref(), adjusted_schema)?;
                }
    ....

```

2. Make available trought LogicalPlan and ExecutionPlan nodes a schema that includes all lambdas parameters from all expressions owned by the node, and use this schema for tree traversals. For nodes which won't own any expression, the regular schema can be returned


```rust
impl LogicalPlan {
    fn lambda_extended_schema(&self) -> &DFSchema;
}

trait ExecutionPlan {
    fn lambda_extended_schema(&self) -> &DFSchema;
}

//usage
impl LogicalPlan {
    pub fn replace_params_with_values(
            self,
            param_values: &ParamValues,
        ) -> Result<LogicalPlan> {
            self.transform_up_with_subqueries(|plan| {
                // use plan.lambda_extended_schema() containing lambdas parameters
                // instead of plan.schema() which wont
                let lambda_extended_schema = Arc::clone(plan.lambda_extended_schema());
                let name_preserver = NamePreserver::new(&plan);
                plan.map_expressions(|e| {
                    // if this expression is child of lambda and contain columns referring it's parameters
                    // the lambda_extended_schema already contain them
                    let (e, has_placeholder) = e.infer_placeholder_types(&lambda_extended_schema)?;
    ....

```
</details>
<br>

`LambdaVariable` evaluation, current implementation:

The physical `LambdaVariable` contains an optional `ColumnarValue` that must be binded for each batch before evaluation with the helper function `bind_lambda_variables`, which rewrites the whole lambda body, binding any variable of the tree.

<details><summary>LambdaVariable::evaluate</summary>

```rust
impl PhysicalExpr for LambdaVariable {
    fn evaluate(&self, _batch: &RecordBatch) -> Result<ColumnarValue> {
        self.value.clone().ok_or_else(|| exec_datafusion_err!("Physical LambdaVariable {} unbinded value", self.name))
    }
}
```

</details>
<br>

Unbinded:
```
LambdaExpression
    parameters: ["v"]
    body:
    ComparisonExpression(!=)
        left:
            LambdaVariableExpression("v", Field::new("", Int32, false), None)
        right:
            ColumnExpression("n")
```

After binding:

```
LambdaExpression
    parameters: ["v"]
    body:
    ComparisonExpression(!=)
        left:
            LambdaVariableExpression("v", Field::new("", Int32, false), Some([2, 3]))
        right:
            ColumnExpression("n")
```

Alternative:

Make the `LambdaVariable` evaluate it's value from the batch passed to `PhysicalExpr::evaluate` as a regular column. For that, instead of binding the body, the `LambdaUDF` implementation would merge the captured batch of a lambda with the values of it's parameters. So that it happen via an index as a regular column, the schema used plan to physical `LambdaVariable` must contain the lambda parameters. This would be the only place during planning that a schema would contain those parameters. Otherwise it only can get the value from the batch via name instead of index

1. Add a index to LambdaVariable, similar to Column, and remove the optional value.

```rust
struct LambdaVariable {
    name: String, // "v", of the lambda body: v != t.n
    field: FieldRef, // Field::new("", DataType::Int32, false)
    index: usize, // 1
}
```

2. Insert the lambda parameters only at the Schema used to do the physical planning, to compute the index of a LambdaVariable 

<details><summary>how physical planning would look like</summary>

```rust
fn create_physical_expr(
    e: &Expr,
    input_dfschema: &DFSchema,
    execution_props: &ExecutionProps,
) -> Result<Arc<dyn PhysicalExpr>> {
    let input_schema = input_dfschema.as_arrow();

    match e {
        ...
        Expr::LambdaFunction(LambdaFunction { func, args}) => {
            let args_metadata = args.iter()
                .map(|arg| if arg.is::<LambdaExpr>() {
                    Ok(ValueOrLambdaParameter::Lambda)
                } else {
                    Ok(ValueOrLambdaParameter::Value(arg.to_field(input_dfschema)?))
                })
                .collect()?;

            let lambdas_parameters = func.lambdas_parameters(&args_metadata)?;

            let physical_args = std::iter::zip(args, lambdas_parameters)
                .map(|(arg, lambda_parameters)| {
                    match (arg.downcast_ref::<LambdaExpr>(), lambda_parameters) {
                        (Some(lambda), Some(lambda_parameters)) => {
                            let extended_dfschema = merge_schema_and_parameters(input_dfschame, lambda_parameters)?;

                            create_physical_expr(body, extended_dfschema, execution_props)
                        }
                        (None, None) => create_physical_expr(arg, input_dfschema, execution_props),
                        (Some(_), None) => plan_err!("lambdas_parameters returned None for a lambda")
                        (None, Some(_)) => plan_err!("lambdas_parameters returned Some for a non lambda")
                    }
                })
                .collect()?;

            Ok(Arc::new(LambdaFunctionExpr::try_new(
                Arc::clone(func),
                physical_args,
                input_schema,
                config_options: ... // irrelevant
            )?))
        }
    }
}
```

</details>
<br>

3. Insert the lambda parameters values into the RecordBatch during the evaluation phase: the LambdaUDF, instead of binding the lambda body variables, inserts it's parameters on the captured RecordBatch it receives on LambdaFunctionLambdaArg. 

How ArrayTransform::invoke_with_args would look like:

```rust
        ...
        let values_param = || Ok(Arc::clone(list_values));
        let indices_param = || elements_indices(&list_array);

        let merged_batch = merge_captures_with_params(
            adjusted_captures,
            &lambda.params,
            &[&values_param, &indices_param],
        )?;

        // call the transforming expression with the record batch
        let transformed_values = lambda.body
            .evaluate(&merged_batch)?
            .into_array(list_values.len())?;
        
        ...
```

<br>

Why is `LambdaVariable` `Field` is an `Option`?

So expr_api users can construct a LambdaVariable just by using it's name, without having to set it's field. An `AnalyzerRule` will then set the `LambdaVariable` field based on the returned values from `LambdaUDF::lambdas_parameters` of any `LambdaFunction` it finds while traversing down a expr tree. We may include that rule on the default rules list for when the plan/expression tree is transformed by another rule in a way that changes the types of non lambda arguments of a lambda function, as it may change the types of it's lambda parameters, which would render `LambdaVariable` field's out of sync, as the rule would fix it. Or to not increase planning time we don't include it by default and instruct `expr_api` users to add it manually if needed



```rust
array_transform(
    col("my_array"),
    lambda(
        vec!["current_value"],
        2 * lambda_variable("current_value")
    )
)

//instead of 

array_transform(
    col("my_array"),
    lambda(
        vec!["current_value"],
        2 * lambda_variable("current_value", Field::new("", DataType::Int32, false))
    )
)
```


Why set `LambdaVariable` field during sql planning if it's optional and can be set later via an `AnalyzerRule`?

Some parts of sql planning checks the type/nullability of the already planned children expression of the expr it's planning, and would error if doing so on a unresolved `LambdaVariable`
Take as example this expression: `array_transform([[0, 1]], v -> v[1])`. `FieldAccess` `v[1]` planning is handled by the `ExprPlanner` `FieldAccessPlanner`, which checks the datatype of `v`, a lambda variable, which `ExprSchemable` implementation depends on it's field being resolved, and not on the `PlannerContext` schema, requiring sql planner to plan `LambdaVariables` with a resolved field


<details><summary>FieldAccessPlanner</summary>

```rust
pub struct FieldAccessPlanner;

impl ExprPlanner for FieldAccessPlanner {
    fn plan_field_access(
        &self,
        expr: RawFieldAccessExpr, // "v[1]"
        schema: &DFSchema,
    ) -> Result<PlannerResult<RawFieldAccessExpr>> {
        // { "v", "[1]" }
        let RawFieldAccessExpr { expr, field_access } = expr;

        match field_access {
            ...
            // expr[idx] ==> array_element(expr, idx)
            GetFieldAccess::ListIndex { key: index } => {
                match expr {
                    ...
                    // ExprSchemable::get_type called
                    _ if matches!(expr.get_type(schema)?, DataType::Map(_, _)) => {
                        Ok(PlannerResult::Planned(Expr::ScalarFunction(
                            ScalarFunction::new_udf(
                                get_field_inner(),
                                vec![expr, *index],
                            ),
                        )))
                    }
                }
            }
        }
    }
}
```

</details>
<br>

 Therefore we can't plan all arguments on a single pass, and must first plan the non-lambda arguments, collect their types and nullability, pass them to `LambdaUDF::lambdas_parameters`, which will derive the type of it's lambda parameters based on the type of it's non-lambda argument, and return it to the planner, which, for each unplanned lambda argument, will create a new `PlannerContext` via `with_lambda_parameters`, which contains a mapping of lambdas parameters names to it's type. Then, when planning a `ast::Identifier`, it first check whether a lambda parameter with the given name exists, and if so, plans it into a `Expr::LambdaVariable` with a resolved field, otherwise plan it into a regular `Expr::Column`.



<details><summary>sql planning</summary>


```rust
struct PlannerContext {
    /// The parameters of all lambdas seen so far
    lambdas_parameters: HashMap<String, FieldRef>,
    // ... omitted fields
}

impl PlannerContext {
    pub fn with_lambda_parameters(
        mut self,
        arguments: impl IntoIterator<Item = FieldRef>,
    ) -> Self {
        self.lambdas_parameters
            .extend(arguments.into_iter().map(|f| (f.name().clone(), f)));

        self
    }
}

// copied from sqlparser
struct LambdaFunction {
    pub params: OneOrManyWithParens<Ident>, // One("v")
    pub body: Box<Expr>, // v != t.n
}

// copied from sqlparser
enum OneOrManyWithParens<T> {
    One(T), // "v"
    Many(Vec<T>),
}

/// the planning would happens as the following:

enum ExprOrLambda {
    Expr(Expr), // planned [2, 3]
    Lambda(ast::LambdaFunction), // unplanned v -> v != t.n
}

impl SqlToRel {
    // example function, won't exist
    fn plan_array_transform(&self, array_transform: Arc<dyn LambdaUDF>, args: Vec<ast::Expr>, schema: &DFSchema, planner_context: &mut PlannerContext) -> Result<Expr> {
        let args = args.into_iter()
            .map(|arg| match arg {
                ast::Expr::LambdaFunction(l) => Ok(ExprOrLambda::Lambda(l)),//skip planning until we plan non lambda args
                arg => Ok(ExprOrLambda::Expr(
                    self.sql_fn_arg_to_logical_expr_with_name(
                        arg,
                        schema,
                        planner_context,
                    )?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let args_metadata = args.iter()
            .map(|arg| match arg {
                Expr(expr) => Ok(ValueOrLambda::Value(expr.to_field(schema)?)),
                Lambda(_) => Ok(ValueOrLambda::Lambda),
            })
            .collect::<Result<Vec<_>>>()?;
        
        let lambdas_parameters = array_transform.lambdas_parameters(&args_metadata)?;

        let args = std::iter::zip(args, lambdas_parameters)
            .map(|(arg, lambdas_parameters)| match (arg, lambdas_parameters) {
                (ExprOrLambda::Expr(planned_expr), None) => Ok(planned_expr),
                (ExprOrLambda::Lambda(unplanned_lambda), Some(lambda_parameters)) => {
                    let params =
                        unplanned_lambda.params
                            .iter()
                            .map(|p| p.value.clone())
                            .collect();

                    let lambda_parameters = lambda_params
                        .into_iter()
                        .zip(&params)
                        .map(|(field, name)| Arc::new(field.with_name(name)));

                    let mut planner_context = planner_context
                        .clone()
                        .with_lambda_parameters(lambda_parameters);

                    Ok((
                        Expr::Lambda(Lambda {
                            params,
                            body: Box::new(self.sql_expr_to_logical_expr(
                                *lambda.body,
                                schema,
                                &mut planner_context,
                            )?),
                        }),
                        None,
                    ))
                }
                (ExprOrLambda::Expr(planned_expr), Some(lambda_parameters)) => plan_err!("lambdas_parameters returned Some for a value"),
                (ExprOrLambda::Lambda(unplanned_lambda), None) => plan_err!("lambdas_parameters returned None for a lambda"),
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Expr::LambdaFunction(LambdaFunction {
            func: array_transform,
            args,
        }))    
    }

    fn sql_identifier_to_expr(
        &self,
        id: ast::Ident,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        // simplified implementation
        if let Some(field) = planner_context.lambdas_parameters.get(id) {
            Ok(Expr::LambdaVariable(LambdaVariable {
                name: id, // "v"
                field, // Field::new("", DataType::Int32, false)
            }))
        } else {
            Ok(Expr::Column(Column::new(id)))
        }
    }
}

```

</details>
</br>

`LambdaFunction` `Signature` is non functional

Currenty, `LambdaUDF::signature` returns the same `Signature` as `ScalarUDF`, but it's `type_signature` field is never used, as most variants of the `TypeSignature` enum aren't applicable to a lambda, and no type coercion is applied on it's arguments, being currently a implementation responsability. We should either add lambda compatible variants to the `TypeSignature` enum, create a new `LambdaTypeSignature` and `LambdaSignature`, or support no automatic type coercion at all on lambda functions.
