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

//! Physical expression for struct-aware casting of columns.

use crate::physical_expr::PhysicalExpr;
use arrow::{
    compute::CastOptions,
    datatypes::{DataType, FieldRef, Schema},
    record_batch::RecordBatch,
};
use datafusion_common::{
    Result, ScalarValue, format::DEFAULT_CAST_OPTIONS, nested_struct::cast_column,
};
use datafusion_expr_common::columnar_value::ColumnarValue;
use std::{
    any::Any,
    fmt::{self, Display},
    hash::Hash,
    sync::Arc,
};
/// A physical expression that applies [`cast_column`] to its input.
///
/// [`CastColumnExpr`] extends the regular [`CastExpr`](super::CastExpr) by
/// retaining schema metadata for both the input and output fields. This allows
/// the evaluator to perform struct-aware casts that honour nested field
/// ordering, preserve nullability, and fill missing fields with null values.
///
/// This expression is intended for schema rewriting scenarios where the
/// planner already resolved the input column but needs to adapt its physical
/// representation to a new [`arrow::datatypes::Field`]. It mirrors the behaviour of the
/// [`datafusion_common::nested_struct::cast_column`] helper while integrating
/// with the `PhysicalExpr` trait so it can participate in the execution plan
/// like any other column expression.
#[derive(Debug, Clone, Eq)]
pub struct CastColumnExpr {
    /// The physical expression producing the value to cast.
    expr: Arc<dyn PhysicalExpr>,
    /// The logical field of the input column.
    input_field: FieldRef,
    /// The field metadata describing the desired output column.
    target_field: FieldRef,
    /// Options forwarded to [`cast_column`].
    cast_options: CastOptions<'static>,
}

// Manually derive `PartialEq`/`Hash` as `Arc<dyn PhysicalExpr>` does not
// implement these traits by default for the trait object.
impl PartialEq for CastColumnExpr {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr)
            && self.input_field.eq(&other.input_field)
            && self.target_field.eq(&other.target_field)
            && self.cast_options.eq(&other.cast_options)
    }
}

impl Hash for CastColumnExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.input_field.hash(state);
        self.target_field.hash(state);
        self.cast_options.hash(state);
    }
}

impl CastColumnExpr {
    /// Create a new [`CastColumnExpr`].
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        input_field: FieldRef,
        target_field: FieldRef,
        cast_options: Option<CastOptions<'static>>,
    ) -> Self {
        Self {
            expr,
            input_field,
            target_field,
            cast_options: cast_options.unwrap_or(DEFAULT_CAST_OPTIONS),
        }
    }

    /// The expression that produces the value to be cast.
    pub fn expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    /// Field metadata describing the resolved input column.
    pub fn input_field(&self) -> &FieldRef {
        &self.input_field
    }

    /// Field metadata describing the output column after casting.
    pub fn target_field(&self) -> &FieldRef {
        &self.target_field
    }
}

impl Display for CastColumnExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CAST_COLUMN({} AS {})",
            self.expr,
            self.target_field.data_type()
        )
    }
}

impl PhysicalExpr for CastColumnExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.target_field.data_type().clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(self.target_field.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let value = self.expr.evaluate(batch)?;
        match value {
            ColumnarValue::Array(array) => {
                let casted =
                    cast_column(&array, self.target_field.as_ref(), &self.cast_options)?;
                Ok(ColumnarValue::Array(casted))
            }
            ColumnarValue::Scalar(scalar) => {
                let as_array = scalar.to_array_of_size(1)?;
                let casted = cast_column(
                    &as_array,
                    self.target_field.as_ref(),
                    &self.cast_options,
                )?;
                let result = ScalarValue::try_from_array(casted.as_ref(), 0)?;
                Ok(ColumnarValue::Scalar(result))
            }
        }
    }

    fn return_field(&self, _input_schema: &Schema) -> Result<FieldRef> {
        Ok(Arc::clone(&self.target_field))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        assert_eq!(children.len(), 1);
        let child = children.pop().expect("CastColumnExpr child");
        Ok(Arc::new(Self::new(
            child,
            Arc::clone(&self.input_field),
            Arc::clone(&self.target_field),
            Some(self.cast_options.clone()),
        )))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::expressions::{Column, Literal};
    use arrow::{
        array::{Array, ArrayRef, BooleanArray, Int32Array, StringArray, StructArray},
        datatypes::{DataType, Field, Fields, SchemaRef},
    };
    use datafusion_common::{
        Result as DFResult, ScalarValue,
        cast::{as_int64_array, as_string_array, as_struct_array, as_uint8_array},
    };

    fn make_schema(field: &Field) -> SchemaRef {
        Arc::new(Schema::new(vec![field.clone()]))
    }

    fn make_struct_array(fields: Fields, arrays: Vec<ArrayRef>) -> StructArray {
        StructArray::new(fields, arrays, None)
    }

    #[test]
    fn cast_primitive_array() -> DFResult<()> {
        let input_field = Field::new("a", DataType::Int32, true);
        let target_field = Field::new("a", DataType::Int64, true);
        let schema = make_schema(&input_field);

        let values = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![values])?;

        let column = Arc::new(Column::new_with_schema("a", schema.as_ref())?);
        let expr = CastColumnExpr::new(
            column,
            Arc::new(input_field.clone()),
            Arc::new(target_field.clone()),
            None,
        );

        let result = expr.evaluate(&batch)?;
        let ColumnarValue::Array(array) = result else {
            panic!("expected array");
        };
        let casted = as_int64_array(array.as_ref())?;
        assert_eq!(casted.value(0), 1);
        assert!(casted.is_null(1));
        assert_eq!(casted.value(2), 3);
        Ok(())
    }

    #[test]
    fn cast_struct_array_missing_child() -> DFResult<()> {
        let source_a = Field::new("a", DataType::Int32, true);
        let source_b = Field::new("b", DataType::Utf8, true);
        let input_field = Field::new(
            "s",
            DataType::Struct(
                vec![Arc::new(source_a.clone()), Arc::new(source_b.clone())].into(),
            ),
            true,
        );
        let target_a = Field::new("a", DataType::Int64, true);
        let target_c = Field::new("c", DataType::Utf8, true);
        let target_field = Field::new(
            "s",
            DataType::Struct(
                vec![Arc::new(target_a.clone()), Arc::new(target_c.clone())].into(),
            ),
            true,
        );

        let schema = make_schema(&input_field);
        let struct_array = make_struct_array(
            vec![Arc::new(source_a.clone()), Arc::new(source_b.clone())].into(),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("alpha"), Some("beta")]))
                    as ArrayRef,
            ],
        );
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(struct_array) as Arc<_>],
        )?;

        let column = Arc::new(Column::new_with_schema("s", schema.as_ref())?);
        let expr = CastColumnExpr::new(
            column,
            Arc::new(input_field.clone()),
            Arc::new(target_field.clone()),
            None,
        );

        let result = expr.evaluate(&batch)?;
        let ColumnarValue::Array(array) = result else {
            panic!("expected array");
        };
        let struct_array = as_struct_array(array.as_ref())?;
        let cast_a = as_int64_array(struct_array.column_by_name("a").unwrap().as_ref())?;
        assert_eq!(cast_a.value(0), 1);
        assert!(cast_a.is_null(1));

        let cast_c = as_string_array(struct_array.column_by_name("c").unwrap().as_ref())?;
        assert!(cast_c.is_null(0));
        assert!(cast_c.is_null(1));
        Ok(())
    }

    #[test]
    fn cast_nested_struct_array() -> DFResult<()> {
        let inner_source = Field::new(
            "inner",
            DataType::Struct(
                vec![Arc::new(Field::new("x", DataType::Int32, true))].into(),
            ),
            true,
        );
        let outer_field = Field::new(
            "root",
            DataType::Struct(vec![Arc::new(inner_source.clone())].into()),
            true,
        );

        let inner_target = Field::new(
            "inner",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("x", DataType::Int64, true)),
                    Arc::new(Field::new("y", DataType::Boolean, true)),
                ]
                .into(),
            ),
            true,
        );
        let target_field = Field::new(
            "root",
            DataType::Struct(vec![Arc::new(inner_target.clone())].into()),
            true,
        );

        let schema = make_schema(&outer_field);

        let inner_struct = make_struct_array(
            vec![Arc::new(Field::new("x", DataType::Int32, true))].into(),
            vec![Arc::new(Int32Array::from(vec![Some(7), None])) as ArrayRef],
        );
        let outer_struct = make_struct_array(
            vec![Arc::new(inner_source.clone())].into(),
            vec![Arc::new(inner_struct) as ArrayRef],
        );
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(outer_struct) as ArrayRef],
        )?;

        let column = Arc::new(Column::new_with_schema("root", schema.as_ref())?);
        let expr = CastColumnExpr::new(
            column,
            Arc::new(outer_field.clone()),
            Arc::new(target_field.clone()),
            None,
        );

        let result = expr.evaluate(&batch)?;
        let ColumnarValue::Array(array) = result else {
            panic!("expected array");
        };
        let struct_array = as_struct_array(array.as_ref())?;
        let inner =
            as_struct_array(struct_array.column_by_name("inner").unwrap().as_ref())?;
        let x = as_int64_array(inner.column_by_name("x").unwrap().as_ref())?;
        assert_eq!(x.value(0), 7);
        assert!(x.is_null(1));
        let y = inner.column_by_name("y").unwrap();
        let y = y
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean array");
        assert!(y.is_null(0));
        assert!(y.is_null(1));
        Ok(())
    }

    #[test]
    fn cast_struct_scalar() -> DFResult<()> {
        let source_field = Field::new("a", DataType::Int32, true);
        let input_field = Field::new(
            "s",
            DataType::Struct(vec![Arc::new(source_field.clone())].into()),
            true,
        );
        let target_field = Field::new(
            "s",
            DataType::Struct(
                vec![Arc::new(Field::new("a", DataType::UInt8, true))].into(),
            ),
            true,
        );

        let schema = make_schema(&input_field);
        let scalar_struct = StructArray::new(
            vec![Arc::new(source_field.clone())].into(),
            vec![Arc::new(Int32Array::from(vec![Some(9)])) as ArrayRef],
            None,
        );
        let literal =
            Arc::new(Literal::new(ScalarValue::Struct(Arc::new(scalar_struct))));
        let expr = CastColumnExpr::new(
            literal,
            Arc::new(input_field.clone()),
            Arc::new(target_field.clone()),
            None,
        );

        let batch = RecordBatch::new_empty(Arc::clone(&schema));
        let result = expr.evaluate(&batch)?;
        let ColumnarValue::Scalar(ScalarValue::Struct(array)) = result else {
            panic!("expected struct scalar");
        };
        let casted = array.column_by_name("a").unwrap();
        let casted = as_uint8_array(casted.as_ref())?;
        assert_eq!(casted.value(0), 9);
        Ok(())
    }
}
