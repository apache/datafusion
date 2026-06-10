#[cfg(test)]
mod multi_group_by_dictionary_test {
    use arrow::datatypes::Int32Type;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, DictionaryArray, PrimitiveArray, StringArray};
    use arrow::datatypes::{ArrowDictionaryKeyType, DataType, Field, Int64Type, Schema};

    use crate::aggregates::group_values::new_group_values;

    /// Build a `DictionaryArray` from a keys primitive array and a values string array.
    fn create_dict_array<K>(keys: PrimitiveArray<K>, values: StringArray) -> ArrayRef
    where
        K: ArrowDictionaryKeyType,
    {
        Arc::new(DictionaryArray::new(keys, Arc::new(values)))
    }

    fn create_multi_dict_schema(date_type_pairs: Vec<(DataType, DataType)>) -> Schema {
        let mut output_schema: Vec<Field> = vec![];
        for (i, (key, value)) in date_type_pairs.iter().enumerate() {
            output_schema.push(Field::new(
                format!("dictioanary_{}", i + 1),
                DataType::Dictionary(Box::new(key.clone()), Box::new(value.clone())),
                true,
            ))
        }
        Schema::new(output_schema)
    }

    #[test]
    fn test_example_guide() -> Result<(), Box<dyn std::error::Error>> {
        let types = vec![
            (DataType::Int32, DataType::Utf8),
            (DataType::Int64, DataType::Utf8View),
        ];
        let result_schema = Arc::new(create_multi_dict_schema(types));

        let arr1 = create_dict_array(
            PrimitiveArray::<Int32Type>::from(vec![Some(0), Some(1), Some(0)]),
            StringArray::from(vec!["foo", "bar"]),
        );
        let arr2 = create_dict_array(
            PrimitiveArray::<Int64Type>::from(vec![Some(1), Some(1), Some(0)]),
            StringArray::from(vec!["hello", "world"]),
        );
        let mut group_values = new_group_values(
            result_schema,
            &crate::aggregates::order::GroupOrdering::None,
        )?;
        let mut groups = vec![0; 200];
        group_values.intern(&[Arc::new(arr1), Arc::new(arr2)], &mut groups)?;
        Ok(())
    }
}
