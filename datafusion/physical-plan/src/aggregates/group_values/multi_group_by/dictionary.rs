#[cfg(test)]
mod multi_group_by_dictionary_test {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, DictionaryArray, LargeStringArray, PrimitiveArray, StringArray,
        StringViewArray,
    };
    use arrow::compute::cast;
    use arrow::datatypes::{
        ArrowDictionaryKeyType, DataType, Field, Int8Type, Int32Type, Schema,
    };
    use datafusion_expr::EmitTo;

    use crate::aggregates::group_values::{GroupValues, new_group_values};

    fn make_dict<K>(keys: PrimitiveArray<K>, values: StringArray) -> ArrayRef
    where
        K: ArrowDictionaryKeyType,
    {
        Arc::new(DictionaryArray::new(keys, Arc::new(values)))
    }

    fn make_dict_view<K>(keys: PrimitiveArray<K>, values: StringViewArray) -> ArrayRef
    where
        K: ArrowDictionaryKeyType,
    {
        Arc::new(DictionaryArray::new(keys, Arc::new(values)))
    }

    fn make_dict_large<K>(keys: PrimitiveArray<K>, values: LargeStringArray) -> ArrayRef
    where
        K: ArrowDictionaryKeyType,
    {
        Arc::new(DictionaryArray::new(keys, Arc::new(values)))
    }

    fn dict(key: DataType, value: DataType) -> DataType {
        DataType::Dictionary(Box::new(key), Box::new(value))
    }

    fn mk_schema(types: Vec<DataType>) -> Arc<Schema> {
        Arc::new(Schema::new(
            types
                .into_iter()
                .enumerate()
                .map(|(i, dt)| Field::new(format!("c{i}"), dt, true))
                .collect::<Vec<_>>(),
        ))
    }

    fn make_gv(schema: Arc<Schema>) -> Box<dyn GroupValues> {
        new_group_values(schema, &crate::aggregates::order::GroupOrdering::None).unwrap()
    }

    fn intern(group_values: &mut Box<dyn GroupValues>, cols: &[ArrayRef]) -> Vec<usize> {
        let mut groups = vec![];
        group_values.intern(cols, &mut groups).unwrap();
        groups
    }

    fn intern_all(schema: Arc<Schema>, cols: &[ArrayRef]) -> Vec<usize> {
        let mut group_values = make_gv(schema);
        intern(&mut group_values, cols)
    }

    fn emit_decoded(
        group_values: &mut Box<dyn GroupValues>,
        emit_to: EmitTo,
    ) -> (Vec<ArrayRef>, Vec<Vec<Option<String>>>) {
        let arrays = group_values.emit(emit_to).unwrap();
        let decoded = arrays
            .iter()
            .map(|arr| {
                let casted = cast(arr.as_ref(), &DataType::Utf8).unwrap();
                let s = casted.as_any().downcast_ref::<StringArray>().unwrap();
                (0..s.len())
                    .map(|i| (!s.is_null(i)).then(|| s.value(i).to_string()))
                    .collect()
            })
            .collect();
        (arrays, decoded)
    }

    fn int32_keys(v: Vec<Option<i32>>) -> PrimitiveArray<Int32Type> {
        PrimitiveArray::<Int32Type>::from(v)
    }

    fn int8_keys(v: Vec<Option<i8>>) -> PrimitiveArray<Int8Type> {
        PrimitiveArray::<Int8Type>::from(v)
    }

    // general_test: correctness invariants that hold for any multi-column dictionary grouping —
    // uniqueness, cross-batch stability, null semantics, multi-column/multi-type composition
    mod general_test {
        use super::*;

        // Covers: all-distinct pairs → unique IDs; cross-batch stability (same pair always same ID)
        #[test]
        fn test_distinct_pairs_and_cross_batch_consistency() {
            let schema = mk_schema(vec![
                dict(DataType::Int32, DataType::Utf8),
                dict(DataType::Int32, DataType::Utf8View),
            ]);
            let mut group_values = make_gv(schema);
            assert_eq!(
                intern(
                    &mut group_values,
                    &[
                        make_dict(
                            int32_keys(vec![Some(0), Some(1), Some(2)]),
                            StringArray::from(vec!["a", "b", "c"]),
                        ),
                        make_dict_view(
                            int32_keys(vec![Some(0), Some(1), Some(2)]),
                            StringViewArray::from(vec!["x", "y", "z"]),
                        ),
                    ],
                ),
                vec![0, 1, 2]
            );
            assert_eq!(
                intern(
                    &mut group_values,
                    &[
                        make_dict(
                            int32_keys(vec![Some(0), Some(1)]),
                            StringArray::from(vec!["c", "a"]),
                        ),
                        make_dict_view(
                            int32_keys(vec![Some(0), Some(1)]),
                            StringViewArray::from(vec!["z", "x"]),
                        ),
                    ],
                ),
                vec![2, 0]
            );
        }

        // Covers: null in one column groups by the other; (NULL,NULL) groups together;
        // null ordering is stable across repeated rows
        #[test]
        fn test_null_handling() {
            let schema = mk_schema(vec![
                dict(DataType::Int32, DataType::Utf8),
                dict(DataType::Int32, DataType::Utf8View),
            ]);
            assert_eq!(
                intern_all(
                    schema,
                    &[
                        make_dict(
                            int32_keys(vec![None, None, None, None, None]),
                            StringArray::from(vec!["x"]),
                        ),
                        make_dict_view(
                            int32_keys(vec![Some(0), Some(1), Some(0), None, None]),
                            StringViewArray::from(vec!["foo", "bar"]),
                        ),
                    ],
                ),
                // (NULL,"foo"), (NULL,"bar"), (NULL,"foo"), (NULL,NULL), (NULL,NULL)
                vec![0, 1, 0, 2, 2]
            );
        }

        // Covers: 5 columns (3+), Utf8/Utf8View/LargeUtf8 value types, Int32/Int8 key types,
        // distinct tuples → unique IDs, repeated tuples → same IDs, null in one column
        #[test]
        fn test_five_column_grouping() {
            let schema = mk_schema(vec![
                dict(DataType::Int32, DataType::Utf8),
                dict(DataType::Int32, DataType::Utf8View),
                dict(DataType::Int32, DataType::LargeUtf8),
                dict(DataType::Int8, DataType::Utf8),
                dict(DataType::Int32, DataType::Utf8View),
            ]);
            let keys = || int32_keys(vec![Some(0), Some(1), None, Some(0)]);
            let rep_keys = || int32_keys(vec![Some(0), Some(1), Some(0), Some(0)]);
            let i8k = || int8_keys(vec![Some(0i8), Some(1i8), Some(0i8), Some(0i8)]);
            assert_eq!(
                intern_all(
                    schema,
                    &[
                        make_dict(keys(), StringArray::from(vec!["a0", "a1"])),
                        make_dict_view(
                            rep_keys(),
                            StringViewArray::from(vec!["b0", "b1"])
                        ),
                        make_dict_large(
                            rep_keys(),
                            LargeStringArray::from(vec!["c0", "c1"])
                        ),
                        make_dict(i8k(), StringArray::from(vec!["d0", "d1"])),
                        make_dict_view(
                            rep_keys(),
                            StringViewArray::from(vec!["e0", "e1"])
                        ),
                    ],
                ),
                // (a0,b0,c0,d0,e0), (a1,b1,c1,d1,e1), (null,b0,c0,d0,e0), (a0,b0,c0,d0,e0) repeat
                vec![0, 1, 2, 0]
            );
        }
    }

    // dictionary_encoding: behavior specific to how dictionary arrays encode values —
    // key-index shuffling, non-normalized value arrays, bloated dictionaries, mixed key types
    mod dictionary_encoding {
        use super::*;

        // Covers: (foo,bar) ≠ (bar,foo) order sensitivity; same decoded value at different key
        // positions cross-batch; non-normalized dict (duplicate entries in values array);
        #[test]
        fn test_order_sensitivity_and_key_shuffling() {
            let schema = mk_schema(vec![
                dict(DataType::Int32, DataType::Utf8),
                dict(DataType::Int32, DataType::Utf8View),
                dict(DataType::Int32, DataType::Utf8),
            ]);
            let mut group_values = make_gv(schema);
            assert_eq!(
                intern(
                    &mut group_values,
                    &[
                        make_dict(
                            int32_keys(vec![Some(2), Some(0), None, Some(2), Some(2)]),
                            StringArray::from(vec!["bar", "junk", "foo"]),
                        ),
                        make_dict_view(
                            int32_keys(vec![Some(1), Some(0), Some(1), Some(1), Some(1)]),
                            StringViewArray::from(vec!["foo", "bar"]),
                        ),
                        make_dict(
                            int32_keys(vec![Some(0), Some(0), Some(0), Some(0), Some(1)]),
                            StringArray::from(vec!["alpha", "beta"]),
                        ),
                    ],
                ),
                // (foo,bar,alpha), (bar,foo,alpha), (null,bar,alpha), (foo,bar,alpha), (foo,bar,beta)
                vec![0, 1, 2, 0, 3]
            );
            assert_eq!(
                intern(
                    &mut group_values,
                    &[
                        make_dict(
                            int32_keys(vec![Some(3), Some(1), Some(4)]),
                            StringArray::from(vec!["x", "bar", "y", "foo", "new"]),
                        ),
                        make_dict_view(
                            int32_keys(vec![Some(2), Some(3), Some(0)]),
                            StringViewArray::from(vec!["val", "x", "bar", "foo"]),
                        ),
                        make_dict(
                            int32_keys(vec![Some(1), Some(1), Some(0)]),
                            StringArray::from(vec!["gamma", "alpha"]),
                        ),
                    ],
                ),
                // (foo,bar,alpha), (bar,foo,alpha), (new,val,gamma)
                vec![0, 1, 4]
            );
        }

        // Covers: mixed key types (Int8 vs Int32), Utf8 vs LargeUtf8 value types
        #[test]
        fn test_different_key_types() {
            let schema = mk_schema(vec![
                dict(DataType::Int8, DataType::Utf8),
                dict(DataType::Int32, DataType::LargeUtf8),
            ]);
            let mut group_values = make_gv(schema);
            assert_eq!(
                intern(
                    &mut group_values,
                    &[
                        make_dict(
                            int8_keys(vec![Some(0i8), Some(1i8)]),
                            StringArray::from(vec!["alpha", "beta"]),
                        ),
                        make_dict_large(
                            int32_keys(vec![Some(0), Some(1)]),
                            LargeStringArray::from(vec!["X", "Y"]),
                        ),
                    ],
                ),
                vec![0, 1]
            );
            assert_eq!(
                intern(
                    &mut group_values,
                    &[
                        make_dict(
                            int8_keys(vec![Some(1i8), Some(3i8)]),
                            StringArray::from(vec!["a", "beta", "b", "alpha"]),
                        ),
                        make_dict_large(
                            int32_keys(vec![Some(0), Some(1)]),
                            LargeStringArray::from(vec!["Y", "X"]),
                        ),
                    ],
                ),
                vec![1, 0]
            );
        }
    }

    mod streaming {
        use super::*;

        #[test]
        fn test_emit_first_renumbers_remaining_groups() {
            let schema = mk_schema(vec![
                dict(DataType::Int8, DataType::Utf8),
                dict(DataType::Int32, DataType::Utf8View),
                dict(DataType::Int32, DataType::Utf8),
            ]);
            let mut group_values = make_gv(schema);
            let b_seq = || int32_keys(vec![Some(0), Some(1), Some(2), Some(3)]);
            assert_eq!(
                intern(
                    &mut group_values,
                    &[
                        make_dict(
                            int8_keys(vec![Some(0i8), Some(1i8), Some(2i8), Some(3i8)]),
                            StringArray::from(vec!["a", "b", "c", "d"])
                        ),
                        make_dict_view(
                            b_seq(),
                            StringViewArray::from(vec!["w", "x", "y", "z"])
                        ),
                        make_dict(
                            int32_keys(vec![Some(0), Some(1), Some(0), Some(1)]),
                            StringArray::from(vec!["p", "q"])
                        ),
                    ],
                ),
                vec![0, 1, 2, 3]
            );

            let (_, decoded) = emit_decoded(&mut group_values, EmitTo::First(2));
            assert_eq!(
                decoded[0],
                vec![Some("a".to_string()), Some("b".to_string())]
            );
            assert_eq!(
                decoded[1],
                vec![Some("w".to_string()), Some("x".to_string())]
            );
            assert_eq!(
                decoded[2],
                vec![Some("p".to_string()), Some("q".to_string())]
            );

            assert_eq!(
                intern(
                    &mut group_values,
                    &[
                        make_dict(
                            int8_keys(vec![Some(3i8), Some(2i8), Some(0i8), Some(1i8)]),
                            StringArray::from(vec!["a", "e", "d", "c"])
                        ),
                        make_dict_view(
                            b_seq(),
                            StringViewArray::from(vec!["y", "z", "w", "v"])
                        ),
                        make_dict(
                            int32_keys(vec![Some(0), Some(1), Some(0), Some(2)]),
                            StringArray::from(vec!["p", "q", "r"])
                        ),
                    ],
                ),
                // ("c","y","p")→0  ("d","z","q")→1  ("a","w","p")→2 (re-interned)  ("e","v","r")→3 (new)
                vec![0, 1, 2, 3]
            );
        }

        #[test]
        fn test_emit_all_and_reingest() {
            let schema = mk_schema(vec![
                dict(DataType::Int32, DataType::Utf8),
                dict(DataType::Int32, DataType::Utf8View),
            ]);
            let mut group_values = make_gv(schema);
            assert_eq!(
                intern(
                    &mut group_values,
                    &[
                        make_dict(
                            int32_keys(vec![Some(0), Some(1)]),
                            StringArray::from(vec!["foo", "baz"]),
                        ),
                        make_dict_view(
                            int32_keys(vec![Some(0), Some(1)]),
                            StringViewArray::from(vec!["bar", "qux"]),
                        ),
                    ],
                ),
                vec![0, 1]
            );

            let (_, decoded) = emit_decoded(&mut group_values, EmitTo::All);
            assert_eq!(
                decoded[0],
                vec![Some("foo".to_string()), Some("baz".to_string())]
            );
            assert_eq!(
                decoded[1],
                vec![Some("bar".to_string()), Some("qux".to_string())]
            );

            assert_eq!(
                intern(
                    &mut group_values,
                    &[
                        make_dict(
                            int32_keys(vec![Some(0), Some(1), Some(2)]),
                            StringArray::from(vec!["baz", "foo", "new"]),
                        ),
                        make_dict_view(
                            int32_keys(vec![Some(0), Some(1), Some(2)]),
                            StringViewArray::from(vec!["qux", "bar", "val"]),
                        ),
                    ],
                ),
                vec![0, 1, 2]
            );
        }
    }
}
