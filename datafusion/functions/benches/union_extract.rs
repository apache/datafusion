#[macro_use]
extern crate criterion;

use crate::criterion::Criterion;
use arrow::{
    array::{
        Array, BooleanArray, Int32Array, Int8Array, NullArray, StringArray, UnionArray,
    },
    datatypes::{DataType, Field, Int32Type, Int8Type, UnionFields, UnionMode},
    util::bench_util::{
        create_boolean_array, create_primitive_array, create_string_array,
    },
};
use arrow_buffer::ScalarBuffer;
use criterion::black_box;
use datafusion_common::ScalarValue;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl};
use datafusion_functions::core::union_extract::{
    eq_scalar_generic, is_sequential_generic, UnionExtractFun,
};
use itertools::repeat_n;
use rand::random;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let union_extract = UnionExtractFun::new();

    c.bench_function("union_extract case 1.1 sparse single field", |b| {
        let union = UnionArray::try_new(
            UnionFields::new(vec![1], vec![Field::new("str", DataType::Utf8, false)]), //single field
            ScalarBuffer::from(vec![1; 2048]), //non empty union
            None,                              //sparse
            vec![Arc::new(create_string_array::<i32>(2048, 0.0))], //non null target
        )
        .unwrap();

        let args = [
            ColumnarValue::Array(Arc::new(union)),
            ColumnarValue::Scalar(ScalarValue::new_utf8("str")),
        ];

        b.iter(|| {
            union_extract.invoke(&args).unwrap();
        })
    });

    c.bench_function("union_extract case 1.2 sparse empty union", |b| {
        let union = UnionArray::try_new(
            // multiple fields
            UnionFields::new(
                vec![1, 2],
                vec![
                    Field::new("str", DataType::Utf8, false),
                    Field::new("str2", DataType::Utf8, false),
                ],
            ),
            ScalarBuffer::from(vec![]), // empty union
            None,                       //sparse
            vec![
                Arc::new(StringArray::new_null(0)),
                Arc::new(StringArray::new_null(0)),
            ],
        )
        .unwrap();

        let args = [
            ColumnarValue::Array(Arc::new(union)),
            ColumnarValue::Scalar(ScalarValue::new_utf8("str")),
        ];

        b.iter(|| {
            union_extract.invoke(&args).unwrap();
        })
    });

    c.bench_function("union_extract case 1.3a sparse child null", |b| {
        let union = UnionArray::try_new(
            // multiple fields
            UnionFields::new(
                vec![1, 3],
                vec![
                    Field::new("str", DataType::Utf8, false),
                    Field::new("null", DataType::Null, true),
                ],
            ),
            ScalarBuffer::from(vec![1; 2048]), // non empty union
            None,                              //sparse
            vec![
                Arc::new(StringArray::new_null(2048)), // null target
                Arc::new(NullArray::new(2048)),
            ],
        )
        .unwrap();

        let args = [
            ColumnarValue::Array(Arc::new(union)),
            ColumnarValue::Scalar(ScalarValue::new_utf8("null")),
        ];

        b.iter(|| {
            union_extract.invoke(&args).unwrap();
        })
    });

    c.bench_function("union_extract case 1.3b sparse child null", |b| {
        let union = UnionArray::try_new(
            // multiple fields
            UnionFields::new(
                vec![1, 3],
                vec![
                    Field::new("str", DataType::Utf8, false),
                    Field::new("int", DataType::Int32, false),
                ],
            ),
            ScalarBuffer::from(vec![1; 2048]), // non empty union
            None,                              //sparse
            vec![
                Arc::new(StringArray::new_null(2048)), // null target
                Arc::new(create_primitive_array::<Int32Type>(2048, 0.0)),
            ],
        )
        .unwrap();

        let args = [
            ColumnarValue::Array(Arc::new(union)),
            ColumnarValue::Scalar(ScalarValue::new_utf8("str")),
        ];

        b.iter(|| {
            union_extract.invoke(&args).unwrap();
        })
    });

    c.bench_function("union_extract case 2 sparse all types match", |b| {
        let union = UnionArray::try_new(
            // multiple fields
            UnionFields::new(
                vec![1, 3],
                vec![
                    Field::new("str", DataType::Utf8, false),
                    Field::new("int", DataType::Int32, false),
                ],
            ),
            ScalarBuffer::from(vec![1; 2048]), //all types match & non empty union
            None,                              //sparse
            vec![
                Arc::new(create_string_array::<i32>(2048, 0.0)), //non null target
                Arc::new(Int32Array::new_null(2048)),
            ],
        )
        .unwrap();

        let args = [
            ColumnarValue::Array(Arc::new(union)),
            ColumnarValue::Scalar(ScalarValue::new_utf8("str")),
        ];

        b.iter(|| {
            union_extract.invoke(&args).unwrap();
        })
    });

    c.bench_function(
        "union_extract case 3.1 none selected target can contain null mask",
        |b| {
            let union = UnionArray::try_new(
                UnionFields::new(
                    vec![1, 3],
                    vec![
                        Field::new("str", DataType::Utf8, false),
                        Field::new("int", DataType::Int32, false),
                    ],
                ),
                ScalarBuffer::from(vec![3; 2048]), //none selected
                None,
                vec![
                    Arc::new(create_string_array::<i32>(2048, 0.5)),
                    Arc::new(Int32Array::new_null(2048)),
                ],
            )
            .unwrap();

            let args = [
                ColumnarValue::Array(Arc::new(union)),
                ColumnarValue::Scalar(ScalarValue::new_utf8("str")),
            ];

            b.iter(|| {
                union_extract.invoke(&args).unwrap();
            })
        },
    );

    c.bench_function(
        "union_extract case 3.2 none matches sparse cant contain null mask",
        |b| {
            let target_fields =
                UnionFields::new([10], [Field::new("bool", DataType::Boolean, true)]);

            let union = UnionArray::try_new(
                // multiple fields
                UnionFields::new(
                    vec![1, 3],
                    vec![
                        Field::new("str", DataType::Utf8, true),
                        Field::new(
                            "union",
                            DataType::Union(target_fields.clone(), UnionMode::Sparse),
                            false,
                        ),
                    ],
                ),
                ScalarBuffer::from_iter(repeat_n(1, 2048)), //none matches
                None,                                       //sparse
                vec![
                    Arc::new(create_string_array::<i32>(2048, 0.5)),
                    Arc::new(
                        UnionArray::try_new(
                            target_fields,
                            ScalarBuffer::from(vec![10; 2048]),
                            None,
                            vec![Arc::new(BooleanArray::from(vec![true; 2048]))],
                        )
                        .unwrap(),
                    ),
                ],
            )
            .unwrap();

            let args = [
                ColumnarValue::Array(Arc::new(union)),
                ColumnarValue::Scalar(ScalarValue::new_utf8("union")),
            ];

            b.iter(|| {
                union_extract.invoke(&args).unwrap();
            })
        },
    );

    c.bench_function(
        "union_extract case 4.1.1 sparse some matches target with nulls",
        |b| {
            let union = UnionArray::try_new(
                //multiple fields
                UnionFields::new(
                    vec![1, 3],
                    vec![
                        Field::new("str", DataType::Utf8, false),
                        Field::new("int", DataType::Int32, false),
                    ],
                ),
                ScalarBuffer::from_iter(repeat_n(1, 2047).chain([3])), //multiple types
                None,                                                  //sparse
                vec![
                    Arc::new(create_string_array::<i32>(2048, 0.5)), //target with some nulls, but not all
                    Arc::new(Int32Array::new_null(2048)),
                ],
            )
            .unwrap();

            let args = [
                ColumnarValue::Array(Arc::new(union)),
                ColumnarValue::Scalar(ScalarValue::new_utf8("str")),
            ];

            b.iter(|| {
                union_extract.invoke(&args).unwrap();
            })
        },
    );

    c.bench_function(
        "union_extract case 4.1.2 sparse some matches target without nulls",
        |b| {
            let union = UnionArray::try_new(
                //multiple fields
                UnionFields::new(
                    vec![1, 3],
                    vec![
                        Field::new("str", DataType::Utf8, false),
                        Field::new("int", DataType::Int32, false),
                    ],
                ),
                ScalarBuffer::from_iter(repeat_n(1, 2047).chain([3])), //multiple types
                None,                                                  //sparse
                vec![
                    Arc::new(create_string_array::<i32>(2048, 0.0)), //target without nulls
                    Arc::new(Int32Array::new_null(2048)),
                ],
            )
            .unwrap();

            let args = [
                ColumnarValue::Array(Arc::new(union)),
                ColumnarValue::Scalar(ScalarValue::new_utf8("str")),
            ];

            b.iter(|| {
                union_extract.invoke(&args).unwrap();
            })
        },
    );

    c.bench_function(
        "union_extract case 4.2 some matches sparse cant contain null mask",
        |b| {
            let target_fields =
                UnionFields::new([10], [Field::new("bool", DataType::Boolean, true)]);

            let union = UnionArray::try_new(
                // multiple fields
                UnionFields::new(
                    vec![1, 3],
                    vec![
                        Field::new("str", DataType::Utf8, false),
                        Field::new(
                            "union",
                            DataType::Union(target_fields.clone(), UnionMode::Sparse),
                            false,
                        ),
                    ],
                ),
                ScalarBuffer::from_iter(repeat_n([1, 3], 1024).flatten()), //some matches
                None,                                                      //sparse
                vec![
                    Arc::new(NullArray::new(2048)), //null target
                    Arc::new(
                        UnionArray::try_new(
                            target_fields,
                            ScalarBuffer::from(vec![10; 2048]),
                            None,
                            vec![Arc::new(BooleanArray::from(vec![true; 2048]))],
                        )
                        .unwrap(),
                    ),
                ],
            )
            .unwrap();

            let args = [
                ColumnarValue::Array(Arc::new(union)),
                ColumnarValue::Scalar(ScalarValue::new_utf8("union")),
            ];

            b.iter(|| {
                union_extract.invoke(&args).unwrap();
            })
        },
    );

    c.bench_function(
        "union_extract case 1.1 dense empty union empty target",
        |b| {
            let union = UnionArray::try_new(
                UnionFields::new(
                    vec![1, 3],
                    vec![
                        Field::new("str", DataType::Utf8, false),
                        Field::new("int", DataType::Int32, false),
                    ],
                ),
                ScalarBuffer::from(vec![]),       //empty union
                Some(ScalarBuffer::from(vec![])), //dense
                vec![
                    Arc::new(StringArray::new_null(0)), //empty target
                    Arc::new(Int32Array::new_null(0)),
                ],
            )
            .unwrap();

            let args = [
                ColumnarValue::Array(Arc::new(union)),
                ColumnarValue::Scalar(ScalarValue::new_utf8("str")),
            ];

            b.iter(|| {
                union_extract.invoke(&args).unwrap();
            })
        },
    );

    c.bench_function(
        "union_extract case 1.2 dense empty union non-empty target",
        |b| {
            let union = UnionArray::try_new(
                UnionFields::new(
                    vec![1, 3],
                    vec![
                        Field::new("str", DataType::Utf8, false),
                        Field::new("int", DataType::Int32, false),
                    ],
                ),
                ScalarBuffer::from(vec![]),       // empty union
                Some(ScalarBuffer::from(vec![])), // dense
                vec![
                    Arc::new(StringArray::from(vec!["a1", "s2"])), // non empty target
                    Arc::new(Int32Array::new_null(0)),
                ],
            )
            .unwrap();

            let args = [
                ColumnarValue::Array(Arc::new(union)),
                ColumnarValue::Scalar(ScalarValue::new_utf8("str")),
            ];

            b.iter(|| {
                union_extract.invoke(&args).unwrap();
            })
        },
    );

    c.bench_function(
        "union_extract case 2 dense non empty union, empty target",
        |b| {
            let union = UnionArray::try_new(
                UnionFields::new(
                    vec![1, 3],
                    vec![
                        Field::new("str", DataType::Utf8, false),
                        Field::new("int", DataType::Int32, false),
                    ],
                ),
                ScalarBuffer::from(vec![3, 3]), // non empty union
                Some(ScalarBuffer::from(vec![0, 1])), // dense
                vec![
                    Arc::new(StringArray::new_null(0)), // empty target
                    Arc::new(Int32Array::new_null(2)),
                ],
            )
            .unwrap();

            let args = [
                ColumnarValue::Array(Arc::new(union)),
                ColumnarValue::Scalar(ScalarValue::new_utf8("str")),
            ];

            b.iter(|| {
                union_extract.invoke(&args).unwrap();
            })
        },
    );

    c.bench_function(
        "union_extract case 3.1 dense null target len smaller",
        |b| {
            let union = UnionArray::try_new(
                UnionFields::new(
                    vec![1, 3],
                    vec![
                        Field::new("str", DataType::Utf8, false),
                        Field::new("int", DataType::Int32, false),
                    ],
                ),
                ScalarBuffer::from(vec![1; 2048]),
                Some(ScalarBuffer::from(vec![0; 2048])), // dense
                vec![
                    Arc::new(StringArray::new_null(1)), // null & len smaller
                    Arc::new(Int32Array::new_null(64)),
                ],
            )
            .unwrap();

            let args = [
                ColumnarValue::Array(Arc::new(union)),
                ColumnarValue::Scalar(ScalarValue::new_utf8("str")),
            ];

            b.iter(|| {
                union_extract.invoke(&args).unwrap();
            })
        },
    );

    c.bench_function("union_extract case 3.2 dense null target len equal", |b| {
        let union = UnionArray::try_new(
            UnionFields::new(
                vec![1, 3],
                vec![
                    Field::new("str", DataType::Utf8, false),
                    Field::new("int", DataType::Int32, false),
                ],
            ),
            ScalarBuffer::from(vec![1; 2048]),
            Some(ScalarBuffer::from_iter(0..2048)), // dense
            vec![
                Arc::new(StringArray::new_null(2048)), // null & same len as parent
                Arc::new(Int32Array::new_null(64)),
            ],
        )
        .unwrap();

        let args = [
            ColumnarValue::Array(Arc::new(union)),
            ColumnarValue::Scalar(ScalarValue::new_utf8("str")),
        ];

        b.iter(|| {
            union_extract.invoke(&args).unwrap();
        })
    });

    c.bench_function("union_extract case 3.3 dense null target len bigger", |b| {
        let union = UnionArray::try_new(
            UnionFields::new(
                vec![1, 3],
                vec![
                    Field::new("str", DataType::Utf8, false),
                    Field::new("int", DataType::Int32, false),
                ],
            ),
            ScalarBuffer::from(vec![1; 2048]),
            Some(ScalarBuffer::from(vec![0; 2048])),
            vec![
                Arc::new(StringArray::new_null(4096)), // null, bigger than parent
                Arc::new(Int32Array::new_null(64)),
            ],
        )
        .unwrap();

        let args = [
            ColumnarValue::Array(Arc::new(union)),
            ColumnarValue::Scalar(ScalarValue::new_utf8("str")),
        ];

        b.iter(|| {
            union_extract.invoke(&args).unwrap();
        })
    });

    c.bench_function(
        "union_extract case 4.1A dense single field sequential offsets equal lens",
        |b| {
            let union = UnionArray::try_new(
                //single field
                UnionFields::new(vec![3], vec![Field::new("int", DataType::Int8, false)]),
                ScalarBuffer::from(vec![3; 2048]),
                Some(ScalarBuffer::from_iter(0..2048)), //sequential offsets
                vec![Arc::new(create_primitive_array::<Int8Type>(2048, 0.0))], //same len as parent, not null
            )
            .unwrap();

            let args = [
                ColumnarValue::Array(Arc::new(union)),
                ColumnarValue::Scalar(ScalarValue::new_utf8("int")),
            ];

            b.iter(|| {
                union_extract.invoke(&args).unwrap();
            })
        },
    );

    c.bench_function(
        "union_extract case 4.2A dense single field sequential offsets bigger len",
        |b| {
            let union = UnionArray::try_new(
                // single field
                UnionFields::new(vec![3], vec![Field::new("int", DataType::Int8, false)]),
                ScalarBuffer::from(vec![3; 2048]),
                Some(ScalarBuffer::from_iter(0..2048)), //sequential offsets
                vec![Arc::new(create_primitive_array::<Int8Type>(4096, 0.0))], //bigger than parent, not null
            )
            .unwrap();

            let args = [
                ColumnarValue::Array(Arc::new(union)),
                ColumnarValue::Scalar(ScalarValue::new_utf8("int")),
            ];

            b.iter(|| {
                union_extract.invoke(&args).unwrap();
            })
        },
    );

    c.bench_function(
        "union_extract case 4.3A dense single field non-sequential offsets",
        |b| {
            let union = UnionArray::try_new(
                // single field
                UnionFields::new(vec![3], vec![Field::new("int", DataType::Int8, false)]),
                ScalarBuffer::from(vec![3; 2048]),
                Some(ScalarBuffer::from_iter((0..2046).chain([2047, 2047]))), // non sequential offsets, avoid fast paths
                vec![Arc::new(create_primitive_array::<Int8Type>(2048, 0.0))], // not null
            )
            .unwrap();

            let args = [
                ColumnarValue::Array(Arc::new(union)),
                ColumnarValue::Scalar(ScalarValue::new_utf8("int")),
            ];

            b.iter(|| {
                union_extract.invoke(&args).unwrap();
            })
        },
    );

    c.bench_function(
        "union_extract case 4.1B dense empty siblings sequential offsets equal len",
        |b| {
            let union = UnionArray::try_new(
                // multiple fields
                UnionFields::new(
                    vec![1, 3],
                    vec![
                        Field::new("str", DataType::Utf8, false),
                        Field::new("int", DataType::Int8, false),
                    ],
                ),
                ScalarBuffer::from(vec![3; 2048]), // all types must match
                Some(ScalarBuffer::from_iter(0..2048)), // sequential offsets
                vec![
                    Arc::new(StringArray::new_null(0)), // empty sibling
                    Arc::new(create_primitive_array::<Int8Type>(2048, 0.0)), // same len as parent, not null
                ],
            )
            .unwrap();

            let args = [
                ColumnarValue::Array(Arc::new(union)),
                ColumnarValue::Scalar(ScalarValue::new_utf8("int")),
            ];

            b.iter(|| {
                union_extract.invoke(&args).unwrap();
            })
        },
    );

    c.bench_function(
        "union_extract case 4.2B dense empty siblings sequential offsets bigger target",
        |b| {
            let union = UnionArray::try_new(
                // multiple fields
                UnionFields::new(
                    vec![1, 3],
                    vec![
                        Field::new("str", DataType::Utf8, false),
                        Field::new("int", DataType::Int8, false),
                    ],
                ),
                ScalarBuffer::from(vec![3; 2048]), // all types match
                Some(ScalarBuffer::from_iter(0..2048)), // sequential offsets
                vec![
                    Arc::new(StringArray::new_null(0)), // empty sibling
                    Arc::new(create_primitive_array::<Int8Type>(4096, 0.0)), // target is bigger than parent, not null
                ],
            )
            .unwrap();

            let args = [
                ColumnarValue::Array(Arc::new(union)),
                ColumnarValue::Scalar(ScalarValue::new_utf8("int")),
            ];

            b.iter(|| {
                union_extract.invoke(&args).unwrap();
            })
        },
    );

    c.bench_function(
        "union_extract case 4.3B dense empty sibling non-sequential offsets",
        |b| {
            let union = UnionArray::try_new(
                // multiple fields
                UnionFields::new(
                    vec![1, 3],
                    vec![
                        Field::new("str", DataType::Utf8, false),
                        Field::new("int", DataType::Int32, false),
                    ],
                ),
                ScalarBuffer::from(vec![3; 2048]), // all types must match
                Some(ScalarBuffer::from_iter((0..2046).chain([2047, 2047]))), // non sequential offsets, avois fast paths
                vec![
                    Arc::new(StringArray::new_null(0)), // empty sibling
                    Arc::new(create_primitive_array::<Int8Type>(2048, 0.0)), // not null
                ],
            )
            .unwrap();

            let args = [
                ColumnarValue::Array(Arc::new(union)),
                ColumnarValue::Scalar(ScalarValue::new_utf8("int")),
            ];

            b.iter(|| {
                union_extract.invoke(&args).unwrap();
            })
        },
    );

    c.bench_function(
        "union_extract case 4.1C dense all types match sequential offsets equal lens",
        |b| {
            let union = UnionArray::try_new(
                // multiple fields
                UnionFields::new(
                    vec![1, 3],
                    vec![
                        Field::new("str", DataType::Utf8, false),
                        Field::new("int", DataType::Int8, false),
                    ],
                ),
                ScalarBuffer::from(vec![3; 2048]), // all types match
                Some(ScalarBuffer::from_iter(0..2048)), // sequential offsets
                vec![
                    Arc::new(StringArray::new_null(1)), // non empty sibling
                    Arc::new(create_primitive_array::<Int8Type>(2048, 0.0)), // same len as parent, not null
                ],
            )
            .unwrap();

            let args = [
                ColumnarValue::Array(Arc::new(union)),
                ColumnarValue::Scalar(ScalarValue::new_utf8("int")),
            ];

            b.iter(|| {
                union_extract.invoke(&args).unwrap();
            })
        },
    );

    c.bench_function(
        "union_extract case 4.2C dense all types match sequential offsets bigger len",
        |b| {
            let union = UnionArray::try_new(
                // multiple fields
                UnionFields::new(
                    vec![1, 3],
                    vec![
                        Field::new("str", DataType::Utf8, false),
                        Field::new("int", DataType::Int8, false),
                    ],
                ),
                ScalarBuffer::from(vec![3; 2048]), // all types match
                Some(ScalarBuffer::from_iter(0..2048)), // sequential offsets
                vec![
                    Arc::new(StringArray::new_null(1)), // non empty sibling
                    Arc::new(create_primitive_array::<Int8Type>(4096, 0.0)), // bigger than parent union, not null
                ],
            )
            .unwrap();

            let args = [
                ColumnarValue::Array(Arc::new(union)),
                ColumnarValue::Scalar(ScalarValue::new_utf8("int")),
            ];

            b.iter(|| {
                union_extract.invoke(&args).unwrap();
            })
        },
    );

    c.bench_function(
        "union_extract case 4.3C dense all types match non-sequential offsets",
        |b| {
            let union = UnionArray::try_new(
                // multiple fields
                UnionFields::new(
                    vec![1, 3],
                    vec![
                        Field::new("str", DataType::Utf8, false),
                        Field::new("int", DataType::Int8, false),
                    ],
                ),
                ScalarBuffer::from(vec![3; 2048]), // all types match
                Some(ScalarBuffer::from_iter((0..2046).chain([2047, 2047]))), //non sequential, avoid fast paths
                vec![
                    Arc::new(StringArray::new_null(1)), // non empty sibling
                    Arc::new(create_primitive_array::<Int8Type>(2048, 0.0)), // not null
                ],
            )
            .unwrap();

            let args = [
                ColumnarValue::Array(Arc::new(union)),
                ColumnarValue::Scalar(ScalarValue::new_utf8("int")),
            ];

            b.iter(|| {
                union_extract.invoke(&args).unwrap();
            })
        },
    );

    c.bench_function("union_extract case 5.1a dense none match less len", |b| {
        let union = UnionArray::try_new(
            // multiple fields
            UnionFields::new(
                vec![1, 3],
                vec![
                    Field::new("str", DataType::Utf8, false),
                    Field::new("int", DataType::Int32, false),
                ],
            ),
            ScalarBuffer::from(vec![1; 2048]), //none match
            Some(ScalarBuffer::from_iter(0..2048)), //dense
            vec![
                Arc::new(create_string_array::<i32>(2048, 0.0)), // non empty
                Arc::new(create_primitive_array::<Int32Type>(1024, 0.0)), //less len, not null
            ],
        )
        .unwrap();

        let args = [
            ColumnarValue::Array(Arc::new(union)),
            ColumnarValue::Scalar(ScalarValue::new_utf8("int")),
        ];

        b.iter(|| {
            union_extract.invoke(&args).unwrap();
        })
    });

    c.bench_function(
        "union_extract case 5.1b dense none match cant contain null mask",
        |b| {
            let union_target = UnionArray::try_new(
                UnionFields::new([1], vec![Field::new("a", DataType::Boolean, true)]),
                vec![1; 2048].into(),
                None,
                vec![Arc::new(create_boolean_array(2048, 0.0, 0.0))],
            )
            .unwrap();

            let parent_union = UnionArray::try_new(
                // multiple fields
                UnionFields::new(
                    vec![1, 3],
                    vec![
                        Field::new("str", DataType::Utf8, false),
                        Field::new("union", union_target.data_type().clone(), false),
                    ],
                ),
                ScalarBuffer::from(vec![1; 2048]), //none match
                Some(ScalarBuffer::from_iter(0..2048)), //dense
                vec![
                    Arc::new(create_string_array::<i32>(2048, 0.0)), // non empty
                    Arc::new(union_target),
                ],
            )
            .unwrap();

            let args = [
                ColumnarValue::Array(Arc::new(parent_union)),
                ColumnarValue::Scalar(ScalarValue::new_utf8("union")),
            ];

            b.iter(|| {
                union_extract.invoke(&args).unwrap();
            })
        },
    );

    c.bench_function("union_extract case 5.2 dense none match equal len", |b| {
        let union = UnionArray::try_new(
            // multiple fields
            UnionFields::new(
                vec![1, 3],
                vec![
                    Field::new("str", DataType::Utf8, false),
                    Field::new("int", DataType::Int32, false),
                ],
            ),
            ScalarBuffer::from(vec![3; 2048]), //none match
            Some(ScalarBuffer::from_iter(0..2048)), //dense
            vec![
                Arc::new(create_string_array::<i32>(2048, 0.0)), // non empty
                Arc::new(create_primitive_array::<Int32Type>(2048, 0.0)), //equal len, not null
            ],
        )
        .unwrap();

        let args = [
            ColumnarValue::Array(Arc::new(union)),
            ColumnarValue::Scalar(ScalarValue::new_utf8("int")),
        ];

        b.iter(|| {
            union_extract.invoke(&args).unwrap();
        })
    });

    c.bench_function("union_extract case 5.3 dense none match greater len", |b| {
        let union = UnionArray::try_new(
            // multiple fields
            UnionFields::new(
                vec![1, 3],
                vec![
                    Field::new("str", DataType::Utf8, false),
                    Field::new("int", DataType::Int32, false),
                ],
            ),
            ScalarBuffer::from(vec![3; 2048]), //none match
            Some(ScalarBuffer::from_iter(0..2048)), //dense
            vec![
                Arc::new(create_string_array::<i32>(2048, 0.0)), // non empty
                Arc::new(create_primitive_array::<Int32Type>(2049, 0.0)), //greater len, not null
            ],
        )
        .unwrap();

        let args = [
            ColumnarValue::Array(Arc::new(union)),
            ColumnarValue::Scalar(ScalarValue::new_utf8("int")),
        ];

        b.iter(|| {
            union_extract.invoke(&args).unwrap();
        })
    });

    c.bench_function("union_extract case 6 some match", |b| {
        let union = UnionArray::try_new(
            // multiple fields
            UnionFields::new(
                vec![1, 3],
                vec![
                    Field::new("str", DataType::Utf8, false),
                    Field::new("int", DataType::Int32, false),
                ],
            ),
            ScalarBuffer::from_iter(repeat_n([1, 3], 1024).flatten()), //some matches but not all
            Some(ScalarBuffer::from_iter(
                std::iter::zip(1024..2048, 0..1024).flat_map(|(a, b)| [a, b]),
            )),
            vec![
                Arc::new(create_string_array::<i32>(2048, 0.0)), // sibling is not empty
                Arc::new(create_primitive_array::<Int32Type>(1024, 0.0)), //not null
            ],
        )
        .unwrap();

        let args = [
            ColumnarValue::Array(Arc::new(union.clone())),
            ColumnarValue::Scalar(ScalarValue::new_utf8("int")),
        ];

        b.iter(|| {
            union_extract.invoke(&args).unwrap();
        })
    });

    {
        let mut is_sequential_group = c.benchmark_group("offsets");

        let start = random::<u16>() as i32;
        let offsets = (start..start + 4096).collect::<Vec<i32>>();

        //compare performance to simpler alternatives

        is_sequential_group.bench_function("offsets sequential windows all", |b| {
            b.iter(|| {
                black_box(offsets.windows(2).all(|window| window[0] + 1 == window[1]));
            })
        });

        is_sequential_group.bench_function("offsets sequential windows fold &&", |b| {
            b.iter(|| {
                black_box(
                    offsets
                        .windows(2)
                        .fold(true, |b, w| b && (w[0] + 1 == w[1])),
                )
            })
        });

        is_sequential_group.bench_function("offsets sequential windows fold &", |b| {
            b.iter(|| {
                black_box(offsets.windows(2).fold(true, |b, w| b & (w[0] + 1 == w[1])))
            })
        });

        is_sequential_group.bench_function("offsets sequential all", |b| {
            b.iter(|| {
                black_box(
                    offsets
                        .iter()
                        .copied()
                        .enumerate()
                        .all(|(i, v)| v == offsets[0] + i as i32),
                )
            })
        });

        is_sequential_group.bench_function("offsets sequential fold &&", |b| {
            b.iter(|| {
                black_box(
                    offsets
                        .iter()
                        .copied()
                        .enumerate()
                        .fold(true, |b, (i, v)| b && (v == offsets[0] + i as i32)),
                )
            })
        });

        is_sequential_group.bench_function("offsets sequential fold &", |b| {
            b.iter(|| {
                black_box(
                    offsets
                        .iter()
                        .copied()
                        .enumerate()
                        .fold(true, |b, (i, v)| b & (v == offsets[0] + i as i32)),
                )
            })
        });

        macro_rules! bench_sequential {
            ($n:literal) => {
                is_sequential_group
                    .bench_function(&format!("offsets sequential chunk {}", $n), |b| {
                        b.iter(|| black_box(is_sequential_generic::<$n>(&offsets)))
                    });
            };
        }

        bench_sequential!(8);
        bench_sequential!(16);
        bench_sequential!(32);
        bench_sequential!(64);
        bench_sequential!(128);
        bench_sequential!(256);
        bench_sequential!(512);
        bench_sequential!(1024);
        bench_sequential!(2048);
        bench_sequential!(4096);

        is_sequential_group.finish();
    }

    {
        let mut type_ids_eq = c.benchmark_group("type_ids_eq");

        let type_id = random::<i8>();
        let type_ids = vec![type_id; 65536];

        //compare performance to simpler alternatives

        type_ids_eq.bench_function("type_ids equal all", |b| {
            b.iter(|| {
                type_ids
                    .iter()
                    .copied()
                    .all(|value_type_id| value_type_id == type_id)
            })
        });

        type_ids_eq.bench_function("type_ids equal fold &&", |b| {
            b.iter(|| type_ids.iter().fold(true, |b, v| b && (*v == type_id)))
        });

        type_ids_eq.bench_function("type_ids equal fold &", |b| {
            b.iter(|| type_ids.iter().fold(true, |b, v| b & (*v == type_id)))
        });

        type_ids_eq.bench_function("type_ids equal compute::eq", |b| {
            let type_ids_array = Int8Array::new(type_ids.clone().into(), None);

            b.iter(|| {
                let eq = arrow::compute::kernels::cmp::eq(
                    &type_ids_array,
                    &Int8Array::new_scalar(black_box(type_id)),
                )
                .unwrap();

                eq.true_count() == type_ids.len()
            })
        });

        macro_rules! bench_type_ids_eq {
            ($n:literal) => {
                type_ids_eq.bench_function(&format!("type_ids equal true {}", $n), |b| {
                    b.iter(|| eq_scalar_generic::<$n>(&type_ids, type_ids[0]))
                });

                type_ids_eq
                    .bench_function(&format!("type_ids equal false {}", $n), |b| {
                        b.iter(|| eq_scalar_generic::<$n>(&type_ids, type_ids[0] + 1))
                    });

                type_ids_eq.bench_function(&format!("type_ids worst case {}", $n), |b| {
                    let mut type_ids = type_ids.clone();

                    type_ids[65535] = !type_ids[65535];

                    b.iter(|| eq_scalar_generic::<$n>(&type_ids, type_ids[0]))
                });

                type_ids_eq.bench_function(&format!("type_ids best case {}", $n), |b| {
                    let mut type_ids = type_ids.clone();

                    type_ids[$n - 1] += 1;

                    b.iter(|| eq_scalar_generic::<$n>(&type_ids, type_ids[0]))
                });
            };
        }

        bench_type_ids_eq!(16);
        bench_type_ids_eq!(32);
        bench_type_ids_eq!(64);
        bench_type_ids_eq!(128);
        bench_type_ids_eq!(256);
        bench_type_ids_eq!(512);
        bench_type_ids_eq!(1024);
        bench_type_ids_eq!(2048);
        bench_type_ids_eq!(4096);
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
