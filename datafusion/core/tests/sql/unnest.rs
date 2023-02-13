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

use super::*;

#[tokio::test]
async fn unnest_single_columns() -> Result<()> {
    let ctx = create_nested_context().await?;

    let sql = "select * from shapes";
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        r#"+----------+-----------------------------------------------------------+--------------------+"#,
        r#"| shape_id | points                                                    | tags               |"#,
        r#"+----------+-----------------------------------------------------------+--------------------+"#,
        r#"| 1        | [{"x": 4, "y": -7}, {"x": 9, "y": 6}]                     | [tag1, tag2, tag3] |"#,
        r#"| 2        |                                                           | [tag1, tag2]       |"#,
        r#"| 3        | [{"x": -1, "y": 5}, {"x": -7, "y": 0}]                    | [tag1]             |"#,
        r#"| 4        | [{"x": 3, "y": -4}, {"x": -6, "y": -8}]                   | [tag1, tag2]       |"#,
        r#"| 5        | [{"x": 5, "y": -3}, {"x": -4, "y": 3}, {"x": -6, "y": 0}] |                    |"#,
        r#"+----------+-----------------------------------------------------------+--------------------+"#,
    ];
    assert_batches_eq!(expected, &results);

    let sql = "select shape_id, unnest(tags) from shapes";
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------+------+",
        "| shape_id | tags |",
        "+----------+------+",
        "| 1        | tag1 |",
        "| 1        | tag2 |",
        "| 1        | tag3 |",
        "| 2        | tag1 |",
        "| 2        | tag2 |",
        "| 3        | tag1 |",
        "| 4        | tag1 |",
        "| 4        | tag2 |",
        "| 5        |      |",
        "+----------+------+",
    ];
    assert_batches_eq!(expected, &results);

    let sql = "select shape_id, unnest(points) from shapes";

    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        r#"+----------+--------------------+"#,
        r#"| shape_id | points             |"#,
        r#"+----------+--------------------+"#,
        r#"| 1        | {"x": 4, "y": -7}  |"#,
        r#"| 1        | {"x": 9, "y": 6}   |"#,
        r#"| 2        |                    |"#,
        r#"| 3        | {"x": -1, "y": 5}  |"#,
        r#"| 3        | {"x": -7, "y": 0}  |"#,
        r#"| 4        | {"x": 3, "y": -4}  |"#,
        r#"| 4        | {"x": -6, "y": -8} |"#,
        r#"| 5        | {"x": 5, "y": -3}  |"#,
        r#"| 5        | {"x": -4, "y": 3}  |"#,
        r#"| 5        | {"x": -6, "y": 0}  |"#,
        r#"+----------+--------------------+"#,
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn unnest_multiple_columns() -> Result<()> {
    let ctx = create_nested_context().await?;

    let sql = "select shape_id, unnest(points), unnest(tags) from shapes";
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        r#"+----------+--------------------+------+"#,
        r#"| shape_id | points             | tags |"#,
        r#"+----------+--------------------+------+"#,
        r#"| 1        | {"x": 4, "y": -7}  | tag1 |"#,
        r#"| 1        | {"x": 4, "y": -7}  | tag2 |"#,
        r#"| 1        | {"x": 4, "y": -7}  | tag3 |"#,
        r#"| 1        | {"x": 9, "y": 6}   | tag1 |"#,
        r#"| 1        | {"x": 9, "y": 6}   | tag2 |"#,
        r#"| 1        | {"x": 9, "y": 6}   | tag3 |"#,
        r#"| 2        |                    | tag1 |"#,
        r#"| 2        |                    | tag2 |"#,
        r#"| 3        | {"x": -1, "y": 5}  | tag1 |"#,
        r#"| 3        | {"x": -7, "y": 0}  | tag1 |"#,
        r#"| 4        | {"x": 3, "y": -4}  | tag1 |"#,
        r#"| 4        | {"x": 3, "y": -4}  | tag2 |"#,
        r#"| 4        | {"x": -6, "y": -8} | tag1 |"#,
        r#"| 4        | {"x": -6, "y": -8} | tag2 |"#,
        r#"| 5        | {"x": 5, "y": -3}  |      |"#,
        r#"| 5        | {"x": -4, "y": 3}  |      |"#,
        r#"| 5        | {"x": -6, "y": 0}  |      |"#,
        r#"+----------+--------------------+------+"#,
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn unnest_distinct() -> Result<()> {
    let ctx = create_nested_context().await?;

    let sql = "select distinct(unnest(tags)) from shapes order by 1;";
    let results = execute_to_batches(&ctx, sql).await;

    // rustfmt makes it hard to read.
    #[rustfmt::skip]
    let expected = vec![
        "+------+",
        "| tags |",
        "+------+",
        "| tag1 |",
        "| tag2 |",
        "| tag3 |",
        "|      |",
        "+------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn unnest_group_by() -> Result<()> {
    let ctx = create_nested_context().await?;

    let sql = "select shape_id, count(unnest(tags)) as tag \
        from shapes \
        group by shape_id \
        order by 1";
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------+-----+",
        "| shape_id | tag |",
        "+----------+-----+",
        "| 1        | 3   |",
        "| 2        | 2   |",
        "| 3        | 1   |",
        "| 4        | 2   |",
        "| 5        | 0   |",
        "+----------+-----+",
    ];
    assert_batches_eq!(expected, &results);

    let sql = "select shape_id, count(unnest(points)) as point \
        from shapes \
        group by shape_id \
        order by 1";
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------+-------+",
        "| shape_id | point |",
        "+----------+-------+",
        "| 1        | 2     |",
        "| 2        | 0     |",
        "| 3        | 2     |",
        "| 4        | 2     |",
        "| 5        | 3     |",
        "+----------+-------+",
    ];
    assert_batches_eq!(expected, &results);

    let sql = "select unnest(tags) as tag, count(*) \
        from shapes \
        group by tag \
        order by 1";
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+------+-----------------+",
        "| tag  | COUNT(UInt8(1)) |",
        "+------+-----------------+",
        "| tag1 | 4               |",
        "| tag2 | 3               |",
        "| tag3 | 1               |",
        "|      | 1               |",
        "+------+-----------------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

/// Create a context that contains nested types.
///
/// Create a data frame with nested types, each row contains:
/// - shape_id an integer primary key
/// - points A list of points structs {x, y}
/// - A list of tags choosen at random from tag1 to tag10.
async fn create_nested_context() -> Result<SessionContext> {
    use rand::prelude::*;
    const NUM_ROWS: usize = 5;

    let mut shape_id_builder = UInt32Builder::new();
    let mut points_builder = ListBuilder::new(StructBuilder::from_fields(
        vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Int32, false),
        ],
        5,
    ));
    let mut tags_builder = ListBuilder::new(StringBuilder::new());

    let mut rng = StdRng::seed_from_u64(97);

    for idx in 0..NUM_ROWS {
        // Append shape id.
        shape_id_builder.append_value(idx as u32 + 1);

        // Add a random number of points
        let num_points: usize = rng.gen_range(0..4);
        if num_points > 0 {
            for _ in 0..num_points.max(2) {
                // Add x value
                points_builder
                    .values()
                    .field_builder::<Int32Builder>(0)
                    .unwrap()
                    .append_value(rng.gen_range(-10..10));
                // Add y value
                points_builder
                    .values()
                    .field_builder::<Int32Builder>(1)
                    .unwrap()
                    .append_value(rng.gen_range(-10..10));
                points_builder.values().append(true);
            }
        }

        // Append null if num points is 0.
        points_builder.append(num_points > 0);

        // Append tags.
        let num_tags: usize = rng.gen_range(0..5);
        for id in 0..num_tags {
            tags_builder.values().append_value(format!("tag{}", id + 1));
        }
        tags_builder.append(num_tags > 0);
    }

    let batch = RecordBatch::try_from_iter(vec![
        ("shape_id", Arc::new(shape_id_builder.finish()) as ArrayRef),
        ("points", Arc::new(points_builder.finish()) as ArrayRef),
        ("tags", Arc::new(tags_builder.finish()) as ArrayRef),
    ])?;

    let ctx = SessionContext::new();
    ctx.register_batch("shapes", batch)?;
    Ok(ctx)
}
