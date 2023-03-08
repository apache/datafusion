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
use datafusion::execution::options::ReadOptions;

#[tokio::test]
async fn window_frame_creation_type_checking() -> Result<()> {
    // The following query has type error. We should test the error could be detected
    // from either the logical plan (when `skip_failed_rules` is set to `false`) or
    // the physical plan (when `skip_failed_rules` is set to `true`).

    // We should remove the type checking in physical plan after we don't skip
    // the failed optimizing rules by default. (see more in https://github.com/apache/arrow-datafusion/issues/4615)
    async fn check_query(skip_failed_rules: bool, err_msg: &str) -> Result<()> {
        use datafusion_common::ScalarValue::Boolean;
        let config = SessionConfig::new().set(
            "datafusion.optimizer.skip_failed_rules",
            Boolean(Some(skip_failed_rules)),
        );
        let ctx = SessionContext::with_config(config);
        register_aggregate_csv(&ctx).await?;
        let df = ctx
            .sql(
                "SELECT
                    COUNT(c1) OVER (ORDER BY c2 RANGE BETWEEN '1 DAY' PRECEDING AND '2 DAY' FOLLOWING)
                    FROM aggregate_test_100;",
            )
            .await?;
        let results = df.collect().await;
        assert_contains!(results.err().unwrap().to_string(), err_msg);
        Ok(())
    }

    // Error is returned from the physical plan.
    check_query(
        true,
        "Internal error: Operator - is not implemented for types UInt32(1) and Utf8(\"1 DAY\")."
    ).await?;

    // Error is returned from the logical plan.
    check_query(
        false,
        "Internal error: Optimizer rule 'type_coercion' failed due to unexpected error: Execution error: Cannot cast Utf8(\"1 DAY\") to UInt32."
    ).await
}

// Return a static RecordBatch and its ordering for tests. RecordBatch is ordered by ts
fn get_test_data1() -> Result<(RecordBatch, Vec<Expr>)> {
    let ts_field = Field::new("ts", DataType::Int32, false);
    let inc_field = Field::new("inc_col", DataType::Int32, false);
    let desc_field = Field::new("desc_col", DataType::Int32, false);
    let low_card_field = Field::new("low_card_col", DataType::Int32, false);

    let schema = Arc::new(Schema::new(vec![
        ts_field,
        inc_field,
        desc_field,
        low_card_field,
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from_slice([
                1, 1, 5, 9, 10, 11, 16, 21, 22, 26, 26, 28, 31, 33, 38, 42, 47, 51, 53,
                53, 58, 63, 67, 68, 70, 72, 72, 76, 81, 85, 86, 88, 91, 96, 97, 98, 100,
                101, 102, 104, 104, 108, 112, 113, 113, 114, 114, 117, 122, 126, 131,
                131, 136, 136, 136, 139, 141, 146, 147, 147, 152, 154, 159, 161, 163,
                164, 167, 172, 173, 177, 180, 185, 186, 191, 195, 195, 199, 203, 207,
                210, 213, 218, 221, 224, 226, 230, 232, 235, 238, 238, 239, 244, 245,
                247, 250, 254, 258, 262, 264, 264,
            ])),
            Arc::new(Int32Array::from_slice([
                1, 5, 10, 15, 20, 21, 26, 29, 30, 33, 37, 40, 43, 44, 45, 49, 51, 53, 58,
                61, 65, 70, 75, 78, 83, 88, 90, 91, 95, 97, 100, 105, 109, 111, 115, 119,
                120, 124, 126, 129, 131, 135, 140, 143, 144, 147, 148, 149, 151, 155,
                156, 159, 160, 163, 165, 170, 172, 177, 181, 182, 186, 187, 192, 196,
                197, 199, 203, 207, 209, 213, 214, 216, 219, 221, 222, 225, 226, 231,
                236, 237, 242, 245, 247, 248, 253, 254, 259, 261, 266, 269, 272, 275,
                278, 283, 286, 289, 291, 296, 301, 305,
            ])),
            Arc::new(Int32Array::from_slice([
                100, 98, 93, 91, 86, 84, 81, 77, 75, 71, 70, 69, 64, 62, 59, 55, 50, 45,
                41, 40, 39, 36, 31, 28, 23, 22, 17, 13, 10, 6, 5, 2, 1, -1, -4, -5, -6,
                -8, -12, -16, -17, -19, -24, -25, -29, -34, -37, -42, -47, -48, -49, -53,
                -57, -58, -61, -65, -67, -68, -71, -73, -75, -76, -78, -83, -87, -91,
                -95, -98, -101, -105, -106, -111, -114, -116, -120, -125, -128, -129,
                -134, -139, -142, -143, -146, -150, -154, -158, -163, -168, -172, -176,
                -181, -184, -189, -193, -196, -201, -203, -208, -210, -213,
            ])),
            Arc::new(Int32Array::from_slice([
                0, 3, 4, 3, 2, 2, 0, 1, 1, 3, 4, 1, 0, 1, 3, 2, 1, 4, 4, 1, 4, 3, 4, 4,
                0, 4, 1, 2, 4, 3, 2, 1, 4, 2, 2, 4, 2, 4, 1, 0, 1, 1, 4, 4, 2, 1, 4, 1,
                4, 1, 1, 3, 4, 4, 3, 0, 4, 0, 3, 0, 4, 4, 0, 0, 4, 3, 1, 0, 3, 3, 1, 1,
                0, 4, 3, 3, 0, 0, 4, 2, 1, 3, 2, 2, 1, 3, 1, 4, 1, 3, 3, 1, 2, 0, 1, 0,
                3, 2, 1, 2,
            ])),
        ],
    )?;
    let file_sort_order = vec![col("ts").sort(true, false)];
    Ok((batch, file_sort_order))
}

// Return a static RecordBatch and its ordering for tests. RecordBatch is ordered by low_card_col1, low_card_col2, inc_col
fn get_test_data2() -> Result<(RecordBatch, Vec<Expr>)> {
    let low_card_col1 = Field::new("low_card_col1", DataType::Int32, false);
    let low_card_col2 = Field::new("low_card_col2", DataType::Int32, false);
    let inc_col = Field::new("inc_col", DataType::Int32, false);
    let unsorted_col = Field::new("unsorted_col", DataType::Int32, false);

    let schema = Arc::new(Schema::new(vec![
        low_card_col1,
        low_card_col2,
        inc_col,
        unsorted_col,
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from_slice([
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1,
            ])),
            Arc::new(Int32Array::from_slice([
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
                2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
                3, 3, 3, 3,
            ])),
            Arc::new(Int32Array::from_slice([
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
                39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56,
                57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74,
                75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92,
                93, 94, 95, 96, 97, 98, 99,
            ])),
            Arc::new(Int32Array::from_slice([
                0, 2, 0, 0, 1, 1, 0, 2, 1, 4, 4, 2, 2, 1, 2, 3, 3, 2, 1, 4, 0, 3, 0, 0,
                4, 0, 2, 0, 1, 1, 3, 4, 2, 2, 4, 0, 1, 4, 0, 1, 1, 3, 3, 2, 3, 0, 0, 1,
                1, 3, 0, 3, 1, 1, 4, 2, 1, 1, 1, 2, 4, 3, 1, 4, 4, 0, 2, 4, 1, 1, 0, 2,
                1, 1, 4, 2, 0, 2, 1, 4, 2, 0, 4, 2, 1, 1, 1, 4, 3, 4, 1, 2, 0, 0, 2, 0,
                4, 2, 4, 3,
            ])),
        ],
    )?;
    let file_sort_order = vec![
        col("low_card_col1").sort(true, false),
        col("low_card_col2").sort(true, false),
        col("inc_col").sort(true, false),
    ];
    Ok((batch, file_sort_order))
}

fn write_test_data_to_csv(
    tmpdir: &TempDir,
    n_file: usize,
    batch: &RecordBatch,
) -> Result<()> {
    let n_chunk = batch.num_rows() / n_file;
    for i in 0..n_file {
        let target_file = tmpdir.path().join(format!("{i}.csv"));
        let file = File::create(target_file).unwrap();
        let chunks_start = i * n_chunk;
        let cur_batch = batch.slice(chunks_start, n_chunk);
        let mut writer = arrow::csv::Writer::new(file);
        writer.write(&cur_batch)?;
    }
    Ok(())
}

async fn get_test_context(tmpdir: &TempDir) -> Result<SessionContext> {
    let session_config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::with_config(session_config);

    let csv_read_options = CsvReadOptions::default();

    let (batch, file_sort_order) = get_test_data1()?;

    let options_sort = csv_read_options
        .to_listing_options(&ctx.copied_config())
        .with_file_sort_order(Some(file_sort_order));

    write_test_data_to_csv(tmpdir, 1, &batch)?;
    let sql_definition = None;
    ctx.register_listing_table(
        "annotated_data",
        tmpdir.path().to_string_lossy(),
        options_sort.clone(),
        Some(batch.schema()),
        sql_definition,
    )
    .await
    .unwrap();
    Ok(ctx)
}

async fn get_test_context2(tmpdir: &TempDir) -> Result<SessionContext> {
    let session_config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::with_config(session_config);

    let csv_read_options = CsvReadOptions::default();
    let (batch, file_sort_order) = get_test_data2()?;

    let options_sort = csv_read_options
        .to_listing_options(&ctx.copied_config())
        .with_file_sort_order(Some(file_sort_order))
        .with_infinite_source(true);

    write_test_data_to_csv(tmpdir, 1, &batch)?;
    let sql_definition = None;
    ctx.register_listing_table(
        "annotated_data2",
        tmpdir.path().to_string_lossy(),
        options_sort.clone(),
        Some(batch.schema()),
        sql_definition,
    )
    .await
    .unwrap();
    Ok(ctx)
}

mod tests {
    use super::*;

    #[tokio::test]
    async fn test_source_sorted_aggregate() -> Result<()> {
        let tmpdir = TempDir::new().unwrap();
        let ctx = get_test_context(&tmpdir).await?;

        let sql = "SELECT
            SUM(inc_col) OVER(ORDER BY ts RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING) as sum1,
            SUM(desc_col) OVER(ORDER BY ts RANGE BETWEEN 5 PRECEDING AND 1 FOLLOWING) as sum2,
            SUM(inc_col) OVER(ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING) as sum3,
            MIN(inc_col) OVER(ORDER BY ts RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING) as min1,
            MIN(desc_col) OVER(ORDER BY ts RANGE BETWEEN 5 PRECEDING AND 1 FOLLOWING) as min2,
            MIN(inc_col) OVER(ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING) as min3,
            MAX(inc_col) OVER(ORDER BY ts RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING) as max1,
            MAX(desc_col) OVER(ORDER BY ts RANGE BETWEEN 5 PRECEDING AND 1 FOLLOWING) as max2,
            MAX(inc_col) OVER(ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING) as max3,
            COUNT(*) OVER(ORDER BY ts RANGE BETWEEN 4 PRECEDING AND 8 FOLLOWING) as cnt1,
            COUNT(*) OVER(ORDER BY ts ROWS BETWEEN 8 PRECEDING AND 1 FOLLOWING) as cnt2,
            SUM(inc_col) OVER(ORDER BY ts DESC RANGE BETWEEN 1 PRECEDING AND 4 FOLLOWING) as sumr1,
            SUM(desc_col) OVER(ORDER BY ts DESC RANGE BETWEEN 1 PRECEDING AND 8 FOLLOWING) as sumr2,
            SUM(desc_col) OVER(ORDER BY ts DESC ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING) as sumr3,
            MIN(inc_col) OVER(ORDER BY ts DESC RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING) as minr1,
            MIN(desc_col) OVER(ORDER BY ts DESC RANGE BETWEEN 5 PRECEDING AND 1 FOLLOWING) as minr2,
            MIN(inc_col) OVER(ORDER BY ts DESC ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING) as minr3,
            MAX(inc_col) OVER(ORDER BY ts DESC RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING) as maxr1,
            MAX(desc_col) OVER(ORDER BY ts DESC RANGE BETWEEN 5 PRECEDING AND 1 FOLLOWING) as maxr2,
            MAX(inc_col) OVER(ORDER BY ts DESC ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING) as maxr3,
            COUNT(*) OVER(ORDER BY ts DESC RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING) as cntr1,
            COUNT(*) OVER(ORDER BY ts DESC ROWS BETWEEN 8 PRECEDING AND 1 FOLLOWING) as cntr2,
            SUM(desc_col) OVER(ROWS BETWEEN 8 PRECEDING AND 1 FOLLOWING) as sum4,
            COUNT(*) OVER(ROWS BETWEEN 8 PRECEDING AND 1 FOLLOWING) as cnt3
            FROM annotated_data
            ORDER BY inc_col DESC
            LIMIT 5
            ";

        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        let formatted = displayable(physical_plan.as_ref()).indent().to_string();
        let expected = {
            vec![
                "ProjectionExec: expr=[sum1@0 as sum1, sum2@1 as sum2, sum3@2 as sum3, min1@3 as min1, min2@4 as min2, min3@5 as min3, max1@6 as max1, max2@7 as max2, max3@8 as max3, cnt1@9 as cnt1, cnt2@10 as cnt2, sumr1@11 as sumr1, sumr2@12 as sumr2, sumr3@13 as sumr3, minr1@14 as minr1, minr2@15 as minr2, minr3@16 as minr3, maxr1@17 as maxr1, maxr2@18 as maxr2, maxr3@19 as maxr3, cntr1@20 as cntr1, cntr2@21 as cntr2, sum4@22 as sum4, cnt3@23 as cnt3]",
                "  GlobalLimitExec: skip=0, fetch=5",
                "    SortExec: fetch=5, expr=[inc_col@24 DESC]",
                "      ProjectionExec: expr=[SUM(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@15 as sum1, SUM(annotated_data.desc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 5 PRECEDING AND 1 FOLLOWING@16 as sum2, SUM(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING@17 as sum3, MIN(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@18 as min1, MIN(annotated_data.desc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 5 PRECEDING AND 1 FOLLOWING@19 as min2, MIN(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING@20 as min3, MAX(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@21 as max1, MAX(annotated_data.desc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 5 PRECEDING AND 1 FOLLOWING@22 as max2, MAX(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING@23 as max3, COUNT(UInt8(1)) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 4 PRECEDING AND 8 FOLLOWING@24 as cnt1, COUNT(UInt8(1)) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 8 PRECEDING AND 1 FOLLOWING@25 as cnt2, SUM(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 1 PRECEDING AND 4 FOLLOWING@4 as sumr1, SUM(annotated_data.desc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 1 PRECEDING AND 8 FOLLOWING@5 as sumr2, SUM(annotated_data.desc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING@6 as sumr3, MIN(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@7 as minr1, MIN(annotated_data.desc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 5 PRECEDING AND 1 FOLLOWING@8 as minr2, MIN(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING@9 as minr3, MAX(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@10 as maxr1, MAX(annotated_data.desc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 5 PRECEDING AND 1 FOLLOWING@11 as maxr2, MAX(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING@12 as maxr3, COUNT(UInt8(1)) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING@13 as cntr1, COUNT(UInt8(1)) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 8 PRECEDING AND 1 FOLLOWING@14 as cntr2, SUM(annotated_data.desc_col) ROWS BETWEEN 8 PRECEDING AND 1 FOLLOWING@26 as sum4, COUNT(UInt8(1)) ROWS BETWEEN 8 PRECEDING AND 1 FOLLOWING@27 as cnt3, inc_col@1 as inc_col]",
                "        BoundedWindowAggExec: wdw=[SUM(annotated_data.desc_col): Ok(Field { name: \"SUM(annotated_data.desc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(8)), end_bound: Following(UInt64(1)) }, COUNT(UInt8(1)): Ok(Field { name: \"COUNT(UInt8(1))\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(8)), end_bound: Following(UInt64(1)) }], mode=[Sorted]",
                "          BoundedWindowAggExec: wdw=[SUM(annotated_data.inc_col): Ok(Field { name: \"SUM(annotated_data.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(10)), end_bound: Following(Int32(1)) }, SUM(annotated_data.desc_col): Ok(Field { name: \"SUM(annotated_data.desc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(5)), end_bound: Following(Int32(1)) }, SUM(annotated_data.inc_col): Ok(Field { name: \"SUM(annotated_data.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(10)) }, MIN(annotated_data.inc_col): Ok(Field { name: \"MIN(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(10)), end_bound: Following(Int32(1)) }, MIN(annotated_data.desc_col): Ok(Field { name: \"MIN(annotated_data.desc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(5)), end_bound: Following(Int32(1)) }, MIN(annotated_data.inc_col): Ok(Field { name: \"MIN(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(10)) }, MAX(annotated_data.inc_col): Ok(Field { name: \"MAX(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(10)), end_bound: Following(Int32(1)) }, MAX(annotated_data.desc_col): Ok(Field { name: \"MAX(annotated_data.desc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(5)), end_bound: Following(Int32(1)) }, MAX(annotated_data.inc_col): Ok(Field { name: \"MAX(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(10)) }, COUNT(UInt8(1)): Ok(Field { name: \"COUNT(UInt8(1))\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(4)), end_bound: Following(Int32(8)) }, COUNT(UInt8(1)): Ok(Field { name: \"COUNT(UInt8(1))\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(8)), end_bound: Following(UInt64(1)) }], mode=[Sorted]",
                "            BoundedWindowAggExec: wdw=[SUM(annotated_data.inc_col): Ok(Field { name: \"SUM(annotated_data.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(4)), end_bound: Following(Int32(1)) }, SUM(annotated_data.desc_col): Ok(Field { name: \"SUM(annotated_data.desc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(8)), end_bound: Following(Int32(1)) }, SUM(annotated_data.desc_col): Ok(Field { name: \"SUM(annotated_data.desc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(5)), end_bound: Following(UInt64(1)) }, MIN(annotated_data.inc_col): Ok(Field { name: \"MIN(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(10)) }, MIN(annotated_data.desc_col): Ok(Field { name: \"MIN(annotated_data.desc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(5)) }, MIN(annotated_data.inc_col): Ok(Field { name: \"MIN(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, MAX(annotated_data.inc_col): Ok(Field { name: \"MAX(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(10)) }, MAX(annotated_data.desc_col): Ok(Field { name: \"MAX(annotated_data.desc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(5)) }, MAX(annotated_data.inc_col): Ok(Field { name: \"MAX(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, COUNT(UInt8(1)): Ok(Field { name: \"COUNT(UInt8(1))\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(2)), end_bound: Following(Int32(6)) }, COUNT(UInt8(1)): Ok(Field { name: \"COUNT(UInt8(1))\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(8)) }], mode=[Sorted]",
            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        let actual_len = actual.len();
        let actual_trim_last = &actual[..actual_len - 1];
        assert_eq!(
            expected, actual_trim_last,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+------+------+------+------+------+------+------+------+------+------+------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+------+",
            "| sum1 | sum2 | sum3 | min1 | min2 | min3 | max1 | max2 | max3 | cnt1 | cnt2 | sumr1 | sumr2 | sumr3 | minr1 | minr2 | minr3 | maxr1 | maxr2 | maxr3 | cntr1 | cntr2 | sum4  | cnt3 |",
            "+------+------+------+------+------+------+------+------+------+------+------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+------+",
            "| 1482 | -631 | 606  | 289  | -213 | 301  | 305  | -208 | 305  | 3    | 9    | 902   | -834  | -1231 | 301   | -213  | 269   | 305   | -210  | 305   | 3     | 2     | -1797 | 9    |",
            "| 1482 | -631 | 902  | 289  | -213 | 296  | 305  | -208 | 305  | 3    | 10   | 902   | -834  | -1424 | 301   | -213  | 266   | 305   | -210  | 305   | 3     | 3     | -1978 | 10   |",
            "| 876  | -411 | 1193 | 289  | -208 | 291  | 296  | -203 | 305  | 4    | 10   | 587   | -612  | -1400 | 296   | -213  | 261   | 305   | -208  | 301   | 3     | 4     | -1941 | 10   |",
            "| 866  | -404 | 1482 | 286  | -203 | 289  | 291  | -201 | 305  | 5    | 10   | 580   | -600  | -1374 | 291   | -208  | 259   | 305   | -203  | 296   | 4     | 5     | -1903 | 10   |",
            "| 1411 | -397 | 1768 | 275  | -201 | 286  | 289  | -196 | 305  | 4    | 10   | 575   | -590  | -1347 | 289   | -203  | 254   | 305   | -201  | 291   | 2     | 6     | -1863 | 10   |",
            "+------+------+------+------+------+------+------+------+------+------+------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+------+",
        ];
        assert_batches_eq!(expected, &actual);
        Ok(())
    }

    #[tokio::test]
    async fn test_source_sorted_builtin() -> Result<()> {
        let tmpdir = TempDir::new().unwrap();
        let ctx = get_test_context(&tmpdir).await?;

        let sql = "SELECT
            FIRST_VALUE(inc_col) OVER(ORDER BY ts RANGE BETWEEN 10 PRECEDING and 1 FOLLOWING) as fv1,
            FIRST_VALUE(inc_col) OVER(ORDER BY ts ROWS BETWEEN 10 PRECEDING and 1 FOLLOWING) as fv2,
            LAST_VALUE(inc_col) OVER(ORDER BY ts RANGE BETWEEN 10 PRECEDING and 1 FOLLOWING) as lv1,
            LAST_VALUE(inc_col) OVER(ORDER BY ts ROWS BETWEEN 10 PRECEDING and 1 FOLLOWING) as lv2,
            NTH_VALUE(inc_col, 5) OVER(ORDER BY ts RANGE BETWEEN 10 PRECEDING and 1 FOLLOWING) as nv1,
            NTH_VALUE(inc_col, 5) OVER(ORDER BY ts ROWS BETWEEN 10 PRECEDING and 1 FOLLOWING) as nv2,
            ROW_NUMBER() OVER(ORDER BY ts RANGE BETWEEN 1 PRECEDING and 10 FOLLOWING) AS rn1,
            ROW_NUMBER() OVER(ORDER BY ts ROWS BETWEEN 10 PRECEDING and 1 FOLLOWING) as rn2,
            RANK() OVER(ORDER BY ts RANGE BETWEEN 1 PRECEDING and 10 FOLLOWING) AS rank1,
            RANK() OVER(ORDER BY ts ROWS BETWEEN 10 PRECEDING and 1 FOLLOWING) as rank2,
            DENSE_RANK() OVER(ORDER BY ts RANGE BETWEEN 1 PRECEDING and 10 FOLLOWING) AS dense_rank1,
            DENSE_RANK() OVER(ORDER BY ts ROWS BETWEEN 10 PRECEDING and 1 FOLLOWING) as dense_rank2,
            LAG(inc_col, 1, 1001) OVER(ORDER BY ts RANGE BETWEEN 1 PRECEDING and 10 FOLLOWING) AS lag1,
            LAG(inc_col, 2, 1002) OVER(ORDER BY ts ROWS BETWEEN 10 PRECEDING and 1 FOLLOWING) as lag2,
            LEAD(inc_col, -1, 1001) OVER(ORDER BY ts RANGE BETWEEN 1 PRECEDING and 10 FOLLOWING) AS lead1,
            LEAD(inc_col, 4, 1004) OVER(ORDER BY ts ROWS BETWEEN 10 PRECEDING and 1 FOLLOWING) as lead2,
            FIRST_VALUE(inc_col) OVER(ORDER BY ts DESC RANGE BETWEEN 10 PRECEDING and 1 FOLLOWING) as fvr1,
            FIRST_VALUE(inc_col) OVER(ORDER BY ts DESC ROWS BETWEEN 10 PRECEDING and 1 FOLLOWING) as fvr2,
            LAST_VALUE(inc_col) OVER(ORDER BY ts DESC RANGE BETWEEN 10 PRECEDING and 1 FOLLOWING) as lvr1,
            LAST_VALUE(inc_col) OVER(ORDER BY ts DESC ROWS BETWEEN 10 PRECEDING and 1 FOLLOWING) as lvr2,
            LAG(inc_col, 1, 1001) OVER(ORDER BY ts DESC RANGE BETWEEN 1 PRECEDING and 10 FOLLOWING) AS lagr1,
            LAG(inc_col, 2, 1002) OVER(ORDER BY ts DESC ROWS BETWEEN 10 PRECEDING and 1 FOLLOWING) as lagr2,
            LEAD(inc_col, -1, 1001) OVER(ORDER BY ts DESC RANGE BETWEEN 1 PRECEDING and 10 FOLLOWING) AS leadr1,
            LEAD(inc_col, 4, 1004) OVER(ORDER BY ts DESC ROWS BETWEEN 10 PRECEDING and 1 FOLLOWING) as leadr2
            FROM annotated_data
            ORDER BY ts DESC
            LIMIT 5
            ";

        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        let formatted = displayable(physical_plan.as_ref()).indent().to_string();
        let expected = {
            vec![
                "ProjectionExec: expr=[fv1@0 as fv1, fv2@1 as fv2, lv1@2 as lv1, lv2@3 as lv2, nv1@4 as nv1, nv2@5 as nv2, rn1@6 as rn1, rn2@7 as rn2, rank1@8 as rank1, rank2@9 as rank2, dense_rank1@10 as dense_rank1, dense_rank2@11 as dense_rank2, lag1@12 as lag1, lag2@13 as lag2, lead1@14 as lead1, lead2@15 as lead2, fvr1@16 as fvr1, fvr2@17 as fvr2, lvr1@18 as lvr1, lvr2@19 as lvr2, lagr1@20 as lagr1, lagr2@21 as lagr2, leadr1@22 as leadr1, leadr2@23 as leadr2]",
                "  GlobalLimitExec: skip=0, fetch=5",
                "    SortExec: fetch=5, expr=[ts@24 DESC]",
                "      ProjectionExec: expr=[FIRST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@12 as fv1, FIRST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@13 as fv2, LAST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@14 as lv1, LAST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@15 as lv2, NTH_VALUE(annotated_data.inc_col,Int64(5)) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@16 as nv1, NTH_VALUE(annotated_data.inc_col,Int64(5)) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@17 as nv2, ROW_NUMBER() ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 1 PRECEDING AND 10 FOLLOWING@18 as rn1, ROW_NUMBER() ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@19 as rn2, RANK() ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 1 PRECEDING AND 10 FOLLOWING@20 as rank1, RANK() ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@21 as rank2, DENSE_RANK() ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 1 PRECEDING AND 10 FOLLOWING@22 as dense_rank1, DENSE_RANK() ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@23 as dense_rank2, LAG(annotated_data.inc_col,Int64(1),Int64(1001)) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 1 PRECEDING AND 10 FOLLOWING@24 as lag1, LAG(annotated_data.inc_col,Int64(2),Int64(1002)) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@25 as lag2, LEAD(annotated_data.inc_col,Int64(-1),Int64(1001)) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 1 PRECEDING AND 10 FOLLOWING@26 as lead1, LEAD(annotated_data.inc_col,Int64(4),Int64(1004)) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@27 as lead2, FIRST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@4 as fvr1, FIRST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@5 as fvr2, LAST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@6 as lvr1, LAST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@7 as lvr2, LAG(annotated_data.inc_col,Int64(1),Int64(1001)) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 1 PRECEDING AND 10 FOLLOWING@8 as lagr1, LAG(annotated_data.inc_col,Int64(2),Int64(1002)) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@9 as lagr2, LEAD(annotated_data.inc_col,Int64(-1),Int64(1001)) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 1 PRECEDING AND 10 FOLLOWING@10 as leadr1, LEAD(annotated_data.inc_col,Int64(4),Int64(1004)) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@11 as leadr2, ts@0 as ts]",
                "        BoundedWindowAggExec: wdw=[FIRST_VALUE(annotated_data.inc_col): Ok(Field { name: \"FIRST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(10)), end_bound: Following(Int32(1)) }, FIRST_VALUE(annotated_data.inc_col): Ok(Field { name: \"FIRST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, LAST_VALUE(annotated_data.inc_col): Ok(Field { name: \"LAST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(10)), end_bound: Following(Int32(1)) }, LAST_VALUE(annotated_data.inc_col): Ok(Field { name: \"LAST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, NTH_VALUE(annotated_data.inc_col,Int64(5)): Ok(Field { name: \"NTH_VALUE(annotated_data.inc_col,Int64(5))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(10)), end_bound: Following(Int32(1)) }, NTH_VALUE(annotated_data.inc_col,Int64(5)): Ok(Field { name: \"NTH_VALUE(annotated_data.inc_col,Int64(5))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, ROW_NUMBER(): Ok(Field { name: \"ROW_NUMBER()\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(10)) }, ROW_NUMBER(): Ok(Field { name: \"ROW_NUMBER()\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, RANK(): Ok(Field { name: \"RANK()\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(10)) }, RANK(): Ok(Field { name: \"RANK()\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, DENSE_RANK(): Ok(Field { name: \"DENSE_RANK()\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(10)) }, DENSE_RANK(): Ok(Field { name: \"DENSE_RANK()\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, LAG(annotated_data.inc_col,Int64(1),Int64(1001)): Ok(Field { name: \"LAG(annotated_data.inc_col,Int64(1),Int64(1001))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(10)) }, LAG(annotated_data.inc_col,Int64(2),Int64(1002)): Ok(Field { name: \"LAG(annotated_data.inc_col,Int64(2),Int64(1002))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, LEAD(annotated_data.inc_col,Int64(-1),Int64(1001)): Ok(Field { name: \"LEAD(annotated_data.inc_col,Int64(-1),Int64(1001))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(10)) }, LEAD(annotated_data.inc_col,Int64(4),Int64(1004)): Ok(Field { name: \"LEAD(annotated_data.inc_col,Int64(4),Int64(1004))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }], mode=[Sorted]",
                "          BoundedWindowAggExec: wdw=[FIRST_VALUE(annotated_data.inc_col): Ok(Field { name: \"FIRST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(10)) }, FIRST_VALUE(annotated_data.inc_col): Ok(Field { name: \"FIRST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(10)) }, LAST_VALUE(annotated_data.inc_col): Ok(Field { name: \"LAST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(10)) }, LAST_VALUE(annotated_data.inc_col): Ok(Field { name: \"LAST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(10)) }, LAG(annotated_data.inc_col,Int64(1),Int64(1001)): Ok(Field { name: \"LAG(annotated_data.inc_col,Int64(1),Int64(1001))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(10)), end_bound: Following(Int32(1)) }, LAG(annotated_data.inc_col,Int64(2),Int64(1002)): Ok(Field { name: \"LAG(annotated_data.inc_col,Int64(2),Int64(1002))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(10)) }, LEAD(annotated_data.inc_col,Int64(-1),Int64(1001)): Ok(Field { name: \"LEAD(annotated_data.inc_col,Int64(-1),Int64(1001))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(10)), end_bound: Following(Int32(1)) }, LEAD(annotated_data.inc_col,Int64(4),Int64(1004)): Ok(Field { name: \"LEAD(annotated_data.inc_col,Int64(4),Int64(1004))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(10)) }], mode=[Sorted]",
            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        let actual_len = actual.len();
        let actual_trim_last = &actual[..actual_len - 1];
        assert_eq!(
            expected, actual_trim_last,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+-----+-----+-----+-----+-----+-----+-----+-----+-------+-------+-------------+-------------+------+------+-------+-------+------+------+------+------+-------+-------+--------+--------+",
            "| fv1 | fv2 | lv1 | lv2 | nv1 | nv2 | rn1 | rn2 | rank1 | rank2 | dense_rank1 | dense_rank2 | lag1 | lag2 | lead1 | lead2 | fvr1 | fvr2 | lvr1 | lvr2 | lagr1 | lagr2 | leadr1 | leadr2 |",
            "+-----+-----+-----+-----+-----+-----+-----+-----+-------+-------+-------------+-------------+------+------+-------+-------+------+------+------+------+-------+-------+--------+--------+",
            "| 289 | 266 | 305 | 305 | 305 | 278 | 99  | 99  | 99    | 99    | 86          | 86          | 296  | 291  | 296   | 1004  | 305  | 305  | 301  | 296  | 305   | 1002  | 305    | 286    |",
            "| 289 | 269 | 305 | 305 | 305 | 283 | 100 | 100 | 99    | 99    | 86          | 86          | 301  | 296  | 301   | 1004  | 305  | 305  | 301  | 301  | 1001  | 1002  | 1001   | 289    |",
            "| 289 | 261 | 296 | 301 |     | 275 | 98  | 98  | 98    | 98    | 85          | 85          | 291  | 289  | 291   | 1004  | 305  | 305  | 296  | 291  | 301   | 305   | 301    | 283    |",
            "| 286 | 259 | 291 | 296 |     | 272 | 97  | 97  | 97    | 97    | 84          | 84          | 289  | 286  | 289   | 1004  | 305  | 305  | 291  | 289  | 296   | 301   | 296    | 278    |",
            "| 275 | 254 | 289 | 291 | 289 | 269 | 96  | 96  | 96    | 96    | 83          | 83          | 286  | 283  | 286   | 305   | 305  | 305  | 289  | 286  | 291   | 296   | 291    | 275    |",
            "+-----+-----+-----+-----+-----+-----+-----+-----+-------+-------+-------------+-------------+------+------+-------+-------+------+------+------+------+-------+-------+--------+--------+",
        ];
        assert_batches_eq!(expected, &actual);
        Ok(())
    }

    #[tokio::test]
    async fn test_source_sorted_unbounded_preceding() -> Result<()> {
        let tmpdir = TempDir::new().unwrap();
        let ctx = get_test_context(&tmpdir).await?;

        let sql = "SELECT
            SUM(inc_col) OVER(ORDER BY ts ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING) as sum1,
            SUM(inc_col) OVER(ORDER BY ts DESC RANGE BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING) as sum2,
            MIN(inc_col) OVER(ORDER BY ts ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING) as min1,
            MIN(inc_col) OVER(ORDER BY ts DESC RANGE BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING) as min2,
            MAX(inc_col) OVER(ORDER BY ts ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING) as max1,
            MAX(inc_col) OVER(ORDER BY ts DESC RANGE BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING) as max2,
            COUNT(inc_col) OVER(ORDER BY ts ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING) as count1,
            COUNT(inc_col) OVER(ORDER BY ts DESC RANGE BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING) as count2,
            AVG(inc_col) OVER(ORDER BY ts ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING) as avg1,
            AVG(inc_col) OVER(ORDER BY ts DESC RANGE BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING) as avg2
            FROM annotated_data
            ORDER BY inc_col ASC
            LIMIT 5";

        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        let formatted = displayable(physical_plan.as_ref()).indent().to_string();
        let expected = {
            vec![
                "ProjectionExec: expr=[sum1@0 as sum1, sum2@1 as sum2, min1@2 as min1, min2@3 as min2, max1@4 as max1, max2@5 as max2, count1@6 as count1, count2@7 as count2, avg1@8 as avg1, avg2@9 as avg2]",
                "  GlobalLimitExec: skip=0, fetch=5",
                "    SortExec: fetch=5, expr=[inc_col@10 ASC NULLS LAST]",
                "      ProjectionExec: expr=[SUM(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING@9 as sum1, SUM(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING@4 as sum2, MIN(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING@10 as min1, MIN(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING@5 as min2, MAX(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING@11 as max1, MAX(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING@6 as max2, COUNT(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING@12 as count1, COUNT(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING@7 as count2, AVG(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING@13 as avg1, AVG(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING@8 as avg2, inc_col@1 as inc_col]",
                "        BoundedWindowAggExec: wdw=[SUM(annotated_data.inc_col): Ok(Field { name: \"SUM(annotated_data.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(5)) }, MIN(annotated_data.inc_col): Ok(Field { name: \"MIN(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(5)) }, MAX(annotated_data.inc_col): Ok(Field { name: \"MAX(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(5)) }, COUNT(annotated_data.inc_col): Ok(Field { name: \"COUNT(annotated_data.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(5)) }, AVG(annotated_data.inc_col): Ok(Field { name: \"AVG(annotated_data.inc_col)\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(5)) }], mode=[Sorted]",
                "          BoundedWindowAggExec: wdw=[SUM(annotated_data.inc_col): Ok(Field { name: \"SUM(annotated_data.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(3)) }, MIN(annotated_data.inc_col): Ok(Field { name: \"MIN(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(3)) }, MAX(annotated_data.inc_col): Ok(Field { name: \"MAX(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(3)) }, COUNT(annotated_data.inc_col): Ok(Field { name: \"COUNT(annotated_data.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(3)) }, AVG(annotated_data.inc_col): Ok(Field { name: \"AVG(annotated_data.inc_col)\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(3)) }], mode=[Sorted]",
            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        let actual_len = actual.len();
        let actual_trim_last = &actual[..actual_len - 1];
        assert_eq!(
            expected, actual_trim_last,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+------+------+------+------+------+------+--------+--------+-------------------+-------------------+",
            "| sum1 | sum2 | min1 | min2 | max1 | max2 | count1 | count2 | avg1              | avg2              |",
            "+------+------+------+------+------+------+--------+--------+-------------------+-------------------+",
            "| 16   | 6    | 1    | 1    | 10   | 5    | 3      | 2      | 5.333333333333333 | 3.0               |",
            "| 16   | 6    | 1    | 1    | 10   | 5    | 3      | 2      | 5.333333333333333 | 3.0               |",
            "| 51   | 16   | 1    | 1    | 20   | 10   | 5      | 3      | 10.2              | 5.333333333333333 |",
            "| 72   | 72   | 1    | 1    | 21   | 21   | 6      | 6      | 12.0              | 12.0              |",
            "| 72   | 72   | 1    | 1    | 21   | 21   | 6      | 6      | 12.0              | 12.0              |",
            "+------+------+------+------+------+------+--------+--------+-------------------+-------------------+",
        ];
        assert_batches_eq!(expected, &actual);
        Ok(())
    }

    #[tokio::test]
    async fn test_source_sorted_unbounded_preceding_builtin() -> Result<()> {
        let tmpdir = TempDir::new().unwrap();
        let ctx = get_test_context(&tmpdir).await?;

        let sql = "SELECT
           FIRST_VALUE(inc_col) OVER(ORDER BY ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) as first_value1,
           FIRST_VALUE(inc_col) OVER(ORDER BY ts DESC ROWS BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING) as first_value2,
           LAST_VALUE(inc_col) OVER(ORDER BY ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) as last_value1,
           LAST_VALUE(inc_col) OVER(ORDER BY ts DESC ROWS BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING) as last_value2,
           NTH_VALUE(inc_col, 2) OVER(ORDER BY ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) as nth_value1
           FROM annotated_data
           ORDER BY inc_col ASC
           LIMIT 5";

        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        let formatted = displayable(physical_plan.as_ref()).indent().to_string();
        let expected = {
            vec![
                "ProjectionExec: expr=[first_value1@0 as first_value1, first_value2@1 as first_value2, last_value1@2 as last_value1, last_value2@3 as last_value2, nth_value1@4 as nth_value1]",
                "  GlobalLimitExec: skip=0, fetch=5",
                "    SortExec: fetch=5, expr=[inc_col@5 ASC NULLS LAST]",
                "      ProjectionExec: expr=[FIRST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING@6 as first_value1, FIRST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING@4 as first_value2, LAST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING@7 as last_value1, LAST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING@5 as last_value2, NTH_VALUE(annotated_data.inc_col,Int64(2)) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING@8 as nth_value1, inc_col@1 as inc_col]",
                "        BoundedWindowAggExec: wdw=[FIRST_VALUE(annotated_data.inc_col): Ok(Field { name: \"FIRST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(1)) }, LAST_VALUE(annotated_data.inc_col): Ok(Field { name: \"LAST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(1)) }, NTH_VALUE(annotated_data.inc_col,Int64(2)): Ok(Field { name: \"NTH_VALUE(annotated_data.inc_col,Int64(2))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(1)) }], mode=[Sorted]",
                "          BoundedWindowAggExec: wdw=[FIRST_VALUE(annotated_data.inc_col): Ok(Field { name: \"FIRST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(3)) }, LAST_VALUE(annotated_data.inc_col): Ok(Field { name: \"LAST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(3)) }], mode=[Sorted]",
            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        let actual_len = actual.len();
        let actual_trim_last = &actual[..actual_len - 1];
        assert_eq!(
            expected, actual_trim_last,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+--------------+--------------+-------------+-------------+------------+",
            "| first_value1 | first_value2 | last_value1 | last_value2 | nth_value1 |",
            "+--------------+--------------+-------------+-------------+------------+",
            "| 1            | 15           | 5           | 1           | 5          |",
            "| 1            | 20           | 10          | 1           | 5          |",
            "| 1            | 21           | 15          | 1           | 5          |",
            "| 1            | 26           | 20          | 1           | 5          |",
            "| 1            | 29           | 21          | 1           | 5          |",
            "+--------------+--------------+-------------+-------------+------------+",
        ];
        assert_batches_eq!(expected, &actual);
        Ok(())
    }

    #[tokio::test]
    async fn test_source_partially_sorted_partition_by() -> Result<()> {
        let tmpdir = TempDir::new().unwrap();
        // Source is ordered by low_card_col1, low_card_col2, inc_col
        let ctx = get_test_context2(&tmpdir).await?;

        let sql = "SELECT low_card_col1, low_card_col2, inc_col,
        SUM(inc_col) OVER(PARTITION BY low_card_col1, unsorted_col ORDER BY low_card_col2, inc_col ASC ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) as sum1,
        SUM(inc_col) OVER(PARTITION BY low_card_col1, unsorted_col ORDER BY low_card_col2, inc_col ASC ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING) as sum2,
        SUM(inc_col) OVER(PARTITION BY unsorted_col ORDER BY low_card_col1, low_card_col2, inc_col ASC ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) as sum3,
        SUM(inc_col) OVER(PARTITION BY unsorted_col ORDER BY low_card_col1, low_card_col2, inc_col ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) as sum4,
        SUM(inc_col) OVER(PARTITION BY low_card_col1, low_card_col2 ORDER BY inc_col ASC ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) as sum5,
        SUM(inc_col) OVER(PARTITION BY low_card_col1, low_card_col2 ORDER BY inc_col ASC ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) as sum6,
        SUM(inc_col) OVER(PARTITION BY low_card_col2, low_card_col1 ORDER BY inc_col ASC ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) as sum7,
        SUM(inc_col) OVER(PARTITION BY low_card_col2, low_card_col1 ORDER BY inc_col ASC ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) as sum8,
        SUM(inc_col) OVER(PARTITION BY low_card_col1, low_card_col2, unsorted_col ORDER BY inc_col ASC ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) as sum9,
        SUM(inc_col) OVER(PARTITION BY low_card_col1, low_card_col2, unsorted_col ORDER BY inc_col ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) as sum10,
        SUM(inc_col) OVER(PARTITION BY low_card_col2, low_card_col1, unsorted_col ORDER BY inc_col ASC ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) as sum11,
        SUM(inc_col) OVER(PARTITION BY low_card_col2, low_card_col1, unsorted_col ORDER BY inc_col ASC ROWS BETWEEN CURRENT ROW  AND 1 FOLLOWING) as sum12
           FROM annotated_data2
           LIMIT 5";

        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        let formatted = displayable(physical_plan.as_ref()).indent().to_string();
        let expected = {
            vec![
                "ProjectionExec: expr=[low_card_col1@0 as low_card_col1, low_card_col2@1 as low_card_col2, inc_col@2 as inc_col, SUM(annotated_data2.inc_col) PARTITION BY [annotated_data2.low_card_col1, annotated_data2.unsorted_col] ORDER BY [annotated_data2.low_card_col2 ASC NULLS LAST, annotated_data2.inc_col ASC NULLS LAST] ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING@8 as sum1, SUM(annotated_data2.inc_col) PARTITION BY [annotated_data2.low_card_col1, annotated_data2.unsorted_col] ORDER BY [annotated_data2.low_card_col2 ASC NULLS LAST, annotated_data2.inc_col ASC NULLS LAST] ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING@9 as sum2, SUM(annotated_data2.inc_col) PARTITION BY [annotated_data2.unsorted_col] ORDER BY [annotated_data2.low_card_col1 ASC NULLS LAST, annotated_data2.low_card_col2 ASC NULLS LAST, annotated_data2.inc_col ASC NULLS LAST] ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING@14 as sum3, SUM(annotated_data2.inc_col) PARTITION BY [annotated_data2.unsorted_col] ORDER BY [annotated_data2.low_card_col1 ASC NULLS LAST, annotated_data2.low_card_col2 ASC NULLS LAST, annotated_data2.inc_col ASC NULLS LAST] ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING@15 as sum4, SUM(annotated_data2.inc_col) PARTITION BY [annotated_data2.low_card_col1, annotated_data2.low_card_col2] ORDER BY [annotated_data2.inc_col ASC NULLS LAST] ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING@4 as sum5, SUM(annotated_data2.inc_col) PARTITION BY [annotated_data2.low_card_col1, annotated_data2.low_card_col2] ORDER BY [annotated_data2.inc_col ASC NULLS LAST] ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING@5 as sum6, SUM(annotated_data2.inc_col) PARTITION BY [annotated_data2.low_card_col2, annotated_data2.low_card_col1] ORDER BY [annotated_data2.inc_col ASC NULLS LAST] ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING@10 as sum7, SUM(annotated_data2.inc_col) PARTITION BY [annotated_data2.low_card_col2, annotated_data2.low_card_col1] ORDER BY [annotated_data2.inc_col ASC NULLS LAST] ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING@11 as sum8, SUM(annotated_data2.inc_col) PARTITION BY [annotated_data2.low_card_col1, annotated_data2.low_card_col2, annotated_data2.unsorted_col] ORDER BY [annotated_data2.inc_col ASC NULLS LAST] ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING@6 as sum9, SUM(annotated_data2.inc_col) PARTITION BY [annotated_data2.low_card_col1, annotated_data2.low_card_col2, annotated_data2.unsorted_col] ORDER BY [annotated_data2.inc_col ASC NULLS LAST] ROWS BETWEEN 5 PRECEDING AND CURRENT ROW@7 as sum10, SUM(annotated_data2.inc_col) PARTITION BY [annotated_data2.low_card_col2, annotated_data2.low_card_col1, annotated_data2.unsorted_col] ORDER BY [annotated_data2.inc_col ASC NULLS LAST] ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING@12 as sum11, SUM(annotated_data2.inc_col) PARTITION BY [annotated_data2.low_card_col2, annotated_data2.low_card_col1, annotated_data2.unsorted_col] ORDER BY [annotated_data2.inc_col ASC NULLS LAST] ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING@13 as sum12]",
                "  GlobalLimitExec: skip=0, fetch=5",
                "    BoundedWindowAggExec: wdw=[SUM(annotated_data2.inc_col): Ok(Field { name: \"SUM(annotated_data2.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(2)), end_bound: Following(UInt64(1)) }, SUM(annotated_data2.inc_col): Ok(Field { name: \"SUM(annotated_data2.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(5)), end_bound: Preceding(UInt64(1)) }], mode=[Linear]",
                "      BoundedWindowAggExec: wdw=[SUM(annotated_data2.inc_col): Ok(Field { name: \"SUM(annotated_data2.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(2)), end_bound: Following(UInt64(1)) }, SUM(annotated_data2.inc_col): Ok(Field { name: \"SUM(annotated_data2.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(1)) }], mode=[PartiallySorted]",
                "        BoundedWindowAggExec: wdw=[SUM(annotated_data2.inc_col): Ok(Field { name: \"SUM(annotated_data2.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(2)), end_bound: Following(UInt64(1)) }, SUM(annotated_data2.inc_col): Ok(Field { name: \"SUM(annotated_data2.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(5)), end_bound: Following(UInt64(5)) }], mode=[Sorted]",
                "          BoundedWindowAggExec: wdw=[SUM(annotated_data2.inc_col): Ok(Field { name: \"SUM(annotated_data2.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(2)), end_bound: Following(UInt64(1)) }, SUM(annotated_data2.inc_col): Ok(Field { name: \"SUM(annotated_data2.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Following(UInt64(1)), end_bound: Following(UInt64(5)) }], mode=[PartiallySorted]",
                "            BoundedWindowAggExec: wdw=[SUM(annotated_data2.inc_col): Ok(Field { name: \"SUM(annotated_data2.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(2)), end_bound: Following(UInt64(1)) }, SUM(annotated_data2.inc_col): Ok(Field { name: \"SUM(annotated_data2.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(5)), end_bound: CurrentRow }], mode=[PartiallySorted]",
                "              BoundedWindowAggExec: wdw=[SUM(annotated_data2.inc_col): Ok(Field { name: \"SUM(annotated_data2.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(2)), end_bound: Following(UInt64(1)) }, SUM(annotated_data2.inc_col): Ok(Field { name: \"SUM(annotated_data2.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(5)), end_bound: Following(UInt64(5)) }], mode=[Sorted]",            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        let actual_len = actual.len();
        let actual_trim_last = &actual[..actual_len - 1];
        assert_eq!(
            expected, actual_trim_last,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+---------------+---------------+---------+------+------+------+------+------+------+------+------+------+-------+-------+-------+",
            "| low_card_col1 | low_card_col2 | inc_col | sum1 | sum2 | sum3 | sum4 | sum5 | sum6 | sum7 | sum8 | sum9 | sum10 | sum11 | sum12 |",
            "+---------------+---------------+---------+------+------+------+------+------+------+------+------+------+-------+-------+-------+",
            "| 0             | 0             | 0       | 2    | 53   | 2    |      | 1    | 15   | 1    | 15   | 2    | 0     | 2     | 2     |",
            "| 0             | 0             | 1       | 8    | 61   | 8    |      | 3    | 21   | 3    | 21   | 8    | 1     | 8     | 8     |",
            "| 0             | 0             | 2       | 5    | 74   | 5    | 0    | 6    | 28   | 6    | 28   | 5    | 2     | 5     | 5     |",
            "| 0             | 0             | 3       | 11   | 96   | 11   | 2    | 10   | 36   | 10   | 36   | 11   | 5     | 11    | 9     |",
            "| 0             | 0             | 4       | 9    | 72   | 9    |      | 14   | 45   | 14   | 45   | 9    | 4     | 9     | 9     |",
            "+---------------+---------------+---------+------+------+------+------+------+------+------+------+------+-------+-------+-------+",
        ];
        assert_batches_eq!(expected, &actual);
        Ok(())
    }
}
