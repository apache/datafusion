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
use ::parquet::arrow::arrow_writer::ArrowWriter;
use ::parquet::file::properties::WriterProperties;
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

fn write_test_data_to_parquet(tmpdir: &TempDir, n_file: usize) -> Result<()> {
    let ts_field = Field::new("ts", DataType::Int32, false);
    let inc_field = Field::new("inc_col", DataType::Int32, false);
    let desc_field = Field::new("desc_col", DataType::Int32, false);

    let schema = Arc::new(Schema::new(vec![ts_field, inc_field, desc_field]));

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
        ],
    )?;
    let n_chunk = batch.num_rows() / n_file;
    for i in 0..n_file {
        let target_file = tmpdir.path().join(format!("{i}.parquet"));
        let file = File::create(target_file).unwrap();
        // Default writer properties
        let props = WriterProperties::builder().build();
        let chunks_start = i * n_chunk;
        let cur_batch = batch.slice(chunks_start, n_chunk);
        // let chunks_end = chunks_start + n_chunk;
        let mut writer =
            ArrowWriter::try_new(file, cur_batch.schema(), Some(props)).unwrap();

        writer.write(&cur_batch).expect("Writing batch");

        // writer must be closed to write footer
        writer.close().unwrap();
    }
    Ok(())
}

async fn get_test_context(tmpdir: &TempDir) -> Result<SessionContext> {
    let session_config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::with_config(session_config);

    let parquet_read_options = ParquetReadOptions::default();
    // The sort order is specified (not actually correct in this case)
    let file_sort_order = [col("ts")]
        .into_iter()
        .map(|e| {
            let ascending = true;
            let nulls_first = false;
            e.sort(ascending, nulls_first)
        })
        .collect::<Vec<_>>();

    let options_sort = parquet_read_options
        .to_listing_options(&ctx.copied_config())
        .with_file_sort_order(Some(file_sort_order));

    write_test_data_to_parquet(tmpdir, 1)?;
    let provided_schema = None;
    let sql_definition = None;
    ctx.register_listing_table(
        "annotated_data",
        tmpdir.path().to_string_lossy(),
        options_sort.clone(),
        provided_schema,
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
                "      ProjectionExec: expr=[SUM(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@14 as sum1, SUM(annotated_data.desc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 5 PRECEDING AND 1 FOLLOWING@15 as sum2, SUM(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING@16 as sum3, MIN(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@17 as min1, MIN(annotated_data.desc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 5 PRECEDING AND 1 FOLLOWING@18 as min2, MIN(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING@19 as min3, MAX(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@20 as max1, MAX(annotated_data.desc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 5 PRECEDING AND 1 FOLLOWING@21 as max2, MAX(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING@22 as max3, COUNT(UInt8(1)) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 4 PRECEDING AND 8 FOLLOWING@23 as cnt1, COUNT(UInt8(1)) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 8 PRECEDING AND 1 FOLLOWING@24 as cnt2, SUM(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 1 PRECEDING AND 4 FOLLOWING@3 as sumr1, SUM(annotated_data.desc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 1 PRECEDING AND 8 FOLLOWING@4 as sumr2, SUM(annotated_data.desc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING@5 as sumr3, MIN(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@6 as minr1, MIN(annotated_data.desc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 5 PRECEDING AND 1 FOLLOWING@7 as minr2, MIN(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING@8 as minr3, MAX(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@9 as maxr1, MAX(annotated_data.desc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 5 PRECEDING AND 1 FOLLOWING@10 as maxr2, MAX(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING@11 as maxr3, COUNT(UInt8(1)) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 6 PRECEDING AND 2 FOLLOWING@12 as cntr1, COUNT(UInt8(1)) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 8 PRECEDING AND 1 FOLLOWING@13 as cntr2, SUM(annotated_data.desc_col) ROWS BETWEEN 8 PRECEDING AND 1 FOLLOWING@25 as sum4, COUNT(UInt8(1)) ROWS BETWEEN 8 PRECEDING AND 1 FOLLOWING@26 as cnt3, inc_col@1 as inc_col]",
                "        BoundedWindowAggExec: wdw=[SUM(annotated_data.desc_col): Ok(Field { name: \"SUM(annotated_data.desc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(8)), end_bound: Following(UInt64(1)) }, COUNT(UInt8(1)): Ok(Field { name: \"COUNT(UInt8(1))\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(8)), end_bound: Following(UInt64(1)) }]",
                "          BoundedWindowAggExec: wdw=[SUM(annotated_data.inc_col): Ok(Field { name: \"SUM(annotated_data.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(10)), end_bound: Following(Int32(1)) }, SUM(annotated_data.desc_col): Ok(Field { name: \"SUM(annotated_data.desc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(5)), end_bound: Following(Int32(1)) }, SUM(annotated_data.inc_col): Ok(Field { name: \"SUM(annotated_data.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(10)) }, MIN(annotated_data.inc_col): Ok(Field { name: \"MIN(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(10)), end_bound: Following(Int32(1)) }, MIN(annotated_data.desc_col): Ok(Field { name: \"MIN(annotated_data.desc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(5)), end_bound: Following(Int32(1)) }, MIN(annotated_data.inc_col): Ok(Field { name: \"MIN(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(10)) }, MAX(annotated_data.inc_col): Ok(Field { name: \"MAX(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(10)), end_bound: Following(Int32(1)) }, MAX(annotated_data.desc_col): Ok(Field { name: \"MAX(annotated_data.desc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(5)), end_bound: Following(Int32(1)) }, MAX(annotated_data.inc_col): Ok(Field { name: \"MAX(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(10)) }, COUNT(UInt8(1)): Ok(Field { name: \"COUNT(UInt8(1))\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(4)), end_bound: Following(Int32(8)) }, COUNT(UInt8(1)): Ok(Field { name: \"COUNT(UInt8(1))\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(8)), end_bound: Following(UInt64(1)) }]",
                "            BoundedWindowAggExec: wdw=[SUM(annotated_data.inc_col): Ok(Field { name: \"SUM(annotated_data.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(4)), end_bound: Following(Int32(1)) }, SUM(annotated_data.desc_col): Ok(Field { name: \"SUM(annotated_data.desc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(8)), end_bound: Following(Int32(1)) }, SUM(annotated_data.desc_col): Ok(Field { name: \"SUM(annotated_data.desc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(5)), end_bound: Following(UInt64(1)) }, MIN(annotated_data.inc_col): Ok(Field { name: \"MIN(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(10)) }, MIN(annotated_data.desc_col): Ok(Field { name: \"MIN(annotated_data.desc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(5)) }, MIN(annotated_data.inc_col): Ok(Field { name: \"MIN(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, MAX(annotated_data.inc_col): Ok(Field { name: \"MAX(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(10)) }, MAX(annotated_data.desc_col): Ok(Field { name: \"MAX(annotated_data.desc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(5)) }, MAX(annotated_data.inc_col): Ok(Field { name: \"MAX(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, COUNT(UInt8(1)): Ok(Field { name: \"COUNT(UInt8(1))\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(2)), end_bound: Following(Int32(6)) }, COUNT(UInt8(1)): Ok(Field { name: \"COUNT(UInt8(1))\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(8)) }]",
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
                "      ProjectionExec: expr=[FIRST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@11 as fv1, FIRST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@12 as fv2, LAST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@13 as lv1, LAST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@14 as lv2, NTH_VALUE(annotated_data.inc_col,Int64(5)) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@15 as nv1, NTH_VALUE(annotated_data.inc_col,Int64(5)) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@16 as nv2, ROW_NUMBER() ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 1 PRECEDING AND 10 FOLLOWING@17 as rn1, ROW_NUMBER() ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@18 as rn2, RANK() ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 1 PRECEDING AND 10 FOLLOWING@19 as rank1, RANK() ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@20 as rank2, DENSE_RANK() ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 1 PRECEDING AND 10 FOLLOWING@21 as dense_rank1, DENSE_RANK() ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@22 as dense_rank2, LAG(annotated_data.inc_col,Int64(1),Int64(1001)) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 1 PRECEDING AND 10 FOLLOWING@23 as lag1, LAG(annotated_data.inc_col,Int64(2),Int64(1002)) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@24 as lag2, LEAD(annotated_data.inc_col,Int64(-1),Int64(1001)) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN 1 PRECEDING AND 10 FOLLOWING@25 as lead1, LEAD(annotated_data.inc_col,Int64(4),Int64(1004)) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@26 as lead2, FIRST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@3 as fvr1, FIRST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@4 as fvr2, LAST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 10 PRECEDING AND 1 FOLLOWING@5 as lvr1, LAST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@6 as lvr2, LAG(annotated_data.inc_col,Int64(1),Int64(1001)) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 1 PRECEDING AND 10 FOLLOWING@7 as lagr1, LAG(annotated_data.inc_col,Int64(2),Int64(1002)) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@8 as lagr2, LEAD(annotated_data.inc_col,Int64(-1),Int64(1001)) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 1 PRECEDING AND 10 FOLLOWING@9 as leadr1, LEAD(annotated_data.inc_col,Int64(4),Int64(1004)) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 10 PRECEDING AND 1 FOLLOWING@10 as leadr2, ts@0 as ts]",
                "        BoundedWindowAggExec: wdw=[FIRST_VALUE(annotated_data.inc_col): Ok(Field { name: \"FIRST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(10)), end_bound: Following(Int32(1)) }, FIRST_VALUE(annotated_data.inc_col): Ok(Field { name: \"FIRST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, LAST_VALUE(annotated_data.inc_col): Ok(Field { name: \"LAST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(10)), end_bound: Following(Int32(1)) }, LAST_VALUE(annotated_data.inc_col): Ok(Field { name: \"LAST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, NTH_VALUE(annotated_data.inc_col,Int64(5)): Ok(Field { name: \"NTH_VALUE(annotated_data.inc_col,Int64(5))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(10)), end_bound: Following(Int32(1)) }, NTH_VALUE(annotated_data.inc_col,Int64(5)): Ok(Field { name: \"NTH_VALUE(annotated_data.inc_col,Int64(5))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, ROW_NUMBER(): Ok(Field { name: \"ROW_NUMBER()\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(10)) }, ROW_NUMBER(): Ok(Field { name: \"ROW_NUMBER()\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, RANK(): Ok(Field { name: \"RANK()\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(10)) }, RANK(): Ok(Field { name: \"RANK()\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, DENSE_RANK(): Ok(Field { name: \"DENSE_RANK()\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(10)) }, DENSE_RANK(): Ok(Field { name: \"DENSE_RANK()\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, LAG(annotated_data.inc_col,Int64(1),Int64(1001)): Ok(Field { name: \"LAG(annotated_data.inc_col,Int64(1),Int64(1001))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(10)) }, LAG(annotated_data.inc_col,Int64(2),Int64(1002)): Ok(Field { name: \"LAG(annotated_data.inc_col,Int64(2),Int64(1002))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }, LEAD(annotated_data.inc_col,Int64(-1),Int64(1001)): Ok(Field { name: \"LEAD(annotated_data.inc_col,Int64(-1),Int64(1001))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(10)) }, LEAD(annotated_data.inc_col,Int64(4),Int64(1004)): Ok(Field { name: \"LEAD(annotated_data.inc_col,Int64(4),Int64(1004))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(10)), end_bound: Following(UInt64(1)) }]",
                "          BoundedWindowAggExec: wdw=[FIRST_VALUE(annotated_data.inc_col): Ok(Field { name: \"FIRST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(10)) }, FIRST_VALUE(annotated_data.inc_col): Ok(Field { name: \"FIRST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(10)) }, LAST_VALUE(annotated_data.inc_col): Ok(Field { name: \"LAST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(1)), end_bound: Following(Int32(10)) }, LAST_VALUE(annotated_data.inc_col): Ok(Field { name: \"LAST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(10)) }, LAG(annotated_data.inc_col,Int64(1),Int64(1001)): Ok(Field { name: \"LAG(annotated_data.inc_col,Int64(1),Int64(1001))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(10)), end_bound: Following(Int32(1)) }, LAG(annotated_data.inc_col,Int64(2),Int64(1002)): Ok(Field { name: \"LAG(annotated_data.inc_col,Int64(2),Int64(1002))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(10)) }, LEAD(annotated_data.inc_col,Int64(-1),Int64(1001)): Ok(Field { name: \"LEAD(annotated_data.inc_col,Int64(-1),Int64(1001))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(10)), end_bound: Following(Int32(1)) }, LEAD(annotated_data.inc_col,Int64(4),Int64(1004)): Ok(Field { name: \"LEAD(annotated_data.inc_col,Int64(4),Int64(1004))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(10)) }]",
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
                "      ProjectionExec: expr=[SUM(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING@8 as sum1, SUM(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING@3 as sum2, MIN(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING@9 as min1, MIN(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING@4 as min2, MAX(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING@10 as max1, MAX(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING@5 as max2, COUNT(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING@11 as count1, COUNT(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING@6 as count2, AVG(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING@12 as avg1, AVG(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] RANGE BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING@7 as avg2, inc_col@1 as inc_col]",
                "        BoundedWindowAggExec: wdw=[SUM(annotated_data.inc_col): Ok(Field { name: \"SUM(annotated_data.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(5)) }, MIN(annotated_data.inc_col): Ok(Field { name: \"MIN(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(5)) }, MAX(annotated_data.inc_col): Ok(Field { name: \"MAX(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(5)) }, COUNT(annotated_data.inc_col): Ok(Field { name: \"COUNT(annotated_data.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(5)) }, AVG(annotated_data.inc_col): Ok(Field { name: \"AVG(annotated_data.inc_col)\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(5)) }]",
                "          BoundedWindowAggExec: wdw=[SUM(annotated_data.inc_col): Ok(Field { name: \"SUM(annotated_data.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(3)) }, MIN(annotated_data.inc_col): Ok(Field { name: \"MIN(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(3)) }, MAX(annotated_data.inc_col): Ok(Field { name: \"MAX(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(3)) }, COUNT(annotated_data.inc_col): Ok(Field { name: \"COUNT(annotated_data.inc_col)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(3)) }, AVG(annotated_data.inc_col): Ok(Field { name: \"AVG(annotated_data.inc_col)\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(Int32(NULL)), end_bound: Following(Int32(3)) }]",
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
                "      ProjectionExec: expr=[FIRST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING@5 as first_value1, FIRST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING@3 as first_value2, LAST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING@6 as last_value1, LAST_VALUE(annotated_data.inc_col) ORDER BY [annotated_data.ts DESC NULLS FIRST] ROWS BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING@4 as last_value2, NTH_VALUE(annotated_data.inc_col,Int64(2)) ORDER BY [annotated_data.ts ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING@7 as nth_value1, inc_col@1 as inc_col]",
                "        BoundedWindowAggExec: wdw=[FIRST_VALUE(annotated_data.inc_col): Ok(Field { name: \"FIRST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(1)) }, LAST_VALUE(annotated_data.inc_col): Ok(Field { name: \"LAST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(1)) }, NTH_VALUE(annotated_data.inc_col,Int64(2)): Ok(Field { name: \"NTH_VALUE(annotated_data.inc_col,Int64(2))\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(1)) }]",
                "          BoundedWindowAggExec: wdw=[FIRST_VALUE(annotated_data.inc_col): Ok(Field { name: \"FIRST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(3)) }, LAST_VALUE(annotated_data.inc_col): Ok(Field { name: \"LAST_VALUE(annotated_data.inc_col)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(3)) }]",
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
}
