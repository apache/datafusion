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
async fn group_by_dictionary() {
    async fn run_test_case<K: ArrowDictionaryKeyType>() {
        let ctx = SessionContext::new();

        // input data looks like:
        // A, 1
        // B, 2
        // A, 2
        // A, 4
        // C, 1
        // A, 1

        let dict_array: DictionaryArray<K> =
            vec!["A", "B", "A", "A", "C", "A"].into_iter().collect();
        let dict_array = Arc::new(dict_array);

        let val_array: Int64Array = vec![1, 2, 2, 4, 1, 1].into();
        let val_array = Arc::new(val_array);

        let schema = Arc::new(Schema::new(vec![
            Field::new("dict", dict_array.data_type().clone(), false),
            Field::new("val", val_array.data_type().clone(), false),
        ]));

        let batch =
            RecordBatch::try_new(schema.clone(), vec![dict_array, val_array]).unwrap();

        ctx.register_batch("t", batch).unwrap();

        let results =
            plan_and_collect(&ctx, "SELECT dict, count(val) FROM t GROUP BY dict")
                .await
                .expect("ran plan correctly");

        let expected = [
            "+------+--------------+",
            "| dict | COUNT(t.val) |",
            "+------+--------------+",
            "| A    | 4            |",
            "| B    | 1            |",
            "| C    | 1            |",
            "+------+--------------+",
        ];
        assert_batches_sorted_eq!(expected, &results);

        // Now, use dict as an aggregate
        let results =
            plan_and_collect(&ctx, "SELECT val, count(dict) FROM t GROUP BY val")
                .await
                .expect("ran plan correctly");

        let expected = [
            "+-----+---------------+",
            "| val | COUNT(t.dict) |",
            "+-----+---------------+",
            "| 1   | 3             |",
            "| 2   | 2             |",
            "| 4   | 1             |",
            "+-----+---------------+",
        ];
        assert_batches_sorted_eq!(expected, &results);

        // Now, use dict as an aggregate
        let results = plan_and_collect(
            &ctx,
            "SELECT val, count(distinct dict) FROM t GROUP BY val",
        )
        .await
        .expect("ran plan correctly");

        let expected = [
            "+-----+------------------------+",
            "| val | COUNT(DISTINCT t.dict) |",
            "+-----+------------------------+",
            "| 1   | 2                      |",
            "| 2   | 2                      |",
            "| 4   | 1                      |",
            "+-----+------------------------+",
        ];
        assert_batches_sorted_eq!(expected, &results);
    }

    run_test_case::<Int8Type>().await;
    run_test_case::<Int16Type>().await;
    run_test_case::<Int32Type>().await;
    run_test_case::<Int64Type>().await;
    run_test_case::<UInt8Type>().await;
    run_test_case::<UInt16Type>().await;
    run_test_case::<UInt32Type>().await;
    run_test_case::<UInt64Type>().await;
}
