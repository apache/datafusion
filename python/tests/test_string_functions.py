# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pyarrow as pa
import pytest
from datafusion import ExecutionContext
from datafusion import functions as f


@pytest.fixture
def df():
    ctx = ExecutionContext()

    # create a RecordBatch and a new DataFrame from it
    batch = pa.RecordBatch.from_arrays(
        [pa.array(["Hello", "World", "!"]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )

    return ctx.create_dataframe([[batch]])


def test_string_functions(df):
    df = df.select(f.md5(f.col("a")), f.lower(f.col("a")))
    result = df.collect()
    assert len(result) == 1
    result = result[0]
    assert result.column(0) == pa.array(
        [
            "8b1a9953c4611296a827abf8c47804d7",
            "f5a7924e621e84c9280a9a27e1bcb7f6",
            "9033e0e305f247c0c3c80d0c7848c8b3",
        ]
    )
    assert result.column(1) == pa.array(["hello", "world", "!"])


def test_hash_functions(df):
    df = df.select(
        *[
            f.digest(f.col("a"), f.lit(m))
            for m in ("md5", "sha256", "sha512", "blake2s", "blake3")
        ]
    )
    result = df.collect()
    assert len(result) == 1
    result = result[0]
    b = bytearray.fromhex
    assert result.column(0) == pa.array(
        [
            b("8B1A9953C4611296A827ABF8C47804D7"),
            b("F5A7924E621E84C9280A9A27E1BCB7F6"),
            b("9033E0E305F247C0C3C80D0C7848C8B3"),
        ]
    )
    assert result.column(1) == pa.array(
        [
            b(
                "185F8DB32271FE25F561A6FC938B2E264306EC304EDA518007D1764826381969"
            ),
            b(
                "78AE647DC5544D227130A0682A51E30BC7777FBB6D8A8F17007463A3ECD1D524"
            ),
            b(
                "BB7208BC9B5D7C04F1236A82A0093A5E33F40423D5BA8D4266F7092C3BA43B62"
            ),
        ]
    )
    assert result.column(2) == pa.array(
        [
            b(
                "3615F80C9D293ED7402687F94B22D58E529B8CC7916F8FAC7FDDF7FBD5AF4CF777D3D795A7A00A16BF7E7F3FB9561EE9BAAE480DA9FE7A18769E71886B03F315"
            ),
            b(
                "8EA77393A42AB8FA92500FB077A9509CC32BC95E72712EFA116EDAF2EDFAE34FBB682EFDD6C5DD13C117E08BD4AAEF71291D8AACE2F890273081D0677C16DF0F"
            ),
            b(
                "3831A6A6155E509DEE59A7F451EB35324D8F8F2DF6E3708894740F98FDEE23889F4DE5ADB0C5010DFB555CDA77C8AB5DC902094C52DE3278F35A75EBC25F093A"
            ),
        ]
    )
    assert result.column(3) == pa.array(
        [
            b(
                "F73A5FBF881F89B814871F46E26AD3FA37CB2921C5E8561618639015B3CCBB71"
            ),
            b(
                "B792A0383FB9E7A189EC150686579532854E44B71AC394831DAED169BA85CCC5"
            ),
            b(
                "27988A0E51812297C77A433F635233346AEE29A829DCF4F46E0F58F402C6CFCB"
            ),
        ]
    )
    assert result.column(4) == pa.array(
        [
            b(
                "FBC2B0516EE8744D293B980779178A3508850FDCFE965985782C39601B65794F"
            ),
            b(
                "BF73D18575A736E4037D45F9E316085B86C19BE6363DE6AA789E13DEAACC1C4E"
            ),
            b(
                "C8D11B9F7237E4034ADBCD2005735F9BC4C597C75AD89F4492BEC8F77D15F7EB"
            ),
        ]
    )
