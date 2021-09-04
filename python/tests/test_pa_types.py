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


def test_type_ids():
    # Having this fixed is very important because internally we rely on this id
    # to parse from python
    for idx, arrow_type in [
        (0, pa.null()),
        (1, pa.bool_()),
        (2, pa.uint8()),
        (3, pa.int8()),
        (4, pa.uint16()),
        (5, pa.int16()),
        (6, pa.uint32()),
        (7, pa.int32()),
        (8, pa.uint64()),
        (9, pa.int64()),
        (10, pa.float16()),
        (11, pa.float32()),
        (12, pa.float64()),
        (13, pa.string()),
        (13, pa.utf8()),
        (14, pa.binary()),
        (16, pa.date32()),
        (17, pa.date64()),
        (18, pa.timestamp("us")),
        (19, pa.time32("s")),
        (20, pa.time64("us")),
        (23, pa.decimal128(8, 1)),
        (34, pa.large_utf8()),
        (35, pa.large_binary()),
    ]:
        assert idx == arrow_type.id
