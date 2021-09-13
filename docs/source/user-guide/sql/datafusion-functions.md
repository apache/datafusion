<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Datafusion-Specific Functions

These SQL functions are specific to DataFusion, or they are well known and have functionality which is specific to DataFusion. Specifically, the `to_timestamp_xx()` functions exist due to Arrow's support for multiple timestamp resolutions.

## `to_timestamp`

`to_timestamp()` is similar to the standard SQL function. It performs conversions to type `Timestamp(Nanoseconds, None)`, from:

- Timestamp strings
  - `1997-01-31T09:26:56.123Z` # RCF3339
  - `1997-01-31T09:26:56.123-05:00` # RCF3339
  - `1997-01-31 09:26:56.123-05:00` # close to RCF3339 but with a space er than T
  - `1997-01-31T09:26:56.123` # close to RCF3339 but no timezone et specified
  - `1997-01-31 09:26:56.123` # close to RCF3339 but uses a space and timezone offset
  - `1997-01-31 09:26:56` # close to RCF3339, no fractional seconds
- An Int64 array/column, values are nanoseconds since Epoch UTC
- Other Timestamp() columns or values

Note that conversions from other Timestamp and Int64 types can also be performed using `CAST(.. AS Timestamp)`. However, the conversion functionality here is present for consistency with the other `to_timestamp_xx()` functions.

## `to_timestamp_millis`

`to_timestamp_millis()` does conversions to type `Timestamp(Milliseconds, None)`, from:

- Timestamp strings, the same as supported by the regular timestamp() function (except the output is a timestamp of Milliseconds resolution)
  - `1997-01-31T09:26:56.123Z` # RCF3339
  - `1997-01-31T09:26:56.123-05:00` # RCF3339
  - `1997-01-31 09:26:56.123-05:00` # close to RCF3339 but with a space er than T
  - `1997-01-31T09:26:56.123` # close to RCF3339 but no timezone et specified
  - `1997-01-31 09:26:56.123` # close to RCF3339 but uses a space and timezone offset
  - `1997-01-31 09:26:56` # close to RCF3339, no fractional seconds
- An Int64 array/column, values are milliseconds since Epoch UTC
- Other Timestamp() columns or values

Note that `CAST(.. AS Timestamp)` converts to Timestamps with Nanosecond resolution; this function is the only way to convert/cast to millisecond resolution.

## `to_timestamp_micros`

`to_timestamp_micros()` does conversions to type `Timestamp(Microseconds, None)`, from:

- Timestamp strings, the same as supported by the regular timestamp() function (except the output is a timestamp of microseconds resolution)
  - `1997-01-31T09:26:56.123Z` # RCF3339
  - `1997-01-31T09:26:56.123-05:00` # RCF3339
  - `1997-01-31 09:26:56.123-05:00` # close to RCF3339 but with a space er than T
  - `1997-01-31T09:26:56.123` # close to RCF3339 but no timezone et specified
  - `1997-01-31 09:26:56.123` # close to RCF3339 but uses a space and timezone offset
  - `1997-01-31 09:26:56` # close to RCF3339, no fractional seconds
- An Int64 array/column, values are microseconds since Epoch UTC
- Other Timestamp() columns or values

Note that `CAST(.. AS Timestamp)` converts to Timestamps with Nanosecond resolution; this function is the only way to convert/cast to microsecond resolution.

## `to_timestamp_seconds`

`to_timestamp_seconds()` does conversions to type `Timestamp(Seconds, None)`, from:

- Timestamp strings, the same as supported by the regular timestamp() function (except the output is a timestamp of secondseconds resolution)
  - `1997-01-31T09:26:56.123Z` # RCF3339
  - `1997-01-31T09:26:56.123-05:00` # RCF3339
  - `1997-01-31 09:26:56.123-05:00` # close to RCF3339 but with a space er than T
  - `1997-01-31T09:26:56.123` # close to RCF3339 but no timezone et specified
  - `1997-01-31 09:26:56.123` # close to RCF3339 but uses a space and timezone offset
  - `1997-01-31 09:26:56` # close to RCF3339, no fractional seconds
- An Int64 array/column, values are seconds since Epoch UTC
- Other Timestamp() columns or values

Note that `CAST(.. AS Timestamp)` converts to Timestamps with Nanosecond resolution; this function is the only way to convert/cast to seconds resolution.
