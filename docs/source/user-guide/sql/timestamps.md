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

# Timestamps and Time Zones

One question controls almost all timestamp results in DataFusion. When
DataFusion adds a time zone to a timestamp, or removes one, what stays the same?
Is it the _instant_, or is it the _wall clock_?

This page gives the answer. Then it uses the answer for casts, the session time
zone, the date and time functions, daylight saving time, and some examples.

This page uses two names:

- An **aware timestamp** has a time zone.
- A **naive timestamp** has no time zone.

`datafusion-cli` on DataFusion 55.0.0 made all the output on this page. The
settings are the default settings, unless a `SET` statement shows a different
setting.

## The data model

DataFusion timestamps are Arrow timestamps. Arrow has two timestamp types. The
difference between the two types controls all the behavior on this page.

| Arrow type                  | Physical value            | Meaning                                                                                                 |
| --------------------------- | ------------------------- | ------------------------------------------------------------------------------------------------------- |
| `Timestamp(unit, Some(tz))` | offset from the UTC epoch | An **instant**. `tz` is a display annotation: it says how to show the instant, not what the instant is. |
| `Timestamp(unit, None)`     | a wall-clock reading      | A **wall clock** with no instant attached. There is no fact about which point in time it names.         |

Two results follow. These two results explain most of this page.

- The `tz` of an aware timestamp **is not data**. Two values with the same
  integer and different `tz` annotations are the _same instant_. In a
  comparison, a sort, a `GROUP BY` or a join, they are equal.
- A naive timestamp has **no instant**. An operation that needs an instant must
  select a time zone. [The session time zone](#the-session-time-zone) shows
  which time zone DataFusion selects. The time zone is not always the same.

### How SQL types agree with Arrow types

| SQL type                                                   | Arrow type                                                    |
| ---------------------------------------------------------- | ------------------------------------------------------------- |
| `TIMESTAMP`, `TIMESTAMP WITHOUT TIME ZONE`, `::timestamp`  | `Timestamp(Nanosecond, None)`                                 |
| `TIMESTAMP WITH TIME ZONE`, `TIMESTAMPTZ`, `::timestamptz` | `Timestamp(Nanosecond, Some(datafusion.execution.time_zone))` |

In `TIMESTAMP(p)`, a `p` of 0, 3, 6 or 9 selects second, millisecond,
microsecond or nanosecond precision.

The second row is important, because DataFusion and PostgreSQL do not agree
here. `TIMESTAMP WITH TIME ZONE` uses the value of
`datafusion.execution.time_zone`. The default value of that setting is **not
set**. If you do not set it, `TIMESTAMP WITH TIME ZONE` is a naive type:

```sql
SELECT arrow_typeof('2024-01-01T12:00:00Z'::timestamptz) AS type,
       '2024-01-01T12:00:00Z'::timestamptz AS value;
```

```text
+---------------+---------------------+
| type          | value               |
+---------------+---------------------+
| Timestamp(ns) | 2024-01-01T12:00:00 |
+---------------+---------------------+
```

DataFusion accepted the `Z` and then removed it. PostgreSQL cannot be in this
condition, because its `TimeZone` parameter always has a value. As a result,
`timestamptz` in PostgreSQL is always an aware type. Set the session time zone.
Then the same query gives an aware value, as in PostgreSQL:

```sql
SET datafusion.execution.time_zone = 'America/Denver';

SELECT arrow_typeof('2024-01-01T12:00:00Z'::timestamptz) AS type,
       '2024-01-01T12:00:00Z'::timestamptz AS value;
```

```text
+---------------------------------+---------------------------+
| type                            | value                     |
+---------------------------------+---------------------------+
| Timestamp(ns, "America/Denver") | 2024-01-01T05:00:00-07:00 |
+---------------------------------+---------------------------+
```

DuckDB and PostgreSQL agree with each other here. Each of the two systems
always has a session time zone, and the default value is the local time zone of
the machine. As a result, `TIMESTAMPTZ` in those systems is always an aware
type:

| System     | Default time zone of the session | `'2024-01-01T12:00:00Z'::timestamptz`       |
| ---------- | -------------------------------- | ------------------------------------------- |
| DataFusion | not set                          | `Timestamp(ns)` — naive, the `Z` is removed |
| PostgreSQL | the machine's time zone          | `timestamp with time zone` — aware          |
| DuckDB     | the machine's time zone          | `TIMESTAMP WITH TIME ZONE` — aware          |

There is also a difference in the data model. In DuckDB, `TIMESTAMP WITH TIME ZONE` is one type, and a value of that type has no time zone of its own. The
session time zone controls the display of each value. In DataFusion, each value
keeps its own time zone in its Arrow type. DataFusion can hold two values in
two different time zones in the same query. DuckDB cannot do this.

:::{note}
Set `datafusion.execution.time_zone` if the time zone of your results is
important. `'UTC'` is a good value. It makes `timestamptz` an aware type, it
makes `now()` aware, and it gives each conversion on this page an offset of
zero.
:::

## The one rule

One rule controls each conversion between the two types. The rule has three
directions.

### From naive to aware: a **shift**

DataFusion reads the naive wall clock as a local time _in the target time zone_.
The wall clock stays the same. The instant changes.

```sql
SELECT
  TIMESTAMP '2024-01-01 12:00:00' AS naive,
  arrow_cast(TIMESTAMP '2024-01-01 12:00:00',
             'Timestamp(Second, Some("America/Denver"))') AS zoned,
  to_unixtime(TIMESTAMP '2024-01-01 12:00:00') AS naive_epoch,
  to_unixtime(arrow_cast(TIMESTAMP '2024-01-01 12:00:00',
             'Timestamp(Second, Some("America/Denver"))')) AS zoned_epoch;
```

```text
+---------------------+---------------------------+-------------+-------------+
| naive               | zoned                     | naive_epoch | zoned_epoch |
+---------------------+---------------------------+-------------+-------------+
| 2024-01-01T12:00:00 | 2024-01-01T12:00:00-07:00 | 1704110400  | 1704135600  |
+---------------------+---------------------------+-------------+-------------+
```

The wall clock stays `12:00:00`. But the epoch moved 25200 seconds, which is
the `-07:00` offset. This direction causes many errors, because the integer
changes although the printed value does not change.

### From aware to aware: a **relabel**

The instant stays the same. Only the display annotation changes.

```sql
CREATE OR REPLACE VIEW utc AS
  SELECT arrow_cast(TIMESTAMP '2024-01-01 12:00:00',
                    'Timestamp(Second, Some("UTC"))') AS t;

SELECT t AS in_utc,
       arrow_cast(t, 'Timestamp(Second, Some("America/Denver"))') AS in_denver,
       to_unixtime(t) AS utc_epoch,
       to_unixtime(arrow_cast(t, 'Timestamp(Second, Some("America/Denver"))')) AS denver_epoch
FROM utc;
```

```text
+----------------------+---------------------------+------------+--------------+
| in_utc               | in_denver                 | utc_epoch  | denver_epoch |
+----------------------+---------------------------+------------+--------------+
| 2024-01-01T12:00:00Z | 2024-01-01T05:00:00-07:00 | 1704110400 | 1704110400   |
+----------------------+---------------------------+------------+--------------+
```

### From aware to naive: the **UTC** wall clock

DataFusion removes the `tz` annotation and keeps the integer. As a result, the
new wall clock is the wall clock of the value **in UTC**. The source annotation
and the session time zone do not change this result.

```sql
SELECT arrow_cast(t, 'Timestamp(Second, None)') AS zoned_to_naive FROM utc;

SELECT arrow_cast(arrow_cast(t, 'Timestamp(Second, Some("America/Denver"))'),
                  'Timestamp(Second, None)') AS denver_to_naive FROM utc;
```

```text
+---------------------+
| zoned_to_naive      |
+---------------------+
| 2024-01-01T12:00:00 |
+---------------------+

+---------------------+
| denver_to_naive     |
+---------------------+
| 2024-01-01T12:00:00 |
+---------------------+
```

The two results are `12:00:00`, which is the UTC wall clock. This is correct
although the second value shows `05:00:00-07:00`. PostgreSQL is different: it
converts to the _session_ time zone. Refer to
[Differences from PostgreSQL](#differences-from-postgresql).

Use [`to_local_time`](scalar_functions.md#to_local_time) to get a local wall
clock. Refer to [Examples](#examples).

### Summary

| Conversion         | What stays the same | What changes                  |
| ------------------ | ------------------- | ----------------------------- |
| naive &rarr; aware | the wall clock      | the instant (it shifts)       |
| aware &rarr; aware | the instant         | the display annotation        |
| aware &rarr; naive | the instant         | it becomes the UTC wall clock |

### `AT TIME ZONE`

`AT TIME ZONE` uses the same rule. The type of the input selects the direction:

- For a **naive** input, `AT TIME ZONE` does the naive-to-aware shift. It reads
  the wall clock as a local time in the time zone that you give.
- For an **aware** input, `AT TIME ZONE` does the aware-to-aware relabel. It
  keeps the instant and replaces the display annotation.

```sql
CREATE OR REPLACE VIEW utc AS
  SELECT arrow_cast(TIMESTAMP '2024-01-01 12:00:00',
                    'Timestamp(Second, Some("UTC"))') AS t;

SELECT arrow_typeof(TIMESTAMP '2024-01-01 12:00:00' AT TIME ZONE 'America/Denver') AS naive_input_type,
       TIMESTAMP '2024-01-01 12:00:00' AT TIME ZONE 'America/Denver'               AS naive_input;

SELECT arrow_typeof(t AT TIME ZONE 'America/Denver') AS zoned_input_type,
       t AT TIME ZONE 'America/Denver'               AS zoned_input,
       (t AT TIME ZONE 'America/Denver')::timestamp  AS then_cast_to_naive
FROM utc;
```

```text
+---------------------------------+---------------------------+
| naive_input_type                | naive_input               |
+---------------------------------+---------------------------+
| Timestamp(ns, "America/Denver") | 2024-01-01T12:00:00-07:00 |
+---------------------------------+---------------------------+

+---------------------------------+---------------------------+---------------------+
| zoned_input_type                | zoned_input               | then_cast_to_naive  |
+---------------------------------+---------------------------+---------------------+
| Timestamp(ns, "America/Denver") | 2024-01-01T05:00:00-07:00 | 2024-01-01T12:00:00 |
+---------------------------------+---------------------------+---------------------+
```

For a naive input, DataFusion and PostgreSQL agree. The two systems give the
instant `2024-01-01T19:00:00Z`. DataFusion shows it in Denver, and PostgreSQL
shows it in the session time zone.

For an aware input, the two systems do not agree. PostgreSQL and DuckDB give a
**naive** `2024-01-01 05:00:00`. As a result,
`(t AT TIME ZONE 'America/Denver')::timestamp` gives `05:00:00` in those
systems. In DataFusion, it gives `12:00:00`, which is the UTC wall clock. Refer
to <https://github.com/apache/datafusion/issues/12218>. This behavior can
change.

### Places where DataFusion does not use the rule

The rule above tells you how the cast kernel operates. But some parts of
DataFusion use a different rule. In that different rule, a naive value is a UTC
value, and a new time zone is only a relabel. You can see this difference from
SQL today. This is the most clear example:

```sql
CREATE OR REPLACE TABLE c AS SELECT TIMESTAMP '2024-01-01 12:00:00' AS ts;

-- 2024-01-01 12:00 in Denver is 2024-01-01T19:00:00Z (epoch 1704135600)
SELECT arrow_cast(ts, 'Timestamp(Second, Some("America/Denver"))') AS ts_in_denver FROM c;

-- literal on both sides: the naive value is shifted, so this is true
SELECT TIMESTAMP '2024-01-01 12:00:00'
       = arrow_cast(1704135600, 'Timestamp(Second, Some("America/Denver"))') AS literal_cmp;

-- the same comparison with the naive value in a column: false
SELECT ts = arrow_cast(1704135600, 'Timestamp(Second, Some("America/Denver"))') AS column_cmp FROM c;
```

```text
+---------------------------+
| ts_in_denver              |
+---------------------------+
| 2024-01-01T12:00:00-07:00 |
+---------------------------+

+-------------+
| literal_cmp |
+-------------+
| true        |
+-------------+

+------------+
| column_cmp |
+------------+
| false      |
+------------+
```

The cast in the first query shifts the value. The `unwrap_cast_in_comparison`
optimizer rule changes the comparison in the third query. That rule removes the
cast and keeps the integer of the literal, which is a relabel. Note that
`datafusion.execution.time_zone` has no value here, and it has no effect in this
example. The two queries disagree only because one path shifts and the other
path relabels.

Until there is a correction for
<https://github.com/apache/datafusion/issues/25095>, obey these two rules:

- Write each conversion in the SQL with `arrow_cast`.
- Compare aware values only with other aware values.

## The session time zone

`datafusion.execution.time_zone` is the time zone that DataFusion uses when it
must select one. Refer to [Configuration Settings](../configs.md). The default
value is **not set**.

This setting changes:

- The Arrow type of `TIMESTAMP WITH TIME ZONE` and `::timestamptz`.
- The type and the value of `now()`, `current_timestamp`, `current_date` and
  `current_time`.
- The time zone that `to_timestamp` and the `to_timestamp_*` functions give.
- The time zone that these functions use for a string argument that has no time
  zone.

This setting does not change:

- The meaning of a `Timestamp(unit, Some(tz))` value that is in memory. A
  column from a Parquet file, or a column from `AT TIME ZONE`, keeps its own
  time zone.
- `from_unixtime`, which always gives the UTC wall clock as a naive value. Refer
  to <https://github.com/apache/datafusion/issues/12892>.
- `date_part` and `EXTRACT` on a naive value. These functions give the wall
  clock that is in memory, and they never read it again in a different time
  zone. Refer to <https://github.com/apache/datafusion/issues/18228>.
- A cast from aware to naive, which always gives the UTC wall clock.

```sql
SET datafusion.execution.time_zone = 'America/Denver';

SELECT arrow_typeof(now())               AS now,
       arrow_typeof(current_timestamp)   AS current_timestamp,
       arrow_typeof(to_timestamp('2024-01-01T12:00:00')) AS to_timestamp,
       arrow_typeof(from_unixtime(1704110400))           AS from_unixtime;

SELECT to_timestamp('2024-01-01T12:00:00') AS to_timestamp_naive_string,
       from_unixtime(1704110400)           AS from_unixtime_value;
```

```text
+---------------------------------+---------------------------------+---------------------------------+---------------+
| now                             | current_timestamp               | to_timestamp                    | from_unixtime |
+---------------------------------+---------------------------------+---------------------------------+---------------+
| Timestamp(ns, "America/Denver") | Timestamp(ns, "America/Denver") | Timestamp(ns, "America/Denver") | Timestamp(s)  |
+---------------------------------+---------------------------------+---------------------------------+---------------+

+---------------------------+---------------------+
| to_timestamp_naive_string | from_unixtime_value |
+---------------------------+---------------------+
| 2024-01-01T12:00:00-07:00 | 2024-01-01T12:00:00 |
+---------------------------+---------------------+
```

Note the difference. `to_timestamp` read the string `'2024-01-01T12:00:00'` as
`12:00` _local_ Denver time. But `from_unixtime` gave the UTC wall clock as a
naive value for the same epoch.

### The risk when the session time zone has no value

If `datafusion.execution.time_zone` has no value, `::timestamptz` **removes**
the time zone of an aware value. It does not convert to a time zone. The cause
is the target type, which is `Timestamp(_, None)`. DataFusion uses the
aware-to-naive rule: it keeps the integer, and it removes the annotation.

If you put `::timestamptz` around a correct expression, you change the meaning
of that expression to UTC. There is no warning:

```sql
CREATE OR REPLACE TABLE hits(t TIMESTAMP) AS VALUES
  (TIMESTAMP '2024-04-30T21:30:00'), (TIMESTAMP '2024-04-30T22:30:00'),
  (TIMESTAMP '2024-04-30T23:30:00'), (TIMESTAMP '2024-05-01T00:00:00'),
  (TIMESTAMP '2024-05-01T00:30:00'), (TIMESTAMP '2024-05-01T10:30:00'),
  (TIMESTAMP '2024-05-01T20:30:00');
CREATE OR REPLACE VIEW hits_utc AS SELECT t AT TIME ZONE 'UTC' AS t FROM hits;

-- correct: group on the aware value
SELECT date_trunc('day', t AT TIME ZONE 'Europe/Brussels') AS day, count(*) AS n
FROM hits_utc GROUP BY 1 ORDER BY 1;

-- wrong: ::timestamptz with datafusion.execution.time_zone unset
SELECT date_trunc('day', (t AT TIME ZONE 'Europe/Brussels')::timestamptz) AS day, count(*) AS n
FROM hits_utc GROUP BY 1 ORDER BY 1;
```

```text
+---------------------------+---+
| day                       | n |
+---------------------------+---+
| 2024-04-30T00:00:00+02:00 | 1 |
| 2024-05-01T00:00:00+02:00 | 6 |
+---------------------------+---+

+---------------------+---+
| day                 | n |
+---------------------+---+
| 2024-04-30T00:00:00 | 3 |
| 2024-05-01T00:00:00 | 4 |
+---------------------+---+
```

The second query groups by the UTC day, although the SQL gives the name of
Brussels. Refer to <https://github.com/apache/datafusion/issues/13962>.

### How to combine values from different time zones

You can put two timestamps of different types together in a comparison, a
`UNION`, a `CASE` or a `coalesce`. DataFusion then selects a common type:

| Left        | Right         | Common type                                |
| ----------- | ------------- | ------------------------------------------ |
| `Some(tz)`  | the same `tz` | that `tz`                                  |
| `Some(tz1)` | `Some(tz2)`   | `Some("UTC")` (both are relabeled)         |
| `None`      | `Some(tz)`    | `Some(tz)` — the naive side is **shifted** |

The common type of two _different_ time zones is UTC. It is not the session time
zone. As a result, a `UNION` of Denver data and Brussels data shows UTC. There
is no loss of data, because all three rows keep each instant.

## Local time and the instant

Some functions use the **local wall clock** of the value. The local wall clock
is the reading of the instant in the `tz` of the value. Other functions use the
**UTC instant**, and they use the annotation only for display.

The group of a function is important. It makes the difference between a correct
query and an incorrect query. This page gives the group of each function.

For an aware value, "local" is the _own_ time zone of the value. It is not the
session time zone. A naive value has no time zone. For a naive value, the local
wall clock is the reading that is in memory. The UTC instant is that same
reading as UTC.

| Function / operator                                                 | Operates on         | Notes                                                                                |
| ------------------------------------------------------------------- | ------------------- | ------------------------------------------------------------------------------------ |
| [`date_trunc`](scalar_functions.md#date_trunc), `datetrunc`         | local wall clock    | Truncates to local midnight, local hour, …                                           |
| [`date_part`](scalar_functions.md#date_part), `datepart`, `EXTRACT` | local wall clock    |                                                                                      |
| [`to_char`](scalar_functions.md#to_char), `date_format`             | local wall clock    |                                                                                      |
| [`to_local_time`](scalar_functions.md#to_local_time)                | local wall clock    | Returns `Timestamp(unit, None)` that holds the local reading                         |
| `CAST(t AS DATE)`, `CAST(t AS TIME)`                                | local wall clock    |                                                                                      |
| `timestamp + interval`, `timestamp - interval`                      | local wall clock    | Calendar units obey DST; refer to the section below                                  |
| [`generate_series`](scalar_functions.md#generate_series) / `range`  | local wall clock    | Steps by calendar units in the own time zone of the value; nanosecond precision only |
| [`date_bin`](scalar_functions.md#date_bin)                          | **the UTC instant** | The bin origin is the UTC epoch, not local midnight                                  |
| [`to_unixtime`](scalar_functions.md#to_unixtime)                    | **the UTC instant** |                                                                                      |
| `AT TIME ZONE`, `CAST(t AS TIMESTAMPTZ)`                            | **the UTC instant** | For an aware input; DataFusion shifts a naive input, see above                       |
| Comparison, `ORDER BY`, `GROUP BY`, joins, `min`/`max`              | **the UTC instant** | The annotations have no effect; equal instants are equal                             |

Remember this result: `date_trunc` and `date_bin` do not agree on the same
value.

```sql
CREATE OR REPLACE VIEW v AS
  SELECT arrow_cast(TIMESTAMP '2024-01-01 12:00:00',
                    'Timestamp(Second, Some("America/Denver"))') AS t;

SELECT t,
       date_trunc('day', t) AS date_trunc,
       date_bin(INTERVAL '1 day', t) AS date_bin
FROM v;
```

```text
+---------------------------+---------------------------+---------------------------+
| t                         | date_trunc                | date_bin                  |
+---------------------------+---------------------------+---------------------------+
| 2024-01-01T12:00:00-07:00 | 2024-01-01T00:00:00-07:00 | 2023-12-31T17:00:00-07:00 |
+---------------------------+---------------------------+---------------------------+
```

`date_trunc` gave local midnight in Denver. `date_bin` gave the start of the UTC
day, and it shows that instant in Denver. The value `2023-12-31T17:00:00-07:00`
is the same instant as `2024-01-01T00:00:00Z`.

Each function agrees with PostgreSQL. But the difference between the two
functions is not easy to see. Use `date_trunc` for local calendar limits. For
local calendar bins with `date_bin`, refer to [Examples](#examples).

These are the other functions on an aware value:

```sql
SELECT date_part('hour', t)  AS date_part_hour,
       to_char(t, '%H:%M')   AS to_char,
       to_local_time(t)      AS to_local_time,
       t::date               AS cast_to_date
FROM v;
```

```text
+----------------+---------+---------------------+--------------+
| date_part_hour | to_char | to_local_time       | cast_to_date |
+----------------+---------+---------------------+--------------+
| 12             | 12:00   | 2024-01-01T12:00:00 | 2024-01-01   |
+----------------+---------+---------------------+--------------+
```

## Daylight saving time

### `INTERVAL '1 day'` is different from `INTERVAL '24 hours'`

This is the most important rule on this page. For an **aware** timestamp,
DataFusion adds calendar units in the local calendar of the value. Calendar
units are years, months and days.

DataFusion adds units that are less than one day as elapsed time. These units
are hours, minutes and seconds. At a DST transition, the two types of unit give
different results:

```sql
CREATE OR REPLACE VIEW d AS
  SELECT arrow_cast(TIMESTAMP '2024-03-09 12:00:00',
                    'Timestamp(Second, Some("America/Denver"))') AS t;

SELECT t,
       t + INTERVAL '1 day'    AS plus_1_day,
       t + INTERVAL '24 hours' AS plus_24_hours,
       to_unixtime(t + INTERVAL '1 day')    - to_unixtime(t) AS seconds_1_day,
       to_unixtime(t + INTERVAL '24 hours') - to_unixtime(t) AS seconds_24_hours
FROM d;
```

```text
+---------------------------+---------------------------+---------------------------+---------------+------------------+
| t                         | plus_1_day                | plus_24_hours             | seconds_1_day | seconds_24_hours |
+---------------------------+---------------------------+---------------------------+---------------+------------------+
| 2024-03-09T12:00:00-07:00 | 2024-03-10T12:00:00-06:00 | 2024-03-10T13:00:00-06:00 | 82800         | 86400            |
+---------------------------+---------------------------+---------------------------+---------------+------------------+
```

The clocks in America/Denver move forward on 2024-03-10. As a result, that local
day has 23 hours. `INTERVAL '1 day'` gives the same wall clock on the next day, which is
82800 seconds later. `INTERVAL '24 hours'` gives an instant 24 hours later,
which is one hour later on the clock.

In the autumn, the same two units give the opposite result. If you add
`INTERVAL '1 day'` to `2024-11-02 12:00:00-06:00`, the instant moves 90000
seconds.

This behavior agrees with PostgreSQL 17. A **naive** timestamp has no DST. For a
naive timestamp, `INTERVAL '1 day'` and `INTERVAL '24 hours'` always give the
same result.

Use calendar units for "the same time tomorrow". Use units of less than one day
for "24 hours of elapsed time". The two types of unit are not the same.

### Local times that are ambiguous or do not exist

In a time zone with DST, two local wall clocks each year do not identify one
instant. The hour that the clocks skip in the spring does not exist. The hour
that the clocks repeat in the autumn is ambiguous.

DataFusion gives an **error** for the two hours. It gives an error for a literal
and also for a column:

```sql
SET datafusion.execution.time_zone = 'America/Denver';
CREATE OR REPLACE TABLE gap AS SELECT TIMESTAMP '2024-03-10 02:30:00' AS ts;
SELECT ts::timestamptz FROM gap;
```

```text
Arrow error: Cast error: Cannot cast timezone to different timezone
```

```sql
SET datafusion.execution.time_zone = 'America/Denver';
SELECT '2024-03-10T02:30:00'::timestamptz;
```

```text
Optimizer rule 'simplify_expressions' failed
caused by
Arrow error: Parser error: Error parsing timestamp from '2024-03-10T02:30:00': error computing timezone offset
```

The ambiguous hour in the autumn gives the same errors. In America/Denver, that
hour is `2024-11-03 01:30:00`. PostgreSQL does not give an error. It gives a
result for the two hours: it moves a time in the gap forward, and it reads the
ambiguous hour as standard time.

A time zone with a fixed offset, such as `'+08:00'`, has no transitions. It
cannot give these errors. Refer to
<https://github.com/apache/datafusion/issues/25084> and to the correction in
<https://github.com/apache/arrow-rs/pull/11038>.

## Examples

### How to group UTC data by the local calendar day

`date_bin` uses the UTC instant. If you give aware data to `date_bin`, you get
UTC days. Do these three steps:

1. Convert the data to the target time zone.
2. Make the result a local wall clock with `to_local_time`.
3. Give that local wall clock to `date_bin`.

```sql
CREATE OR REPLACE TABLE hits(t TIMESTAMP) AS VALUES
  (TIMESTAMP '2024-04-30T21:30:00'), (TIMESTAMP '2024-04-30T22:30:00'),
  (TIMESTAMP '2024-04-30T23:30:00'), (TIMESTAMP '2024-05-01T00:00:00'),
  (TIMESTAMP '2024-05-01T00:30:00'), (TIMESTAMP '2024-05-01T10:30:00'),
  (TIMESTAMP '2024-05-01T20:30:00');

CREATE OR REPLACE VIEW hits_utc AS SELECT t AT TIME ZONE 'UTC' AS t FROM hits;

SELECT t AT TIME ZONE 'Europe/Brussels' AS local,
       date_bin(INTERVAL '1 day',
                to_local_time(t AT TIME ZONE 'Europe/Brussels')) AS local_day
FROM hits_utc;
```

```text
+---------------------------+---------------------+
| local                     | local_day           |
+---------------------------+---------------------+
| 2024-04-30T23:30:00+02:00 | 2024-04-30T00:00:00 |
| 2024-05-01T00:30:00+02:00 | 2024-05-01T00:00:00 |
| 2024-05-01T01:30:00+02:00 | 2024-05-01T00:00:00 |
| 2024-05-01T02:00:00+02:00 | 2024-05-01T00:00:00 |
| 2024-05-01T02:30:00+02:00 | 2024-05-01T00:00:00 |
| 2024-05-01T12:30:00+02:00 | 2024-05-01T00:00:00 |
| 2024-05-01T22:30:00+02:00 | 2024-05-01T00:00:00 |
+---------------------------+---------------------+
```

`local_day` is a naive value. It is a label for a local calendar day, and it is
not an instant. This is the correct type for a `GROUP BY` key.

For a whole calendar unit, `date_trunc('day', t AT TIME ZONE 'Europe/Brussels')`
gives the same groups. It also keeps the result aware:

```sql
SELECT date_trunc('day', t AT TIME ZONE 'Europe/Brussels') AS day, count(*) AS n
FROM hits_utc GROUP BY 1 ORDER BY 1;
```

```text
+---------------------------+---+
| day                       | n |
+---------------------------+---+
| 2024-04-30T00:00:00+02:00 | 1 |
| 2024-05-01T00:00:00+02:00 | 6 |
+---------------------------+---+
```

Use the `to_local_time` form if the width of the bin is not a whole calendar
unit. `date_trunc` cannot give `INTERVAL '15 minutes'` or `INTERVAL '4 hours'`.

:::{warning}
Do not give `date_bin` an origin in the target time zone. For example, do not
use
`date_bin(INTERVAL '1 day', t, TIMESTAMP '2024-04-30 22:00:00' AT TIME ZONE 'UTC')`.
The result is correct only while the offset of the time zone stays the same. At
a DST transition, the result moves one hour, and there is no warning. Use
`to_local_time` instead.
:::

### How to find the local wall clock of a time zone

```sql
SELECT to_local_time(t AT TIME ZONE 'Europe/Brussels') AS brussels_wall_clock
FROM hits_utc LIMIT 1;
```

`AT TIME ZONE` relabels the instant for display in Brussels. Then
`to_local_time` makes a naive value from that display. Do **not** use
`::timestamp` for this task. `::timestamp` gives the UTC wall clock, not the
local wall clock.

### How to convert safely

- Keep instants as `Timestamp(unit, Some("UTC"))`. Convert to a display time
  zone only at the end of the query.
- Set `datafusion.execution.time_zone = 'UTC'`. Then `timestamptz`, `now()` and
  `to_timestamp` are aware, and each conversion on this page has an offset of
  zero.
- Use `arrow_cast` with an Arrow type if you want an exact conversion. The
  result of `::timestamptz` changes with the session configuration.
- Do not convert an aware value through `Timestamp(_, None)`, unless you want to
  make it a UTC wall clock.

## Differences from PostgreSQL

DataFusion follows PostgreSQL for most of the behavior above. These are the
known differences today.

| Behavior                                               | DataFusion                                                                                           | PostgreSQL                                                                   | Issue                                               |
| ------------------------------------------------------ | ---------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- | --------------------------------------------------- |
| `TIMESTAMP WITH TIME ZONE` with no session zone set    | **Naive** `Timestamp(_, None)`; DataFusion removes the offset in a literal                           | Always **aware**; `TimeZone` always has a value                              | —                                                   |
| `AT TIME ZONE` used on an **aware** value              | Gives an **aware** value in the given time zone                                                      | Gives a **naive** value                                                      | <https://github.com/apache/datafusion/issues/12218> |
| `tstz::timestamp`                                      | The **UTC** wall clock                                                                               | The **session zone** wall clock                                              | <https://github.com/apache/datafusion/issues/12218> |
| **Naive** value compared with an **aware** value       | DataFusion shifts the naive side into the zone of the **other operand**, not the session zone        | PostgreSQL reads the naive side in the **session** zone                      | <https://github.com/apache/datafusion/issues/13212> |
| **Naive** column compared with an **aware** literal    | The optimizer removes the shift. The answer is the opposite of the same comparison with two literals | The same answer as the literal form                                          | <https://github.com/apache/datafusion/issues/25095> |
| `tstz - timestamp`                                     | Naive side read as UTC, so the session offset is lost                                                | Naive side read in the session zone                                          | <https://github.com/apache/datafusion/issues/13212> |
| Ambiguous / nonexistent local time                     | Error                                                                                                | Resolved (gap moves forward, ambiguity reads as standard time)               | <https://github.com/apache/datafusion/issues/25084> |
| `from_unixtime`                                        | Gives the UTC wall clock as a **naive** value. The session zone has no effect                        | `to_timestamp(double)` returns `timestamptz`                                 | <https://github.com/apache/datafusion/issues/12892> |
| `date_part` / `EXTRACT` on a **naive** value           | Uses the wall clock in memory. The session zone has no effect                                        | The same — but DataFusion's config docs once promised session-zone awareness | <https://github.com/apache/datafusion/issues/18228> |
| `to_timestamp_*` on an input that is already **aware** | Replaces the zone with the session zone. If the session zone has no value, the zone is removed       | n/a                                                                          | <https://github.com/apache/datafusion/issues/23841> |

:::{note}
The `AT TIME ZONE` row shows the behavior of DataFusion today. The DataFusion
community discusses a change in
<https://github.com/apache/datafusion/issues/12218>. If the community makes that
change, the **aware** input will agree with PostgreSQL and DuckDB. The second
row and the third row above will also change. DuckDB also gives a naive value
there. The **naive** input agrees with PostgreSQL now, because the two systems
give the same instant.
:::

## See also

- [Data Types](data_types.md)
- [Date and Time Functions](scalar_functions.md#time-and-date-functions)
- [Configuration Settings](../configs.md)
