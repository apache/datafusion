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

Almost every surprising result involving timestamps in DataFusion comes down to a
single question: when a timestamp gains or loses a time zone, does the _instant_
stay the same or does the _wall clock_ stay the same? This page answers that
question once, and then applies the answer to casts, the session time zone, the
date/time functions, daylight saving time, and a handful of recipes.

All output on this page was produced with `datafusion-cli` on DataFusion 55.0.0
with default settings unless a `SET` statement says otherwise.

## The data model

DataFusion timestamps are Arrow timestamps. Arrow has exactly two timestamp
shapes, and the difference between them is the whole story:

| Arrow type                  | Physical value            | Meaning                                                                                                   |
| --------------------------- | ------------------------- | --------------------------------------------------------------------------------------------------------- |
| `Timestamp(unit, Some(tz))` | offset from the UTC epoch | An **instant**. `tz` is a display annotation: it says how to render the instant, not what the instant is. |
| `Timestamp(unit, None)`     | a wall-clock reading      | A **wall clock** with no instant attached. There is no fact about which point in time it names.           |

Two consequences follow, and they explain most of the rest of this page.

- A zone-aware timestamp's `tz` **is not data**. Two values with the same
  integer and different `tz` annotations are the _same instant_; comparing,
  sorting, grouping or joining them treats them as equal.
- A zone-naive timestamp has **no instant**, so any operation that needs one has
  to invent a zone. Which zone gets invented is the subject of
  [The session time zone](#the-session-time-zone) below, and it is not always
  the same zone.

### How SQL types map to Arrow types

| SQL type                                                   | Arrow type                                                    |
| ---------------------------------------------------------- | ------------------------------------------------------------- |
| `TIMESTAMP`, `TIMESTAMP WITHOUT TIME ZONE`, `::timestamp`  | `Timestamp(Nanosecond, None)`                                 |
| `TIMESTAMP WITH TIME ZONE`, `TIMESTAMPTZ`, `::timestamptz` | `Timestamp(Nanosecond, Some(datafusion.execution.time_zone))` |

`TIMESTAMP(p)` with `p` of 0, 3, 6 or 9 selects second, millisecond,
microsecond or nanosecond precision respectively.

The second row is the important one, and it is where DataFusion parts company
with PostgreSQL. `TIMESTAMP WITH TIME ZONE` resolves to whatever
`datafusion.execution.time_zone` is set to — and that setting **defaults to
unset**. With it unset, `TIMESTAMP WITH TIME ZONE` is a zone-_naive_ type:

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

The `Z` was accepted and then discarded. PostgreSQL has no equivalent state: its
`TimeZone` parameter is always set to something, so `timestamptz` is always a
zone-aware type there. Set the session time zone and the same query behaves the
way a PostgreSQL user expects:

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

:::{note}
If you are writing SQL that must behave predictably, set
`datafusion.execution.time_zone` explicitly. `'UTC'` is a good default:
it makes `timestamptz` a genuinely zone-aware type, makes `now()` zone-aware,
and keeps every conversion on this page a no-op shift.
:::

## The one rule

Every conversion between the two shapes follows from one rule, applied in three
directions.

### Zone-naive to zone-aware: a **shift**

The naive wall clock is read as a local time _in the target zone_. The wall
clock is preserved; the instant changes.

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

The wall clock is still `12:00:00`; the epoch moved by 25200 seconds, the
`-07:00` offset. This is the direction that trips people up, because the
underlying integer changed even though nothing about the printed value did.

### Zone-aware to zone-aware: a **relabel**

The instant is preserved; only the display annotation changes.

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

### Zone-aware to zone-naive: the **UTC** wall clock

The zone annotation is dropped and the integer is kept, which means the
resulting wall clock is the value's wall clock **in UTC** — regardless of what
the source annotation was and regardless of the session time zone.

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

Both give `12:00:00`, the UTC wall clock, even though the second value displays
as `05:00:00-07:00`. PostgreSQL instead converts to the _session_ time zone
here; see [Differences from PostgreSQL](#differences-from-postgresql).

To get a local wall clock instead, use
[`to_local_time`](scalar_functions.md#to_local_time) — see
[Recipes](#recipes).

### Summary

| Conversion         | What is preserved | What changes               |
| ------------------ | ----------------- | -------------------------- |
| naive &rarr; zoned | wall clock        | the instant (shifted)      |
| zoned &rarr; zoned | the instant       | the display annotation     |
| zoned &rarr; naive | the instant       | becomes the UTC wall clock |

### `AT TIME ZONE`

`AT TIME ZONE` applies the same rule, chosen by the input's shape:

- On a zone-**naive** input it is the naive &rarr; zoned shift: the wall clock
  is read as local time in the named zone.
- On a zone-**aware** input it is the zoned &rarr; zoned relabel: the instant is
  kept and the display annotation is replaced.

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

The zone-naive case agrees with PostgreSQL: both name the instant
`2024-01-01T19:00:00Z`, DataFusion displaying it in Denver and PostgreSQL in the
session zone.

The zone-aware case does not. PostgreSQL and DuckDB both return a zone-**naive**
`2024-01-01 05:00:00` there, so `(t AT TIME ZONE 'America/Denver')::timestamp`
gives `05:00:00` in those systems and `12:00:00` — the UTC wall clock — in
DataFusion. See <https://github.com/apache/datafusion/issues/12218>; this is
under active discussion and may change.

### Where the rule is not applied consistently

The rule above describes the cast kernel. Some parts of DataFusion take the
other reading — that a naive value is "really" UTC and gaining a zone is a
relabel — and that inconsistency is visible from SQL today. The clearest case:

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

The cast in the first query shifts. The comparison in the third query is
rewritten by the `unwrap_cast_in_comparison` optimizer rule, which removes the
cast and keeps the literal's integer unchanged — a relabel. Note that
`datafusion.execution.time_zone` is not set here and plays no part: the two
queries disagree purely because one path shifts and the other relabels. Until
<https://github.com/apache/datafusion/issues/25095> is fixed, prefer to make the
conversion explicit and to compare zone-aware values against zone-aware values.

## The session time zone

`datafusion.execution.time_zone` (see [Configuration Settings](../configs.md))
is the zone DataFusion uses when it has to invent one. It **defaults to unset**.

What it affects:

- The Arrow type that `TIMESTAMP WITH TIME ZONE` / `::timestamptz` resolves to.
- The type and value returned by `now()`, `current_timestamp`, `current_date`
  and `current_time`.
- The zone that `to_timestamp` and the `to_timestamp_*` family produce, and the
  zone that a zone-less string argument to them is interpreted in.

What it does **not** affect:

- The meaning of a `Timestamp(unit, Some(tz))` value that already exists. A
  column read from Parquet or created by `AT TIME ZONE` keeps its own zone.
- `from_unixtime`, which always produces a zone-naive UTC wall clock
  (<https://github.com/apache/datafusion/issues/12892>).
- `date_part` / `EXTRACT` on a zone-naive value, which reports that value's
  stored wall clock and never reinterprets it
  (<https://github.com/apache/datafusion/issues/18228>).
- Zone-aware &rarr; zone-naive casts, which always produce the UTC wall clock.

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

Note that `to_timestamp` read the zone-less string `'2024-01-01T12:00:00'` as
`12:00` _local_ Denver time, while `from_unixtime` returned a naive UTC wall
clock for the same epoch.

### The default-unset footgun

With `datafusion.execution.time_zone` unset, `::timestamptz` **erases** an
existing zone rather than converting to one, because the target type is
`Timestamp(_, None)` and the conversion is the zone-aware &rarr; zone-naive
rule: keep the integer, drop the annotation. Wrapping a correct expression in
`::timestamptz` therefore silently changes its meaning to UTC:

```sql
CREATE OR REPLACE TABLE hits(t TIMESTAMP) AS VALUES
  (TIMESTAMP '2024-04-30T21:30:00'), (TIMESTAMP '2024-04-30T22:30:00'),
  (TIMESTAMP '2024-04-30T23:30:00'), (TIMESTAMP '2024-05-01T00:00:00'),
  (TIMESTAMP '2024-05-01T00:30:00'), (TIMESTAMP '2024-05-01T10:30:00'),
  (TIMESTAMP '2024-05-01T20:30:00');
CREATE OR REPLACE VIEW hits_utc AS SELECT t AT TIME ZONE 'UTC' AS t FROM hits;

-- correct: group on the zone-aware value
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

The second query groups by UTC day even though it names Brussels. See
<https://github.com/apache/datafusion/issues/13962>.

### Combining values from different zones

When two timestamps of different types meet — in a comparison, a `UNION`, a
`CASE`, a `coalesce` — DataFusion picks a common type:

| Left        | Right         | Common type                                |
| ----------- | ------------- | ------------------------------------------ |
| `Some(tz)`  | the same `tz` | that `tz`                                  |
| `Some(tz1)` | `Some(tz2)`   | `Some("UTC")` (both are relabelled)        |
| `None`      | `Some(tz)`    | `Some(tz)` — the naive side is **shifted** |

Because the common type of two _different_ named zones is UTC and not the
session zone, a `UNION` of Denver and Brussels data displays as UTC. Nothing is
lost: all three cases preserve every instant.

## Local time versus the instant

Some functions work on the value's **local wall clock** — the reading you get
after applying the value's own `tz` annotation. Others work on the **UTC
instant** and ignore the annotation except for display. Which group a function
falls into is rarely stated anywhere, and it is the difference between a correct
and an incorrect query, so here it is explicitly.

For a zone-aware value, "local" means the value's _own_ zone, not the session
zone. For a zone-naive value there is no zone to apply: the local-wall-clock
functions use the stored reading as-is, and the instant-based functions treat it
as UTC.

| Function / operator                                                 | Operates on         | Notes                                                                              |
| ------------------------------------------------------------------- | ------------------- | ---------------------------------------------------------------------------------- |
| [`date_trunc`](scalar_functions.md#date_trunc), `datetrunc`         | local wall clock    | Truncates to local midnight, local hour, …                                         |
| [`date_part`](scalar_functions.md#date_part), `datepart`, `EXTRACT` | local wall clock    |                                                                                    |
| [`to_char`](scalar_functions.md#to_char), `date_format`             | local wall clock    |                                                                                    |
| [`to_local_time`](scalar_functions.md#to_local_time)                | local wall clock    | Returns `Timestamp(unit, None)` holding the local reading                          |
| `CAST(t AS DATE)`, `CAST(t AS TIME)`                                | local wall clock    |                                                                                    |
| `timestamp + interval`, `timestamp - interval`                      | local wall clock    | Calendar units are DST-aware; see below                                            |
| [`generate_series`](scalar_functions.md#generate_series) / `range`  | local wall clock    | Steps by calendar units in the value's own zone; accepts nanosecond precision only |
| [`date_bin`](scalar_functions.md#date_bin)                          | **the UTC instant** | Bins are anchored at the UTC epoch, not at local midnight                          |
| [`to_unixtime`](scalar_functions.md#to_unixtime)                    | **the UTC instant** |                                                                                    |
| `AT TIME ZONE`, `CAST(t AS TIMESTAMPTZ)`                            | **the UTC instant** | For a zone-aware input; a zone-naive input is shifted, see above                   |
| Comparison, `ORDER BY`, `GROUP BY`, joins, `min`/`max`              | **the UTC instant** | Annotations are ignored; equal instants are equal                                  |

The consequence worth memorising is that `date_trunc` and `date_bin` disagree on
the same value:

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
day, displayed in Denver (`2023-12-31T17:00:00-07:00` is `2024-01-01T00:00:00Z`).
Each individually matches PostgreSQL; the pair is still surprising. Use
`date_trunc` when you want local calendar boundaries, and see
[Recipes](#recipes) for local-calendar binning with `date_bin`.

The other functions on a zone-aware value:

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

### `INTERVAL '1 day'` is not `INTERVAL '24 hours'`

This is the single most useful thing to know on this page. On a **zone-aware**
timestamp, DataFusion adds calendar units (years, months, days) in the value's
own local calendar and adds sub-day units (hours, minutes, seconds) as elapsed
time. Across a DST transition the two differ:

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

America/Denver springs forward on 2024-03-10, so that local day is 23 hours
long. `INTERVAL '1 day'` lands on the same wall clock the next day (82800
seconds later); `INTERVAL '24 hours'` lands 24 hours later, an hour further on
the clock. In the autumn the same pair goes the other way: adding
`INTERVAL '1 day'` to `2024-11-02 12:00:00-06:00` advances the instant by 90000
seconds.

This matches PostgreSQL exactly (verified against PostgreSQL 17). On a
**zone-naive** timestamp there is no DST to apply, so `INTERVAL '1 day'` and
`INTERVAL '24 hours'` always agree.

Use calendar units when you mean "the same time tomorrow" and sub-day units when
you mean "24 hours of elapsed time". They are not interchangeable.

### Ambiguous and nonexistent local times

Two local wall clocks per year are not well defined in a zone with DST: the hour
skipped when the clocks go forward does not exist, and the hour repeated when
they go back is ambiguous. DataFusion currently **errors** on both, in both the
literal and the column path:

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

The same happens for the ambiguous fall-back hour, `2024-11-03 01:30:00` in
America/Denver. PostgreSQL resolves both instead of erroring (the gap moves
forward, the ambiguous hour is read as standard time).

Fixed-offset zones such as `'+08:00'` never have transitions and are never
affected. See <https://github.com/apache/datafusion/issues/25084> and the
upstream fix <https://github.com/apache/arrow-rs/pull/11038>.

## Recipes

### Aggregate UTC data by local calendar day in a named zone

`date_bin` bins on the UTC instant, so binning zone-aware data directly gives
UTC days. Convert to the target zone, then flatten to a local wall clock with
`to_local_time`, and bin that:

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

`local_day` is a zone-naive value: it is a local calendar day label, not an
instant, which is exactly what you want as a `GROUP BY` key. For whole calendar
units, `date_trunc('day', t AT TIME ZONE 'Europe/Brussels')` gives the same
grouping while keeping the result zone-aware:

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

Use the `to_local_time` form when the bin width is not a whole calendar unit
(`INTERVAL '15 minutes'`, `INTERVAL '4 hours'`), which `date_trunc` cannot
express.

:::{warning}
Passing `date_bin` an origin in the target zone — for example
`date_bin(INTERVAL '1 day', t, TIMESTAMP '2024-04-30 22:00:00' AT TIME ZONE 'UTC')`
— appears to work but is only correct while the zone's offset does not change.
It silently drifts by an hour across a DST transition. Prefer `to_local_time`.
:::

### Get a zone's local wall clock

```sql
SELECT to_local_time(t AT TIME ZONE 'Europe/Brussels') AS brussels_wall_clock
FROM hits_utc LIMIT 1;
```

`AT TIME ZONE` relabels the instant for display in Brussels; `to_local_time`
then converts that display into an actual zone-naive value. Do **not** use
`::timestamp` for this — that returns the UTC wall clock, not the local one.

### Round-trip safely

- Store instants as `Timestamp(unit, Some("UTC"))` and convert to a display zone
  only at the edge of the query.
- Set `datafusion.execution.time_zone = 'UTC'` so that `timestamptz`, `now()`
  and `to_timestamp` are zone-aware and every conversion in this page is a
  zero-offset shift.
- Use `arrow_cast` with an explicit Arrow type when you want the exact
  conversion; `::timestamptz` depends on session configuration.
- Never round-trip a zone-aware value through `Timestamp(_, None)` unless you
  intend to reduce it to a UTC wall clock.

## Differences from PostgreSQL

DataFusion aims to follow PostgreSQL, and does for most of the above. These are
the known divergences today.

| Behaviour                                                | DataFusion                                                                                             | PostgreSQL                                                                   | Issue                                               |
| -------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------- | --------------------------------------------------- |
| `TIMESTAMP WITH TIME ZONE` with no session zone set      | Zone-**naive** `Timestamp(_, None)`; the offset in a literal is discarded                              | Always zone-aware; `TimeZone` is always set                                  | —                                                   |
| `AT TIME ZONE` applied to a zone-**aware** value         | Returns a zone-**aware** value in the named zone                                                       | Returns a zone-**naive** value                                               | <https://github.com/apache/datafusion/issues/12218> |
| `tstz::timestamp`                                        | The **UTC** wall clock                                                                                 | The **session zone** wall clock                                              | <https://github.com/apache/datafusion/issues/12218> |
| Zone-naive value compared with a zone-aware one          | The naive side is shifted into the **other operand's** zone, not the session zone                      | The naive side is read in the **session** zone                               | <https://github.com/apache/datafusion/issues/13212> |
| Zone-naive **column** compared with a zone-aware literal | The optimizer drops the shift, giving the opposite answer to the same comparison written with literals | Consistent with the literal form                                             | <https://github.com/apache/datafusion/issues/25095> |
| `tstz - timestamp`                                       | Naive side read as UTC, so the session offset is lost                                                  | Naive side read in the session zone                                          | <https://github.com/apache/datafusion/issues/13212> |
| Ambiguous / nonexistent local time                       | Error                                                                                                  | Resolved (gap moves forward, ambiguity reads as standard time)               | <https://github.com/apache/datafusion/issues/25084> |
| `from_unixtime`                                          | Zone-naive UTC wall clock; ignores the session zone                                                    | `to_timestamp(double)` returns `timestamptz`                                 | <https://github.com/apache/datafusion/issues/12892> |
| `date_part` / `EXTRACT` on a zone-naive value            | Uses the stored wall clock; ignores the session zone                                                   | The same — but DataFusion's config docs once promised session-zone awareness | <https://github.com/apache/datafusion/issues/18228> |
| `to_timestamp_*` on an already zone-aware input          | Rewrites the zone to the session zone, dropping it entirely when unset                                 | n/a                                                                          | <https://github.com/apache/datafusion/issues/23841> |

:::{note}
The `AT TIME ZONE` row describes DataFusion's behaviour as of this writing. There
is active discussion in <https://github.com/apache/datafusion/issues/12218>
about aligning the zone-**aware**-input case with PostgreSQL and DuckDB, which
would change the second and third rows above; DuckDB also returns a zone-naive
value there. The zone-**naive** input case already agrees with PostgreSQL: both
produce the same instant.
:::

## See also

- [Data Types](data_types.md)
- [Date and Time Functions](scalar_functions.md#time-and-date-functions)
- [Configuration Settings](../configs.md)
