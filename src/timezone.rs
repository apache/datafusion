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

/// Utils for timezone. This is basically from arrow-array::timezone (private).
use arrow_schema::ArrowError;
use chrono::{
    format::{parse, Parsed, StrftimeItems},
    offset::TimeZone,
    FixedOffset, LocalResult, NaiveDate, NaiveDateTime, Offset,
};
use std::str::FromStr;

/// Parses a fixed offset of the form "+09:00"
fn parse_fixed_offset(tz: &str) -> Result<FixedOffset, ArrowError> {
    let mut parsed = Parsed::new();

    if let Ok(fixed_offset) =
        parse(&mut parsed, tz, StrftimeItems::new("%:z")).and_then(|_| parsed.to_fixed_offset())
    {
        return Ok(fixed_offset);
    }

    if let Ok(fixed_offset) =
        parse(&mut parsed, tz, StrftimeItems::new("%#z")).and_then(|_| parsed.to_fixed_offset())
    {
        return Ok(fixed_offset);
    }

    Err(ArrowError::ParseError(format!(
        "Invalid timezone \"{}\": Expected format [+-]XX:XX, [+-]XX, or [+-]XXXX",
        tz
    )))
}

/// An [`Offset`] for [`Tz`]
#[derive(Debug, Copy, Clone)]
pub struct TzOffset {
    tz: Tz,
    offset: FixedOffset,
}

impl std::fmt::Display for TzOffset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.offset.fmt(f)
    }
}

impl Offset for TzOffset {
    fn fix(&self) -> FixedOffset {
        self.offset
    }
}

/// An Arrow [`TimeZone`]
#[derive(Debug, Copy, Clone)]
pub struct Tz(TzInner);

#[derive(Debug, Copy, Clone)]
enum TzInner {
    Timezone(chrono_tz::Tz),
    Offset(FixedOffset),
}

impl FromStr for Tz {
    type Err = ArrowError;

    fn from_str(tz: &str) -> Result<Self, Self::Err> {
        if tz.starts_with('+') || tz.starts_with('-') {
            Ok(Self(TzInner::Offset(parse_fixed_offset(tz)?)))
        } else {
            Ok(Self(TzInner::Timezone(tz.parse().map_err(|e| {
                ArrowError::ParseError(format!("Invalid timezone \"{}\": {}", tz, e))
            })?)))
        }
    }
}

macro_rules! tz {
    ($s:ident, $tz:ident, $b:block) => {
        match $s.0 {
            TzInner::Timezone($tz) => $b,
            TzInner::Offset($tz) => $b,
        }
    };
}

impl TimeZone for Tz {
    type Offset = TzOffset;

    fn from_offset(offset: &Self::Offset) -> Self {
        offset.tz
    }

    fn offset_from_local_date(&self, local: &NaiveDate) -> LocalResult<Self::Offset> {
        tz!(self, tz, {
            tz.offset_from_local_date(local).map(|x| TzOffset {
                tz: *self,
                offset: x.fix(),
            })
        })
    }

    fn offset_from_local_datetime(&self, local: &NaiveDateTime) -> LocalResult<Self::Offset> {
        tz!(self, tz, {
            tz.offset_from_local_datetime(local).map(|x| TzOffset {
                tz: *self,
                offset: x.fix(),
            })
        })
    }

    fn offset_from_utc_date(&self, utc: &NaiveDate) -> Self::Offset {
        tz!(self, tz, {
            TzOffset {
                tz: *self,
                offset: tz.offset_from_utc_date(utc).fix(),
            }
        })
    }

    fn offset_from_utc_datetime(&self, utc: &NaiveDateTime) -> Self::Offset {
        tz!(self, tz, {
            TzOffset {
                tz: *self,
                offset: tz.offset_from_utc_datetime(utc).fix(),
            }
        })
    }
}
