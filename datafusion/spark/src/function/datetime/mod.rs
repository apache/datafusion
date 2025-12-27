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

pub mod date_add;
pub mod date_sub;
pub mod extract;
pub mod last_day;
pub mod make_dt_interval;
pub mod make_interval;
pub mod next_day;

use datafusion_expr::ScalarUDF;
use datafusion_functions::make_udf_function;
use std::sync::Arc;

make_udf_function!(date_add::SparkDateAdd, date_add);
make_udf_function!(date_sub::SparkDateSub, date_sub);
make_udf_function!(extract::SparkHour, hour);
make_udf_function!(extract::SparkMinute, minute);
make_udf_function!(extract::SparkSecond, second);
make_udf_function!(last_day::SparkLastDay, last_day);
make_udf_function!(make_dt_interval::SparkMakeDtInterval, make_dt_interval);
make_udf_function!(make_interval::SparkMakeInterval, make_interval);
make_udf_function!(next_day::SparkNextDay, next_day);

pub mod expr_fn {
    use datafusion_functions::export_functions;

    export_functions!((
        date_add,
        "Returns the date that is days days after start. The function returns NULL if at least one of the input parameters is NULL.",
        arg1 arg2
    ));
    export_functions!((
        date_sub,
        "Returns the date that is days days before start. The function returns NULL if at least one of the input parameters is NULL.",
        arg1 arg2
    ));
    export_functions!((hour, "Extracts the hour component of a timestamp.", arg1));
    export_functions!((
        minute,
        "Extracts the minute component of a timestamp.",
        arg1
    ));
    export_functions!((
        second,
        "Extracts the second component of a timestamp.",
        arg1
    ));
    export_functions!((
        last_day,
        "Returns the last day of the month which the date belongs to.",
        arg1
    ));
    export_functions!((
        make_dt_interval,
        "Make a day time interval from given days, hours, mins and secs (return type is actually a Duration(Microsecond))",
         days hours mins secs
    ));
    export_functions!((
        make_interval,
        "Make interval from years, months, weeks, days, hours, mins and secs.",
        years months weeks days hours mins secs
    ));
    // TODO: add once ANSI support is added:
    // "When both of the input parameters are not NULL and day_of_week is an invalid input, the function throws SparkIllegalArgumentException if spark.sql.ansi.enabled is set to true, otherwise NULL."
    export_functions!((
        next_day,
        "Returns the first date which is later than start_date and named as indicated. The function returns NULL if at least one of the input parameters is NULL.",
        arg1 arg2
    ));
}

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        date_add(),
        date_sub(),
        hour(),
        minute(),
        second(),
        last_day(),
        make_dt_interval(),
        make_interval(),
        next_day(),
    ]
}
