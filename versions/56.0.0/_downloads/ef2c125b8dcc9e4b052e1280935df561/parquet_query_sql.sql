-- Test scanning out dictionary columns
select dict_10_optional from t;
select dict_100_optional from t;
select dict_1000_optional from t;

-- Test filtering dictionary columns
select count(*) from t where dict_10_required = 'prefix#0';
select count(*) from t where dict_100_required = 'prefix#0';
select count(*) from t where dict_1000_required = 'prefix#0';

-- Test select integers
select i64_optional from t where dict_10_required = 'prefix#2' and dict_1000_required = 'prefix#10';
select i64_required from t where dict_10_required = 'prefix#2' and dict_1000_required = 'prefix#10';

-- Test select integers
select string_optional from t where dict_10_required = 'prefix#1' and dict_1000_required = 'prefix#1';
select string_required from t where dict_10_required = 'prefix#1' and dict_1000_required = 'prefix#1';

-- Test select distinct
select distinct dict_10_required from t where dict_1000_optional is not NULL and i64_optional > 0;
select distinct dict_10_required from t where dict_1000_optional is not NULL and i64_optional > 0;
select distinct dict_10_required from t where dict_1000_optional is not NULL and i64_required > 0;
select distinct dict_10_required from t where dict_1000_optional is not NULL and i64_required > 0;

-- Test basic aggregations
select dict_10_optional, count(*) from t group by dict_10_optional;
select dict_10_optional, dict_100_optional, count(*) from t group by dict_10_optional, dict_100_optional;

-- Test float aggregations
select dict_10_optional, dict_100_optional, MIN(f64_required), MAX(f64_required), AVG(f64_required) from t group by dict_10_optional, dict_100_optional;
select dict_10_optional, dict_100_optional, MIN(f64_optional), MAX(f64_optional), AVG(f64_optional) from t group by dict_10_optional, dict_100_optional;
select dict_10_required, dict_100_required, MIN(f64_optional), MAX(f64_optional), AVG(f64_optional) from t group by dict_10_required, dict_100_required;
