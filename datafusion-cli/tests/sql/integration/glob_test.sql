-- Test glob function with files available in CI
-- Test 1: Single CSV file - verify basic functionality
SELECT COUNT(*) AS cars_count FROM glob('../datafusion/core/tests/data/cars.csv');

-- Test 2: Data aggregation from CSV file - verify actual data reading
SELECT car, COUNT(*) as count FROM glob('../datafusion/core/tests/data/cars.csv') GROUP BY car ORDER BY car;

-- Test 3: JSON file with explicit format parameter - verify format specification
SELECT COUNT(*) AS json_count FROM glob('../datafusion/core/tests/data/1.json', 'json');

-- Test 4: Single specific CSV file - verify another CSV works
SELECT COUNT(*) AS example_count FROM glob('../datafusion/core/tests/data/example.csv');

-- Test 5: Glob pattern with wildcard - test actual glob functionality
SELECT COUNT(*) AS glob_pattern_count FROM glob('../datafusion/core/tests/data/exa*.csv'); 