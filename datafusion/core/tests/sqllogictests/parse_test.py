import re

pattern = re.compile(
    r"async fn (.*?)\(\) [.\s\S\t]*?let sql =\s*([.\s\S\t]*?);[.\s\S\t]*?let expected =\s*([.\s\S\t]*?);"
)

TEST_BASE_PATH = "../../datafusion/core/tests/sql"
TEST_FILES = [
    "aggregates.rs",
]
IGNORE_FUNCTIONS = [
    "csv_query_approx_percentile_cont",
    "csv_query_approx_percentile_cont_with_weight",
    "csv_query_approx_percentile_cont_with_histogram_bins",
]


def construct_sqllogictest(function_name, column_count, sql_query, result):
    return (
        "# "
        + function_name
        + "\nquery "
        + ("I" * column_count)
        + "\n"
        + sql_query
        + "\n"
        + result
    )


def strip_formatting(result: str) -> str:
    lines = result.split("\n")
    if "vec!" in result:
        lines = lines[4:-2]
    lines = [line.replace('"', "").replace(",", "") for line in lines]
    lines = [
        ",".join(re.findall("(.*?)\|", line)[1:]).replace(" ", "") for line in lines
    ]

    result = []
    length = 0
    for line in lines:
        res = []
        split = line.split(",")
        length = max(length, len(split))
        for x in split:
            res.append(x)
        #     if x == "":
        #         res.append(" ")
        #     else:
        #         res.append(x)
        result.append(" ".join(res))
    return (length, "\n".join(result).rstrip())


count = 0
for test_file in TEST_FILES:
    with open("output.txt", "a") as the_file:
        with open(f"{TEST_BASE_PATH}/{test_file}", "r") as file:
            file_string = file.read()
            split = file_string.split("#[tokio::test]")
            sqllogic_test = None
            for i in split[1:]:
                result = re.search(pattern, i)
                if not result:
                    sqllogic_test = "# could not parse function"
                    # try parse function name
                    result = re.search(r"async fn (.*?)\(\) [.\s\S\t]*?", i)
                    if result:
                        sqllogic_test = "# could not parse function " + result.group(1)
                    else:
                        sqllogic_test = "# UNKNOWN"
                else:
                    count += 1
                    function_name = result.group(1)
                    sql_query = result.group(2).replace('"', "")
                    (length, expected_result) = strip_formatting(result.group(3))
                    # print(f"function_name: {function_name}")
                    # print(f"sql_query: {sql_query}")
                    # print(f"expected_result (formatted): {expected_result}")
                    # print(f"expected_result: {result.group(3)}")
                    sqllogic_test = construct_sqllogictest(
                        function_name,
                        length,
                        sql_query,
                        expected_result,
                    )

                the_file.write(sqllogic_test + "\n\n")
