#!/usr/bin/python
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import glob
import os
import re


def copy_test_source(test_filename, test_method, output):
    lines = []
    with open(test_filename) as test:
        found = False
        for test_line in test.readlines():
            if test_line.startswith("fn {}".format(test_method)):
                found = True
                continue
            if found:
                if test_line.strip() == "Ok(())":
                    break
                lines.append(test_line)

    # remove blank lines from the end of the list
    while lines and lines[-1] == "":
        lines.pop()

    # remove leading indent when possible
    consistent_indent = True
    for line in lines:
        if len(line.strip()) > 0 and not (
            line.startswith("    ") or line.startswith("\t")
        ):
            print("not consistent", line)
            consistent_indent = False
            break
    if consistent_indent:
        old_lines = lines
        lines = []
        for line in old_lines:
            if len(line) >= 4:
                lines.append(line[4:])
            else:
                lines.append(line)

    # write to output
    output.write("```rust\n")
    for line in lines:
        output.write(line)
    output.write("```")


def add_source(input, output):
    print("Copying", input, "to", output)
    # <!-- include: library_logical_plan::plan_builder_1 -->
    include_pattern = "<!-- include: ([a-z0-9_]*)::([a-z0-9_]*) -->"
    with open(input, "r") as input:
        with open(output, "w") as output:
            for line in input.readlines():
                matches = re.search(include_pattern, line)
                if matches is not None:
                    test_file = matches.group(1)
                    test_method = matches.group(2)
                    test_filename = "src/{}.rs".format(test_file)
                    copy_test_source(test_filename, test_method, output)
                else:
                    output.write(line)


def main():
    for file in glob.glob("source/**/*.md"):
        dest = "temp/" + file[7:]
        last_path_sep = dest.rindex("/")
        dir = dest[0:last_path_sep]
        if not os.path.exists(dir):
            os.makedirs(dir)
        add_source(file, dest)


if __name__ == "__main__":
    main()
