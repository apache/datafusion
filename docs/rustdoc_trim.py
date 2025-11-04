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

import re
from sphinx.application import Sphinx

# Regex pattern to match Rust code blocks in Markdown
RUST_CODE_BLOCK_PATTERN = re.compile(r"```rust(?:,ignore)?\s*(.*?)```", re.DOTALL)


def remove_hashtag_lines_in_rust_blocks(markdown_content):
    """
    Removes lines starting with '# ' in Rust code blocks within a Markdown string.
    """

    def _process_code_block(match):
        # Extract the code block content
        code_block_content = match.group(1).strip()

        # Remove lines starting with '#'
        modified_code_block = "\n".join(
            line
            for line in code_block_content.splitlines()
            if (not line.lstrip().startswith("# ")) and line.strip() != "#"
        )

        # Return the modified code block wrapped in triple backticks
        return f"```rust\n{modified_code_block}\n```"

    # Replace all Rust code blocks using the _process_code_block function
    return RUST_CODE_BLOCK_PATTERN.sub(_process_code_block, markdown_content)


def process_source_file(app: Sphinx, docname: str, source: list[str]):
    original_content = source[0]
    # Remove lines starting with '#' in Rust code blocks
    modified_content = remove_hashtag_lines_in_rust_blocks(original_content)
    source[0] = modified_content


def setup(app: Sphinx):
    app.connect("source-read", process_source_file)
    return dict(
        parallel_read_safe=True,
        parallel_write_safe=True,
    )
