# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
