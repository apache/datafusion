import re

from pathlib import Path

# Regex pattern to match Rust code blocks in Markdown
RUST_CODE_BLOCK_PATTERN = re.compile(r"```rust\s*(.*?)```", re.DOTALL)


def remove_hashtag_lines_in_rust_blocks(markdown_content):
    """
    Removes lines starting with '# ' in Rust code blocks within a Markdown string.
    """

    def _process_code_block(match):
        # Extract the code block content
        code_block_content = match.group(1).strip()
        print(code_block_content)

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


# Example usage
def process_markdown_file(file_path):
    # Read the Markdown file
    with open(file_path, "r", encoding="utf-8") as file:
        markdown_content = file.read()

    # Remove lines starting with '#' in Rust code blocks
    updated_markdown_content = remove_hashtag_lines_in_rust_blocks(markdown_content)

    # Write the updated content back to the Markdown file
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(updated_markdown_content)

    print("Lines starting with '# ' removed from Rust code blocks")


root_directory = Path("./temp")

for file_path in root_directory.rglob("*.md"):
    print(f"Processing file: {file_path}")
    process_markdown_file(file_path)

print("All Markdown files processed.")
