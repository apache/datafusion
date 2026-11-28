import runpy
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from types import SimpleNamespace


SCRIPT = Path(__file__).with_name("generate-changelog.py")
PRINT_PULLS = runpy.run_path(str(SCRIPT))["print_pulls"]


class TestPrintPulls(unittest.TestCase):
    def render(self, author, git_author_name):
        pull = SimpleNamespace(number=13794, title="Example change")
        commit = SimpleNamespace(
            author=author,
            commit=SimpleNamespace(
                author=SimpleNamespace(name=git_author_name)
            ),
        )
        output = StringIO()
        with redirect_stdout(output):
            PRINT_PULLS("apache/datafusion", "Fixed bugs", [(pull, commit)])
        return output.getvalue()

    def test_unlinked_github_author_uses_git_author_name(self):
        output = self.render(None, "mertak-synnada")
        self.assertIn("(mertak-synnada)", output)

    def test_linked_github_author_uses_github_login(self):
        output = self.render(
            SimpleNamespace(login="alice"),
            "Alice Example",
        )
        self.assertIn("(alice)", output)
        self.assertNotIn("(Alice Example)", output)


if __name__ == "__main__":
    unittest.main()
