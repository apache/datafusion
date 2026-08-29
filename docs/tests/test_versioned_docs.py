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

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

DOCS_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = DOCS_DIR / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))


def load_script(name: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS_DIR / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


versioned_docs = load_script("versioned_docs")
snapshot_site = load_script("snapshot_site")
assemble_site = load_script("assemble_site")
validate_site = load_script("validate_site")


def write(path: Path, content: str = "file") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


class VersionedDocsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_manifest_has_one_site_wide_exact_release(self) -> None:
        entries = versioned_docs.load_versions()
        self.assertEqual(
            entries,
            [
                {
                    "name": "Development",
                    "url": f"{versioned_docs.SITE_URL}/",
                    "version": "main",
                },
                {
                    "commit": versioned_docs.EXPECTED_55_COMMIT,
                    "name": "55.0.0",
                    "tag": "55.0.0",
                    "url": f"{versioned_docs.SITE_URL}/versions/55.0.0/",
                    "version": "55.0.0",
                },
            ],
        )

    def test_manifest_is_appendable_and_rejects_duplicates(self) -> None:
        manifest = self.root / "versions.json"
        entries = versioned_docs.load_versions()
        entries.append(
            {
                "commit": "a" * 40,
                "name": "56.0.0",
                "tag": "56.0.0",
                "url": f"{versioned_docs.SITE_URL}/versions/56.0.0/",
                "version": "56.0.0",
            }
        )
        write(manifest, json.dumps(entries))
        self.assertEqual(len(versioned_docs.load_versions(manifest)), 3)
        entries[-1]["version"] = "55.0.0"
        write(manifest, json.dumps(entries))
        with self.assertRaisesRegex(ValueError, "duplicate version"):
            versioned_docs.load_versions(manifest)

    def test_exact_lightweight_tag_and_peeled_commit(self) -> None:
        snapshot_site.verify_tag("55.0.0", versioned_docs.EXPECTED_55_COMMIT)
        self.assertEqual(
            snapshot_site.git("cat-file", "-t", "refs/tags/55.0.0", capture=True),
            "commit",
        )

    def test_exact_tag_keeps_unreleased_55_prose(self) -> None:
        prose = snapshot_site.git(
            "show",
            "55.0.0:docs/source/library-user-guide/upgrading/55.0.0.md",
            capture=True,
        )
        self.assertIn("DataFusion `55.0.0` has not been released yet.", prose)

    def test_missing_tag_error_is_actionable(self) -> None:
        repository = self.root / "repository"
        subprocess.run(
            ["git", "init", str(repository)], check=True, capture_output=True
        )
        with mock.patch.object(snapshot_site, "REPOSITORY", repository):
            with self.assertRaisesRegex(RuntimeError, "git fetch origin tag missing"):
                snapshot_site.verify_tag("missing", "0" * 40)

    def test_generated_html_adaptations_leave_sources_unchanged(self) -> None:
        site = self.root / "site"
        original = (
            "https://datafusion.apache.org/contributor-guide/testing.html\n"
            "https://datafusion.apache.org/contributor-guide/gsoc_application_guidelines.html\n"
            "https://github.com/apache/datafusion/blob/main/docs/source/index.rst\n"
            "https://docs.rs/datafusion/latest/datafusion/\n"
        )
        write(site / "index.html", original)
        write(site / "_sources" / "index.rst.txt", original)
        write(site / ".buildinfo", "environment-specific metadata")
        write(site / ".doctrees" / "environment.pickle", "temporary paths")
        write(
            site / "llms.txt",
            original + "https://github.com/apache/datafusion/tree/main\n",
        )
        snapshot_site.adapt_generated_output(site, "55.0.0", "55.0.0")
        output = (site / "index.html").read_text(encoding="utf-8")
        self.assertIn("/versions/55.0.0/contributor-guide/", output)
        self.assertIn(
            "/versions/55.0.0/contributor-guide/gsoc/gsoc_application_guidelines_2025.html",
            output,
        )
        self.assertIn("/blob/55.0.0/docs/source/", output)
        self.assertIn("docs.rs/datafusion/55.0.0/datafusion/", output)
        self.assertEqual(
            (site / "_sources" / "index.rst.txt").read_text(encoding="utf-8"),
            original,
        )
        llms = (site / "llms.txt").read_text(encoding="utf-8")
        self.assertIn("/versions/55.0.0/contributor-guide/", llms)
        self.assertIn("/tree/55.0.0", llms)
        self.assertNotIn("/tree/main", llms)
        self.assertFalse((site / ".buildinfo").exists())
        self.assertFalse((site / ".doctrees").exists())

    def test_publication_is_one_directory_and_refuses_overwrite(self) -> None:
        staged = self.root / "staged"
        package = self.root / "package with spaces"
        write(staged / "index.html")
        destination = snapshot_site.publish_snapshot(staged, package, "55.0.0")
        self.assertEqual(destination, package / "versions" / "55.0.0")
        self.assertTrue((destination / "index.html").is_file())
        write(staged / "index.html")
        with self.assertRaisesRegex(RuntimeError, "refusing to overwrite"):
            snapshot_site.publish_snapshot(staged, package, "55.0.0")

    def test_publication_failure_leaves_no_release_directory(self) -> None:
        staged = self.root / "staged"
        package = self.root / "package"
        write(staged / "index.html")
        with mock.patch.object(
            snapshot_site.shutil,
            "copytree",
            side_effect=OSError("simulated copy failure"),
        ):
            with self.assertRaisesRegex(OSError, "simulated copy failure"):
                snapshot_site.publish_snapshot(staged, package, "55.0.0")
        self.assertFalse((package / "versions" / "55.0.0").exists())
        self.assertEqual(list((package / "versions").iterdir()), [])

    def test_snapshot_rejects_unsafe_output_and_version(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "must not contain the repository"):
            snapshot_site.validate_package_root(snapshot_site.REPOSITORY.parent)
        with self.assertRaisesRegex(RuntimeError, "must be outside"):
            snapshot_site.validate_package_root(snapshot_site.REPOSITORY / "docs/build")
        with self.assertRaisesRegex(RuntimeError, "exact X.Y.Z"):
            snapshot_site.build_snapshot("../55", "../55", self.root / "output")

    def test_assembly_replaces_current_and_retains_complete_archive(self) -> None:
        current = self.root / "current"
        published = self.root / "published"
        output = self.root / "output"
        write(current / "index.html", "new")
        write(current / "sitemap.xml", "<urlset/>")
        write(published / "obsolete.html", "old current")
        for version in ("54.0.0", "55.0.0"):
            write(published / "versions" / version / "index.html", version)
            write(published / "versions" / version / "sitemap.xml", "<urlset/>")
        assemble_site.assemble(current, published, output)
        self.assertEqual((output / "index.html").read_text(encoding="utf-8"), "new")
        self.assertFalse((output / "obsolete.html").exists())
        self.assertTrue((output / "versions" / "54.0.0" / "index.html").is_file())
        self.assertTrue((output / "versions" / "55.0.0" / "index.html").is_file())
        sitemap = (output / "sitemap.xml").read_text(encoding="utf-8")
        self.assertIn("sitemap-main.xml", sitemap)
        self.assertIn("versions/54.0.0/sitemap.xml", sitemap)
        errors: list[str] = []
        validate_site.validate_sitemaps(output, ["54.0.0", "55.0.0"], errors)
        self.assertEqual(errors, [])

    def test_removed_picker_entry_does_not_delete_archive(self) -> None:
        current = self.root / "current"
        published = self.root / "published"
        output = self.root / "output"
        write(current / "index.html")
        write(current / "sitemap.xml", "<urlset/>")
        write(published / "versions" / "54.0.0" / "index.html")
        write(published / "versions" / "54.0.0" / "sitemap.xml")
        assemble_site.assemble(current, published, output)
        self.assertTrue((output / "versions" / "54.0.0" / "index.html").is_file())

    def test_assembly_rejects_unsafe_archive_name_and_path_overlap(self) -> None:
        published = self.root / "published"
        write(published / "versions" / "latest" / "index.html")
        write(published / "versions" / "latest" / "sitemap.xml")
        with self.assertRaisesRegex(RuntimeError, "unsafe published version"):
            assemble_site.archived_versions(published)
        current = self.root / "container" / "current"
        write(current / "index.html")
        with self.assertRaisesRegex(RuntimeError, "must not contain one another"):
            assemble_site.validate_paths(current, published, self.root / "container")

    def test_assembly_rejects_links_in_archive_and_output(self) -> None:
        current = self.root / "current"
        published = self.root / "published"
        external = self.root / "external"
        output_link = self.root / "output-link"
        write(current / "index.html")
        write(published / "versions" / "55.0.0" / "index.html")
        write(published / "versions" / "55.0.0" / "sitemap.xml")
        write(external / "secret.txt")
        archive_link = published / "versions" / "55.0.0" / "external"
        try:
            archive_link.symlink_to(external, target_is_directory=True)
            output_link.symlink_to(external, target_is_directory=True)
        except OSError as error:
            self.skipTest(f"directory symlinks are unavailable: {error}")
        with self.assertRaisesRegex(RuntimeError, "archived path must not be a link"):
            assemble_site.archived_versions(published)
        archive_link.unlink()
        with self.assertRaisesRegex(RuntimeError, "output site must not be a link"):
            assemble_site.assemble(current, published, output_link)
        self.assertTrue((external / "secret.txt").is_file())

    def test_assembly_refuses_to_replace_existing_output(self) -> None:
        current = self.root / "current"
        published = self.root / "published"
        output = self.root / "output"
        write(current / "index.html")
        write(current / "sitemap.xml", "<urlset/>")
        published.mkdir()
        write(output / "sentinel.txt", "previous")
        with self.assertRaisesRegex(RuntimeError, "output site already exists"):
            assemble_site.assemble(current, published, output)
        self.assertEqual(
            (output / "sentinel.txt").read_text(encoding="utf-8"), "previous"
        )

    def test_assembly_refuses_output_created_while_staging(self) -> None:
        current = self.root / "current"
        published = self.root / "published"
        output = self.root / "output"
        write(current / "index.html")
        write(current / "sitemap.xml", "<urlset/>")
        published.mkdir()
        real_copytree = shutil.copytree

        def create_output(source: Path, destination: Path) -> Path:
            copied = real_copytree(source, destination)
            write(output / "sentinel.txt", "concurrent")
            return copied

        with mock.patch.object(assemble_site.shutil, "copytree", create_output):
            with self.assertRaisesRegex(RuntimeError, "output site already exists"):
                assemble_site.assemble(current, published, output)
        self.assertEqual(
            (output / "sentinel.txt").read_text(encoding="utf-8"), "concurrent"
        )

    def test_picker_equivalent_paths_and_fallback(self) -> None:
        release = f"{versioned_docs.SITE_URL}/versions/55.0.0/"
        candidate = validate_site.switch_candidate(
            f"{versioned_docs.SITE_URL}/user-guide/sql/select.html", "main", release
        )
        self.assertEqual(candidate, release + "user-guide/sql/select.html")
        reverse = validate_site.switch_candidate(
            candidate, "55.0.0", f"{versioned_docs.SITE_URL}/"
        )
        self.assertEqual(
            reverse, f"{versioned_docs.SITE_URL}/user-guide/sql/select.html"
        )
        current_only = validate_site.switch_candidate(
            f"{versioned_docs.SITE_URL}/library-user-guide/upgrading/56.0.0.html",
            "main",
            release,
        )
        self.assertEqual(
            current_only, release + "library-user-guide/upgrading/56.0.0.html"
        )
        self.assertNotIn("/versions/55.0.0/versions/", current_only)

    @unittest.skipUnless(shutil.which("rsync"), "rsync is not installed")
    def test_rsync_keeps_git_and_deletes_stale_current_files(self) -> None:
        source = self.root / "source"
        destination = self.root / "destination"
        write(source / "index.html", "new")
        write(source / "versions" / "55.0.0" / "index.html")
        write(source / ".asf.yaml")
        write(source / ".nojekyll")
        write(destination / "stale.html")
        write(destination / ".git" / "HEAD", "git")
        subprocess.run(
            [
                "rsync",
                "-a",
                "--delete",
                "--exclude",
                "/.git/",
                f"{source}/",
                f"{destination}/",
            ],
            check=True,
        )
        self.assertFalse((destination / "stale.html").exists())
        self.assertTrue((destination / "versions" / "55.0.0" / "index.html").is_file())
        self.assertEqual(
            (destination / ".git" / "HEAD").read_text(encoding="utf-8"), "git"
        )


if __name__ == "__main__":
    unittest.main()
