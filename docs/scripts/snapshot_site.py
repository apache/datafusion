#!/usr/bin/env python3
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

"""Build one complete documentation snapshot from an exact release tag."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

from versioned_docs import DOCS_DIR, VERSION_PATTERN, is_link, release_entry

REPOSITORY = DOCS_DIR.parent
INTERNAL_DOC_LINK = re.compile(
    rb"https://datafusion\.apache\.org/"
    rb"(?=(?:user-guide|library-user-guide|contributor-guide)/)"
)
GITHUB_MAIN_LINK = re.compile(
    rb"https://github\.com/apache/(?:arrow-)?datafusion/(blob|tree)/main"
    rb"(?=/|[\"'#<)\s]|$)"
)
DOCS_RS_LATEST_LINK = re.compile(
    rb"https://docs\.rs/(datafusion(?:-[a-z0-9-]+)?)/latest(?=/|[\"'#<])"
)
TAGGED_GSOC_GUIDELINES_LINK = re.compile(
    rb"https://datafusion\.apache\.org/contributor-guide/"
    rb"gsoc_application_guidelines\.html"
)


def git(*arguments: str, capture: bool = False) -> str:
    result = subprocess.run(
        ["git", *arguments],
        cwd=REPOSITORY,
        check=True,
        text=True,
        stdout=subprocess.PIPE if capture else None,
    )
    return result.stdout.strip() if capture else ""


def verify_tag(tag: str, expected_commit: str) -> None:
    tag_ref = f"refs/tags/{tag}"
    try:
        object_type = git("cat-file", "-t", tag_ref, capture=True)
        commit = git("rev-parse", f"{tag_ref}^{{commit}}", capture=True)
        peeled = git("rev-parse", f"{tag_ref}^{{}}", capture=True)
    except subprocess.CalledProcessError as error:
        raise RuntimeError(
            f"tag {tag} is missing; fetch it with: git fetch origin tag {tag}"
        ) from error
    if object_type not in {"commit", "tag"}:
        raise RuntimeError(f"tag {tag} has unexpected object type {object_type}")
    if commit != expected_commit or peeled != expected_commit:
        raise RuntimeError(f"tag {tag} peels to {commit}, expected {expected_commit}")


def validate_package_root(package_root: Path) -> Path:
    package_root = package_root.resolve()
    repository = REPOSITORY.resolve()
    if package_root.is_relative_to(repository) or repository.is_relative_to(
        package_root
    ):
        raise RuntimeError(
            "snapshot output must be outside and must not contain the repository"
        )
    return package_root


def adapt_generated_output(site: Path, version: str, tag: str) -> None:
    """Pin published links without changing tagged sources or ``_sources``."""
    prefix = f"https://datafusion.apache.org/versions/{version}/".encode()
    tag_bytes = tag.encode()
    paths = [*site.rglob("*.html"), site / "llms.txt"]
    for path in paths:
        if not path.is_file():
            continue
        content = path.read_bytes()
        if path.suffix == ".html":
            content = TAGGED_GSOC_GUIDELINES_LINK.sub(
                prefix
                + b"contributor-guide/gsoc/gsoc_application_guidelines_2025.html",
                content,
            )
        content = INTERNAL_DOC_LINK.sub(prefix, content)
        content = GITHUB_MAIN_LINK.sub(
            rb"https://github.com/apache/datafusion/\1/" + tag_bytes,
            content,
        )
        content = DOCS_RS_LATEST_LINK.sub(
            lambda match: b"https://docs.rs/"
            + match.group(1)
            + b"/"
            + version.encode(),
            content,
        )
        path.write_bytes(content)


def generate_dependency_graph(docs_dir: Path) -> None:
    script = docs_dir / "scripts" / "generate_dependency_graph.sh"
    if os.name != "nt":
        subprocess.run(["bash", str(script)], cwd=docs_dir, check=True)
        return

    for command in ("cargo", "dot"):
        if shutil.which(command) is None:
            raise RuntimeError(
                f"{command} is required to build the tagged dependency graph"
            )
    output = docs_dir / "source" / "_static" / "data" / "deps.svg"
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("wb") as destination:
        cargo = subprocess.Popen(
            [
                "cargo",
                "depgraph",
                "--workspace-only",
                "--all-deps",
                "--dedup-transitive-deps",
                "--exclude",
                "gen,gen-common",
            ],
            cwd=docs_dir.parent,
            stdout=subprocess.PIPE,
        )
        assert cargo.stdout is not None
        dot = subprocess.run(
            [
                "dot",
                "-Grankdir=TB",
                "-Gconcentrate=true",
                "-Goverlap=false",
                "-Tsvg",
            ],
            stdin=cargo.stdout,
            stdout=destination,
            check=False,
        )
        cargo.stdout.close()
        cargo_status = cargo.wait()
    if cargo_status != 0:
        raise subprocess.CalledProcessError(cargo_status, cargo.args)
    if dot.returncode != 0:
        raise subprocess.CalledProcessError(dot.returncode, dot.args)


def publish_snapshot(staged_site: Path, package_root: Path, version: str) -> Path:
    versions_root = package_root / "versions"
    if is_link(versions_root):
        raise RuntimeError(f"versions output must not be a link: {versions_root}")
    versions_root.mkdir(parents=True, exist_ok=True)
    destination = versions_root / version
    if destination.exists() or is_link(destination):
        raise RuntimeError(f"refusing to overwrite {destination}")
    temporary_root = Path(tempfile.mkdtemp(prefix=f".{version}-", dir=versions_root))
    local_stage = temporary_root / "site"
    try:
        shutil.copytree(staged_site, local_stage)
        if destination.exists() or is_link(destination):
            raise RuntimeError(f"refusing to overwrite {destination}")
        local_stage.rename(destination)
    finally:
        shutil.rmtree(temporary_root, ignore_errors=True)
    return destination


def build_snapshot(version: str, tag: str, package_root: Path) -> Path:
    if not VERSION_PATTERN.fullmatch(version) or tag != version:
        raise RuntimeError("version and tag must be the same exact X.Y.Z release")
    release = release_entry(version, tag)
    expected_commit = str(release["commit"])
    verify_tag(tag, expected_commit)
    package_root = validate_package_root(package_root)
    destination = package_root / "versions" / version
    if destination.exists() or is_link(destination):
        raise RuntimeError(f"refusing to overwrite {destination}")

    temporary_parent = Path(tempfile.mkdtemp(prefix="datafusion-release-site-"))
    worktree = temporary_parent / "worktree"
    built_site = temporary_parent / "html"
    worktree_added = False
    try:
        git("worktree", "add", "--detach", str(worktree), expected_commit)
        worktree_added = True
        actual_commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=worktree,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
        ).stdout.strip()
        if actual_commit != expected_commit:
            raise RuntimeError(
                f"temporary worktree is {actual_commit}, expected {expected_commit}"
            )

        docs_dir = worktree / "docs"
        generate_dependency_graph(docs_dir)
        environment = os.environ.copy()
        environment.update(
            DATAFUSION_RELEASE_SOURCE=str(docs_dir / "source"),
            DATAFUSION_RELEASE_VERSION=version,
            DATAFUSION_RELEASE_TAG=tag,
        )
        config_dir = temporary_parent / "config"
        config_dir.mkdir()
        shutil.copy2(DOCS_DIR / "scripts" / "release_conf.py", config_dir / "conf.py")
        subprocess.run(
            [
                "uv",
                "run",
                "--project",
                str(worktree),
                "--package",
                "datafusion-docs",
                "--with",
                "sphinx-sitemap==2.9.0",
                "python",
                "-m",
                "sphinx",
                "-W",
                "-b",
                "html",
                "-c",
                str(config_dir),
                str(docs_dir / "source"),
                str(built_site),
            ],
            cwd=docs_dir,
            env=environment,
            check=True,
        )
        adapt_generated_output(built_site, version, tag)
        return publish_snapshot(built_site, package_root, version)
    finally:
        if worktree_added:
            subprocess.run(
                ["git", "worktree", "remove", "--force", str(worktree)],
                cwd=REPOSITORY,
                check=False,
            )
        shutil.rmtree(temporary_parent, ignore_errors=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version")
    parser.add_argument("tag")
    parser.add_argument("package_root", type=Path)
    args = parser.parse_args()
    destination = build_snapshot(args.version, args.tag, args.package_root)
    print(f"snapshot created: {destination}")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
