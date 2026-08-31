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

"""Shared catalog handling for full-site documentation snapshots."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import stat

DOCS_DIR = Path(__file__).resolve().parent.parent
MANIFEST = DOCS_DIR / "source" / "_static" / "versions.json"
SITE_URL = "https://datafusion.apache.org"
VERSION_PATTERN = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+")
EXPECTED_55_COMMIT = "d5552342012888b7d1a3ab88d92e3d292fc0cde0"


def is_link(path: Path) -> bool:
    """Return whether a path is a symlink or Windows reparse point."""
    try:
        status = path.lstat()
    except FileNotFoundError:
        return False
    reparse_point = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    file_attributes = getattr(status, "st_file_attributes", 0)
    return stat.S_ISLNK(status.st_mode) or bool(file_attributes & reparse_point)


def reject_links(root: Path) -> None:
    """Reject links before copying an immutable archive tree."""
    if is_link(root):
        raise RuntimeError(f"archived path must not be a link: {root}")
    for directory, directories, files in os.walk(root, followlinks=False):
        for name in [*directories, *files]:
            path = Path(directory) / name
            if is_link(path):
                raise RuntimeError(f"archived path must not be a link: {path}")


def load_versions(path: Path = MANIFEST) -> list[dict[str, object]]:
    try:
        entries = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read versions manifest {path}: {error}") from error
    if not isinstance(entries, list) or not entries:
        raise ValueError("versions manifest must contain a non-empty list")

    versions: set[str] = set()
    urls: set[str] = set()
    preferred = 0
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise ValueError("each versions manifest entry must be an object")
        version = entry.get("version")
        url = entry.get("url")
        if not isinstance(version, str) or version in versions:
            raise ValueError(f"invalid or duplicate version: {version!r}")
        if not isinstance(url, str) or url in urls:
            raise ValueError(f"invalid or duplicate version URL: {url!r}")
        versions.add(version)
        urls.add(url)
        preferred += entry.get("preferred") is True

        if index == 0:
            if entry != {
                "name": "Development",
                "url": f"{SITE_URL}/",
                "version": "main",
            }:
                raise ValueError(
                    "the first entry must be Development/main at site root"
                )
            continue
        if not VERSION_PATTERN.fullmatch(version):
            raise ValueError(f"invalid semantic release version: {version!r}")
        expected_url = f"{SITE_URL}/versions/{version}/"
        if entry.get("name") != version or url != expected_url:
            raise ValueError(f"release {version} has an invalid name or URL")
        if entry.get("tag") != version or not re.fullmatch(
            r"[0-9a-f]{40}", str(entry.get("commit", ""))
        ):
            raise ValueError(f"release {version} must have an exact tag and commit")
    if preferred > 1:
        raise ValueError("at most one versions manifest entry may be preferred")

    release_55 = next(
        (entry for entry in entries if entry["version"] == "55.0.0"), None
    )
    if release_55 is None or release_55.get("commit") != EXPECTED_55_COMMIT:
        raise ValueError("55.0.0 must resolve to its expected exact commit")
    return entries


def release_entry(version: str, tag: str) -> dict[str, object]:
    for entry in load_versions():
        if entry["version"] == version and entry.get("tag") == tag:
            return entry
    raise RuntimeError("version and tag must exactly match versions.json")
