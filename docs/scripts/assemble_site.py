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

"""Replace the current site while retaining immutable full-site snapshots."""

from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import sys
import tempfile
import xml.etree.ElementTree as ET

from versioned_docs import SITE_URL, VERSION_PATTERN, is_link, reject_links


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--current-site", type=Path, required=True)
    parser.add_argument("--published-site", type=Path, required=True)
    parser.add_argument("--output-site", type=Path, required=True)
    return parser.parse_args()


def validate_paths(current_site: Path, published_site: Path, output_site: Path) -> None:
    if not current_site.is_dir():
        raise RuntimeError(f"current site does not exist: {current_site}")
    if not published_site.is_dir():
        raise RuntimeError(f"published site does not exist: {published_site}")
    if not (current_site / "index.html").is_file():
        raise RuntimeError(f"current site has no index.html: {current_site}")
    if is_link(output_site):
        raise RuntimeError(f"output site must not be a link: {output_site}")
    for left, right in (
        (current_site, output_site),
        (published_site, output_site),
    ):
        left = left.resolve()
        right = right.resolve()
        if left == right or left.is_relative_to(right) or right.is_relative_to(left):
            raise RuntimeError(
                "output site and input sites must not contain one another"
            )
    if output_site.exists():
        raise RuntimeError(f"output site already exists: {output_site}")


def archived_versions(published_site: Path) -> list[str]:
    versions_root = published_site / "versions"
    if not versions_root.exists():
        return []
    if is_link(versions_root) or not versions_root.is_dir():
        raise RuntimeError(f"published versions must be a directory: {versions_root}")
    versions: list[str] = []
    for child in versions_root.iterdir():
        if is_link(child) or not child.is_dir():
            raise RuntimeError(f"invalid published version directory: {child}")
        if not VERSION_PATTERN.fullmatch(child.name):
            raise RuntimeError(f"unsafe published version directory name: {child.name}")
        if not (child / "index.html").is_file():
            raise RuntimeError(f"published version has no index.html: {child}")
        if not (child / "sitemap.xml").is_file():
            raise RuntimeError(f"published version has no sitemap.xml: {child}")
        reject_links(child)
        versions.append(child.name)
    return sorted(versions, key=lambda value: tuple(map(int, value.split("."))))


def write_sitemap_index(site: Path, versions: list[str]) -> None:
    current_sitemap = site / "sitemap.xml"
    if not current_sitemap.is_file():
        raise RuntimeError(f"current site sitemap is missing: {current_sitemap}")
    current_sitemap.rename(site / "sitemap-main.xml")
    locations = [f"{SITE_URL}/sitemap-main.xml"] + [
        f"{SITE_URL}/versions/{version}/sitemap.xml" for version in versions
    ]
    root = ET.Element(
        "sitemapindex", xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
    )
    for location in locations:
        sitemap = ET.SubElement(root, "sitemap")
        ET.SubElement(sitemap, "loc").text = location
    ET.indent(root, space="  ")
    ET.ElementTree(root).write(current_sitemap, encoding="utf-8", xml_declaration=True)


def assemble(current_site: Path, published_site: Path, output_site: Path) -> None:
    validate_paths(current_site, published_site, output_site)
    current_site = current_site.resolve()
    published_site = published_site.resolve()
    output_site = output_site.resolve()
    versions = archived_versions(published_site)
    output_site.parent.mkdir(parents=True, exist_ok=True)
    temporary_root = Path(
        tempfile.mkdtemp(prefix=f".{output_site.name}-", dir=output_site.parent)
    )
    staged_site = temporary_root / "site"
    try:
        shutil.copytree(current_site, staged_site)
        if versions:
            shutil.copytree(published_site / "versions", staged_site / "versions")
            write_sitemap_index(staged_site, versions)
        if output_site.exists() or is_link(output_site):
            raise RuntimeError(f"output site already exists: {output_site}")
        staged_site.rename(output_site)
    finally:
        shutil.rmtree(temporary_root, ignore_errors=True)


if __name__ == "__main__":
    try:
        args = parse_args()
        assemble(args.current_site, args.published_site, args.output_site)
    except (OSError, RuntimeError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
