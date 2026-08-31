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

"""Validate current and immutable full-site documentation output."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from functools import partial
from html.parser import HTMLParser
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import re
import sys
import threading
from typing import Iterator
import xml.etree.ElementTree as ET
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urljoin, urlsplit
from urllib.request import Request, urlopen

from versioned_docs import SITE_URL, load_versions

PICKER_URL = re.compile(
    r"DOCUMENTATION_OPTIONS\.theme_switcher_json_url\s*=\s*['\"]([^'\"]+)"
)
PICKER_VERSION = re.compile(
    r"DOCUMENTATION_OPTIONS\.theme_switcher_version_match\s*=\s*['\"]([^'\"]+)"
)
MUTABLE_RELEASE_LINK = re.compile(
    r"https://(?:"
    r"github\.com/apache/(?:arrow-)?datafusion/(?:blob|tree)/main(?:/|[\"'#<)\s]|$)|"
    r"docs\.rs/datafusion(?:-[a-z0-9-]+)?/latest(?:/|[\"'#<])"
    r")"
)
CSS_URL = re.compile(r"url\(\s*(['\"]?)(.*?)\1\s*\)")
META_REFRESH_URL = re.compile(r"(?:^|;)\s*url\s*=\s*(['\"]?)(.*?)\1\s*$", re.I)


class ValidationError(RuntimeError):
    pass


class HTMLReferences(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.references: list[str] = []
        self.canonicals: list[str] = []
        self.ids: set[str] = set()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = {name.lower(): value for name, value in attrs if value is not None}
        for attribute in ("href", "src", "action", "poster", "data"):
            if attribute in values:
                self.references.append(values[attribute])
        for value in values.get("srcset", "").split(","):
            if value.strip():
                self.references.append(value.strip().split()[0])
        if tag.lower() == "meta" and values.get("http-equiv", "").lower() == "refresh":
            refresh = META_REFRESH_URL.search(values.get("content", ""))
            if refresh:
                self.references.append(refresh.group(2))
        for identifier in (values.get("id"), values.get("name")):
            if identifier:
                self.ids.add(identifier)
        if tag.lower() == "link" and "canonical" in values.get("rel", "").split():
            self.canonicals.append(values["href"])


def read_html(path: Path) -> tuple[str, HTMLReferences]:
    text = path.read_text(encoding="utf-8")
    parsed = HTMLReferences()
    parsed.feed(text)
    return text, parsed


def site_target(site_root: Path, page: Path, url: str) -> Path | None:
    parsed = urlsplit(url)
    if parsed.scheme not in ("", "http", "https"):
        return None
    if parsed.netloc and parsed.netloc != urlsplit(SITE_URL).netloc:
        return None
    if parsed.netloc and parsed.path.startswith(
        ("/blog/", "/python/", "/java/", "/comet/", "/ballista/")
    ):
        return None
    if not parsed.path:
        return page
    target = (
        site_root / unquote(parsed.path.lstrip("/"))
        if parsed.netloc or parsed.path.startswith("/")
        else page.parent / unquote(parsed.path)
    ).resolve()
    try:
        target.relative_to(site_root.resolve())
    except ValueError as error:
        raise ValidationError(
            f"local target escapes site root: {page}: {url}"
        ) from error
    if parsed.path.endswith("/"):
        target /= "index.html"
    return target


def switch_candidate(page: str, source_version: str, target_url: str) -> str:
    path = urlsplit(page).path.lstrip("/")
    prefix = "" if source_version == "main" else f"versions/{source_version}/"
    if not path.startswith(prefix):
        raise ValidationError(f"page is outside its version prefix: {page}")
    relative = path[len(prefix) :]
    candidate = urljoin(target_url, relative)
    if f"/versions/{source_version}/versions/" in candidate:
        raise ValidationError(f"version path is duplicated: {candidate}")
    return candidate


def validate_page(
    site_root: Path, relative: str, version: str, errors: list[str]
) -> None:
    path = site_root / relative
    if not path.is_file():
        errors.append(f"representative page is missing: {path}")
        return
    text, parsed = read_html(path)
    canonical = SITE_URL + "/" + relative.replace("\\", "/")
    if parsed.canonicals != [canonical]:
        errors.append(f"incorrect canonical in {path}: expected {canonical}")
    picker_url = PICKER_URL.search(text)
    if picker_url is None or picker_url.group(1) != f"{SITE_URL}/_static/versions.json":
        errors.append(f"incorrect picker manifest URL in {path}")
    picker_version = PICKER_VERSION.search(text)
    if picker_version is None or picker_version.group(1) != version:
        errors.append(f"incorrect picker version in {path}")


def validate_local_links(site_root: Path, root: Path, errors: list[str]) -> None:
    parsed_pages: dict[Path, HTMLReferences] = {}
    for page in root.rglob("*.html"):
        if "_static" in page.relative_to(root).parts:
            continue
        _, parsed = read_html(page)
        parsed_pages[page.resolve()] = parsed
        expected_canonical = f"{SITE_URL}/{page.relative_to(site_root).as_posix()}"
        if parsed.canonicals and parsed.canonicals != [expected_canonical]:
            errors.append(
                f"incorrect canonical in {page}: expected {expected_canonical}"
            )
        for url in parsed.references:
            parsed_url = urlsplit(url)
            if parsed_url.netloc == urlsplit(SITE_URL).netloc and root == site_root:
                # Current-source absolute links are checked by the repository's
                # Markdown link checker, not by snapshot hosting validation.
                continue
            try:
                target = site_target(site_root, page, url)
            except ValidationError as error:
                errors.append(str(error))
                continue
            if target is not None and not target.exists():
                errors.append(f"missing local target in {page}: {url}")
                continue
            fragment = urlsplit(url).fragment
            if target is not None and fragment and target.suffix == ".html":
                target_parsed = parsed_pages.get(target.resolve())
                if target_parsed is None:
                    _, target_parsed = read_html(target)
                    parsed_pages[target.resolve()] = target_parsed
                if unquote(fragment) not in target_parsed.ids:
                    errors.append(f"missing local fragment in {page}: {url}")
    for stylesheet in root.rglob("*.css"):
        text = stylesheet.read_text(encoding="utf-8")
        for _, url in CSS_URL.findall(text):
            target = site_target(site_root, stylesheet, url)
            if target is not None and not target.exists():
                errors.append(f"missing CSS target in {stylesheet}: {url}")


def validate_sitemaps(site_root: Path, versions: list[str], errors: list[str]) -> None:
    namespace = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    if versions:
        try:
            tree = ET.parse(site_root / "sitemap.xml")
        except (OSError, ET.ParseError) as error:
            errors.append(f"invalid sitemap index: {error}")
            return
        locations = [item.text for item in tree.findall("sm:sitemap/sm:loc", namespace)]
        expected = [f"{SITE_URL}/sitemap-main.xml"] + [
            f"{SITE_URL}/versions/{version}/sitemap.xml" for version in versions
        ]
        if locations != expected:
            errors.append("sitemap index has incorrect entries")
    for version in versions:
        sitemap = site_root / "versions" / version / "sitemap.xml"
        try:
            tree = ET.parse(sitemap)
        except (OSError, ET.ParseError) as error:
            errors.append(f"invalid release sitemap {sitemap}: {error}")
            continue
        prefix = f"{SITE_URL}/versions/{version}/"
        for location in tree.findall("sm:url/sm:loc", namespace):
            if location.text is None or not location.text.startswith(prefix):
                errors.append(f"release sitemap URL escapes {prefix}: {location.text}")


class QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        pass


@contextmanager
def local_server(site_root: Path) -> Iterator[str]:
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0), partial(QuietHandler, directory=str(site_root))
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def validate_http(site_root: Path, paths: list[str], errors: list[str]) -> None:
    with local_server(site_root) as server:
        for relative in paths:
            for method in ("GET", "HEAD"):
                try:
                    with urlopen(
                        Request(f"{server}/{relative}", method=method), timeout=10
                    ) as response:
                        if response.status != 200:
                            errors.append(
                                f"HTTP {method} {relative}: {response.status}"
                            )
                except (HTTPError, URLError, TimeoutError) as error:
                    errors.append(f"HTTP {method} {relative} failed: {error}")


def validate_site(
    site_root: Path, *, require_snapshots: bool = False, http_checks: bool = True
) -> None:
    site_root = site_root.resolve()
    if not site_root.is_dir():
        raise ValidationError(f"site root does not exist: {site_root}")
    entries = load_versions()
    errors: list[str] = []
    manifest = site_root / "_static" / "versions.json"
    if (
        not manifest.is_file()
        or json.loads(manifest.read_text(encoding="utf-8")) != entries
    ):
        errors.append(f"site has a missing or stale versions manifest: {manifest}")

    releases = [str(entry["version"]) for entry in entries[1:]]
    versions_root = site_root / "versions"
    present = []
    if versions_root.is_dir():
        present = sorted(
            (
                child.name
                for child in versions_root.iterdir()
                if child.is_dir()
                and re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", child.name)
            ),
            key=lambda value: tuple(map(int, value.split("."))),
        )
    if require_snapshots and not set(releases).issubset(present):
        errors.append("required release snapshots are missing")

    representatives = (
        "index.html",
        "user-guide/introduction.html",
        "library-user-guide/index.html",
        "contributor-guide/index.html",
        "download.html",
        "search.html",
        "_static/theme_overrides.css",
        "_sources/index.rst.txt",
        "llms.txt",
    )
    redirects = (
        "library-user-guide/adding-udfs.html",
        "library-user-guide/upgrading.html",
        "user-guide/runtime_configs.html",
    )
    http_paths = ["_static/versions.json"]
    for version in ["main", *present]:
        prefix = "" if version == "main" else f"versions/{version}/"
        root = site_root if version == "main" else site_root / "versions" / version
        if not (root / "searchindex.js").is_file():
            errors.append(f"search index is missing: {root}")
        elif (root / "searchindex.js").stat().st_size == 0:
            errors.append(f"search index is empty: {root}")
        if not (root / "objects.inv").is_file():
            errors.append(f"Sphinx inventory is missing: {root}")
        elif (root / "objects.inv").stat().st_size == 0:
            errors.append(f"Sphinx inventory is empty: {root}")
        for representative in representatives:
            relative = prefix + representative
            if representative.endswith((".html",)):
                validate_page(site_root, relative, version, errors)
            elif not (site_root / relative).is_file():
                errors.append(f"representative file is missing: {relative}")
            elif (site_root / relative).stat().st_size == 0:
                errors.append(f"representative file is empty: {relative}")
            http_paths.append(relative)
        for redirect in redirects:
            relative = prefix + redirect
            if not (site_root / relative).is_file():
                errors.append(f"redirect is missing: {relative}")
            http_paths.append(relative)
        validate_local_links(site_root, root, errors)

        page = f"{SITE_URL}/{prefix}user-guide/sql/select.html"
        for entry in entries:
            candidate = switch_candidate(page, version, str(entry["url"]))
            if "/versions/55.0.0/versions/" in candidate:
                errors.append(f"picker duplicates a versions path: {candidate}")

        if version != "main":
            release_files = [*root.rglob("*.html"), root / "llms.txt"]
            for page_path in release_files:
                if page_path.suffix == ".html":
                    text, parsed = read_html(page_path)
                else:
                    text = page_path.read_text(encoding="utf-8")
                    parsed = HTMLReferences()
                mutable = MUTABLE_RELEASE_LINK.search(text)
                if mutable:
                    errors.append(
                        f"mutable release link in {page_path}: {mutable.group(0)}"
                    )
                for url in parsed.references:
                    parsed_url = urlsplit(url)
                    if (
                        parsed_url.netloc == urlsplit(SITE_URL).netloc
                        and parsed_url.path.startswith(
                            (
                                "/user-guide/",
                                "/library-user-guide/",
                                "/contributor-guide/",
                            )
                        )
                        and (site_root / parsed_url.path.lstrip("/")).exists()
                    ):
                        errors.append(
                            f"release documentation link escapes in {page_path}: {url}"
                        )

    current_only = site_root / "library-user-guide" / "upgrading" / "56.0.0.html"
    release_current_only = (
        site_root
        / "versions"
        / "55.0.0"
        / "library-user-guide"
        / "upgrading"
        / "56.0.0.html"
    )
    if not current_only.is_file():
        errors.append(f"current-only page is missing: {current_only}")
    if release_current_only.exists():
        errors.append(f"release contains current-only page: {release_current_only}")
    if "55.0.0" in present:
        tagged_source = (
            site_root
            / "versions"
            / "55.0.0"
            / "_sources"
            / "library-user-guide"
            / "upgrading"
            / "55.0.0.md.txt"
        )
        if (
            not tagged_source.is_file()
            or "DataFusion `55.0.0` has not been released yet."
            not in tagged_source.read_text(encoding="utf-8")
        ):
            errors.append("exact tagged 55.0.0 release-note prose was not preserved")

    validate_sitemaps(site_root, present, errors)
    if http_checks and not errors:
        validate_http(site_root, http_paths, errors)
    if errors:
        raise ValidationError("site validation failed:\n- " + "\n- ".join(errors))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--site-root", type=Path, required=True)
    parser.add_argument("--require-snapshots", action="store_true")
    args = parser.parse_args()
    validate_site(args.site_root, require_snapshots=args.require_snapshots)
    print("site validation passed")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, ValidationError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
