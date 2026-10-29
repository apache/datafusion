# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

"""Sphinx configuration overlay for building docs from a release tag.

Loads ``conf.py`` from a``docs/source`` directory located at
``DATAFUSION_DOCS_SOURCE`` and overrides the following settings:
- the version string (``DATAFUSION_DOCS_VERSION``),
- the site base URL used for the sitemap
- the version switcher.

Why?

Documentation from older release tags do not have the version picker code and
expect their base URL to be ``/`` rather than ```versions/<version>/``, so we can
not publish docs built for the archived version without modification.

Rather than modifying the documentation in the old release branch,
the release process builds docs with the following command from a main
checkout:

sphinx-build -c docs/scripts/release
"""

import os
from pathlib import Path
import sys

source = Path(os.environ["DATAFUSION_DOCS_SOURCE"]).resolve()
sys.path.insert(0, str(source.parent))
exec(compile((source / "conf.py").read_bytes(), str(source / "conf.py"), "exec"))

# Resolve paths relative to the tag, not this configuration overlay.
templates_path = [str(source / path) for path in templates_path]
html_static_path = [str(source / path) for path in html_static_path]
html_extra_path = [str(source / path) for path in html_extra_path]
html_logo = str(source / html_logo)
html_favicon = str(source / html_favicon)

version = release = os.environ["DATAFUSION_DOCS_VERSION"]
html_baseurl = f"https://datafusion.apache.org/versions/{version}/"
sitemap_url_scheme = "{link}"
if "sphinx_sitemap" not in extensions:
    extensions.append("sphinx_sitemap")
html_context = {**html_context, "github_repo": "datafusion", "github_version": version}
html_theme_options = {
    **html_theme_options,
    "navbar_end": ["version-switcher", "theme-switcher"],
    "check_switcher": False,
    "switcher": {
        "json_url": "/_static/versions.json",
        "version_match": version,
    },
}
# The old tag's absolute redirect would leave the release documentation.
redirects = {**redirects, "library-user-guide/upgrading": "upgrading/index.html"}
