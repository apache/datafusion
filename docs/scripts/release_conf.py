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

"""Minimal Sphinx configuration overlay for a full release site."""

from __future__ import annotations

import os
from pathlib import Path
import sys

source_dir = Path(os.environ["DATAFUSION_RELEASE_SOURCE"])
docs_dir = source_dir.parent
sys.path.insert(0, str(docs_dir))
tagged_conf = source_dir / "conf.py"
exec(compile(tagged_conf.read_bytes(), str(tagged_conf), "exec"))

version = release = os.environ["DATAFUSION_RELEASE_VERSION"]
base_url = f"https://datafusion.apache.org/versions/{version}/"
html_baseurl = base_url
sitemap_url_scheme = "{link}"
extensions = [*extensions, "sphinx_sitemap"]
templates_path = [str(source_dir / path) for path in templates_path]
html_static_path = [str(source_dir / path) for path in html_static_path]
html_extra_path = [str(source_dir / path) for path in html_extra_path]
html_logo = str(source_dir / html_logo)
html_favicon = str(source_dir / html_favicon)

html_context = dict(html_context)
html_context.update(
    github_user="apache",
    github_repo="datafusion",
    github_version=os.environ["DATAFUSION_RELEASE_TAG"],
)
html_theme_options = dict(html_theme_options)
html_theme_options.update(
    check_switcher=False,
    navbar_end=["version-switcher", "theme-switcher"],
    switcher={
        "json_url": "https://datafusion.apache.org/_static/versions.json",
        "version_match": version,
    },
)

redirects = dict(redirects)
redirects["library-user-guide/upgrading"] = "upgrading/index.html"
