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

"""Use the tagged Sphinx configuration when manually building release docs."""

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
docs_base_url = os.environ.get("DATAFUSION_DOCS_BASE_URL", "https://datafusion.apache.org/")
docs_base_url = docs_base_url.rstrip("/") + "/"
html_baseurl = f"{docs_base_url}versions/{version}/"
sitemap_url_scheme = "{link}"
if "sphinx_sitemap" not in extensions:
    extensions.append("sphinx_sitemap")
html_context = {**html_context, "github_repo": "datafusion", "github_version": version}
html_theme_options = {
    **html_theme_options,
    "navbar_end": ["version-switcher", "theme-switcher"],
    "check_switcher": False,
    "switcher": {
        "json_url": docs_base_url + "_static/versions.json",
        "version_match": version,
    },
}
# The old tag's absolute redirect would leave the release documentation.
redirects = {**redirects, "library-user-guide/upgrading": "upgrading/index.html"}
