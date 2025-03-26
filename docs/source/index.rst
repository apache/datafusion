.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

.. image:: _static/images/2x_bgwhite_original.png
  :alt: DataFusion Logo

=================
Apache DataFusion
=================

.. raw:: html

   <style type="text/css">
      .custom-social-badge{display:inline;padding:4px}.custom-social-badge a{text-decoration:none;outline:0}.custom-social-badge .widget{display:inline-block;overflow:hidden;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Helvetica,Arial,sans-serif;font-size:0;line-height:0;white-space:nowrap}.custom-social-badge .btn:not(:last-child){border-radius:.25em 0 0 .25em}.custom-social-badge .widget-lg .btn{height:28px;padding:5px 10px;font-size:12px;line-height:16px}.custom-social-badge .btn{border-radius:.25em;color:#25292e;background-color:#ebf0f4;border-color:#d1d9e0;background-image:url("data:image/svg+xml,%3csvg xmlns='http://www.w3.org/2000/svg'%3e%3clinearGradient id='o' x2='0' y2='1'%3e%3cstop stop-color='%23f6f8fa'/%3e%3cstop offset='90%25' stop-color='%23ebf0f4'/%3e%3c/linearGradient%3e%3crect width='100%25' height='100%25' fill='url(%23o)'/%3e%3c/svg%3e");background-image:-moz-linear-gradient(top,#f6f8fa,#ebf0f4 90%);background-image:linear-gradient(180deg,#f6f8fa,#ebf0f4 90%);filter:progid:DXImageTransform.Microsoft.Gradient(startColorstr='#FFF6F8FA',endColorstr='#FFEAEFF3');filter:none;position:relative;display:inline-block;display:inline-flex;height:14px;padding:2px 5px;font-size:11px;font-weight:600;line-height:14px;vertical-align:bottom;cursor:pointer;-webkit-user-select:none;-moz-user-select:none;-ms-user-select:none;user-select:none;background-repeat:repeat-x;background-position:-1px -1px;background-size:110% 110%}.custom-social-badge .btn:hover,.custom-social-badge .btn:focus{background-color:#e5eaee;background-position:0 -.5em;border-color:#d1d9e0;background-image:url("data:image/svg+xml,%3csvg xmlns='http://www.w3.org/2000/svg'%3e%3clinearGradient id='o' x2='0' y2='1'%3e%3cstop stop-color='%23eff2f5'/%3e%3cstop offset='90%25' stop-color='%23e5eaee'/%3e%3c/linearGradient%3e%3crect width='100%25' height='100%25' fill='url(%23o)'/%3e%3c/svg%3e");background-image:-moz-linear-gradient(top,#eff2f5,#e5eaee 90%);background-image:linear-gradient(180deg,#eff2f5,#e5eaee 90%);filter:progid:DXImageTransform.Microsoft.Gradient(startColorstr='#FFEFF2F5',endColorstr='#FFE4E9ED')}.custom-social-badge .btn:hover,.custom-social-badge .btn:focus{filter:none}.custom-social-badge .btn:active{background-color:#e6eaef;border-color:#d1d9e0;background-image:none;filter:none}
   </style>
   <div class="custom-social-badge">
      <span>
         <div class="widget widget-lg"><a class="btn" href="https://github.com/apache/datafusion" rel="noopener" target="_blank" target="_blank" aria-label="Star Apache Arrow on GitHub"><svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 16 16" fill="none" role="img" aria-hidden="true"><path d="M8 0c4.42 0 8 3.58 8 8a8.013 8.013 0 0 1-5.45 7.59c-.4.08-.55-.17-.55-.38 0-.27.01-1.13.01-2.2 0-.75-.25-1.23-.54-1.48 1.78-.2 3.65-.88 3.65-3.95 0-.88-.31-1.59-.82-2.15.08-.2.36-1.02-.08-2.12 0 0-.67-.22-2.2.82-.64-.18-1.32-.27-2-.27-.68 0-1.36.09-2 .27-1.53-1.03-2.2-.82-2.2-.82-.44 1.1-.16 1.92-.08 2.12-.51.56-.82 1.28-.82 2.15 0 3.06 1.86 3.75 3.64 3.95-.23.2-.44.55-.51 1.07-.46.21-1.61.55-2.33-.66-.15-.24-.6-.83-1.23-.82-.67.01-.27.38.01.53.34.19.73.9.82 1.13.16.45.68 1.31 2.69.94 0 .67.01 1.3.01 1.49 0 .21-.15.45-.55.38A7.995 7.995 0 0 1 0 8c0-4.42 3.58-8 8-8Z" fill="#000"></path></svg>&nbsp;<span>Star</span></a></div>
      </span>
   </div>
      <div class="custom-social-badge">
      <span>
         <div class="widget widget-lg"><a class="btn" href="https://github.com/apache/datafusion/fork" rel="noopener" target="_blank" target="_blank" aria-label="Star Apache Arrow on GitHub"><svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 16 16" fill="none" role="img" aria-hidden="true"><path d="M8 0c4.42 0 8 3.58 8 8a8.013 8.013 0 0 1-5.45 7.59c-.4.08-.55-.17-.55-.38 0-.27.01-1.13.01-2.2 0-.75-.25-1.23-.54-1.48 1.78-.2 3.65-.88 3.65-3.95 0-.88-.31-1.59-.82-2.15.08-.2.36-1.02-.08-2.12 0 0-.67-.22-2.2.82-.64-.18-1.32-.27-2-.27-.68 0-1.36.09-2 .27-1.53-1.03-2.2-.82-2.2-.82-.44 1.1-.16 1.92-.08 2.12-.51.56-.82 1.28-.82 2.15 0 3.06 1.86 3.75 3.64 3.95-.23.2-.44.55-.51 1.07-.46.21-1.61.55-2.33-.66-.15-.24-.6-.83-1.23-.82-.67.01-.27.38.01.53.34.19.73.9.82 1.13.16.45.68 1.31 2.69.94 0 .67.01 1.3.01 1.49 0 .21-.15.45-.55.38A7.995 7.995 0 0 1 0 8c0-4.42 3.58-8 8-8Z" fill="#000"></path></svg>&nbsp;<span>Fork</span></a></div>
      </span>
   </div>

DataFusion is an extensible query engine written in `Rust <http://rustlang.org>`_ that
uses `Apache Arrow <https://arrow.apache.org>`_ as its in-memory format.

The documentation on this site is for the `core DataFusion project <https://github.com/apache/datafusion>`_, which contains
libraries and binaries for developers building fast and feature rich database and analytic systems,
customized to particular workloads. See `use cases <https://datafusion.apache.org/user-guide/introduction.html#use-cases>`_ for examples.

The following related subprojects target end users and have separate documentation.

- `DataFusion Python <https://datafusion.apache.org/python/>`_ offers a Python interface for SQL and DataFrame
  queries.
- `DataFusion Ray <https://github.com/apache/datafusion-ray/>`_ provides a distributed version of DataFusion
  that scales out on `Ray <https://www.ray.io>`_ clusters.
- `DataFusion Comet <https://datafusion.apache.org/comet/>`_ is an accelerator for Apache Spark based on
  DataFusion.

"Out of the box," DataFusion offers `SQL <https://datafusion.apache.org/user-guide/sql/index.html>`_
and `Dataframe <https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html>`_ APIs,
excellent `performance <https://benchmark.clickhouse.com/>`_, built-in support for CSV, Parquet, JSON, and Avro,
extensive customization, and a great community.
`Python Bindings <https://github.com/apache/datafusion-python>`_ are also available.

DataFusion features a full query planner, a columnar, streaming, multi-threaded,
vectorized execution engine, and partitioned data sources. You can
customize DataFusion at almost all points including additional data sources,
query languages, functions, custom operators and more.
See the `Architecture <https://datafusion.apache.org/contributor-guide/architecture.html>`_ section for more details.

To get started, see

* The `example usage`_ section of the user guide and the `datafusion-examples`_ directory.
* The `library user guide`_ for examples of using DataFusion's extension APIs
* The `developer’s guide`_ for contributing and `communication`_ for getting in touch with us.

.. _example usage: user-guide/example-usage.html
.. _datafusion-examples: https://github.com/apache/datafusion/tree/main/datafusion-examples
.. _developer’s guide: contributor-guide/index.html#developer-s-guide
.. _library user guide: library-user-guide/index.html
.. _communication: contributor-guide/communication.html

.. _toc.asf-links:
.. toctree::
   :maxdepth: 1
   :caption: ASF Links

   Apache Software Foundation <https://apache.org>
   License <https://www.apache.org/licenses/>
   Donate <https://www.apache.org/foundation/sponsorship.html>
   Thanks <https://www.apache.org/foundation/thanks.html>
   Security <https://www.apache.org/security/>

.. _toc.links:
.. toctree::
   :maxdepth: 1
   :caption: Links

   GitHub and Issue Tracker <https://github.com/apache/datafusion>
   crates.io <https://crates.io/crates/datafusion>
   API Docs <https://docs.rs/datafusion/latest/datafusion/>
   Blog <https://datafusion.apache.org/blog/>
   Code of conduct <https://github.com/apache/datafusion/blob/main/CODE_OF_CONDUCT.md>
   Download <download>

.. _toc.guide:
.. toctree::
   :maxdepth: 1
   :caption: User Guide

   user-guide/introduction
   user-guide/example-usage
   user-guide/features
   user-guide/concepts-readings-events
   user-guide/crate-configuration
   user-guide/cli/index
   user-guide/dataframe
   user-guide/expressions
   user-guide/sql/index
   user-guide/configs
   user-guide/explain-usage
   user-guide/faq

.. _toc.library-user-guide:

.. toctree::
   :maxdepth: 1
   :caption: Library User Guide
   
   library-user-guide/index
   library-user-guide/extensions
   library-user-guide/using-the-sql-api
   library-user-guide/working-with-exprs
   library-user-guide/using-the-dataframe-api
   library-user-guide/building-logical-plans
   library-user-guide/catalogs
   library-user-guide/adding-udfs
   library-user-guide/custom-table-providers
   library-user-guide/extending-operators
   library-user-guide/profiling
   library-user-guide/query-optimizer
   library-user-guide/upgrading

.. .. _toc.contributor-guide:

.. toctree::
   :maxdepth: 1
   :caption: Contributor Guide

   contributor-guide/index
   contributor-guide/communication
   contributor-guide/development_environment
   contributor-guide/architecture
   contributor-guide/testing
   contributor-guide/api-health
   contributor-guide/howtos
   contributor-guide/roadmap
   contributor-guide/governance
   contributor-guide/inviting
   contributor-guide/specification/index
   contributor-guide/gsoc_application_guidelines
   contributor-guide/gsoc_project_ideas

.. _toc.subprojects:

.. toctree::
   :maxdepth: 1
   :caption: DataFusion Subprojects

   DataFusion Ballista <https://arrow.apache.org/ballista/>
   DataFusion Comet <https://datafusion.apache.org/comet/>
   DataFusion Python <https://datafusion.apache.org/python/>
