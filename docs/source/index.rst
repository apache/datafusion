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

.. Code from https://buttons.github.io/
.. raw:: html

  <p>
    <!-- Place this tag where you want the button to render. -->
    <a class="github-button" href="https://github.com/apache/datafusion" data-size="large" data-show-count="true" aria-label="Star apache/datafusion on GitHub">Star</a>
    <!-- Place this tag where you want the button to render. -->
     <a class="github-button" href="https://github.com/apache/datafusion/fork" data-size="large" data-show-count="true" aria-label="Fork apache/datafusion on GitHub">Fork</a>
  </p>


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
   library-user-guide/api-health
.. _toc.contributor-guide:

.. toctree::
   :maxdepth: 1
   :caption: Contributor Guide

   contributor-guide/index
   contributor-guide/communication
   contributor-guide/getting_started
   contributor-guide/architecture
   contributor-guide/testing
   contributor-guide/howtos
   contributor-guide/roadmap
   contributor-guide/governance
   contributor-guide/inviting
   contributor-guide/specification/index
   contributor-guide/gsoc_application_guidelines

.. _toc.subprojects:

.. toctree::
   :maxdepth: 1
   :caption: DataFusion Subprojects

   DataFusion Ballista <https://arrow.apache.org/ballista/>
   DataFusion Comet <https://datafusion.apache.org/comet/>
   DataFusion Python <https://datafusion.apache.org/python/>
