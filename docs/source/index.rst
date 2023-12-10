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

.. image:: _static/images/DataFusion-Logo-Background-White.png
  :alt: DataFusion Logo

=======================
Apache Arrow DataFusion
=======================

.. Code from https://buttons.github.io/
.. raw:: html

  <p>
    <!-- Place this tag where you want the button to render. -->
    <a class="github-button" href="https://github.com/apache/arrow-datafusion" data-size="large" data-show-count="true" aria-label="Star apache/arrow-datafusion on GitHub">Star</a>
    <!-- Place this tag where you want the button to render. -->
     <a class="github-button" href="https://github.com/apache/arrow-datafusion/fork" data-size="large" data-show-count="true" aria-label="Fork apache/arrow-datafusion on GitHub">Fork</a>
  </p>

DataFusion is a very fast, extensible query engine for building high-quality data-centric systems in
`Rust <http://rustlang.org>`_, using the `Apache Arrow <https://arrow.apache.org>`_
in-memory format.

DataFusion offers SQL and Dataframe APIs, excellent
`performance <https://benchmark.clickhouse.com>`_, built-in support for
CSV, Parquet, JSON, and Avro, extensive customization, and a great
community.

The `example usage`_ section in the user guide and the `datafusion-examples`_ code in the crate contain information on using DataFusion.

Please see the `developer’s guide`_ for contributing and `communication`_ for getting in touch with us.

.. _example usage: user-guide/example-usage.html
.. _datafusion-examples: https://github.com/apache/arrow-datafusion/tree/master/datafusion-examples
.. _developer’s guide: contributor-guide/index.html#developer-s-guide
.. _communication: contributor-guide/communication.html

.. _toc.links:
.. toctree::
   :maxdepth: 1
   :caption: Links

   Github and Issue Tracker <https://github.com/apache/arrow-datafusion>
   crates.io <https://crates.io/crates/datafusion>
   API Docs <https://docs.rs/datafusion/latest/datafusion/>
   Code of conduct <https://github.com/apache/arrow-datafusion/blob/main/CODE_OF_CONDUCT.md>

.. _toc.guide:
.. toctree::
   :maxdepth: 1
   :caption: User Guide

   user-guide/introduction
   user-guide/example-usage
   user-guide/cli
   user-guide/dataframe
   user-guide/expressions
   user-guide/sql/index
   user-guide/configs
   user-guide/faq

.. _toc.library-user-guide:

.. toctree::
   :maxdepth: 1
   :caption: Library User Guide

   library-user-guide/index
   library-user-guide/using-the-sql-api
   library-user-guide/working-with-exprs
   library-user-guide/using-the-dataframe-api
   library-user-guide/building-logical-plans
   library-user-guide/catalogs
   library-user-guide/adding-udfs
   library-user-guide/custom-table-providers
   library-user-guide/extending-operators

.. _toc.contributor-guide:

.. toctree::
   :maxdepth: 1
   :caption: Contributor Guide

   contributor-guide/index
   contributor-guide/communication
   contributor-guide/architecture
   contributor-guide/roadmap
   contributor-guide/quarterly_roadmap
   contributor-guide/specification/index
