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

DataFusion is a very fast, extensible query engine for building high-quality data-centric systems in
`Rust <http://rustlang.org>`_, using the `Apache Arrow <https://arrow.apache.org>`_
in-memory format.

DataFusion offers SQL and Dataframe APIs, excellent
`performance <https://benchmark.clickhouse.com>`_, built-in support for
CSV, Parquet, JSON, and Avro, extensive customization, and a great
community.

The `example usage`_ section in the user guide and the `datafusion-examples`_ code in the crate contain information on using DataFusion.

The `developer’s guide`_ contains information on how to contribute.

.. _example usage: user-guide/example-usage.html
.. _datafusion-examples: https://github.com/apache/arrow-datafusion/tree/master/datafusion-examples
.. _developer’s guide: contributor-guide/index.html#developer-s-guide

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
