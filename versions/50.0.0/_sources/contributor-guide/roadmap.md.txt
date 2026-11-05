<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Roadmap and Improvement Proposals

The [project introduction](../user-guide/introduction) explains the
overview and goals of DataFusion, and our development efforts largely
align to that vision.

## Planning `EPIC`s

DataFusion uses [GitHub
issues](https://github.com/apache/datafusion/issues) to track
planned work. We collect related tickets using tracking issues labeled
with `[EPIC]` which contain discussion and links to more detailed items.

Epics offer a high level roadmap of what the DataFusion
community is thinking about. The epics are not meant to restrict
possibilities, but rather help the community see where development is
headed, align our work, and inspire additional contributions.

As this project is entirely driven by volunteers, we welcome
contributions for items not currently covered by epics. However,
before submitting a large PR, we strongly suggest and request you
start a conversation using a github issue or the
[dev@arrow.apache.org](mailto:dev@arrow.apache.org) mailing list to
make review efficient and avoid surprises.

[The current list of `EPIC`s can be found here](https://github.com/apache/datafusion/issues?q=is%3Aissue+is%3Aopen+epic).

## Quarterly Roadmap

The DataFusion roadmap is driven by the priorities of contributors rather than
any single organization or coordinating committee. We typically discuss our
roadmap using GitHub issues, approximately quarterly, and invite you to join the
discussion.

For more information:

1. [Search for issues labeled `roadmap`](https://github.com/apache/datafusion/issues?q=is%3Aissue%20%20%20roadmap)
2. [DataFusion Road Map: Q3-Q4 2025](https://github.com/apache/datafusion/issues/15878)
3. [2024 Q4 / 2025 Q1 Roadmap](https://github.com/apache/datafusion/issues/13274)

## Improvement Proposals

### Discussing New Features

If you plan to work on a new feature that doesn't have an existing ticket, it is
a good idea to open a ticket to discuss the feature. Advanced discussion often
helps avoid wasted effort by determining early if the feature is a good fit for
DataFusion before too much time is invested. Discussion on a ticket can help
gather feedback from the community and is likely easier to discuss than a 1000
line PR.

If you open a ticket and it doesn't get any response, you can try `@`-mentioning
recently active community members in the ticket to get their attention.

### What Contributions are Good Fits?

DataFusion is designed to be highly extensible, and many features can be
implemented as extensions without changes or additions to the core. Support for
new functions, data formats, and similar functionality can be added using those
extension APIs, and there are already many existing community supported
extensions listed in the [extensions list].

Query engines are complex pieces of software to develop and maintain. Given our
limited maintenance bandwidth, we try to keep the DataFusion core as simple and
focused as possible, while still satisfying the [design goal] of an easy to
start initial experience.

With that in mind, contributions that meet the following criteria are more likely
to be accepted:

1. Bug fixes for existing features
2. Test coverage for existing features
3. Documentation improvements / examples
4. Performance improvements to existing features (with benchmarks)
5. "Small" functional improvements to existing features (if they don't change existing behavior)
6. Additional APIs for extending DataFusion's capabilities
7. CI improvements

Contributions that will likely involve more discussion (see Discussing New
Features above) prior to acceptance include:

1. Major new functionality (even if it is part of the "standard SQL")
2. New functions, especially if they aren't part of "standard SQL"
3. New data sources (e.g. support for Apache ORC)

[extensions list]: ../library-user-guide/extensions.md
[design goal]: https://docs.rs/datafusion/latest/datafusion/index.html#design-goals

### Design Build vs. Big Up Front Design

Typically, the DataFusion community attacks large problems by solving them bit
by bit and refining a solution iteratively on the `main` branch as a series of
Pull Requests. This is different from projects which front-load the effort
with a more comprehensive design process.

By "advancing the front" the community always makes tangible progress, and the strategy is
especially effective in a project that relies on individual contributors who may
not have the time or resources to invest in a large upfront design effort.
However, this "bit by bit approach" doesn't always succeed, and sometimes we get
stuck or go down the wrong path and then change directions.

Our process necessarily results in imperfect solutions being the "state of the
code" in some cases, and larger visions are not yet fully realized. However, the
community is good at driving things to completion in the long run. If you see
something that needs improvement or an area that is not yet fully realized,
please consider submitting an issue or PR to improve it. We are always looking
for more contributions.
