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

DataFusion uses [GitHub issues] to track planned work. We collect related
tickets using tracking issues marked with the `EPIC` label, containing
discussion and links to more detailed items:

[github issues]: https://github.com/apache/datafusion/issues

- [The current list of `EPIC`s can be found here.](https://github.com/apache/datafusion/issues?q=is%3Aissue%20state%3Aopen%20label%3AEPIC)

- [The current list of `PROPOSAL EPIC` (that are not yet underway) can be found here.](https://github.com/apache/datafusion/issues?q=is%3Aissue%20state%3Aopen%20label%3A%22PROPOSAL%20EPIC%22)

Epics offer a high level roadmap of what the DataFusion community is thinking
about. The epics are not meant to restrict possibilities, but rather help
organize the community and make it easier to see where development is headed,
align our work, and inspire additional contributions.

We also welcome contributions for items not covered by epics. However, before
submitting a large PR, we strongly suggest and request you start a conversation as described in [Discussing New Features](#discussing-new-features) below.

[dev@arrow.apache.org]: mailto:dev@arrow.apache.org

## Quarterly Roadmap

The DataFusion roadmap is driven by the priorities of contributors rather than
any single organization or coordinating committee. We typically discuss our
roadmap using GitHub issues, approximately quarterly, and invite you to join the
discussion.

For more information:

1. [Search for issues labeled `roadmap`](https://github.com/apache/datafusion/issues?q=is%3Aissue%20%20%20roadmap)
2. [DataFusion Road Map: Q1 2026](https://github.com/apache/datafusion/issues/18494)
3. [DataFusion Road Map: Q3-Q4 2025](https://github.com/apache/datafusion/issues/15878)
4. [2024 Q4 / 2025 Q1 Roadmap](https://github.com/apache/datafusion/issues/13274)

## Improvement Proposals

### Discussing New Features

If you plan to work on a new feature that doesn't have an existing ticket, it is
a good idea to open one for discussion. Advanced discussion helps avoid wasted
effort by determining if the feature is a good fit for DataFusion before too
much time is invested. Discussion on a ticket can help gather feedback from the
community and is likely easier to discuss than a 1000 line PR.

Maintainers will mark major proposals as `PROPOSED EPIC` to make them more
visible, but we are very limited on review bandwidth. If you open a ticket and it
doesn't get any response, try `@`-mentioning recently active community members
in the ticket, or [posting to the mailing list or Discord](communication.md).

### Supervising Maintainers

We have found that most successful epics have one or more "supervising
maintainers", a committer ([see here for current list]) who take the lead on
reviewing and committing PRs, helps with design, and coordinates and
communicates with the community. If you want to ship a large feature, we
recommend finding such maintainer upfront; otherwise, your PRs may
remain unreviewed for a very long time.

Supervising maintainers have no additional formal authority and there is
currently no formal process for appointing, approving or tracking who has that
role for a given epic. Instead, we rely on discussion on the ticket or PR.
Helping complete an epic is a significant time commitment, so maintainers are
more likely to help features they are particularly interested in or align with
their own project's use of DataFusion.

If you are willing to be a supervising maintainer for a feature, please say so
explicitly. If you are unsure, we suggest asking directly who is willing to take
the role, as it can be hard to tell sometimes whether a committer is simply
participating and giving general feedback.

[see here for current list]: governance.md

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
