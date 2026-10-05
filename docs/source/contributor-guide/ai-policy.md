<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# AI Policy

DataFusion has the following policy for AI-assisted PRs:

- We welcome AI-assisted PRs from anyone. We do not welcome unreviewed "AI dumps" (defined below).
- The PR author should have personally read the entire PR they submit and **understand the core ideas end-to-end**. Authors should be ready to justify and help reviewers understand the design and code during review.
- **Call out unknowns and assumptions**. It's okay to not fully understand some bits of AI-generated code. Please point these cases out so we can work together to clear up any concerns.

While "understand the core ideas" is partly subjective, it means more than being
able to follow the diff textually. We expect the PR author to take an active
role in responding to feedback and crafting the PR to make sure it fits well
into the project as a whole. The worst situation is where the reviewer is simply
driving the contributor's LLM for the reasons described in the next section.

## What is an "AI dump" and why it is not helpful

An "AI dump" is a PR, or a series of PRs, consisting largely of AI-generated
code and descriptions that the author has not reviewed and does not understand.
The code may even be correct. The problem is that all the work of understanding
falls on the reviewer.

Code review serves two purposes:

1. Finish the intended task.
2. Share knowledge between authors and reviewers, as a long-term investment in
   the project. For this reason, even if someone familiar with the codebase
   could finish a task more quickly by themselves, we are still happy to help
   a new contributor work on it.

An AI dump meets neither purpose. Maintainers could finish the task faster by
running the AI tool themselves, and an author who acts only as a pass-through
proxy for the tool learns little from the review.

Reviewing capacity for the project is **very limited**, so PRs that appear to be
AI dumps may not get reviewed, and may eventually be closed.

Multiple PRs created in a short amount of time, especially by a first-time
contributor, that in our judgment show a lack of understanding or author
engagement may be treated as spam and closed. One high quality PR that you work
with maintainers to merge is far more valuable to you and the project than ten
PRs you have your agent generate and submit for you.

## Responding to review comments

The same policy applies to review discussion as to the code itself: reviewers
want to talk to **you**, not to your AI tool. Please do not paste an AI-generated
response to a review comment verbatim or have your agent respond to
reviewer comments. Some signs that a reply is an unreviewed AI dump:

- It summarizes the diff rather than answering the question that was asked.
- It lists the commands that were run locally (e.g. `cargo fmt`, `cargo test`)
  and whether they passed. This is not useful to reviewers because CI already
  runs these checks.
- It contains statements that don't make sense in context, such as claiming
  tests could not be run because `cargo` is not installed.

For example, see [this review thread in arrow-rs][arrow-rs-review-example] where
the reviewer asked a design question, and received several replies that
described what had changed and which commands had been run, rather than an
answer to the question.

The point of code review is to help the project **and** to help you grow as an
engineer, so please read each comment, make sure you understand it, and reply
in your own words.

[arrow-rs-review-example]: https://github.com/apache/arrow-rs/pull/11209#discussion_r4145512124

## AI-assisted reviews

The same standard applies to AI-generated reviews as to AI-generated code: you
should have read and understand anything you post. Raw AI review output is often
verbose with unnecessary details, and it can take substantial effort to figure
out what is actually being asked.

If you review PRs with the help of an AI tool, read all comments first, remove
detail that is unnecessary or you don't understand, and explain the rest in your
own words so that each comment makes a clear, specific request.

## Better ways to contribute than an “AI dump”

It's recommended to write a high-quality issue with a clear problem statement
and a minimal, reproducible example. The reproducer should focus on the end-user-visible
behavior rather than explaining the details of some code defect.

This will make it easier for others to contribute and for reviewers to
understand the problem being addressed.
