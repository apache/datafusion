# Datafusion RFCs

## Motivation

The RFCs (request for comments) provides a consistent and controlled path for
Datafusion developers to propose formalized semantics and non-trivial changes
to the project.

## Creating new RFC

* Create a new markdown file within the `rfcs` folder with RFC title as the file name.
  * At the very minimal, a RFC should contain the following sections:
    * Summary
    * Motivation
    * Detailed design
    * Unresolved questions
* Send a PR for proposed RFC.
* Once a RFC PR is reviewed and merged, the RFC is considered accepted and active.

## Updating existing RFC

Minor changes can be applied to the existing RFCs directly via follow-up PRs.
Exactly what counts as minor changes is up to the committers to decide.

## Archiving RFC

If an active RFC becomes inactive for some reason, it should be marked as so at
the beginning of the document right under the title.
