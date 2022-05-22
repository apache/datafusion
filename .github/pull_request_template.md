# Which issue does this PR close?

<!--
We generally require a GitHub issue to be filed for all bug fixes and enhancements and this helps us generate change logs for our releases. You can link an issue to this PR using the GitHub syntax. For example `Closes #123` indicates that this PR will close issue #123.
-->

Closes #.

 # Rationale for this change
<!--
 Why are you proposing this change? If this is already explained clearly in the issue then this section is not needed.
 Explaining clearly why changes are proposed helps reviewers understand your changes and offer better suggestions for fixes.  
-->

# What changes are included in this PR?
<!--
There is no need to duplicate the description in the issue here but it is sometimes worth providing a summary of the individual changes in this PR.
-->

# Are there any user-facing changes?
<!--
If there are user-facing changes then we may require documentation to be updated before approving the PR.
-->

<!--
If there are any breaking changes to public APIs, please add the `api change` label.
-->

# Does this PR break compatibility with Ballista?

<!--
The CI checks will attempt to build [arrow-ballista](https://github.com/apache/arrow-ballista) against this PR. If 
this check fails then it indicates that this PR makes a breaking change to the DataFusion API.

If possible, try to make the change in a way that is not a breaking API change. For example, if code has moved 
 around, try adding `pub use` from the original location to preserve the current API.

If it is not possible to avoid a breaking change (such as when adding enum variants) then follow this process:

- Make a corresponding PR against `arrow-ballista` with the changes required there
- Update `dev/build-arrow-ballista.sh` to clone the appropriate `arrow-ballista` repo & branch
- Merge this PR when CI passes
- Merge the Ballista PR
- Create a new PR here to reset `dev/build-arrow-ballista.sh` to point to `arrow-ballista` master again

_If you would like to help improve this process, please see https://github.com/apache/arrow-datafusion/issues/2583_
-->