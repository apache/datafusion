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

# Inviting New Committers and PMC Members

This is a cookbook of the recommended DataFusion specific process for inviting
new committers and PMC members. It is intended to follow the [Apache Project
Management Committee guidelines], which takes precedence over this document if
there is a conflict.

This process is intended for PMC members. While the process is open, the actual
discussions and invitations are not (one of the very few things that we do not
do in the open).

When following this process, in doubt, check the `private@datafusion.apache.org`
[mailing list archive] for examples of previous emails.

The general process is:

1. A PMC member starts a discussion on `private@datafusion.apache.org`
   about a candidate they feel should be considered who would use the additional
   trust granted to a committer or PMC member to help the community grow and thrive.
2. When consensus is reached a formal [vote] occurs
3. Assuming the vote is successful, that person is granted the appropriate
   access via ASF systems.

[apache project management committee guidelines]: https://www.apache.org/dev/pmc.html
[mailing list archive]: https://lists.apache.org/list.html?dev@datafusion.apache.org
[vote]: https://www.apache.org/foundation/voting

## New Committers

### Step 1: Start a Discussion Thread

The goal of this step is to allow free form discussion prior to calling a vote.
This helps the other PMC members understand why you are proposing to invite the
person to become a committer and makes the voting process easier.

Best practice is to include some details about why you are proposing to invite
the person. Here is an example:

```
To: private@datafusion.apache.org
Subject: [DISCUSS] $PERSONS_NAME for Committer

$PERSONS_NAME has been an active contributor to the DataFusion community for the
last 6 months[1][2], helping others, answering questions, and improving the
project's code.

Are there any thoughts about inviting $PERSONS_NAME to become a committer?

Thanks,
Your Name

[1]: https://github.com/apache/datafusion/issues?q=commenter%3A<github id>
[2]: https://github.com/apache/datafusion/commits?author=<github id>
```

### Step 2: Formal Vote

Assuming the discussion thread goes well, start a formal vote, with an email like:

```
To: private@datafusion.apache.org
Subject: [VOTE] $PERSONS_NAME for DataFusion Committer
I propose to invite $PERSONS_NAME to be a committer. See discussion here [1].

[ ] +1 : Invite $PERSONS_NAME to become a committer
[ ] +0: ...
[ ] -1: I disagree because ...

The vote will be open for at least 48 hours.

My vote: +1

Thanks,
Your Name

[1] LINK TO DISCUSSION THREAD (e.g. https://lists.apache.org/thread/7rocc026wckknrjt9j6bsqk3z4c0g5yf)
```

If the vote passes (requires 3 `+1`, and no `-1` voites) , send a result email
like the following (substitute `N` with the number of `+1` votes)

```
to: private@datafusion.apache.org
subject: [RESULT][VOTE] $PERSONS_NAME for DataFusion Committer

The vote carries with N +1 votes.
```

### Step 3: Send Invitation to the Candidate

Once the vote on `private@` has passed and the `[RESULT]` e-mail sent, send an
invitation to the new committer and cc: `private@datafusion.apache.org`

In order to be given write access, the committer needs an apache.org account (e.g. `name@apache.org`). To get one they need:

1. An [ICLA] on file. Note that Sending an ICLA to `secretary@apache.org` will trigger account creation. If they already have an ICLA on file, but no Apache account, see instructions below.
2. Add GitHub username to the account at [id.apache.org](http://id.apache.org/)
3. Connect [gitbox.apache.org] to their GitHub account
4. Follow the instructions at [gitbox.apache.org] to link your GitHub account with your

[gitbox.apache.org]: https://gitbox.apache.org

If the new committer is already a committer on another Apache project (so they
already had an Apache account), The PMC Chair (or an ASF member) simply needs to
explicitly add them to the roster on the [Whimsy Roster Tool].

### Step 4: Announce and Celebrate the New Committer

Email to Send an email such as the following to
[dev@datafusion.apache.org](mailto:dev@datafusion.apache.org]) to celebrate and
acknowledge the new committer to the community.

```
To: dev@datafusion.apache.org
Subject: [ANNOUNCE] New DataFusion committer: $NEW_COMMITTER

On behalf of the DataFusion PMC, I'm happy to announce that $NEW_COMMITTER
has accepted an invitation to become a committer on Apache
DataFusion. Welcome, and thank you for your contributions!

<your name>
```

[icla]: http://www.apache.org/licenses/#clas

## Email Templates for Inviting New Committers

### Committers WITHOUT an Apache account and WITHOUT ICLA on file

You can check here to see if someone has an Apache account: http://people.apache.org/committer-index.html

If you aren't sure whether someone has an ICLA on file, ask the DataFusion PMC
chair or an ASF Member to check [the ICLA file list].

[the icla file list]: https://whimsy.apache.org/officers/unlistedclas.cgi

```
To: $EMAIL
Cc: private@datafusion.apache.org
Subject: Invitation to become a DataFusion Committer

Dear $NEW_COMMITTER,

The DataFusion Project Management Committee (PMC) hereby offers you
committer privileges to the project. These privileges are offered on
the understanding that you'll use them reasonably and with common
sense. We like to work on trust rather than unnecessary constraints.

Being a committer enables you to merge PRs to DataFusion git repositories.

Being a committer does not require you to participate any more than
you already do. It does tend to make one even more committed. You will
probably find that you spend more time here.

Of course, you can decline and instead remain as a contributor,
participating as you do now.

A. This personal invitation is a chance for you to accept or decline
in private. Either way, please let us know in reply to the
private@datafusion.apache.org address only.

B. If you accept, the next step is to register an ICLA:

Details of the iCLA and how to submit them are found through this link:
https://www.apache.org/licenses/contributor-agreements.html#clas

When you transmit the completed ICLA, request to notify Apache
DataFusion and choose a unique Apache id. Look to see if your preferred id
is already taken at http://people.apache.org/committer-index.html This
will allow the Secretary to notify the PMC when your ICLA has been
recorded.

Once you are notified that your Apache account has been created, you must then:

1. Add your GitHub username to your account at id.apache.org

2. Follow the instructions at gitbox.apache.org to link your GitHub account
with your Apache account.

You will then have write (but not admin) access to the DataFusion repositories.
If you have questions or run into issues, please reply-all to this e-mail.
```

After the new account has been created, you can announce the new committer on `dev@`

### Committers WITHOUT an Apache account but WITH an ICLA on file

In this scenario, an officer (the project VP or an Apache Member) needs only to
request an account to be created for the new committer, since through the
ordinary process the ASF Secretary will do it automatically. This is done at
https://whimsy.apache.org/officers/acreq. Before requesting the new account,
send an e-mail to the new committer (cc'ing `private@`) like this:

```
To: $EMAIL
Cc: private@datafusion.apache.org
Subject: Invitation to become a DataFusion Committer

Dear $NEW_COMMITTER,

The DataFusion Project Management Committee (PMC) hereby offers you
committer privileges to the project. These privileges are offered on
the understanding that you'll use them reasonably and with common
sense. We like to work on trust rather than unnecessary constraints.

Being a committer enables you to merge PRs to DataFusion git repositories.

Being a committer does not require you to participate any more than
you already do. It does tend to make one even more committed. You will
probably find that you spend more time here.

Of course, you can decline and instead remain as a contributor,
participating as you do now.

This personal invitation is a chance for you to accept or decline
in private. Either way, please let us know in reply to the
private@datafusion.apache.org address only. We will have to request an
Apache account be created for you, so please let us know what user id
you would prefer.

Once you are notified that your Apache account has been created, you must then:

1. Add your GitHub username to your account at id.apache.org

2. Follow the instructions at gitbox.apache.org to link your GitHub account
with your Apache account.

You will then have write (but not admin) access to the DataFusion repositories.
If you have questions or run into issues, please reply-all to this e-mail.
```

### Committers WITH an existing Apache account

In this scenario, an officer (the project PMC Chair or an ASF Member) can simply add
the new committer on the [Whimsy Roster Tool]. Before doing this, e-mail the new
committer inviting them to be a committer like so (cc private@):

```
To: $EMAIL
Cc: private@datafusion.apache.org
Subject: Invitation to become a DataFusion Committer

Dear $NEW_COMMITTER,

The DataFusion Project Management Committee (PMC) hereby offers you
committer privileges to the project. These privileges are offered on
the understanding that you'll use them reasonably and with common
sense. We like to work on trust rather than unnecessary constraints.

Being a committer enables you to merge PRs to DataFusion git repositories.

Being a committer does not require you to participate any more than
you already do. It does tend to make one even more committed. You will
probably find that you spend more time here.

Of course, you can decline and instead remain as a contributor,
participating as you do now.

If you accept, please let us know by replying to private@datafusion.apache.org.
```

## New PMC Members

See also the ASF instructions on [how to add a PMC member].

[how to add a pmc member]: https://www.apache.org/dev/pmc.html#newpmc

### Step 1: Start a Discussion Thread

As for committers, start a discussion thread on the `private@` mailing list

```
To: private@datafusion.apache.org
Subject: [DISCUSS] $NEW_PMC_MEMBER for PMC

I would like to propose adding $NEW_PMC_MEMBER[1] to the DataFusion PMC.

$NEW_PMC_MEMBMER has been a committer since $COMMITTER_MONTH [2], has a
strong and sustained contribution record for more than a year, and focused on
helping the community and the project grow[3].

Are there any thoughts about inviting $NEW_PMC_MEMBER to become a PMC member?

[1] https://github.com/$NEW_PMC_MEMBERS_GITHUB_ACCOUNT
[2] LINK TO COMMMITER VOTE RESULT THREAD (e.g. https://lists.apache.org/thread/ovgp8z97l1vh0wzjkgn0ktktggomxq9t)
[3]: https://github.com/apache/datafusion/pulls?q=commenter%3A<$NEW_PMC_MEMBERS_GITHUB_ACCOUNT>+

Thanks,
YOUR NAME
```

### Step 2: Formal Vote

Assuming the discussion thread goes well, start a formal vote with an email like:

```
To: private@datafusion.apache.org
Subject: [VOTE] $NEW_PMC_MEMBER for PMC

I propose inviting $NEW_PMC_MEMBER to join the DataFusion PMC. We previously
discussed the merits of inviting $NEW_PMC_MEMBER to join the PMC [1].

The vote will be open for at least 7 days.

[ ] +1 : Invite $NEW_PMC_MEMBER to become a PMC member
[ ] +0: ...
[ ] -1: I disagree because ...

My vote: +1

[1] LINK TO DISCUSSION THREAD (e.g. https://lists.apache.org/thread/x2zno2hs1ormvfy13n7h82hmsxp3j66c)

Thanks,
Your Name
```

### Step 3: Send Notice to ASF Board

The DataFusion PMC Chair then sends a NOTICE to `board@apache.org` (cc'ing
`private@`) like this:

```
To: board@apache.org
Cc: private@datafusion.apache.org
Subject: [NOTICE] $NEW_PMC_MEMBER to join DataFusion PMC

DataFusion proposes to invite $NEW_PMC_MEMBER ($NEW_PMC_MEMBER_APACHE_ID) to join the PMC.

The vote result is available here:
$VOTE_RESULT_URL

FYI: Full vote details:
$VOTE_URL
```

### Step 4: Send invitation email

Once, the PMC chair has confirmed that the email sent to `board@apache.org` has
made it to the archives, the Chair sends an invitation e-mail to the new PMC
member (cc'ing `private@`) like this:

```
To: $EMAIL
Cc: private@datafusion.apache.org
Subject: Invitation to join the DataFusion PMC
Dear $NEW_PMC_MEMBER,

In recognition of your demonstrated commitment to, and alignment with, the
goals of the Apache DataFusion project, the DataFusion PMC has voted to offer you
membership in the DataFusion PMC ("Project Management Committee").

Please let us know if you accept by subscribing to the private alias [by
sending mail to private-subscribe@datafusion.apache.org], and posting
a message to private@datafusion.apache.org.

The PMC for every top-level project is tasked by the Apache Board of
Directors with official oversight and binding votes in that project.

As a PMC member, you are responsible for continuing the general project, code,
and community oversight that you have exhibited so far. The votes of the PMC
are legally binding.

All PMC members are subscribed to the project's private mail list, which is
used to discuss issues unsuitable for an open, public forum, such as people
issues (e.g. new committers, problematic community members, etc.), security
issues, and the like. It can't be emphasized enough that care should be taken
to minimize the use of the private list, discussing everything possible on the
appropriate public list.

The private PMC list is *private* - it is strictly for the use of the
PMC. Messages are not to be forwarded to anyone else without the express
permission of the PMC. Also note that any Member of the Foundation has the
right to review and participate in any PMC list, as a PMC is acting on behalf
of the Membership.

Finally, the PMC is not meant to create a hierarchy within the committership or
the community. Therefore, in our day-to-day interactions with the rest of the
community, we continue to interact as peers, where every reasonable opinion is
considered, and all community members are invited to participate in our public
voting. If there ever is a situation where the PMC's view differs significantly
from that of the rest of the community, this is a symptom of a problem that
needs to be addressed.

With the expectation of your acceptance, welcome!

The Apache DataFusion PMC
```

### Step 5: Chair Promotes the Committer to PMC

The PMC chair adds the user to the PMC using the [Whimsy Roster Tool].

### Step 6: Announce and Celebrate the New PMC Member

Send an email such as the following to `dev@datafusion.apache.org` to celebrate:

```
To: dev@datafusion.apache.org
Subject: [ANNOUNCE] New DataFusion PMC member: $NEW_PMC_MEMBER

The Project Management Committee (PMC) for Apache DataFusion has invited
$NEW_PMC_MEMBER to become a PMC member and we are pleased to announce
that $NEW_PMC_MEMBER has accepted.

Congratulations and welcome!
```

[whimsy roster tool]: https://whimsy.apache.org/roster/committee/datafusion
