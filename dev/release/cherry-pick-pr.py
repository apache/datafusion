#!/usr/bin/python3
##############################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
##############################################################################

# Adapted from
# https://github.com/apache/arrow-rs/tree/master/dev/release/cherry-pick-pr.py

# This script is designed to create a cherry pick PR to a target branch
#
# Usage: python3 cherry_pick_pr.py
#
# To test locally:
#
# git clone git@github.com:apache/arrow-datafusion.git /tmp/arrow-datafusion
#
# pip3 install PyGithub
# ARROW_GITHUB_API_TOKEN=<..>
#     CHECKOUT_ROOT=<path>
#     CHERRY_PICK_SHA=<sha> python3 cherry-pick-pr.py
#
import os
import sys
import six
import subprocess

from pathlib import Path

TARGET_BRANCH = 'active_release'
TARGET_REPO = 'apache/arrow-datafusion'

p = Path(__file__)

# Use github workspace if specified
repo_root = os.environ.get("CHECKOUT_ROOT")
if repo_root is None:
    print("arrow-datafusion checkout must be supplied in CHECKOUT_ROOT environment")
    sys.exit(1)

print("Using checkout in {}".format(repo_root))

token = os.environ.get('ARROW_GITHUB_API_TOKEN', None)
if token is None:
    print("GITHUB token must be supplied in ARROW_GITHUB_API_TOKEN environmet")
    sys.exit(1)

new_sha = os.environ.get('CHERRY_PICK_SHA', None)
if new_sha is None:
    print("SHA to cherry pick must be supplied in CHERRY_PICK_SHA environment")
    sys.exit(1)


# from merge_pr.py from arrow repo
def run_cmd(cmd):
    if isinstance(cmd, six.string_types):
        cmd = cmd.split(' ')
    try:
        output = subprocess.check_output(cmd)
    except subprocess.CalledProcessError as e:
        # this avoids hiding the stdout / stderr of failed processes
        print('Command failed: %s' % cmd)
        print('With output:')
        print('--------------')
        print(e.output)
        print('--------------')
        raise e

    if isinstance(output, six.binary_type):
        output = output.decode('utf-8')

    return output


os.chdir(repo_root)
new_sha_short = run_cmd("git rev-parse --short {}".format(new_sha)).strip()
new_branch = 'cherry_pick_{}'.format(new_sha_short)


def make_cherry_pick():
    if os.environ.get('GITHUB_SHA', None) is not None:
        print("Running on github runner, setting email/username")
        run_cmd(['git', 'config', 'user.email', 'dev@arrow.apache.com'])
        run_cmd(['git', 'config', 'user.name', 'Arrow-Datafusion Automation'])

    #
    # Create a new branch from active_release
    # and cherry pick to there.
    #

    print("Creating cherry pick from {} to {}".format(
        new_sha_short, new_branch
    ))

    # The following tortured dance is required due to how the github
    # actions/checkout works (it doesn't pull other branches and pulls
    # only one commit back)

    # pull 10 commits back so we can get the proper cherry pick
    # (probably only need 2 but 10 must be better, right?)
    run_cmd(['git', 'fetch', '--depth', '10', 'origin', 'master'])
    run_cmd(['git', 'fetch', 'origin', 'active_release'])
    run_cmd(['git', 'checkout', '-b', new_branch])
    run_cmd(['git', 'reset', '--hard', 'origin/active_release'])
    run_cmd(['git', 'cherry-pick', new_sha])
    run_cmd(['git', 'push', '-u', 'origin', new_branch])


def make_cherry_pick_pr():
    from github import Github
    g = Github(token)
    repo = g.get_repo(TARGET_REPO)

    release_cherry_pick_label = repo.get_label('release-cherry-pick')
    cherry_picked_label = repo.get_label('cherry-picked')

    # Default titles
    new_title = 'Cherry pick {} to active_release'.format(new_sha)
    new_commit_message = 'Automatic cherry-pick of {}\n'.format(new_sha)

    # try and get info from github api
    commit = repo.get_commit(new_sha)
    for orig_pull in commit.get_pulls():
        new_commit_message += '* Originally appeared in {}: {}\n'.format(
            orig_pull.html_url, orig_pull.title)
        new_title = 'Cherry pick {} to active_release'.format(orig_pull.title)
        orig_pull.add_to_labels(cherry_picked_label)

    pr = repo.create_pull(title=new_title,
                          body=new_commit_message,
                          base='refs/heads/active_release',
                          head='refs/heads/{}'.format(new_branch),
                          maintainer_can_modify=True,
                          )

    pr.add_to_labels(release_cherry_pick_label)

    print('Created PR {}'.format(pr.html_url))


make_cherry_pick()
make_cherry_pick_pr()
