#!/usr/bin/env python3

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


"""
Utility for updating the committer list in the governance documentation
by reading from the Apache DataFusion phonebook and combining with existing data.
"""

import re
import requests
import sys
import os
from typing import Dict, List, NamedTuple, Set


class Committer(NamedTuple):
    name: str
    apache: str
    github: str
    affiliation: str
    role: str


# Return (pmc, committers) each a dictionary like
# key: apache id
# value: Real name

def get_asf_roster():
    """Get the current roster from Apache phonebook."""
    # See https://home.apache.org/phonebook-about.html
    committers_url = "https://whimsy.apache.org/public/public_ldap_projects.json"

    # people https://whimsy.apache.org/public/public_ldap_people.json
    people_url = "https://whimsy.apache.org/public/public_ldap_people.json"

    try:
        r = requests.get(committers_url)
        r.raise_for_status()
        j = r.json()
        proj = j['projects']['datafusion']

        # Get PMC members and committers
        pmc_ids = set(proj['owners'])
        committer_ids = set(proj['members']) - pmc_ids

    except Exception as e:
        print(f"Error fetching ASF roster: {e}")
        return set(), set()

    # Fetch people to get github handles and affiliations
    #
    # The data looks like this:
    # {
    #   "lastCreateTimestamp": "20250913131506Z",
    #   "people_count": 9932,
    #   "people": {
    #     "a_budroni": {
    #       "name": "Alessandro Budroni",
    #       "createTimestamp": "20160720223917Z"
    #     },
    #   ...
    #  }
    try:
        r = requests.get(people_url)
        r.raise_for_status()
        j = r.json()
        people = j['people']

        # make a dictionary with each pmc_id and value their real name
        pmcs = {p: people[p]['name'] for p in pmc_ids}
        committers = {c: people[c]['name'] for c in committer_ids}

    except Exception as e:
        print(f"Error fetching ASF people: {e}")


    return pmcs, committers



def parse_existing_table(content: str) -> List[Committer]:
    """Parse the existing committer table from the markdown content."""
    committers = []

    # Find the table between the markers
    start_marker = "<!-- Begin Auto-Generated Committer List -->"
    end_marker = "<!-- End Auto-Generated Committer List -->"

    start_idx = content.find(start_marker)
    end_idx = content.find(end_marker)

    if start_idx == -1 or end_idx == -1:
        return committers

    table_content = content[start_idx:end_idx]

    # Parse table rows (skip header and separator)
    lines = table_content.split('\n')
    for line in lines:
        line = line.strip()
        if line.startswith('|')  and '---' not in line and line.count('|') >= 4:
            # Split by | and clean up
            parts = [part.strip() for part in line.split('|')]
            if len(parts) >= 5:
                name = parts[1].strip()
                apache = parts[2].strip()
                github = parts[3].strip()
                affiliation = parts[4].strip()
                role = parts[5].strip()

                if name and name != 'Name' and (not '-----' in name):
                    committers.append(Committer(name, apache, github, affiliation, role))

    return committers


def generate_table_row(committer: Committer) -> str:
    """Generate a markdown table row for a committer."""
    github_link = f"[{committer.github}](https://github.com/{committer.github})"
    return f"| {committer.name:<23} | {committer.apache:<39} |{committer.github:<39} | {committer.affiliation:<11} | {committer.role:<9} |"


def sort_committers(committers: List[Committer]) -> List[Committer]:
    """Sort committers by role ('PMC Chair', PMC, Committer) then by apache id."""
    role_order = {'PMC Chair': 0, 'PMC': 1, 'Committer': 2}

    return sorted(committers, key=lambda c: (role_order.get(c.role, 3), c.apache.lower()))


def update_governance_file(file_path: str):
    """Update the governance file with the latest committer information."""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"Error: File {file_path} not found")
        return False

    # Parse existing committers
    existing_committers = parse_existing_table(content)
    print(f"Found {len(existing_committers)} existing committers")

    # Get ASF roster
    asf_pmcs, asf_committers = get_asf_roster()
    print(f"Found {len(asf_pmcs)} PMCs and {len(asf_committers)} committers in ASF roster")


    # Create a map of existing committers by apache id
    existing_by_apache = {c.apache: c for c in existing_committers}

    # Update the entries based on the ASF roster
    updated_committers = []
    for apache_id, name in {**asf_pmcs, **asf_committers}.items():
        role = 'PMC' if apache_id in asf_pmcs else 'Committer'
        if apache_id in existing_by_apache:
            existing = existing_by_apache[apache_id]
            # Preserve PMC Chair role if already set
            if existing.role == 'PMC Chair':
                role = 'PMC Chair'
            updated_committers.append(Committer(
                name=existing.name,
                apache=apache_id,
                github=existing.github,
                affiliation=existing.affiliation,
                role=role
            ))
        # add a new entry for new committers with placeholder values
        else:
            print(f"New entry found: {name} ({apache_id})")
            # Placeholder github and affiliation
            updated_committers.append(Committer(
                name=name,
                apache=apache_id,
                github="", # user should update
                affiliation="",  # User should update
                role=role
            ))


    # Sort the committers
    sorted_committers = sort_committers(updated_committers)

    # Generate new table
    table_lines = [
        "| Name                    | Apache ID | github                     | Affiliation | Role      |",
        "|-------------------------|-----------|----------------------------|-------------|-----------|"
    ]

    for committer in sorted_committers:
        table_lines.append(generate_table_row(committer))

    new_table = '\n'.join(table_lines)

    # Replace the table in the content
    start_marker = "<!-- Begin Auto-Generated Committer List -->"
    end_marker = "<!-- End Auto-Generated Committer List -->"

    start_idx = content.find(start_marker)
    end_idx = content.find(end_marker)

    if start_idx == -1 or end_idx == -1:
        print("Error: Could not find table markers in file")
        return False

    # Find the end of the start marker line
    start_line_end = content.find('\n', start_idx) + 1

    new_content = (
        content[:start_line_end] +
        new_table + '\n' +
        content[end_idx:]
    )

    # Write back to file
    try:
        with open(file_path, 'w') as f:
            f.write(new_content)
        print(f"Successfully updated {file_path}")
        return True
    except Exception as e:
        print(f"Error writing file: {e}")
        return False


def main():
    """Main function."""
    # Default path to governance file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(script_dir)
    governance_file = os.path.join(repo_root, "source", "contributor-guide", "governance.md")

    if len(sys.argv) > 1:
        governance_file = sys.argv[1]

    if not os.path.exists(governance_file):
        print(f"Error: Governance file not found at {governance_file}")
        sys.exit(1)

    print(f"Updating committer list in {governance_file}")

    if update_governance_file(governance_file):
        print("Committer list updated successfully")
    else:
        print("Failed to update committer list")
        sys.exit(1)


if __name__ == "__main__":
    main()
