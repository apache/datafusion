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

"""The prototype this module is a port of, kept so the test fixtures can be
regenerated. Run `fixtures.py` with PYTHONHASHSEED=0 for stable digits."""

from itertools import combinations

def buildHelix(m):
    vertices = ["P0"]
    for i in range(m):
        vertices += ["A%d" % i, "B%d" % i, "P%d" % (i + 1)]
    adjacency = {v: set() for v in vertices}
    edges = []
    def connect(u, v):
        adjacency[u].add(v)
        adjacency[v].add(u)
        edges.append(frozenset((u, v)))
    for i in range(m):
        connect("P%d" % i, "A%d" % i)
        connect("P%d" % i, "B%d" % i)
        connect("A%d" % i, "P%d" % (i + 1))
        connect("B%d" % i, "P%d" % (i + 1))
    return vertices, adjacency, edges

def isConnected(vertexSet, adjacency):
    startVertex = next(iter(vertexSet))
    reached = {startVertex}
    frontier = {startVertex}
    while frontier:
        newlyReached = set()
        for u in frontier:
            newlyReached |= adjacency[u] & vertexSet
        newlyReached -= reached
        reached |= newlyReached
        frontier = newlyReached
    return reached == vertexSet

def crosses(leftHalf, rightHalf, adjacency):
    for u in leftHalf:
        if adjacency[u] & rightHalf:
            return True
    return False

def subsetSize(vertexSet, cards, sels):
    total = 1.0
    for v in vertexSet:
        total *= cards[v]
    for u, w in combinations(sorted(vertexSet), 2):
        edge = frozenset((u, w))
        if edge in sels:
            total *= sels[edge]
    return total

def allSubsets(vertices):
    for r in range(1, len(vertices) + 1):
        for combo in combinations(vertices, r):
            yield frozenset(combo)

def properSubsets(vertexSet):
    items = sorted(vertexSet)
    for r in range(1, len(items)):
        for combo in combinations(items, r):
            yield frozenset(combo)

def helixMinCost(vertices, adjacency, cards, sels):
    connectedSets = [S for S in allSubsets(vertices) if isConnected(S, adjacency)]
    connectedSets.sort(key=len)
    sizeTable = {S: subsetSize(S, cards, sels) for S in connectedSets}
    dpTable = {}
    for vertexSet in connectedSets:
        if len(vertexSet) == 1:
            dpTable[vertexSet] = 0.0
            continue
        bestCost = float('inf')
        anchor = min(vertexSet)
        for leftHalf in properSubsets(vertexSet):
            if anchor not in leftHalf:
                continue
            rightHalf = vertexSet - leftHalf
            if leftHalf in dpTable and rightHalf in dpTable \
                    and crosses(leftHalf, rightHalf, adjacency):
                candidateCost = dpTable[leftHalf] + dpTable[rightHalf]
                if candidateCost < bestCost:
                    bestCost = candidateCost
        dpTable[vertexSet] = bestCost + sizeTable[vertexSet]
    return dpTable[frozenset(vertices)]
