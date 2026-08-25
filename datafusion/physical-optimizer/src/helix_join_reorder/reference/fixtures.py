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

from helix_ref import *

def helixMinCostTree(vertices, adjacency, cards, sels):
    connectedSets = [S for S in allSubsets(vertices) if isConnected(S, adjacency)]
    connectedSets.sort(key=len)
    sizeTable = {S: subsetSize(S, cards, sels) for S in connectedSets}
    dpTable, backPtr, ties = {}, {}, {}
    for vertexSet in connectedSets:
        if len(vertexSet) == 1:
            dpTable[vertexSet] = 0.0
            continue
        bestCost, best, tie = float('inf'), None, 0
        anchor = min(vertexSet)
        for leftHalf in properSubsets(vertexSet):
            if anchor not in leftHalf:
                continue
            rightHalf = vertexSet - leftHalf
            if leftHalf in dpTable and rightHalf in dpTable \
                    and crosses(leftHalf, rightHalf, adjacency):
                c = dpTable[leftHalf] + dpTable[rightHalf]
                if c < bestCost:
                    bestCost, best, tie = c, leftHalf, 1
                elif c == bestCost:
                    tie += 1
        dpTable[vertexSet] = bestCost + sizeTable[vertexSet]
        backPtr[vertexSet], ties[vertexSet] = best, tie
    full = frozenset(vertices)

    def render(S):
        if len(S) == 1:
            return next(iter(S))
        L = backPtr[S]
        return "(%s %s)" % (render(L), render(S - L))

    return dpTable[full], render(full), ties[full]

def run(name, vertices, adjacency, cards, sels):
    c, t, tie = helixMinCostTree(vertices, adjacency, cards, sels)
    print("%-10s cost=%.17g  ties_at_root=%d\n           tree=%s" % (name, c, tie, t))

v, adj, _ = buildHelix(1)
cards = {"P0": 1000.0, "A0": 50.0, "B0": 200.0, "P1": 5000.0}
sels = {frozenset(("P0","A0")):0.01, frozenset(("P0","B0")):0.005,
        frozenset(("A0","P1")):0.002, frozenset(("B0","P1")):0.001}
run("helix m=1", v, adj, cards, sels)

v2, adj2, _ = buildHelix(2)
c2 = dict(cards); c2.update({"A1":80.0,"B1":300.0,"P2":2000.0})
s2 = dict(sels); s2.update({frozenset(("P1","A1")):0.02, frozenset(("P1","B1")):0.004,
                            frozenset(("A1","P2")):0.0015, frozenset(("B1","P2")):0.003})
run("helix m=2", v2, adj2, c2, s2)

pv = ["a","b","c","d"]
padj = {"a":{"b"}, "b":{"a","c"}, "c":{"b","d"}, "d":{"c"}}
run("path", pv, padj, {"a":100.0,"b":1000.0,"c":20000.0,"d":300.0},
    {frozenset(("a","b")):0.01, frozenset(("b","c")):0.001, frozenset(("c","d")):0.005})
