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
# Tokomak Optimizer

Experimental e-graph based optimizer. Uses [egg](https://github.com/egraphs-good/egg).
## Todo list
1. Add round trip test for expressions and plans.
2. Improve documentation
3. Ensure that invariants, such as expression naming, are preserved by the optimizer. Add tests to ensure they are upheld.
4. Write invariant checking functionality
5. Add support for timezones in TokomakScalar for timestamp types.
6. Add support for UserDefinedLogicalPlans.
7. Add support for plans containing Values. 
8. Replace Symbol uses with another type. Symbol uses a global mutex protected cache for storing the value of symbol, could cause heavy contention on lock if optimizer is run from multiple threads at once.
9. Add custom DSL with some conveniences such as specifying the type of node that will be matched.


## Potentially interesting things
1. Sketch based equality saturation. Useful for when long rewrite chains are required. May not be applicable to this use case: https://arxiv.org/pdf/2111.13040.pdf.
2. The paper 'Caviar: An E-graph Based TRS for Automatic Code Optimization' has a section on optimizing execution speed, may not be applicable. https://arxiv.org/pdf/2111.12116.pdf
3. An E-Graph Based Term Rewriting System for Automatic Code Optimization. Contains quite a bit of general info on egraphs and term rewriting systems. https://www.researchgate.net/profile/Smail-Kourta/publication/353403145_An_E-Graph_Based_Term_Rewriting_System_for_Automatic_Code_Optimization/links/60fa8acc1e95fe241a817529/An-E-Graph-Based-Term-Rewriting-System-for-Automatic-Code-Optimization.pdf
4. Programming Language Tools and Techniques for Computational Fabrication contains some material on multi-objective cost functions and egraphs. https://digital.lib.washington.edu/researchworks/handle/1773/47996