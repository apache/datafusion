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

Experimental equality graph based optimizer. Uses [egg](https://github.com/egraphs-good/egg) for the equality graph implementation.

## Potentially interesting things
1. Sketch based equality saturation. Useful for when long rewrite chains are required. May not be applicable to this use case: https://arxiv.org/pdf/2111.13040.pdf.