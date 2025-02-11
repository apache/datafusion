// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use syn::meta::ParseNestedMeta;
use syn::spanned::Spanned;
use syn::{Error, LitStr};

#[derive(Default)]
pub struct EFAttributes {
    pub name: Option<String>,
}

impl EFAttributes {
    pub fn parse(&mut self, meta: ParseNestedMeta) -> syn::Result<()> {
        match meta.path.get_ident().map(syn::Ident::to_string).as_deref() {
            Some("name") => {
                let value: LitStr = meta.value()?.parse()?;
                self.name = Some(value.value());
                Ok(())
            }
            _ => Err(Error::new(meta.path.span(), "Unknown attribute")),
        }
    }
}
