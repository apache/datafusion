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

use proc_macro2::Ident;
use syn::spanned::Spanned;
use syn::{Error, FnArg, ItemFn, Pat, Result, ReturnType, Type};

pub struct InputFnInfo {
    pub name: Ident,
    pub args: Vec<NameType>,
    pub out_arg: Option<NameType>,
    pub return_ty: Type,
}

pub struct NameType {
    pub name: Ident,
    pub ty: Type,
}

impl InputFnInfo {
    /// Validate the input and capture the necessary information
    pub fn try_from(input_fn: ItemFn) -> Result<Self> {
        let sig = input_fn.sig;

        if sig.asyncness.is_some() {
            return Err(Error::new(
                sig.span(),
                "Function cannot be async for use with Excalibur",
            ));
        }
        if sig.variadic.is_some() {
            return Err(Error::new(
                sig.span(),
                "Function cannot be variadic for use with Excalibur",
            ));
        }

        let mut inputs: Vec<_> = sig.inputs.iter().collect();
        let mut out_arg = None;
        if let Some(FnArg::Typed(typed)) = inputs.last() {
            if let Pat::Ident(ident) = &*typed.pat {
                if let Type::Reference(type_reference) = &*typed.ty {
                    if type_reference.mutability.is_some() {
                        out_arg = Some(NameType {
                            name: ident.ident.clone(),
                            ty: (*typed.ty).clone(),
                        });
                        inputs.pop();
                    }
                }
            }
        }

        let args = inputs
            .into_iter()
            .map(|arg| {
                if let FnArg::Typed(typed) = arg {
                    if let Pat::Ident(ident) = &*typed.pat {
                        if typed.attrs.is_empty() {
                            return Ok(NameType {
                                name: ident.ident.clone(),
                                ty: (*typed.ty).clone(),
                            });
                        }
                    }
                }
                Err(Error::new(
                    arg.span(),
                    "Unsupported function argument (name, type or attributes) for use with Excalibur",
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let ReturnType::Type(_, return_ty) = sig.output else {
            return Err(Error::new(
                sig.output.span(),
                "Function needs a return type for use with Excalibur",
            ));
        };

        Ok(InputFnInfo {
            name: sig.ident,
            args,
            out_arg,
            return_ty: (*return_ty).to_owned(),
        })
    }
}
