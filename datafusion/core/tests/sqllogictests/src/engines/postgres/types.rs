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

use postgres_types::Type;
use tokio_postgres::types::FromSql;

pub struct PgRegtype {
    value: String,
}

impl<'a> FromSql<'a> for PgRegtype {
    fn from_sql(
        _: &Type,
        buf: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let oid = postgres_protocol::types::oid_from_sql(buf)?;
        let value = Type::from_oid(oid).ok_or("bad type")?.to_string();
        Ok(PgRegtype { value })
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::REGTYPE)
    }
}

impl ToString for PgRegtype {
    fn to_string(&self) -> String {
        self.value.clone()
    }
}
