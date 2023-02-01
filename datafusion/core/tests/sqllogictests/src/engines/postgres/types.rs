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
