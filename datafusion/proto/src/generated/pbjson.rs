impl serde::Serialize for AggregateExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.group_expr.is_empty() {
            len += 1;
        }
        if !self.aggr_expr.is_empty() {
            len += 1;
        }
        if self.mode != 0 {
            len += 1;
        }
        if self.input.is_some() {
            len += 1;
        }
        if !self.group_expr_name.is_empty() {
            len += 1;
        }
        if !self.aggr_expr_name.is_empty() {
            len += 1;
        }
        if self.input_schema.is_some() {
            len += 1;
        }
        if !self.null_expr.is_empty() {
            len += 1;
        }
        if !self.groups.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.AggregateExecNode", len)?;
        if !self.group_expr.is_empty() {
            struct_ser.serialize_field("groupExpr", &self.group_expr)?;
        }
        if !self.aggr_expr.is_empty() {
            struct_ser.serialize_field("aggrExpr", &self.aggr_expr)?;
        }
        if self.mode != 0 {
            let v = AggregateMode::from_i32(self.mode)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.mode)))?;
            struct_ser.serialize_field("mode", &v)?;
        }
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if !self.group_expr_name.is_empty() {
            struct_ser.serialize_field("groupExprName", &self.group_expr_name)?;
        }
        if !self.aggr_expr_name.is_empty() {
            struct_ser.serialize_field("aggrExprName", &self.aggr_expr_name)?;
        }
        if let Some(v) = self.input_schema.as_ref() {
            struct_ser.serialize_field("inputSchema", v)?;
        }
        if !self.null_expr.is_empty() {
            struct_ser.serialize_field("nullExpr", &self.null_expr)?;
        }
        if !self.groups.is_empty() {
            struct_ser.serialize_field("groups", &self.groups)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AggregateExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "group_expr",
            "groupExpr",
            "aggr_expr",
            "aggrExpr",
            "mode",
            "input",
            "group_expr_name",
            "groupExprName",
            "aggr_expr_name",
            "aggrExprName",
            "input_schema",
            "inputSchema",
            "null_expr",
            "nullExpr",
            "groups",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            GroupExpr,
            AggrExpr,
            Mode,
            Input,
            GroupExprName,
            AggrExprName,
            InputSchema,
            NullExpr,
            Groups,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "groupExpr" | "group_expr" => Ok(GeneratedField::GroupExpr),
                            "aggrExpr" | "aggr_expr" => Ok(GeneratedField::AggrExpr),
                            "mode" => Ok(GeneratedField::Mode),
                            "input" => Ok(GeneratedField::Input),
                            "groupExprName" | "group_expr_name" => Ok(GeneratedField::GroupExprName),
                            "aggrExprName" | "aggr_expr_name" => Ok(GeneratedField::AggrExprName),
                            "inputSchema" | "input_schema" => Ok(GeneratedField::InputSchema),
                            "nullExpr" | "null_expr" => Ok(GeneratedField::NullExpr),
                            "groups" => Ok(GeneratedField::Groups),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AggregateExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.AggregateExecNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<AggregateExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut group_expr__ = None;
                let mut aggr_expr__ = None;
                let mut mode__ = None;
                let mut input__ = None;
                let mut group_expr_name__ = None;
                let mut aggr_expr_name__ = None;
                let mut input_schema__ = None;
                let mut null_expr__ = None;
                let mut groups__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::GroupExpr => {
                            if group_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("groupExpr"));
                            }
                            group_expr__ = Some(map.next_value()?);
                        }
                        GeneratedField::AggrExpr => {
                            if aggr_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggrExpr"));
                            }
                            aggr_expr__ = Some(map.next_value()?);
                        }
                        GeneratedField::Mode => {
                            if mode__.is_some() {
                                return Err(serde::de::Error::duplicate_field("mode"));
                            }
                            mode__ = Some(map.next_value::<AggregateMode>()? as i32);
                        }
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map.next_value()?;
                        }
                        GeneratedField::GroupExprName => {
                            if group_expr_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("groupExprName"));
                            }
                            group_expr_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::AggrExprName => {
                            if aggr_expr_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggrExprName"));
                            }
                            aggr_expr_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::InputSchema => {
                            if input_schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("inputSchema"));
                            }
                            input_schema__ = map.next_value()?;
                        }
                        GeneratedField::NullExpr => {
                            if null_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nullExpr"));
                            }
                            null_expr__ = Some(map.next_value()?);
                        }
                        GeneratedField::Groups => {
                            if groups__.is_some() {
                                return Err(serde::de::Error::duplicate_field("groups"));
                            }
                            groups__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(AggregateExecNode {
                    group_expr: group_expr__.unwrap_or_default(),
                    aggr_expr: aggr_expr__.unwrap_or_default(),
                    mode: mode__.unwrap_or_default(),
                    input: input__,
                    group_expr_name: group_expr_name__.unwrap_or_default(),
                    aggr_expr_name: aggr_expr_name__.unwrap_or_default(),
                    input_schema: input_schema__,
                    null_expr: null_expr__.unwrap_or_default(),
                    groups: groups__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.AggregateExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AggregateExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.aggr_function != 0 {
            len += 1;
        }
        if !self.expr.is_empty() {
            len += 1;
        }
        if self.distinct {
            len += 1;
        }
        if self.filter.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.AggregateExprNode", len)?;
        if self.aggr_function != 0 {
            let v = AggregateFunction::from_i32(self.aggr_function)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.aggr_function)))?;
            struct_ser.serialize_field("aggrFunction", &v)?;
        }
        if !self.expr.is_empty() {
            struct_ser.serialize_field("expr", &self.expr)?;
        }
        if self.distinct {
            struct_ser.serialize_field("distinct", &self.distinct)?;
        }
        if let Some(v) = self.filter.as_ref() {
            struct_ser.serialize_field("filter", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AggregateExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "aggr_function",
            "aggrFunction",
            "expr",
            "distinct",
            "filter",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            AggrFunction,
            Expr,
            Distinct,
            Filter,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "aggrFunction" | "aggr_function" => Ok(GeneratedField::AggrFunction),
                            "expr" => Ok(GeneratedField::Expr),
                            "distinct" => Ok(GeneratedField::Distinct),
                            "filter" => Ok(GeneratedField::Filter),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AggregateExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.AggregateExprNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<AggregateExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut aggr_function__ = None;
                let mut expr__ = None;
                let mut distinct__ = None;
                let mut filter__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::AggrFunction => {
                            if aggr_function__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggrFunction"));
                            }
                            aggr_function__ = Some(map.next_value::<AggregateFunction>()? as i32);
                        }
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = Some(map.next_value()?);
                        }
                        GeneratedField::Distinct => {
                            if distinct__.is_some() {
                                return Err(serde::de::Error::duplicate_field("distinct"));
                            }
                            distinct__ = Some(map.next_value()?);
                        }
                        GeneratedField::Filter => {
                            if filter__.is_some() {
                                return Err(serde::de::Error::duplicate_field("filter"));
                            }
                            filter__ = map.next_value()?;
                        }
                    }
                }
                Ok(AggregateExprNode {
                    aggr_function: aggr_function__.unwrap_or_default(),
                    expr: expr__.unwrap_or_default(),
                    distinct: distinct__.unwrap_or_default(),
                    filter: filter__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.AggregateExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AggregateFunction {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Min => "MIN",
            Self::Max => "MAX",
            Self::Sum => "SUM",
            Self::Avg => "AVG",
            Self::Count => "COUNT",
            Self::ApproxDistinct => "APPROX_DISTINCT",
            Self::ArrayAgg => "ARRAY_AGG",
            Self::Variance => "VARIANCE",
            Self::VariancePop => "VARIANCE_POP",
            Self::Covariance => "COVARIANCE",
            Self::CovariancePop => "COVARIANCE_POP",
            Self::Stddev => "STDDEV",
            Self::StddevPop => "STDDEV_POP",
            Self::Correlation => "CORRELATION",
            Self::ApproxPercentileCont => "APPROX_PERCENTILE_CONT",
            Self::ApproxMedian => "APPROX_MEDIAN",
            Self::ApproxPercentileContWithWeight => "APPROX_PERCENTILE_CONT_WITH_WEIGHT",
            Self::Grouping => "GROUPING",
            Self::Median => "MEDIAN",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for AggregateFunction {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "MIN",
            "MAX",
            "SUM",
            "AVG",
            "COUNT",
            "APPROX_DISTINCT",
            "ARRAY_AGG",
            "VARIANCE",
            "VARIANCE_POP",
            "COVARIANCE",
            "COVARIANCE_POP",
            "STDDEV",
            "STDDEV_POP",
            "CORRELATION",
            "APPROX_PERCENTILE_CONT",
            "APPROX_MEDIAN",
            "APPROX_PERCENTILE_CONT_WITH_WEIGHT",
            "GROUPING",
            "MEDIAN",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AggregateFunction;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(AggregateFunction::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(AggregateFunction::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "MIN" => Ok(AggregateFunction::Min),
                    "MAX" => Ok(AggregateFunction::Max),
                    "SUM" => Ok(AggregateFunction::Sum),
                    "AVG" => Ok(AggregateFunction::Avg),
                    "COUNT" => Ok(AggregateFunction::Count),
                    "APPROX_DISTINCT" => Ok(AggregateFunction::ApproxDistinct),
                    "ARRAY_AGG" => Ok(AggregateFunction::ArrayAgg),
                    "VARIANCE" => Ok(AggregateFunction::Variance),
                    "VARIANCE_POP" => Ok(AggregateFunction::VariancePop),
                    "COVARIANCE" => Ok(AggregateFunction::Covariance),
                    "COVARIANCE_POP" => Ok(AggregateFunction::CovariancePop),
                    "STDDEV" => Ok(AggregateFunction::Stddev),
                    "STDDEV_POP" => Ok(AggregateFunction::StddevPop),
                    "CORRELATION" => Ok(AggregateFunction::Correlation),
                    "APPROX_PERCENTILE_CONT" => Ok(AggregateFunction::ApproxPercentileCont),
                    "APPROX_MEDIAN" => Ok(AggregateFunction::ApproxMedian),
                    "APPROX_PERCENTILE_CONT_WITH_WEIGHT" => Ok(AggregateFunction::ApproxPercentileContWithWeight),
                    "GROUPING" => Ok(AggregateFunction::Grouping),
                    "MEDIAN" => Ok(AggregateFunction::Median),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for AggregateMode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Partial => "PARTIAL",
            Self::Final => "FINAL",
            Self::FinalPartitioned => "FINAL_PARTITIONED",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for AggregateMode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "PARTIAL",
            "FINAL",
            "FINAL_PARTITIONED",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AggregateMode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(AggregateMode::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(AggregateMode::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "PARTIAL" => Ok(AggregateMode::Partial),
                    "FINAL" => Ok(AggregateMode::Final),
                    "FINAL_PARTITIONED" => Ok(AggregateMode::FinalPartitioned),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for AggregateNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if !self.group_expr.is_empty() {
            len += 1;
        }
        if !self.aggr_expr.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.AggregateNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if !self.group_expr.is_empty() {
            struct_ser.serialize_field("groupExpr", &self.group_expr)?;
        }
        if !self.aggr_expr.is_empty() {
            struct_ser.serialize_field("aggrExpr", &self.aggr_expr)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AggregateNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "group_expr",
            "groupExpr",
            "aggr_expr",
            "aggrExpr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            GroupExpr,
            AggrExpr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "groupExpr" | "group_expr" => Ok(GeneratedField::GroupExpr),
                            "aggrExpr" | "aggr_expr" => Ok(GeneratedField::AggrExpr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AggregateNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.AggregateNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<AggregateNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut group_expr__ = None;
                let mut aggr_expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map.next_value()?;
                        }
                        GeneratedField::GroupExpr => {
                            if group_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("groupExpr"));
                            }
                            group_expr__ = Some(map.next_value()?);
                        }
                        GeneratedField::AggrExpr => {
                            if aggr_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggrExpr"));
                            }
                            aggr_expr__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(AggregateNode {
                    input: input__,
                    group_expr: group_expr__.unwrap_or_default(),
                    aggr_expr: aggr_expr__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.AggregateNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AggregateUdfExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.fun_name.is_empty() {
            len += 1;
        }
        if !self.args.is_empty() {
            len += 1;
        }
        if self.filter.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.AggregateUDFExprNode", len)?;
        if !self.fun_name.is_empty() {
            struct_ser.serialize_field("funName", &self.fun_name)?;
        }
        if !self.args.is_empty() {
            struct_ser.serialize_field("args", &self.args)?;
        }
        if let Some(v) = self.filter.as_ref() {
            struct_ser.serialize_field("filter", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AggregateUdfExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "fun_name",
            "funName",
            "args",
            "filter",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            FunName,
            Args,
            Filter,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "funName" | "fun_name" => Ok(GeneratedField::FunName),
                            "args" => Ok(GeneratedField::Args),
                            "filter" => Ok(GeneratedField::Filter),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AggregateUdfExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.AggregateUDFExprNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<AggregateUdfExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut fun_name__ = None;
                let mut args__ = None;
                let mut filter__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::FunName => {
                            if fun_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("funName"));
                            }
                            fun_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::Args => {
                            if args__.is_some() {
                                return Err(serde::de::Error::duplicate_field("args"));
                            }
                            args__ = Some(map.next_value()?);
                        }
                        GeneratedField::Filter => {
                            if filter__.is_some() {
                                return Err(serde::de::Error::duplicate_field("filter"));
                            }
                            filter__ = map.next_value()?;
                        }
                    }
                }
                Ok(AggregateUdfExprNode {
                    fun_name: fun_name__.unwrap_or_default(),
                    args: args__.unwrap_or_default(),
                    filter: filter__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.AggregateUDFExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AliasNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if !self.alias.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.AliasNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if !self.alias.is_empty() {
            struct_ser.serialize_field("alias", &self.alias)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AliasNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "alias",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            Alias,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "alias" => Ok(GeneratedField::Alias),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AliasNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.AliasNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<AliasNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut alias__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                        GeneratedField::Alias => {
                            if alias__.is_some() {
                                return Err(serde::de::Error::duplicate_field("alias"));
                            }
                            alias__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(AliasNode {
                    expr: expr__,
                    alias: alias__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.AliasNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AnalyzeNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if self.verbose {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.AnalyzeNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if self.verbose {
            struct_ser.serialize_field("verbose", &self.verbose)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AnalyzeNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "verbose",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Verbose,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "verbose" => Ok(GeneratedField::Verbose),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AnalyzeNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.AnalyzeNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<AnalyzeNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut verbose__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map.next_value()?;
                        }
                        GeneratedField::Verbose => {
                            if verbose__.is_some() {
                                return Err(serde::de::Error::duplicate_field("verbose"));
                            }
                            verbose__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(AnalyzeNode {
                    input: input__,
                    verbose: verbose__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.AnalyzeNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ArrowType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.arrow_type_enum.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ArrowType", len)?;
        if let Some(v) = self.arrow_type_enum.as_ref() {
            match v {
                arrow_type::ArrowTypeEnum::None(v) => {
                    struct_ser.serialize_field("NONE", v)?;
                }
                arrow_type::ArrowTypeEnum::Bool(v) => {
                    struct_ser.serialize_field("BOOL", v)?;
                }
                arrow_type::ArrowTypeEnum::Uint8(v) => {
                    struct_ser.serialize_field("UINT8", v)?;
                }
                arrow_type::ArrowTypeEnum::Int8(v) => {
                    struct_ser.serialize_field("INT8", v)?;
                }
                arrow_type::ArrowTypeEnum::Uint16(v) => {
                    struct_ser.serialize_field("UINT16", v)?;
                }
                arrow_type::ArrowTypeEnum::Int16(v) => {
                    struct_ser.serialize_field("INT16", v)?;
                }
                arrow_type::ArrowTypeEnum::Uint32(v) => {
                    struct_ser.serialize_field("UINT32", v)?;
                }
                arrow_type::ArrowTypeEnum::Int32(v) => {
                    struct_ser.serialize_field("INT32", v)?;
                }
                arrow_type::ArrowTypeEnum::Uint64(v) => {
                    struct_ser.serialize_field("UINT64", v)?;
                }
                arrow_type::ArrowTypeEnum::Int64(v) => {
                    struct_ser.serialize_field("INT64", v)?;
                }
                arrow_type::ArrowTypeEnum::Float16(v) => {
                    struct_ser.serialize_field("FLOAT16", v)?;
                }
                arrow_type::ArrowTypeEnum::Float32(v) => {
                    struct_ser.serialize_field("FLOAT32", v)?;
                }
                arrow_type::ArrowTypeEnum::Float64(v) => {
                    struct_ser.serialize_field("FLOAT64", v)?;
                }
                arrow_type::ArrowTypeEnum::Utf8(v) => {
                    struct_ser.serialize_field("UTF8", v)?;
                }
                arrow_type::ArrowTypeEnum::LargeUtf8(v) => {
                    struct_ser.serialize_field("LARGEUTF8", v)?;
                }
                arrow_type::ArrowTypeEnum::Binary(v) => {
                    struct_ser.serialize_field("BINARY", v)?;
                }
                arrow_type::ArrowTypeEnum::FixedSizeBinary(v) => {
                    struct_ser.serialize_field("FIXEDSIZEBINARY", v)?;
                }
                arrow_type::ArrowTypeEnum::LargeBinary(v) => {
                    struct_ser.serialize_field("LARGEBINARY", v)?;
                }
                arrow_type::ArrowTypeEnum::Date32(v) => {
                    struct_ser.serialize_field("DATE32", v)?;
                }
                arrow_type::ArrowTypeEnum::Date64(v) => {
                    struct_ser.serialize_field("DATE64", v)?;
                }
                arrow_type::ArrowTypeEnum::Duration(v) => {
                    let v = TimeUnit::from_i32(*v)
                        .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
                    struct_ser.serialize_field("DURATION", &v)?;
                }
                arrow_type::ArrowTypeEnum::Timestamp(v) => {
                    struct_ser.serialize_field("TIMESTAMP", v)?;
                }
                arrow_type::ArrowTypeEnum::Time32(v) => {
                    let v = TimeUnit::from_i32(*v)
                        .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
                    struct_ser.serialize_field("TIME32", &v)?;
                }
                arrow_type::ArrowTypeEnum::Time64(v) => {
                    let v = TimeUnit::from_i32(*v)
                        .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
                    struct_ser.serialize_field("TIME64", &v)?;
                }
                arrow_type::ArrowTypeEnum::Interval(v) => {
                    let v = IntervalUnit::from_i32(*v)
                        .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
                    struct_ser.serialize_field("INTERVAL", &v)?;
                }
                arrow_type::ArrowTypeEnum::Decimal(v) => {
                    struct_ser.serialize_field("DECIMAL", v)?;
                }
                arrow_type::ArrowTypeEnum::List(v) => {
                    struct_ser.serialize_field("LIST", v)?;
                }
                arrow_type::ArrowTypeEnum::LargeList(v) => {
                    struct_ser.serialize_field("LARGELIST", v)?;
                }
                arrow_type::ArrowTypeEnum::FixedSizeList(v) => {
                    struct_ser.serialize_field("FIXEDSIZELIST", v)?;
                }
                arrow_type::ArrowTypeEnum::Struct(v) => {
                    struct_ser.serialize_field("STRUCT", v)?;
                }
                arrow_type::ArrowTypeEnum::Union(v) => {
                    struct_ser.serialize_field("UNION", v)?;
                }
                arrow_type::ArrowTypeEnum::Dictionary(v) => {
                    struct_ser.serialize_field("DICTIONARY", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ArrowType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "NONE",
            "BOOL",
            "UINT8",
            "INT8",
            "UINT16",
            "INT16",
            "UINT32",
            "INT32",
            "UINT64",
            "INT64",
            "FLOAT16",
            "FLOAT32",
            "FLOAT64",
            "UTF8",
            "LARGE_UTF8",
            "LARGEUTF8",
            "BINARY",
            "FIXED_SIZE_BINARY",
            "FIXEDSIZEBINARY",
            "LARGE_BINARY",
            "LARGEBINARY",
            "DATE32",
            "DATE64",
            "DURATION",
            "TIMESTAMP",
            "TIME32",
            "TIME64",
            "INTERVAL",
            "DECIMAL",
            "LIST",
            "LARGE_LIST",
            "LARGELIST",
            "FIXED_SIZE_LIST",
            "FIXEDSIZELIST",
            "STRUCT",
            "UNION",
            "DICTIONARY",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            None,
            Bool,
            Uint8,
            Int8,
            Uint16,
            Int16,
            Uint32,
            Int32,
            Uint64,
            Int64,
            Float16,
            Float32,
            Float64,
            Utf8,
            LargeUtf8,
            Binary,
            FixedSizeBinary,
            LargeBinary,
            Date32,
            Date64,
            Duration,
            Timestamp,
            Time32,
            Time64,
            Interval,
            Decimal,
            List,
            LargeList,
            FixedSizeList,
            Struct,
            Union,
            Dictionary,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "NONE" => Ok(GeneratedField::None),
                            "BOOL" => Ok(GeneratedField::Bool),
                            "UINT8" => Ok(GeneratedField::Uint8),
                            "INT8" => Ok(GeneratedField::Int8),
                            "UINT16" => Ok(GeneratedField::Uint16),
                            "INT16" => Ok(GeneratedField::Int16),
                            "UINT32" => Ok(GeneratedField::Uint32),
                            "INT32" => Ok(GeneratedField::Int32),
                            "UINT64" => Ok(GeneratedField::Uint64),
                            "INT64" => Ok(GeneratedField::Int64),
                            "FLOAT16" => Ok(GeneratedField::Float16),
                            "FLOAT32" => Ok(GeneratedField::Float32),
                            "FLOAT64" => Ok(GeneratedField::Float64),
                            "UTF8" => Ok(GeneratedField::Utf8),
                            "LARGEUTF8" | "LARGE_UTF8" => Ok(GeneratedField::LargeUtf8),
                            "BINARY" => Ok(GeneratedField::Binary),
                            "FIXEDSIZEBINARY" | "FIXED_SIZE_BINARY" => Ok(GeneratedField::FixedSizeBinary),
                            "LARGEBINARY" | "LARGE_BINARY" => Ok(GeneratedField::LargeBinary),
                            "DATE32" => Ok(GeneratedField::Date32),
                            "DATE64" => Ok(GeneratedField::Date64),
                            "DURATION" => Ok(GeneratedField::Duration),
                            "TIMESTAMP" => Ok(GeneratedField::Timestamp),
                            "TIME32" => Ok(GeneratedField::Time32),
                            "TIME64" => Ok(GeneratedField::Time64),
                            "INTERVAL" => Ok(GeneratedField::Interval),
                            "DECIMAL" => Ok(GeneratedField::Decimal),
                            "LIST" => Ok(GeneratedField::List),
                            "LARGELIST" | "LARGE_LIST" => Ok(GeneratedField::LargeList),
                            "FIXEDSIZELIST" | "FIXED_SIZE_LIST" => Ok(GeneratedField::FixedSizeList),
                            "STRUCT" => Ok(GeneratedField::Struct),
                            "UNION" => Ok(GeneratedField::Union),
                            "DICTIONARY" => Ok(GeneratedField::Dictionary),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ArrowType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ArrowType")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ArrowType, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut arrow_type_enum__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::None => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("NONE"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::None)
;
                        }
                        GeneratedField::Bool => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("BOOL"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Bool)
;
                        }
                        GeneratedField::Uint8 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("UINT8"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Uint8)
;
                        }
                        GeneratedField::Int8 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("INT8"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Int8)
;
                        }
                        GeneratedField::Uint16 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("UINT16"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Uint16)
;
                        }
                        GeneratedField::Int16 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("INT16"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Int16)
;
                        }
                        GeneratedField::Uint32 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("UINT32"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Uint32)
;
                        }
                        GeneratedField::Int32 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("INT32"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Int32)
;
                        }
                        GeneratedField::Uint64 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("UINT64"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Uint64)
;
                        }
                        GeneratedField::Int64 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("INT64"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Int64)
;
                        }
                        GeneratedField::Float16 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("FLOAT16"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Float16)
;
                        }
                        GeneratedField::Float32 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("FLOAT32"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Float32)
;
                        }
                        GeneratedField::Float64 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("FLOAT64"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Float64)
;
                        }
                        GeneratedField::Utf8 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("UTF8"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Utf8)
;
                        }
                        GeneratedField::LargeUtf8 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("LARGEUTF8"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::LargeUtf8)
;
                        }
                        GeneratedField::Binary => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("BINARY"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Binary)
;
                        }
                        GeneratedField::FixedSizeBinary => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("FIXEDSIZEBINARY"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| arrow_type::ArrowTypeEnum::FixedSizeBinary(x.0));
                        }
                        GeneratedField::LargeBinary => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("LARGEBINARY"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::LargeBinary)
;
                        }
                        GeneratedField::Date32 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("DATE32"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Date32)
;
                        }
                        GeneratedField::Date64 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("DATE64"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Date64)
;
                        }
                        GeneratedField::Duration => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("DURATION"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<TimeUnit>>()?.map(|x| arrow_type::ArrowTypeEnum::Duration(x as i32));
                        }
                        GeneratedField::Timestamp => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("TIMESTAMP"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Timestamp)
;
                        }
                        GeneratedField::Time32 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("TIME32"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<TimeUnit>>()?.map(|x| arrow_type::ArrowTypeEnum::Time32(x as i32));
                        }
                        GeneratedField::Time64 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("TIME64"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<TimeUnit>>()?.map(|x| arrow_type::ArrowTypeEnum::Time64(x as i32));
                        }
                        GeneratedField::Interval => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("INTERVAL"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<IntervalUnit>>()?.map(|x| arrow_type::ArrowTypeEnum::Interval(x as i32));
                        }
                        GeneratedField::Decimal => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("DECIMAL"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Decimal)
;
                        }
                        GeneratedField::List => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("LIST"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::List)
;
                        }
                        GeneratedField::LargeList => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("LARGELIST"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::LargeList)
;
                        }
                        GeneratedField::FixedSizeList => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("FIXEDSIZELIST"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::FixedSizeList)
;
                        }
                        GeneratedField::Struct => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("STRUCT"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Struct)
;
                        }
                        GeneratedField::Union => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("UNION"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Union)
;
                        }
                        GeneratedField::Dictionary => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("DICTIONARY"));
                            }
                            arrow_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Dictionary)
;
                        }
                    }
                }
                Ok(ArrowType {
                    arrow_type_enum: arrow_type_enum__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ArrowType", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AvroFormat {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("datafusion.AvroFormat", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AvroFormat {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                            Err(serde::de::Error::unknown_field(value, FIELDS))
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AvroFormat;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.AvroFormat")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<AvroFormat, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map.next_key::<GeneratedField>()?.is_some() {
                    let _ = map.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(AvroFormat {
                })
            }
        }
        deserializer.deserialize_struct("datafusion.AvroFormat", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AvroScanExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.base_conf.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.AvroScanExecNode", len)?;
        if let Some(v) = self.base_conf.as_ref() {
            struct_ser.serialize_field("baseConf", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AvroScanExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "base_conf",
            "baseConf",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            BaseConf,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "baseConf" | "base_conf" => Ok(GeneratedField::BaseConf),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AvroScanExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.AvroScanExecNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<AvroScanExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut base_conf__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::BaseConf => {
                            if base_conf__.is_some() {
                                return Err(serde::de::Error::duplicate_field("baseConf"));
                            }
                            base_conf__ = map.next_value()?;
                        }
                    }
                }
                Ok(AvroScanExecNode {
                    base_conf: base_conf__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.AvroScanExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for BareTableReference {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.table.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.BareTableReference", len)?;
        if !self.table.is_empty() {
            struct_ser.serialize_field("table", &self.table)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for BareTableReference {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "table",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Table,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "table" => Ok(GeneratedField::Table),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = BareTableReference;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.BareTableReference")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<BareTableReference, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Table => {
                            if table__.is_some() {
                                return Err(serde::de::Error::duplicate_field("table"));
                            }
                            table__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(BareTableReference {
                    table: table__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.BareTableReference", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for BetweenNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if self.negated {
            len += 1;
        }
        if self.low.is_some() {
            len += 1;
        }
        if self.high.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.BetweenNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if self.negated {
            struct_ser.serialize_field("negated", &self.negated)?;
        }
        if let Some(v) = self.low.as_ref() {
            struct_ser.serialize_field("low", v)?;
        }
        if let Some(v) = self.high.as_ref() {
            struct_ser.serialize_field("high", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for BetweenNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "negated",
            "low",
            "high",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            Negated,
            Low,
            High,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "negated" => Ok(GeneratedField::Negated),
                            "low" => Ok(GeneratedField::Low),
                            "high" => Ok(GeneratedField::High),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = BetweenNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.BetweenNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<BetweenNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut negated__ = None;
                let mut low__ = None;
                let mut high__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                        GeneratedField::Negated => {
                            if negated__.is_some() {
                                return Err(serde::de::Error::duplicate_field("negated"));
                            }
                            negated__ = Some(map.next_value()?);
                        }
                        GeneratedField::Low => {
                            if low__.is_some() {
                                return Err(serde::de::Error::duplicate_field("low"));
                            }
                            low__ = map.next_value()?;
                        }
                        GeneratedField::High => {
                            if high__.is_some() {
                                return Err(serde::de::Error::duplicate_field("high"));
                            }
                            high__ = map.next_value()?;
                        }
                    }
                }
                Ok(BetweenNode {
                    expr: expr__,
                    negated: negated__.unwrap_or_default(),
                    low: low__,
                    high: high__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.BetweenNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for BinaryExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.operands.is_empty() {
            len += 1;
        }
        if !self.op.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.BinaryExprNode", len)?;
        if !self.operands.is_empty() {
            struct_ser.serialize_field("operands", &self.operands)?;
        }
        if !self.op.is_empty() {
            struct_ser.serialize_field("op", &self.op)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for BinaryExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "operands",
            "op",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Operands,
            Op,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "operands" => Ok(GeneratedField::Operands),
                            "op" => Ok(GeneratedField::Op),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = BinaryExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.BinaryExprNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<BinaryExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut operands__ = None;
                let mut op__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Operands => {
                            if operands__.is_some() {
                                return Err(serde::de::Error::duplicate_field("operands"));
                            }
                            operands__ = Some(map.next_value()?);
                        }
                        GeneratedField::Op => {
                            if op__.is_some() {
                                return Err(serde::de::Error::duplicate_field("op"));
                            }
                            op__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(BinaryExprNode {
                    operands: operands__.unwrap_or_default(),
                    op: op__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.BinaryExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for BuiltInWindowFunction {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::RowNumber => "ROW_NUMBER",
            Self::Rank => "RANK",
            Self::DenseRank => "DENSE_RANK",
            Self::PercentRank => "PERCENT_RANK",
            Self::CumeDist => "CUME_DIST",
            Self::Ntile => "NTILE",
            Self::Lag => "LAG",
            Self::Lead => "LEAD",
            Self::FirstValue => "FIRST_VALUE",
            Self::LastValue => "LAST_VALUE",
            Self::NthValue => "NTH_VALUE",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for BuiltInWindowFunction {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "ROW_NUMBER",
            "RANK",
            "DENSE_RANK",
            "PERCENT_RANK",
            "CUME_DIST",
            "NTILE",
            "LAG",
            "LEAD",
            "FIRST_VALUE",
            "LAST_VALUE",
            "NTH_VALUE",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = BuiltInWindowFunction;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(BuiltInWindowFunction::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(BuiltInWindowFunction::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "ROW_NUMBER" => Ok(BuiltInWindowFunction::RowNumber),
                    "RANK" => Ok(BuiltInWindowFunction::Rank),
                    "DENSE_RANK" => Ok(BuiltInWindowFunction::DenseRank),
                    "PERCENT_RANK" => Ok(BuiltInWindowFunction::PercentRank),
                    "CUME_DIST" => Ok(BuiltInWindowFunction::CumeDist),
                    "NTILE" => Ok(BuiltInWindowFunction::Ntile),
                    "LAG" => Ok(BuiltInWindowFunction::Lag),
                    "LEAD" => Ok(BuiltInWindowFunction::Lead),
                    "FIRST_VALUE" => Ok(BuiltInWindowFunction::FirstValue),
                    "LAST_VALUE" => Ok(BuiltInWindowFunction::LastValue),
                    "NTH_VALUE" => Ok(BuiltInWindowFunction::NthValue),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for CaseNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if !self.when_then_expr.is_empty() {
            len += 1;
        }
        if self.else_expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CaseNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if !self.when_then_expr.is_empty() {
            struct_ser.serialize_field("whenThenExpr", &self.when_then_expr)?;
        }
        if let Some(v) = self.else_expr.as_ref() {
            struct_ser.serialize_field("elseExpr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CaseNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "when_then_expr",
            "whenThenExpr",
            "else_expr",
            "elseExpr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            WhenThenExpr,
            ElseExpr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "whenThenExpr" | "when_then_expr" => Ok(GeneratedField::WhenThenExpr),
                            "elseExpr" | "else_expr" => Ok(GeneratedField::ElseExpr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CaseNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CaseNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CaseNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut when_then_expr__ = None;
                let mut else_expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                        GeneratedField::WhenThenExpr => {
                            if when_then_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("whenThenExpr"));
                            }
                            when_then_expr__ = Some(map.next_value()?);
                        }
                        GeneratedField::ElseExpr => {
                            if else_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("elseExpr"));
                            }
                            else_expr__ = map.next_value()?;
                        }
                    }
                }
                Ok(CaseNode {
                    expr: expr__,
                    when_then_expr: when_then_expr__.unwrap_or_default(),
                    else_expr: else_expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CaseNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CastNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if self.arrow_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CastNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if let Some(v) = self.arrow_type.as_ref() {
            struct_ser.serialize_field("arrowType", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CastNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "arrow_type",
            "arrowType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            ArrowType,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "arrowType" | "arrow_type" => Ok(GeneratedField::ArrowType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CastNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CastNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CastNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut arrow_type__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                        GeneratedField::ArrowType => {
                            if arrow_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("arrowType"));
                            }
                            arrow_type__ = map.next_value()?;
                        }
                    }
                }
                Ok(CastNode {
                    expr: expr__,
                    arrow_type: arrow_type__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CastNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CoalesceBatchesExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if self.target_batch_size != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CoalesceBatchesExecNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if self.target_batch_size != 0 {
            struct_ser.serialize_field("targetBatchSize", &self.target_batch_size)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CoalesceBatchesExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "target_batch_size",
            "targetBatchSize",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            TargetBatchSize,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "targetBatchSize" | "target_batch_size" => Ok(GeneratedField::TargetBatchSize),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CoalesceBatchesExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CoalesceBatchesExecNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CoalesceBatchesExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut target_batch_size__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map.next_value()?;
                        }
                        GeneratedField::TargetBatchSize => {
                            if target_batch_size__.is_some() {
                                return Err(serde::de::Error::duplicate_field("targetBatchSize"));
                            }
                            target_batch_size__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(CoalesceBatchesExecNode {
                    input: input__,
                    target_batch_size: target_batch_size__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CoalesceBatchesExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CoalescePartitionsExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CoalescePartitionsExecNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CoalescePartitionsExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CoalescePartitionsExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CoalescePartitionsExecNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CoalescePartitionsExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map.next_value()?;
                        }
                    }
                }
                Ok(CoalescePartitionsExecNode {
                    input: input__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CoalescePartitionsExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Column {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.name.is_empty() {
            len += 1;
        }
        if self.relation.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.Column", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if let Some(v) = self.relation.as_ref() {
            struct_ser.serialize_field("relation", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Column {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "relation",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Relation,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "relation" => Ok(GeneratedField::Relation),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Column;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.Column")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Column, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut relation__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map.next_value()?);
                        }
                        GeneratedField::Relation => {
                            if relation__.is_some() {
                                return Err(serde::de::Error::duplicate_field("relation"));
                            }
                            relation__ = map.next_value()?;
                        }
                    }
                }
                Ok(Column {
                    name: name__.unwrap_or_default(),
                    relation: relation__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.Column", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ColumnIndex {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.index != 0 {
            len += 1;
        }
        if self.side != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ColumnIndex", len)?;
        if self.index != 0 {
            struct_ser.serialize_field("index", &self.index)?;
        }
        if self.side != 0 {
            let v = JoinSide::from_i32(self.side)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.side)))?;
            struct_ser.serialize_field("side", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ColumnIndex {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "index",
            "side",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Index,
            Side,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "index" => Ok(GeneratedField::Index),
                            "side" => Ok(GeneratedField::Side),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ColumnIndex;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ColumnIndex")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ColumnIndex, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut index__ = None;
                let mut side__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Index => {
                            if index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("index"));
                            }
                            index__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Side => {
                            if side__.is_some() {
                                return Err(serde::de::Error::duplicate_field("side"));
                            }
                            side__ = Some(map.next_value::<JoinSide>()? as i32);
                        }
                    }
                }
                Ok(ColumnIndex {
                    index: index__.unwrap_or_default(),
                    side: side__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ColumnIndex", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ColumnRelation {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.relation.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ColumnRelation", len)?;
        if !self.relation.is_empty() {
            struct_ser.serialize_field("relation", &self.relation)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ColumnRelation {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "relation",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Relation,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "relation" => Ok(GeneratedField::Relation),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ColumnRelation;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ColumnRelation")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ColumnRelation, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut relation__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Relation => {
                            if relation__.is_some() {
                                return Err(serde::de::Error::duplicate_field("relation"));
                            }
                            relation__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ColumnRelation {
                    relation: relation__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ColumnRelation", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ColumnStats {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.min_value.is_some() {
            len += 1;
        }
        if self.max_value.is_some() {
            len += 1;
        }
        if self.null_count != 0 {
            len += 1;
        }
        if self.distinct_count != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ColumnStats", len)?;
        if let Some(v) = self.min_value.as_ref() {
            struct_ser.serialize_field("minValue", v)?;
        }
        if let Some(v) = self.max_value.as_ref() {
            struct_ser.serialize_field("maxValue", v)?;
        }
        if self.null_count != 0 {
            struct_ser.serialize_field("nullCount", &self.null_count)?;
        }
        if self.distinct_count != 0 {
            struct_ser.serialize_field("distinctCount", &self.distinct_count)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ColumnStats {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "min_value",
            "minValue",
            "max_value",
            "maxValue",
            "null_count",
            "nullCount",
            "distinct_count",
            "distinctCount",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            MinValue,
            MaxValue,
            NullCount,
            DistinctCount,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "minValue" | "min_value" => Ok(GeneratedField::MinValue),
                            "maxValue" | "max_value" => Ok(GeneratedField::MaxValue),
                            "nullCount" | "null_count" => Ok(GeneratedField::NullCount),
                            "distinctCount" | "distinct_count" => Ok(GeneratedField::DistinctCount),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ColumnStats;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ColumnStats")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ColumnStats, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut min_value__ = None;
                let mut max_value__ = None;
                let mut null_count__ = None;
                let mut distinct_count__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::MinValue => {
                            if min_value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("minValue"));
                            }
                            min_value__ = map.next_value()?;
                        }
                        GeneratedField::MaxValue => {
                            if max_value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("maxValue"));
                            }
                            max_value__ = map.next_value()?;
                        }
                        GeneratedField::NullCount => {
                            if null_count__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nullCount"));
                            }
                            null_count__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::DistinctCount => {
                            if distinct_count__.is_some() {
                                return Err(serde::de::Error::duplicate_field("distinctCount"));
                            }
                            distinct_count__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(ColumnStats {
                    min_value: min_value__,
                    max_value: max_value__,
                    null_count: null_count__.unwrap_or_default(),
                    distinct_count: distinct_count__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ColumnStats", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateCatalogNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.catalog_name.is_empty() {
            len += 1;
        }
        if self.if_not_exists {
            len += 1;
        }
        if self.schema.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CreateCatalogNode", len)?;
        if !self.catalog_name.is_empty() {
            struct_ser.serialize_field("catalogName", &self.catalog_name)?;
        }
        if self.if_not_exists {
            struct_ser.serialize_field("ifNotExists", &self.if_not_exists)?;
        }
        if let Some(v) = self.schema.as_ref() {
            struct_ser.serialize_field("schema", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateCatalogNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "catalog_name",
            "catalogName",
            "if_not_exists",
            "ifNotExists",
            "schema",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            CatalogName,
            IfNotExists,
            Schema,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "catalogName" | "catalog_name" => Ok(GeneratedField::CatalogName),
                            "ifNotExists" | "if_not_exists" => Ok(GeneratedField::IfNotExists),
                            "schema" => Ok(GeneratedField::Schema),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreateCatalogNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CreateCatalogNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateCatalogNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut catalog_name__ = None;
                let mut if_not_exists__ = None;
                let mut schema__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::CatalogName => {
                            if catalog_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("catalogName"));
                            }
                            catalog_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::IfNotExists => {
                            if if_not_exists__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ifNotExists"));
                            }
                            if_not_exists__ = Some(map.next_value()?);
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = map.next_value()?;
                        }
                    }
                }
                Ok(CreateCatalogNode {
                    catalog_name: catalog_name__.unwrap_or_default(),
                    if_not_exists: if_not_exists__.unwrap_or_default(),
                    schema: schema__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CreateCatalogNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateCatalogSchemaNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.schema_name.is_empty() {
            len += 1;
        }
        if self.if_not_exists {
            len += 1;
        }
        if self.schema.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CreateCatalogSchemaNode", len)?;
        if !self.schema_name.is_empty() {
            struct_ser.serialize_field("schemaName", &self.schema_name)?;
        }
        if self.if_not_exists {
            struct_ser.serialize_field("ifNotExists", &self.if_not_exists)?;
        }
        if let Some(v) = self.schema.as_ref() {
            struct_ser.serialize_field("schema", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateCatalogSchemaNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "schema_name",
            "schemaName",
            "if_not_exists",
            "ifNotExists",
            "schema",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SchemaName,
            IfNotExists,
            Schema,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "schemaName" | "schema_name" => Ok(GeneratedField::SchemaName),
                            "ifNotExists" | "if_not_exists" => Ok(GeneratedField::IfNotExists),
                            "schema" => Ok(GeneratedField::Schema),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreateCatalogSchemaNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CreateCatalogSchemaNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateCatalogSchemaNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut schema_name__ = None;
                let mut if_not_exists__ = None;
                let mut schema__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::SchemaName => {
                            if schema_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schemaName"));
                            }
                            schema_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::IfNotExists => {
                            if if_not_exists__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ifNotExists"));
                            }
                            if_not_exists__ = Some(map.next_value()?);
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = map.next_value()?;
                        }
                    }
                }
                Ok(CreateCatalogSchemaNode {
                    schema_name: schema_name__.unwrap_or_default(),
                    if_not_exists: if_not_exists__.unwrap_or_default(),
                    schema: schema__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CreateCatalogSchemaNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateExternalTableNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.name.is_some() {
            len += 1;
        }
        if !self.location.is_empty() {
            len += 1;
        }
        if !self.file_type.is_empty() {
            len += 1;
        }
        if self.has_header {
            len += 1;
        }
        if self.schema.is_some() {
            len += 1;
        }
        if !self.table_partition_cols.is_empty() {
            len += 1;
        }
        if self.if_not_exists {
            len += 1;
        }
        if !self.delimiter.is_empty() {
            len += 1;
        }
        if !self.definition.is_empty() {
            len += 1;
        }
        if !self.file_compression_type.is_empty() {
            len += 1;
        }
        if !self.options.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CreateExternalTableNode", len)?;
        if let Some(v) = self.name.as_ref() {
            struct_ser.serialize_field("name", v)?;
        }
        if !self.location.is_empty() {
            struct_ser.serialize_field("location", &self.location)?;
        }
        if !self.file_type.is_empty() {
            struct_ser.serialize_field("fileType", &self.file_type)?;
        }
        if self.has_header {
            struct_ser.serialize_field("hasHeader", &self.has_header)?;
        }
        if let Some(v) = self.schema.as_ref() {
            struct_ser.serialize_field("schema", v)?;
        }
        if !self.table_partition_cols.is_empty() {
            struct_ser.serialize_field("tablePartitionCols", &self.table_partition_cols)?;
        }
        if self.if_not_exists {
            struct_ser.serialize_field("ifNotExists", &self.if_not_exists)?;
        }
        if !self.delimiter.is_empty() {
            struct_ser.serialize_field("delimiter", &self.delimiter)?;
        }
        if !self.definition.is_empty() {
            struct_ser.serialize_field("definition", &self.definition)?;
        }
        if !self.file_compression_type.is_empty() {
            struct_ser.serialize_field("fileCompressionType", &self.file_compression_type)?;
        }
        if !self.options.is_empty() {
            struct_ser.serialize_field("options", &self.options)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateExternalTableNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "location",
            "file_type",
            "fileType",
            "has_header",
            "hasHeader",
            "schema",
            "table_partition_cols",
            "tablePartitionCols",
            "if_not_exists",
            "ifNotExists",
            "delimiter",
            "definition",
            "file_compression_type",
            "fileCompressionType",
            "options",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Location,
            FileType,
            HasHeader,
            Schema,
            TablePartitionCols,
            IfNotExists,
            Delimiter,
            Definition,
            FileCompressionType,
            Options,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "location" => Ok(GeneratedField::Location),
                            "fileType" | "file_type" => Ok(GeneratedField::FileType),
                            "hasHeader" | "has_header" => Ok(GeneratedField::HasHeader),
                            "schema" => Ok(GeneratedField::Schema),
                            "tablePartitionCols" | "table_partition_cols" => Ok(GeneratedField::TablePartitionCols),
                            "ifNotExists" | "if_not_exists" => Ok(GeneratedField::IfNotExists),
                            "delimiter" => Ok(GeneratedField::Delimiter),
                            "definition" => Ok(GeneratedField::Definition),
                            "fileCompressionType" | "file_compression_type" => Ok(GeneratedField::FileCompressionType),
                            "options" => Ok(GeneratedField::Options),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreateExternalTableNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CreateExternalTableNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateExternalTableNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut location__ = None;
                let mut file_type__ = None;
                let mut has_header__ = None;
                let mut schema__ = None;
                let mut table_partition_cols__ = None;
                let mut if_not_exists__ = None;
                let mut delimiter__ = None;
                let mut definition__ = None;
                let mut file_compression_type__ = None;
                let mut options__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = map.next_value()?;
                        }
                        GeneratedField::Location => {
                            if location__.is_some() {
                                return Err(serde::de::Error::duplicate_field("location"));
                            }
                            location__ = Some(map.next_value()?);
                        }
                        GeneratedField::FileType => {
                            if file_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fileType"));
                            }
                            file_type__ = Some(map.next_value()?);
                        }
                        GeneratedField::HasHeader => {
                            if has_header__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hasHeader"));
                            }
                            has_header__ = Some(map.next_value()?);
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = map.next_value()?;
                        }
                        GeneratedField::TablePartitionCols => {
                            if table_partition_cols__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tablePartitionCols"));
                            }
                            table_partition_cols__ = Some(map.next_value()?);
                        }
                        GeneratedField::IfNotExists => {
                            if if_not_exists__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ifNotExists"));
                            }
                            if_not_exists__ = Some(map.next_value()?);
                        }
                        GeneratedField::Delimiter => {
                            if delimiter__.is_some() {
                                return Err(serde::de::Error::duplicate_field("delimiter"));
                            }
                            delimiter__ = Some(map.next_value()?);
                        }
                        GeneratedField::Definition => {
                            if definition__.is_some() {
                                return Err(serde::de::Error::duplicate_field("definition"));
                            }
                            definition__ = Some(map.next_value()?);
                        }
                        GeneratedField::FileCompressionType => {
                            if file_compression_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fileCompressionType"));
                            }
                            file_compression_type__ = Some(map.next_value()?);
                        }
                        GeneratedField::Options => {
                            if options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("options"));
                            }
                            options__ = Some(
                                map.next_value::<std::collections::HashMap<_, _>>()?
                            );
                        }
                    }
                }
                Ok(CreateExternalTableNode {
                    name: name__,
                    location: location__.unwrap_or_default(),
                    file_type: file_type__.unwrap_or_default(),
                    has_header: has_header__.unwrap_or_default(),
                    schema: schema__,
                    table_partition_cols: table_partition_cols__.unwrap_or_default(),
                    if_not_exists: if_not_exists__.unwrap_or_default(),
                    delimiter: delimiter__.unwrap_or_default(),
                    definition: definition__.unwrap_or_default(),
                    file_compression_type: file_compression_type__.unwrap_or_default(),
                    options: options__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CreateExternalTableNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateViewNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.name.is_some() {
            len += 1;
        }
        if self.input.is_some() {
            len += 1;
        }
        if self.or_replace {
            len += 1;
        }
        if !self.definition.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CreateViewNode", len)?;
        if let Some(v) = self.name.as_ref() {
            struct_ser.serialize_field("name", v)?;
        }
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if self.or_replace {
            struct_ser.serialize_field("orReplace", &self.or_replace)?;
        }
        if !self.definition.is_empty() {
            struct_ser.serialize_field("definition", &self.definition)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateViewNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "input",
            "or_replace",
            "orReplace",
            "definition",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Input,
            OrReplace,
            Definition,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "input" => Ok(GeneratedField::Input),
                            "orReplace" | "or_replace" => Ok(GeneratedField::OrReplace),
                            "definition" => Ok(GeneratedField::Definition),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreateViewNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CreateViewNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateViewNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut input__ = None;
                let mut or_replace__ = None;
                let mut definition__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = map.next_value()?;
                        }
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map.next_value()?;
                        }
                        GeneratedField::OrReplace => {
                            if or_replace__.is_some() {
                                return Err(serde::de::Error::duplicate_field("orReplace"));
                            }
                            or_replace__ = Some(map.next_value()?);
                        }
                        GeneratedField::Definition => {
                            if definition__.is_some() {
                                return Err(serde::de::Error::duplicate_field("definition"));
                            }
                            definition__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(CreateViewNode {
                    name: name__,
                    input: input__,
                    or_replace: or_replace__.unwrap_or_default(),
                    definition: definition__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CreateViewNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CrossJoinExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.left.is_some() {
            len += 1;
        }
        if self.right.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CrossJoinExecNode", len)?;
        if let Some(v) = self.left.as_ref() {
            struct_ser.serialize_field("left", v)?;
        }
        if let Some(v) = self.right.as_ref() {
            struct_ser.serialize_field("right", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CrossJoinExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "left",
            "right",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Left,
            Right,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "left" => Ok(GeneratedField::Left),
                            "right" => Ok(GeneratedField::Right),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CrossJoinExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CrossJoinExecNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CrossJoinExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut left__ = None;
                let mut right__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Left => {
                            if left__.is_some() {
                                return Err(serde::de::Error::duplicate_field("left"));
                            }
                            left__ = map.next_value()?;
                        }
                        GeneratedField::Right => {
                            if right__.is_some() {
                                return Err(serde::de::Error::duplicate_field("right"));
                            }
                            right__ = map.next_value()?;
                        }
                    }
                }
                Ok(CrossJoinExecNode {
                    left: left__,
                    right: right__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CrossJoinExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CrossJoinNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.left.is_some() {
            len += 1;
        }
        if self.right.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CrossJoinNode", len)?;
        if let Some(v) = self.left.as_ref() {
            struct_ser.serialize_field("left", v)?;
        }
        if let Some(v) = self.right.as_ref() {
            struct_ser.serialize_field("right", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CrossJoinNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "left",
            "right",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Left,
            Right,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "left" => Ok(GeneratedField::Left),
                            "right" => Ok(GeneratedField::Right),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CrossJoinNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CrossJoinNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CrossJoinNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut left__ = None;
                let mut right__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Left => {
                            if left__.is_some() {
                                return Err(serde::de::Error::duplicate_field("left"));
                            }
                            left__ = map.next_value()?;
                        }
                        GeneratedField::Right => {
                            if right__.is_some() {
                                return Err(serde::de::Error::duplicate_field("right"));
                            }
                            right__ = map.next_value()?;
                        }
                    }
                }
                Ok(CrossJoinNode {
                    left: left__,
                    right: right__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CrossJoinNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CsvFormat {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.has_header {
            len += 1;
        }
        if !self.delimiter.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CsvFormat", len)?;
        if self.has_header {
            struct_ser.serialize_field("hasHeader", &self.has_header)?;
        }
        if !self.delimiter.is_empty() {
            struct_ser.serialize_field("delimiter", &self.delimiter)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CsvFormat {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "has_header",
            "hasHeader",
            "delimiter",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            HasHeader,
            Delimiter,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "hasHeader" | "has_header" => Ok(GeneratedField::HasHeader),
                            "delimiter" => Ok(GeneratedField::Delimiter),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CsvFormat;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CsvFormat")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CsvFormat, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut has_header__ = None;
                let mut delimiter__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::HasHeader => {
                            if has_header__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hasHeader"));
                            }
                            has_header__ = Some(map.next_value()?);
                        }
                        GeneratedField::Delimiter => {
                            if delimiter__.is_some() {
                                return Err(serde::de::Error::duplicate_field("delimiter"));
                            }
                            delimiter__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(CsvFormat {
                    has_header: has_header__.unwrap_or_default(),
                    delimiter: delimiter__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CsvFormat", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CsvScanExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.base_conf.is_some() {
            len += 1;
        }
        if self.has_header {
            len += 1;
        }
        if !self.delimiter.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CsvScanExecNode", len)?;
        if let Some(v) = self.base_conf.as_ref() {
            struct_ser.serialize_field("baseConf", v)?;
        }
        if self.has_header {
            struct_ser.serialize_field("hasHeader", &self.has_header)?;
        }
        if !self.delimiter.is_empty() {
            struct_ser.serialize_field("delimiter", &self.delimiter)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CsvScanExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "base_conf",
            "baseConf",
            "has_header",
            "hasHeader",
            "delimiter",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            BaseConf,
            HasHeader,
            Delimiter,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "baseConf" | "base_conf" => Ok(GeneratedField::BaseConf),
                            "hasHeader" | "has_header" => Ok(GeneratedField::HasHeader),
                            "delimiter" => Ok(GeneratedField::Delimiter),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CsvScanExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CsvScanExecNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CsvScanExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut base_conf__ = None;
                let mut has_header__ = None;
                let mut delimiter__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::BaseConf => {
                            if base_conf__.is_some() {
                                return Err(serde::de::Error::duplicate_field("baseConf"));
                            }
                            base_conf__ = map.next_value()?;
                        }
                        GeneratedField::HasHeader => {
                            if has_header__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hasHeader"));
                            }
                            has_header__ = Some(map.next_value()?);
                        }
                        GeneratedField::Delimiter => {
                            if delimiter__.is_some() {
                                return Err(serde::de::Error::duplicate_field("delimiter"));
                            }
                            delimiter__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(CsvScanExecNode {
                    base_conf: base_conf__,
                    has_header: has_header__.unwrap_or_default(),
                    delimiter: delimiter__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CsvScanExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CubeNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.expr.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CubeNode", len)?;
        if !self.expr.is_empty() {
            struct_ser.serialize_field("expr", &self.expr)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CubeNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CubeNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CubeNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CubeNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(CubeNode {
                    expr: expr__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CubeNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CustomTableScanNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.table_name.is_empty() {
            len += 1;
        }
        if self.projection.is_some() {
            len += 1;
        }
        if self.schema.is_some() {
            len += 1;
        }
        if !self.filters.is_empty() {
            len += 1;
        }
        if !self.custom_table_data.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CustomTableScanNode", len)?;
        if !self.table_name.is_empty() {
            struct_ser.serialize_field("tableName", &self.table_name)?;
        }
        if let Some(v) = self.projection.as_ref() {
            struct_ser.serialize_field("projection", v)?;
        }
        if let Some(v) = self.schema.as_ref() {
            struct_ser.serialize_field("schema", v)?;
        }
        if !self.filters.is_empty() {
            struct_ser.serialize_field("filters", &self.filters)?;
        }
        if !self.custom_table_data.is_empty() {
            struct_ser.serialize_field("customTableData", pbjson::private::base64::encode(&self.custom_table_data).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CustomTableScanNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "table_name",
            "tableName",
            "projection",
            "schema",
            "filters",
            "custom_table_data",
            "customTableData",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableName,
            Projection,
            Schema,
            Filters,
            CustomTableData,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "tableName" | "table_name" => Ok(GeneratedField::TableName),
                            "projection" => Ok(GeneratedField::Projection),
                            "schema" => Ok(GeneratedField::Schema),
                            "filters" => Ok(GeneratedField::Filters),
                            "customTableData" | "custom_table_data" => Ok(GeneratedField::CustomTableData),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CustomTableScanNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CustomTableScanNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CustomTableScanNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_name__ = None;
                let mut projection__ = None;
                let mut schema__ = None;
                let mut filters__ = None;
                let mut custom_table_data__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TableName => {
                            if table_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableName"));
                            }
                            table_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::Projection => {
                            if projection__.is_some() {
                                return Err(serde::de::Error::duplicate_field("projection"));
                            }
                            projection__ = map.next_value()?;
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = map.next_value()?;
                        }
                        GeneratedField::Filters => {
                            if filters__.is_some() {
                                return Err(serde::de::Error::duplicate_field("filters"));
                            }
                            filters__ = Some(map.next_value()?);
                        }
                        GeneratedField::CustomTableData => {
                            if custom_table_data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("customTableData"));
                            }
                            custom_table_data__ = 
                                Some(map.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(CustomTableScanNode {
                    table_name: table_name__.unwrap_or_default(),
                    projection: projection__,
                    schema: schema__,
                    filters: filters__.unwrap_or_default(),
                    custom_table_data: custom_table_data__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CustomTableScanNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DateUnit {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Day => "Day",
            Self::DateMillisecond => "DateMillisecond",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for DateUnit {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "Day",
            "DateMillisecond",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DateUnit;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(DateUnit::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(DateUnit::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "Day" => Ok(DateUnit::Day),
                    "DateMillisecond" => Ok(DateUnit::DateMillisecond),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for Decimal {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.precision != 0 {
            len += 1;
        }
        if self.scale != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.Decimal", len)?;
        if self.precision != 0 {
            struct_ser.serialize_field("precision", &self.precision)?;
        }
        if self.scale != 0 {
            struct_ser.serialize_field("scale", &self.scale)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Decimal {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "precision",
            "scale",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Precision,
            Scale,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "precision" => Ok(GeneratedField::Precision),
                            "scale" => Ok(GeneratedField::Scale),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Decimal;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.Decimal")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Decimal, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut precision__ = None;
                let mut scale__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Precision => {
                            if precision__.is_some() {
                                return Err(serde::de::Error::duplicate_field("precision"));
                            }
                            precision__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Scale => {
                            if scale__.is_some() {
                                return Err(serde::de::Error::duplicate_field("scale"));
                            }
                            scale__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(Decimal {
                    precision: precision__.unwrap_or_default(),
                    scale: scale__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.Decimal", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Decimal128 {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.value.is_empty() {
            len += 1;
        }
        if self.p != 0 {
            len += 1;
        }
        if self.s != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.Decimal128", len)?;
        if !self.value.is_empty() {
            struct_ser.serialize_field("value", pbjson::private::base64::encode(&self.value).as_str())?;
        }
        if self.p != 0 {
            struct_ser.serialize_field("p", ToString::to_string(&self.p).as_str())?;
        }
        if self.s != 0 {
            struct_ser.serialize_field("s", ToString::to_string(&self.s).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Decimal128 {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "value",
            "p",
            "s",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Value,
            P,
            S,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "value" => Ok(GeneratedField::Value),
                            "p" => Ok(GeneratedField::P),
                            "s" => Ok(GeneratedField::S),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Decimal128;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.Decimal128")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Decimal128, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut value__ = None;
                let mut p__ = None;
                let mut s__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("value"));
                            }
                            value__ = 
                                Some(map.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::P => {
                            if p__.is_some() {
                                return Err(serde::de::Error::duplicate_field("p"));
                            }
                            p__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::S => {
                            if s__.is_some() {
                                return Err(serde::de::Error::duplicate_field("s"));
                            }
                            s__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(Decimal128 {
                    value: value__.unwrap_or_default(),
                    p: p__.unwrap_or_default(),
                    s: s__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.Decimal128", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DfField {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.field.is_some() {
            len += 1;
        }
        if self.qualifier.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.DfField", len)?;
        if let Some(v) = self.field.as_ref() {
            struct_ser.serialize_field("field", v)?;
        }
        if let Some(v) = self.qualifier.as_ref() {
            struct_ser.serialize_field("qualifier", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DfField {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "field",
            "qualifier",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Field,
            Qualifier,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "field" => Ok(GeneratedField::Field),
                            "qualifier" => Ok(GeneratedField::Qualifier),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DfField;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.DfField")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DfField, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut field__ = None;
                let mut qualifier__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Field => {
                            if field__.is_some() {
                                return Err(serde::de::Error::duplicate_field("field"));
                            }
                            field__ = map.next_value()?;
                        }
                        GeneratedField::Qualifier => {
                            if qualifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("qualifier"));
                            }
                            qualifier__ = map.next_value()?;
                        }
                    }
                }
                Ok(DfField {
                    field: field__,
                    qualifier: qualifier__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.DfField", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DfSchema {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.columns.is_empty() {
            len += 1;
        }
        if !self.metadata.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.DfSchema", len)?;
        if !self.columns.is_empty() {
            struct_ser.serialize_field("columns", &self.columns)?;
        }
        if !self.metadata.is_empty() {
            struct_ser.serialize_field("metadata", &self.metadata)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DfSchema {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "columns",
            "metadata",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Columns,
            Metadata,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "columns" => Ok(GeneratedField::Columns),
                            "metadata" => Ok(GeneratedField::Metadata),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DfSchema;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.DfSchema")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DfSchema, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut columns__ = None;
                let mut metadata__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Columns => {
                            if columns__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columns"));
                            }
                            columns__ = Some(map.next_value()?);
                        }
                        GeneratedField::Metadata => {
                            if metadata__.is_some() {
                                return Err(serde::de::Error::duplicate_field("metadata"));
                            }
                            metadata__ = Some(
                                map.next_value::<std::collections::HashMap<_, _>>()?
                            );
                        }
                    }
                }
                Ok(DfSchema {
                    columns: columns__.unwrap_or_default(),
                    metadata: metadata__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.DfSchema", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Dictionary {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.key.is_some() {
            len += 1;
        }
        if self.value.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.Dictionary", len)?;
        if let Some(v) = self.key.as_ref() {
            struct_ser.serialize_field("key", v)?;
        }
        if let Some(v) = self.value.as_ref() {
            struct_ser.serialize_field("value", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Dictionary {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "key",
            "value",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Key,
            Value,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "key" => Ok(GeneratedField::Key),
                            "value" => Ok(GeneratedField::Value),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Dictionary;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.Dictionary")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Dictionary, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut key__ = None;
                let mut value__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Key => {
                            if key__.is_some() {
                                return Err(serde::de::Error::duplicate_field("key"));
                            }
                            key__ = map.next_value()?;
                        }
                        GeneratedField::Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("value"));
                            }
                            value__ = map.next_value()?;
                        }
                    }
                }
                Ok(Dictionary {
                    key: key__,
                    value: value__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.Dictionary", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DistinctNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.DistinctNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DistinctNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DistinctNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.DistinctNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DistinctNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map.next_value()?;
                        }
                    }
                }
                Ok(DistinctNode {
                    input: input__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.DistinctNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for EmptyExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.produce_one_row {
            len += 1;
        }
        if self.schema.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.EmptyExecNode", len)?;
        if self.produce_one_row {
            struct_ser.serialize_field("produceOneRow", &self.produce_one_row)?;
        }
        if let Some(v) = self.schema.as_ref() {
            struct_ser.serialize_field("schema", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for EmptyExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "produce_one_row",
            "produceOneRow",
            "schema",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ProduceOneRow,
            Schema,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "produceOneRow" | "produce_one_row" => Ok(GeneratedField::ProduceOneRow),
                            "schema" => Ok(GeneratedField::Schema),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = EmptyExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.EmptyExecNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<EmptyExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut produce_one_row__ = None;
                let mut schema__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ProduceOneRow => {
                            if produce_one_row__.is_some() {
                                return Err(serde::de::Error::duplicate_field("produceOneRow"));
                            }
                            produce_one_row__ = Some(map.next_value()?);
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = map.next_value()?;
                        }
                    }
                }
                Ok(EmptyExecNode {
                    produce_one_row: produce_one_row__.unwrap_or_default(),
                    schema: schema__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.EmptyExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for EmptyMessage {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("datafusion.EmptyMessage", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for EmptyMessage {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                            Err(serde::de::Error::unknown_field(value, FIELDS))
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = EmptyMessage;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.EmptyMessage")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<EmptyMessage, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map.next_key::<GeneratedField>()?.is_some() {
                    let _ = map.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(EmptyMessage {
                })
            }
        }
        deserializer.deserialize_struct("datafusion.EmptyMessage", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for EmptyRelationNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.produce_one_row {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.EmptyRelationNode", len)?;
        if self.produce_one_row {
            struct_ser.serialize_field("produceOneRow", &self.produce_one_row)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for EmptyRelationNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "produce_one_row",
            "produceOneRow",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ProduceOneRow,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "produceOneRow" | "produce_one_row" => Ok(GeneratedField::ProduceOneRow),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = EmptyRelationNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.EmptyRelationNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<EmptyRelationNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut produce_one_row__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ProduceOneRow => {
                            if produce_one_row__.is_some() {
                                return Err(serde::de::Error::duplicate_field("produceOneRow"));
                            }
                            produce_one_row__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(EmptyRelationNode {
                    produce_one_row: produce_one_row__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.EmptyRelationNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ExecutorHeartbeat {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.executor_id.is_empty() {
            len += 1;
        }
        if self.timestamp != 0 {
            len += 1;
        }
        if !self.metrics.is_empty() {
            len += 1;
        }
        if self.status.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ExecutorHeartbeat", len)?;
        if !self.executor_id.is_empty() {
            struct_ser.serialize_field("executorId", &self.executor_id)?;
        }
        if self.timestamp != 0 {
            struct_ser.serialize_field("timestamp", ToString::to_string(&self.timestamp).as_str())?;
        }
        if !self.metrics.is_empty() {
            struct_ser.serialize_field("metrics", &self.metrics)?;
        }
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ExecutorHeartbeat {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "executor_id",
            "executorId",
            "timestamp",
            "metrics",
            "status",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ExecutorId,
            Timestamp,
            Metrics,
            Status,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "executorId" | "executor_id" => Ok(GeneratedField::ExecutorId),
                            "timestamp" => Ok(GeneratedField::Timestamp),
                            "metrics" => Ok(GeneratedField::Metrics),
                            "status" => Ok(GeneratedField::Status),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ExecutorHeartbeat;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ExecutorHeartbeat")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ExecutorHeartbeat, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut executor_id__ = None;
                let mut timestamp__ = None;
                let mut metrics__ = None;
                let mut status__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ExecutorId => {
                            if executor_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("executorId"));
                            }
                            executor_id__ = Some(map.next_value()?);
                        }
                        GeneratedField::Timestamp => {
                            if timestamp__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timestamp"));
                            }
                            timestamp__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Metrics => {
                            if metrics__.is_some() {
                                return Err(serde::de::Error::duplicate_field("metrics"));
                            }
                            metrics__ = Some(map.next_value()?);
                        }
                        GeneratedField::Status => {
                            if status__.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status__ = map.next_value()?;
                        }
                    }
                }
                Ok(ExecutorHeartbeat {
                    executor_id: executor_id__.unwrap_or_default(),
                    timestamp: timestamp__.unwrap_or_default(),
                    metrics: metrics__.unwrap_or_default(),
                    status: status__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ExecutorHeartbeat", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ExecutorMetadata {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.id.is_empty() {
            len += 1;
        }
        if !self.host.is_empty() {
            len += 1;
        }
        if self.port != 0 {
            len += 1;
        }
        if self.grpc_port != 0 {
            len += 1;
        }
        if self.specification.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ExecutorMetadata", len)?;
        if !self.id.is_empty() {
            struct_ser.serialize_field("id", &self.id)?;
        }
        if !self.host.is_empty() {
            struct_ser.serialize_field("host", &self.host)?;
        }
        if self.port != 0 {
            struct_ser.serialize_field("port", &self.port)?;
        }
        if self.grpc_port != 0 {
            struct_ser.serialize_field("grpcPort", &self.grpc_port)?;
        }
        if let Some(v) = self.specification.as_ref() {
            struct_ser.serialize_field("specification", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ExecutorMetadata {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "id",
            "host",
            "port",
            "grpc_port",
            "grpcPort",
            "specification",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Id,
            Host,
            Port,
            GrpcPort,
            Specification,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "id" => Ok(GeneratedField::Id),
                            "host" => Ok(GeneratedField::Host),
                            "port" => Ok(GeneratedField::Port),
                            "grpcPort" | "grpc_port" => Ok(GeneratedField::GrpcPort),
                            "specification" => Ok(GeneratedField::Specification),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ExecutorMetadata;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ExecutorMetadata")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ExecutorMetadata, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut id__ = None;
                let mut host__ = None;
                let mut port__ = None;
                let mut grpc_port__ = None;
                let mut specification__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Id => {
                            if id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id__ = Some(map.next_value()?);
                        }
                        GeneratedField::Host => {
                            if host__.is_some() {
                                return Err(serde::de::Error::duplicate_field("host"));
                            }
                            host__ = Some(map.next_value()?);
                        }
                        GeneratedField::Port => {
                            if port__.is_some() {
                                return Err(serde::de::Error::duplicate_field("port"));
                            }
                            port__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::GrpcPort => {
                            if grpc_port__.is_some() {
                                return Err(serde::de::Error::duplicate_field("grpcPort"));
                            }
                            grpc_port__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Specification => {
                            if specification__.is_some() {
                                return Err(serde::de::Error::duplicate_field("specification"));
                            }
                            specification__ = map.next_value()?;
                        }
                    }
                }
                Ok(ExecutorMetadata {
                    id: id__.unwrap_or_default(),
                    host: host__.unwrap_or_default(),
                    port: port__.unwrap_or_default(),
                    grpc_port: grpc_port__.unwrap_or_default(),
                    specification: specification__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ExecutorMetadata", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ExecutorMetric {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.metric.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ExecutorMetric", len)?;
        if let Some(v) = self.metric.as_ref() {
            match v {
                executor_metric::Metric::AvailableMemory(v) => {
                    struct_ser.serialize_field("availableMemory", ToString::to_string(&v).as_str())?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ExecutorMetric {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "available_memory",
            "availableMemory",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            AvailableMemory,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "availableMemory" | "available_memory" => Ok(GeneratedField::AvailableMemory),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ExecutorMetric;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ExecutorMetric")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ExecutorMetric, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut metric__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::AvailableMemory => {
                            if metric__.is_some() {
                                return Err(serde::de::Error::duplicate_field("availableMemory"));
                            }
                            metric__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| executor_metric::Metric::AvailableMemory(x.0));
                        }
                    }
                }
                Ok(ExecutorMetric {
                    metric: metric__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ExecutorMetric", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ExecutorRegistration {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.id.is_empty() {
            len += 1;
        }
        if self.port != 0 {
            len += 1;
        }
        if self.grpc_port != 0 {
            len += 1;
        }
        if self.specification.is_some() {
            len += 1;
        }
        if self.optional_host.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ExecutorRegistration", len)?;
        if !self.id.is_empty() {
            struct_ser.serialize_field("id", &self.id)?;
        }
        if self.port != 0 {
            struct_ser.serialize_field("port", &self.port)?;
        }
        if self.grpc_port != 0 {
            struct_ser.serialize_field("grpcPort", &self.grpc_port)?;
        }
        if let Some(v) = self.specification.as_ref() {
            struct_ser.serialize_field("specification", v)?;
        }
        if let Some(v) = self.optional_host.as_ref() {
            match v {
                executor_registration::OptionalHost::Host(v) => {
                    struct_ser.serialize_field("host", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ExecutorRegistration {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "id",
            "port",
            "grpc_port",
            "grpcPort",
            "specification",
            "host",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Id,
            Port,
            GrpcPort,
            Specification,
            Host,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "id" => Ok(GeneratedField::Id),
                            "port" => Ok(GeneratedField::Port),
                            "grpcPort" | "grpc_port" => Ok(GeneratedField::GrpcPort),
                            "specification" => Ok(GeneratedField::Specification),
                            "host" => Ok(GeneratedField::Host),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ExecutorRegistration;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ExecutorRegistration")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ExecutorRegistration, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut id__ = None;
                let mut port__ = None;
                let mut grpc_port__ = None;
                let mut specification__ = None;
                let mut optional_host__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Id => {
                            if id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id__ = Some(map.next_value()?);
                        }
                        GeneratedField::Port => {
                            if port__.is_some() {
                                return Err(serde::de::Error::duplicate_field("port"));
                            }
                            port__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::GrpcPort => {
                            if grpc_port__.is_some() {
                                return Err(serde::de::Error::duplicate_field("grpcPort"));
                            }
                            grpc_port__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Specification => {
                            if specification__.is_some() {
                                return Err(serde::de::Error::duplicate_field("specification"));
                            }
                            specification__ = map.next_value()?;
                        }
                        GeneratedField::Host => {
                            if optional_host__.is_some() {
                                return Err(serde::de::Error::duplicate_field("host"));
                            }
                            optional_host__ = map.next_value::<::std::option::Option<_>>()?.map(executor_registration::OptionalHost::Host);
                        }
                    }
                }
                Ok(ExecutorRegistration {
                    id: id__.unwrap_or_default(),
                    port: port__.unwrap_or_default(),
                    grpc_port: grpc_port__.unwrap_or_default(),
                    specification: specification__,
                    optional_host: optional_host__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ExecutorRegistration", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ExecutorResource {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.resource.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ExecutorResource", len)?;
        if let Some(v) = self.resource.as_ref() {
            match v {
                executor_resource::Resource::TaskSlots(v) => {
                    struct_ser.serialize_field("taskSlots", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ExecutorResource {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "task_slots",
            "taskSlots",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TaskSlots,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "taskSlots" | "task_slots" => Ok(GeneratedField::TaskSlots),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ExecutorResource;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ExecutorResource")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ExecutorResource, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut resource__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TaskSlots => {
                            if resource__.is_some() {
                                return Err(serde::de::Error::duplicate_field("taskSlots"));
                            }
                            resource__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| executor_resource::Resource::TaskSlots(x.0));
                        }
                    }
                }
                Ok(ExecutorResource {
                    resource: resource__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ExecutorResource", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ExecutorSpecification {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.resources.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ExecutorSpecification", len)?;
        if !self.resources.is_empty() {
            struct_ser.serialize_field("resources", &self.resources)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ExecutorSpecification {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "resources",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Resources,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "resources" => Ok(GeneratedField::Resources),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ExecutorSpecification;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ExecutorSpecification")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ExecutorSpecification, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut resources__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Resources => {
                            if resources__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resources"));
                            }
                            resources__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ExecutorSpecification {
                    resources: resources__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ExecutorSpecification", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ExecutorStatus {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.status.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ExecutorStatus", len)?;
        if let Some(v) = self.status.as_ref() {
            match v {
                executor_status::Status::Active(v) => {
                    struct_ser.serialize_field("active", v)?;
                }
                executor_status::Status::Dead(v) => {
                    struct_ser.serialize_field("dead", v)?;
                }
                executor_status::Status::Unknown(v) => {
                    struct_ser.serialize_field("unknown", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ExecutorStatus {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "active",
            "dead",
            "unknown",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Active,
            Dead,
            Unknown,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "active" => Ok(GeneratedField::Active),
                            "dead" => Ok(GeneratedField::Dead),
                            "unknown" => Ok(GeneratedField::Unknown),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ExecutorStatus;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ExecutorStatus")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ExecutorStatus, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Active => {
                            if status__.is_some() {
                                return Err(serde::de::Error::duplicate_field("active"));
                            }
                            status__ = map.next_value::<::std::option::Option<_>>()?.map(executor_status::Status::Active);
                        }
                        GeneratedField::Dead => {
                            if status__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dead"));
                            }
                            status__ = map.next_value::<::std::option::Option<_>>()?.map(executor_status::Status::Dead);
                        }
                        GeneratedField::Unknown => {
                            if status__.is_some() {
                                return Err(serde::de::Error::duplicate_field("unknown"));
                            }
                            status__ = map.next_value::<::std::option::Option<_>>()?.map(executor_status::Status::Unknown);
                        }
                    }
                }
                Ok(ExecutorStatus {
                    status: status__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ExecutorStatus", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ExplainExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.schema.is_some() {
            len += 1;
        }
        if !self.stringified_plans.is_empty() {
            len += 1;
        }
        if self.verbose {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ExplainExecNode", len)?;
        if let Some(v) = self.schema.as_ref() {
            struct_ser.serialize_field("schema", v)?;
        }
        if !self.stringified_plans.is_empty() {
            struct_ser.serialize_field("stringifiedPlans", &self.stringified_plans)?;
        }
        if self.verbose {
            struct_ser.serialize_field("verbose", &self.verbose)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ExplainExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "schema",
            "stringified_plans",
            "stringifiedPlans",
            "verbose",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Schema,
            StringifiedPlans,
            Verbose,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "schema" => Ok(GeneratedField::Schema),
                            "stringifiedPlans" | "stringified_plans" => Ok(GeneratedField::StringifiedPlans),
                            "verbose" => Ok(GeneratedField::Verbose),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ExplainExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ExplainExecNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ExplainExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut schema__ = None;
                let mut stringified_plans__ = None;
                let mut verbose__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = map.next_value()?;
                        }
                        GeneratedField::StringifiedPlans => {
                            if stringified_plans__.is_some() {
                                return Err(serde::de::Error::duplicate_field("stringifiedPlans"));
                            }
                            stringified_plans__ = Some(map.next_value()?);
                        }
                        GeneratedField::Verbose => {
                            if verbose__.is_some() {
                                return Err(serde::de::Error::duplicate_field("verbose"));
                            }
                            verbose__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ExplainExecNode {
                    schema: schema__,
                    stringified_plans: stringified_plans__.unwrap_or_default(),
                    verbose: verbose__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ExplainExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ExplainNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if self.verbose {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ExplainNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if self.verbose {
            struct_ser.serialize_field("verbose", &self.verbose)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ExplainNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "verbose",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Verbose,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "verbose" => Ok(GeneratedField::Verbose),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ExplainNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ExplainNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ExplainNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut verbose__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map.next_value()?;
                        }
                        GeneratedField::Verbose => {
                            if verbose__.is_some() {
                                return Err(serde::de::Error::duplicate_field("verbose"));
                            }
                            verbose__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ExplainNode {
                    input: input__,
                    verbose: verbose__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ExplainNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Field {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.name.is_empty() {
            len += 1;
        }
        if self.arrow_type.is_some() {
            len += 1;
        }
        if self.nullable {
            len += 1;
        }
        if !self.children.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.Field", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if let Some(v) = self.arrow_type.as_ref() {
            struct_ser.serialize_field("arrowType", v)?;
        }
        if self.nullable {
            struct_ser.serialize_field("nullable", &self.nullable)?;
        }
        if !self.children.is_empty() {
            struct_ser.serialize_field("children", &self.children)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Field {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "arrow_type",
            "arrowType",
            "nullable",
            "children",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            ArrowType,
            Nullable,
            Children,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "arrowType" | "arrow_type" => Ok(GeneratedField::ArrowType),
                            "nullable" => Ok(GeneratedField::Nullable),
                            "children" => Ok(GeneratedField::Children),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Field;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.Field")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Field, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut arrow_type__ = None;
                let mut nullable__ = None;
                let mut children__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map.next_value()?);
                        }
                        GeneratedField::ArrowType => {
                            if arrow_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("arrowType"));
                            }
                            arrow_type__ = map.next_value()?;
                        }
                        GeneratedField::Nullable => {
                            if nullable__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nullable"));
                            }
                            nullable__ = Some(map.next_value()?);
                        }
                        GeneratedField::Children => {
                            if children__.is_some() {
                                return Err(serde::de::Error::duplicate_field("children"));
                            }
                            children__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(Field {
                    name: name__.unwrap_or_default(),
                    arrow_type: arrow_type__,
                    nullable: nullable__.unwrap_or_default(),
                    children: children__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.Field", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FileGroup {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.files.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.FileGroup", len)?;
        if !self.files.is_empty() {
            struct_ser.serialize_field("files", &self.files)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FileGroup {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "files",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Files,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "files" => Ok(GeneratedField::Files),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FileGroup;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.FileGroup")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<FileGroup, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut files__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Files => {
                            if files__.is_some() {
                                return Err(serde::de::Error::duplicate_field("files"));
                            }
                            files__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(FileGroup {
                    files: files__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.FileGroup", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FileRange {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.start != 0 {
            len += 1;
        }
        if self.end != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.FileRange", len)?;
        if self.start != 0 {
            struct_ser.serialize_field("start", ToString::to_string(&self.start).as_str())?;
        }
        if self.end != 0 {
            struct_ser.serialize_field("end", ToString::to_string(&self.end).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FileRange {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "start",
            "end",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Start,
            End,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "start" => Ok(GeneratedField::Start),
                            "end" => Ok(GeneratedField::End),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FileRange;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.FileRange")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<FileRange, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut start__ = None;
                let mut end__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Start => {
                            if start__.is_some() {
                                return Err(serde::de::Error::duplicate_field("start"));
                            }
                            start__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::End => {
                            if end__.is_some() {
                                return Err(serde::de::Error::duplicate_field("end"));
                            }
                            end__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(FileRange {
                    start: start__.unwrap_or_default(),
                    end: end__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.FileRange", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FileScanExecConf {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.file_groups.is_empty() {
            len += 1;
        }
        if self.schema.is_some() {
            len += 1;
        }
        if !self.projection.is_empty() {
            len += 1;
        }
        if self.limit.is_some() {
            len += 1;
        }
        if self.statistics.is_some() {
            len += 1;
        }
        if !self.table_partition_cols.is_empty() {
            len += 1;
        }
        if !self.object_store_url.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.FileScanExecConf", len)?;
        if !self.file_groups.is_empty() {
            struct_ser.serialize_field("fileGroups", &self.file_groups)?;
        }
        if let Some(v) = self.schema.as_ref() {
            struct_ser.serialize_field("schema", v)?;
        }
        if !self.projection.is_empty() {
            struct_ser.serialize_field("projection", &self.projection)?;
        }
        if let Some(v) = self.limit.as_ref() {
            struct_ser.serialize_field("limit", v)?;
        }
        if let Some(v) = self.statistics.as_ref() {
            struct_ser.serialize_field("statistics", v)?;
        }
        if !self.table_partition_cols.is_empty() {
            struct_ser.serialize_field("tablePartitionCols", &self.table_partition_cols)?;
        }
        if !self.object_store_url.is_empty() {
            struct_ser.serialize_field("objectStoreUrl", &self.object_store_url)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FileScanExecConf {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "file_groups",
            "fileGroups",
            "schema",
            "projection",
            "limit",
            "statistics",
            "table_partition_cols",
            "tablePartitionCols",
            "object_store_url",
            "objectStoreUrl",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            FileGroups,
            Schema,
            Projection,
            Limit,
            Statistics,
            TablePartitionCols,
            ObjectStoreUrl,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "fileGroups" | "file_groups" => Ok(GeneratedField::FileGroups),
                            "schema" => Ok(GeneratedField::Schema),
                            "projection" => Ok(GeneratedField::Projection),
                            "limit" => Ok(GeneratedField::Limit),
                            "statistics" => Ok(GeneratedField::Statistics),
                            "tablePartitionCols" | "table_partition_cols" => Ok(GeneratedField::TablePartitionCols),
                            "objectStoreUrl" | "object_store_url" => Ok(GeneratedField::ObjectStoreUrl),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FileScanExecConf;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.FileScanExecConf")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<FileScanExecConf, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut file_groups__ = None;
                let mut schema__ = None;
                let mut projection__ = None;
                let mut limit__ = None;
                let mut statistics__ = None;
                let mut table_partition_cols__ = None;
                let mut object_store_url__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::FileGroups => {
                            if file_groups__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fileGroups"));
                            }
                            file_groups__ = Some(map.next_value()?);
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = map.next_value()?;
                        }
                        GeneratedField::Projection => {
                            if projection__.is_some() {
                                return Err(serde::de::Error::duplicate_field("projection"));
                            }
                            projection__ = 
                                Some(map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                        GeneratedField::Limit => {
                            if limit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("limit"));
                            }
                            limit__ = map.next_value()?;
                        }
                        GeneratedField::Statistics => {
                            if statistics__.is_some() {
                                return Err(serde::de::Error::duplicate_field("statistics"));
                            }
                            statistics__ = map.next_value()?;
                        }
                        GeneratedField::TablePartitionCols => {
                            if table_partition_cols__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tablePartitionCols"));
                            }
                            table_partition_cols__ = Some(map.next_value()?);
                        }
                        GeneratedField::ObjectStoreUrl => {
                            if object_store_url__.is_some() {
                                return Err(serde::de::Error::duplicate_field("objectStoreUrl"));
                            }
                            object_store_url__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(FileScanExecConf {
                    file_groups: file_groups__.unwrap_or_default(),
                    schema: schema__,
                    projection: projection__.unwrap_or_default(),
                    limit: limit__,
                    statistics: statistics__,
                    table_partition_cols: table_partition_cols__.unwrap_or_default(),
                    object_store_url: object_store_url__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.FileScanExecConf", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FilterExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.FilterExecNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FilterExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FilterExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.FilterExecNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<FilterExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map.next_value()?;
                        }
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                    }
                }
                Ok(FilterExecNode {
                    input: input__,
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.FilterExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FixedSizeBinary {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.length != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.FixedSizeBinary", len)?;
        if self.length != 0 {
            struct_ser.serialize_field("length", &self.length)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FixedSizeBinary {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "length",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Length,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "length" => Ok(GeneratedField::Length),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FixedSizeBinary;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.FixedSizeBinary")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<FixedSizeBinary, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut length__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Length => {
                            if length__.is_some() {
                                return Err(serde::de::Error::duplicate_field("length"));
                            }
                            length__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(FixedSizeBinary {
                    length: length__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.FixedSizeBinary", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FixedSizeList {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.field_type.is_some() {
            len += 1;
        }
        if self.list_size != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.FixedSizeList", len)?;
        if let Some(v) = self.field_type.as_ref() {
            struct_ser.serialize_field("fieldType", v)?;
        }
        if self.list_size != 0 {
            struct_ser.serialize_field("listSize", &self.list_size)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FixedSizeList {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "field_type",
            "fieldType",
            "list_size",
            "listSize",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            FieldType,
            ListSize,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "fieldType" | "field_type" => Ok(GeneratedField::FieldType),
                            "listSize" | "list_size" => Ok(GeneratedField::ListSize),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FixedSizeList;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.FixedSizeList")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<FixedSizeList, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut field_type__ = None;
                let mut list_size__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::FieldType => {
                            if field_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fieldType"));
                            }
                            field_type__ = map.next_value()?;
                        }
                        GeneratedField::ListSize => {
                            if list_size__.is_some() {
                                return Err(serde::de::Error::duplicate_field("listSize"));
                            }
                            list_size__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(FixedSizeList {
                    field_type: field_type__,
                    list_size: list_size__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.FixedSizeList", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FullTableReference {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.catalog.is_empty() {
            len += 1;
        }
        if !self.schema.is_empty() {
            len += 1;
        }
        if !self.table.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.FullTableReference", len)?;
        if !self.catalog.is_empty() {
            struct_ser.serialize_field("catalog", &self.catalog)?;
        }
        if !self.schema.is_empty() {
            struct_ser.serialize_field("schema", &self.schema)?;
        }
        if !self.table.is_empty() {
            struct_ser.serialize_field("table", &self.table)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FullTableReference {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "catalog",
            "schema",
            "table",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Catalog,
            Schema,
            Table,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "catalog" => Ok(GeneratedField::Catalog),
                            "schema" => Ok(GeneratedField::Schema),
                            "table" => Ok(GeneratedField::Table),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FullTableReference;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.FullTableReference")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<FullTableReference, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut catalog__ = None;
                let mut schema__ = None;
                let mut table__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Catalog => {
                            if catalog__.is_some() {
                                return Err(serde::de::Error::duplicate_field("catalog"));
                            }
                            catalog__ = Some(map.next_value()?);
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = Some(map.next_value()?);
                        }
                        GeneratedField::Table => {
                            if table__.is_some() {
                                return Err(serde::de::Error::duplicate_field("table"));
                            }
                            table__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(FullTableReference {
                    catalog: catalog__.unwrap_or_default(),
                    schema: schema__.unwrap_or_default(),
                    table: table__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.FullTableReference", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GetIndexedField {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if self.key.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.GetIndexedField", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if let Some(v) = self.key.as_ref() {
            struct_ser.serialize_field("key", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GetIndexedField {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "key",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            Key,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "key" => Ok(GeneratedField::Key),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = GetIndexedField;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.GetIndexedField")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<GetIndexedField, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut key__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                        GeneratedField::Key => {
                            if key__.is_some() {
                                return Err(serde::de::Error::duplicate_field("key"));
                            }
                            key__ = map.next_value()?;
                        }
                    }
                }
                Ok(GetIndexedField {
                    expr: expr__,
                    key: key__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.GetIndexedField", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GlobalLimitExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if self.skip != 0 {
            len += 1;
        }
        if self.fetch != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.GlobalLimitExecNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if self.skip != 0 {
            struct_ser.serialize_field("skip", &self.skip)?;
        }
        if self.fetch != 0 {
            struct_ser.serialize_field("fetch", ToString::to_string(&self.fetch).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GlobalLimitExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "skip",
            "fetch",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Skip,
            Fetch,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "skip" => Ok(GeneratedField::Skip),
                            "fetch" => Ok(GeneratedField::Fetch),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = GlobalLimitExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.GlobalLimitExecNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<GlobalLimitExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut skip__ = None;
                let mut fetch__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map.next_value()?;
                        }
                        GeneratedField::Skip => {
                            if skip__.is_some() {
                                return Err(serde::de::Error::duplicate_field("skip"));
                            }
                            skip__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Fetch => {
                            if fetch__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fetch"));
                            }
                            fetch__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(GlobalLimitExecNode {
                    input: input__,
                    skip: skip__.unwrap_or_default(),
                    fetch: fetch__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.GlobalLimitExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GroupingSetNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.expr.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.GroupingSetNode", len)?;
        if !self.expr.is_empty() {
            struct_ser.serialize_field("expr", &self.expr)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GroupingSetNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = GroupingSetNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.GroupingSetNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<GroupingSetNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(GroupingSetNode {
                    expr: expr__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.GroupingSetNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for HashJoinExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.left.is_some() {
            len += 1;
        }
        if self.right.is_some() {
            len += 1;
        }
        if !self.on.is_empty() {
            len += 1;
        }
        if self.join_type != 0 {
            len += 1;
        }
        if self.partition_mode != 0 {
            len += 1;
        }
        if self.null_equals_null {
            len += 1;
        }
        if self.filter.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.HashJoinExecNode", len)?;
        if let Some(v) = self.left.as_ref() {
            struct_ser.serialize_field("left", v)?;
        }
        if let Some(v) = self.right.as_ref() {
            struct_ser.serialize_field("right", v)?;
        }
        if !self.on.is_empty() {
            struct_ser.serialize_field("on", &self.on)?;
        }
        if self.join_type != 0 {
            let v = JoinType::from_i32(self.join_type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.join_type)))?;
            struct_ser.serialize_field("joinType", &v)?;
        }
        if self.partition_mode != 0 {
            let v = PartitionMode::from_i32(self.partition_mode)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.partition_mode)))?;
            struct_ser.serialize_field("partitionMode", &v)?;
        }
        if self.null_equals_null {
            struct_ser.serialize_field("nullEqualsNull", &self.null_equals_null)?;
        }
        if let Some(v) = self.filter.as_ref() {
            struct_ser.serialize_field("filter", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for HashJoinExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "left",
            "right",
            "on",
            "join_type",
            "joinType",
            "partition_mode",
            "partitionMode",
            "null_equals_null",
            "nullEqualsNull",
            "filter",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Left,
            Right,
            On,
            JoinType,
            PartitionMode,
            NullEqualsNull,
            Filter,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "left" => Ok(GeneratedField::Left),
                            "right" => Ok(GeneratedField::Right),
                            "on" => Ok(GeneratedField::On),
                            "joinType" | "join_type" => Ok(GeneratedField::JoinType),
                            "partitionMode" | "partition_mode" => Ok(GeneratedField::PartitionMode),
                            "nullEqualsNull" | "null_equals_null" => Ok(GeneratedField::NullEqualsNull),
                            "filter" => Ok(GeneratedField::Filter),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = HashJoinExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.HashJoinExecNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<HashJoinExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut left__ = None;
                let mut right__ = None;
                let mut on__ = None;
                let mut join_type__ = None;
                let mut partition_mode__ = None;
                let mut null_equals_null__ = None;
                let mut filter__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Left => {
                            if left__.is_some() {
                                return Err(serde::de::Error::duplicate_field("left"));
                            }
                            left__ = map.next_value()?;
                        }
                        GeneratedField::Right => {
                            if right__.is_some() {
                                return Err(serde::de::Error::duplicate_field("right"));
                            }
                            right__ = map.next_value()?;
                        }
                        GeneratedField::On => {
                            if on__.is_some() {
                                return Err(serde::de::Error::duplicate_field("on"));
                            }
                            on__ = Some(map.next_value()?);
                        }
                        GeneratedField::JoinType => {
                            if join_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("joinType"));
                            }
                            join_type__ = Some(map.next_value::<JoinType>()? as i32);
                        }
                        GeneratedField::PartitionMode => {
                            if partition_mode__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionMode"));
                            }
                            partition_mode__ = Some(map.next_value::<PartitionMode>()? as i32);
                        }
                        GeneratedField::NullEqualsNull => {
                            if null_equals_null__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nullEqualsNull"));
                            }
                            null_equals_null__ = Some(map.next_value()?);
                        }
                        GeneratedField::Filter => {
                            if filter__.is_some() {
                                return Err(serde::de::Error::duplicate_field("filter"));
                            }
                            filter__ = map.next_value()?;
                        }
                    }
                }
                Ok(HashJoinExecNode {
                    left: left__,
                    right: right__,
                    on: on__.unwrap_or_default(),
                    join_type: join_type__.unwrap_or_default(),
                    partition_mode: partition_mode__.unwrap_or_default(),
                    null_equals_null: null_equals_null__.unwrap_or_default(),
                    filter: filter__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.HashJoinExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for HashRepartition {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.hash_expr.is_empty() {
            len += 1;
        }
        if self.partition_count != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.HashRepartition", len)?;
        if !self.hash_expr.is_empty() {
            struct_ser.serialize_field("hashExpr", &self.hash_expr)?;
        }
        if self.partition_count != 0 {
            struct_ser.serialize_field("partitionCount", ToString::to_string(&self.partition_count).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for HashRepartition {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "hash_expr",
            "hashExpr",
            "partition_count",
            "partitionCount",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            HashExpr,
            PartitionCount,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "hashExpr" | "hash_expr" => Ok(GeneratedField::HashExpr),
                            "partitionCount" | "partition_count" => Ok(GeneratedField::PartitionCount),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = HashRepartition;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.HashRepartition")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<HashRepartition, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut hash_expr__ = None;
                let mut partition_count__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::HashExpr => {
                            if hash_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hashExpr"));
                            }
                            hash_expr__ = Some(map.next_value()?);
                        }
                        GeneratedField::PartitionCount => {
                            if partition_count__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionCount"));
                            }
                            partition_count__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(HashRepartition {
                    hash_expr: hash_expr__.unwrap_or_default(),
                    partition_count: partition_count__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.HashRepartition", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ILikeNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.negated {
            len += 1;
        }
        if self.expr.is_some() {
            len += 1;
        }
        if self.pattern.is_some() {
            len += 1;
        }
        if !self.escape_char.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ILikeNode", len)?;
        if self.negated {
            struct_ser.serialize_field("negated", &self.negated)?;
        }
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if let Some(v) = self.pattern.as_ref() {
            struct_ser.serialize_field("pattern", v)?;
        }
        if !self.escape_char.is_empty() {
            struct_ser.serialize_field("escapeChar", &self.escape_char)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ILikeNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "negated",
            "expr",
            "pattern",
            "escape_char",
            "escapeChar",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Negated,
            Expr,
            Pattern,
            EscapeChar,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "negated" => Ok(GeneratedField::Negated),
                            "expr" => Ok(GeneratedField::Expr),
                            "pattern" => Ok(GeneratedField::Pattern),
                            "escapeChar" | "escape_char" => Ok(GeneratedField::EscapeChar),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ILikeNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ILikeNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ILikeNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut negated__ = None;
                let mut expr__ = None;
                let mut pattern__ = None;
                let mut escape_char__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Negated => {
                            if negated__.is_some() {
                                return Err(serde::de::Error::duplicate_field("negated"));
                            }
                            negated__ = Some(map.next_value()?);
                        }
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                        GeneratedField::Pattern => {
                            if pattern__.is_some() {
                                return Err(serde::de::Error::duplicate_field("pattern"));
                            }
                            pattern__ = map.next_value()?;
                        }
                        GeneratedField::EscapeChar => {
                            if escape_char__.is_some() {
                                return Err(serde::de::Error::duplicate_field("escapeChar"));
                            }
                            escape_char__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ILikeNode {
                    negated: negated__.unwrap_or_default(),
                    expr: expr__,
                    pattern: pattern__,
                    escape_char: escape_char__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ILikeNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for InListNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if !self.list.is_empty() {
            len += 1;
        }
        if self.negated {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.InListNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if !self.list.is_empty() {
            struct_ser.serialize_field("list", &self.list)?;
        }
        if self.negated {
            struct_ser.serialize_field("negated", &self.negated)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for InListNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "list",
            "negated",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            List,
            Negated,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "list" => Ok(GeneratedField::List),
                            "negated" => Ok(GeneratedField::Negated),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = InListNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.InListNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<InListNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut list__ = None;
                let mut negated__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                        GeneratedField::List => {
                            if list__.is_some() {
                                return Err(serde::de::Error::duplicate_field("list"));
                            }
                            list__ = Some(map.next_value()?);
                        }
                        GeneratedField::Negated => {
                            if negated__.is_some() {
                                return Err(serde::de::Error::duplicate_field("negated"));
                            }
                            negated__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(InListNode {
                    expr: expr__,
                    list: list__.unwrap_or_default(),
                    negated: negated__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.InListNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for IntervalMonthDayNanoValue {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.months != 0 {
            len += 1;
        }
        if self.days != 0 {
            len += 1;
        }
        if self.nanos != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.IntervalMonthDayNanoValue", len)?;
        if self.months != 0 {
            struct_ser.serialize_field("months", &self.months)?;
        }
        if self.days != 0 {
            struct_ser.serialize_field("days", &self.days)?;
        }
        if self.nanos != 0 {
            struct_ser.serialize_field("nanos", ToString::to_string(&self.nanos).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for IntervalMonthDayNanoValue {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "months",
            "days",
            "nanos",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Months,
            Days,
            Nanos,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "months" => Ok(GeneratedField::Months),
                            "days" => Ok(GeneratedField::Days),
                            "nanos" => Ok(GeneratedField::Nanos),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = IntervalMonthDayNanoValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.IntervalMonthDayNanoValue")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<IntervalMonthDayNanoValue, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut months__ = None;
                let mut days__ = None;
                let mut nanos__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Months => {
                            if months__.is_some() {
                                return Err(serde::de::Error::duplicate_field("months"));
                            }
                            months__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Days => {
                            if days__.is_some() {
                                return Err(serde::de::Error::duplicate_field("days"));
                            }
                            days__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Nanos => {
                            if nanos__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nanos"));
                            }
                            nanos__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(IntervalMonthDayNanoValue {
                    months: months__.unwrap_or_default(),
                    days: days__.unwrap_or_default(),
                    nanos: nanos__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.IntervalMonthDayNanoValue", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for IntervalUnit {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::YearMonth => "YearMonth",
            Self::DayTime => "DayTime",
            Self::MonthDayNano => "MonthDayNano",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for IntervalUnit {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "YearMonth",
            "DayTime",
            "MonthDayNano",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = IntervalUnit;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(IntervalUnit::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(IntervalUnit::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "YearMonth" => Ok(IntervalUnit::YearMonth),
                    "DayTime" => Ok(IntervalUnit::DayTime),
                    "MonthDayNano" => Ok(IntervalUnit::MonthDayNano),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for IsFalse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.IsFalse", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for IsFalse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = IsFalse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.IsFalse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<IsFalse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                    }
                }
                Ok(IsFalse {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.IsFalse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for IsNotFalse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.IsNotFalse", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for IsNotFalse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = IsNotFalse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.IsNotFalse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<IsNotFalse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                    }
                }
                Ok(IsNotFalse {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.IsNotFalse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for IsNotNull {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.IsNotNull", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for IsNotNull {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = IsNotNull;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.IsNotNull")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<IsNotNull, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                    }
                }
                Ok(IsNotNull {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.IsNotNull", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for IsNotTrue {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.IsNotTrue", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for IsNotTrue {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = IsNotTrue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.IsNotTrue")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<IsNotTrue, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                    }
                }
                Ok(IsNotTrue {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.IsNotTrue", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for IsNotUnknown {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.IsNotUnknown", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for IsNotUnknown {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = IsNotUnknown;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.IsNotUnknown")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<IsNotUnknown, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                    }
                }
                Ok(IsNotUnknown {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.IsNotUnknown", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for IsNull {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.IsNull", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for IsNull {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = IsNull;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.IsNull")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<IsNull, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                    }
                }
                Ok(IsNull {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.IsNull", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for IsTrue {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.IsTrue", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for IsTrue {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = IsTrue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.IsTrue")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<IsTrue, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                    }
                }
                Ok(IsTrue {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.IsTrue", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for IsUnknown {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.IsUnknown", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for IsUnknown {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = IsUnknown;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.IsUnknown")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<IsUnknown, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                    }
                }
                Ok(IsUnknown {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.IsUnknown", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for JoinConstraint {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::On => "ON",
            Self::Using => "USING",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for JoinConstraint {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "ON",
            "USING",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = JoinConstraint;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(JoinConstraint::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(JoinConstraint::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "ON" => Ok(JoinConstraint::On),
                    "USING" => Ok(JoinConstraint::Using),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for JoinFilter {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expression.is_some() {
            len += 1;
        }
        if !self.column_indices.is_empty() {
            len += 1;
        }
        if self.schema.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.JoinFilter", len)?;
        if let Some(v) = self.expression.as_ref() {
            struct_ser.serialize_field("expression", v)?;
        }
        if !self.column_indices.is_empty() {
            struct_ser.serialize_field("columnIndices", &self.column_indices)?;
        }
        if let Some(v) = self.schema.as_ref() {
            struct_ser.serialize_field("schema", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for JoinFilter {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expression",
            "column_indices",
            "columnIndices",
            "schema",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expression,
            ColumnIndices,
            Schema,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expression" => Ok(GeneratedField::Expression),
                            "columnIndices" | "column_indices" => Ok(GeneratedField::ColumnIndices),
                            "schema" => Ok(GeneratedField::Schema),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = JoinFilter;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.JoinFilter")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<JoinFilter, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expression__ = None;
                let mut column_indices__ = None;
                let mut schema__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expression => {
                            if expression__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expression"));
                            }
                            expression__ = map.next_value()?;
                        }
                        GeneratedField::ColumnIndices => {
                            if column_indices__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnIndices"));
                            }
                            column_indices__ = Some(map.next_value()?);
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = map.next_value()?;
                        }
                    }
                }
                Ok(JoinFilter {
                    expression: expression__,
                    column_indices: column_indices__.unwrap_or_default(),
                    schema: schema__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.JoinFilter", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for JoinNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.left.is_some() {
            len += 1;
        }
        if self.right.is_some() {
            len += 1;
        }
        if self.join_type != 0 {
            len += 1;
        }
        if self.join_constraint != 0 {
            len += 1;
        }
        if !self.left_join_column.is_empty() {
            len += 1;
        }
        if !self.right_join_column.is_empty() {
            len += 1;
        }
        if self.null_equals_null {
            len += 1;
        }
        if self.filter.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.JoinNode", len)?;
        if let Some(v) = self.left.as_ref() {
            struct_ser.serialize_field("left", v)?;
        }
        if let Some(v) = self.right.as_ref() {
            struct_ser.serialize_field("right", v)?;
        }
        if self.join_type != 0 {
            let v = JoinType::from_i32(self.join_type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.join_type)))?;
            struct_ser.serialize_field("joinType", &v)?;
        }
        if self.join_constraint != 0 {
            let v = JoinConstraint::from_i32(self.join_constraint)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.join_constraint)))?;
            struct_ser.serialize_field("joinConstraint", &v)?;
        }
        if !self.left_join_column.is_empty() {
            struct_ser.serialize_field("leftJoinColumn", &self.left_join_column)?;
        }
        if !self.right_join_column.is_empty() {
            struct_ser.serialize_field("rightJoinColumn", &self.right_join_column)?;
        }
        if self.null_equals_null {
            struct_ser.serialize_field("nullEqualsNull", &self.null_equals_null)?;
        }
        if let Some(v) = self.filter.as_ref() {
            struct_ser.serialize_field("filter", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for JoinNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "left",
            "right",
            "join_type",
            "joinType",
            "join_constraint",
            "joinConstraint",
            "left_join_column",
            "leftJoinColumn",
            "right_join_column",
            "rightJoinColumn",
            "null_equals_null",
            "nullEqualsNull",
            "filter",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Left,
            Right,
            JoinType,
            JoinConstraint,
            LeftJoinColumn,
            RightJoinColumn,
            NullEqualsNull,
            Filter,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "left" => Ok(GeneratedField::Left),
                            "right" => Ok(GeneratedField::Right),
                            "joinType" | "join_type" => Ok(GeneratedField::JoinType),
                            "joinConstraint" | "join_constraint" => Ok(GeneratedField::JoinConstraint),
                            "leftJoinColumn" | "left_join_column" => Ok(GeneratedField::LeftJoinColumn),
                            "rightJoinColumn" | "right_join_column" => Ok(GeneratedField::RightJoinColumn),
                            "nullEqualsNull" | "null_equals_null" => Ok(GeneratedField::NullEqualsNull),
                            "filter" => Ok(GeneratedField::Filter),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = JoinNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.JoinNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<JoinNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut left__ = None;
                let mut right__ = None;
                let mut join_type__ = None;
                let mut join_constraint__ = None;
                let mut left_join_column__ = None;
                let mut right_join_column__ = None;
                let mut null_equals_null__ = None;
                let mut filter__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Left => {
                            if left__.is_some() {
                                return Err(serde::de::Error::duplicate_field("left"));
                            }
                            left__ = map.next_value()?;
                        }
                        GeneratedField::Right => {
                            if right__.is_some() {
                                return Err(serde::de::Error::duplicate_field("right"));
                            }
                            right__ = map.next_value()?;
                        }
                        GeneratedField::JoinType => {
                            if join_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("joinType"));
                            }
                            join_type__ = Some(map.next_value::<JoinType>()? as i32);
                        }
                        GeneratedField::JoinConstraint => {
                            if join_constraint__.is_some() {
                                return Err(serde::de::Error::duplicate_field("joinConstraint"));
                            }
                            join_constraint__ = Some(map.next_value::<JoinConstraint>()? as i32);
                        }
                        GeneratedField::LeftJoinColumn => {
                            if left_join_column__.is_some() {
                                return Err(serde::de::Error::duplicate_field("leftJoinColumn"));
                            }
                            left_join_column__ = Some(map.next_value()?);
                        }
                        GeneratedField::RightJoinColumn => {
                            if right_join_column__.is_some() {
                                return Err(serde::de::Error::duplicate_field("rightJoinColumn"));
                            }
                            right_join_column__ = Some(map.next_value()?);
                        }
                        GeneratedField::NullEqualsNull => {
                            if null_equals_null__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nullEqualsNull"));
                            }
                            null_equals_null__ = Some(map.next_value()?);
                        }
                        GeneratedField::Filter => {
                            if filter__.is_some() {
                                return Err(serde::de::Error::duplicate_field("filter"));
                            }
                            filter__ = map.next_value()?;
                        }
                    }
                }
                Ok(JoinNode {
                    left: left__,
                    right: right__,
                    join_type: join_type__.unwrap_or_default(),
                    join_constraint: join_constraint__.unwrap_or_default(),
                    left_join_column: left_join_column__.unwrap_or_default(),
                    right_join_column: right_join_column__.unwrap_or_default(),
                    null_equals_null: null_equals_null__.unwrap_or_default(),
                    filter: filter__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.JoinNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for JoinOn {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.left.is_some() {
            len += 1;
        }
        if self.right.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.JoinOn", len)?;
        if let Some(v) = self.left.as_ref() {
            struct_ser.serialize_field("left", v)?;
        }
        if let Some(v) = self.right.as_ref() {
            struct_ser.serialize_field("right", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for JoinOn {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "left",
            "right",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Left,
            Right,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "left" => Ok(GeneratedField::Left),
                            "right" => Ok(GeneratedField::Right),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = JoinOn;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.JoinOn")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<JoinOn, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut left__ = None;
                let mut right__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Left => {
                            if left__.is_some() {
                                return Err(serde::de::Error::duplicate_field("left"));
                            }
                            left__ = map.next_value()?;
                        }
                        GeneratedField::Right => {
                            if right__.is_some() {
                                return Err(serde::de::Error::duplicate_field("right"));
                            }
                            right__ = map.next_value()?;
                        }
                    }
                }
                Ok(JoinOn {
                    left: left__,
                    right: right__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.JoinOn", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for JoinSide {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::LeftSide => "LEFT_SIDE",
            Self::RightSide => "RIGHT_SIDE",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for JoinSide {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "LEFT_SIDE",
            "RIGHT_SIDE",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = JoinSide;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(JoinSide::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(JoinSide::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "LEFT_SIDE" => Ok(JoinSide::LeftSide),
                    "RIGHT_SIDE" => Ok(JoinSide::RightSide),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for JoinType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Inner => "INNER",
            Self::Left => "LEFT",
            Self::Right => "RIGHT",
            Self::Full => "FULL",
            Self::Leftsemi => "LEFTSEMI",
            Self::Leftanti => "LEFTANTI",
            Self::Rightsemi => "RIGHTSEMI",
            Self::Rightanti => "RIGHTANTI",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for JoinType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "INNER",
            "LEFT",
            "RIGHT",
            "FULL",
            "LEFTSEMI",
            "LEFTANTI",
            "RIGHTSEMI",
            "RIGHTANTI",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = JoinType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(JoinType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(JoinType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "INNER" => Ok(JoinType::Inner),
                    "LEFT" => Ok(JoinType::Left),
                    "RIGHT" => Ok(JoinType::Right),
                    "FULL" => Ok(JoinType::Full),
                    "LEFTSEMI" => Ok(JoinType::Leftsemi),
                    "LEFTANTI" => Ok(JoinType::Leftanti),
                    "RIGHTSEMI" => Ok(JoinType::Rightsemi),
                    "RIGHTANTI" => Ok(JoinType::Rightanti),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for LikeNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.negated {
            len += 1;
        }
        if self.expr.is_some() {
            len += 1;
        }
        if self.pattern.is_some() {
            len += 1;
        }
        if !self.escape_char.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.LikeNode", len)?;
        if self.negated {
            struct_ser.serialize_field("negated", &self.negated)?;
        }
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if let Some(v) = self.pattern.as_ref() {
            struct_ser.serialize_field("pattern", v)?;
        }
        if !self.escape_char.is_empty() {
            struct_ser.serialize_field("escapeChar", &self.escape_char)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for LikeNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "negated",
            "expr",
            "pattern",
            "escape_char",
            "escapeChar",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Negated,
            Expr,
            Pattern,
            EscapeChar,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "negated" => Ok(GeneratedField::Negated),
                            "expr" => Ok(GeneratedField::Expr),
                            "pattern" => Ok(GeneratedField::Pattern),
                            "escapeChar" | "escape_char" => Ok(GeneratedField::EscapeChar),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = LikeNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.LikeNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<LikeNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut negated__ = None;
                let mut expr__ = None;
                let mut pattern__ = None;
                let mut escape_char__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Negated => {
                            if negated__.is_some() {
                                return Err(serde::de::Error::duplicate_field("negated"));
                            }
                            negated__ = Some(map.next_value()?);
                        }
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                        GeneratedField::Pattern => {
                            if pattern__.is_some() {
                                return Err(serde::de::Error::duplicate_field("pattern"));
                            }
                            pattern__ = map.next_value()?;
                        }
                        GeneratedField::EscapeChar => {
                            if escape_char__.is_some() {
                                return Err(serde::de::Error::duplicate_field("escapeChar"));
                            }
                            escape_char__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(LikeNode {
                    negated: negated__.unwrap_or_default(),
                    expr: expr__,
                    pattern: pattern__,
                    escape_char: escape_char__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.LikeNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for LimitNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if self.skip != 0 {
            len += 1;
        }
        if self.fetch != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.LimitNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if self.skip != 0 {
            struct_ser.serialize_field("skip", ToString::to_string(&self.skip).as_str())?;
        }
        if self.fetch != 0 {
            struct_ser.serialize_field("fetch", ToString::to_string(&self.fetch).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for LimitNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "skip",
            "fetch",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Skip,
            Fetch,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "skip" => Ok(GeneratedField::Skip),
                            "fetch" => Ok(GeneratedField::Fetch),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = LimitNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.LimitNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<LimitNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut skip__ = None;
                let mut fetch__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map.next_value()?;
                        }
                        GeneratedField::Skip => {
                            if skip__.is_some() {
                                return Err(serde::de::Error::duplicate_field("skip"));
                            }
                            skip__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Fetch => {
                            if fetch__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fetch"));
                            }
                            fetch__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(LimitNode {
                    input: input__,
                    skip: skip__.unwrap_or_default(),
                    fetch: fetch__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.LimitNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for List {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.field_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.List", len)?;
        if let Some(v) = self.field_type.as_ref() {
            struct_ser.serialize_field("fieldType", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for List {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "field_type",
            "fieldType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            FieldType,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "fieldType" | "field_type" => Ok(GeneratedField::FieldType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = List;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.List")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<List, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut field_type__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::FieldType => {
                            if field_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fieldType"));
                            }
                            field_type__ = map.next_value()?;
                        }
                    }
                }
                Ok(List {
                    field_type: field_type__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.List", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ListingTableScanNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.table_name.is_empty() {
            len += 1;
        }
        if !self.paths.is_empty() {
            len += 1;
        }
        if !self.file_extension.is_empty() {
            len += 1;
        }
        if self.projection.is_some() {
            len += 1;
        }
        if self.schema.is_some() {
            len += 1;
        }
        if !self.filters.is_empty() {
            len += 1;
        }
        if !self.table_partition_cols.is_empty() {
            len += 1;
        }
        if self.collect_stat {
            len += 1;
        }
        if self.target_partitions != 0 {
            len += 1;
        }
        if !self.file_sort_order.is_empty() {
            len += 1;
        }
        if self.file_format_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ListingTableScanNode", len)?;
        if !self.table_name.is_empty() {
            struct_ser.serialize_field("tableName", &self.table_name)?;
        }
        if !self.paths.is_empty() {
            struct_ser.serialize_field("paths", &self.paths)?;
        }
        if !self.file_extension.is_empty() {
            struct_ser.serialize_field("fileExtension", &self.file_extension)?;
        }
        if let Some(v) = self.projection.as_ref() {
            struct_ser.serialize_field("projection", v)?;
        }
        if let Some(v) = self.schema.as_ref() {
            struct_ser.serialize_field("schema", v)?;
        }
        if !self.filters.is_empty() {
            struct_ser.serialize_field("filters", &self.filters)?;
        }
        if !self.table_partition_cols.is_empty() {
            struct_ser.serialize_field("tablePartitionCols", &self.table_partition_cols)?;
        }
        if self.collect_stat {
            struct_ser.serialize_field("collectStat", &self.collect_stat)?;
        }
        if self.target_partitions != 0 {
            struct_ser.serialize_field("targetPartitions", &self.target_partitions)?;
        }
        if !self.file_sort_order.is_empty() {
            struct_ser.serialize_field("fileSortOrder", &self.file_sort_order)?;
        }
        if let Some(v) = self.file_format_type.as_ref() {
            match v {
                listing_table_scan_node::FileFormatType::Csv(v) => {
                    struct_ser.serialize_field("csv", v)?;
                }
                listing_table_scan_node::FileFormatType::Parquet(v) => {
                    struct_ser.serialize_field("parquet", v)?;
                }
                listing_table_scan_node::FileFormatType::Avro(v) => {
                    struct_ser.serialize_field("avro", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ListingTableScanNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "table_name",
            "tableName",
            "paths",
            "file_extension",
            "fileExtension",
            "projection",
            "schema",
            "filters",
            "table_partition_cols",
            "tablePartitionCols",
            "collect_stat",
            "collectStat",
            "target_partitions",
            "targetPartitions",
            "file_sort_order",
            "fileSortOrder",
            "csv",
            "parquet",
            "avro",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableName,
            Paths,
            FileExtension,
            Projection,
            Schema,
            Filters,
            TablePartitionCols,
            CollectStat,
            TargetPartitions,
            FileSortOrder,
            Csv,
            Parquet,
            Avro,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "tableName" | "table_name" => Ok(GeneratedField::TableName),
                            "paths" => Ok(GeneratedField::Paths),
                            "fileExtension" | "file_extension" => Ok(GeneratedField::FileExtension),
                            "projection" => Ok(GeneratedField::Projection),
                            "schema" => Ok(GeneratedField::Schema),
                            "filters" => Ok(GeneratedField::Filters),
                            "tablePartitionCols" | "table_partition_cols" => Ok(GeneratedField::TablePartitionCols),
                            "collectStat" | "collect_stat" => Ok(GeneratedField::CollectStat),
                            "targetPartitions" | "target_partitions" => Ok(GeneratedField::TargetPartitions),
                            "fileSortOrder" | "file_sort_order" => Ok(GeneratedField::FileSortOrder),
                            "csv" => Ok(GeneratedField::Csv),
                            "parquet" => Ok(GeneratedField::Parquet),
                            "avro" => Ok(GeneratedField::Avro),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ListingTableScanNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ListingTableScanNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ListingTableScanNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_name__ = None;
                let mut paths__ = None;
                let mut file_extension__ = None;
                let mut projection__ = None;
                let mut schema__ = None;
                let mut filters__ = None;
                let mut table_partition_cols__ = None;
                let mut collect_stat__ = None;
                let mut target_partitions__ = None;
                let mut file_sort_order__ = None;
                let mut file_format_type__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TableName => {
                            if table_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableName"));
                            }
                            table_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::Paths => {
                            if paths__.is_some() {
                                return Err(serde::de::Error::duplicate_field("paths"));
                            }
                            paths__ = Some(map.next_value()?);
                        }
                        GeneratedField::FileExtension => {
                            if file_extension__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fileExtension"));
                            }
                            file_extension__ = Some(map.next_value()?);
                        }
                        GeneratedField::Projection => {
                            if projection__.is_some() {
                                return Err(serde::de::Error::duplicate_field("projection"));
                            }
                            projection__ = map.next_value()?;
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = map.next_value()?;
                        }
                        GeneratedField::Filters => {
                            if filters__.is_some() {
                                return Err(serde::de::Error::duplicate_field("filters"));
                            }
                            filters__ = Some(map.next_value()?);
                        }
                        GeneratedField::TablePartitionCols => {
                            if table_partition_cols__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tablePartitionCols"));
                            }
                            table_partition_cols__ = Some(map.next_value()?);
                        }
                        GeneratedField::CollectStat => {
                            if collect_stat__.is_some() {
                                return Err(serde::de::Error::duplicate_field("collectStat"));
                            }
                            collect_stat__ = Some(map.next_value()?);
                        }
                        GeneratedField::TargetPartitions => {
                            if target_partitions__.is_some() {
                                return Err(serde::de::Error::duplicate_field("targetPartitions"));
                            }
                            target_partitions__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::FileSortOrder => {
                            if file_sort_order__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fileSortOrder"));
                            }
                            file_sort_order__ = Some(map.next_value()?);
                        }
                        GeneratedField::Csv => {
                            if file_format_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("csv"));
                            }
                            file_format_type__ = map.next_value::<::std::option::Option<_>>()?.map(listing_table_scan_node::FileFormatType::Csv)
;
                        }
                        GeneratedField::Parquet => {
                            if file_format_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("parquet"));
                            }
                            file_format_type__ = map.next_value::<::std::option::Option<_>>()?.map(listing_table_scan_node::FileFormatType::Parquet)
;
                        }
                        GeneratedField::Avro => {
                            if file_format_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("avro"));
                            }
                            file_format_type__ = map.next_value::<::std::option::Option<_>>()?.map(listing_table_scan_node::FileFormatType::Avro)
;
                        }
                    }
                }
                Ok(ListingTableScanNode {
                    table_name: table_name__.unwrap_or_default(),
                    paths: paths__.unwrap_or_default(),
                    file_extension: file_extension__.unwrap_or_default(),
                    projection: projection__,
                    schema: schema__,
                    filters: filters__.unwrap_or_default(),
                    table_partition_cols: table_partition_cols__.unwrap_or_default(),
                    collect_stat: collect_stat__.unwrap_or_default(),
                    target_partitions: target_partitions__.unwrap_or_default(),
                    file_sort_order: file_sort_order__.unwrap_or_default(),
                    file_format_type: file_format_type__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ListingTableScanNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for LocalLimitExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if self.fetch != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.LocalLimitExecNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if self.fetch != 0 {
            struct_ser.serialize_field("fetch", &self.fetch)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for LocalLimitExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "fetch",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Fetch,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "fetch" => Ok(GeneratedField::Fetch),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = LocalLimitExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.LocalLimitExecNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<LocalLimitExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut fetch__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map.next_value()?;
                        }
                        GeneratedField::Fetch => {
                            if fetch__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fetch"));
                            }
                            fetch__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(LocalLimitExecNode {
                    input: input__,
                    fetch: fetch__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.LocalLimitExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for LogicalExprList {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.expr.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.LogicalExprList", len)?;
        if !self.expr.is_empty() {
            struct_ser.serialize_field("expr", &self.expr)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for LogicalExprList {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = LogicalExprList;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.LogicalExprList")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<LogicalExprList, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(LogicalExprList {
                    expr: expr__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.LogicalExprList", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for LogicalExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.LogicalExprNode", len)?;
        if let Some(v) = self.expr_type.as_ref() {
            match v {
                logical_expr_node::ExprType::Column(v) => {
                    struct_ser.serialize_field("column", v)?;
                }
                logical_expr_node::ExprType::Alias(v) => {
                    struct_ser.serialize_field("alias", v)?;
                }
                logical_expr_node::ExprType::Literal(v) => {
                    struct_ser.serialize_field("literal", v)?;
                }
                logical_expr_node::ExprType::BinaryExpr(v) => {
                    struct_ser.serialize_field("binaryExpr", v)?;
                }
                logical_expr_node::ExprType::AggregateExpr(v) => {
                    struct_ser.serialize_field("aggregateExpr", v)?;
                }
                logical_expr_node::ExprType::IsNullExpr(v) => {
                    struct_ser.serialize_field("isNullExpr", v)?;
                }
                logical_expr_node::ExprType::IsNotNullExpr(v) => {
                    struct_ser.serialize_field("isNotNullExpr", v)?;
                }
                logical_expr_node::ExprType::NotExpr(v) => {
                    struct_ser.serialize_field("notExpr", v)?;
                }
                logical_expr_node::ExprType::Between(v) => {
                    struct_ser.serialize_field("between", v)?;
                }
                logical_expr_node::ExprType::Case(v) => {
                    struct_ser.serialize_field("case", v)?;
                }
                logical_expr_node::ExprType::Cast(v) => {
                    struct_ser.serialize_field("cast", v)?;
                }
                logical_expr_node::ExprType::Sort(v) => {
                    struct_ser.serialize_field("sort", v)?;
                }
                logical_expr_node::ExprType::Negative(v) => {
                    struct_ser.serialize_field("negative", v)?;
                }
                logical_expr_node::ExprType::InList(v) => {
                    struct_ser.serialize_field("inList", v)?;
                }
                logical_expr_node::ExprType::Wildcard(v) => {
                    struct_ser.serialize_field("wildcard", v)?;
                }
                logical_expr_node::ExprType::ScalarFunction(v) => {
                    struct_ser.serialize_field("scalarFunction", v)?;
                }
                logical_expr_node::ExprType::TryCast(v) => {
                    struct_ser.serialize_field("tryCast", v)?;
                }
                logical_expr_node::ExprType::WindowExpr(v) => {
                    struct_ser.serialize_field("windowExpr", v)?;
                }
                logical_expr_node::ExprType::AggregateUdfExpr(v) => {
                    struct_ser.serialize_field("aggregateUdfExpr", v)?;
                }
                logical_expr_node::ExprType::ScalarUdfExpr(v) => {
                    struct_ser.serialize_field("scalarUdfExpr", v)?;
                }
                logical_expr_node::ExprType::GetIndexedField(v) => {
                    struct_ser.serialize_field("getIndexedField", v)?;
                }
                logical_expr_node::ExprType::GroupingSet(v) => {
                    struct_ser.serialize_field("groupingSet", v)?;
                }
                logical_expr_node::ExprType::Cube(v) => {
                    struct_ser.serialize_field("cube", v)?;
                }
                logical_expr_node::ExprType::Rollup(v) => {
                    struct_ser.serialize_field("rollup", v)?;
                }
                logical_expr_node::ExprType::IsTrue(v) => {
                    struct_ser.serialize_field("isTrue", v)?;
                }
                logical_expr_node::ExprType::IsFalse(v) => {
                    struct_ser.serialize_field("isFalse", v)?;
                }
                logical_expr_node::ExprType::IsUnknown(v) => {
                    struct_ser.serialize_field("isUnknown", v)?;
                }
                logical_expr_node::ExprType::IsNotTrue(v) => {
                    struct_ser.serialize_field("isNotTrue", v)?;
                }
                logical_expr_node::ExprType::IsNotFalse(v) => {
                    struct_ser.serialize_field("isNotFalse", v)?;
                }
                logical_expr_node::ExprType::IsNotUnknown(v) => {
                    struct_ser.serialize_field("isNotUnknown", v)?;
                }
                logical_expr_node::ExprType::Like(v) => {
                    struct_ser.serialize_field("like", v)?;
                }
                logical_expr_node::ExprType::Ilike(v) => {
                    struct_ser.serialize_field("ilike", v)?;
                }
                logical_expr_node::ExprType::SimilarTo(v) => {
                    struct_ser.serialize_field("similarTo", v)?;
                }
                logical_expr_node::ExprType::Placeholder(v) => {
                    struct_ser.serialize_field("placeholder", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for LogicalExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "column",
            "alias",
            "literal",
            "binary_expr",
            "binaryExpr",
            "aggregate_expr",
            "aggregateExpr",
            "is_null_expr",
            "isNullExpr",
            "is_not_null_expr",
            "isNotNullExpr",
            "not_expr",
            "notExpr",
            "between",
            "case_",
            "case",
            "cast",
            "sort",
            "negative",
            "in_list",
            "inList",
            "wildcard",
            "scalar_function",
            "scalarFunction",
            "try_cast",
            "tryCast",
            "window_expr",
            "windowExpr",
            "aggregate_udf_expr",
            "aggregateUdfExpr",
            "scalar_udf_expr",
            "scalarUdfExpr",
            "get_indexed_field",
            "getIndexedField",
            "grouping_set",
            "groupingSet",
            "cube",
            "rollup",
            "is_true",
            "isTrue",
            "is_false",
            "isFalse",
            "is_unknown",
            "isUnknown",
            "is_not_true",
            "isNotTrue",
            "is_not_false",
            "isNotFalse",
            "is_not_unknown",
            "isNotUnknown",
            "like",
            "ilike",
            "similar_to",
            "similarTo",
            "placeholder",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Column,
            Alias,
            Literal,
            BinaryExpr,
            AggregateExpr,
            IsNullExpr,
            IsNotNullExpr,
            NotExpr,
            Between,
            Case,
            Cast,
            Sort,
            Negative,
            InList,
            Wildcard,
            ScalarFunction,
            TryCast,
            WindowExpr,
            AggregateUdfExpr,
            ScalarUdfExpr,
            GetIndexedField,
            GroupingSet,
            Cube,
            Rollup,
            IsTrue,
            IsFalse,
            IsUnknown,
            IsNotTrue,
            IsNotFalse,
            IsNotUnknown,
            Like,
            Ilike,
            SimilarTo,
            Placeholder,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "column" => Ok(GeneratedField::Column),
                            "alias" => Ok(GeneratedField::Alias),
                            "literal" => Ok(GeneratedField::Literal),
                            "binaryExpr" | "binary_expr" => Ok(GeneratedField::BinaryExpr),
                            "aggregateExpr" | "aggregate_expr" => Ok(GeneratedField::AggregateExpr),
                            "isNullExpr" | "is_null_expr" => Ok(GeneratedField::IsNullExpr),
                            "isNotNullExpr" | "is_not_null_expr" => Ok(GeneratedField::IsNotNullExpr),
                            "notExpr" | "not_expr" => Ok(GeneratedField::NotExpr),
                            "between" => Ok(GeneratedField::Between),
                            "case" | "case_" => Ok(GeneratedField::Case),
                            "cast" => Ok(GeneratedField::Cast),
                            "sort" => Ok(GeneratedField::Sort),
                            "negative" => Ok(GeneratedField::Negative),
                            "inList" | "in_list" => Ok(GeneratedField::InList),
                            "wildcard" => Ok(GeneratedField::Wildcard),
                            "scalarFunction" | "scalar_function" => Ok(GeneratedField::ScalarFunction),
                            "tryCast" | "try_cast" => Ok(GeneratedField::TryCast),
                            "windowExpr" | "window_expr" => Ok(GeneratedField::WindowExpr),
                            "aggregateUdfExpr" | "aggregate_udf_expr" => Ok(GeneratedField::AggregateUdfExpr),
                            "scalarUdfExpr" | "scalar_udf_expr" => Ok(GeneratedField::ScalarUdfExpr),
                            "getIndexedField" | "get_indexed_field" => Ok(GeneratedField::GetIndexedField),
                            "groupingSet" | "grouping_set" => Ok(GeneratedField::GroupingSet),
                            "cube" => Ok(GeneratedField::Cube),
                            "rollup" => Ok(GeneratedField::Rollup),
                            "isTrue" | "is_true" => Ok(GeneratedField::IsTrue),
                            "isFalse" | "is_false" => Ok(GeneratedField::IsFalse),
                            "isUnknown" | "is_unknown" => Ok(GeneratedField::IsUnknown),
                            "isNotTrue" | "is_not_true" => Ok(GeneratedField::IsNotTrue),
                            "isNotFalse" | "is_not_false" => Ok(GeneratedField::IsNotFalse),
                            "isNotUnknown" | "is_not_unknown" => Ok(GeneratedField::IsNotUnknown),
                            "like" => Ok(GeneratedField::Like),
                            "ilike" => Ok(GeneratedField::Ilike),
                            "similarTo" | "similar_to" => Ok(GeneratedField::SimilarTo),
                            "placeholder" => Ok(GeneratedField::Placeholder),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = LogicalExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.LogicalExprNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<LogicalExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr_type__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Column => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("column"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Column)
;
                        }
                        GeneratedField::Alias => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("alias"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Alias)
;
                        }
                        GeneratedField::Literal => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("literal"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Literal)
;
                        }
                        GeneratedField::BinaryExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("binaryExpr"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::BinaryExpr)
;
                        }
                        GeneratedField::AggregateExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggregateExpr"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::AggregateExpr)
;
                        }
                        GeneratedField::IsNullExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isNullExpr"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::IsNullExpr)
;
                        }
                        GeneratedField::IsNotNullExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isNotNullExpr"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::IsNotNullExpr)
;
                        }
                        GeneratedField::NotExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("notExpr"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::NotExpr)
;
                        }
                        GeneratedField::Between => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("between"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Between)
;
                        }
                        GeneratedField::Case => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("case"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Case)
;
                        }
                        GeneratedField::Cast => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("cast"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Cast)
;
                        }
                        GeneratedField::Sort => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sort"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Sort)
;
                        }
                        GeneratedField::Negative => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("negative"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Negative)
;
                        }
                        GeneratedField::InList => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("inList"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::InList)
;
                        }
                        GeneratedField::Wildcard => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("wildcard"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Wildcard);
                        }
                        GeneratedField::ScalarFunction => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("scalarFunction"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::ScalarFunction)
;
                        }
                        GeneratedField::TryCast => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tryCast"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::TryCast)
;
                        }
                        GeneratedField::WindowExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("windowExpr"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::WindowExpr)
;
                        }
                        GeneratedField::AggregateUdfExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggregateUdfExpr"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::AggregateUdfExpr)
;
                        }
                        GeneratedField::ScalarUdfExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("scalarUdfExpr"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::ScalarUdfExpr)
;
                        }
                        GeneratedField::GetIndexedField => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("getIndexedField"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::GetIndexedField)
;
                        }
                        GeneratedField::GroupingSet => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("groupingSet"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::GroupingSet)
;
                        }
                        GeneratedField::Cube => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("cube"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Cube)
;
                        }
                        GeneratedField::Rollup => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("rollup"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Rollup)
;
                        }
                        GeneratedField::IsTrue => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isTrue"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::IsTrue)
;
                        }
                        GeneratedField::IsFalse => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isFalse"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::IsFalse)
;
                        }
                        GeneratedField::IsUnknown => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isUnknown"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::IsUnknown)
;
                        }
                        GeneratedField::IsNotTrue => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isNotTrue"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::IsNotTrue)
;
                        }
                        GeneratedField::IsNotFalse => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isNotFalse"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::IsNotFalse)
;
                        }
                        GeneratedField::IsNotUnknown => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isNotUnknown"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::IsNotUnknown)
;
                        }
                        GeneratedField::Like => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("like"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Like)
;
                        }
                        GeneratedField::Ilike => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ilike"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Ilike)
;
                        }
                        GeneratedField::SimilarTo => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("similarTo"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::SimilarTo)
;
                        }
                        GeneratedField::Placeholder => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("placeholder"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Placeholder)
;
                        }
                    }
                }
                Ok(LogicalExprNode {
                    expr_type: expr_type__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.LogicalExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for LogicalExtensionNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.node.is_empty() {
            len += 1;
        }
        if !self.inputs.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.LogicalExtensionNode", len)?;
        if !self.node.is_empty() {
            struct_ser.serialize_field("node", pbjson::private::base64::encode(&self.node).as_str())?;
        }
        if !self.inputs.is_empty() {
            struct_ser.serialize_field("inputs", &self.inputs)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for LogicalExtensionNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "node",
            "inputs",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Node,
            Inputs,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "node" => Ok(GeneratedField::Node),
                            "inputs" => Ok(GeneratedField::Inputs),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = LogicalExtensionNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.LogicalExtensionNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<LogicalExtensionNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node__ = None;
                let mut inputs__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Node => {
                            if node__.is_some() {
                                return Err(serde::de::Error::duplicate_field("node"));
                            }
                            node__ = 
                                Some(map.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Inputs => {
                            if inputs__.is_some() {
                                return Err(serde::de::Error::duplicate_field("inputs"));
                            }
                            inputs__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(LogicalExtensionNode {
                    node: node__.unwrap_or_default(),
                    inputs: inputs__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.LogicalExtensionNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for LogicalPlanNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.logical_plan_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.LogicalPlanNode", len)?;
        if let Some(v) = self.logical_plan_type.as_ref() {
            match v {
                logical_plan_node::LogicalPlanType::ListingScan(v) => {
                    struct_ser.serialize_field("listingScan", v)?;
                }
                logical_plan_node::LogicalPlanType::Projection(v) => {
                    struct_ser.serialize_field("projection", v)?;
                }
                logical_plan_node::LogicalPlanType::Selection(v) => {
                    struct_ser.serialize_field("selection", v)?;
                }
                logical_plan_node::LogicalPlanType::Limit(v) => {
                    struct_ser.serialize_field("limit", v)?;
                }
                logical_plan_node::LogicalPlanType::Aggregate(v) => {
                    struct_ser.serialize_field("aggregate", v)?;
                }
                logical_plan_node::LogicalPlanType::Join(v) => {
                    struct_ser.serialize_field("join", v)?;
                }
                logical_plan_node::LogicalPlanType::Sort(v) => {
                    struct_ser.serialize_field("sort", v)?;
                }
                logical_plan_node::LogicalPlanType::Repartition(v) => {
                    struct_ser.serialize_field("repartition", v)?;
                }
                logical_plan_node::LogicalPlanType::EmptyRelation(v) => {
                    struct_ser.serialize_field("emptyRelation", v)?;
                }
                logical_plan_node::LogicalPlanType::CreateExternalTable(v) => {
                    struct_ser.serialize_field("createExternalTable", v)?;
                }
                logical_plan_node::LogicalPlanType::Explain(v) => {
                    struct_ser.serialize_field("explain", v)?;
                }
                logical_plan_node::LogicalPlanType::Window(v) => {
                    struct_ser.serialize_field("window", v)?;
                }
                logical_plan_node::LogicalPlanType::Analyze(v) => {
                    struct_ser.serialize_field("analyze", v)?;
                }
                logical_plan_node::LogicalPlanType::CrossJoin(v) => {
                    struct_ser.serialize_field("crossJoin", v)?;
                }
                logical_plan_node::LogicalPlanType::Values(v) => {
                    struct_ser.serialize_field("values", v)?;
                }
                logical_plan_node::LogicalPlanType::Extension(v) => {
                    struct_ser.serialize_field("extension", v)?;
                }
                logical_plan_node::LogicalPlanType::CreateCatalogSchema(v) => {
                    struct_ser.serialize_field("createCatalogSchema", v)?;
                }
                logical_plan_node::LogicalPlanType::Union(v) => {
                    struct_ser.serialize_field("union", v)?;
                }
                logical_plan_node::LogicalPlanType::CreateCatalog(v) => {
                    struct_ser.serialize_field("createCatalog", v)?;
                }
                logical_plan_node::LogicalPlanType::SubqueryAlias(v) => {
                    struct_ser.serialize_field("subqueryAlias", v)?;
                }
                logical_plan_node::LogicalPlanType::CreateView(v) => {
                    struct_ser.serialize_field("createView", v)?;
                }
                logical_plan_node::LogicalPlanType::Distinct(v) => {
                    struct_ser.serialize_field("distinct", v)?;
                }
                logical_plan_node::LogicalPlanType::ViewScan(v) => {
                    struct_ser.serialize_field("viewScan", v)?;
                }
                logical_plan_node::LogicalPlanType::CustomScan(v) => {
                    struct_ser.serialize_field("customScan", v)?;
                }
                logical_plan_node::LogicalPlanType::Prepare(v) => {
                    struct_ser.serialize_field("prepare", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for LogicalPlanNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "listing_scan",
            "listingScan",
            "projection",
            "selection",
            "limit",
            "aggregate",
            "join",
            "sort",
            "repartition",
            "empty_relation",
            "emptyRelation",
            "create_external_table",
            "createExternalTable",
            "explain",
            "window",
            "analyze",
            "cross_join",
            "crossJoin",
            "values",
            "extension",
            "create_catalog_schema",
            "createCatalogSchema",
            "union",
            "create_catalog",
            "createCatalog",
            "subquery_alias",
            "subqueryAlias",
            "create_view",
            "createView",
            "distinct",
            "view_scan",
            "viewScan",
            "custom_scan",
            "customScan",
            "prepare",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ListingScan,
            Projection,
            Selection,
            Limit,
            Aggregate,
            Join,
            Sort,
            Repartition,
            EmptyRelation,
            CreateExternalTable,
            Explain,
            Window,
            Analyze,
            CrossJoin,
            Values,
            Extension,
            CreateCatalogSchema,
            Union,
            CreateCatalog,
            SubqueryAlias,
            CreateView,
            Distinct,
            ViewScan,
            CustomScan,
            Prepare,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "listingScan" | "listing_scan" => Ok(GeneratedField::ListingScan),
                            "projection" => Ok(GeneratedField::Projection),
                            "selection" => Ok(GeneratedField::Selection),
                            "limit" => Ok(GeneratedField::Limit),
                            "aggregate" => Ok(GeneratedField::Aggregate),
                            "join" => Ok(GeneratedField::Join),
                            "sort" => Ok(GeneratedField::Sort),
                            "repartition" => Ok(GeneratedField::Repartition),
                            "emptyRelation" | "empty_relation" => Ok(GeneratedField::EmptyRelation),
                            "createExternalTable" | "create_external_table" => Ok(GeneratedField::CreateExternalTable),
                            "explain" => Ok(GeneratedField::Explain),
                            "window" => Ok(GeneratedField::Window),
                            "analyze" => Ok(GeneratedField::Analyze),
                            "crossJoin" | "cross_join" => Ok(GeneratedField::CrossJoin),
                            "values" => Ok(GeneratedField::Values),
                            "extension" => Ok(GeneratedField::Extension),
                            "createCatalogSchema" | "create_catalog_schema" => Ok(GeneratedField::CreateCatalogSchema),
                            "union" => Ok(GeneratedField::Union),
                            "createCatalog" | "create_catalog" => Ok(GeneratedField::CreateCatalog),
                            "subqueryAlias" | "subquery_alias" => Ok(GeneratedField::SubqueryAlias),
                            "createView" | "create_view" => Ok(GeneratedField::CreateView),
                            "distinct" => Ok(GeneratedField::Distinct),
                            "viewScan" | "view_scan" => Ok(GeneratedField::ViewScan),
                            "customScan" | "custom_scan" => Ok(GeneratedField::CustomScan),
                            "prepare" => Ok(GeneratedField::Prepare),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = LogicalPlanNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.LogicalPlanNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<LogicalPlanNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut logical_plan_type__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ListingScan => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("listingScan"));
                            }
                            logical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::ListingScan)
;
                        }
                        GeneratedField::Projection => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("projection"));
                            }
                            logical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Projection)
;
                        }
                        GeneratedField::Selection => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("selection"));
                            }
                            logical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Selection)
;
                        }
                        GeneratedField::Limit => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("limit"));
                            }
                            logical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Limit)
;
                        }
                        GeneratedField::Aggregate => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggregate"));
                            }
                            logical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Aggregate)
;
                        }
                        GeneratedField::Join => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("join"));
                            }
                            logical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Join)
;
                        }
                        GeneratedField::Sort => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sort"));
                            }
                            logical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Sort)
;
                        }
                        GeneratedField::Repartition => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("repartition"));
                            }
                            logical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Repartition)
;
                        }
                        GeneratedField::EmptyRelation => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("emptyRelation"));
                            }
                            logical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::EmptyRelation)
;
                        }
                        GeneratedField::CreateExternalTable => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("createExternalTable"));
                            }
                            logical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::CreateExternalTable)
;
                        }
                        GeneratedField::Explain => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("explain"));
                            }
                            logical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Explain)
;
                        }
                        GeneratedField::Window => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("window"));
                            }
                            logical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Window)
;
                        }
                        GeneratedField::Analyze => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("analyze"));
                            }
                            logical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Analyze)
;
                        }
                        GeneratedField::CrossJoin => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("crossJoin"));
                            }
                            logical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::CrossJoin)
;
                        }
                        GeneratedField::Values => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("values"));
                            }
                            logical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Values)
;
                        }
                        GeneratedField::Extension => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("extension"));
                            }
                            logical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Extension)
;
                        }
                        GeneratedField::CreateCatalogSchema => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("createCatalogSchema"));
                            }
                            logical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::CreateCatalogSchema)
;
                        }
                        GeneratedField::Union => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("union"));
                            }
                            logical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Union)
;
                        }
                        GeneratedField::CreateCatalog => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("createCatalog"));
                            }
                            logical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::CreateCatalog)
;
                        }
                        GeneratedField::SubqueryAlias => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("subqueryAlias"));
                            }
                            logical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::SubqueryAlias)
;
                        }
                        GeneratedField::CreateView => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("createView"));
                            }
                            logical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::CreateView)
;
                        }
                        GeneratedField::Distinct => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("distinct"));
                            }
                            logical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Distinct)
;
                        }
                        GeneratedField::ViewScan => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("viewScan"));
                            }
                            logical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::ViewScan)
;
                        }
                        GeneratedField::CustomScan => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("customScan"));
                            }
                            logical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::CustomScan)
;
                        }
                        GeneratedField::Prepare => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("prepare"));
                            }
                            logical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Prepare)
;
                        }
                    }
                }
                Ok(LogicalPlanNode {
                    logical_plan_type: logical_plan_type__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.LogicalPlanNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for NegativeNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.NegativeNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for NegativeNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = NegativeNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.NegativeNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<NegativeNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                    }
                }
                Ok(NegativeNode {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.NegativeNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Not {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.Not", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Not {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Not;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.Not")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Not, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                    }
                }
                Ok(Not {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.Not", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for OptimizedLogicalPlanType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.optimizer_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.OptimizedLogicalPlanType", len)?;
        if !self.optimizer_name.is_empty() {
            struct_ser.serialize_field("optimizerName", &self.optimizer_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for OptimizedLogicalPlanType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "optimizer_name",
            "optimizerName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            OptimizerName,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "optimizerName" | "optimizer_name" => Ok(GeneratedField::OptimizerName),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = OptimizedLogicalPlanType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.OptimizedLogicalPlanType")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<OptimizedLogicalPlanType, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut optimizer_name__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::OptimizerName => {
                            if optimizer_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("optimizerName"));
                            }
                            optimizer_name__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(OptimizedLogicalPlanType {
                    optimizer_name: optimizer_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.OptimizedLogicalPlanType", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for OptimizedPhysicalPlanType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.optimizer_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.OptimizedPhysicalPlanType", len)?;
        if !self.optimizer_name.is_empty() {
            struct_ser.serialize_field("optimizerName", &self.optimizer_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for OptimizedPhysicalPlanType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "optimizer_name",
            "optimizerName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            OptimizerName,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "optimizerName" | "optimizer_name" => Ok(GeneratedField::OptimizerName),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = OptimizedPhysicalPlanType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.OptimizedPhysicalPlanType")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<OptimizedPhysicalPlanType, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut optimizer_name__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::OptimizerName => {
                            if optimizer_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("optimizerName"));
                            }
                            optimizer_name__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(OptimizedPhysicalPlanType {
                    optimizer_name: optimizer_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.OptimizedPhysicalPlanType", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for OwnedTableReference {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.table_reference_enum.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.OwnedTableReference", len)?;
        if let Some(v) = self.table_reference_enum.as_ref() {
            match v {
                owned_table_reference::TableReferenceEnum::Bare(v) => {
                    struct_ser.serialize_field("bare", v)?;
                }
                owned_table_reference::TableReferenceEnum::Partial(v) => {
                    struct_ser.serialize_field("partial", v)?;
                }
                owned_table_reference::TableReferenceEnum::Full(v) => {
                    struct_ser.serialize_field("full", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for OwnedTableReference {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "bare",
            "partial",
            "full",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Bare,
            Partial,
            Full,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "bare" => Ok(GeneratedField::Bare),
                            "partial" => Ok(GeneratedField::Partial),
                            "full" => Ok(GeneratedField::Full),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = OwnedTableReference;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.OwnedTableReference")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<OwnedTableReference, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_reference_enum__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Bare => {
                            if table_reference_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bare"));
                            }
                            table_reference_enum__ = map.next_value::<::std::option::Option<_>>()?.map(owned_table_reference::TableReferenceEnum::Bare)
;
                        }
                        GeneratedField::Partial => {
                            if table_reference_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partial"));
                            }
                            table_reference_enum__ = map.next_value::<::std::option::Option<_>>()?.map(owned_table_reference::TableReferenceEnum::Partial)
;
                        }
                        GeneratedField::Full => {
                            if table_reference_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("full"));
                            }
                            table_reference_enum__ = map.next_value::<::std::option::Option<_>>()?.map(owned_table_reference::TableReferenceEnum::Full)
;
                        }
                    }
                }
                Ok(OwnedTableReference {
                    table_reference_enum: table_reference_enum__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.OwnedTableReference", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ParquetFormat {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.enable_pruning {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ParquetFormat", len)?;
        if self.enable_pruning {
            struct_ser.serialize_field("enablePruning", &self.enable_pruning)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ParquetFormat {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "enable_pruning",
            "enablePruning",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            EnablePruning,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "enablePruning" | "enable_pruning" => Ok(GeneratedField::EnablePruning),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ParquetFormat;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ParquetFormat")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ParquetFormat, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut enable_pruning__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::EnablePruning => {
                            if enable_pruning__.is_some() {
                                return Err(serde::de::Error::duplicate_field("enablePruning"));
                            }
                            enable_pruning__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ParquetFormat {
                    enable_pruning: enable_pruning__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ParquetFormat", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ParquetScanExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.base_conf.is_some() {
            len += 1;
        }
        if self.pruning_predicate.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ParquetScanExecNode", len)?;
        if let Some(v) = self.base_conf.as_ref() {
            struct_ser.serialize_field("baseConf", v)?;
        }
        if let Some(v) = self.pruning_predicate.as_ref() {
            struct_ser.serialize_field("pruningPredicate", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ParquetScanExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "base_conf",
            "baseConf",
            "pruning_predicate",
            "pruningPredicate",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            BaseConf,
            PruningPredicate,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "baseConf" | "base_conf" => Ok(GeneratedField::BaseConf),
                            "pruningPredicate" | "pruning_predicate" => Ok(GeneratedField::PruningPredicate),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ParquetScanExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ParquetScanExecNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ParquetScanExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut base_conf__ = None;
                let mut pruning_predicate__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::BaseConf => {
                            if base_conf__.is_some() {
                                return Err(serde::de::Error::duplicate_field("baseConf"));
                            }
                            base_conf__ = map.next_value()?;
                        }
                        GeneratedField::PruningPredicate => {
                            if pruning_predicate__.is_some() {
                                return Err(serde::de::Error::duplicate_field("pruningPredicate"));
                            }
                            pruning_predicate__ = map.next_value()?;
                        }
                    }
                }
                Ok(ParquetScanExecNode {
                    base_conf: base_conf__,
                    pruning_predicate: pruning_predicate__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ParquetScanExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PartialTableReference {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.schema.is_empty() {
            len += 1;
        }
        if !self.table.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PartialTableReference", len)?;
        if !self.schema.is_empty() {
            struct_ser.serialize_field("schema", &self.schema)?;
        }
        if !self.table.is_empty() {
            struct_ser.serialize_field("table", &self.table)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PartialTableReference {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "schema",
            "table",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Schema,
            Table,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "schema" => Ok(GeneratedField::Schema),
                            "table" => Ok(GeneratedField::Table),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PartialTableReference;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PartialTableReference")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PartialTableReference, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut schema__ = None;
                let mut table__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = Some(map.next_value()?);
                        }
                        GeneratedField::Table => {
                            if table__.is_some() {
                                return Err(serde::de::Error::duplicate_field("table"));
                            }
                            table__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(PartialTableReference {
                    schema: schema__.unwrap_or_default(),
                    table: table__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PartialTableReference", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PartitionId {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.job_id.is_empty() {
            len += 1;
        }
        if self.stage_id != 0 {
            len += 1;
        }
        if self.partition_id != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PartitionId", len)?;
        if !self.job_id.is_empty() {
            struct_ser.serialize_field("jobId", &self.job_id)?;
        }
        if self.stage_id != 0 {
            struct_ser.serialize_field("stageId", &self.stage_id)?;
        }
        if self.partition_id != 0 {
            struct_ser.serialize_field("partitionId", &self.partition_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PartitionId {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "job_id",
            "jobId",
            "stage_id",
            "stageId",
            "partition_id",
            "partitionId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            JobId,
            StageId,
            PartitionId,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "jobId" | "job_id" => Ok(GeneratedField::JobId),
                            "stageId" | "stage_id" => Ok(GeneratedField::StageId),
                            "partitionId" | "partition_id" => Ok(GeneratedField::PartitionId),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PartitionId;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PartitionId")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PartitionId, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut job_id__ = None;
                let mut stage_id__ = None;
                let mut partition_id__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::JobId => {
                            if job_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("jobId"));
                            }
                            job_id__ = Some(map.next_value()?);
                        }
                        GeneratedField::StageId => {
                            if stage_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("stageId"));
                            }
                            stage_id__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::PartitionId => {
                            if partition_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionId"));
                            }
                            partition_id__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(PartitionId {
                    job_id: job_id__.unwrap_or_default(),
                    stage_id: stage_id__.unwrap_or_default(),
                    partition_id: partition_id__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PartitionId", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PartitionLocation {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.map_partition_id != 0 {
            len += 1;
        }
        if self.partition_id.is_some() {
            len += 1;
        }
        if self.executor_meta.is_some() {
            len += 1;
        }
        if self.partition_stats.is_some() {
            len += 1;
        }
        if !self.path.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PartitionLocation", len)?;
        if self.map_partition_id != 0 {
            struct_ser.serialize_field("mapPartitionId", &self.map_partition_id)?;
        }
        if let Some(v) = self.partition_id.as_ref() {
            struct_ser.serialize_field("partitionId", v)?;
        }
        if let Some(v) = self.executor_meta.as_ref() {
            struct_ser.serialize_field("executorMeta", v)?;
        }
        if let Some(v) = self.partition_stats.as_ref() {
            struct_ser.serialize_field("partitionStats", v)?;
        }
        if !self.path.is_empty() {
            struct_ser.serialize_field("path", &self.path)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PartitionLocation {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "map_partition_id",
            "mapPartitionId",
            "partition_id",
            "partitionId",
            "executor_meta",
            "executorMeta",
            "partition_stats",
            "partitionStats",
            "path",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            MapPartitionId,
            PartitionId,
            ExecutorMeta,
            PartitionStats,
            Path,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "mapPartitionId" | "map_partition_id" => Ok(GeneratedField::MapPartitionId),
                            "partitionId" | "partition_id" => Ok(GeneratedField::PartitionId),
                            "executorMeta" | "executor_meta" => Ok(GeneratedField::ExecutorMeta),
                            "partitionStats" | "partition_stats" => Ok(GeneratedField::PartitionStats),
                            "path" => Ok(GeneratedField::Path),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PartitionLocation;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PartitionLocation")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PartitionLocation, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut map_partition_id__ = None;
                let mut partition_id__ = None;
                let mut executor_meta__ = None;
                let mut partition_stats__ = None;
                let mut path__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::MapPartitionId => {
                            if map_partition_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("mapPartitionId"));
                            }
                            map_partition_id__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::PartitionId => {
                            if partition_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionId"));
                            }
                            partition_id__ = map.next_value()?;
                        }
                        GeneratedField::ExecutorMeta => {
                            if executor_meta__.is_some() {
                                return Err(serde::de::Error::duplicate_field("executorMeta"));
                            }
                            executor_meta__ = map.next_value()?;
                        }
                        GeneratedField::PartitionStats => {
                            if partition_stats__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionStats"));
                            }
                            partition_stats__ = map.next_value()?;
                        }
                        GeneratedField::Path => {
                            if path__.is_some() {
                                return Err(serde::de::Error::duplicate_field("path"));
                            }
                            path__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(PartitionLocation {
                    map_partition_id: map_partition_id__.unwrap_or_default(),
                    partition_id: partition_id__,
                    executor_meta: executor_meta__,
                    partition_stats: partition_stats__,
                    path: path__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PartitionLocation", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PartitionMode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::CollectLeft => "COLLECT_LEFT",
            Self::Partitioned => "PARTITIONED",
            Self::Auto => "AUTO",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for PartitionMode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "COLLECT_LEFT",
            "PARTITIONED",
            "AUTO",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PartitionMode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(PartitionMode::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(PartitionMode::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "COLLECT_LEFT" => Ok(PartitionMode::CollectLeft),
                    "PARTITIONED" => Ok(PartitionMode::Partitioned),
                    "AUTO" => Ok(PartitionMode::Auto),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for PartitionStats {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.num_rows != 0 {
            len += 1;
        }
        if self.num_batches != 0 {
            len += 1;
        }
        if self.num_bytes != 0 {
            len += 1;
        }
        if !self.column_stats.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PartitionStats", len)?;
        if self.num_rows != 0 {
            struct_ser.serialize_field("numRows", ToString::to_string(&self.num_rows).as_str())?;
        }
        if self.num_batches != 0 {
            struct_ser.serialize_field("numBatches", ToString::to_string(&self.num_batches).as_str())?;
        }
        if self.num_bytes != 0 {
            struct_ser.serialize_field("numBytes", ToString::to_string(&self.num_bytes).as_str())?;
        }
        if !self.column_stats.is_empty() {
            struct_ser.serialize_field("columnStats", &self.column_stats)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PartitionStats {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "num_rows",
            "numRows",
            "num_batches",
            "numBatches",
            "num_bytes",
            "numBytes",
            "column_stats",
            "columnStats",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NumRows,
            NumBatches,
            NumBytes,
            ColumnStats,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "numRows" | "num_rows" => Ok(GeneratedField::NumRows),
                            "numBatches" | "num_batches" => Ok(GeneratedField::NumBatches),
                            "numBytes" | "num_bytes" => Ok(GeneratedField::NumBytes),
                            "columnStats" | "column_stats" => Ok(GeneratedField::ColumnStats),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PartitionStats;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PartitionStats")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PartitionStats, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut num_rows__ = None;
                let mut num_batches__ = None;
                let mut num_bytes__ = None;
                let mut column_stats__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::NumRows => {
                            if num_rows__.is_some() {
                                return Err(serde::de::Error::duplicate_field("numRows"));
                            }
                            num_rows__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::NumBatches => {
                            if num_batches__.is_some() {
                                return Err(serde::de::Error::duplicate_field("numBatches"));
                            }
                            num_batches__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::NumBytes => {
                            if num_bytes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("numBytes"));
                            }
                            num_bytes__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::ColumnStats => {
                            if column_stats__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnStats"));
                            }
                            column_stats__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(PartitionStats {
                    num_rows: num_rows__.unwrap_or_default(),
                    num_batches: num_batches__.unwrap_or_default(),
                    num_bytes: num_bytes__.unwrap_or_default(),
                    column_stats: column_stats__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PartitionStats", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PartitionedFile {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.path.is_empty() {
            len += 1;
        }
        if self.size != 0 {
            len += 1;
        }
        if self.last_modified_ns != 0 {
            len += 1;
        }
        if !self.partition_values.is_empty() {
            len += 1;
        }
        if self.range.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PartitionedFile", len)?;
        if !self.path.is_empty() {
            struct_ser.serialize_field("path", &self.path)?;
        }
        if self.size != 0 {
            struct_ser.serialize_field("size", ToString::to_string(&self.size).as_str())?;
        }
        if self.last_modified_ns != 0 {
            struct_ser.serialize_field("lastModifiedNs", ToString::to_string(&self.last_modified_ns).as_str())?;
        }
        if !self.partition_values.is_empty() {
            struct_ser.serialize_field("partitionValues", &self.partition_values)?;
        }
        if let Some(v) = self.range.as_ref() {
            struct_ser.serialize_field("range", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PartitionedFile {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "path",
            "size",
            "last_modified_ns",
            "lastModifiedNs",
            "partition_values",
            "partitionValues",
            "range",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Path,
            Size,
            LastModifiedNs,
            PartitionValues,
            Range,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "path" => Ok(GeneratedField::Path),
                            "size" => Ok(GeneratedField::Size),
                            "lastModifiedNs" | "last_modified_ns" => Ok(GeneratedField::LastModifiedNs),
                            "partitionValues" | "partition_values" => Ok(GeneratedField::PartitionValues),
                            "range" => Ok(GeneratedField::Range),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PartitionedFile;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PartitionedFile")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PartitionedFile, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut path__ = None;
                let mut size__ = None;
                let mut last_modified_ns__ = None;
                let mut partition_values__ = None;
                let mut range__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Path => {
                            if path__.is_some() {
                                return Err(serde::de::Error::duplicate_field("path"));
                            }
                            path__ = Some(map.next_value()?);
                        }
                        GeneratedField::Size => {
                            if size__.is_some() {
                                return Err(serde::de::Error::duplicate_field("size"));
                            }
                            size__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::LastModifiedNs => {
                            if last_modified_ns__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lastModifiedNs"));
                            }
                            last_modified_ns__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::PartitionValues => {
                            if partition_values__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionValues"));
                            }
                            partition_values__ = Some(map.next_value()?);
                        }
                        GeneratedField::Range => {
                            if range__.is_some() {
                                return Err(serde::de::Error::duplicate_field("range"));
                            }
                            range__ = map.next_value()?;
                        }
                    }
                }
                Ok(PartitionedFile {
                    path: path__.unwrap_or_default(),
                    size: size__.unwrap_or_default(),
                    last_modified_ns: last_modified_ns__.unwrap_or_default(),
                    partition_values: partition_values__.unwrap_or_default(),
                    range: range__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PartitionedFile", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalAggregateExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.aggr_function != 0 {
            len += 1;
        }
        if !self.expr.is_empty() {
            len += 1;
        }
        if self.distinct {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalAggregateExprNode", len)?;
        if self.aggr_function != 0 {
            let v = AggregateFunction::from_i32(self.aggr_function)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.aggr_function)))?;
            struct_ser.serialize_field("aggrFunction", &v)?;
        }
        if !self.expr.is_empty() {
            struct_ser.serialize_field("expr", &self.expr)?;
        }
        if self.distinct {
            struct_ser.serialize_field("distinct", &self.distinct)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalAggregateExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "aggr_function",
            "aggrFunction",
            "expr",
            "distinct",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            AggrFunction,
            Expr,
            Distinct,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "aggrFunction" | "aggr_function" => Ok(GeneratedField::AggrFunction),
                            "expr" => Ok(GeneratedField::Expr),
                            "distinct" => Ok(GeneratedField::Distinct),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalAggregateExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalAggregateExprNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PhysicalAggregateExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut aggr_function__ = None;
                let mut expr__ = None;
                let mut distinct__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::AggrFunction => {
                            if aggr_function__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggrFunction"));
                            }
                            aggr_function__ = Some(map.next_value::<AggregateFunction>()? as i32);
                        }
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = Some(map.next_value()?);
                        }
                        GeneratedField::Distinct => {
                            if distinct__.is_some() {
                                return Err(serde::de::Error::duplicate_field("distinct"));
                            }
                            distinct__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(PhysicalAggregateExprNode {
                    aggr_function: aggr_function__.unwrap_or_default(),
                    expr: expr__.unwrap_or_default(),
                    distinct: distinct__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalAggregateExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalAliasNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if !self.alias.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalAliasNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if !self.alias.is_empty() {
            struct_ser.serialize_field("alias", &self.alias)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalAliasNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "alias",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            Alias,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "alias" => Ok(GeneratedField::Alias),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalAliasNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalAliasNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PhysicalAliasNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut alias__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                        GeneratedField::Alias => {
                            if alias__.is_some() {
                                return Err(serde::de::Error::duplicate_field("alias"));
                            }
                            alias__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(PhysicalAliasNode {
                    expr: expr__,
                    alias: alias__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalAliasNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalBinaryExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.l.is_some() {
            len += 1;
        }
        if self.r.is_some() {
            len += 1;
        }
        if !self.op.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalBinaryExprNode", len)?;
        if let Some(v) = self.l.as_ref() {
            struct_ser.serialize_field("l", v)?;
        }
        if let Some(v) = self.r.as_ref() {
            struct_ser.serialize_field("r", v)?;
        }
        if !self.op.is_empty() {
            struct_ser.serialize_field("op", &self.op)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalBinaryExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "l",
            "r",
            "op",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            L,
            R,
            Op,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "l" => Ok(GeneratedField::L),
                            "r" => Ok(GeneratedField::R),
                            "op" => Ok(GeneratedField::Op),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalBinaryExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalBinaryExprNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PhysicalBinaryExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut l__ = None;
                let mut r__ = None;
                let mut op__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::L => {
                            if l__.is_some() {
                                return Err(serde::de::Error::duplicate_field("l"));
                            }
                            l__ = map.next_value()?;
                        }
                        GeneratedField::R => {
                            if r__.is_some() {
                                return Err(serde::de::Error::duplicate_field("r"));
                            }
                            r__ = map.next_value()?;
                        }
                        GeneratedField::Op => {
                            if op__.is_some() {
                                return Err(serde::de::Error::duplicate_field("op"));
                            }
                            op__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(PhysicalBinaryExprNode {
                    l: l__,
                    r: r__,
                    op: op__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalBinaryExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalCaseNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if !self.when_then_expr.is_empty() {
            len += 1;
        }
        if self.else_expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalCaseNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if !self.when_then_expr.is_empty() {
            struct_ser.serialize_field("whenThenExpr", &self.when_then_expr)?;
        }
        if let Some(v) = self.else_expr.as_ref() {
            struct_ser.serialize_field("elseExpr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalCaseNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "when_then_expr",
            "whenThenExpr",
            "else_expr",
            "elseExpr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            WhenThenExpr,
            ElseExpr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "whenThenExpr" | "when_then_expr" => Ok(GeneratedField::WhenThenExpr),
                            "elseExpr" | "else_expr" => Ok(GeneratedField::ElseExpr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalCaseNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalCaseNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PhysicalCaseNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut when_then_expr__ = None;
                let mut else_expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                        GeneratedField::WhenThenExpr => {
                            if when_then_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("whenThenExpr"));
                            }
                            when_then_expr__ = Some(map.next_value()?);
                        }
                        GeneratedField::ElseExpr => {
                            if else_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("elseExpr"));
                            }
                            else_expr__ = map.next_value()?;
                        }
                    }
                }
                Ok(PhysicalCaseNode {
                    expr: expr__,
                    when_then_expr: when_then_expr__.unwrap_or_default(),
                    else_expr: else_expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalCaseNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalCastNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if self.arrow_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalCastNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if let Some(v) = self.arrow_type.as_ref() {
            struct_ser.serialize_field("arrowType", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalCastNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "arrow_type",
            "arrowType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            ArrowType,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "arrowType" | "arrow_type" => Ok(GeneratedField::ArrowType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalCastNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalCastNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PhysicalCastNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut arrow_type__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                        GeneratedField::ArrowType => {
                            if arrow_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("arrowType"));
                            }
                            arrow_type__ = map.next_value()?;
                        }
                    }
                }
                Ok(PhysicalCastNode {
                    expr: expr__,
                    arrow_type: arrow_type__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalCastNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalColumn {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.name.is_empty() {
            len += 1;
        }
        if self.index != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalColumn", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if self.index != 0 {
            struct_ser.serialize_field("index", &self.index)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalColumn {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "index",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Index,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "index" => Ok(GeneratedField::Index),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalColumn;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalColumn")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PhysicalColumn, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut index__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map.next_value()?);
                        }
                        GeneratedField::Index => {
                            if index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("index"));
                            }
                            index__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(PhysicalColumn {
                    name: name__.unwrap_or_default(),
                    index: index__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalColumn", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalDateTimeIntervalExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.l.is_some() {
            len += 1;
        }
        if self.r.is_some() {
            len += 1;
        }
        if !self.op.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalDateTimeIntervalExprNode", len)?;
        if let Some(v) = self.l.as_ref() {
            struct_ser.serialize_field("l", v)?;
        }
        if let Some(v) = self.r.as_ref() {
            struct_ser.serialize_field("r", v)?;
        }
        if !self.op.is_empty() {
            struct_ser.serialize_field("op", &self.op)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalDateTimeIntervalExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "l",
            "r",
            "op",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            L,
            R,
            Op,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "l" => Ok(GeneratedField::L),
                            "r" => Ok(GeneratedField::R),
                            "op" => Ok(GeneratedField::Op),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalDateTimeIntervalExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalDateTimeIntervalExprNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PhysicalDateTimeIntervalExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut l__ = None;
                let mut r__ = None;
                let mut op__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::L => {
                            if l__.is_some() {
                                return Err(serde::de::Error::duplicate_field("l"));
                            }
                            l__ = map.next_value()?;
                        }
                        GeneratedField::R => {
                            if r__.is_some() {
                                return Err(serde::de::Error::duplicate_field("r"));
                            }
                            r__ = map.next_value()?;
                        }
                        GeneratedField::Op => {
                            if op__.is_some() {
                                return Err(serde::de::Error::duplicate_field("op"));
                            }
                            op__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(PhysicalDateTimeIntervalExprNode {
                    l: l__,
                    r: r__,
                    op: op__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalDateTimeIntervalExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalExprNode", len)?;
        if let Some(v) = self.expr_type.as_ref() {
            match v {
                physical_expr_node::ExprType::Column(v) => {
                    struct_ser.serialize_field("column", v)?;
                }
                physical_expr_node::ExprType::Literal(v) => {
                    struct_ser.serialize_field("literal", v)?;
                }
                physical_expr_node::ExprType::BinaryExpr(v) => {
                    struct_ser.serialize_field("binaryExpr", v)?;
                }
                physical_expr_node::ExprType::AggregateExpr(v) => {
                    struct_ser.serialize_field("aggregateExpr", v)?;
                }
                physical_expr_node::ExprType::IsNullExpr(v) => {
                    struct_ser.serialize_field("isNullExpr", v)?;
                }
                physical_expr_node::ExprType::IsNotNullExpr(v) => {
                    struct_ser.serialize_field("isNotNullExpr", v)?;
                }
                physical_expr_node::ExprType::NotExpr(v) => {
                    struct_ser.serialize_field("notExpr", v)?;
                }
                physical_expr_node::ExprType::Case(v) => {
                    struct_ser.serialize_field("case", v)?;
                }
                physical_expr_node::ExprType::Cast(v) => {
                    struct_ser.serialize_field("cast", v)?;
                }
                physical_expr_node::ExprType::Sort(v) => {
                    struct_ser.serialize_field("sort", v)?;
                }
                physical_expr_node::ExprType::Negative(v) => {
                    struct_ser.serialize_field("negative", v)?;
                }
                physical_expr_node::ExprType::InList(v) => {
                    struct_ser.serialize_field("inList", v)?;
                }
                physical_expr_node::ExprType::ScalarFunction(v) => {
                    struct_ser.serialize_field("scalarFunction", v)?;
                }
                physical_expr_node::ExprType::TryCast(v) => {
                    struct_ser.serialize_field("tryCast", v)?;
                }
                physical_expr_node::ExprType::WindowExpr(v) => {
                    struct_ser.serialize_field("windowExpr", v)?;
                }
                physical_expr_node::ExprType::ScalarUdf(v) => {
                    struct_ser.serialize_field("scalarUdf", v)?;
                }
                physical_expr_node::ExprType::DateTimeIntervalExpr(v) => {
                    struct_ser.serialize_field("dateTimeIntervalExpr", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "column",
            "literal",
            "binary_expr",
            "binaryExpr",
            "aggregate_expr",
            "aggregateExpr",
            "is_null_expr",
            "isNullExpr",
            "is_not_null_expr",
            "isNotNullExpr",
            "not_expr",
            "notExpr",
            "case_",
            "case",
            "cast",
            "sort",
            "negative",
            "in_list",
            "inList",
            "scalar_function",
            "scalarFunction",
            "try_cast",
            "tryCast",
            "window_expr",
            "windowExpr",
            "scalar_udf",
            "scalarUdf",
            "date_time_interval_expr",
            "dateTimeIntervalExpr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Column,
            Literal,
            BinaryExpr,
            AggregateExpr,
            IsNullExpr,
            IsNotNullExpr,
            NotExpr,
            Case,
            Cast,
            Sort,
            Negative,
            InList,
            ScalarFunction,
            TryCast,
            WindowExpr,
            ScalarUdf,
            DateTimeIntervalExpr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "column" => Ok(GeneratedField::Column),
                            "literal" => Ok(GeneratedField::Literal),
                            "binaryExpr" | "binary_expr" => Ok(GeneratedField::BinaryExpr),
                            "aggregateExpr" | "aggregate_expr" => Ok(GeneratedField::AggregateExpr),
                            "isNullExpr" | "is_null_expr" => Ok(GeneratedField::IsNullExpr),
                            "isNotNullExpr" | "is_not_null_expr" => Ok(GeneratedField::IsNotNullExpr),
                            "notExpr" | "not_expr" => Ok(GeneratedField::NotExpr),
                            "case" | "case_" => Ok(GeneratedField::Case),
                            "cast" => Ok(GeneratedField::Cast),
                            "sort" => Ok(GeneratedField::Sort),
                            "negative" => Ok(GeneratedField::Negative),
                            "inList" | "in_list" => Ok(GeneratedField::InList),
                            "scalarFunction" | "scalar_function" => Ok(GeneratedField::ScalarFunction),
                            "tryCast" | "try_cast" => Ok(GeneratedField::TryCast),
                            "windowExpr" | "window_expr" => Ok(GeneratedField::WindowExpr),
                            "scalarUdf" | "scalar_udf" => Ok(GeneratedField::ScalarUdf),
                            "dateTimeIntervalExpr" | "date_time_interval_expr" => Ok(GeneratedField::DateTimeIntervalExpr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalExprNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PhysicalExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr_type__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Column => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("column"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::Column)
;
                        }
                        GeneratedField::Literal => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("literal"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::Literal)
;
                        }
                        GeneratedField::BinaryExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("binaryExpr"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::BinaryExpr)
;
                        }
                        GeneratedField::AggregateExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggregateExpr"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::AggregateExpr)
;
                        }
                        GeneratedField::IsNullExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isNullExpr"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::IsNullExpr)
;
                        }
                        GeneratedField::IsNotNullExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isNotNullExpr"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::IsNotNullExpr)
;
                        }
                        GeneratedField::NotExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("notExpr"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::NotExpr)
;
                        }
                        GeneratedField::Case => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("case"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::Case)
;
                        }
                        GeneratedField::Cast => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("cast"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::Cast)
;
                        }
                        GeneratedField::Sort => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sort"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::Sort)
;
                        }
                        GeneratedField::Negative => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("negative"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::Negative)
;
                        }
                        GeneratedField::InList => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("inList"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::InList)
;
                        }
                        GeneratedField::ScalarFunction => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("scalarFunction"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::ScalarFunction)
;
                        }
                        GeneratedField::TryCast => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tryCast"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::TryCast)
;
                        }
                        GeneratedField::WindowExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("windowExpr"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::WindowExpr)
;
                        }
                        GeneratedField::ScalarUdf => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("scalarUdf"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::ScalarUdf)
;
                        }
                        GeneratedField::DateTimeIntervalExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dateTimeIntervalExpr"));
                            }
                            expr_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::DateTimeIntervalExpr)
;
                        }
                    }
                }
                Ok(PhysicalExprNode {
                    expr_type: expr_type__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalExtensionNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.node.is_empty() {
            len += 1;
        }
        if !self.inputs.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalExtensionNode", len)?;
        if !self.node.is_empty() {
            struct_ser.serialize_field("node", pbjson::private::base64::encode(&self.node).as_str())?;
        }
        if !self.inputs.is_empty() {
            struct_ser.serialize_field("inputs", &self.inputs)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalExtensionNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "node",
            "inputs",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Node,
            Inputs,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "node" => Ok(GeneratedField::Node),
                            "inputs" => Ok(GeneratedField::Inputs),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalExtensionNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalExtensionNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PhysicalExtensionNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node__ = None;
                let mut inputs__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Node => {
                            if node__.is_some() {
                                return Err(serde::de::Error::duplicate_field("node"));
                            }
                            node__ = 
                                Some(map.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Inputs => {
                            if inputs__.is_some() {
                                return Err(serde::de::Error::duplicate_field("inputs"));
                            }
                            inputs__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(PhysicalExtensionNode {
                    node: node__.unwrap_or_default(),
                    inputs: inputs__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalExtensionNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalHashRepartition {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.hash_expr.is_empty() {
            len += 1;
        }
        if self.partition_count != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalHashRepartition", len)?;
        if !self.hash_expr.is_empty() {
            struct_ser.serialize_field("hashExpr", &self.hash_expr)?;
        }
        if self.partition_count != 0 {
            struct_ser.serialize_field("partitionCount", ToString::to_string(&self.partition_count).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalHashRepartition {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "hash_expr",
            "hashExpr",
            "partition_count",
            "partitionCount",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            HashExpr,
            PartitionCount,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "hashExpr" | "hash_expr" => Ok(GeneratedField::HashExpr),
                            "partitionCount" | "partition_count" => Ok(GeneratedField::PartitionCount),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalHashRepartition;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalHashRepartition")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PhysicalHashRepartition, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut hash_expr__ = None;
                let mut partition_count__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::HashExpr => {
                            if hash_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hashExpr"));
                            }
                            hash_expr__ = Some(map.next_value()?);
                        }
                        GeneratedField::PartitionCount => {
                            if partition_count__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionCount"));
                            }
                            partition_count__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(PhysicalHashRepartition {
                    hash_expr: hash_expr__.unwrap_or_default(),
                    partition_count: partition_count__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalHashRepartition", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalInListNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if !self.list.is_empty() {
            len += 1;
        }
        if self.negated {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalInListNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if !self.list.is_empty() {
            struct_ser.serialize_field("list", &self.list)?;
        }
        if self.negated {
            struct_ser.serialize_field("negated", &self.negated)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalInListNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "list",
            "negated",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            List,
            Negated,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "list" => Ok(GeneratedField::List),
                            "negated" => Ok(GeneratedField::Negated),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalInListNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalInListNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PhysicalInListNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut list__ = None;
                let mut negated__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                        GeneratedField::List => {
                            if list__.is_some() {
                                return Err(serde::de::Error::duplicate_field("list"));
                            }
                            list__ = Some(map.next_value()?);
                        }
                        GeneratedField::Negated => {
                            if negated__.is_some() {
                                return Err(serde::de::Error::duplicate_field("negated"));
                            }
                            negated__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(PhysicalInListNode {
                    expr: expr__,
                    list: list__.unwrap_or_default(),
                    negated: negated__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalInListNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalIsNotNull {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalIsNotNull", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalIsNotNull {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalIsNotNull;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalIsNotNull")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PhysicalIsNotNull, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                    }
                }
                Ok(PhysicalIsNotNull {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalIsNotNull", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalIsNull {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalIsNull", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalIsNull {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalIsNull;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalIsNull")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PhysicalIsNull, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                    }
                }
                Ok(PhysicalIsNull {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalIsNull", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalNegativeNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalNegativeNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalNegativeNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalNegativeNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalNegativeNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PhysicalNegativeNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                    }
                }
                Ok(PhysicalNegativeNode {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalNegativeNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalNot {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalNot", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalNot {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalNot;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalNot")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PhysicalNot, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                    }
                }
                Ok(PhysicalNot {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalNot", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalPlanNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.physical_plan_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalPlanNode", len)?;
        if let Some(v) = self.physical_plan_type.as_ref() {
            match v {
                physical_plan_node::PhysicalPlanType::ParquetScan(v) => {
                    struct_ser.serialize_field("parquetScan", v)?;
                }
                physical_plan_node::PhysicalPlanType::CsvScan(v) => {
                    struct_ser.serialize_field("csvScan", v)?;
                }
                physical_plan_node::PhysicalPlanType::Empty(v) => {
                    struct_ser.serialize_field("empty", v)?;
                }
                physical_plan_node::PhysicalPlanType::Projection(v) => {
                    struct_ser.serialize_field("projection", v)?;
                }
                physical_plan_node::PhysicalPlanType::GlobalLimit(v) => {
                    struct_ser.serialize_field("globalLimit", v)?;
                }
                physical_plan_node::PhysicalPlanType::LocalLimit(v) => {
                    struct_ser.serialize_field("localLimit", v)?;
                }
                physical_plan_node::PhysicalPlanType::Aggregate(v) => {
                    struct_ser.serialize_field("aggregate", v)?;
                }
                physical_plan_node::PhysicalPlanType::HashJoin(v) => {
                    struct_ser.serialize_field("hashJoin", v)?;
                }
                physical_plan_node::PhysicalPlanType::Sort(v) => {
                    struct_ser.serialize_field("sort", v)?;
                }
                physical_plan_node::PhysicalPlanType::CoalesceBatches(v) => {
                    struct_ser.serialize_field("coalesceBatches", v)?;
                }
                physical_plan_node::PhysicalPlanType::Filter(v) => {
                    struct_ser.serialize_field("filter", v)?;
                }
                physical_plan_node::PhysicalPlanType::Merge(v) => {
                    struct_ser.serialize_field("merge", v)?;
                }
                physical_plan_node::PhysicalPlanType::Repartition(v) => {
                    struct_ser.serialize_field("repartition", v)?;
                }
                physical_plan_node::PhysicalPlanType::Window(v) => {
                    struct_ser.serialize_field("window", v)?;
                }
                physical_plan_node::PhysicalPlanType::CrossJoin(v) => {
                    struct_ser.serialize_field("crossJoin", v)?;
                }
                physical_plan_node::PhysicalPlanType::AvroScan(v) => {
                    struct_ser.serialize_field("avroScan", v)?;
                }
                physical_plan_node::PhysicalPlanType::Extension(v) => {
                    struct_ser.serialize_field("extension", v)?;
                }
                physical_plan_node::PhysicalPlanType::Union(v) => {
                    struct_ser.serialize_field("union", v)?;
                }
                physical_plan_node::PhysicalPlanType::Explain(v) => {
                    struct_ser.serialize_field("explain", v)?;
                }
                physical_plan_node::PhysicalPlanType::SortPreservingMerge(v) => {
                    struct_ser.serialize_field("sortPreservingMerge", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalPlanNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "parquet_scan",
            "parquetScan",
            "csv_scan",
            "csvScan",
            "empty",
            "projection",
            "global_limit",
            "globalLimit",
            "local_limit",
            "localLimit",
            "aggregate",
            "hash_join",
            "hashJoin",
            "sort",
            "coalesce_batches",
            "coalesceBatches",
            "filter",
            "merge",
            "repartition",
            "window",
            "cross_join",
            "crossJoin",
            "avro_scan",
            "avroScan",
            "extension",
            "union",
            "explain",
            "sort_preserving_merge",
            "sortPreservingMerge",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ParquetScan,
            CsvScan,
            Empty,
            Projection,
            GlobalLimit,
            LocalLimit,
            Aggregate,
            HashJoin,
            Sort,
            CoalesceBatches,
            Filter,
            Merge,
            Repartition,
            Window,
            CrossJoin,
            AvroScan,
            Extension,
            Union,
            Explain,
            SortPreservingMerge,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "parquetScan" | "parquet_scan" => Ok(GeneratedField::ParquetScan),
                            "csvScan" | "csv_scan" => Ok(GeneratedField::CsvScan),
                            "empty" => Ok(GeneratedField::Empty),
                            "projection" => Ok(GeneratedField::Projection),
                            "globalLimit" | "global_limit" => Ok(GeneratedField::GlobalLimit),
                            "localLimit" | "local_limit" => Ok(GeneratedField::LocalLimit),
                            "aggregate" => Ok(GeneratedField::Aggregate),
                            "hashJoin" | "hash_join" => Ok(GeneratedField::HashJoin),
                            "sort" => Ok(GeneratedField::Sort),
                            "coalesceBatches" | "coalesce_batches" => Ok(GeneratedField::CoalesceBatches),
                            "filter" => Ok(GeneratedField::Filter),
                            "merge" => Ok(GeneratedField::Merge),
                            "repartition" => Ok(GeneratedField::Repartition),
                            "window" => Ok(GeneratedField::Window),
                            "crossJoin" | "cross_join" => Ok(GeneratedField::CrossJoin),
                            "avroScan" | "avro_scan" => Ok(GeneratedField::AvroScan),
                            "extension" => Ok(GeneratedField::Extension),
                            "union" => Ok(GeneratedField::Union),
                            "explain" => Ok(GeneratedField::Explain),
                            "sortPreservingMerge" | "sort_preserving_merge" => Ok(GeneratedField::SortPreservingMerge),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalPlanNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalPlanNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PhysicalPlanNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut physical_plan_type__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ParquetScan => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("parquetScan"));
                            }
                            physical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::ParquetScan)
;
                        }
                        GeneratedField::CsvScan => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("csvScan"));
                            }
                            physical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::CsvScan)
;
                        }
                        GeneratedField::Empty => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("empty"));
                            }
                            physical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::Empty)
;
                        }
                        GeneratedField::Projection => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("projection"));
                            }
                            physical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::Projection)
;
                        }
                        GeneratedField::GlobalLimit => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("globalLimit"));
                            }
                            physical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::GlobalLimit)
;
                        }
                        GeneratedField::LocalLimit => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("localLimit"));
                            }
                            physical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::LocalLimit)
;
                        }
                        GeneratedField::Aggregate => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggregate"));
                            }
                            physical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::Aggregate)
;
                        }
                        GeneratedField::HashJoin => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hashJoin"));
                            }
                            physical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::HashJoin)
;
                        }
                        GeneratedField::Sort => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sort"));
                            }
                            physical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::Sort)
;
                        }
                        GeneratedField::CoalesceBatches => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("coalesceBatches"));
                            }
                            physical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::CoalesceBatches)
;
                        }
                        GeneratedField::Filter => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("filter"));
                            }
                            physical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::Filter)
;
                        }
                        GeneratedField::Merge => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("merge"));
                            }
                            physical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::Merge)
;
                        }
                        GeneratedField::Repartition => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("repartition"));
                            }
                            physical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::Repartition)
;
                        }
                        GeneratedField::Window => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("window"));
                            }
                            physical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::Window)
;
                        }
                        GeneratedField::CrossJoin => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("crossJoin"));
                            }
                            physical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::CrossJoin)
;
                        }
                        GeneratedField::AvroScan => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("avroScan"));
                            }
                            physical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::AvroScan)
;
                        }
                        GeneratedField::Extension => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("extension"));
                            }
                            physical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::Extension)
;
                        }
                        GeneratedField::Union => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("union"));
                            }
                            physical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::Union)
;
                        }
                        GeneratedField::Explain => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("explain"));
                            }
                            physical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::Explain)
;
                        }
                        GeneratedField::SortPreservingMerge => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sortPreservingMerge"));
                            }
                            physical_plan_type__ = map.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::SortPreservingMerge)
;
                        }
                    }
                }
                Ok(PhysicalPlanNode {
                    physical_plan_type: physical_plan_type__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalPlanNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalScalarFunctionNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.name.is_empty() {
            len += 1;
        }
        if self.fun != 0 {
            len += 1;
        }
        if !self.args.is_empty() {
            len += 1;
        }
        if self.return_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalScalarFunctionNode", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if self.fun != 0 {
            let v = ScalarFunction::from_i32(self.fun)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.fun)))?;
            struct_ser.serialize_field("fun", &v)?;
        }
        if !self.args.is_empty() {
            struct_ser.serialize_field("args", &self.args)?;
        }
        if let Some(v) = self.return_type.as_ref() {
            struct_ser.serialize_field("returnType", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalScalarFunctionNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "fun",
            "args",
            "return_type",
            "returnType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Fun,
            Args,
            ReturnType,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "fun" => Ok(GeneratedField::Fun),
                            "args" => Ok(GeneratedField::Args),
                            "returnType" | "return_type" => Ok(GeneratedField::ReturnType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalScalarFunctionNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalScalarFunctionNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PhysicalScalarFunctionNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut fun__ = None;
                let mut args__ = None;
                let mut return_type__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map.next_value()?);
                        }
                        GeneratedField::Fun => {
                            if fun__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fun"));
                            }
                            fun__ = Some(map.next_value::<ScalarFunction>()? as i32);
                        }
                        GeneratedField::Args => {
                            if args__.is_some() {
                                return Err(serde::de::Error::duplicate_field("args"));
                            }
                            args__ = Some(map.next_value()?);
                        }
                        GeneratedField::ReturnType => {
                            if return_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("returnType"));
                            }
                            return_type__ = map.next_value()?;
                        }
                    }
                }
                Ok(PhysicalScalarFunctionNode {
                    name: name__.unwrap_or_default(),
                    fun: fun__.unwrap_or_default(),
                    args: args__.unwrap_or_default(),
                    return_type: return_type__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalScalarFunctionNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalScalarUdfNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.name.is_empty() {
            len += 1;
        }
        if !self.args.is_empty() {
            len += 1;
        }
        if self.return_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalScalarUdfNode", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if !self.args.is_empty() {
            struct_ser.serialize_field("args", &self.args)?;
        }
        if let Some(v) = self.return_type.as_ref() {
            struct_ser.serialize_field("returnType", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalScalarUdfNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "args",
            "return_type",
            "returnType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Args,
            ReturnType,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "args" => Ok(GeneratedField::Args),
                            "returnType" | "return_type" => Ok(GeneratedField::ReturnType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalScalarUdfNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalScalarUdfNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PhysicalScalarUdfNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut args__ = None;
                let mut return_type__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map.next_value()?);
                        }
                        GeneratedField::Args => {
                            if args__.is_some() {
                                return Err(serde::de::Error::duplicate_field("args"));
                            }
                            args__ = Some(map.next_value()?);
                        }
                        GeneratedField::ReturnType => {
                            if return_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("returnType"));
                            }
                            return_type__ = map.next_value()?;
                        }
                    }
                }
                Ok(PhysicalScalarUdfNode {
                    name: name__.unwrap_or_default(),
                    args: args__.unwrap_or_default(),
                    return_type: return_type__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalScalarUdfNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalSortExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if self.asc {
            len += 1;
        }
        if self.nulls_first {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalSortExprNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if self.asc {
            struct_ser.serialize_field("asc", &self.asc)?;
        }
        if self.nulls_first {
            struct_ser.serialize_field("nullsFirst", &self.nulls_first)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalSortExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "asc",
            "nulls_first",
            "nullsFirst",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            Asc,
            NullsFirst,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "asc" => Ok(GeneratedField::Asc),
                            "nullsFirst" | "nulls_first" => Ok(GeneratedField::NullsFirst),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalSortExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalSortExprNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PhysicalSortExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut asc__ = None;
                let mut nulls_first__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                        GeneratedField::Asc => {
                            if asc__.is_some() {
                                return Err(serde::de::Error::duplicate_field("asc"));
                            }
                            asc__ = Some(map.next_value()?);
                        }
                        GeneratedField::NullsFirst => {
                            if nulls_first__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nullsFirst"));
                            }
                            nulls_first__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(PhysicalSortExprNode {
                    expr: expr__,
                    asc: asc__.unwrap_or_default(),
                    nulls_first: nulls_first__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalSortExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalTryCastNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if self.arrow_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalTryCastNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if let Some(v) = self.arrow_type.as_ref() {
            struct_ser.serialize_field("arrowType", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalTryCastNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "arrow_type",
            "arrowType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            ArrowType,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "arrowType" | "arrow_type" => Ok(GeneratedField::ArrowType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalTryCastNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalTryCastNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PhysicalTryCastNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut arrow_type__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                        GeneratedField::ArrowType => {
                            if arrow_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("arrowType"));
                            }
                            arrow_type__ = map.next_value()?;
                        }
                    }
                }
                Ok(PhysicalTryCastNode {
                    expr: expr__,
                    arrow_type: arrow_type__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalTryCastNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalWhenThen {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.when_expr.is_some() {
            len += 1;
        }
        if self.then_expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalWhenThen", len)?;
        if let Some(v) = self.when_expr.as_ref() {
            struct_ser.serialize_field("whenExpr", v)?;
        }
        if let Some(v) = self.then_expr.as_ref() {
            struct_ser.serialize_field("thenExpr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalWhenThen {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "when_expr",
            "whenExpr",
            "then_expr",
            "thenExpr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            WhenExpr,
            ThenExpr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "whenExpr" | "when_expr" => Ok(GeneratedField::WhenExpr),
                            "thenExpr" | "then_expr" => Ok(GeneratedField::ThenExpr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalWhenThen;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalWhenThen")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PhysicalWhenThen, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut when_expr__ = None;
                let mut then_expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::WhenExpr => {
                            if when_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("whenExpr"));
                            }
                            when_expr__ = map.next_value()?;
                        }
                        GeneratedField::ThenExpr => {
                            if then_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("thenExpr"));
                            }
                            then_expr__ = map.next_value()?;
                        }
                    }
                }
                Ok(PhysicalWhenThen {
                    when_expr: when_expr__,
                    then_expr: then_expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalWhenThen", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalWindowExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if self.window_function.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalWindowExprNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if let Some(v) = self.window_function.as_ref() {
            match v {
                physical_window_expr_node::WindowFunction::AggrFunction(v) => {
                    let v = AggregateFunction::from_i32(*v)
                        .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
                    struct_ser.serialize_field("aggrFunction", &v)?;
                }
                physical_window_expr_node::WindowFunction::BuiltInFunction(v) => {
                    let v = BuiltInWindowFunction::from_i32(*v)
                        .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
                    struct_ser.serialize_field("builtInFunction", &v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalWindowExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "aggr_function",
            "aggrFunction",
            "built_in_function",
            "builtInFunction",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            AggrFunction,
            BuiltInFunction,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "aggrFunction" | "aggr_function" => Ok(GeneratedField::AggrFunction),
                            "builtInFunction" | "built_in_function" => Ok(GeneratedField::BuiltInFunction),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalWindowExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalWindowExprNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PhysicalWindowExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut window_function__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                        GeneratedField::AggrFunction => {
                            if window_function__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggrFunction"));
                            }
                            window_function__ = map.next_value::<::std::option::Option<AggregateFunction>>()?.map(|x| physical_window_expr_node::WindowFunction::AggrFunction(x as i32));
                        }
                        GeneratedField::BuiltInFunction => {
                            if window_function__.is_some() {
                                return Err(serde::de::Error::duplicate_field("builtInFunction"));
                            }
                            window_function__ = map.next_value::<::std::option::Option<BuiltInWindowFunction>>()?.map(|x| physical_window_expr_node::WindowFunction::BuiltInFunction(x as i32));
                        }
                    }
                }
                Ok(PhysicalWindowExprNode {
                    expr: expr__,
                    window_function: window_function__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalWindowExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PlaceholderNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.id.is_empty() {
            len += 1;
        }
        if self.data_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PlaceholderNode", len)?;
        if !self.id.is_empty() {
            struct_ser.serialize_field("id", &self.id)?;
        }
        if let Some(v) = self.data_type.as_ref() {
            struct_ser.serialize_field("dataType", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PlaceholderNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "id",
            "data_type",
            "dataType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Id,
            DataType,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "id" => Ok(GeneratedField::Id),
                            "dataType" | "data_type" => Ok(GeneratedField::DataType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PlaceholderNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PlaceholderNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PlaceholderNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut id__ = None;
                let mut data_type__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Id => {
                            if id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id__ = Some(map.next_value()?);
                        }
                        GeneratedField::DataType => {
                            if data_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dataType"));
                            }
                            data_type__ = map.next_value()?;
                        }
                    }
                }
                Ok(PlaceholderNode {
                    id: id__.unwrap_or_default(),
                    data_type: data_type__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PlaceholderNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PlanType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.plan_type_enum.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PlanType", len)?;
        if let Some(v) = self.plan_type_enum.as_ref() {
            match v {
                plan_type::PlanTypeEnum::InitialLogicalPlan(v) => {
                    struct_ser.serialize_field("InitialLogicalPlan", v)?;
                }
                plan_type::PlanTypeEnum::OptimizedLogicalPlan(v) => {
                    struct_ser.serialize_field("OptimizedLogicalPlan", v)?;
                }
                plan_type::PlanTypeEnum::FinalLogicalPlan(v) => {
                    struct_ser.serialize_field("FinalLogicalPlan", v)?;
                }
                plan_type::PlanTypeEnum::InitialPhysicalPlan(v) => {
                    struct_ser.serialize_field("InitialPhysicalPlan", v)?;
                }
                plan_type::PlanTypeEnum::OptimizedPhysicalPlan(v) => {
                    struct_ser.serialize_field("OptimizedPhysicalPlan", v)?;
                }
                plan_type::PlanTypeEnum::FinalPhysicalPlan(v) => {
                    struct_ser.serialize_field("FinalPhysicalPlan", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PlanType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "InitialLogicalPlan",
            "OptimizedLogicalPlan",
            "FinalLogicalPlan",
            "InitialPhysicalPlan",
            "OptimizedPhysicalPlan",
            "FinalPhysicalPlan",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            InitialLogicalPlan,
            OptimizedLogicalPlan,
            FinalLogicalPlan,
            InitialPhysicalPlan,
            OptimizedPhysicalPlan,
            FinalPhysicalPlan,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "InitialLogicalPlan" => Ok(GeneratedField::InitialLogicalPlan),
                            "OptimizedLogicalPlan" => Ok(GeneratedField::OptimizedLogicalPlan),
                            "FinalLogicalPlan" => Ok(GeneratedField::FinalLogicalPlan),
                            "InitialPhysicalPlan" => Ok(GeneratedField::InitialPhysicalPlan),
                            "OptimizedPhysicalPlan" => Ok(GeneratedField::OptimizedPhysicalPlan),
                            "FinalPhysicalPlan" => Ok(GeneratedField::FinalPhysicalPlan),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PlanType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PlanType")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PlanType, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut plan_type_enum__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::InitialLogicalPlan => {
                            if plan_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("InitialLogicalPlan"));
                            }
                            plan_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(plan_type::PlanTypeEnum::InitialLogicalPlan)
;
                        }
                        GeneratedField::OptimizedLogicalPlan => {
                            if plan_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("OptimizedLogicalPlan"));
                            }
                            plan_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(plan_type::PlanTypeEnum::OptimizedLogicalPlan)
;
                        }
                        GeneratedField::FinalLogicalPlan => {
                            if plan_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("FinalLogicalPlan"));
                            }
                            plan_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(plan_type::PlanTypeEnum::FinalLogicalPlan)
;
                        }
                        GeneratedField::InitialPhysicalPlan => {
                            if plan_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("InitialPhysicalPlan"));
                            }
                            plan_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(plan_type::PlanTypeEnum::InitialPhysicalPlan)
;
                        }
                        GeneratedField::OptimizedPhysicalPlan => {
                            if plan_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("OptimizedPhysicalPlan"));
                            }
                            plan_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(plan_type::PlanTypeEnum::OptimizedPhysicalPlan)
;
                        }
                        GeneratedField::FinalPhysicalPlan => {
                            if plan_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("FinalPhysicalPlan"));
                            }
                            plan_type_enum__ = map.next_value::<::std::option::Option<_>>()?.map(plan_type::PlanTypeEnum::FinalPhysicalPlan)
;
                        }
                    }
                }
                Ok(PlanType {
                    plan_type_enum: plan_type_enum__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PlanType", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PrepareNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.name.is_empty() {
            len += 1;
        }
        if !self.data_types.is_empty() {
            len += 1;
        }
        if self.input.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PrepareNode", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if !self.data_types.is_empty() {
            struct_ser.serialize_field("dataTypes", &self.data_types)?;
        }
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PrepareNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "data_types",
            "dataTypes",
            "input",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            DataTypes,
            Input,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "dataTypes" | "data_types" => Ok(GeneratedField::DataTypes),
                            "input" => Ok(GeneratedField::Input),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PrepareNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PrepareNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PrepareNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut data_types__ = None;
                let mut input__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map.next_value()?);
                        }
                        GeneratedField::DataTypes => {
                            if data_types__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dataTypes"));
                            }
                            data_types__ = Some(map.next_value()?);
                        }
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map.next_value()?;
                        }
                    }
                }
                Ok(PrepareNode {
                    name: name__.unwrap_or_default(),
                    data_types: data_types__.unwrap_or_default(),
                    input: input__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PrepareNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ProjectionColumns {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.columns.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ProjectionColumns", len)?;
        if !self.columns.is_empty() {
            struct_ser.serialize_field("columns", &self.columns)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ProjectionColumns {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "columns",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Columns,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "columns" => Ok(GeneratedField::Columns),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ProjectionColumns;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ProjectionColumns")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ProjectionColumns, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut columns__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Columns => {
                            if columns__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columns"));
                            }
                            columns__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ProjectionColumns {
                    columns: columns__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ProjectionColumns", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ProjectionExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if !self.expr.is_empty() {
            len += 1;
        }
        if !self.expr_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ProjectionExecNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if !self.expr.is_empty() {
            struct_ser.serialize_field("expr", &self.expr)?;
        }
        if !self.expr_name.is_empty() {
            struct_ser.serialize_field("exprName", &self.expr_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ProjectionExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "expr",
            "expr_name",
            "exprName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Expr,
            ExprName,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "expr" => Ok(GeneratedField::Expr),
                            "exprName" | "expr_name" => Ok(GeneratedField::ExprName),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ProjectionExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ProjectionExecNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ProjectionExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut expr__ = None;
                let mut expr_name__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map.next_value()?;
                        }
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = Some(map.next_value()?);
                        }
                        GeneratedField::ExprName => {
                            if expr_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("exprName"));
                            }
                            expr_name__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ProjectionExecNode {
                    input: input__,
                    expr: expr__.unwrap_or_default(),
                    expr_name: expr_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ProjectionExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ProjectionNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if !self.expr.is_empty() {
            len += 1;
        }
        if self.optional_alias.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ProjectionNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if !self.expr.is_empty() {
            struct_ser.serialize_field("expr", &self.expr)?;
        }
        if let Some(v) = self.optional_alias.as_ref() {
            match v {
                projection_node::OptionalAlias::Alias(v) => {
                    struct_ser.serialize_field("alias", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ProjectionNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "expr",
            "alias",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Expr,
            Alias,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "expr" => Ok(GeneratedField::Expr),
                            "alias" => Ok(GeneratedField::Alias),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ProjectionNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ProjectionNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ProjectionNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut expr__ = None;
                let mut optional_alias__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map.next_value()?;
                        }
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = Some(map.next_value()?);
                        }
                        GeneratedField::Alias => {
                            if optional_alias__.is_some() {
                                return Err(serde::de::Error::duplicate_field("alias"));
                            }
                            optional_alias__ = map.next_value::<::std::option::Option<_>>()?.map(projection_node::OptionalAlias::Alias);
                        }
                    }
                }
                Ok(ProjectionNode {
                    input: input__,
                    expr: expr__.unwrap_or_default(),
                    optional_alias: optional_alias__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ProjectionNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for RepartitionExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if self.partition_method.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.RepartitionExecNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if let Some(v) = self.partition_method.as_ref() {
            match v {
                repartition_exec_node::PartitionMethod::RoundRobin(v) => {
                    struct_ser.serialize_field("roundRobin", ToString::to_string(&v).as_str())?;
                }
                repartition_exec_node::PartitionMethod::Hash(v) => {
                    struct_ser.serialize_field("hash", v)?;
                }
                repartition_exec_node::PartitionMethod::Unknown(v) => {
                    struct_ser.serialize_field("unknown", ToString::to_string(&v).as_str())?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for RepartitionExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "round_robin",
            "roundRobin",
            "hash",
            "unknown",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            RoundRobin,
            Hash,
            Unknown,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "roundRobin" | "round_robin" => Ok(GeneratedField::RoundRobin),
                            "hash" => Ok(GeneratedField::Hash),
                            "unknown" => Ok(GeneratedField::Unknown),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = RepartitionExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.RepartitionExecNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<RepartitionExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut partition_method__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map.next_value()?;
                        }
                        GeneratedField::RoundRobin => {
                            if partition_method__.is_some() {
                                return Err(serde::de::Error::duplicate_field("roundRobin"));
                            }
                            partition_method__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| repartition_exec_node::PartitionMethod::RoundRobin(x.0));
                        }
                        GeneratedField::Hash => {
                            if partition_method__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hash"));
                            }
                            partition_method__ = map.next_value::<::std::option::Option<_>>()?.map(repartition_exec_node::PartitionMethod::Hash)
;
                        }
                        GeneratedField::Unknown => {
                            if partition_method__.is_some() {
                                return Err(serde::de::Error::duplicate_field("unknown"));
                            }
                            partition_method__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| repartition_exec_node::PartitionMethod::Unknown(x.0));
                        }
                    }
                }
                Ok(RepartitionExecNode {
                    input: input__,
                    partition_method: partition_method__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.RepartitionExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for RepartitionNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if self.partition_method.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.RepartitionNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if let Some(v) = self.partition_method.as_ref() {
            match v {
                repartition_node::PartitionMethod::RoundRobin(v) => {
                    struct_ser.serialize_field("roundRobin", ToString::to_string(&v).as_str())?;
                }
                repartition_node::PartitionMethod::Hash(v) => {
                    struct_ser.serialize_field("hash", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for RepartitionNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "round_robin",
            "roundRobin",
            "hash",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            RoundRobin,
            Hash,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "roundRobin" | "round_robin" => Ok(GeneratedField::RoundRobin),
                            "hash" => Ok(GeneratedField::Hash),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = RepartitionNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.RepartitionNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<RepartitionNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut partition_method__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map.next_value()?;
                        }
                        GeneratedField::RoundRobin => {
                            if partition_method__.is_some() {
                                return Err(serde::de::Error::duplicate_field("roundRobin"));
                            }
                            partition_method__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| repartition_node::PartitionMethod::RoundRobin(x.0));
                        }
                        GeneratedField::Hash => {
                            if partition_method__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hash"));
                            }
                            partition_method__ = map.next_value::<::std::option::Option<_>>()?.map(repartition_node::PartitionMethod::Hash)
;
                        }
                    }
                }
                Ok(RepartitionNode {
                    input: input__,
                    partition_method: partition_method__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.RepartitionNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for RollupNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.expr.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.RollupNode", len)?;
        if !self.expr.is_empty() {
            struct_ser.serialize_field("expr", &self.expr)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for RollupNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = RollupNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.RollupNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<RollupNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(RollupNode {
                    expr: expr__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.RollupNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ScalarDictionaryValue {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.index_type.is_some() {
            len += 1;
        }
        if self.value.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ScalarDictionaryValue", len)?;
        if let Some(v) = self.index_type.as_ref() {
            struct_ser.serialize_field("indexType", v)?;
        }
        if let Some(v) = self.value.as_ref() {
            struct_ser.serialize_field("value", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ScalarDictionaryValue {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "index_type",
            "indexType",
            "value",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            IndexType,
            Value,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "indexType" | "index_type" => Ok(GeneratedField::IndexType),
                            "value" => Ok(GeneratedField::Value),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ScalarDictionaryValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ScalarDictionaryValue")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ScalarDictionaryValue, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut index_type__ = None;
                let mut value__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::IndexType => {
                            if index_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("indexType"));
                            }
                            index_type__ = map.next_value()?;
                        }
                        GeneratedField::Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("value"));
                            }
                            value__ = map.next_value()?;
                        }
                    }
                }
                Ok(ScalarDictionaryValue {
                    index_type: index_type__,
                    value: value__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ScalarDictionaryValue", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ScalarFixedSizeBinary {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.values.is_empty() {
            len += 1;
        }
        if self.length != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ScalarFixedSizeBinary", len)?;
        if !self.values.is_empty() {
            struct_ser.serialize_field("values", pbjson::private::base64::encode(&self.values).as_str())?;
        }
        if self.length != 0 {
            struct_ser.serialize_field("length", &self.length)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ScalarFixedSizeBinary {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "values",
            "length",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Values,
            Length,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "values" => Ok(GeneratedField::Values),
                            "length" => Ok(GeneratedField::Length),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ScalarFixedSizeBinary;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ScalarFixedSizeBinary")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ScalarFixedSizeBinary, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut values__ = None;
                let mut length__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Values => {
                            if values__.is_some() {
                                return Err(serde::de::Error::duplicate_field("values"));
                            }
                            values__ = 
                                Some(map.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Length => {
                            if length__.is_some() {
                                return Err(serde::de::Error::duplicate_field("length"));
                            }
                            length__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(ScalarFixedSizeBinary {
                    values: values__.unwrap_or_default(),
                    length: length__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ScalarFixedSizeBinary", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ScalarFunction {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Abs => "Abs",
            Self::Acos => "Acos",
            Self::Asin => "Asin",
            Self::Atan => "Atan",
            Self::Ascii => "Ascii",
            Self::Ceil => "Ceil",
            Self::Cos => "Cos",
            Self::Digest => "Digest",
            Self::Exp => "Exp",
            Self::Floor => "Floor",
            Self::Ln => "Ln",
            Self::Log => "Log",
            Self::Log10 => "Log10",
            Self::Log2 => "Log2",
            Self::Round => "Round",
            Self::Signum => "Signum",
            Self::Sin => "Sin",
            Self::Sqrt => "Sqrt",
            Self::Tan => "Tan",
            Self::Trunc => "Trunc",
            Self::Array => "Array",
            Self::RegexpMatch => "RegexpMatch",
            Self::BitLength => "BitLength",
            Self::Btrim => "Btrim",
            Self::CharacterLength => "CharacterLength",
            Self::Chr => "Chr",
            Self::Concat => "Concat",
            Self::ConcatWithSeparator => "ConcatWithSeparator",
            Self::DatePart => "DatePart",
            Self::DateTrunc => "DateTrunc",
            Self::InitCap => "InitCap",
            Self::Left => "Left",
            Self::Lpad => "Lpad",
            Self::Lower => "Lower",
            Self::Ltrim => "Ltrim",
            Self::Md5 => "MD5",
            Self::NullIf => "NullIf",
            Self::OctetLength => "OctetLength",
            Self::Random => "Random",
            Self::RegexpReplace => "RegexpReplace",
            Self::Repeat => "Repeat",
            Self::Replace => "Replace",
            Self::Reverse => "Reverse",
            Self::Right => "Right",
            Self::Rpad => "Rpad",
            Self::Rtrim => "Rtrim",
            Self::Sha224 => "SHA224",
            Self::Sha256 => "SHA256",
            Self::Sha384 => "SHA384",
            Self::Sha512 => "SHA512",
            Self::SplitPart => "SplitPart",
            Self::StartsWith => "StartsWith",
            Self::Strpos => "Strpos",
            Self::Substr => "Substr",
            Self::ToHex => "ToHex",
            Self::ToTimestamp => "ToTimestamp",
            Self::ToTimestampMillis => "ToTimestampMillis",
            Self::ToTimestampMicros => "ToTimestampMicros",
            Self::ToTimestampSeconds => "ToTimestampSeconds",
            Self::Now => "Now",
            Self::Translate => "Translate",
            Self::Trim => "Trim",
            Self::Upper => "Upper",
            Self::Coalesce => "Coalesce",
            Self::Power => "Power",
            Self::StructFun => "StructFun",
            Self::FromUnixtime => "FromUnixtime",
            Self::Atan2 => "Atan2",
            Self::DateBin => "DateBin",
            Self::ArrowTypeof => "ArrowTypeof",
            Self::CurrentDate => "CurrentDate",
            Self::CurrentTime => "CurrentTime",
            Self::Uuid => "Uuid",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for ScalarFunction {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "Abs",
            "Acos",
            "Asin",
            "Atan",
            "Ascii",
            "Ceil",
            "Cos",
            "Digest",
            "Exp",
            "Floor",
            "Ln",
            "Log",
            "Log10",
            "Log2",
            "Round",
            "Signum",
            "Sin",
            "Sqrt",
            "Tan",
            "Trunc",
            "Array",
            "RegexpMatch",
            "BitLength",
            "Btrim",
            "CharacterLength",
            "Chr",
            "Concat",
            "ConcatWithSeparator",
            "DatePart",
            "DateTrunc",
            "InitCap",
            "Left",
            "Lpad",
            "Lower",
            "Ltrim",
            "MD5",
            "NullIf",
            "OctetLength",
            "Random",
            "RegexpReplace",
            "Repeat",
            "Replace",
            "Reverse",
            "Right",
            "Rpad",
            "Rtrim",
            "SHA224",
            "SHA256",
            "SHA384",
            "SHA512",
            "SplitPart",
            "StartsWith",
            "Strpos",
            "Substr",
            "ToHex",
            "ToTimestamp",
            "ToTimestampMillis",
            "ToTimestampMicros",
            "ToTimestampSeconds",
            "Now",
            "Translate",
            "Trim",
            "Upper",
            "Coalesce",
            "Power",
            "StructFun",
            "FromUnixtime",
            "Atan2",
            "DateBin",
            "ArrowTypeof",
            "CurrentDate",
            "CurrentTime",
            "Uuid",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ScalarFunction;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(ScalarFunction::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(ScalarFunction::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "Abs" => Ok(ScalarFunction::Abs),
                    "Acos" => Ok(ScalarFunction::Acos),
                    "Asin" => Ok(ScalarFunction::Asin),
                    "Atan" => Ok(ScalarFunction::Atan),
                    "Ascii" => Ok(ScalarFunction::Ascii),
                    "Ceil" => Ok(ScalarFunction::Ceil),
                    "Cos" => Ok(ScalarFunction::Cos),
                    "Digest" => Ok(ScalarFunction::Digest),
                    "Exp" => Ok(ScalarFunction::Exp),
                    "Floor" => Ok(ScalarFunction::Floor),
                    "Ln" => Ok(ScalarFunction::Ln),
                    "Log" => Ok(ScalarFunction::Log),
                    "Log10" => Ok(ScalarFunction::Log10),
                    "Log2" => Ok(ScalarFunction::Log2),
                    "Round" => Ok(ScalarFunction::Round),
                    "Signum" => Ok(ScalarFunction::Signum),
                    "Sin" => Ok(ScalarFunction::Sin),
                    "Sqrt" => Ok(ScalarFunction::Sqrt),
                    "Tan" => Ok(ScalarFunction::Tan),
                    "Trunc" => Ok(ScalarFunction::Trunc),
                    "Array" => Ok(ScalarFunction::Array),
                    "RegexpMatch" => Ok(ScalarFunction::RegexpMatch),
                    "BitLength" => Ok(ScalarFunction::BitLength),
                    "Btrim" => Ok(ScalarFunction::Btrim),
                    "CharacterLength" => Ok(ScalarFunction::CharacterLength),
                    "Chr" => Ok(ScalarFunction::Chr),
                    "Concat" => Ok(ScalarFunction::Concat),
                    "ConcatWithSeparator" => Ok(ScalarFunction::ConcatWithSeparator),
                    "DatePart" => Ok(ScalarFunction::DatePart),
                    "DateTrunc" => Ok(ScalarFunction::DateTrunc),
                    "InitCap" => Ok(ScalarFunction::InitCap),
                    "Left" => Ok(ScalarFunction::Left),
                    "Lpad" => Ok(ScalarFunction::Lpad),
                    "Lower" => Ok(ScalarFunction::Lower),
                    "Ltrim" => Ok(ScalarFunction::Ltrim),
                    "MD5" => Ok(ScalarFunction::Md5),
                    "NullIf" => Ok(ScalarFunction::NullIf),
                    "OctetLength" => Ok(ScalarFunction::OctetLength),
                    "Random" => Ok(ScalarFunction::Random),
                    "RegexpReplace" => Ok(ScalarFunction::RegexpReplace),
                    "Repeat" => Ok(ScalarFunction::Repeat),
                    "Replace" => Ok(ScalarFunction::Replace),
                    "Reverse" => Ok(ScalarFunction::Reverse),
                    "Right" => Ok(ScalarFunction::Right),
                    "Rpad" => Ok(ScalarFunction::Rpad),
                    "Rtrim" => Ok(ScalarFunction::Rtrim),
                    "SHA224" => Ok(ScalarFunction::Sha224),
                    "SHA256" => Ok(ScalarFunction::Sha256),
                    "SHA384" => Ok(ScalarFunction::Sha384),
                    "SHA512" => Ok(ScalarFunction::Sha512),
                    "SplitPart" => Ok(ScalarFunction::SplitPart),
                    "StartsWith" => Ok(ScalarFunction::StartsWith),
                    "Strpos" => Ok(ScalarFunction::Strpos),
                    "Substr" => Ok(ScalarFunction::Substr),
                    "ToHex" => Ok(ScalarFunction::ToHex),
                    "ToTimestamp" => Ok(ScalarFunction::ToTimestamp),
                    "ToTimestampMillis" => Ok(ScalarFunction::ToTimestampMillis),
                    "ToTimestampMicros" => Ok(ScalarFunction::ToTimestampMicros),
                    "ToTimestampSeconds" => Ok(ScalarFunction::ToTimestampSeconds),
                    "Now" => Ok(ScalarFunction::Now),
                    "Translate" => Ok(ScalarFunction::Translate),
                    "Trim" => Ok(ScalarFunction::Trim),
                    "Upper" => Ok(ScalarFunction::Upper),
                    "Coalesce" => Ok(ScalarFunction::Coalesce),
                    "Power" => Ok(ScalarFunction::Power),
                    "StructFun" => Ok(ScalarFunction::StructFun),
                    "FromUnixtime" => Ok(ScalarFunction::FromUnixtime),
                    "Atan2" => Ok(ScalarFunction::Atan2),
                    "DateBin" => Ok(ScalarFunction::DateBin),
                    "ArrowTypeof" => Ok(ScalarFunction::ArrowTypeof),
                    "CurrentDate" => Ok(ScalarFunction::CurrentDate),
                    "CurrentTime" => Ok(ScalarFunction::CurrentTime),
                    "Uuid" => Ok(ScalarFunction::Uuid),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for ScalarFunctionNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.fun != 0 {
            len += 1;
        }
        if !self.args.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ScalarFunctionNode", len)?;
        if self.fun != 0 {
            let v = ScalarFunction::from_i32(self.fun)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.fun)))?;
            struct_ser.serialize_field("fun", &v)?;
        }
        if !self.args.is_empty() {
            struct_ser.serialize_field("args", &self.args)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ScalarFunctionNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "fun",
            "args",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Fun,
            Args,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "fun" => Ok(GeneratedField::Fun),
                            "args" => Ok(GeneratedField::Args),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ScalarFunctionNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ScalarFunctionNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ScalarFunctionNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut fun__ = None;
                let mut args__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Fun => {
                            if fun__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fun"));
                            }
                            fun__ = Some(map.next_value::<ScalarFunction>()? as i32);
                        }
                        GeneratedField::Args => {
                            if args__.is_some() {
                                return Err(serde::de::Error::duplicate_field("args"));
                            }
                            args__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ScalarFunctionNode {
                    fun: fun__.unwrap_or_default(),
                    args: args__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ScalarFunctionNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ScalarListValue {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.is_null {
            len += 1;
        }
        if self.field.is_some() {
            len += 1;
        }
        if !self.values.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ScalarListValue", len)?;
        if self.is_null {
            struct_ser.serialize_field("isNull", &self.is_null)?;
        }
        if let Some(v) = self.field.as_ref() {
            struct_ser.serialize_field("field", v)?;
        }
        if !self.values.is_empty() {
            struct_ser.serialize_field("values", &self.values)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ScalarListValue {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "is_null",
            "isNull",
            "field",
            "values",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            IsNull,
            Field,
            Values,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "isNull" | "is_null" => Ok(GeneratedField::IsNull),
                            "field" => Ok(GeneratedField::Field),
                            "values" => Ok(GeneratedField::Values),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ScalarListValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ScalarListValue")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ScalarListValue, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut is_null__ = None;
                let mut field__ = None;
                let mut values__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::IsNull => {
                            if is_null__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isNull"));
                            }
                            is_null__ = Some(map.next_value()?);
                        }
                        GeneratedField::Field => {
                            if field__.is_some() {
                                return Err(serde::de::Error::duplicate_field("field"));
                            }
                            field__ = map.next_value()?;
                        }
                        GeneratedField::Values => {
                            if values__.is_some() {
                                return Err(serde::de::Error::duplicate_field("values"));
                            }
                            values__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ScalarListValue {
                    is_null: is_null__.unwrap_or_default(),
                    field: field__,
                    values: values__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ScalarListValue", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ScalarTime32Value {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.value.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ScalarTime32Value", len)?;
        if let Some(v) = self.value.as_ref() {
            match v {
                scalar_time32_value::Value::Time32SecondValue(v) => {
                    struct_ser.serialize_field("time32SecondValue", v)?;
                }
                scalar_time32_value::Value::Time32MillisecondValue(v) => {
                    struct_ser.serialize_field("time32MillisecondValue", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ScalarTime32Value {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "time32_second_value",
            "time32SecondValue",
            "time32_millisecond_value",
            "time32MillisecondValue",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Time32SecondValue,
            Time32MillisecondValue,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "time32SecondValue" | "time32_second_value" => Ok(GeneratedField::Time32SecondValue),
                            "time32MillisecondValue" | "time32_millisecond_value" => Ok(GeneratedField::Time32MillisecondValue),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ScalarTime32Value;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ScalarTime32Value")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ScalarTime32Value, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut value__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Time32SecondValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("time32SecondValue"));
                            }
                            value__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_time32_value::Value::Time32SecondValue(x.0));
                        }
                        GeneratedField::Time32MillisecondValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("time32MillisecondValue"));
                            }
                            value__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_time32_value::Value::Time32MillisecondValue(x.0));
                        }
                    }
                }
                Ok(ScalarTime32Value {
                    value: value__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ScalarTime32Value", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ScalarTime64Value {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.value.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ScalarTime64Value", len)?;
        if let Some(v) = self.value.as_ref() {
            match v {
                scalar_time64_value::Value::Time64MicrosecondValue(v) => {
                    struct_ser.serialize_field("time64MicrosecondValue", ToString::to_string(&v).as_str())?;
                }
                scalar_time64_value::Value::Time64NanosecondValue(v) => {
                    struct_ser.serialize_field("time64NanosecondValue", ToString::to_string(&v).as_str())?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ScalarTime64Value {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "time64_microsecond_value",
            "time64MicrosecondValue",
            "time64_nanosecond_value",
            "time64NanosecondValue",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Time64MicrosecondValue,
            Time64NanosecondValue,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "time64MicrosecondValue" | "time64_microsecond_value" => Ok(GeneratedField::Time64MicrosecondValue),
                            "time64NanosecondValue" | "time64_nanosecond_value" => Ok(GeneratedField::Time64NanosecondValue),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ScalarTime64Value;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ScalarTime64Value")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ScalarTime64Value, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut value__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Time64MicrosecondValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("time64MicrosecondValue"));
                            }
                            value__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_time64_value::Value::Time64MicrosecondValue(x.0));
                        }
                        GeneratedField::Time64NanosecondValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("time64NanosecondValue"));
                            }
                            value__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_time64_value::Value::Time64NanosecondValue(x.0));
                        }
                    }
                }
                Ok(ScalarTime64Value {
                    value: value__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ScalarTime64Value", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ScalarTimestampValue {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.timezone.is_empty() {
            len += 1;
        }
        if self.value.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ScalarTimestampValue", len)?;
        if !self.timezone.is_empty() {
            struct_ser.serialize_field("timezone", &self.timezone)?;
        }
        if let Some(v) = self.value.as_ref() {
            match v {
                scalar_timestamp_value::Value::TimeMicrosecondValue(v) => {
                    struct_ser.serialize_field("timeMicrosecondValue", ToString::to_string(&v).as_str())?;
                }
                scalar_timestamp_value::Value::TimeNanosecondValue(v) => {
                    struct_ser.serialize_field("timeNanosecondValue", ToString::to_string(&v).as_str())?;
                }
                scalar_timestamp_value::Value::TimeSecondValue(v) => {
                    struct_ser.serialize_field("timeSecondValue", ToString::to_string(&v).as_str())?;
                }
                scalar_timestamp_value::Value::TimeMillisecondValue(v) => {
                    struct_ser.serialize_field("timeMillisecondValue", ToString::to_string(&v).as_str())?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ScalarTimestampValue {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "timezone",
            "time_microsecond_value",
            "timeMicrosecondValue",
            "time_nanosecond_value",
            "timeNanosecondValue",
            "time_second_value",
            "timeSecondValue",
            "time_millisecond_value",
            "timeMillisecondValue",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Timezone,
            TimeMicrosecondValue,
            TimeNanosecondValue,
            TimeSecondValue,
            TimeMillisecondValue,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "timezone" => Ok(GeneratedField::Timezone),
                            "timeMicrosecondValue" | "time_microsecond_value" => Ok(GeneratedField::TimeMicrosecondValue),
                            "timeNanosecondValue" | "time_nanosecond_value" => Ok(GeneratedField::TimeNanosecondValue),
                            "timeSecondValue" | "time_second_value" => Ok(GeneratedField::TimeSecondValue),
                            "timeMillisecondValue" | "time_millisecond_value" => Ok(GeneratedField::TimeMillisecondValue),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ScalarTimestampValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ScalarTimestampValue")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ScalarTimestampValue, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut timezone__ = None;
                let mut value__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Timezone => {
                            if timezone__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timezone"));
                            }
                            timezone__ = Some(map.next_value()?);
                        }
                        GeneratedField::TimeMicrosecondValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timeMicrosecondValue"));
                            }
                            value__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_timestamp_value::Value::TimeMicrosecondValue(x.0));
                        }
                        GeneratedField::TimeNanosecondValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timeNanosecondValue"));
                            }
                            value__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_timestamp_value::Value::TimeNanosecondValue(x.0));
                        }
                        GeneratedField::TimeSecondValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timeSecondValue"));
                            }
                            value__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_timestamp_value::Value::TimeSecondValue(x.0));
                        }
                        GeneratedField::TimeMillisecondValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timeMillisecondValue"));
                            }
                            value__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_timestamp_value::Value::TimeMillisecondValue(x.0));
                        }
                    }
                }
                Ok(ScalarTimestampValue {
                    timezone: timezone__.unwrap_or_default(),
                    value: value__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ScalarTimestampValue", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ScalarUdfExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.fun_name.is_empty() {
            len += 1;
        }
        if !self.args.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ScalarUDFExprNode", len)?;
        if !self.fun_name.is_empty() {
            struct_ser.serialize_field("funName", &self.fun_name)?;
        }
        if !self.args.is_empty() {
            struct_ser.serialize_field("args", &self.args)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ScalarUdfExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "fun_name",
            "funName",
            "args",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            FunName,
            Args,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "funName" | "fun_name" => Ok(GeneratedField::FunName),
                            "args" => Ok(GeneratedField::Args),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ScalarUdfExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ScalarUDFExprNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ScalarUdfExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut fun_name__ = None;
                let mut args__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::FunName => {
                            if fun_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("funName"));
                            }
                            fun_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::Args => {
                            if args__.is_some() {
                                return Err(serde::de::Error::duplicate_field("args"));
                            }
                            args__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ScalarUdfExprNode {
                    fun_name: fun_name__.unwrap_or_default(),
                    args: args__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ScalarUDFExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ScalarValue {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.value.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ScalarValue", len)?;
        if let Some(v) = self.value.as_ref() {
            match v {
                scalar_value::Value::NullValue(v) => {
                    struct_ser.serialize_field("nullValue", v)?;
                }
                scalar_value::Value::BoolValue(v) => {
                    struct_ser.serialize_field("boolValue", v)?;
                }
                scalar_value::Value::Utf8Value(v) => {
                    struct_ser.serialize_field("utf8Value", v)?;
                }
                scalar_value::Value::LargeUtf8Value(v) => {
                    struct_ser.serialize_field("largeUtf8Value", v)?;
                }
                scalar_value::Value::Int8Value(v) => {
                    struct_ser.serialize_field("int8Value", v)?;
                }
                scalar_value::Value::Int16Value(v) => {
                    struct_ser.serialize_field("int16Value", v)?;
                }
                scalar_value::Value::Int32Value(v) => {
                    struct_ser.serialize_field("int32Value", v)?;
                }
                scalar_value::Value::Int64Value(v) => {
                    struct_ser.serialize_field("int64Value", ToString::to_string(&v).as_str())?;
                }
                scalar_value::Value::Uint8Value(v) => {
                    struct_ser.serialize_field("uint8Value", v)?;
                }
                scalar_value::Value::Uint16Value(v) => {
                    struct_ser.serialize_field("uint16Value", v)?;
                }
                scalar_value::Value::Uint32Value(v) => {
                    struct_ser.serialize_field("uint32Value", v)?;
                }
                scalar_value::Value::Uint64Value(v) => {
                    struct_ser.serialize_field("uint64Value", ToString::to_string(&v).as_str())?;
                }
                scalar_value::Value::Float32Value(v) => {
                    struct_ser.serialize_field("float32Value", v)?;
                }
                scalar_value::Value::Float64Value(v) => {
                    struct_ser.serialize_field("float64Value", v)?;
                }
                scalar_value::Value::Date32Value(v) => {
                    struct_ser.serialize_field("date32Value", v)?;
                }
                scalar_value::Value::Time32Value(v) => {
                    struct_ser.serialize_field("time32Value", v)?;
                }
                scalar_value::Value::ListValue(v) => {
                    struct_ser.serialize_field("listValue", v)?;
                }
                scalar_value::Value::Decimal128Value(v) => {
                    struct_ser.serialize_field("decimal128Value", v)?;
                }
                scalar_value::Value::Date64Value(v) => {
                    struct_ser.serialize_field("date64Value", ToString::to_string(&v).as_str())?;
                }
                scalar_value::Value::IntervalYearmonthValue(v) => {
                    struct_ser.serialize_field("intervalYearmonthValue", v)?;
                }
                scalar_value::Value::IntervalDaytimeValue(v) => {
                    struct_ser.serialize_field("intervalDaytimeValue", ToString::to_string(&v).as_str())?;
                }
                scalar_value::Value::TimestampValue(v) => {
                    struct_ser.serialize_field("timestampValue", v)?;
                }
                scalar_value::Value::DictionaryValue(v) => {
                    struct_ser.serialize_field("dictionaryValue", v)?;
                }
                scalar_value::Value::BinaryValue(v) => {
                    struct_ser.serialize_field("binaryValue", pbjson::private::base64::encode(&v).as_str())?;
                }
                scalar_value::Value::LargeBinaryValue(v) => {
                    struct_ser.serialize_field("largeBinaryValue", pbjson::private::base64::encode(&v).as_str())?;
                }
                scalar_value::Value::Time64Value(v) => {
                    struct_ser.serialize_field("time64Value", v)?;
                }
                scalar_value::Value::IntervalMonthDayNano(v) => {
                    struct_ser.serialize_field("intervalMonthDayNano", v)?;
                }
                scalar_value::Value::StructValue(v) => {
                    struct_ser.serialize_field("structValue", v)?;
                }
                scalar_value::Value::FixedSizeBinaryValue(v) => {
                    struct_ser.serialize_field("fixedSizeBinaryValue", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ScalarValue {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "null_value",
            "nullValue",
            "bool_value",
            "boolValue",
            "utf8_value",
            "utf8Value",
            "large_utf8_value",
            "largeUtf8Value",
            "int8_value",
            "int8Value",
            "int16_value",
            "int16Value",
            "int32_value",
            "int32Value",
            "int64_value",
            "int64Value",
            "uint8_value",
            "uint8Value",
            "uint16_value",
            "uint16Value",
            "uint32_value",
            "uint32Value",
            "uint64_value",
            "uint64Value",
            "float32_value",
            "float32Value",
            "float64_value",
            "float64Value",
            "date_32_value",
            "date32Value",
            "time32_value",
            "time32Value",
            "list_value",
            "listValue",
            "decimal128_value",
            "decimal128Value",
            "date_64_value",
            "date64Value",
            "interval_yearmonth_value",
            "intervalYearmonthValue",
            "interval_daytime_value",
            "intervalDaytimeValue",
            "timestamp_value",
            "timestampValue",
            "dictionary_value",
            "dictionaryValue",
            "binary_value",
            "binaryValue",
            "large_binary_value",
            "largeBinaryValue",
            "time64_value",
            "time64Value",
            "interval_month_day_nano",
            "intervalMonthDayNano",
            "struct_value",
            "structValue",
            "fixed_size_binary_value",
            "fixedSizeBinaryValue",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NullValue,
            BoolValue,
            Utf8Value,
            LargeUtf8Value,
            Int8Value,
            Int16Value,
            Int32Value,
            Int64Value,
            Uint8Value,
            Uint16Value,
            Uint32Value,
            Uint64Value,
            Float32Value,
            Float64Value,
            Date32Value,
            Time32Value,
            ListValue,
            Decimal128Value,
            Date64Value,
            IntervalYearmonthValue,
            IntervalDaytimeValue,
            TimestampValue,
            DictionaryValue,
            BinaryValue,
            LargeBinaryValue,
            Time64Value,
            IntervalMonthDayNano,
            StructValue,
            FixedSizeBinaryValue,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "nullValue" | "null_value" => Ok(GeneratedField::NullValue),
                            "boolValue" | "bool_value" => Ok(GeneratedField::BoolValue),
                            "utf8Value" | "utf8_value" => Ok(GeneratedField::Utf8Value),
                            "largeUtf8Value" | "large_utf8_value" => Ok(GeneratedField::LargeUtf8Value),
                            "int8Value" | "int8_value" => Ok(GeneratedField::Int8Value),
                            "int16Value" | "int16_value" => Ok(GeneratedField::Int16Value),
                            "int32Value" | "int32_value" => Ok(GeneratedField::Int32Value),
                            "int64Value" | "int64_value" => Ok(GeneratedField::Int64Value),
                            "uint8Value" | "uint8_value" => Ok(GeneratedField::Uint8Value),
                            "uint16Value" | "uint16_value" => Ok(GeneratedField::Uint16Value),
                            "uint32Value" | "uint32_value" => Ok(GeneratedField::Uint32Value),
                            "uint64Value" | "uint64_value" => Ok(GeneratedField::Uint64Value),
                            "float32Value" | "float32_value" => Ok(GeneratedField::Float32Value),
                            "float64Value" | "float64_value" => Ok(GeneratedField::Float64Value),
                            "date32Value" | "date_32_value" => Ok(GeneratedField::Date32Value),
                            "time32Value" | "time32_value" => Ok(GeneratedField::Time32Value),
                            "listValue" | "list_value" => Ok(GeneratedField::ListValue),
                            "decimal128Value" | "decimal128_value" => Ok(GeneratedField::Decimal128Value),
                            "date64Value" | "date_64_value" => Ok(GeneratedField::Date64Value),
                            "intervalYearmonthValue" | "interval_yearmonth_value" => Ok(GeneratedField::IntervalYearmonthValue),
                            "intervalDaytimeValue" | "interval_daytime_value" => Ok(GeneratedField::IntervalDaytimeValue),
                            "timestampValue" | "timestamp_value" => Ok(GeneratedField::TimestampValue),
                            "dictionaryValue" | "dictionary_value" => Ok(GeneratedField::DictionaryValue),
                            "binaryValue" | "binary_value" => Ok(GeneratedField::BinaryValue),
                            "largeBinaryValue" | "large_binary_value" => Ok(GeneratedField::LargeBinaryValue),
                            "time64Value" | "time64_value" => Ok(GeneratedField::Time64Value),
                            "intervalMonthDayNano" | "interval_month_day_nano" => Ok(GeneratedField::IntervalMonthDayNano),
                            "structValue" | "struct_value" => Ok(GeneratedField::StructValue),
                            "fixedSizeBinaryValue" | "fixed_size_binary_value" => Ok(GeneratedField::FixedSizeBinaryValue),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ScalarValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ScalarValue")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ScalarValue, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut value__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::NullValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nullValue"));
                            }
                            value__ = map.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::NullValue)
;
                        }
                        GeneratedField::BoolValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("boolValue"));
                            }
                            value__ = map.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::BoolValue);
                        }
                        GeneratedField::Utf8Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("utf8Value"));
                            }
                            value__ = map.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::Utf8Value);
                        }
                        GeneratedField::LargeUtf8Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("largeUtf8Value"));
                            }
                            value__ = map.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::LargeUtf8Value);
                        }
                        GeneratedField::Int8Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("int8Value"));
                            }
                            value__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::Int8Value(x.0));
                        }
                        GeneratedField::Int16Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("int16Value"));
                            }
                            value__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::Int16Value(x.0));
                        }
                        GeneratedField::Int32Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("int32Value"));
                            }
                            value__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::Int32Value(x.0));
                        }
                        GeneratedField::Int64Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("int64Value"));
                            }
                            value__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::Int64Value(x.0));
                        }
                        GeneratedField::Uint8Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uint8Value"));
                            }
                            value__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::Uint8Value(x.0));
                        }
                        GeneratedField::Uint16Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uint16Value"));
                            }
                            value__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::Uint16Value(x.0));
                        }
                        GeneratedField::Uint32Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uint32Value"));
                            }
                            value__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::Uint32Value(x.0));
                        }
                        GeneratedField::Uint64Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uint64Value"));
                            }
                            value__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::Uint64Value(x.0));
                        }
                        GeneratedField::Float32Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("float32Value"));
                            }
                            value__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::Float32Value(x.0));
                        }
                        GeneratedField::Float64Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("float64Value"));
                            }
                            value__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::Float64Value(x.0));
                        }
                        GeneratedField::Date32Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("date32Value"));
                            }
                            value__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::Date32Value(x.0));
                        }
                        GeneratedField::Time32Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("time32Value"));
                            }
                            value__ = map.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::Time32Value)
;
                        }
                        GeneratedField::ListValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("listValue"));
                            }
                            value__ = map.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::ListValue)
;
                        }
                        GeneratedField::Decimal128Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("decimal128Value"));
                            }
                            value__ = map.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::Decimal128Value)
;
                        }
                        GeneratedField::Date64Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("date64Value"));
                            }
                            value__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::Date64Value(x.0));
                        }
                        GeneratedField::IntervalYearmonthValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("intervalYearmonthValue"));
                            }
                            value__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::IntervalYearmonthValue(x.0));
                        }
                        GeneratedField::IntervalDaytimeValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("intervalDaytimeValue"));
                            }
                            value__ = map.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::IntervalDaytimeValue(x.0));
                        }
                        GeneratedField::TimestampValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timestampValue"));
                            }
                            value__ = map.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::TimestampValue)
;
                        }
                        GeneratedField::DictionaryValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dictionaryValue"));
                            }
                            value__ = map.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::DictionaryValue)
;
                        }
                        GeneratedField::BinaryValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("binaryValue"));
                            }
                            value__ = map.next_value::<::std::option::Option<::pbjson::private::BytesDeserialize<_>>>()?.map(|x| scalar_value::Value::BinaryValue(x.0));
                        }
                        GeneratedField::LargeBinaryValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("largeBinaryValue"));
                            }
                            value__ = map.next_value::<::std::option::Option<::pbjson::private::BytesDeserialize<_>>>()?.map(|x| scalar_value::Value::LargeBinaryValue(x.0));
                        }
                        GeneratedField::Time64Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("time64Value"));
                            }
                            value__ = map.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::Time64Value)
;
                        }
                        GeneratedField::IntervalMonthDayNano => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("intervalMonthDayNano"));
                            }
                            value__ = map.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::IntervalMonthDayNano)
;
                        }
                        GeneratedField::StructValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("structValue"));
                            }
                            value__ = map.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::StructValue)
;
                        }
                        GeneratedField::FixedSizeBinaryValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fixedSizeBinaryValue"));
                            }
                            value__ = map.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::FixedSizeBinaryValue)
;
                        }
                    }
                }
                Ok(ScalarValue {
                    value: value__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ScalarValue", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ScanLimit {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.limit != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ScanLimit", len)?;
        if self.limit != 0 {
            struct_ser.serialize_field("limit", &self.limit)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ScanLimit {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "limit",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Limit,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "limit" => Ok(GeneratedField::Limit),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ScanLimit;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ScanLimit")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ScanLimit, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut limit__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Limit => {
                            if limit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("limit"));
                            }
                            limit__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(ScanLimit {
                    limit: limit__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ScanLimit", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Schema {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.columns.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.Schema", len)?;
        if !self.columns.is_empty() {
            struct_ser.serialize_field("columns", &self.columns)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Schema {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "columns",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Columns,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "columns" => Ok(GeneratedField::Columns),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Schema;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.Schema")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Schema, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut columns__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Columns => {
                            if columns__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columns"));
                            }
                            columns__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(Schema {
                    columns: columns__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.Schema", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SelectionExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.SelectionExecNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SelectionExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SelectionExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.SelectionExecNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SelectionExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                    }
                }
                Ok(SelectionExecNode {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.SelectionExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SelectionNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.SelectionNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SelectionNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SelectionNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.SelectionNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SelectionNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map.next_value()?;
                        }
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                    }
                }
                Ok(SelectionNode {
                    input: input__,
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.SelectionNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SimilarToNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.negated {
            len += 1;
        }
        if self.expr.is_some() {
            len += 1;
        }
        if self.pattern.is_some() {
            len += 1;
        }
        if !self.escape_char.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.SimilarToNode", len)?;
        if self.negated {
            struct_ser.serialize_field("negated", &self.negated)?;
        }
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if let Some(v) = self.pattern.as_ref() {
            struct_ser.serialize_field("pattern", v)?;
        }
        if !self.escape_char.is_empty() {
            struct_ser.serialize_field("escapeChar", &self.escape_char)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SimilarToNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "negated",
            "expr",
            "pattern",
            "escape_char",
            "escapeChar",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Negated,
            Expr,
            Pattern,
            EscapeChar,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "negated" => Ok(GeneratedField::Negated),
                            "expr" => Ok(GeneratedField::Expr),
                            "pattern" => Ok(GeneratedField::Pattern),
                            "escapeChar" | "escape_char" => Ok(GeneratedField::EscapeChar),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SimilarToNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.SimilarToNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SimilarToNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut negated__ = None;
                let mut expr__ = None;
                let mut pattern__ = None;
                let mut escape_char__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Negated => {
                            if negated__.is_some() {
                                return Err(serde::de::Error::duplicate_field("negated"));
                            }
                            negated__ = Some(map.next_value()?);
                        }
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                        GeneratedField::Pattern => {
                            if pattern__.is_some() {
                                return Err(serde::de::Error::duplicate_field("pattern"));
                            }
                            pattern__ = map.next_value()?;
                        }
                        GeneratedField::EscapeChar => {
                            if escape_char__.is_some() {
                                return Err(serde::de::Error::duplicate_field("escapeChar"));
                            }
                            escape_char__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(SimilarToNode {
                    negated: negated__.unwrap_or_default(),
                    expr: expr__,
                    pattern: pattern__,
                    escape_char: escape_char__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.SimilarToNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SortExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if !self.expr.is_empty() {
            len += 1;
        }
        if self.fetch != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.SortExecNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if !self.expr.is_empty() {
            struct_ser.serialize_field("expr", &self.expr)?;
        }
        if self.fetch != 0 {
            struct_ser.serialize_field("fetch", ToString::to_string(&self.fetch).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SortExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "expr",
            "fetch",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Expr,
            Fetch,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "expr" => Ok(GeneratedField::Expr),
                            "fetch" => Ok(GeneratedField::Fetch),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SortExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.SortExecNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SortExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut expr__ = None;
                let mut fetch__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map.next_value()?;
                        }
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = Some(map.next_value()?);
                        }
                        GeneratedField::Fetch => {
                            if fetch__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fetch"));
                            }
                            fetch__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(SortExecNode {
                    input: input__,
                    expr: expr__.unwrap_or_default(),
                    fetch: fetch__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.SortExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SortExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if self.asc {
            len += 1;
        }
        if self.nulls_first {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.SortExprNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if self.asc {
            struct_ser.serialize_field("asc", &self.asc)?;
        }
        if self.nulls_first {
            struct_ser.serialize_field("nullsFirst", &self.nulls_first)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SortExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "asc",
            "nulls_first",
            "nullsFirst",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            Asc,
            NullsFirst,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "asc" => Ok(GeneratedField::Asc),
                            "nullsFirst" | "nulls_first" => Ok(GeneratedField::NullsFirst),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SortExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.SortExprNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SortExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut asc__ = None;
                let mut nulls_first__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                        GeneratedField::Asc => {
                            if asc__.is_some() {
                                return Err(serde::de::Error::duplicate_field("asc"));
                            }
                            asc__ = Some(map.next_value()?);
                        }
                        GeneratedField::NullsFirst => {
                            if nulls_first__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nullsFirst"));
                            }
                            nulls_first__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(SortExprNode {
                    expr: expr__,
                    asc: asc__.unwrap_or_default(),
                    nulls_first: nulls_first__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.SortExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SortNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if !self.expr.is_empty() {
            len += 1;
        }
        if self.fetch != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.SortNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if !self.expr.is_empty() {
            struct_ser.serialize_field("expr", &self.expr)?;
        }
        if self.fetch != 0 {
            struct_ser.serialize_field("fetch", ToString::to_string(&self.fetch).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SortNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "expr",
            "fetch",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Expr,
            Fetch,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "expr" => Ok(GeneratedField::Expr),
                            "fetch" => Ok(GeneratedField::Fetch),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SortNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.SortNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SortNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut expr__ = None;
                let mut fetch__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map.next_value()?;
                        }
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = Some(map.next_value()?);
                        }
                        GeneratedField::Fetch => {
                            if fetch__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fetch"));
                            }
                            fetch__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(SortNode {
                    input: input__,
                    expr: expr__.unwrap_or_default(),
                    fetch: fetch__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.SortNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SortPreservingMergeExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if !self.expr.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.SortPreservingMergeExecNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if !self.expr.is_empty() {
            struct_ser.serialize_field("expr", &self.expr)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SortPreservingMergeExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SortPreservingMergeExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.SortPreservingMergeExecNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SortPreservingMergeExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map.next_value()?;
                        }
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(SortPreservingMergeExecNode {
                    input: input__,
                    expr: expr__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.SortPreservingMergeExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Statistics {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.num_rows != 0 {
            len += 1;
        }
        if self.total_byte_size != 0 {
            len += 1;
        }
        if !self.column_stats.is_empty() {
            len += 1;
        }
        if self.is_exact {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.Statistics", len)?;
        if self.num_rows != 0 {
            struct_ser.serialize_field("numRows", ToString::to_string(&self.num_rows).as_str())?;
        }
        if self.total_byte_size != 0 {
            struct_ser.serialize_field("totalByteSize", ToString::to_string(&self.total_byte_size).as_str())?;
        }
        if !self.column_stats.is_empty() {
            struct_ser.serialize_field("columnStats", &self.column_stats)?;
        }
        if self.is_exact {
            struct_ser.serialize_field("isExact", &self.is_exact)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Statistics {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "num_rows",
            "numRows",
            "total_byte_size",
            "totalByteSize",
            "column_stats",
            "columnStats",
            "is_exact",
            "isExact",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NumRows,
            TotalByteSize,
            ColumnStats,
            IsExact,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "numRows" | "num_rows" => Ok(GeneratedField::NumRows),
                            "totalByteSize" | "total_byte_size" => Ok(GeneratedField::TotalByteSize),
                            "columnStats" | "column_stats" => Ok(GeneratedField::ColumnStats),
                            "isExact" | "is_exact" => Ok(GeneratedField::IsExact),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Statistics;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.Statistics")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Statistics, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut num_rows__ = None;
                let mut total_byte_size__ = None;
                let mut column_stats__ = None;
                let mut is_exact__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::NumRows => {
                            if num_rows__.is_some() {
                                return Err(serde::de::Error::duplicate_field("numRows"));
                            }
                            num_rows__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::TotalByteSize => {
                            if total_byte_size__.is_some() {
                                return Err(serde::de::Error::duplicate_field("totalByteSize"));
                            }
                            total_byte_size__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::ColumnStats => {
                            if column_stats__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnStats"));
                            }
                            column_stats__ = Some(map.next_value()?);
                        }
                        GeneratedField::IsExact => {
                            if is_exact__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isExact"));
                            }
                            is_exact__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(Statistics {
                    num_rows: num_rows__.unwrap_or_default(),
                    total_byte_size: total_byte_size__.unwrap_or_default(),
                    column_stats: column_stats__.unwrap_or_default(),
                    is_exact: is_exact__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.Statistics", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for StringifiedPlan {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.plan_type.is_some() {
            len += 1;
        }
        if !self.plan.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.StringifiedPlan", len)?;
        if let Some(v) = self.plan_type.as_ref() {
            struct_ser.serialize_field("planType", v)?;
        }
        if !self.plan.is_empty() {
            struct_ser.serialize_field("plan", &self.plan)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for StringifiedPlan {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "plan_type",
            "planType",
            "plan",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            PlanType,
            Plan,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "planType" | "plan_type" => Ok(GeneratedField::PlanType),
                            "plan" => Ok(GeneratedField::Plan),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = StringifiedPlan;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.StringifiedPlan")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<StringifiedPlan, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut plan_type__ = None;
                let mut plan__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::PlanType => {
                            if plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("planType"));
                            }
                            plan_type__ = map.next_value()?;
                        }
                        GeneratedField::Plan => {
                            if plan__.is_some() {
                                return Err(serde::de::Error::duplicate_field("plan"));
                            }
                            plan__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(StringifiedPlan {
                    plan_type: plan_type__,
                    plan: plan__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.StringifiedPlan", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Struct {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.sub_field_types.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.Struct", len)?;
        if !self.sub_field_types.is_empty() {
            struct_ser.serialize_field("subFieldTypes", &self.sub_field_types)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Struct {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "sub_field_types",
            "subFieldTypes",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SubFieldTypes,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "subFieldTypes" | "sub_field_types" => Ok(GeneratedField::SubFieldTypes),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Struct;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.Struct")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Struct, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut sub_field_types__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::SubFieldTypes => {
                            if sub_field_types__.is_some() {
                                return Err(serde::de::Error::duplicate_field("subFieldTypes"));
                            }
                            sub_field_types__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(Struct {
                    sub_field_types: sub_field_types__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.Struct", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for StructValue {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.field_values.is_empty() {
            len += 1;
        }
        if !self.fields.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.StructValue", len)?;
        if !self.field_values.is_empty() {
            struct_ser.serialize_field("fieldValues", &self.field_values)?;
        }
        if !self.fields.is_empty() {
            struct_ser.serialize_field("fields", &self.fields)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for StructValue {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "field_values",
            "fieldValues",
            "fields",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            FieldValues,
            Fields,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "fieldValues" | "field_values" => Ok(GeneratedField::FieldValues),
                            "fields" => Ok(GeneratedField::Fields),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = StructValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.StructValue")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<StructValue, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut field_values__ = None;
                let mut fields__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::FieldValues => {
                            if field_values__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fieldValues"));
                            }
                            field_values__ = Some(map.next_value()?);
                        }
                        GeneratedField::Fields => {
                            if fields__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fields"));
                            }
                            fields__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(StructValue {
                    field_values: field_values__.unwrap_or_default(),
                    fields: fields__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.StructValue", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SubqueryAliasNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if !self.alias.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.SubqueryAliasNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if !self.alias.is_empty() {
            struct_ser.serialize_field("alias", &self.alias)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SubqueryAliasNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "alias",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Alias,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "alias" => Ok(GeneratedField::Alias),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SubqueryAliasNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.SubqueryAliasNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SubqueryAliasNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut alias__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map.next_value()?;
                        }
                        GeneratedField::Alias => {
                            if alias__.is_some() {
                                return Err(serde::de::Error::duplicate_field("alias"));
                            }
                            alias__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(SubqueryAliasNode {
                    input: input__,
                    alias: alias__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.SubqueryAliasNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TimeUnit {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Second => "Second",
            Self::Millisecond => "Millisecond",
            Self::Microsecond => "Microsecond",
            Self::Nanosecond => "Nanosecond",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for TimeUnit {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "Second",
            "Millisecond",
            "Microsecond",
            "Nanosecond",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TimeUnit;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(TimeUnit::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(TimeUnit::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "Second" => Ok(TimeUnit::Second),
                    "Millisecond" => Ok(TimeUnit::Millisecond),
                    "Microsecond" => Ok(TimeUnit::Microsecond),
                    "Nanosecond" => Ok(TimeUnit::Nanosecond),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for Timestamp {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.time_unit != 0 {
            len += 1;
        }
        if !self.timezone.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.Timestamp", len)?;
        if self.time_unit != 0 {
            let v = TimeUnit::from_i32(self.time_unit)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.time_unit)))?;
            struct_ser.serialize_field("timeUnit", &v)?;
        }
        if !self.timezone.is_empty() {
            struct_ser.serialize_field("timezone", &self.timezone)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Timestamp {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "time_unit",
            "timeUnit",
            "timezone",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TimeUnit,
            Timezone,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "timeUnit" | "time_unit" => Ok(GeneratedField::TimeUnit),
                            "timezone" => Ok(GeneratedField::Timezone),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Timestamp;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.Timestamp")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Timestamp, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut time_unit__ = None;
                let mut timezone__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TimeUnit => {
                            if time_unit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timeUnit"));
                            }
                            time_unit__ = Some(map.next_value::<TimeUnit>()? as i32);
                        }
                        GeneratedField::Timezone => {
                            if timezone__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timezone"));
                            }
                            timezone__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(Timestamp {
                    time_unit: time_unit__.unwrap_or_default(),
                    timezone: timezone__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.Timestamp", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TryCastNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if self.arrow_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.TryCastNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if let Some(v) = self.arrow_type.as_ref() {
            struct_ser.serialize_field("arrowType", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TryCastNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "arrow_type",
            "arrowType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            ArrowType,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "arrowType" | "arrow_type" => Ok(GeneratedField::ArrowType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TryCastNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.TryCastNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<TryCastNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut arrow_type__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                        GeneratedField::ArrowType => {
                            if arrow_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("arrowType"));
                            }
                            arrow_type__ = map.next_value()?;
                        }
                    }
                }
                Ok(TryCastNode {
                    expr: expr__,
                    arrow_type: arrow_type__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.TryCastNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Union {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.union_types.is_empty() {
            len += 1;
        }
        if self.union_mode != 0 {
            len += 1;
        }
        if !self.type_ids.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.Union", len)?;
        if !self.union_types.is_empty() {
            struct_ser.serialize_field("unionTypes", &self.union_types)?;
        }
        if self.union_mode != 0 {
            let v = UnionMode::from_i32(self.union_mode)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.union_mode)))?;
            struct_ser.serialize_field("unionMode", &v)?;
        }
        if !self.type_ids.is_empty() {
            struct_ser.serialize_field("typeIds", &self.type_ids)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Union {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "union_types",
            "unionTypes",
            "union_mode",
            "unionMode",
            "type_ids",
            "typeIds",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            UnionTypes,
            UnionMode,
            TypeIds,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "unionTypes" | "union_types" => Ok(GeneratedField::UnionTypes),
                            "unionMode" | "union_mode" => Ok(GeneratedField::UnionMode),
                            "typeIds" | "type_ids" => Ok(GeneratedField::TypeIds),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Union;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.Union")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Union, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut union_types__ = None;
                let mut union_mode__ = None;
                let mut type_ids__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::UnionTypes => {
                            if union_types__.is_some() {
                                return Err(serde::de::Error::duplicate_field("unionTypes"));
                            }
                            union_types__ = Some(map.next_value()?);
                        }
                        GeneratedField::UnionMode => {
                            if union_mode__.is_some() {
                                return Err(serde::de::Error::duplicate_field("unionMode"));
                            }
                            union_mode__ = Some(map.next_value::<UnionMode>()? as i32);
                        }
                        GeneratedField::TypeIds => {
                            if type_ids__.is_some() {
                                return Err(serde::de::Error::duplicate_field("typeIds"));
                            }
                            type_ids__ = 
                                Some(map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                    }
                }
                Ok(Union {
                    union_types: union_types__.unwrap_or_default(),
                    union_mode: union_mode__.unwrap_or_default(),
                    type_ids: type_ids__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.Union", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for UnionExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.inputs.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.UnionExecNode", len)?;
        if !self.inputs.is_empty() {
            struct_ser.serialize_field("inputs", &self.inputs)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for UnionExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "inputs",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Inputs,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "inputs" => Ok(GeneratedField::Inputs),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = UnionExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.UnionExecNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<UnionExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut inputs__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Inputs => {
                            if inputs__.is_some() {
                                return Err(serde::de::Error::duplicate_field("inputs"));
                            }
                            inputs__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(UnionExecNode {
                    inputs: inputs__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.UnionExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for UnionMode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Sparse => "sparse",
            Self::Dense => "dense",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for UnionMode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "sparse",
            "dense",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = UnionMode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(UnionMode::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(UnionMode::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "sparse" => Ok(UnionMode::Sparse),
                    "dense" => Ok(UnionMode::Dense),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for UnionNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.inputs.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.UnionNode", len)?;
        if !self.inputs.is_empty() {
            struct_ser.serialize_field("inputs", &self.inputs)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for UnionNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "inputs",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Inputs,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "inputs" => Ok(GeneratedField::Inputs),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = UnionNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.UnionNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<UnionNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut inputs__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Inputs => {
                            if inputs__.is_some() {
                                return Err(serde::de::Error::duplicate_field("inputs"));
                            }
                            inputs__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(UnionNode {
                    inputs: inputs__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.UnionNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ValuesNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.n_cols != 0 {
            len += 1;
        }
        if !self.values_list.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ValuesNode", len)?;
        if self.n_cols != 0 {
            struct_ser.serialize_field("nCols", ToString::to_string(&self.n_cols).as_str())?;
        }
        if !self.values_list.is_empty() {
            struct_ser.serialize_field("valuesList", &self.values_list)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ValuesNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "n_cols",
            "nCols",
            "values_list",
            "valuesList",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NCols,
            ValuesList,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "nCols" | "n_cols" => Ok(GeneratedField::NCols),
                            "valuesList" | "values_list" => Ok(GeneratedField::ValuesList),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ValuesNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ValuesNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ValuesNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut n_cols__ = None;
                let mut values_list__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::NCols => {
                            if n_cols__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nCols"));
                            }
                            n_cols__ = 
                                Some(map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::ValuesList => {
                            if values_list__.is_some() {
                                return Err(serde::de::Error::duplicate_field("valuesList"));
                            }
                            values_list__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ValuesNode {
                    n_cols: n_cols__.unwrap_or_default(),
                    values_list: values_list__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ValuesNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ViewTableScanNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.table_name.is_empty() {
            len += 1;
        }
        if self.input.is_some() {
            len += 1;
        }
        if self.schema.is_some() {
            len += 1;
        }
        if self.projection.is_some() {
            len += 1;
        }
        if !self.definition.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ViewTableScanNode", len)?;
        if !self.table_name.is_empty() {
            struct_ser.serialize_field("tableName", &self.table_name)?;
        }
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if let Some(v) = self.schema.as_ref() {
            struct_ser.serialize_field("schema", v)?;
        }
        if let Some(v) = self.projection.as_ref() {
            struct_ser.serialize_field("projection", v)?;
        }
        if !self.definition.is_empty() {
            struct_ser.serialize_field("definition", &self.definition)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ViewTableScanNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "table_name",
            "tableName",
            "input",
            "schema",
            "projection",
            "definition",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableName,
            Input,
            Schema,
            Projection,
            Definition,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "tableName" | "table_name" => Ok(GeneratedField::TableName),
                            "input" => Ok(GeneratedField::Input),
                            "schema" => Ok(GeneratedField::Schema),
                            "projection" => Ok(GeneratedField::Projection),
                            "definition" => Ok(GeneratedField::Definition),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ViewTableScanNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ViewTableScanNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ViewTableScanNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_name__ = None;
                let mut input__ = None;
                let mut schema__ = None;
                let mut projection__ = None;
                let mut definition__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TableName => {
                            if table_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableName"));
                            }
                            table_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map.next_value()?;
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = map.next_value()?;
                        }
                        GeneratedField::Projection => {
                            if projection__.is_some() {
                                return Err(serde::de::Error::duplicate_field("projection"));
                            }
                            projection__ = map.next_value()?;
                        }
                        GeneratedField::Definition => {
                            if definition__.is_some() {
                                return Err(serde::de::Error::duplicate_field("definition"));
                            }
                            definition__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ViewTableScanNode {
                    table_name: table_name__.unwrap_or_default(),
                    input: input__,
                    schema: schema__,
                    projection: projection__,
                    definition: definition__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ViewTableScanNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for WhenThen {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.when_expr.is_some() {
            len += 1;
        }
        if self.then_expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.WhenThen", len)?;
        if let Some(v) = self.when_expr.as_ref() {
            struct_ser.serialize_field("whenExpr", v)?;
        }
        if let Some(v) = self.then_expr.as_ref() {
            struct_ser.serialize_field("thenExpr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for WhenThen {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "when_expr",
            "whenExpr",
            "then_expr",
            "thenExpr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            WhenExpr,
            ThenExpr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "whenExpr" | "when_expr" => Ok(GeneratedField::WhenExpr),
                            "thenExpr" | "then_expr" => Ok(GeneratedField::ThenExpr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = WhenThen;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.WhenThen")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<WhenThen, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut when_expr__ = None;
                let mut then_expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::WhenExpr => {
                            if when_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("whenExpr"));
                            }
                            when_expr__ = map.next_value()?;
                        }
                        GeneratedField::ThenExpr => {
                            if then_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("thenExpr"));
                            }
                            then_expr__ = map.next_value()?;
                        }
                    }
                }
                Ok(WhenThen {
                    when_expr: when_expr__,
                    then_expr: then_expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.WhenThen", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for WindowAggExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if !self.window_expr.is_empty() {
            len += 1;
        }
        if !self.window_expr_name.is_empty() {
            len += 1;
        }
        if self.input_schema.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.WindowAggExecNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if !self.window_expr.is_empty() {
            struct_ser.serialize_field("windowExpr", &self.window_expr)?;
        }
        if !self.window_expr_name.is_empty() {
            struct_ser.serialize_field("windowExprName", &self.window_expr_name)?;
        }
        if let Some(v) = self.input_schema.as_ref() {
            struct_ser.serialize_field("inputSchema", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for WindowAggExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "window_expr",
            "windowExpr",
            "window_expr_name",
            "windowExprName",
            "input_schema",
            "inputSchema",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            WindowExpr,
            WindowExprName,
            InputSchema,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "windowExpr" | "window_expr" => Ok(GeneratedField::WindowExpr),
                            "windowExprName" | "window_expr_name" => Ok(GeneratedField::WindowExprName),
                            "inputSchema" | "input_schema" => Ok(GeneratedField::InputSchema),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = WindowAggExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.WindowAggExecNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<WindowAggExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut window_expr__ = None;
                let mut window_expr_name__ = None;
                let mut input_schema__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map.next_value()?;
                        }
                        GeneratedField::WindowExpr => {
                            if window_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("windowExpr"));
                            }
                            window_expr__ = Some(map.next_value()?);
                        }
                        GeneratedField::WindowExprName => {
                            if window_expr_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("windowExprName"));
                            }
                            window_expr_name__ = Some(map.next_value()?);
                        }
                        GeneratedField::InputSchema => {
                            if input_schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("inputSchema"));
                            }
                            input_schema__ = map.next_value()?;
                        }
                    }
                }
                Ok(WindowAggExecNode {
                    input: input__,
                    window_expr: window_expr__.unwrap_or_default(),
                    window_expr_name: window_expr_name__.unwrap_or_default(),
                    input_schema: input_schema__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.WindowAggExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for WindowExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if !self.partition_by.is_empty() {
            len += 1;
        }
        if !self.order_by.is_empty() {
            len += 1;
        }
        if self.window_frame.is_some() {
            len += 1;
        }
        if self.window_function.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.WindowExprNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if !self.partition_by.is_empty() {
            struct_ser.serialize_field("partitionBy", &self.partition_by)?;
        }
        if !self.order_by.is_empty() {
            struct_ser.serialize_field("orderBy", &self.order_by)?;
        }
        if let Some(v) = self.window_frame.as_ref() {
            struct_ser.serialize_field("windowFrame", v)?;
        }
        if let Some(v) = self.window_function.as_ref() {
            match v {
                window_expr_node::WindowFunction::AggrFunction(v) => {
                    let v = AggregateFunction::from_i32(*v)
                        .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
                    struct_ser.serialize_field("aggrFunction", &v)?;
                }
                window_expr_node::WindowFunction::BuiltInFunction(v) => {
                    let v = BuiltInWindowFunction::from_i32(*v)
                        .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
                    struct_ser.serialize_field("builtInFunction", &v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for WindowExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "partition_by",
            "partitionBy",
            "order_by",
            "orderBy",
            "window_frame",
            "windowFrame",
            "aggr_function",
            "aggrFunction",
            "built_in_function",
            "builtInFunction",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            PartitionBy,
            OrderBy,
            WindowFrame,
            AggrFunction,
            BuiltInFunction,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "partitionBy" | "partition_by" => Ok(GeneratedField::PartitionBy),
                            "orderBy" | "order_by" => Ok(GeneratedField::OrderBy),
                            "windowFrame" | "window_frame" => Ok(GeneratedField::WindowFrame),
                            "aggrFunction" | "aggr_function" => Ok(GeneratedField::AggrFunction),
                            "builtInFunction" | "built_in_function" => Ok(GeneratedField::BuiltInFunction),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = WindowExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.WindowExprNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<WindowExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut partition_by__ = None;
                let mut order_by__ = None;
                let mut window_frame__ = None;
                let mut window_function__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map.next_value()?;
                        }
                        GeneratedField::PartitionBy => {
                            if partition_by__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionBy"));
                            }
                            partition_by__ = Some(map.next_value()?);
                        }
                        GeneratedField::OrderBy => {
                            if order_by__.is_some() {
                                return Err(serde::de::Error::duplicate_field("orderBy"));
                            }
                            order_by__ = Some(map.next_value()?);
                        }
                        GeneratedField::WindowFrame => {
                            if window_frame__.is_some() {
                                return Err(serde::de::Error::duplicate_field("windowFrame"));
                            }
                            window_frame__ = map.next_value()?;
                        }
                        GeneratedField::AggrFunction => {
                            if window_function__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggrFunction"));
                            }
                            window_function__ = map.next_value::<::std::option::Option<AggregateFunction>>()?.map(|x| window_expr_node::WindowFunction::AggrFunction(x as i32));
                        }
                        GeneratedField::BuiltInFunction => {
                            if window_function__.is_some() {
                                return Err(serde::de::Error::duplicate_field("builtInFunction"));
                            }
                            window_function__ = map.next_value::<::std::option::Option<BuiltInWindowFunction>>()?.map(|x| window_expr_node::WindowFunction::BuiltInFunction(x as i32));
                        }
                    }
                }
                Ok(WindowExprNode {
                    expr: expr__,
                    partition_by: partition_by__.unwrap_or_default(),
                    order_by: order_by__.unwrap_or_default(),
                    window_frame: window_frame__,
                    window_function: window_function__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.WindowExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for WindowFrame {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.window_frame_units != 0 {
            len += 1;
        }
        if self.start_bound.is_some() {
            len += 1;
        }
        if self.end_bound.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.WindowFrame", len)?;
        if self.window_frame_units != 0 {
            let v = WindowFrameUnits::from_i32(self.window_frame_units)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.window_frame_units)))?;
            struct_ser.serialize_field("windowFrameUnits", &v)?;
        }
        if let Some(v) = self.start_bound.as_ref() {
            struct_ser.serialize_field("startBound", v)?;
        }
        if let Some(v) = self.end_bound.as_ref() {
            match v {
                window_frame::EndBound::Bound(v) => {
                    struct_ser.serialize_field("bound", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for WindowFrame {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "window_frame_units",
            "windowFrameUnits",
            "start_bound",
            "startBound",
            "bound",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            WindowFrameUnits,
            StartBound,
            Bound,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "windowFrameUnits" | "window_frame_units" => Ok(GeneratedField::WindowFrameUnits),
                            "startBound" | "start_bound" => Ok(GeneratedField::StartBound),
                            "bound" => Ok(GeneratedField::Bound),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = WindowFrame;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.WindowFrame")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<WindowFrame, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut window_frame_units__ = None;
                let mut start_bound__ = None;
                let mut end_bound__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::WindowFrameUnits => {
                            if window_frame_units__.is_some() {
                                return Err(serde::de::Error::duplicate_field("windowFrameUnits"));
                            }
                            window_frame_units__ = Some(map.next_value::<WindowFrameUnits>()? as i32);
                        }
                        GeneratedField::StartBound => {
                            if start_bound__.is_some() {
                                return Err(serde::de::Error::duplicate_field("startBound"));
                            }
                            start_bound__ = map.next_value()?;
                        }
                        GeneratedField::Bound => {
                            if end_bound__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bound"));
                            }
                            end_bound__ = map.next_value::<::std::option::Option<_>>()?.map(window_frame::EndBound::Bound)
;
                        }
                    }
                }
                Ok(WindowFrame {
                    window_frame_units: window_frame_units__.unwrap_or_default(),
                    start_bound: start_bound__,
                    end_bound: end_bound__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.WindowFrame", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for WindowFrameBound {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.window_frame_bound_type != 0 {
            len += 1;
        }
        if self.bound_value.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.WindowFrameBound", len)?;
        if self.window_frame_bound_type != 0 {
            let v = WindowFrameBoundType::from_i32(self.window_frame_bound_type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.window_frame_bound_type)))?;
            struct_ser.serialize_field("windowFrameBoundType", &v)?;
        }
        if let Some(v) = self.bound_value.as_ref() {
            struct_ser.serialize_field("boundValue", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for WindowFrameBound {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "window_frame_bound_type",
            "windowFrameBoundType",
            "bound_value",
            "boundValue",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            WindowFrameBoundType,
            BoundValue,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "windowFrameBoundType" | "window_frame_bound_type" => Ok(GeneratedField::WindowFrameBoundType),
                            "boundValue" | "bound_value" => Ok(GeneratedField::BoundValue),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = WindowFrameBound;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.WindowFrameBound")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<WindowFrameBound, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut window_frame_bound_type__ = None;
                let mut bound_value__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::WindowFrameBoundType => {
                            if window_frame_bound_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("windowFrameBoundType"));
                            }
                            window_frame_bound_type__ = Some(map.next_value::<WindowFrameBoundType>()? as i32);
                        }
                        GeneratedField::BoundValue => {
                            if bound_value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("boundValue"));
                            }
                            bound_value__ = map.next_value()?;
                        }
                    }
                }
                Ok(WindowFrameBound {
                    window_frame_bound_type: window_frame_bound_type__.unwrap_or_default(),
                    bound_value: bound_value__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.WindowFrameBound", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for WindowFrameBoundType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::CurrentRow => "CURRENT_ROW",
            Self::Preceding => "PRECEDING",
            Self::Following => "FOLLOWING",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for WindowFrameBoundType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "CURRENT_ROW",
            "PRECEDING",
            "FOLLOWING",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = WindowFrameBoundType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(WindowFrameBoundType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(WindowFrameBoundType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "CURRENT_ROW" => Ok(WindowFrameBoundType::CurrentRow),
                    "PRECEDING" => Ok(WindowFrameBoundType::Preceding),
                    "FOLLOWING" => Ok(WindowFrameBoundType::Following),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for WindowFrameUnits {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Rows => "ROWS",
            Self::Range => "RANGE",
            Self::Groups => "GROUPS",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for WindowFrameUnits {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "ROWS",
            "RANGE",
            "GROUPS",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = WindowFrameUnits;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(WindowFrameUnits::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(WindowFrameUnits::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "ROWS" => Ok(WindowFrameUnits::Rows),
                    "RANGE" => Ok(WindowFrameUnits::Range),
                    "GROUPS" => Ok(WindowFrameUnits::Groups),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for WindowNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if !self.window_expr.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.WindowNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if !self.window_expr.is_empty() {
            struct_ser.serialize_field("windowExpr", &self.window_expr)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for WindowNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "window_expr",
            "windowExpr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            WindowExpr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "windowExpr" | "window_expr" => Ok(GeneratedField::WindowExpr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = WindowNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.WindowNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<WindowNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut window_expr__ = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map.next_value()?;
                        }
                        GeneratedField::WindowExpr => {
                            if window_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("windowExpr"));
                            }
                            window_expr__ = Some(map.next_value()?);
                        }
                    }
                }
                Ok(WindowNode {
                    input: input__,
                    window_expr: window_expr__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.WindowNode", FIELDS, GeneratedVisitor)
    }
}
