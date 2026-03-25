use std::str::FromStr;

use sea_query::{Alias, DynIden, IntoIden, PgInterval, SeaRc, StringLen};

use crate::{intermediate::Rc, types::json::JsonType};

#[derive(Debug, Clone, PartialEq, Default)]
pub enum ColumnType {
    Char(Option<u32>),
    String(StringLen),
    Text,
    Blob,
    TinyInteger,
    SmallInteger,
    Integer,
    BigInteger,
    TinyUnsigned,
    SmallUnsigned,
    Unsigned,
    BigUnsigned,
    Float,
    Double,
    Decimal(Option<(u32, u32)>),
    DateTime,
    Timestamp,
    TimestampWithTimeZone,
    Time,
    Date,
    Year,
    Interval(Option<PgInterval>, Option<u32>),
    Binary(u32),
    VarBinary(StringLen),
    Bit(Option<u32>),
    VarBit(u32),
    Boolean,
    Money(Option<(u32, u32)>),
    Json(JsonType, bool),
    Uuid,
    Custom(DynIden),
    Enum {
        name: DynIden,
        variants: Vec<DynIden>,
    },
    Array(Rc<Self>),
    Vector(Option<u32>),
    Cidr,
    Inet,
    MacAddr,
    LTree,
    Null,
    #[default]
    Inferred,
}

macro_rules! cast_list {
    ($($($from:tt),* -> $($to:tt),*;)*) => {

        const fn can_cast_inner(a: &Self, b: &Self) -> bool {
            match (a, b) {
                $(($(ColumnType::$from)|*, $(ColumnType::$to)|*) => true,)*
                _ => false
            }
        }

    };
}

impl ColumnType {
    #[must_use]
    pub const fn is_json(&self) -> bool {
        matches!(self, Self::Json(_, _))
    }

    #[must_use]
    pub fn can_cast_to(&self, other: &Self) -> bool {
        #[allow(clippy::match_same_arms)]
        match (self, other) {
            (_, Self::Inferred) => false,
            (_, Self::Null) => false,
            (a, b) if a == b => true,
            (Self::Array(me), Self::Array(other)) => me.can_cast_to(other),
            (_, Self::Array(_)) => false,
            (Self::Json(_, _), Self::Json(JsonType::Any, _)) => true,
            (Self::Json(json_type, _), other) => json_type.can_cast_to(other),
            (a, b) => Self::can_cast_inner(a, b),
        }
    }

    pub fn is_number(&self) -> bool {
        self.is_float() || self.is_integer()
    }

    pub fn is_float(&self) -> bool {
        matches!(self, Self::Double | Self::Float)
    }

    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            Self::TinyUnsigned
                | Self::SmallUnsigned
                | Self::Unsigned
                | Self::BigUnsigned
                | Self::TinyInteger
                | Self::SmallInteger
                | Self::Integer
                | Self::BigInteger
        )
    }

    cast_list!(
        TinyUnsigned, SmallUnsigned, Unsigned, BigUnsigned, TinyInteger, SmallInteger, Integer, BigInteger
        ->
        TinyUnsigned, SmallUnsigned, Unsigned, BigUnsigned, TinyInteger, SmallInteger, Integer, BigInteger;

        Double -> Float;
        Float -> Double;
    );
}

pub struct UnknownType(pub String);

impl FromStr for ColumnType {
    type Err = UnknownType;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "bool" => Self::Boolean,
            "text" => Self::Text,
            "float" => Self::Float,
            // "bytes" => ColumnType::Binary(_),
            "double" => Self::Double,
            // "json" => ColumnType::JsonText(_),
            // "jsonb" => ColumnType::Jsonb(_),
            // "char" => ColumnType::Char(_),
            // "string" => ColumnType::String(_),
            "bytea" => Self::Blob,
            "smallint" => Self::SmallInteger,
            "integer" => Self::BigInteger,
            "bigint" => Self::Integer,

            // "decimal" => ColumnType::Decimal(_),
            "timestamp without time zone" => Self::DateTime,
            "timestamp" => Self::Timestamp,
            "timestamp with time zone" => Self::TimestampWithTimeZone,
            "date" => Self::Date,
            "N/A" => Self::Year,
            // "interval" => ColumnType::Interval(_, _),
            // "bytea" => ColumnType::VarBinary(_),
            // "bit" => ColumnType::Bit(_),
            // "varbit" => ColumnType::VarBit(_),
            // "money" => ColumnType::Money(_),
            "uuid" => Self::Uuid,
            // "vector" => ColumnType::Vector(_),
            "cidr" => Self::Cidr,
            "inet" => Self::Inet,
            "macaddr" => Self::MacAddr,
            "ltree" => Self::LTree,

            "_" => Self::Inferred,
            "null" => Self::Null,
            s => return Err(UnknownType(s.to_owned())),
        })
    }
}

impl IntoIden for ColumnType {
    fn into_iden(self) -> sea_query::DynIden {
        #[allow(clippy::match_same_arms, reason = "list out each type")]
        SeaRc::new(match self {
            Self::Boolean => "bool",
            Self::Text => "text",
            Self::Float => "float",
            Self::Binary(_) => "bytes",
            Self::Time => "timestamp with time zone",
            Self::Double => "double",
            Self::Array(col_type) => {
                return Alias::new(format!("{}[]", (*col_type).clone().into_iden().to_string()))
                    .into_iden();
            }
            Self::Json(_, true) => "json",
            Self::Json(_, false) => "jsonb",
            Self::Char(_) => "char",
            Self::String(_) => "string",
            Self::Blob => "bytea",
            Self::TinyInteger => "smallint",
            Self::SmallInteger => "smallint",
            Self::BigInteger => "integer",
            Self::Integer => "bigint",
            Self::Unsigned => "smallint",
            Self::TinyUnsigned => "smallint",
            Self::SmallUnsigned => "integer",
            Self::BigUnsigned => "bigint",

            Self::Decimal(_) => "decimal",
            Self::DateTime => "timestamp without time zone",
            Self::Timestamp => "timestamp",
            Self::TimestampWithTimeZone => "timestamp with time zone",
            Self::Date => "date",
            Self::Year => "N/A",
            Self::Interval(_, _) => "interval",
            Self::VarBinary(_) => "bytea",
            Self::Bit(_) => "bit",
            Self::VarBit(_) => "varbit",
            Self::Money(_) => "money",
            Self::Uuid => "uuid",
            Self::Enum { name, .. } => return name,
            Self::Vector(_) => "vector",
            Self::Cidr => "cidr",
            Self::Inet => "inet",
            Self::MacAddr => "macaddr",
            Self::LTree => "ltree",
            Self::Custom(inner) => return inner,

            Self::Inferred => "text",
            Self::Null => "NULLTYPE",
        })
    }
}

impl From<sea_query::ColumnType> for ColumnType {
    fn from(value: sea_query::ColumnType) -> Self {
        #[allow(clippy::match_same_arms, reason = "list out each type")]
        match value {
            sea_query::ColumnType::Char(x) => Self::Char(x),
            sea_query::ColumnType::String(string_len) => Self::String(string_len),
            sea_query::ColumnType::Text => Self::Text,
            sea_query::ColumnType::Blob => Self::Blob,
            sea_query::ColumnType::TinyInteger => Self::TinyInteger,
            sea_query::ColumnType::SmallInteger => Self::SmallInteger,
            sea_query::ColumnType::Integer => Self::Integer,
            sea_query::ColumnType::BigInteger => Self::BigInteger,
            sea_query::ColumnType::TinyUnsigned => Self::TinyUnsigned,
            sea_query::ColumnType::SmallUnsigned => Self::SmallUnsigned,
            sea_query::ColumnType::Unsigned => Self::Unsigned,
            sea_query::ColumnType::BigUnsigned => Self::BigUnsigned,
            sea_query::ColumnType::Float => Self::Float,
            sea_query::ColumnType::Double => Self::Double,
            sea_query::ColumnType::Decimal(x) => Self::Decimal(x),
            sea_query::ColumnType::DateTime => Self::DateTime,
            sea_query::ColumnType::Timestamp => Self::Timestamp,
            sea_query::ColumnType::TimestampWithTimeZone => Self::TimestampWithTimeZone,
            sea_query::ColumnType::Time => Self::Time,
            sea_query::ColumnType::Date => Self::Date,
            sea_query::ColumnType::Year => Self::Year,
            sea_query::ColumnType::Interval(pg_interval, x) => Self::Interval(pg_interval, x),
            sea_query::ColumnType::Binary(b) => Self::Binary(b),
            sea_query::ColumnType::VarBinary(string_len) => Self::VarBinary(string_len),
            sea_query::ColumnType::Bit(b) => Self::Bit(b),
            sea_query::ColumnType::VarBit(b) => Self::VarBit(b),
            sea_query::ColumnType::Boolean => Self::Boolean,
            sea_query::ColumnType::Money(m) => Self::Money(m),
            sea_query::ColumnType::Json => Self::Json(JsonType::Any, false),
            sea_query::ColumnType::JsonBinary => Self::Json(JsonType::Any, true),
            sea_query::ColumnType::Uuid => Self::Uuid,
            sea_query::ColumnType::Custom(sea_rc) => Self::Custom(sea_rc),
            sea_query::ColumnType::Enum { name, variants } => Self::Enum { name, variants },
            sea_query::ColumnType::Array(column_type) => {
                Self::Array(Rc::new((*column_type).clone().into()))
            }
            sea_query::ColumnType::Vector(v) => Self::Vector(v),
            sea_query::ColumnType::Cidr => Self::Cidr,
            sea_query::ColumnType::Inet => Self::Inet,
            sea_query::ColumnType::MacAddr => Self::MacAddr,
            sea_query::ColumnType::LTree => Self::LTree,
            _ => unimplemented!("Columntype is non-exhaustive"),
        }
    }
}

impl From<ColumnType> for sea_query::ColumnType {
    fn from(value: ColumnType) -> Self {
        #[allow(clippy::match_same_arms, reason = "list out each type")]
        match value {
            ColumnType::Char(x) => Self::Char(x),
            ColumnType::String(string_len) => Self::String(string_len),
            ColumnType::Text => Self::Text,
            ColumnType::Blob => Self::Blob,
            ColumnType::TinyInteger => Self::TinyInteger,
            ColumnType::SmallInteger => Self::SmallInteger,
            ColumnType::Integer => Self::Integer,
            ColumnType::BigInteger => Self::BigInteger,
            ColumnType::TinyUnsigned => Self::TinyUnsigned,
            ColumnType::SmallUnsigned => Self::SmallUnsigned,
            ColumnType::Unsigned => Self::Unsigned,
            ColumnType::BigUnsigned => Self::BigUnsigned,
            ColumnType::Float => Self::Float,
            ColumnType::Double => Self::Double,
            ColumnType::Decimal(x) => Self::Decimal(x),
            ColumnType::DateTime => Self::DateTime,
            ColumnType::Timestamp => Self::Timestamp,
            ColumnType::TimestampWithTimeZone => Self::TimestampWithTimeZone,
            ColumnType::Time => Self::Time,
            ColumnType::Date => Self::Date,
            ColumnType::Year => Self::Year,
            ColumnType::Interval(pg_interval, x) => Self::Interval(pg_interval, x),
            ColumnType::Binary(b) => Self::Binary(b),
            ColumnType::VarBinary(string_len) => Self::VarBinary(string_len),
            ColumnType::Bit(b) => Self::Bit(b),
            ColumnType::VarBit(b) => Self::VarBit(b),
            ColumnType::Boolean => Self::Boolean,
            ColumnType::Money(m) => Self::Money(m),
            ColumnType::Json(_, true) => Self::Json,
            ColumnType::Json(_, false) => Self::JsonBinary,
            ColumnType::Uuid => Self::Uuid,
            ColumnType::Custom(sea_rc) => Self::Custom(sea_rc),
            ColumnType::Enum { name, variants } => Self::Enum { name, variants },
            ColumnType::Array(column_type) => Self::Array(Rc::new((*column_type).clone().into())),
            ColumnType::Vector(v) => Self::Vector(v),
            ColumnType::Cidr => Self::Cidr,
            ColumnType::Inet => Self::Inet,
            ColumnType::MacAddr => Self::MacAddr,
            ColumnType::LTree => Self::LTree,

            ColumnType::Null => Self::Text,
            ColumnType::Inferred => Self::Text,
        }
    }
}
