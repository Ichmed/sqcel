use sea_query::{BinOper, SimpleExpr, extension::postgres::PgBinOper};
pub trait SqlExtension {
    #[must_use]
    fn into_json_cast(self) -> Self;
    #[must_use]
    fn into_json_get(self) -> Self;
}

impl SqlExtension for SimpleExpr {
    fn into_json_cast(self) -> Self {
        match self {
            Self::Binary(a, BinOper::PgOperator(PgBinOper::GetJsonField), b) => {
                Self::Binary(a, BinOper::PgOperator(PgBinOper::CastJsonField), b)
            }
            x => x,
        }
    }

    fn into_json_get(self) -> Self {
        match self {
            Self::Binary(a, BinOper::PgOperator(PgBinOper::CastJsonField), b) => {
                Self::Binary(a, BinOper::PgOperator(PgBinOper::GetJsonField), b)
            }
            x => x,
        }
    }
}
