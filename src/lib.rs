pub mod structure;
mod transpiler;

pub use sea_query::{SimpleExpr as SqlExpr, backend::*, query::Query};
pub use transpiler::{ParseError, Result, Transpiler};
pub mod functions;
pub mod hacks;
pub mod intermediate;
pub mod magic;
pub mod types;
pub mod variables;

#[cfg(test)]
mod test {
    use sea_query::{Expr, PostgresQueryBuilder};

    use crate::{
        Transpiler, hacks,
        structure::{Database, Schema, SqlLayout, Table},
        types::SqlType,
    };
}
