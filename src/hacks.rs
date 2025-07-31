use crate::{Transpiler, transpiler::Result};
use sea_query::{PostgresQueryBuilder, Query, QueryBuilder};

/// Get _just_ the expression and the bind params without a surrounding SELECT statement
pub fn get_plaintext_expression(
    code: &str,
    tp: &Transpiler,
    builder: impl QueryBuilder,
) -> Result<String> {
    let sql = Query::select().expr(tp.transpile(code)?).to_string(builder);
    let sql = sql.strip_prefix("SELECT ").unwrap().to_owned();
    Ok(sql)
}

pub fn postgres(code: &str) -> Result<String> {
    get_plaintext_expression(
        code,
        &Transpiler::new().reduce(true).build(),
        PostgresQueryBuilder,
    )
}
