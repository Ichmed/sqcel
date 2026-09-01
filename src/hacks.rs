use crate::{
    Transpiler,
    transpiler::{self, Result},
};
use sea_query::{Query, QueryBuilder};

/// Get _just_ the expression and the bind params without a surrounding SELECT statement
pub fn get_plaintext_expression(
    code: &str,
    tp: &Transpiler,
    builder: impl QueryBuilder,
) -> Result<String> {
    let sql = Query::select().expr(tp.transpile(code)?).to_string(builder);
    let sql = sql
        .strip_prefix("SELECT ")
        .ok_or(transpiler::Error::NotASelectStatement)?
        .to_owned();
    Ok(sql)
}

#[cfg(test)]
#[allow(unused)]
pub(crate) fn postgres(code: &str) -> Result<String> {
    use sea_query::PostgresQueryBuilder;

    get_plaintext_expression(
        code,
        &Transpiler::new().reduce(true).build()?,
        PostgresQueryBuilder,
    )
}
