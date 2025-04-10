use crate::{Transpiler, transpiler::Result};
use sea_query::{Query, QueryBuilder};

/// Get _just_ the expression and the bind params without a surrounding SELECT statement
pub fn get_plaintext_expression(
    code: &str,
    tp: &Transpiler,
    builder: impl QueryBuilder,
) -> Result<(String, Vec<String>)> {
    let (sql, params) = Query::select().expr(tp.transpile(code)?).build(builder);
    let sql = sql.strip_prefix("SELECT ").unwrap().to_owned();
    let params = params.into_iter().map(|v| v.to_string()).collect();
    Ok((sql, params))
}
