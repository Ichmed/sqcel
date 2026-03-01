//! String Functions
use sea_query::{Func, PgFunc};

use crate::{
    functions::Function,
    intermediate::{Expression, ToSql},
    sql_extensions::SqlExtension,
    transpiler::ParseError,
    transpiler::alias,
    types::SqlType,
};

/// contains - Tests whether the string operand contains the substring.
///
/// Time complexity is proportional to the product of the sizes of the arguments.
///
/// Signatures:
/// `string.contains(string) -> bool`
///
/// Examples:
/// `"hello world".contains("world") // true`
/// `"foobar".contains("baz") // false`
pub struct Contains {
    pub rec: Expression,
    pub arg: Expression,
}

impl Function for Contains {}

impl ToSql for Contains {
    fn to_sql(&self, _tp: &crate::Transpiler) -> crate::Result<crate::types::TypedExpression> {
        Err(ParseError::Todo(
            "contains has no trivial implementation in SQL. Use `matches()` for now",
        ))
    }

    fn returntype(&self, _tp: &crate::Transpiler) -> crate::types::Type {
        SqlType::Boolean.into()
    }
}

/// endsWith - Tests whether the string operand ends with the specified suffix.
///
/// Average time complexity is linear with respect to the size of the suffix string.
/// Worst-case time complexity is proportional to the product of the sizes of the arguments.
///
/// Signatures:
/// `string.endsWith(string) -> bool`
///
/// Examples:
/// `"hello world".endsWith("world") // true`
/// `"foobar".endsWith("bar") // true`
pub struct EndsWith {
    pub rec: Expression,
    pub arg: Expression,
}

impl Function for EndsWith {}

impl ToSql for EndsWith {
    fn to_sql(&self, _tp: &crate::Transpiler) -> crate::Result<crate::types::TypedExpression> {
        Err(ParseError::Todo(
            "ends_with has no trivial implementation in SQL. Use `matches()` for now",
        ))
    }

    fn returntype(&self, _tp: &crate::Transpiler) -> crate::types::Type {
        SqlType::Boolean.into()
    }
}

/// matches - Tests whether a string matches a given RE2 regular expression.
/// Time complexity is proportional to the product of the sizes of the arguments as guaranteed by the RE2 design.
///
/// Signatures:
/// `matches(string, string) -> bool`
/// `string.matches(string) -> bool`
///
/// Examples:
/// `matches("foobar", "foo.*") // true`
/// `"foobar".matches("foo.*") // true`
///
pub struct Matches {
    pub rec: Expression,
    pub arg: Expression,
}

impl Function for Matches {}

impl ToSql for Matches {
    fn to_sql(&self, tp: &crate::Transpiler) -> crate::Result<crate::types::TypedExpression> {
        Ok(Func::cust(alias("regexp_like"))
            .arg(self.rec.to_sql(tp)?.expr)
            .arg(self.arg.to_sql(tp)?.expr)
            .with_type(SqlType::Boolean))
    }

    fn returntype(&self, _tp: &crate::Transpiler) -> crate::types::Type {
        SqlType::Boolean.into()
    }
}
/// startsWith - Tests whether the string operand starts with the specified prefix.
///
/// Average time complexity is linear with respect to the size of the prefix.
/// Worst-case time complexity is proportional to the product of the sizes of the arguments.
///
/// Signatures:
/// string.startsWith(string) -> bool
///
///  Examples:
/// "hello world".startsWith("hello") // true
/// "foobar".startsWith("foo") // true
///
pub struct StartsWith {
    pub rec: Expression,
    pub arg: Expression,
}

impl Function for StartsWith {}

impl ToSql for StartsWith {
    fn to_sql(&self, tp: &crate::Transpiler) -> crate::Result<crate::types::TypedExpression> {
        Ok(
            PgFunc::starts_with(self.rec.to_sql(tp)?.expr, self.arg.to_sql(tp)?.expr)
                .with_type(SqlType::Boolean),
        )
    }

    fn returntype(&self, _tp: &crate::Transpiler) -> crate::types::Type {
        SqlType::Boolean.into()
    }
}

/// size - Determine the length of the string in terms of the number of Unicode codepoints
///
/// Signatures:
/// - `string.size() -> int`
/// - `size(string) -> int`
///
/// Examples:
/// - `"hello".size() // 5`
/// - `size("world!") // 6`
/// - `"fiance\u0301".size() // 7`
/// - `size(string(b'\xF0\x9F\xA4\xAA')) // 1`
///
pub struct Size {
    pub arg: Expression,
}

impl Function for Size {}

impl ToSql for Size {
    fn to_sql(&self, tp: &crate::Transpiler) -> crate::Result<crate::types::TypedExpression> {
        Ok(Func::cust(alias("length"))
            .arg(self.arg.to_sql(tp)?.expr)
            .with_type(SqlType::UInteger))
    }

    fn returntype(&self, _tp: &crate::Transpiler) -> crate::types::Type {
        SqlType::UInteger.into()
    }
}
