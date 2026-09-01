//! String Functions
use indoc::indoc;
use sea_query::{Func, PgFunc};

use crate::{
    func_args,
    functions::{Function, FunctionOrigin, FunctionPattern},
    intermediate::ToSql,
    sql_extensions::SqlExtension,
    transpiler::str_alias,
    types::ColumnType,
};

use super::Pattern;

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
#[must_use]
pub fn contains() -> Function {
    Function::define_disabled(
        func_args!(Text.endsWith(Text) -> Boolean),
        indoc!(
            r#"
            contains - Tests whether the string operand contains the substring.
            
            Time complexity is proportional to the product of the sizes of the arguments.
            
            Examples:
            `"hello world".contains("world") // true`
            `"foobar".contains("baz") // false`
            "#
        ),
        "`contains` has no trivial implementation in SQL. Use `matches()` for now",
    )
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
#[must_use]
pub fn ends_with() -> Function {
    Function::define_disabled(
        func_args!(Text.endsWith(Text) -> Boolean),
        indoc!(
            r#"
                endsWith - Tests whether the string operand ends with the specified suffix.

                Average time complexity is linear with respect to the size of the prefix.
                Worst-case time complexity is proportional to the product of the sizes of the arguments.
                
               Examples:
               `"hello world".endsWith("world") // true`
               `"foobar".endsWith("bar") // true`
                "#
        ),
        "`ends_with` has no trivial implementation in SQL. Use `matches()` for now",
    )
}

/// matches - Tests whether a string matches a given RE2 regular expression.
/// Time complexity is proportional to the product of the sizes of the arguments as guaranteed by the RE2 design.
///
/// *NOTE: This implementation uses the native Postgres regex syntax*
/// Signatures:
/// `matches(string, string) -> bool`
/// `string.matches(string) -> bool`
///
/// Examples:
/// `matches("foobar", "foo.*") // true`
/// `"foobar".matches("foo.*") // true`
#[must_use]
pub fn matches() -> Function {
    Function::define_with_origin(
        FunctionPattern {
            name: "matches".to_owned(),
            receiver: Some(Pattern::ExpressionCast(ColumnType::Text.into())),
            args: vec![Pattern::Custom {
                display: "Regex".to_owned(),
                accepts: |_| Ok(()),
            }],
            variadic: None,
            returns: ColumnType::Boolean.into(),
        },
        indoc!(
            r#"
                matches - Tests whether a string matches a given RE2 regular expression.
                Time complexity is proportional to the product of the sizes of the arguments as guaranteed by the RE2 design.

                *NOTE: This implementation uses the native Postgres regex syntax*

                Examples:
                `matches("foobar", "foo.*") // true`
                `"foobar".matches("foo.*") // true`
                "#
        ),
        |tp, x| {
            Ok(Func::cust(str_alias("regexp_like"))
                .arg(x.receiver()?.to_sql(tp)?.expr)
                .arg(x.arg(0)?.to_sql(tp)?.expr)
                .with_type(ColumnType::Boolean))
        },
        FunctionOrigin::Cel,
    )
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
#[must_use]
pub fn starts_with() -> Function {
    Function::define_with_origin(
        func_args!(Text.startsWith(Text) -> Boolean),
        indoc!(
            r#"
                startsWith - Tests whether the string operand starts with the specified prefix.

                Average time complexity is linear with respect to the size of the prefix.
                Worst-case time complexity is proportional to the product of the sizes of the arguments.
                
                Examples:
                "hello world".startsWith("hello")   // true
                "foobar".startsWith("foo")          // true
                "#
        ),
        |tp, x| {
            Ok(PgFunc::starts_with(
                x.receiver()?.to_sql(tp)?.convert(tp, &ColumnType::Text)?,
                x.arg(0)?.to_sql(tp)?,
            )
            .with_type(ColumnType::Boolean))
        },
        FunctionOrigin::Cel,
    )
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
#[must_use]
pub fn size() -> Function {
    Function::define_with_origin(
        func_args!(Text.size() -> Unsigned),
        indoc!(
            r#"
                size - Determine the length of the string in terms of the number of Unicode codepoints
                
                Examples:
                - `"hello".size() // 5`
                - `size("world!") // 6`
                - `"fiance\u0301".size() // 7`
                - `size(string(b'\xF0\x9F\xA4\xAA')) // 1`
            "#
        ),
        |tp, x| {
            Ok(Func::cust(str_alias("length"))
                .arg(x.receiver()?.to_sql(tp)?.convert(tp, &ColumnType::Text)?)
                .with_type(ColumnType::Unsigned))
        },
        FunctionOrigin::Cel,
    )
}
