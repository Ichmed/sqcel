// #![cfg(feature = "fuzzing")]
/*!
 * Test suite to find/document places where the SQL conversion and the
 * official [CEL specification](https://github.com/google/cel-spec/blob/master/doc/langdef.md) diverge
 *
 * - good!     These expressions evaluate succesfully in both engines
 *             and return an equivalent value
 *
 * - diverge!  These expressions evaluate succesfully in both engines
 *             but do not return an equivalent value
 *
 * - bug_sql!  These expression fail to compile or evalute in SQL
 *             even though they are valid CEL
 *
 * - bug_cel!  Theses expressions fail to compile or evaluate in the
 *             CEL interpreter even though they are valid CEL.
 *             They may or may not evaluate correctly in SQL
 *
 * - bug_both! Theses expressions fail to compile or evaluate in both
 *             engines even though they are valid CEL.
 *
 * To add a test, start of by using the good! macro, if the test fails, either fix the
 * underlying issue or change the label accordingly. If there is a GitHub issue corresponding
 * to the bug, put a link to it in a comment above the test
 */
use anyhow::anyhow;
use cel_interpreter::Program;
use sea_query::{Alias, Asterisk};
use serde::de::DeserializeOwned;
use serde_json::Value;
use sqcel::{
    PostgresQueryBuilder, Query, Transpiler,
    intermediate::{ToIntermediate, ToSql},
};
use std::env;
use tokio_postgres::{Client, NoTls};

async fn connect_pg() -> Client {
    let con_string = env::var("POSTGRES_CON_STRING")
        .unwrap_or("host=localhost user=postgres password=password".to_owned());
    let (client, connection) = tokio_postgres::connect(&con_string, NoTls).await.unwrap();
    tokio::spawn(connection);
    client
}

async fn from_pg<T: DeserializeOwned>(src: &str) -> anyhow::Result<T> {
    let tp = Transpiler::new()
        .accept_unknown_types(true)
        .reduce(false)
        .build();

    let q = dbg!(dbg!(cel_parser::parse(src))?.to_sqcel(&tp))?.to_sql(&tp)?;

    let sql = Query::select()
        .column(Asterisk)
        .from_subquery(Query::select().expr(q).take(), Alias::new("data"))
        .build(PostgresQueryBuilder)
        .0;

    eprintln!("{sql}");

    let from_pg = connect_pg()
        .await
        .query(sql.as_str(), Default::default())
        .await?
        .remove(0);

    Ok(serde_postgres::from_row::<T>(&from_pg)?)
}

async fn from_cel<T: DeserializeOwned>(code: &str) -> anyhow::Result<T> {
    let val = Program::compile(code)?.execute(&Default::default())?;

    match val.json() {
        Ok(val) => Ok(serde_json::from_value(val)?),
        Err(err) => Err(anyhow!("{err}")),
    }
}

#[allow(unused)]
macro_rules! good {
    ($($(#[$($attrss:tt)*])* $name:ident $src:literal $(: $ty:ident)? $(== $expected:expr)?);* $(;)?) => {
        $(
            #[tokio::test]
            #[allow(unused_mut)]
            #[allow(unused_assignments)]

            $(#[$($attrss)*])*
            async fn $name() {
                let mut pg $(: $ty)? $(= $expected)?;
                let mut cel $(: $ty)? $(= $expected)?;

                pg = from_pg$(::<$ty>)?($src).await.unwrap();
                cel = from_cel$(::<$ty>)?($src).await.unwrap();

                assert_eq!(pg, cel);
                $(assert_eq!(pg, $expected);)?
            }
        )*
    };
}

#[allow(unused)]
macro_rules! diverge {
    ($(#[$($attrss:tt)*])* $name:ident $src:literal : $ty:ident) => {
        #[tokio::test]
        #[doc = "Results diverge\n\n"]
        $(#[$($attrss)*])*
        async fn $name() {
            let from_pg = from_pg::<$ty>($src).await.unwrap();
            let from_cel = from_cel::<$ty>($src).await.unwrap();

            assert_ne!(from_pg, from_cel)
        }
    };
}

#[allow(unused)]
macro_rules! bug_sql {
    ($(#[$($attrss:tt)*])* $name:ident $src:literal : $ty:ident) => {
        #[tokio::test]
        #[doc = "Bug in Transpiler\n\n"]
        $(#[$($attrss)*])*
        async fn $name() {
            from_pg::<$ty>($src).await.unwrap_err();
            from_cel::<$ty>($src).await.unwrap();
        }
    };
}

#[allow(unused)]
macro_rules! bug_cel {
    ($(#[$($attrss:tt)*])* $name:ident $src:literal : $ty:ident) => {
        #[tokio::test]
        #[doc = "Bug in the Rust CEL interpreter\n\n"]
        $(#[$($attrss)*])*
        async fn $name() {
            from_pg::<$ty>($src).await.unwrap();
            from_cel::<$ty>($src).await.unwrap_err();
        }
    };
}

#[allow(unused)]
macro_rules! bug_both {
    ($(#[$($attrss:tt)*])* $name:ident $src:literal : $ty:ident) => {
        #[tokio::test]
        #[doc = "Separate bugs in both the transpiler and the Rust CEL interpreter\n\n"]
        $(#[$($attrss)*])*
        async fn $name() {
            from_pg::<$ty>($src).await.unwrap_err();
            from_cel::<$ty>($src).await.unwrap_err();
        }
    };
}

// --- Primitives ---

// -- bool --
good! {
    boolean_true
    "true" == true;
    boolean_false
    "false" == false;
    boolean_equal
    "true == false" == false;
}

// -- integer --
// Weird bug in postgres_serde that fails to
// deserialize int4 as a i64
// good!(int64 = "1": i64);

good! {
    int32
    "1": i32;
    simple_sum
    "1 + 2" == 3;
    int_compare
    "1 == 1" == true;
}

// -- float --
bug_sql!(
    /// Postgres removes the trailing zeroes
    float_becomes_int "1.0": f32
);

// -- String --
good!(string_literal "\"foobar\"" == "foobar".to_owned());

// Weird bug in postgres_serde
// bug_sql!(float_32 "1.1": f32);
// bug_sql!(float_64 "1.1": f64);

// --- Objects

good!(extract_int r#"int({"foo": 1}.foo)"# == 1);

// -- Proto Messages --
bug_cel!(
    /// Named classes are not yet supported in the rust CEL interpreter
    empty_message_compare "Foo{} == Foo{}": bool
);

bug_both!(
    /// Named classes are not yet supported in the rust CEL interpreter
    ///
    /// Namespaced classes are not yet implemented in the transpiler
    multi_level_message_classes "some.Class{}": Value
);

// --- Macros

// -- Map Array

good! {
    map_array_correct
    "[1, 2, 3].map(x, int(x) + 1) == [2, 3, 4]": bool;
    map_array_not_incorrect
    "[1, 2, 3].map(x, int(x) + 1) != [1, 2, 3]": bool;
    filter_array_correct
    "[1, 2, 3].filter(x, int(x) > 1) == [2, 3]": bool;

}

type IntList = Vec<i64>;

bug_cel!(filter_map_array_correct "[1, 2, 3].map(x, int(x) > 1, int(x) + 1)": IntList);
diverge!(compare_filter_map_array "[1, 2, 3].map(x, int(x) > 1, int(x) + 1) == [3, 4]": bool);

// -- Map Object

type StringList = Vec<String>;
bug_sql!(
    /// Weird serde nonsens again
    ///
    /// Running `SELECT * FROM (SELECT array((SELECT "x"
    /// FROM (SELECT ARRAY ['a','b'] AS "x") AS "_"))) AS "data"`
    /// works fine
    map_object_correct r#"{"a": 1, "b": 1}.map(x, x)"#: StringList
);
