#![cfg(feature = "fuzzing")]
#![allow(clippy::doc_overindented_list_items)]
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
use serde_json::Value;
use sqcel::{
    PostgresQueryBuilder, Query, Transpiler,
    intermediate::{ToIntermediate, ToSql},
    types::{JsonType, TypeConversion},
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

async fn from_pg(src: &str) -> anyhow::Result<Value> {
    let tp = Transpiler::new().reduce(false).build();

    // let q = dbg!(dbg!(cel_parser::parse(src))?.to_sqcel(&tp))?.to_sql(&tp)?;
    let q = cel_parser::parse(src)?.to_sqcel(&tp)?.to_sql(&tp)?;

    let sql = Query::select()
        .expr(JsonType::Any.try_convert(q).unwrap().expr)
        .take()
        .build(PostgresQueryBuilder)
        .0;

    eprintln!("{sql}");

    let from_pg = connect_pg()
        .await
        .query(sql.as_str(), Default::default())
        .await?
        .remove(0);

    Ok(from_pg.get::<usize, Value>(0).clone())
}

async fn from_cel(code: &str) -> anyhow::Result<Value> {
    let val = Program::compile(code)?.execute(&Default::default())?;

    match val.json() {
        Ok(val) => Ok(val),
        Err(err) => Err(anyhow!("{err}")),
    }
}

#[allow(unused)]
macro_rules! good {
    ($($(#[$($attrss:tt)*])* $name:ident $src:literal $(== $expected:expr)?);* $(;)?) => {
        $(
            #[tokio::test]
            #[allow(unused_mut)]
            #[allow(unused_assignments)]

            $(#[$($attrss)*])*
            async fn $name() {
                let pg = from_pg($src).await.unwrap();
                let cel = from_cel($src).await.unwrap();


                assert_eq!(pg, cel);
                $(
                    let expected = serde_json::to_value($expected).unwrap();
                    assert_eq!(pg, expected);
                )?
            }
        )*
    };
}

#[allow(unused)]
macro_rules! diverge {
    ($(#[$($attrss:tt)*])* $name:ident $src:literal) => {
        #[tokio::test]
        #[doc = "Results diverge\n\n"]
        $(#[$($attrss)*])*
        async fn $name() {
            let from_pg = from_pg($src).await.unwrap();
            let from_cel = from_cel($src).await.unwrap();

            assert_ne!(from_pg, from_cel)
        }
    };
}

#[allow(unused)]
macro_rules! bug_sql {
    ($(#[$($attrss:tt)*])* $name:ident $src:literal) => {
        #[tokio::test]
        #[doc = "Bug in Transpiler\n\n"]
        $(#[$($attrss)*])*
        async fn $name() {
            let cel = from_cel($src).await.unwrap();
            if let Ok(pg) = from_pg($src).await {
                assert_ne!(pg, cel)
            }

        }
    };
}

#[allow(unused)]
macro_rules! bug_cel {
    ($(#[$($attrss:tt)*])* $name:ident $src:literal) => {
        #[tokio::test]
        #[doc = "Bug in the Rust CEL interpreter\n\n"]
        $(#[$($attrss)*])*
        async fn $name() {
            let pg = from_pg($src).await.unwrap();
            if let Ok(cel) = from_cel($src).await {
                assert_ne!(pg, cel)
            }
        }
    };
}

#[allow(unused)]
macro_rules! bug_both {
    ($(#[$($attrss:tt)*])* $name:ident $src:literal) => {
        #[tokio::test]
        #[doc = "Separate bugs in both the transpiler and the Rust CEL interpreter\n\n"]
        $(#[$($attrss)*])*
        async fn $name() {
            from_pg($src).await.unwrap_err();
            from_cel($src).await.unwrap_err();
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
good! {
    int32
    "1";
    simple_sum
    "1 + 2 == 3" == true;
    int_compare
    "1 == 1" == true;
}

// -- float --
bug_sql!(
    /// Postgres removes the trailing zeroes
    float_becomes_int "1.0"
);

// -- String --
good!(string_literal "\"foobar\"" == "foobar");

// Weird bug in postgres_serde
good!(float_32 "1.1" == 1.1);

// --- Arrays
good!(
    simple_index_array r#"[1, 2, 3][1]"# == 2;
    nested_index_array r#"{"a": [1, 2, 3]}.a[1]"# == 2
);

// --- Objects

good!(extract_int r#"int({"foo": 1}.foo)"# == 1);

// disabled until .proto file is included
// -- Proto Messages --
// bug_cel!(
//     /// Named classes are not yet supported in the rust CEL interpreter
//     empty_message_compare "Foo{} == Foo{}"
// );

bug_both!(
    /// Named classes are not yet supported in the rust CEL interpreter
    ///
    /// Namespaced classes are not yet implemented in the transpiler
    multi_level_message_classes "some.Class{}"
);

// --- Macros

// -- Map Array

good! {
    array
    "[1, 2, 3]" == [1, 2, 3];
    array_equals
    "[1, 2, 3] == [1, 2, 3]";
    map_array
    "[1, 2, 3].map(x, int(x) + 1)" == [2, 3, 4];
    filter_array_correct
    "[1, 2, 3].filter(x, int(x) > 1)" == [2, 3];

}

bug_sql! {
    map_array_compare
    "[1, 2, 3].map(x, int(x) + 1) == [2, 3, 4]"
}
bug_sql!(compare_filter_map_array "[1, 2, 3].map(x, int(x) > 1, int(x) + 1) == [3, 4]");

bug_cel!(filter_map_array_correct "[1, 2, 3].map(x, int(x) > 1, int(int(x) + 1))");

// -- Map Object

good!(
    map_object_correct
    r#"{"a": 1, "b": 1}.map(x, "haha")"# == ["haha", "haha"]
);
