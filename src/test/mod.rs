use crate::{
    Transpiler,
    types::{ColumnType, JsonType},
};

#[macro_export]
macro_rules! compile {
    ($expression:literal) => {
        $crate::compile!($expression in $crate::test::default_env)
    };
    ($expression:literal in $env:path) => {
        $env().transpile($expression)
    };
}
#[macro_export]
macro_rules! assert_compiles {
    ($name:ident $($rest:tt)*) => {
        #[test]
        fn $name() {
            $crate::compile!($($rest)*).unwrap();
        }
    };
}
#[macro_export]
macro_rules! assert_does_not_compile {
    ($name:ident $($rest:tt)*) => {
        #[test]
        fn $name() {
            $crate::compile!($($rest)*).unwrap_err();
        }
    };
}

#[macro_export]
macro_rules! assert_returntype {
    ($name:ident $expression:literal $(in $env:path)? => $expected:expr) => {
        #[test]
        fn $name() {
            assert_eq!($crate::compile!($expression $(in $env)?).unwrap().ty, $expected.into());
        }
    };
}

pub fn default_env() -> Transpiler {
    #[allow(clippy::disallowed_names)]
    let foo = vec![
        ("number", ColumnType::Integer),
        ("list", JsonType::List(JsonType::String.into()).into()),
    ];
    Transpiler::quick(vec![("sch", vec![("foo", foo)])])
}
