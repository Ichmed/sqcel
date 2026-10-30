use sqcel::assert_returntype;
use sqcel::types::JsonType;
use sqcel::types::{
    Cell,
    SqlType::{self, *},
    Type,
};

fn literal(l: impl Into<SqlType>) -> Cell {
    Cell::Literal(l.into())
}

fn col(l: impl Into<SqlType>) -> Type {
    Type::Column(None, l.into())
}

fn named_col(n: &str, t: impl Into<SqlType>) -> Type {
    Type::Column(Some(n.to_string()), t.into())
}

assert_returntype!(boolean_true "true" => literal(Boolean));
assert_returntype!(boolean_false "false" => literal(Boolean));
assert_returntype!(integer "123" => literal(BigInteger));
assert_returntype!(float "1.23" => literal(Double));
assert_returntype!(string "\"Hello, World!\"" => literal(Text));

assert_returntype!( or "true || false" => Boolean);
assert_returntype!(and "true && false" => Boolean);

// assert_returntype!(array "[1, 2, 3]" => JsonType::List(JsonType::Number.into()));
assert_returntype!(array "[1, 2, 3]" => JsonType::List(JsonType::Any.into()));

// // assert_returntype!(map "[1, 2, 3].map(x, x + 1)" => named_col("x", BigInteger));
// assert_returntype!(map "[1, 2, 3].map(x, x + 1)" => named_col("x", JsonType::Number));
assert_returntype!(map "[1, 2, 3].map(x, x + 1)" => named_col("x", JsonType::Any));
assert_returntype!(map_cast "[1, 2, 3].map(x, int(x))" => col(Integer));

assert_returntype!(filter "[1, 2, 3].filter(x, x > 1)" => named_col("x", JsonType::Any));

assert_returntype!(cast_string "string(1)" => Text);
