#![cfg(test)]
use sqcel::*;

assert_compiles!(simple_bool "true");
assert_does_not_compile!(unknown_ident "what_even_is_this");

// TODO: Type system inherently broken again
// x.list bellow is interpreted as a whole column by the map, not as an entry in the column
// should iterating a column of type list iterate the list before the column?
// should it be impossible to iterate a raw column?
// assert_compiles!(collect_modified_object r#"
// foo
//   .group_by(x,
//     x.list.map(x, string(x)).join("."),
//   )
//   .count()
//   .collect_object()
// "#);
