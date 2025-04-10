A crate to convert CEL expressions to SQL expression.

## **NOTE ON CORRECTNESS** 
This crate uses a mostly naive mapping of CEL expressions to equivalent SQL expression when converting. As such certain nuances as described in the [CEL language definition](https://github.com/google/cel-spec/blob/master/doc/langdef.md) may not be reproduced correctly, since the evaluation relies on SQL semantics rather then CEL semantics.

A non-exhaustive list of equivalent/diverging expressions can be found in `fuzzing.rs`


## Objects/Messages
**Note**: multi-level names like `foo.bar.Message` are not yet supported

Named objects (like `Foo{bar: 123}`) will be checked against the list of known protobuf definitions, and only
accepted if they contain _all_ required fields and _no_ unknown fields.

This behaviour can be controlled with the following flags:
- `accept_unknown_types` (will still validate known types)

## Variables
Top level idents in a CEL expression are treated as variables and resolved as follows:

- Constants that are defined in the transpiler are replaced with literals
- Schemas, tables and columns are replaced with column access
- All leftover variables are exposed as bindings

Note that in some contexts (such as trigger creation) bindings are not allowed, so all idents should refer to columns or constants.

## Functions
All functions that are not explicitly defined are assumed to be DB internal functions and are forwarded accordingly. In this case, method calls are translated to function calls with the receiver as the first argument e.g. `foo.bar("baz")` becomes `bar(foo, 'baz')`

### Builtin Functions
#### Conversions
The functions 
 - `int(val) -> sql::integer`
 - `bool(val) -> sql::boolean`
 - `string(val) -> sql::text`
 - `float(val) -> sql::double`

Should be used when comparing values with something else (another value or a literal) e.g. `int(data.amount) > 10` instead of `data.amount > 10`.

Doing so:
- Asserts the correct type
- Allows you to compare  `jsonb` with bolean and integer literals
- Allows you to comapre `json` to other values _at all_
- Prevents weirdness when using `json` or `jsonb`, since `json::null` != `NULL` in postgres.


### Custom Functions
Not yet implemented