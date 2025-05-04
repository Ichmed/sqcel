use cel_parser::{ArithmeticOp, Atom, Expression as CelExpr, Member, RelationOp, UnaryOp};
use derive_builder::Builder;
use sea_query::{
    Alias, BinOper, CaseStatement, ColumnRef, DynIden, Func, Query, SeaRc, SimpleExpr as SqlExpr,
    SubQueryStatement, Value,
};
use std::{collections::HashMap, sync::Arc};
use thiserror::Error;

/// A simple example
///```
/// # use sqcel::{Transpiler, Query, PostgresQueryBuilder};
/// // Create a `Transpiler`
/// let my_expr = Transpiler::new().transpile("5").unwrap();
/// // Use a sea_query `Query` to actually use the result
/// assert_eq!(
///     Query::select().expr(my_expr).to_string(PostgresQueryBuilder),
///     r#"SELECT 5"#
/// )
///```
/// Or more complex
///```
/// # use sqcel::{Transpiler, Query, PostgresQueryBuilder};
/// // Create a `Transpiler` and set a variable and column
/// let tp = Transpiler::new().var("foo", 2).column("some_col").build();
/// let code = r#"int({"val": foo}.foo) + 1 + some_col"#;
/// let my_expr = tp.transpile(code).unwrap();
/// assert_eq!(
///     Query::select().expr(my_expr).to_string(PostgresQueryBuilder),
///     r#"SELECT CAST(nullif(jsonb_build_object('val', 2) -> 'foo', 'null') AS integer) + 1 + "some_col""#
/// )
///```
#[derive(Debug, Default, Builder)]
#[builder(default, build_fn(name = "_build", private))]
pub struct Transpiler {
    /// Identifiers included in vars will become
    /// constants during conversion (instead of bind params)
    pub(crate) variables: HashMap<String, SqlExpr>,
    /// These identifiers are treated as column names
    pub(crate) columns: HashMap<String, String>,
    /// These identifiers are treated as table names
    ///
    /// Members of theses identifiers will be interpreted as column names
    pub(crate) tables: HashMap<String, String>,
    /// These identifiers are treated as record names. This is usually
    /// only usefull for use in triggers where the records `NEW` and `OLD`
    /// exist
    ///
    /// Members of theses identifiers will be interpreted as column names
    pub(crate) records: HashMap<String, String>,
    /// These identifiers are treated as schema names
    ///
    /// Members of theses identifiers will be interpreted as table names
    ///
    /// Members of those identifiers will be interpreted as column names
    pub(crate) schemas: HashMap<String, String>,

    /// List of known message types
    pub(crate) types: HashMap<String, protobuf::descriptor::DescriptorProto>,

    /// Whether to accept unknown types
    ///
    /// Known types will still be valdiated
    pub(crate) accept_unknown_types: bool,

    /// Transform all column accesses into record accesses
    ///
    /// e.g. `foo` becomes `NEW."foo"`
    pub(crate) trigger_mode: bool,
}

impl Transpiler {
    pub fn new() -> TranspilerBuilder {
        TranspilerBuilder::create_empty()
    }

    pub fn transpile(&self, src: &str) -> Result<SqlExpr> {
        self.transpile_expr(cel_parser::parse(src)?)
    }

    pub fn transpile_expr(&self, expr: CelExpr) -> Result<SqlExpr> {
        expr.into_sql(self)
    }

    pub fn to_builder(&self) -> TranspilerBuilder {
        let Transpiler {
            variables,
            columns,
            tables,
            records,
            schemas,
            types,
            accept_unknown_types,
            trigger_mode,
        } = self;
        TranspilerBuilder {
            variables: Some(variables.clone()),
            columns: Some(columns.clone()),
            tables: Some(tables.clone()),
            records: Some(records.clone()),
            schemas: Some(schemas.clone()),
            types: Some(types.clone()),
            accept_unknown_types: Some(accept_unknown_types.clone()),
            trigger_mode: Some(trigger_mode.clone()),
        }
    }

    fn resolve_ident(&self, iden: &str) -> Result<SqlExpr> {
        Ok(match iden {
            iden if self.columns.contains_key(iden) => {
                if self.trigger_mode {
                    SqlExpr::Custom(format!("NEW.{iden:?}"))
                } else {
                    SqlExpr::Column(ColumnRef::Column(alias(self.columns.get(iden).unwrap())))
                }
            }
            iden => self
                .variables
                .get(iden)
                .map(|v| v.clone())
                .unwrap_or_else(|| {
                    // I know this is wrong, but there is no SqlExpr::Bind,
                    // so this is the only way to propagate the position and name
                    // of the param "correctly"
                    SqlExpr::Value(sea_query::Value::String(Some(Box::new(iden.to_owned()))))
                }),
        })
    }
}

impl TranspilerBuilder {
    pub fn transpile(&self, src: &str) -> Result<SqlExpr> {
        self.transpile_expr(cel_parser::parse(src)?)
    }

    pub fn transpile_expr(&self, expr: CelExpr) -> Result<SqlExpr> {
        expr.into_sql(&self._build().unwrap())
    }

    pub fn var(&mut self, key: impl ToString, val: impl Into<Value>) -> &mut Self {
        self.variables
            .get_or_insert_default()
            .insert(key.to_string(), SqlExpr::Constant(val.into()));
        self
    }

    pub fn vars(
        &mut self,
        vars: impl IntoIterator<Item = (impl ToString, impl Into<Value>)>,
    ) -> &mut Self {
        self.variables.get_or_insert_default().extend(
            vars.into_iter()
                .map(|(key, val)| (key.to_string(), SqlExpr::Constant(val.into()))),
        );
        self
    }

    pub fn build(&mut self) -> Transpiler {
        self._build().unwrap()
    }

    pub fn column(&mut self, column: impl ToString) -> &mut Self {
        self.columns
            .get_or_insert_default()
            .insert(column.to_string(), column.to_string());
        self
    }

    pub fn column_alias(&mut self, alias: impl ToString, column: impl ToString) -> &mut Self {
        self.columns
            .get_or_insert_default()
            .insert(alias.to_string(), column.to_string());
        self
    }

    pub fn table(&mut self, table: impl ToString) -> &mut Self {
        self.tables
            .get_or_insert_default()
            .insert(table.to_string(), table.to_string());
        self
    }

    pub fn table_alias(&mut self, alias: impl ToString, table: impl ToString) -> &mut Self {
        self.tables
            .get_or_insert_default()
            .insert(alias.to_string(), table.to_string());
        self
    }

    pub fn schema(&mut self, schema: impl ToString) -> &mut Self {
        self.schemas
            .get_or_insert_default()
            .insert(schema.to_string(), schema.to_string());
        self
    }

    pub fn schema_alias(&mut self, alias: impl ToString, schema: impl ToString) -> &mut Self {
        self.schemas
            .get_or_insert_default()
            .insert(alias.to_string(), schema.to_string());
        self
    }

    pub fn record(&mut self, record: impl ToString) -> &mut Self {
        self.records
            .get_or_insert_default()
            .insert(record.to_string(), record.to_string());
        self
    }

    pub fn record_alias(&mut self, alias: impl ToString, record: impl ToString) -> &mut Self {
        self.records
            .get_or_insert_default()
            .insert(alias.to_string(), record.to_string());
        self
    }
}

fn alias(s: impl ToString) -> DynIden {
    SeaRc::new(Alias::new(s.to_string()))
}

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("A non ident column, field or schema name was supplied")]
    NonIdentColumnFieldSchema,
    #[error(transparent)]
    CelError(#[from] cel_parser::error::ParseError),
    #[error("A non ident function name was supplied")]
    NonIdentFunctionName,
    #[error(
        "Transpiler bug: Tried to convert Members::Fields directly (should be caught in a previous stage)"
    )]
    BugDirectFields,
    #[error("Unknown message type `{}`", .0)]
    UnknownType(String),
    #[error("Unknown field `{}` on type `{}`", .1, .0)]
    UnknownField(String, String),
    #[error("Message of type `{}` is missing the fields: {:?}", .0, .1)]
    MissingFields(String, Vec<String>),
}

pub type Result<T> = std::result::Result<T, ParseError>;

trait ToSql<E> {
    fn into_sql(self, tp: &Transpiler) -> Result<E>;
}

impl ToSql<SqlExpr> for CelExpr {
    fn into_sql(self, tp: &Transpiler) -> Result<SqlExpr> {
        Ok(match self {
            CelExpr::Arithmetic(lhs, op, rhs) => lhs
                .into_sql(tp)?
                .binary(op.into_sql(tp)?, rhs.into_sql(tp)?),
            CelExpr::Relation(lhs, op, rhs) => lhs
                .into_sql(tp)?
                .binary(op.into_sql(tp)?, rhs.into_sql(tp)?),
            CelExpr::Ternary(r#if, then, r#else) => SqlExpr::Case(Box::new(
                CaseStatement::new()
                    .case(r#if.into_sql(tp)?, then.into_sql(tp)?)
                    .finally(r#else.into_sql(tp)?),
            )),
            CelExpr::Or(a, b) => a.into_sql(tp)?.or(b.into_sql(tp)?),
            CelExpr::And(a, b) => a.into_sql(tp)?.add(b.into_sql(tp)?),
            CelExpr::Unary(unary_op, expression) => (unary_op, *expression).into_sql(tp)?,
            CelExpr::Member(expression, member) => (*expression, *member).into_sql(tp)?,
            CelExpr::FunctionCall(fun, rec, args) => {
                function_call(*fun, rec.map(|x| *x), args, tp)?
            }
            CelExpr::List(items) => list(items, tp)?,
            CelExpr::Map(items) => map(items, tp)?,
            CelExpr::Atom(atom) => atom.into_sql(tp)?,
            CelExpr::Ident(iden) => tp.resolve_ident(&iden)?,
        })
    }
}

fn list(items: Vec<CelExpr>, tp: &Transpiler) -> Result<SqlExpr> {
    Ok(SqlExpr::FunctionCall(
        Func::cust(Alias::new("jsonb_build_array")).args(
            items
                .into_iter()
                .map(|x| x.into_sql(tp))
                .collect::<Result<Vec<_>>>()?,
        ),
    ))
}

fn map(items: Vec<(CelExpr, CelExpr)>, tp: &Transpiler) -> Result<SqlExpr> {
    Ok(SqlExpr::FunctionCall(
        Func::cust(Alias::new("jsonb_build_object")).args(
            items
                .into_iter()
                .flat_map(|(a, b)| [a.into_sql(tp), b.into_sql(tp)])
                .collect::<Result<Vec<_>>>()?,
        ),
    ))
}

/// Verify and build an object
fn obj(type_name: &str, items: Vec<(Arc<String>, CelExpr)>, tp: &Transpiler) -> Result<SqlExpr> {
    // Verify struct layout
    let type_info = match tp.types.get(type_name) {
        Some(ty) => ty,
        None if tp.accept_unknown_types => return _obj(items, tp),
        None => return Err(ParseError::UnknownType(type_name.to_owned())),
    };

    let mut fields: HashMap<String, bool> = type_info
        .field
        .iter()
        .flat_map(|f| {
            f.name
                .as_ref()
                .map(|name| (name.clone(), f.proto3_optional.unwrap_or_default()))
        })
        .collect();

    // Remove all fields that _are_ in the message and error if there is an unknwon field
    items
        .iter()
        .flat_map(|(name, _)| fields.remove(&**name).is_none().then_some(name))
        .next()
        .map(|f| {
            Err(ParseError::UnknownField(
                type_name.to_owned(),
                (**f).clone(),
            ))
        })
        .unwrap_or(Ok(()))?;

    // Error if there is a field missing in the message that is _not_ optional
    let missing_fields: Vec<_> = fields
        .into_iter()
        .filter_map(|(k, v)| (!v).then_some(k))
        .collect();

    if !missing_fields.is_empty() {
        return Err(ParseError::MissingFields(
            type_name.to_owned(),
            missing_fields,
        ));
    }

    _obj(items, tp)
}

/// Build an object directly without comparing it to the list of known types first
fn _obj(items: Vec<(Arc<String>, CelExpr)>, tp: &Transpiler) -> Result<SqlExpr> {
    Ok(SqlExpr::FunctionCall(
        Func::cust(Alias::new("jsonb_build_object")).args(
            items
                .into_iter()
                .flat_map(|(a, b)| {
                    [
                        Ok(SqlExpr::Constant(Value::String(Some(Box::new(
                            (*a).clone(),
                        ))))),
                        b.into_sql(tp),
                    ]
                })
                .collect::<Result<Vec<_>>>()?,
        ),
    ))
}

fn function_call(
    ident: CelExpr,
    receiver: Option<CelExpr>,
    args: Vec<CelExpr>,
    tp: &Transpiler,
) -> Result<SqlExpr> {
    match ident {
        // Clone makes me sad ;(
        CelExpr::Ident(i) => match (i.as_str(), receiver, args.as_slice()) {
            // Default conversions
            ("int", _, [exp]) => json_cast("integer", exp.clone(), tp),
            ("string", _, [exp]) => json_cast("text", exp.clone(), tp),
            ("bool", _, [exp]) => json_cast("boolean", exp.clone(), tp),
            ("double", _, [exp]) => json_cast("double precision", exp.clone(), tp),

            // Custom conversions

            // Macros
            ("map", Some(rec), [CelExpr::Ident(a), transform]) => {
                let inner_context = tp.to_builder().column(a).build();

                let transform = inner_context.transpile_expr(transform.clone())?;
                let rec = rec.into_sql(tp)?;
                let mut expr_inner = Query::select();
                expr_inner.expr(transform).from_function(
                    Func::cust(alias("jsonb_array_elements")).arg(rec.clone()),
                    // Func::coalesce([
                    //     SqlExpr::FunctionCall(
                    //         Func::cust(alias("jsonb_array_elements")).arg(rec.clone()),
                    //     ),
                    //     SqlExpr::FunctionCall(Func::cast_as(
                    //         Func::cust(alias("jsonb_object_keys")).arg(rec),
                    //         alias("jsonb"),
                    //     )),
                    // ]),
                    alias(a),
                );

                Ok(SqlExpr::FunctionCall(Func::cast_as(
                    Func::cust(alias("to_json")).arg(Func::cust(alias("ARRAY")).arg(
                        SqlExpr::SubQuery(
                            None,
                            Box::new(SubQueryStatement::SelectStatement(expr_inner)),
                        ),
                    )),
                    alias("jsonb"),
                )))
            }
            ("map", Some(rec), [CelExpr::Ident(a), predicate, transform]) => {
                let inner_context = tp.to_builder().column(a).build();

                let predicate = inner_context.transpile_expr(predicate.clone())?;
                let transform = inner_context.transpile_expr(transform.clone())?;

                let rec = rec.into_sql(tp)?;

                let mut expr_inner = Query::select();
                expr_inner
                    .expr(transform)
                    .and_where(predicate)
                    .from_function(
                        Func::cust(alias("jsonb_array_elements")).arg(rec.clone()),
                        // Func::coalesce([
                        //     SqlExpr::FunctionCall(
                        //         Func::cust(alias("jsonb_array_elements")).arg(rec.clone()),
                        //     ),
                        //     SqlExpr::FunctionCall(Func::cast_as(
                        //         Func::cust(alias("jsonb_object_keys")).arg(rec),
                        //         alias("jsonb"),
                        //     )),
                        // ]),
                        alias(a),
                    );

                Ok(SqlExpr::FunctionCall(Func::cast_as(
                    Func::cust(alias("to_json")).arg(Func::cust(alias("ARRAY")).arg(
                        SqlExpr::SubQuery(
                            None,
                            Box::new(SubQueryStatement::SelectStatement(expr_inner)),
                        ),
                    )),
                    alias("jsonb"),
                )))
            }
            ("filter", Some(rec), [CelExpr::Ident(a), predicate]) => {
                let inner_context = tp.to_builder().column(a).build();

                let predicate = inner_context.transpile_expr(predicate.clone())?;

                let rec = rec.into_sql(tp)?;

                let mut expr_inner = Query::select();
                expr_inner
                    .column(alias(a))
                    .and_where(predicate)
                    .from_function(
                        Func::cust(alias("jsonb_array_elements")).arg(rec.clone()),
                        // Func::coalesce([
                        //     SqlExpr::FunctionCall(
                        //         Func::cust(alias("jsonb_array_elements")).arg(rec.clone()),
                        //     ),
                        //     SqlExpr::FunctionCall(Func::cast_as(
                        //         Func::cust(alias("jsonb_object_keys")).arg(rec),
                        //         alias("jsonb"),
                        //     )),
                        // ]),
                        alias(a),
                    );

                Ok(SqlExpr::FunctionCall(Func::cast_as(
                    Func::cust(alias("to_json")).arg(Func::cust(alias("ARRAY")).arg(
                        SqlExpr::SubQuery(
                            None,
                            Box::new(SubQueryStatement::SelectStatement(expr_inner)),
                        ),
                    )),
                    alias("jsonb"),
                )))
            }
            // Fall through
            (name, receiver, _) => Ok(SqlExpr::FunctionCall(
                Func::cust(Alias::new(name)).args(
                    receiver
                        .into_iter()
                        .chain(args)
                        .map(|x| x.into_sql(tp))
                        .collect::<Result<Vec<_>>>()?,
                ),
            )),
        },
        _ => Err(ParseError::NonIdentFunctionName),
    }
}

#[inline]
fn json_cast(ty: &str, exp: CelExpr, tp: &Transpiler) -> Result<SqlExpr> {
    Ok(
        SqlExpr::FunctionCall(Func::cust(Alias::new("nullif")).arg(exp.into_sql(tp)?).arg(
            SqlExpr::Constant(Value::String(Some(Box::new("null".to_owned())))),
        ))
        .cast_as(Alias::new(ty)),
    )
}

impl ToSql<BinOper> for ArithmeticOp {
    fn into_sql(self, _tp: &Transpiler) -> Result<BinOper> {
        Ok(match self {
            ArithmeticOp::Add => BinOper::Add,
            ArithmeticOp::Subtract => BinOper::Sub,
            ArithmeticOp::Divide => BinOper::Div,
            ArithmeticOp::Multiply => BinOper::Mul,
            ArithmeticOp::Modulus => BinOper::Mod,
        })
    }
}

impl ToSql<BinOper> for RelationOp {
    fn into_sql(self, _tp: &Transpiler) -> Result<BinOper> {
        Ok(match self {
            RelationOp::LessThan => BinOper::SmallerThan,
            RelationOp::LessThanEq => BinOper::SmallerThanOrEqual,
            RelationOp::GreaterThan => BinOper::GreaterThan,
            RelationOp::GreaterThanEq => BinOper::GreaterThanOrEqual,
            RelationOp::Equals => BinOper::Equal,
            RelationOp::NotEquals => BinOper::NotEqual,
            RelationOp::In => BinOper::In,
        })
    }
}

impl ToSql<SqlExpr> for Atom {
    fn into_sql(self, _tp: &Transpiler) -> Result<SqlExpr> {
        Ok(SqlExpr::Constant(match self {
            Atom::Int(num) => Value::BigInt(Some(num)),
            Atom::UInt(num) => Value::BigUnsigned(Some(num)),
            Atom::Float(f) => Value::Double(Some(f)),
            Atom::String(s) => Value::String(Some(Box::new((*s).clone()))),
            Atom::Bytes(items) => Value::Bytes(Some(Box::new((*items).clone()))),
            Atom::Bool(b) => Value::Bool(Some(b)),
            Atom::Null => Value::Int(None),
        }))
    }
}

impl ToSql<SqlExpr> for (UnaryOp, CelExpr) {
    fn into_sql(self, tp: &Transpiler) -> Result<SqlExpr> {
        Ok(match self {
            (UnaryOp::DoubleMinus | UnaryOp::DoubleNot, x) => x.into_sql(tp)?,
            (UnaryOp::Not, x) => x.into_sql(tp)?.not(),
            (UnaryOp::Minus, x) => SqlExpr::Constant(Value::BigInt(Some(0))).sub(x.into_sql(tp)?),
        })
    }
}

// Variable Access
impl ToSql<SqlExpr> for (CelExpr, Member) {
    fn into_sql(self, tp: &Transpiler) -> Result<SqlExpr> {
        Ok(match self {
            // Turn a protobuf message into a dict
            (CelExpr::Ident(name), Member::Fields(items)) => obj(&name, items, tp)?,
            // Resolve table.column access
            (CelExpr::Ident(t), Member::Attribute(c)) if tp.records.contains_key(&*t) => {
                SqlExpr::Custom(format!("{t}.{c:?}"))
            }
            (CelExpr::Ident(t), Member::Attribute(c)) if tp.tables.contains_key(&*t) => {
                SqlExpr::Column(ColumnRef::TableColumn(
                    alias(tp.tables.get(&*t).unwrap()),
                    alias(c),
                ))
            }
            // Resolve schema.table.column
            (CelExpr::Member(s, t), Member::Attribute(c)) => match (*s, *t) {
                (CelExpr::Ident(s), Member::Attribute(t)) if tp.schemas.contains_key(&*s) => {
                    SqlExpr::Column(ColumnRef::SchemaTableColumn(
                        alias(tp.schemas.get(&*s).unwrap()),
                        alias(t),
                        alias(c),
                    ))
                }
                (s, t) => CelExpr::Member(Box::new(s), Box::new(t))
                    .into_sql(tp)?
                    .cast_as(alias("jsonb"))
                    .cast_json_field(Member::Attribute(c).into_sql(tp)?),
                            },
            (receiver, field) => receiver
                .into_sql(tp)?
                .cast_as(alias("jsonb"))
                .cast_json_field(field.into_sql(tp)?),
                    })
    }
}

impl ToSql<SqlExpr> for Member {
    fn into_sql(self, tp: &Transpiler) -> Result<SqlExpr> {
        Ok(match self {
            Member::Attribute(iden) => {
                SqlExpr::Constant(Value::String(Some(Box::new((*iden).to_owned()))))
            }
            Member::Index(expression) => expression.into_sql(tp)?,
            Member::Fields(_) => return Err(ParseError::BugDirectFields),
        })
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use indoc::indoc;
    use sea_query::{PostgresQueryBuilder, Query};

    use crate::transpiler::Transpiler;

    use super::TranspilerBuilder;

    #[test]
    fn test() {
        let tp = Transpiler::new().build();

        let input = indoc!(r#"[1, 2, 3].map(x, int(x) > 1, int(x) + 1)"#);
        let sql = helper(input, &tp);
        println!("{input}\n\n{sql}");
    }

    fn read_proto() -> HashMap<String, protobuf::descriptor::DescriptorProto> {
        protobuf_parse::Parser::new()
            .pure()
            .include(".")
            .input("test.proto")
            .parse_and_typecheck()
            .unwrap()
            .file_descriptors
            .remove(0)
            .message_type
            .into_iter()
            .map(|m| (m.name().to_owned(), m))
            .collect()
    }

    fn helper(code: &str, tp: &Transpiler) -> String {
        let q = Query::select()
            .expr(tp.transpile(code).unwrap())
            .build(PostgresQueryBuilder)
            .0;
        q
    }

    #[test]
    #[should_panic]
    fn proto_unknwon_field() {
        TranspilerBuilder::create_empty()
            .types(read_proto())
            .transpile(r#"M{unknown: 5}"#)
            .unwrap();
    }

    #[test]
    fn proto_fields() {
        let tp = Transpiler::new().types(read_proto()).build();
        assert_eq!(
            helper(r#"M{needed: 5, not_needed: 5}"#, &tp),
            r#"SELECT jsonb_build_object('needed', 5, 'not_needed', 5)"#
        );
        assert_eq!(
            helper(r#"M{needed: 5}"#, &tp),
            r#"SELECT jsonb_build_object('needed', 5)"#
        );
    }
    #[test]
    #[should_panic]
    fn proto_missing_field() {
        TranspilerBuilder::create_empty()
            .types(read_proto())
            .build()
            .transpile(r#"M{not_needed: 5}"#)
            .unwrap();
    }

    #[test]
    fn table_access() {
        let tp = TranspilerBuilder::create_empty()
            .column("my_col")
            .column_alias("my_alias", "hidden")
            .table("my_tab")
            .schema("my_sch")
            .build();

        assert_eq!(helper("my_col", &tp), r#"SELECT "my_col""#);
        assert_eq!(helper("my_alias", &tp), r#"SELECT "hidden""#);
        assert_eq!(helper("my_tab", &tp), r#"SELECT $1"#);
        assert_eq!(helper("other", &tp), r#"SELECT $1"#);
        assert_eq!(helper("my_tab.my_col", &tp), r#"SELECT "my_tab"."my_col""#);
        assert_eq!(
helper("other.thing", &tp),
r#"SELECT CAST($1 AS jsonb) ->> 'thing'"#
);
        assert_eq!(
helper("other.my_col", &tp),
r#"SELECT CAST($1 AS jsonb) ->> 'my_col'"#
);
        assert_eq!(
            helper("my_sch.my_tab.my_col", &tp),
            r#"SELECT "my_sch"."my_tab"."my_col""#
        );
        assert_eq!(
            helper("some.thing.else", &tp),
            r#"SELECT CAST((CAST($1 AS jsonb) ->> 'thing') AS jsonb) ->> 'else'"#
        );
        assert_eq!(
            helper("some.my_tab.else", &tp),
            r#"SELECT CAST((CAST($1 AS jsonb) ->> 'my_tab') AS jsonb) ->> 'else'"#
        );

        assert_eq!(
            helper("four.things.are.nice", &tp),
            r#"SELECT CAST((CAST((CAST($1 AS jsonb) ->> 'things') AS jsonb) ->> 'are') AS jsonb) ->> 'nice'"#
        );
        assert_eq!(
            helper("my_sch.things.are.nice", &tp),
            r#"SELECT CAST("my_sch"."things"."are" AS jsonb) ->> 'nice'"#
        );
    }
}
