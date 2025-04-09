use cel_parser::{ArithmeticOp, Atom, Expression as CelExpr, Member, RelationOp, UnaryOp};
use sea_query::{
    Alias, BinOper, CaseStatement, ColumnRef, DynIden, Func, SeaRc, SimpleExpr as SqlExpr, Value,
};
use std::{collections::HashMap, sync::Arc};
use thiserror::Error;

#[derive(Debug, Default)]
pub struct Transpiler {
    /// Identifiers included in vars will become
    /// constants during conversion (instead of bind params)
    pub(crate) variables: HashMap<String, Value>,
    /// These identifiers are treated as column names
    pub(crate) columns: HashMap<String, String>,
    /// These identifiers are treated as table names
    ///
    /// Members of theses identifiers will be interpreted as column names
    pub(crate) tables: HashMap<String, String>,
    /// These identifiers are treated as schema names
    ///
    /// Members of theses identifiers will be interpreted as table names
    ///
    /// Members of those identifiers will be interpreted as column names
    pub(crate) schemas: HashMap<String, String>,
}
impl Transpiler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn transpile(&self, src: &str) -> Result<SqlExpr> {
        cel_parser::parse(src)?.into_sql(self)
    }

    pub fn var(mut self, key: impl ToString, val: impl Into<Value>) -> Self {
        self.variables.insert(key.to_string(), val.into());
        self
    }

    pub fn column(mut self, column: impl ToString) -> Self {
        self.columns.insert(column.to_string(), column.to_string());
        self
    }

    pub fn column_alias(mut self, alias: impl ToString, column: impl ToString) -> Self {
        self.columns.insert(alias.to_string(), column.to_string());
        self
    }

    pub fn table(mut self, table: impl ToString) -> Self {
        self.tables.insert(table.to_string(), table.to_string());
        self
    }

    pub fn table_alias(mut self, alias: impl ToString, table: impl ToString) -> Self {
        self.tables.insert(alias.to_string(), table.to_string());
        self
    }

    pub fn schema(mut self, schema: impl ToString) -> Self {
        self.schemas.insert(schema.to_string(), schema.to_string());
        self
    }

    pub fn schema_alias(mut self, alias: impl ToString, schema: impl ToString) -> Self {
        self.schemas.insert(alias.to_string(), schema.to_string());
        self
    }

    fn resolve_ident(&self, iden: &str) -> Result<SqlExpr> {
        Ok(match iden {
            iden if self.columns.contains_key(iden) => {
                SqlExpr::Column(ColumnRef::Column(alias(self.columns.get(iden).unwrap())))
            }
            iden => self
                .variables
                .get(iden)
                .map(|v| SqlExpr::Constant(v.clone()))
                .unwrap_or_else(|| {
                    SqlExpr::Value(sea_query::Value::String(Some(Box::new(iden.to_owned()))))
                }),
        })
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
            CelExpr::FunctionCall(ident, receiver, args) => {
                function_call(*ident, receiver, args, tp)?
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
        Func::cust(Alias::new("jsonb_build_list")).args(
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

fn obj(items: Vec<(Arc<String>, CelExpr)>, tp: &Transpiler) -> Result<SqlExpr> {
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
    receiver: Option<Box<CelExpr>>,
    args: Vec<CelExpr>,
    tp: &Transpiler,
) -> Result<SqlExpr> {
    match ident {
        // Clone makes me sad ;(
        CelExpr::Ident(i) => match (i.as_str(), receiver, args.as_slice()) {
            ("int", _, [exp, ..]) => json_cast("integer", exp.clone(), tp),
            ("str", _, [exp, ..]) => json_cast("text", exp.clone(), tp),
            ("bool", _, [exp, ..]) => json_cast("boolean", exp.clone(), tp),
            ("float", _, [exp, ..]) => json_cast("double", exp.clone(), tp),
            (name, receiver, _) => Ok(SqlExpr::FunctionCall(
                Func::cust(Alias::new(name)).args(
                    receiver
                        .map(|x| *x)
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
            // TODO: validate the structure first
            (CelExpr::Ident(_), Member::Fields(items)) => obj(items, tp)?,
            // Resolve table.column access
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
                (s, t) => SqlExpr::Binary(
                    Box::new(CelExpr::Member(Box::new(s), Box::new(t)).into_sql(tp)?),
                    BinOper::Custom("->"),
                    Box::new(Member::Attribute(c).into_sql(tp)?),
                ),
            },
            (receiver, field) => SqlExpr::Binary(
                Box::new(receiver.into_sql(tp)?),
                BinOper::Custom("->"),
                Box::new(field.into_sql(tp)?),
            ),
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
            Member::Fields(items) => obj(items, tp)?,
        })
    }
}

#[cfg(test)]
mod test {
    use sea_query::{PostgresQueryBuilder, Query};

    use crate::transpiler::Transpiler;

    use super::ToSql;
    #[test]
    fn test() {
        let x = "int({bar: 5}.bar) + 1 == -3 ? 10 : 5";
        println!("{x}");
        let x = cel_parser::parse(x)
            .unwrap()
            .into_sql(&Transpiler::new())
            .unwrap();
        let x = Query::select().expr(x).build(PostgresQueryBuilder);
        println!("{}", x.0);
    }

    #[test]
    fn funtest() {
        dbg!(cel_parser::parse(r#"M{}.value"#).unwrap());
    }

    #[test]
    fn trigger() {
        let q = Query::select()
            .expr(
                Transpiler::new()
                    .var("input_a", "foo")
                    .column("condition")
                    .transpile(r#"{"test": condition ? input_a : 5, "data": data}"#)
                    .unwrap(),
            )
            .build(PostgresQueryBuilder);
        println!("{}", q.0);
        dbg!(q.1);
    }

    #[test]
    fn bike() {
        let tp = Transpiler::new()
            .column("my_col")
            .column_alias("my_alias", "hidden")
            .table("my_tab")
            .schema("my_sch");

        fn helper(code: &str, tp: &Transpiler) -> String {
            let q = Query::select()
                .expr(tp.transpile(code).unwrap())
                .build(PostgresQueryBuilder)
                .0;
            q
        }

        assert_eq!(helper("my_col", &tp), r#"SELECT "my_col""#);
        assert_eq!(helper("my_alias", &tp), r#"SELECT "hidden""#);
        assert_eq!(helper("my_tab", &tp), r#"SELECT $1"#);
        assert_eq!(helper("other", &tp), r#"SELECT $1"#);
        assert_eq!(helper("my_tab.my_col", &tp), r#"SELECT "my_tab"."my_col""#);
        assert_eq!(helper("other.thing", &tp), r#"SELECT $1 -> 'thing'"#);
        assert_eq!(helper("other.my_col", &tp), r#"SELECT $1 -> 'my_col'"#);
        assert_eq!(
            helper("my_sch.my_tab.my_col", &tp),
            r#"SELECT "my_sch"."my_tab"."my_col""#
        );
        assert_eq!(
            helper("some.thing.else", &tp),
            r#"SELECT ($1 -> 'thing') -> 'else'"#
        );
        assert_eq!(
            helper("some.my_tab.else", &tp),
            r#"SELECT ($1 -> 'my_tab') -> 'else'"#
        );

        assert_eq!(
            helper("four.things.are.nice", &tp),
            r#"SELECT (($1 -> 'things') -> 'are') -> 'nice'"#
        );
        assert_eq!(
            helper("my_sch.things.are.nice", &tp),
            r#"SELECT "my_sch"."things"."are" -> 'nice'"#
        );
    }
}
