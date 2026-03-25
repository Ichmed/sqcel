use crate::{
    Transpiler,
    functions::iter::{IterKind, Iterable},
    intermediate::{
        CantBeAValue, Expression, ExpressionInner, ToSql,
        access_chain::{AccessChain, json_to_iterable},
    },
    sql_extensions::{IntoSqlExpression, SqlExtension},
    structure::{Column, Table},
    transpiler::{Error, Result},
    types::{JsonType, Type, TypedExpression},
    types::{Cell, ColumnType, JsonObject},
};
use indexmap::IndexMap;
use sea_query::{Alias, DynIden, Func, Query, SelectStatement, SimpleExpr, TableRef};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum Variable {
    /// A JSON-Object or Proto-Message
    Object(Object),
    /// A List of Variables
    List(Vec<Expression>),
    /// A String, int, float, etc.
    Atom(Atom),
    /// An SQL query that will evaluate to a list
    SqlSubQuery(Box<SelectStatement>, IndexMap<String, Column>),
    /// An SQL query that will evaluate to a single record
    SqlSubQueryAtom(Box<SelectStatement>, IndexMap<String, Column>),
    /// An SQL expression that will evaluate to anything
    SqlAny(Box<SimpleExpr>),
}

#[derive(Clone, Debug)]
pub struct Object {
    pub data: Vec<(Expression, Expression)>,
    /// The schema to validate this object against.
    /// Some objects don't have a schema and thats OK :)
    pub schema: Option<AccessChain>,
}

impl Object {
    fn verify(&self, tp: &Transpiler) -> Result<()> {
        // Verify struct layout

        let Some(type_name) = self.schema.as_ref() else {
            return Ok(());
        };

        let (type_name, type_info) = if let Some(schema) = type_name.to_path() {
            match tp.types.get(schema.as_ref()) {
                Some(ty) => (schema, ty),
                None if tp.accept_unknown_types => return Ok(()),
                None => return Err(Error::UnknownType(schema.into_owned())),
            }
        } else {
            return Err(Error::Todo("Invalid type"));
        };

        let mut fields: IndexMap<String, bool> = type_info
            .field
            .iter()
            .filter_map(|f| {
                f.name
                    .as_ref()
                    .map(|name| (name.clone(), f.proto3_optional.unwrap_or_default()))
            })
            .collect();

        // Remove all fields that _are_ in the message and error if there is an unknwown field
        self.data
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .find(|name| fields.swap_remove(*name).is_none())
            .map_or(Ok(()), |f| {
                Err(Error::UnknownField(
                    type_name.clone().into_owned(),
                    f.to_owned(),
                ))
            })?;

        // Error if there is a field missing in the message that is _not_ optional
        let missing_fields: Vec<_> = fields
            .into_iter()
            .filter_map(|(k, v)| (!v).then_some(k))
            .collect();

        if !missing_fields.is_empty() {
            return Err(Error::MissingFields(type_name.into_owned(), missing_fields));
        }

        Ok(())
    }
}

impl ToSql for Object {
    fn to_sql(&self, tp: &Transpiler) -> Result<TypedExpression> {
        self.verify(tp)?;
        Ok(TypedExpression {
            ty: Cell::Value(JsonObject::AnyContent.into()).into(),
            expr: SimpleExpr::FunctionCall(
                Func::cust(Alias::new("jsonb_build_object")).args(
                    self.data
                        .iter()
                        .map(|(k, v)| Ok([k.to_sql(tp)?.expr, v.to_sql(tp)?.expr]))
                        .collect::<Result<Vec<_>>>()?
                        .into_iter()
                        .flatten(),
                ),
            ),
        })
    }

    fn returntype(&self, _tp: &Transpiler) -> Type {
        Cell::Literal(JsonObject::AnyContent.into()).into()
    }
}

impl ToSql for Variable {
    fn to_sql(&self, tp: &Transpiler) -> Result<TypedExpression> {
        Ok(match self {
            Self::Object(o) => o.to_sql(tp)?,
            Self::List(variables) => TypedExpression {
                ty: Cell::Value(JsonType::List(Box::new(JsonType::Any)).into()).into(),
                expr: SimpleExpr::FunctionCall(
                    Func::cust(Alias::new("jsonb_build_array")).args(
                        variables
                            .iter()
                            .map(|x| Ok(x.clone().to_sql(tp)?.expr))
                            .collect::<Result<Vec<_>>>()?,
                    ),
                ),
            },
            Self::Atom(atom) => atom.to_sql(tp)?,

            Self::SqlSubQuery(select_statement, cols)
            | Self::SqlSubQueryAtom(select_statement, cols) => (**select_statement)
                .clone()
                .into_expr()
                .with_type(Type::NamedView(
                    cols.iter().map(|c| (c.0.clone(), c.1.ty.clone())).collect(),
                )),
            Self::SqlAny(simple_expr) => TypedExpression {
                ty: Type::Unknown,
                expr: *simple_expr.clone(),
            },
        })
    }

    fn returntype(&self, tp: &Transpiler) -> Type {
        match self {
            Self::Object(_) => Cell::Value(JsonObject::AnyContent.into()).into(),
            Self::List(l) => {
                if let Some(Some(Type::Cell(c))) = l
                    .iter()
                    .map(|x| x.returntype(tp))
                    .map(Some)
                    .reduce(|a, b| if a == b { a } else { None })
                    && let Cell::Literal(ColumnType::Json(j, _)) = c
                {
                    Cell::Value(JsonType::List(Box::new(j)).into()).into()
                } else {
                    Cell::Value(JsonType::List(Box::new(JsonType::Any)).into()).into()
                }
            }
            Self::Atom(atom) => atom.returntype(tp),
            Self::SqlSubQuery(_, cols) | Self::SqlSubQueryAtom(_, cols) => {
                Type::NamedView(cols.iter().map(|c| (c.0.clone(), c.1.ty.clone())).collect())
            }
            Self::SqlAny(_) => Type::Unknown,
        }
    }

    fn try_iterate(&self, tp: &Transpiler, var: DynIden) -> Result<Iterable> {
        Ok(match self {
            Self::Object(object) => Self::List(object.data.iter().map(|x| x.0.clone()).collect())
                .try_iterate(tp, var)?,
            Self::List(_) => {
                json_to_iterable(tp, var, self.to_sql(tp)?, None, JsonType::Any.into())?
            }
            Self::Atom(atom) => atom.try_iterate(tp, var)?,
            Self::SqlSubQuery(select_statement, index_map)
            | Self::SqlSubQueryAtom(select_statement, index_map) => Iterable {
                expr: TableRef::SubQuery((**select_statement).clone(), var.clone()),
                kind: IterKind::Table(Table::new(var.to_string()).columns(index_map.clone())),
            },
            Self::SqlAny(_simple_expr) => {
                return Err(Error::CanNotIterateType(self.returntype(tp)));
            }
        })
    }
}

impl Variable {
    #[must_use]
    pub const fn is_object(&self) -> bool {
        matches!(self, Self::Object { .. })
    }

    pub fn as_postive_integer(&self) -> Result<u64> {
        match self {
            Self::Atom(atom) => atom.as_positive_integer(),
            _ => Err(Error::Todo("Must be an integer")),
        }
    }

    /// Turn this into a select query
    pub fn to_select(&self, tp: &Transpiler) -> Result<SelectStatement> {
        Ok(match self {
            Self::SqlSubQuery(select_statement, _) => *select_statement.clone(),
            other => Query::select().expr(other.to_sql(tp)?.expr).take(),
        })
    }
}

impl From<i64> for Variable {
    fn from(value: i64) -> Self {
        Self::Atom(Atom::from(value))
    }
}

#[derive(Clone, Debug)]
pub enum Atom {
    Null,
    Bool(bool),
    String(Arc<String>),
    Bytes(Arc<Vec<u8>>),
    Int(i64),
    UInt(u64),
    Float(f64),
}

impl Atom {
    fn as_positive_integer(&self) -> Result<u64> {
        match self {
            Self::Int(i) => (*i).try_into().map_err(|_| Error::Todo("Must be positive")),
            Self::UInt(u) => Ok(*u),
            _ => Err(Error::Todo("Must be an integer")),
        }
    }
}

impl From<i64> for Atom {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl TryFrom<Variable> for cel_interpreter::Value {
    type Error = CantBeAValue;

    fn try_from(value: Variable) -> std::result::Result<Self, Self::Error> {
        Ok(match value {
            Variable::Object(Object { data, .. }) => Self::Map(cel_interpreter::objects::Map {
                map: Arc::new(
                    data.into_iter()
                        .map(|(k, v)| Ok((k.try_into()?, v.try_into()?)))
                        .collect::<std::result::Result<_, _>>()?,
                ),
            }),
            Variable::List(expresions) => Self::List(Arc::new(
                expresions
                    .into_iter()
                    .map(|v| match &*v.inner {
                        ExpressionInner::Variable(x) => x.clone().try_into(),
                        _ => Err(CantBeAValue),
                    })
                    .collect::<std::result::Result<_, _>>()?,
            )),
            Variable::Atom(atom) => match atom {
                Atom::Null => Self::Null,
                Atom::Bool(b) => Self::Bool(b),
                Atom::String(s) => Self::String(s),
                Atom::Bytes(items) => Self::Bytes(items),
                Atom::Int(i) => Self::Int(i),
                Atom::UInt(u) => Self::UInt(u),
                Atom::Float(f) => Self::Float(f),
            },
            _ => return Err(CantBeAValue),
        })
    }
}
