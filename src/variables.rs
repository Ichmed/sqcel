use crate::{
    Transpiler,
    functions::subquery,
    intermediate::{AccessChain, Expression, ToSql},
    transpiler::{ParseError, Result, alias},
    types::{JsonType, Type, TypedExpression},
};
use sea_query::{Alias, Func, Query, SelectStatement, SimpleExpr};
use std::{collections::HashMap, sync::Arc};

#[derive(Clone, Debug)]
pub enum Variable {
    /// A JSON-Object or Proto-Message
    Object(Object),
    /// A List of Variables
    List(Vec<Expression>),
    /// A String, int, float, etc.
    Atom(Atom),
    /// An SQL query that will evaluate to a list
    SqlSubQuery(Box<SelectStatement>),
    /// An SQL query that will evaluate to a single record
    SqlSubQueryAtom(Box<SelectStatement>),
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

        let type_name = if let Some(schema) = self.schema.as_ref() {
            schema
        } else {
            return Ok(());
        };

        let (type_name, type_info) = if let Some(schema) = type_name.to_path() {
            match tp.types.get(schema.as_ref()) {
                Some(ty) => (schema, ty),
                None if tp.accept_unknown_types => return Ok(()),
                None => return Err(ParseError::UnknownType(schema.into_owned())),
            }
        } else {
            return Err(ParseError::Todo("Invalid type"));
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

        // Remove all fields that _are_ in the message and error if there is an unknwown field
        self.data
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flat_map(|name| fields.remove(name).is_none().then_some(name))
            .next()
            .map(|f| {
                Err(ParseError::UnknownField(
                    type_name.clone().into_owned(),
                    f.to_owned(),
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
                type_name.into_owned(),
                missing_fields,
            ));
        }

        Ok(())
    }
}

impl ToSql for Object {
    fn to_sql(&self, tp: &Transpiler) -> Result<TypedExpression> {
        self.verify(tp)?;
        Ok(TypedExpression {
            ty: Type::Unknown,
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
}

impl ToSql for Variable {
    fn to_sql(&self, tp: &Transpiler) -> Result<TypedExpression> {
        Ok(match self {
            Variable::Object(o) => o.to_sql(tp)?,
            Variable::List(variables) => TypedExpression {
                ty: Type::Json(JsonType::List),
                expr: SimpleExpr::FunctionCall(
                    Func::cust(Alias::new("jsonb_build_array")).args(
                        variables
                            .iter()
                            .map(|x| Ok(x.clone().to_sql(tp)?.expr))
                            .collect::<Result<Vec<_>>>()?,
                    ),
                ),
            },
            Variable::Atom(atom) => atom.to_sql(tp)?,

            Variable::SqlSubQuery(select_statement) => subquery(*select_statement.clone()),
            Variable::SqlSubQueryAtom(select_statement) => subquery(*select_statement.clone()),
            Variable::SqlAny(simple_expr) => TypedExpression {
                ty: Type::Unknown,
                expr: *simple_expr.clone(),
            },
        })
    }
}

impl Variable {
    pub fn is_object(&self) -> bool {
        matches!(self, Self::Object { .. })
    }

    pub fn as_postive_integer(&self) -> Result<u64> {
        match self {
            Variable::Atom(atom) => atom.as_positive_integer(),
            _ => Err(ParseError::Todo("Must be an integer")),
        }
    }

    /// Turn this into a recordset
    pub fn to_record_set(&self, tp: &Transpiler, a: &str) -> Result<SelectStatement> {
        Ok(match self {
            Variable::SqlSubQuery(select_statement) => *select_statement.clone(),
            Variable::List(content) if content.iter().all(Expression::is_object) => {
                todo!("populate_record_set")
            }

            Variable::List(_) => Query::select()
                .expr_as(
                    Func::cust(alias("jsonb_array_elements")).arg(self.to_sql(tp)?.expr),
                    alias(a),
                )
                .take(),
            _ => todo!("Missing into record set"),
        })
    }

    /// Turn this into a select query
    pub fn to_select(&self, tp: &Transpiler) -> Result<SelectStatement> {
        Ok(match self {
            Variable::SqlSubQuery(select_statement) => *select_statement.clone(),
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
            Atom::Int(i) => (*i)
                .try_into()
                .map_err(|_| ParseError::Todo("Must be positive")),
            Atom::UInt(u) => Ok(*u),
            _ => Err(ParseError::Todo("Must be an integer")),
        }
    }
}

impl From<i64> for Atom {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}
