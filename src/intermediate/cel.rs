use cel_interpreter::{ExecutionError, Value as CelValue, objects::Key};
use cel_parser::{Atom as CelAtom, Expression as CelExpression, Member};

use crate::{
    Result, Transpiler, functions,
    transpiler::ParseError,
    variables::{Atom, Object, Variable},
};

use super::{AccessChain, Error, Expression, ExpressionInner, Ident, ToIntermediate};

macro_rules! box_ex {
    ($tp:ident, $ex:ident) => {
        Box::new($ex.to_sqcel($tp)?)
    };
}

impl ToIntermediate for CelExpression {
    fn to_sqcel(&self, tp: &Transpiler) -> Result<Expression> {
        if tp.reduce {
            match CelValue::resolve(self, &tp.to_context()?) {
                // We succesfully reduced the expression down to a single value
                // that can be baked into the SQL as a constant
                Ok(value) => return value.to_sqcel(tp),
                // Don't recover from "Unsupported" errors, they will (should)
                // just error inside the SQL statement as well
                e @ Err(
                    ExecutionError::UnsupportedBinaryOperator(_, _, _)
                    | ExecutionError::UnsupportedFunctionCallIdentifierType(_)
                    | ExecutionError::UnsupportedIndex(_, _)
                    | ExecutionError::UnsupportedKeyType(_)
                    | ExecutionError::UnsupportedListIndex(_)
                    | ExecutionError::UnsupportedMapIndex(_)
                    | ExecutionError::UnsupportedTargetType { .. }
                    | ExecutionError::UnsupportedUnaryOperator(_, _),
                ) => {
                    e?;
                }
                // Even though this is called "Unsupported" it is different
                // to the exceptions above (should probably be "Unimplemented")
                // We can still forward this to the SQL statement because
                // it _is_ implemented there
                #[allow(clippy::match_same_arms, reason = "Call out the weird Error name")]
                Err(ExecutionError::UnsupportedFieldsConstruction(_)) => (),
                _ => (),
            }
        }

        Ok(match self {
            Self::Arithmetic(a, op, b) => {
                ExpressionInner::Arithmetic(box_ex!(tp, a), op.clone(), box_ex!(tp, b))
            }
            Self::Relation(a, op, b) => {
                ExpressionInner::Relation(box_ex!(tp, a), op.clone(), box_ex!(tp, b))
            }
            Self::Ternary(a, b, c) => {
                ExpressionInner::Ternary(box_ex!(tp, a), box_ex!(tp, b), box_ex!(tp, c))
            }
            Self::Or(a, b) => ExpressionInner::Or(box_ex!(tp, a), box_ex!(tp, b)),
            Self::And(a, b) => ExpressionInner::And(box_ex!(tp, a), box_ex!(tp, b)),
            Self::Unary(unary_op, e) => {
                ExpressionInner::Unary(unary_op.clone(), box_ex!(tp, e))
            }
            Self::Member(expression, member) => resolve_member(tp, expression, member)?,
            Self::FunctionCall(name, rec, args) => {
                ExpressionInner::FunctionCall(functions::get(
                    tp,
                    &name.to_sqcel(tp)?,
                    rec.as_ref().map(|rec| rec.to_sqcel(tp)).transpose()?,
                    args.iter()
                        .map(|arg| arg.to_sqcel(tp))
                        .collect::<Result<_>>()?,
                )?)
            }
            Self::List(expressions) => ExpressionInner::Variable(Variable::List(
                expressions
                    .iter()
                    .map(|x| x.to_sqcel(tp))
                    .collect::<Result<_>>()?,
            )),
            Self::Map(items) => ExpressionInner::Variable(Variable::Object(Object {
                data: items
                    .iter()
                    .map(|(k, v)| Ok((k.to_sqcel(tp)?, v.to_sqcel(tp)?)))
                    .collect::<Result<_>>()?,
                schema: None,
            })),
            Self::Atom(atom) => ExpressionInner::Variable(Variable::Atom(match atom {
                CelAtom::Int(i) => Atom::Int(*i),
                CelAtom::UInt(u) => Atom::UInt(*u),
                CelAtom::Float(f) => Atom::Float(*f),
                CelAtom::String(s) => Atom::String(s.clone()),
                CelAtom::Bytes(items) => Atom::Bytes(items.clone()),
                CelAtom::Bool(b) => Atom::Bool(*b),
                CelAtom::Null => Atom::Null,
            })),
            Self::Ident(i) => {
                ExpressionInner::Access(AccessChain::new(vec![Ident(i.clone())]))
            }
        }
        .into_anonymous())
    }
}

fn resolve_member(
    tp: &Transpiler,
    expression: &CelExpression,
    member: &Member,
) -> Result<ExpressionInner> {
    Ok(match member {
        Member::Attribute(s) => {
            let x = expression.to_sqcel(tp)?;
            match &*x.inner {
                ExpressionInner::Access(access_chain) => {
                    let mut access_chain = access_chain.clone();
                    access_chain.idents.push(Ident(s.clone()));
                    ExpressionInner::Access(access_chain)
                }
                ExpressionInner::Variable(v) => match v {
                    Variable::Object { .. } => ExpressionInner::Access(AccessChain {
                        head: Some(Box::new(x)),
                        idents: vec![Ident(s.clone())],
                    }),
                    _ => return Err(Error::Todo("Does not support member access")),
                },
                _ => ExpressionInner::Access(AccessChain {
                    head: Some(Box::new(x)),
                    idents: vec![Ident(s.clone())],
                }),
            }
        }
        Member::Index(index) => ExpressionInner::Index(
            box_ex!(tp, expression),
            match **index {
                CelExpression::Atom(cel_parser::Atom::Int(i)) => i,
                _ => return Err(Error::Todo("None int index")),
            },
        ),
        Member::Fields(items) => ExpressionInner::Variable(Variable::Object(Object {
            data: items
                .iter()
                .map(|(k, v)| {
                    Ok((
                        ExpressionInner::Variable(Variable::Atom(Atom::String(k.clone())))
                            .into_anonymous(),
                        v.to_sqcel(tp)?,
                    ))
                })
                .collect::<Result<_>>()?,
            schema: match &*expression.to_sqcel(tp)?.inner {
                ExpressionInner::Access(access) => Some(access.clone()),
                _ => unreachable!(),
            },
        })),
    })
}

impl ToIntermediate for CelValue {
    fn to_sqcel(&self, tp: &Transpiler) -> Result<Expression> {
        Ok(ExpressionInner::Variable(match self {
            Self::List(values) => Variable::List(
                values
                    .iter()
                    .map(|x| x.to_sqcel(tp))
                    .collect::<Result<_>>()?,
            ),
            Self::Map(map) => Variable::Object(Object {
                data: map
                    .map
                    .iter()
                    .map(|(k, v)| Ok((k.to_sqcel(tp)?, v.to_sqcel(tp)?)))
                    .collect::<Result<_>>()?,
                schema: None,
            }),
            Self::Int(i) => Variable::Atom(Atom::Int(*i)),
            Self::UInt(u) => Variable::Atom(Atom::UInt(*u)),
            Self::Float(f) => Variable::Atom(Atom::Float(*f)),
            Self::String(s) => Variable::Atom(Atom::String(s.clone())),
            Self::Bytes(items) => Variable::Atom(Atom::Bytes(items.clone())),
            Self::Bool(b) => Variable::Atom(Atom::Bool(*b)),
            Self::Null => Variable::Atom(Atom::Null),
            Self::Function(_, _) => {
                return Err(ParseError::Todo("Function pointers are not supported"));
            }
        })
        .into_anonymous())
    }
}

impl ToIntermediate for Key {
    fn to_sqcel(&self, _tp: &Transpiler) -> Result<Expression> {
        Ok(match self {
            Self::Int(i) => ExpressionInner::Variable(Variable::Atom(Atom::Int(*i))),
            Self::Uint(u) => ExpressionInner::Variable(Variable::Atom(Atom::UInt(*u))),
            Self::Bool(b) => ExpressionInner::Variable(Variable::Atom(Atom::Bool(*b))),
            Self::String(s) => ExpressionInner::Variable(Variable::Atom(Atom::String(s.clone()))),
        }
        .into_anonymous())
    }
}
