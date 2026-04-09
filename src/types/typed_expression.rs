use sea_query::{
    Alias, BinOper, ColumnRef, Func, IntoIden, Query, SimpleExpr, SubQueryStatement,
    extension::postgres::PgBinOper,
};

use crate::{
    Transpiler,
    sql_extensions::SqlExtension,
    types::{Cell, ColumnType, ConversionError, JsonObject, JsonType, Type, subquery_as},
};

#[derive(Debug, Clone, PartialEq)]
pub struct TypedExpression {
    pub expr: SimpleExpr,
    pub ty: Type,
}

impl From<TypedExpression> for SimpleExpr {
    fn from(value: TypedExpression) -> Self {
        value.expr
    }
}

impl std::ops::Deref for TypedExpression {
    type Target = SimpleExpr;

    fn deref(&self) -> &Self::Target {
        &self.expr
    }
}

impl TypedExpression {
    fn map_expr(mut self, f: impl Fn(SimpleExpr) -> SimpleExpr) -> Self {
        self.expr = f(self.expr);
        self
    }

    #[must_use]
    pub fn with_type(self, ty: impl Into<Type>) -> Self {
        Self {
            ty: ty.into(),
            ..self
        }
    }

    // Reinterprete the shape
    pub fn reshape(self, tp: &Transpiler, other: &Type) -> Result<Self, ConversionError> {
        #[allow(clippy::match_same_arms)]
        Ok(match (self.ty.clone(), other) {
            (me, other) if me == *other => self,
            (Type::Column(_, _), Type::Column(None, other)) => self.convert(tp, other)?,
            (Type::Column(_, _), Type::Column(Some(name), other)) => self
                .convert(tp, other)?
                .map_expr(|expr| subquery_as(expr, Alias::new(name))),
            (Type::Column(_, _), Type::View(None)) => self,
            (
                me @ Type::Column(_, _),
                other @ (Type::Row(Some(items)) | Type::View(Some(items))),
            ) => {
                if let Some((name, ty)) = items.first() {
                    let x = self.convert(tp, ty)?;
                    if let Some(name) = name {
                        x.map_expr(|expr| subquery_as(expr, Alias::new(name)))
                    } else {
                        x
                    }
                    .with_type(other.clone())
                } else {
                    return Err(ConversionError::CantConvert(
                        Box::new(me),
                        Box::new(other.clone()),
                    ));
                }
            }
            (a @ Type::Column(_, _), b) => {
                return Err(ConversionError::CantConvert(
                    Box::new(a),
                    Box::new(b.clone()),
                ));
            }

            (Type::Cell(_), Type::Cell(Cell::Value(b) | Cell::Literal(b))) => {
                self.convert(tp, b)?
            }
            (a, b @ Type::Cell(Cell::Literal(_))) => {
                return Err(ConversionError::CantConvert(
                    Box::new(a),
                    Box::new(b.clone()),
                ));
            }
            (Type::Cell(_), Type::Column(None, b)) => self.convert(tp, b)?,
            (Type::Cell(_), Type::Column(Some(name), b)) => self
                .convert(tp, b)?
                .map_expr(|expr| subquery_as(expr, Alias::new(name))),
            (Type::Cell(cell), Type::Row(None)) => {
                self.with_type(Type::View(Some(vec![(None, cell.col_type().clone())])))
            }
            (Type::Cell(cell), Type::View(None)) => {
                self.with_type(Type::View(Some(vec![(None, cell.col_type().clone())])))
            }
            (_me @ Type::Cell(_), other @ (Type::Row(Some(items)) | Type::View(Some(items)))) => {
                if let Some((name, ty)) = items.first() {
                    let x = self.convert(tp, ty)?;
                    if let Some(name) = name {
                        x.map_expr(|expr| subquery_as(expr, Alias::new(name)))
                    } else {
                        x
                    }
                    .with_type(other.clone())
                } else {
                    self
                }
            }
            (a, b) => {
                return Err(ConversionError::UnimplementedConvertion(
                    Box::new(a),
                    Box::new(b.clone()),
                ));
            }
        })
    }

    // Change the Column Type but retain the shape
    pub fn convert(self, tp: &Transpiler, other: &ColumnType) -> Result<Self, ConversionError> {
        Ok(if let Some(col_type) = self.ty.col_type() {
            if col_type.can_cast_to(other) {
                self.cast(tp, other)?
            } else {
                match (col_type, other) {
                    (ColumnType::Null, ColumnType::Json(_, _)) => {
                        SimpleExpr::Constant("null".into())
                            .cast_as(ColumnType::Json(JsonType::Any, false))
                            .with_type(JsonType::Null)
                    }
                    (
                        ColumnType::Text | ColumnType::Char(_) | ColumnType::String(_),
                        ColumnType::Json(JsonType::Any | JsonType::String, _),
                    ) => Func::cust("to_jsonb")
                        .arg(self.expr.cast_as(ColumnType::Text))
                        .with_type(JsonType::String),
                    (ColumnType::Json(JsonType::Any | JsonType::String, _), ColumnType::Text) => {
                        match self.expr {
                            SimpleExpr::Binary(
                                a,
                                BinOper::PgOperator(PgBinOper::GetJsonField),
                                b,
                            ) => SimpleExpr::Binary(
                                a,
                                BinOper::PgOperator(PgBinOper::CastJsonField),
                                b,
                            ),
                            x => SimpleExpr::Binary(
                                Box::new(x),
                                BinOper::Custom("#>>"),
                                Box::new(SimpleExpr::Constant("{}".into())),
                            ),
                        }
                        .with_type(ColumnType::Text)
                    }
                    (
                        ColumnType::Time
                        | ColumnType::Timestamp
                        | ColumnType::TimestampWithTimeZone
                        | ColumnType::DateTime,
                        a,
                    ) if a.is_integer() => self.expr.extract("epoch"),

                    (_, other @ ColumnType::Json(JsonType::Any, _)) => Func::cust("to_jsonb")
                        .arg(self.expr)
                        .with_type(other.clone()),
                    (_, b) => {
                        return Err(ConversionError::CantConvert(
                            Box::new(self.ty),
                            Box::new(Cell::Value(b.clone()).into()),
                        ));
                    }
                }
            }
        } else {
            return Err(ConversionError::CantConvert(
                Box::new(self.ty),
                Box::new(Cell::Value(other.clone()).into()),
            ));
        })
    }

    /// Will only perform cheap direct casts
    pub fn cast(self, _tp: &Transpiler, other: &ColumnType) -> Result<Self, ConversionError> {
        let mt = self
            .ty
            .col_type()
            .ok_or_else(|| ConversionError::CantReduce(Box::new(self.ty.clone()), other.clone()))?;
        if mt == other || (mt.is_json() && other.is_json()) {
            return Ok(self);
        }
        if mt.can_cast_to(other) {
            Ok(Self {
                ty: match &self.ty {
                    Type::Column(name, _) => Type::Column(name.clone(), other.clone()),
                    Type::Cell(_) => Type::Cell(Cell::Value(other.clone())),
                    _ => {
                        return Err(ConversionError::CantReduce(
                            Box::new(self.ty),
                            other.clone(),
                        ));
                    }
                },
                expr: self.into_json_cast().expr.cast_as(other.clone()),
            })
        } else {
            Err(ConversionError::CantCast(mt.clone(), other.clone()))
        }
    }

    // Force the Expression into a single Cell
    pub fn reduce(self, tp: &Transpiler, other: &Cell) -> Result<Self, ConversionError> {
        Ok(match (&self.ty, other) {
            (Type::Cell(_), Cell::Value(ty)) => self.convert(tp, ty)?,
            (
                Type::Row(_) | Type::NamedRow(_),
                Cell::Value(ColumnType::Json(JsonType::Any | JsonType::List(_), _)),
            ) => Func::cust("to_jsonb")
                .arg(self.expr)
                .with_type(JsonType::List(Box::new(JsonType::Any))),
            (
                Type::Column(_, _),
                Cell::Value(ColumnType::Json(JsonType::Any | JsonType::Map(_), _)),
            ) => Func::cust("to_jsonb")
                .arg(Func::cust("array").arg(self.expr))
                .with_type(JsonType::Map(JsonObject::AnyContent)),
            (
                Type::View(_) | Type::NamedView(_),
                Cell::Value(ColumnType::Json(JsonType::Any | JsonType::Map(_), _)),
            ) => {
                let a = tp.alias();

                Func::cust("to_jsonb")
                    .arg(
                        Func::cust("array").arg(SimpleExpr::SubQuery(
                            None,
                            Box::new(SubQueryStatement::SelectStatement(
                                Query::select()
                                    .expr(
                                        Func::cust("to_jsonb")
                                            .arg(ColumnRef::Column(a.into_iden())),
                                    )
                                    .from_subquery(self.as_select()?.clone(), a)
                                    .take()
                                    .into(),
                            )),
                        )),
                    )
                    .with_type(JsonType::Map(JsonObject::AnyContent))
            }
            (a, b) => {
                return Err(ConversionError::CantReduce(
                    Box::new(a.clone()),
                    b.col_type().clone(),
                ));
            }
        })
    }
}
