use indexmap::IndexMap;
use sea_query::{IntoIden, Query, SelectExpr, SimpleExpr, SubQueryStatement, Value};
use thiserror::Error;

use crate::structure::Column;
use crate::{Error, Result};

pub mod agree;
mod column_types;
pub mod json;
mod typed_expression;
pub use column_types::ColumnType as SqlType;
pub use column_types::*;
pub use json::*;
pub use typed_expression::*;

#[derive(Debug, Error)]
pub enum ConversionError {
    // #[error("Unsafe Conversion from {:?} to {:?}", .0, .1.ty)]
    // UnsafeConversion(Type, TypedExpression),
    #[error("Can not convert from {:?} to {:?}", .0, .1)]
    CantConvert(Box<Type>, Box<Type>),
    #[error("Unimplemented conversion from {:?} to {:?}", .0, .1)]
    UnimplementedConvertion(Box<Type>, Box<Type>),
    #[error("Can not reduce from {:?} to {:?}", .0, .1)]
    CantReduce(Box<Type>, ColumnType),
    #[error("Unimplemented reduction from {:?} to {:?}", .0, .1)]
    UnimplementedReduction(Box<Type>, ColumnType),
    #[error("Can not reduce from {:?} to {:?}", .0, .1)]
    CantCast(ColumnType, ColumnType),
    #[error("Unimplemented reduction from {:?} to {:?}", .0, .1)]
    UnimplementedCast(ColumnType, ColumnType),
    #[error(transparent)]
    ParseError(Box<Error>),
}

impl From<Error> for ConversionError {
    fn from(value: Error) -> Self {
        Self::ParseError(Box::new(value))
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
#[non_exhaustive]
pub enum Type {
    /// `foo` or `SELECT "something" as "foo" FROM bar`
    Column(Option<String>, ColumnType),

    Row(Option<Vec<(Option<String>, ColumnType)>>),
    NamedRow(Vec<(String, ColumnType)>),

    View(Option<Vec<(Option<String>, ColumnType)>>),
    NamedView {
        fields: IndexMap<String, ColumnType>,
        groups: IndexMap<String, ColumnType>,
    },

    /// A (maybe named) 1x1 value
    Cell(Cell),
    #[default]
    Unknown,
}

impl From<IndexMap<String, Column>> for Type {
    fn from(value: IndexMap<String, Column>) -> Self {
        Self::NamedView {
            fields: value
                .into_iter()
                .map(|(_, Column { name, ty })| (name, ty))
                .collect(),
            groups: Default::default(),
        }
    }
}

impl Type {
    #[must_use]
    pub const fn is_single_value(&self) -> bool {
        matches!(self, Self::Cell(_))
    }

    /// i.e. can be converted with `CAST(<value> as type)`
    #[must_use]
    pub const fn is_single_type(&self) -> bool {
        self.col_type().is_some()
    }

    /// the single type of this structure
    ///
    /// only returns a value for `Cell`, `Column` and `NamedColumn`
    #[must_use]
    pub const fn col_type(&self) -> Option<&ColumnType> {
        Some(match self {
            Self::Cell(cell) => cell.col_type(),
            Self::Column(_, ty) => ty,
            _ => return None,
        })
    }

    /// the type of elements in this array or column
    ///
    /// only returns a value for `Cell`, `Column` and `NamedColumn`
    #[must_use]
    pub fn array_type(&self) -> Option<&ColumnType> {
        Some(match self {
            Self::Cell(cell) => match cell.col_type() {
                ColumnType::Array(x) => x,
                _ => return None,
            },
            Self::Column(_, ty) => ty,
            _ => return None,
        })
    }

    #[must_use]
    pub fn iter_type(&self) -> Option<ColumnType> {
        if *self == Self::Unknown {
            return Some(ColumnType::Inferred);
        }
        match self {
            Self::Column(_, t) => Some(t.clone()),
            _ => match self.col_type()? {
                ColumnType::Array(x) => Some((**x).clone()),
                ColumnType::Json(JsonType::List(x), _) => Some((**x).clone().into()),
                ColumnType::Json(JsonType::Map(_), _) => Some(JsonType::String.into()),
                ColumnType::Inferred => Some(ColumnType::Inferred),
                _ => None,
            },
        }
    }

    /// the single type of this structure
    ///
    /// only returns a value for `Cell`, `Column` and `NamedColumn`
    #[must_use]
    pub fn replace_type(self, ty: &ColumnType) -> Option<Self> {
        Some(match self {
            Self::Cell(_) => Self::Cell(Cell::Value(ty.clone())),
            Self::Column(name, _) => Self::Column(name, ty.clone()),
            _ => return None,
        })
    }

    #[must_use]
    pub fn is_json(&self) -> bool {
        self.col_type()
            .is_some_and(column_types::ColumnType::is_json)
    }

    pub fn into_column(self) -> Result<Self> {
        match self {
            Self::Cell(Cell::Literal(t) | Cell::Value(t)) => Ok(Self::Column(None, t)),
            _ => Ok(self),
            // _ => Err(Error::MoreThanOneColumn),
        }
    }

    #[must_use]
    pub fn is_subtype(&self, other: &Self) -> bool {
        #[allow(clippy::match_same_arms)]
        match (self, other) {
            (Self::Row(None), Self::Row(_) | Self::NamedRow(_)) => true,
            (Self::Row(_) | Self::NamedRow(_), Self::Row(None)) => true,
            (Self::View(None), Self::View(_) | Self::NamedView { .. }) => true,
            (Self::View(_) | Self::NamedView { .. }, Self::View(None)) => true,
            (a, b) => a.col_type() == b.col_type(),
        }
    }

    pub(crate) fn can_convert_to(&self, other: &Self) -> bool {
        if *self == Self::Unknown {
            return true;
        }
        match (self.col_type(), other.col_type()) {
            (Some(ty1), Some(ty2)) => ty1.can_convert_to(ty2),
            _ => self.can_cast_to(other),
        }
    }

    #[must_use]
    pub fn can_cast_to(&self, other: &Self) -> bool {
        if *self == Self::Unknown {
            return true;
        }
        match (self.col_type(), other.col_type()) {
            (Some(ty1), Some(ty2)) => ty1.can_cast_to(ty2),
            _ => false,
        }
    }

    #[must_use]
    pub fn rename(self, name: String) -> Self {
        match self {
            Self::Column(_, t) => Self::Column(Some(name), t),
            x => x,
        }
    }
}

impl From<SqlType> for Type {
    fn from(value: SqlType) -> Self {
        Self::Cell(Cell::Value(value))
    }
}

impl From<Cell> for Type {
    fn from(value: Cell) -> Self {
        Self::Cell(value)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    Literal(ColumnType),
    Value(ColumnType),
}

impl Cell {
    /// the single type of this structure
    ///
    /// only returns a value for `Cell`, `Column` and `NamedColumn`
    #[must_use]
    pub const fn col_type(&self) -> &ColumnType {
        match self {
            Self::Literal(col_type) | Self::Value(col_type) => col_type,
        }
    }
}

impl From<cel_parser::Atom> for TypedExpression {
    fn from(value: cel_parser::Atom) -> Self {
        let (expr, col_type) = match value {
            cel_parser::Atom::Int(val) => (val.into(), ColumnType::Integer),
            cel_parser::Atom::UInt(val) => (val.into(), ColumnType::Unsigned),
            cel_parser::Atom::Float(val) => (val.into(), ColumnType::Float),
            cel_parser::Atom::String(val) => ((*val).clone().into(), ColumnType::Text),
            cel_parser::Atom::Bytes(items) => (
                (*items).clone().into(),
                ColumnType::Binary(items.len() as u32),
            ),
            cel_parser::Atom::Bool(val) => (val.into(), ColumnType::Boolean),
            cel_parser::Atom::Null => (Value::Bool(None), ColumnType::Null),
        };

        Self {
            expr: SimpleExpr::Constant(expr),
            ty: Cell::Literal(col_type).into(),
        }
    }
}

#[must_use]
pub fn subquery(expr: impl Into<SelectExpr>) -> SimpleExpr {
    SimpleExpr::SubQuery(
        None,
        Box::new(SubQueryStatement::SelectStatement(
            Query::select().expr(expr).take(),
        )),
    )
}

#[must_use]
pub fn subquery_as(expr: SimpleExpr, name: impl IntoIden) -> SimpleExpr {
    subquery(SelectExpr {
        expr,
        alias: Some(name.into_iden()),
        window: None,
    })
}

#[cfg(test)]
mod test {
    use crate::types::{Cell, ColumnType, Type, TypedExpression};

    #[test]
    fn num_to_num() {
        let x = TypedExpression {
            expr: 5.into(),
            ty: Cell::Literal(ColumnType::Integer).into(),
        };
        let old = x.clone();
        let new = x
            .reshape(
                &Default::default(),
                &Cell::Literal(ColumnType::Integer).into(),
            )
            .unwrap();

        assert_eq!(old, new);
    }

    #[test]
    fn literal_to_col() {
        let x = TypedExpression {
            expr: 5.into(),
            ty: Cell::Literal(ColumnType::Integer).into(),
        };
        let old = x.clone();
        let new = x
            .reshape(
                &Default::default(),
                &Type::Column(None, ColumnType::Integer),
            )
            .unwrap();

        assert_eq!(old, new);
    }

    #[test]
    fn num_to_bool() {
        TypedExpression {
            expr: 5.into(),
            ty: Cell::Literal(ColumnType::Integer).into(),
        }
        .reshape(
            &Default::default(),
            &Cell::Literal(ColumnType::Boolean).into(),
        )
        .unwrap_err();
    }

    #[test]
    fn assert_conversions() {
        assert!(ColumnType::Inferred.can_convert_to(&ColumnType::Text));
        assert!(
            Type::Column(None, ColumnType::Inferred)
                .can_convert_to(&Type::Column(None, ColumnType::Inferred))
        );
    }
}
