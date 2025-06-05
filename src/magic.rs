use crate::intermediate::{AccessChain, Expression, ExpressionInner, Ident};
use std::sync::{Arc, LazyLock};

macro_rules! ident {
    ($(#[$attr:meta])* $name:ident) => {
        $(#[$attr])*
        pub fn $name() -> Expression {
            static NAME: LazyLock<Arc<String>> =
                LazyLock::new(|| Arc::new(stringify!($name).to_owned()));
            ExpressionInner::Access(AccessChain {
                head: None,
                idents: vec![Ident(NAME.clone())],
            }).into_anonymous()
        }
    };
}

ident!(
    /// Contains a refference to the variable `here`
    /// which should be set by the outside context
    /// such that it evaluates to a SELECT query that
    /// returns a view into a reasonable "universe"
    ///
    /// In case of a TRIGGER `here` should evaluate
    /// to a RecordSet that contains (among others) `NEW`
    /// and would have contained `OLD`
    ///
    /// Example `WHERE "user" = {user}`
    ///
    /// built-in functions should only ever use `here`
    /// as a fallback value and always allow callers to
    /// specify an override
    here
);

ident!(
    /// Contains a refference to the variable `we`
    /// which should be set by the outside context
    /// such that it evaluates to a SELECT query that
    /// returns a RecordSet that is a reasonable sub set
    /// of `here`. As such `we` should fall back to `here`
    /// if not explicitly set.
    ///
    /// Example: `{here} WHERE "customer" = {customer}`
    ///
    /// built-in functions should only ever use `we`
    /// as a fallback value and always allow callers to
    /// specify an override
    ///
    we
);

ident!(
    /// Just an `x`, use as a placeholder variable
    /// e.g. to de-sugar `all(list)` into `all(list, x, x)`
    ///
    /// WARNING: should never be used with the assumption
    /// that a user supplied expression references `x` or
    /// that `x` was supplied by a user! This is because
    /// the variable may be shadowed by or be shadowing
    /// an actual variable called `x`
    x
);
