//! The minimal synchronous SQL surface a platform provides.

use core::ops::ControlFlow;

use crate::StoreError;

/// One bound statement parameter.
#[derive(Debug, Clone, Copy)]
pub enum Param<'a> {
    /// SQL `NULL`.
    Null,
    /// A signed 64-bit integer (JavaScript-safe: `<= 2^53 - 1`).
    Int(i64),
    /// UTF-8 text.
    Text(&'a str),
    /// Binary data.
    Blob(&'a [u8]),
}

/// One column value of a result row.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// SQL `NULL`.
    Null,
    /// A signed 64-bit integer.
    Int(i64),
    /// A double.
    Real(f64),
    /// UTF-8 text.
    Text(String),
    /// Binary data.
    Blob(Vec<u8>),
}

/// The synchronous single-connection SQL surface the store runs on.
///
/// Everything is `&self` because the executor is a handle, not a
/// connection guard: workerd's `SqlStorage` is one, and rusqlite's
/// `unchecked_transaction` gives the same shape.
pub trait Executor {
    /// The platform's statement error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Run one statement; each row streams to `row`, which may stop
    /// the iteration early with [`ControlFlow::Break`].
    ///
    /// # Errors
    ///
    /// `Self::Error` on statement failure.
    fn query(
        &self,
        sql: &str,
        params: &[Param<'_>],
        row: &mut dyn FnMut(Vec<Value>) -> ControlFlow<()>,
    ) -> Result<(), Self::Error>;

    /// Run one statement that produces no rows.
    ///
    /// # Errors
    ///
    /// `Self::Error` on statement failure.
    fn execute(&self, sql: &str, params: &[Param<'_>]) -> Result<(), Self::Error>;

    /// Run `f` atomically; an `Err` rolls everything back. Mirrors
    /// workerd's `transactionSync`: without it a failing statement
    /// leaves the earlier ones committed.
    ///
    /// # Errors
    ///
    /// `StoreError::Sql` on a statement failure, or `f`'s error
    /// verbatim after rolling back.
    fn transaction<T>(&self, f: impl FnOnce() -> Result<T, StoreError>) -> Result<T, StoreError>;
}
