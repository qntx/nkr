//! `Executor` on `rusqlite` — the same surface the workerd
//! `SqlStorage` executor implements, used by tests and native tooling.
//!
//! The connection carries workerd's statement limits so a query that
//! exceeds them fails here exactly as it would in a Durable Object.

use std::ops::ControlFlow;
use std::path::Path;

use ::rusqlite::limits::Limit;
use ::rusqlite::types::{Value as DbValue, ValueRef};
use ::rusqlite::{Connection as Inner, params_from_iter};

use crate::StoreError;
use crate::exec::{Executor, Param, Value};
use crate::store::JS_MAX;

/// workerd's `SQLITE_LIMIT_*` values, applied to every
/// [`Connection`].
const LIMITS: &[(Limit, i32)] = &[
    (Limit::SQLITE_LIMIT_VARIABLE_NUMBER, 100),
    (Limit::SQLITE_LIMIT_COMPOUND_SELECT, 5),
    (Limit::SQLITE_LIMIT_SQL_LENGTH, 100_000),
    (Limit::SQLITE_LIMIT_EXPR_DEPTH, 100),
    (Limit::SQLITE_LIMIT_VDBE_OP, 25_000),
    (Limit::SQLITE_LIMIT_FUNCTION_ARG, 32),
];

/// A rusqlite [`Executor`]: one connection with workerd's
/// `SqlStorage` statement limits.
#[derive(Debug)]
pub struct Connection(Inner);

impl Connection {
    /// Open a file-backed database at `path`.
    ///
    /// # Errors
    ///
    /// `rusqlite::Error` on open or limit failure.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ::rusqlite::Error> {
        Self::limited(Inner::open(path)?)
    }

    /// Open an in-memory database.
    ///
    /// # Errors
    ///
    /// `rusqlite::Error` on open or limit failure.
    pub fn open_in_memory() -> Result<Self, ::rusqlite::Error> {
        Self::limited(Inner::open_in_memory()?)
    }

    /// Apply workerd's statement limits.
    fn limited(inner: Inner) -> Result<Self, ::rusqlite::Error> {
        for (limit, value) in LIMITS {
            inner.set_limit(*limit, *value)?;
        }
        Ok(Self(inner))
    }
}

/// `Param` as an owned rusqlite value; an integer outside the
/// JavaScript-safe range fails like `SqlStorageValue::try_from_i64`.
fn bind(param: &Param<'_>) -> Result<DbValue, ::rusqlite::Error> {
    Ok(match param {
        Param::Null => DbValue::Null,
        Param::Int(v) => {
            if v.unsigned_abs() > JS_MAX {
                return Err(::rusqlite::Error::SqliteFailure(
                    ::rusqlite::ffi::Error::new(::rusqlite::ffi::SQLITE_RANGE),
                    Some("integer outside the JavaScript-safe range".to_owned()),
                ));
            }
            DbValue::Integer(*v)
        }
        Param::Text(s) => DbValue::Text((*s).to_owned()),
        Param::Blob(b) => DbValue::Blob(b.to_vec()),
    })
}

/// Bind `params` after range-checking every integer.
fn bind_all(params: &[Param<'_>]) -> Result<Vec<DbValue>, ::rusqlite::Error> {
    params.iter().map(bind).collect()
}

/// `ValueRef` as an owned [`Value`]; non-UTF-8 text degrades to
/// `Blob` (SQLite allows arbitrary bytes in TEXT).
fn value(col: ValueRef<'_>) -> Value {
    match col {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(v) => Value::Int(v),
        ValueRef::Real(v) => Value::Real(v),
        ValueRef::Text(bytes) => std::str::from_utf8(bytes).map_or_else(
            |_| Value::Blob(bytes.to_vec()),
            |s| Value::Text(s.to_owned()),
        ),
        ValueRef::Blob(bytes) => Value::Blob(bytes.to_vec()),
    }
}

impl Executor for Connection {
    type Error = ::rusqlite::Error;

    fn query(
        &self,
        sql: &str,
        params: &[Param<'_>],
        row: &mut dyn FnMut(Vec<Value>) -> ControlFlow<()>,
    ) -> Result<(), Self::Error> {
        let mut statement = self.0.prepare_cached(sql)?;
        let mut rows = statement.query(params_from_iter(bind_all(params)?))?;
        while let Some(r) = rows.next()? {
            let count = r.as_ref().column_count();
            let mut values = Vec::with_capacity(count);
            for i in 0..count {
                values.push(value(r.get_ref(i)?));
            }
            if row(values).is_break() {
                return Ok(());
            }
        }
        Ok(())
    }

    fn execute(&self, sql: &str, params: &[Param<'_>]) -> Result<(), Self::Error> {
        self.0
            .prepare_cached(sql)?
            .execute(params_from_iter(bind_all(params)?))?;
        Ok(())
    }

    fn transaction<T>(&self, f: impl FnOnce() -> Result<T, StoreError>) -> Result<T, StoreError> {
        self.0
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(StoreError::sql)?;
        match f() {
            Ok(value) => {
                self.0.execute_batch("COMMIT").map_err(StoreError::sql)?;
                Ok(value)
            }
            Err(err) => {
                drop(self.0.execute_batch("ROLLBACK"));
                Err(err)
            }
        }
    }
}
