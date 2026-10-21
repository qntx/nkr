//! [`SqlStore`](crate::SqlStore) failure modes.

/// Why a store operation failed.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The SQL executor reported a failure.
    #[error("sql: {0}")]
    Sql(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// On-disk state violates an invariant the store maintains.
    #[error("corrupt store: {0}")]
    Corrupt(&'static str),
    /// A value lies outside what the platform's SQL layer represents.
    #[error("value out of range: {0}")]
    OutOfRange(&'static str),
}

impl StoreError {
    /// Box an executor error.
    pub(crate) fn sql<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
        Self::Sql(Box::new(error))
    }
}
