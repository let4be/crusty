use thiserror::{self, Error};

#[derive(Error, Debug)]
pub(crate) enum Error {
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
pub(crate) type Result<T> = std::result::Result<T, Error>;
