use anyhow::Context as _;
use thiserror::{self, Error};

#[derive(Error, Debug)]
pub(crate) enum Error {
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
pub(crate) type Result<T> = std::result::Result<T, Error>;

pub type Domain = interop::Domain;
pub type DomainDescriptor = interop::DomainDescriptor;

pub(crate) fn parse_domain(s: &str) -> Result<Domain> {
    Ok(serde_json::from_str::<Domain>(s).context("cannot parse domain")?)
}

pub(crate) fn parse_domain_descriptor(s: &str) -> Result<DomainDescriptor> {
    Ok(serde_json::from_str::<DomainDescriptor>(s).context("cannot parse domain descriptor")?)
}
