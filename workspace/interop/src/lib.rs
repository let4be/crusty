use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Domain {
    pub name: String,
    pub addr_key: String,
    pub addrs: Vec<SocketAddr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopHits {
    pub tld: String,
    pub domain: String,
    pub hits: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainDescriptor {
    pub name: String,
    pub addr_key: String,
}

impl DomainDescriptor {
    pub fn new(name: &str, addr_key: &str) -> Self {
        Self {
            name: String::from(name),
            addr_key: String::from(addr_key),
        }
    }
}

impl Domain {
    pub fn new(name: &str, addr_key: &str, addrs: &[SocketAddr]) -> Self {
        Self {
            name: String::from(name),
            addr_key: String::from(addr_key),
            addrs: Vec::from(addrs),
        }
    }

    pub fn descriptor(&self) -> DomainDescriptor {
        DomainDescriptor::new(&self.name, &self.addr_key)
    }
}
