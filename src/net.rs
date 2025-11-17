use std::ops::Deref;
use std::str::FromStr;

/// A wrapper around a network multiaddress.
///
/// This struct provides a stable, public-facing API for network addresses,
/// hiding the underlying `libp2p` implementation details.
///
/// It can be created from a string using the `FromStr` trait (e.g., `"/ip4/127.0.0.1/tcp/25565".parse()`).
#[derive(Clone, Debug)]
pub struct Multiaddr(pub(crate) libp2p::Multiaddr);

impl FromStr for Multiaddr {
    type Err = crate::error::RevenantError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let inner = libp2p::Multiaddr::from_str(s)
            .map_err(|e| RevenantError::Configuration(e.to_string()))?;
        Ok(Self(inner))
    }
}

// Implement Deref to allow our wrapper to be used where the inner type is expected.
// This is a convenient way to pass our `Multiaddr` to internal libp2p functions.
impl Deref for Multiaddr {
    type Target = libp2p::Multiaddr;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
