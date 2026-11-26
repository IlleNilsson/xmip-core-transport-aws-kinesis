//! How a Location of this technology is configured: `Configured`, reading
//! the settings its [`declaration`](crate::declaration) declares once
//! (ADR-0064, amendments 2026-09-26 and 2026-11-26), and the origin a
//! record is written with under the scheme declared there.

use transport::Configured;
use transport::error::Result;
use xcore::settings::{Read, Settings};

use crate::{KinesisTransport, declaration};

impl Configured for KinesisTransport {
    const SETTINGS: &'static Settings = declaration::SETTINGS;
    const SCHEMES: &'static [&'static str] = declaration::SCHEMES;

    /// The stream setting: the address names only the endpoint.
    fn own_target(_address: &str) -> &str {
        transport::destination::OWN
    }

    fn timeout_kept(&mut self) -> Option<&mut dyn transport::configured::TimeoutKept> {
        Some(&mut self.timeout)
    }

    fn configured(address: &str, settings: &Read) -> Result<Self> {
        // The secret access key comes through the Location's secret.
        let mut transport = Self::new(address, settings.text("region"), settings.text("stream"));
        if let Some(access_key) = settings.optional_text(aws::access_key::ACCESS_KEY_ID.name) {
            transport.access_key = access_key.to_string();
        }
        if let Some(shard) = settings.optional_text("shard") {
            transport = transport.on_shard(shard);
        }
        if let Some(partition_key) = settings.optional_text("partition_key") {
            transport = transport.keyed_by(partition_key);
        }
        Ok(transport)
    }

    /// The secret access key the Location's secret holds, signed with at
    /// each request as its `access_key_id`; refused where it names none.
    fn secured_by(mut self, secret: transport::Secured) -> Result<Self> {
        let secret = aws::access_key::signing_as(&self.access_key, secret)?;
        self.secret = Some(secret);
        Ok(self)
    }

    /// The connections its clients share, an HTTP/2 one's frames in a span
    /// counted on `clock`, its node's (ADR-0068, amendment 2026-11-16).
    fn timed_by(mut self, clock: &std::sync::Arc<dyn xcore::Clock>) -> Self {
        self.connections = self.connections.timed_by(clock);
        self
    }
}

/// The record at `sequence_number` on `shard` of `stream`, as an origin
/// says it, under the scheme the declaration names:
/// `aws-kinesis://stream/shard/sequence`.
/// The transport's receive and the session's put both name a record by it.
#[must_use]
pub(crate) fn origin(stream: &str, shard: &str, sequence_number: &str) -> String {
    format!("aws-kinesis://{stream}/{shard}/{sequence_number}")
}
