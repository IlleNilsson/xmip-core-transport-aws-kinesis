//! The settings a Location of this technology takes, declared once and read
//! through (ADR-0064, amendment 2026-09-26).

use transport::Configured;
use transport::error::Result;
use xcore::settings::{Applies, Fixed, Kind, Presence, Read, Setting, Settings};

use crate::{KinesisTransport, SHARD};

impl Configured for KinesisTransport {
    /// The address is the Kinesis endpoint, `https://kinesis.<region>.
    /// amazonaws.com`. The access key and its secret are not settings: a
    /// secret never is.
    const SETTINGS: &'static Settings = &Settings {
        technology: env!("CARGO_PKG_NAME"),
        settings: &[
            Setting {
                name: "region",
                kind: Kind::Text,
                presence: Presence::Required,
                meaning: "The AWS region requests are signed for, eu-north-1.",
                applies: Applies::Both,
            },
            Setting {
                name: "stream",
                kind: Kind::Text,
                presence: Presence::Required,
                meaning: "The data stream read from, and put to when a send target names none.",
                applies: Applies::Both,
            },
            Setting {
                name: "shard",
                kind: Kind::Text,
                presence: Presence::Default(Fixed::Text(SHARD)),
                meaning: "The shard a Receive Location reads; the stream's first when left out.",
                applies: Applies::Receive,
            },
            Setting {
                name: "partition_key",
                kind: Kind::Text,
                presence: Presence::Optional,
                meaning: "The partition key a record is put under, which decides its shard; \
                          one fixed key for every record when left out.",
                applies: Applies::Send,
            },
            Setting {
                name: "timeout",
                kind: Kind::Duration,
                presence: Presence::Optional,
                meaning: "How long an endpoint that stops answering is waited on; unbounded \
                          when left out.",
                applies: Applies::Both,
            },
        ],
    };

    /// The schemes a Location's address is entered with to choose this technology.
    const SCHEMES: &'static [&'static str] = &["kinesis"];

    /// The stream setting: the address names only the endpoint.
    fn own_target(_address: &str) -> &str {
        transport::destination::OWN
    }

    fn configured(address: &str, settings: &Read) -> Result<Self> {
        // The access key and secret are never a setting; a Location naming a
        // secret is refused, as by default (`Configured::secured_by`).
        let mut transport = Self::new(address, settings.text("region"), settings.text("stream"));
        if let Some(shard) = settings.optional_text("shard") {
            transport = transport.on_shard(shard);
        }
        if let Some(partition_key) = settings.optional_text("partition_key") {
            transport = transport.keyed_by(partition_key);
        }
        Ok(match settings.optional_duration("timeout") {
            Some(timeout) => transport.timing_out_after(timeout),
            None => transport,
        })
    }
}
