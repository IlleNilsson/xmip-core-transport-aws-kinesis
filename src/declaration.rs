//! This technology's declaration: the transport guide's "The declaration",
//! `module/core/capability/transport/doc/adding-a-transport.md`.
//! `access_key_id` is the AWS crate's, taken by that crate's own
//! `declaration` feature; `shard` defaults to [`SHARD`].

use xcore::settings::{Applies, Fixed, Kind, Presence, Setting, Settings};

use crate::SHARD;

/// Every setting a Location takes beyond its address, the Kinesis endpoint,
/// `https://kinesis.<region>.amazonaws.com`. The access key ID is
/// `access_key_id`; its secret access key is the Location's `secret`, never
/// a setting.
pub const SETTINGS: &Settings = &Settings {
    technology: env!("CARGO_PKG_NAME"),
    settings: &[
        Setting {
            name: "region",
            kind: Kind::Text,
            presence: Presence::Required,
            meaning: "The AWS region requests are signed for, eu-north-1.",
            applies: Applies::Both,
        },
        aws::access_key::ACCESS_KEY_ID,
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
        transport::configured::timeout(
            Applies::Both,
            None,
            "How long an endpoint that stops answering is waited on; unbounded \
             when left out.",
        ),
    ],
};

/// Its schemes, [`transport::Configured::SCHEMES`]: its name (ADR-0011,
/// amendment 2026-11-19).
pub const SCHEMES: &[&str] = &["aws-kinesis"];
