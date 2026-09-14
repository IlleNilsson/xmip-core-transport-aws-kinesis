//! The far end: enough of Kinesis to answer one Location, and what a test
//! or the playground puts on loopback.
//!
//! Not Kinesis. One session holds the streams it was told of, each one
//! shard of records in memory, verifies every request against one
//! credential, and answers the three calls with the shapes Kinesis
//! answers them — the shard and sequence number, the iterator, the records
//! with the iterator to continue on, the exception with its type. A
//! sequence number is one counter across the session; an iterator is the
//! stream and an index into it, in the clear, because nothing here needs
//! to hide it.

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde_json::{Value, json};
use transport::Arrived;
use transport::error::Result;

use crate::SHARD;
use crate::json;
use http::message::{Request, Response};
use http::server;
use http::sigv4::Signer;

/// What the client did, as [`Session::serve_one`] reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// The client put a record; here is the Stream, its origin the stream,
    /// shard and sequence number it landed at.
    Put(Arrived),
    /// The client asked for an iterator over `stream` of this kind.
    Iterated { stream: String, kind: String },
    /// The client read `count` records from `stream`.
    Read { stream: String, count: usize },
    /// The client was answered with this exception.
    Refused(String),
}

/// One record held.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Held {
    pub sequence_number: String,
    pub partition_key: String,
    pub data: Vec<u8>,
}

pub struct Session {
    signer: Signer,
    streams: BTreeMap<String, Vec<Held>>,
    next: u64,
    timeout: Option<Duration>,
}

impl Session {
    /// Answer requests signed in `region` as `access_key` with `secret_key`.
    #[must_use]
    pub fn new(region: &str, access_key: &str, secret_key: &str) -> Self {
        Self {
            signer: Signer::new("kinesis", region, access_key, secret_key),
            streams: BTreeMap::new(),
            next: 1,
            timeout: None,
        }
    }

    /// Hold a stream called `name`, one shard, empty.
    #[must_use]
    pub fn with_stream(mut self, name: &str) -> Self {
        self.streams.entry(name.to_string()).or_default();
        self
    }

    /// Give up on a client that stops mid-request after `timeout`.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Every record on `stream`, in order.
    #[must_use]
    pub fn records(&self, stream: &str) -> &[Held] {
        self.streams.get(stream).map_or(&[], Vec::as_slice)
    }

    /// Accept one connection on `listener`, answer its one request, and say
    /// what it was.
    ///
    /// # Errors
    /// Where the connection could not be accepted, broke, or sent nothing.
    pub fn serve_one(&mut self, listener: &TcpListener) -> Result<Event> {
        server::serve_one(listener, self.timeout, |request| self.answer(request))
    }

    fn answer(&mut self, request: &Request) -> (Event, Response) {
        if let Err(failure) = self.signer.verify(request) {
            return refused(403, "InvalidSignatureException", &failure.message);
        }
        let document = match json::document(request) {
            Ok(document) => document,
            Err(failure) => return refused(400, "SerializationException", &failure.message),
        };
        match json::action(request) {
            Some("PutRecord") => self.put(&document),
            Some("GetShardIterator") => self.iterate(&document),
            Some("GetRecords") => self.read(&document),
            _ => refused(
                400,
                "UnknownOperationException",
                "not one of the three calls",
            ),
        }
    }

    fn put(&mut self, document: &Value) -> (Event, Response) {
        let stream = field(document, "StreamName");
        let Ok(data) = STANDARD.decode(field(document, "Data")) else {
            return refused(400, "SerializationException", "Data that is not base64");
        };
        let Some(held) = self.streams.get_mut(stream) else {
            return refused(400, "ResourceNotFoundException", "no such stream");
        };
        let sequence_number = format!("{:021}", self.next);
        self.next += 1;
        held.push(Held {
            sequence_number: sequence_number.clone(),
            partition_key: field(document, "PartitionKey").to_string(),
            data: data.clone(),
        });
        let answer = json!({ "ShardId": SHARD, "SequenceNumber": sequence_number });
        (
            Event::Put(Arrived::new(origin(stream, &sequence_number), data)),
            json::answer(&answer),
        )
    }

    fn iterate(&self, document: &Value) -> (Event, Response) {
        let stream = field(document, "StreamName");
        let kind = field(document, "ShardIteratorType");
        let Some(held) = self.streams.get(stream) else {
            return refused(400, "ResourceNotFoundException", "no such stream");
        };
        if field(document, "ShardId") != SHARD {
            return refused(400, "ResourceNotFoundException", "no such shard");
        }
        let at = match kind {
            "TRIM_HORIZON" => 0,
            "LATEST" => held.len(),
            "AFTER_SEQUENCE_NUMBER" => {
                let after = field(document, "StartingSequenceNumber");
                match held.iter().position(|r| r.sequence_number == after) {
                    Some(at) => at + 1,
                    None => return refused(400, "InvalidArgumentException", "no such sequence"),
                }
            }
            _ => {
                return refused(
                    400,
                    "InvalidArgumentException",
                    "an iterator type not served",
                );
            }
        };
        (
            Event::Iterated {
                stream: stream.to_string(),
                kind: kind.to_string(),
            },
            json::answer(&json!({ "ShardIterator": iterator(stream, at) })),
        )
    }

    fn read(&self, document: &Value) -> (Event, Response) {
        let Some((stream, at)) = field(document, "ShardIterator").rsplit_once('/') else {
            return refused(
                400,
                "InvalidArgumentException",
                "an iterator not handed out",
            );
        };
        let (Some(held), Ok(at)) = (self.streams.get(stream), at.parse::<usize>()) else {
            return refused(
                400,
                "InvalidArgumentException",
                "an iterator not handed out",
            );
        };
        let limit = document["Limit"]
            .as_u64()
            .map_or(usize::MAX, |n| usize::try_from(n).unwrap_or(usize::MAX));
        let taken: Vec<Value> = held
            .iter()
            .skip(at)
            .take(limit)
            .map(|r| {
                json!({
                    "SequenceNumber": r.sequence_number,
                    "PartitionKey": r.partition_key,
                    "Data": STANDARD.encode(&r.data),
                })
            })
            .collect();
        let count = taken.len();
        let answer = json!({
            "Records": taken,
            "NextShardIterator": iterator(stream, at + count),
            "MillisBehindLatest": 0,
        });
        (
            Event::Read {
                stream: stream.to_string(),
                count,
            },
            json::answer(&answer),
        )
    }
}

/// The record at `sequence_number` on `stream`, as an origin says it.
#[must_use]
pub fn origin(stream: &str, sequence_number: &str) -> String {
    format!("kinesis://{stream}/{SHARD}/{sequence_number}")
}

fn iterator(stream: &str, at: usize) -> String {
    format!("{stream}/{at}")
}

fn field<'a>(document: &'a Value, name: &str) -> &'a str {
    document[name].as_str().unwrap_or_default()
}

fn refused(status: u16, kind: &str, message: &str) -> (Event, Response) {
    (
        Event::Refused(kind.to_string()),
        json::error(status, kind, message),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const AT: &str = "20260910T000000Z";

    fn signed(action: &str, document: &Value) -> Request {
        Signer::new("kinesis", "r", "AKID", "secret").sign(
            json::request(action, document).header("Host", "kinesis.local"),
            AT,
        )
    }

    #[test]
    fn a_session_answers_in_kinesiss_shapes_and_refuses_a_bad_signature() {
        let mut session = Session::new("r", "AKID", "secret").with_stream("orders");
        let put = json!({ "StreamName": "orders", "PartitionKey": "k", "Data": "YTxi" });
        let (event, response) = session.answer(&signed("PutRecord", &put));
        assert_eq!(response.status, 200);
        assert!(
            response
                .text()
                .contains("\"ShardId\":\"shardId-000000000000\"")
        );
        let origin = origin("orders", "000000000000000000001");
        assert_eq!(
            event,
            Event::Put(Arrived::new(origin.clone(), b"a<b".to_vec()))
        );
        let iterate = json!({
            "StreamName": "orders", "ShardId": SHARD, "ShardIteratorType": "TRIM_HORIZON"
        });
        let (_, response) = session.answer(&signed("GetShardIterator", &iterate));
        assert!(response.text().contains("\"ShardIterator\":\"orders/0\""));
        let read = json!({ "ShardIterator": "orders/0", "Limit": 10 });
        let (event, response) = session.answer(&signed("GetRecords", &read));
        assert!(response.text().contains("\"Data\":\"YTxi\""));
        assert!(
            response
                .text()
                .contains("\"NextShardIterator\":\"orders/1\"")
        );
        assert!(matches!(event, Event::Read { count: 1, .. }));
        let read = json!({ "ShardIterator": "orders/1" });
        let (event, _) = session.answer(&signed("GetRecords", &read));
        assert!(matches!(event, Event::Read { count: 0, .. }));
        let read = json!({ "ShardIterator": "invoices/0" });
        let (event, _) = session.answer(&signed("GetRecords", &read));
        assert_eq!(
            event,
            Event::Refused("InvalidArgumentException".to_string())
        );
        let latest = json!({
            "StreamName": "orders", "ShardId": SHARD, "ShardIteratorType": "LATEST"
        });
        let (_, response) = session.answer(&signed("GetShardIterator", &latest));
        assert!(response.text().contains("\"orders/1\""));
        let (_, response) = session.answer(&signed("ListStreams", &json!({})));
        assert_eq!(response.status, 400);
        let other = Signer::new("kinesis", "r", "AKID", "wrong").sign(
            json::request("PutRecord", &put).header("Host", "kinesis.local"),
            AT,
        );
        let (event, response) = session.answer(&other);
        assert_eq!(
            event,
            Event::Refused("InvalidSignatureException".to_string())
        );
        assert_eq!(response.status, 403);
        assert_eq!(session.records("orders").len(), 1);
        assert!(session.records("invoices").is_empty());
    }
}
