//! Xmip's side: the three calls a Location makes, each one signed JSON
//! request over one connection to the Kinesis endpoint.
//!
//! The endpoint is the service — `https://kinesis.eu-north-1.amazonaws.com`
//! in the cloud, `http://127.0.0.1:4566` for a stand-in — and every
//! request is a `POST` to `/` naming its action. A stream is read a shard
//! at a time through an iterator: ask for one at a position, take records
//! with it, continue with the one the answer hands back.

use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde_json::{Value, json};
use transport::error::{Result, protocol_error};

use crate::json;
use http::endpoint;
use http::message;
use transport_aws_sqs::sigv4::{self, Signer};

/// The most one `GetRecords` hands back.
pub const MAX_RECORDS: u16 = 10_000;

/// One record as it came off a shard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub sequence_number: String,
    pub partition_key: String,
    pub data: Vec<u8>,
}

/// Where a shard is read from, in Kinesis's own words.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Position<'a> {
    /// The oldest record still held: `TRIM_HORIZON`.
    Oldest,
    /// The record after this sequence number: `AFTER_SEQUENCE_NUMBER`.
    After(&'a str),
    /// Only what arrives from now on: `LATEST`.
    Latest,
}

impl Position<'_> {
    /// The `ShardIteratorType` this position asks for.
    #[must_use]
    pub const fn kind(self) -> &'static str {
        match self {
            Self::Oldest => "TRIM_HORIZON",
            Self::After(_) => "AFTER_SEQUENCE_NUMBER",
            Self::Latest => "LATEST",
        }
    }
}

pub struct Client {
    endpoint: String,
    host: String,
    signer: Signer,
    timeout: Option<Duration>,
}

impl Client {
    /// Speak to the Kinesis endpoint at `endpoint` — `http://host:port` or
    /// `https://host:port` — in `region`, signing as `access_key`.
    ///
    /// # Errors
    /// Where `endpoint` is not an HTTP URL.
    pub fn new(endpoint: &str, region: &str, access_key: &str, secret_key: &str) -> Result<Self> {
        Ok(Self {
            endpoint: endpoint.to_string(),
            host: endpoint::authority(endpoint)?,
            signer: Signer::new("kinesis", region, access_key, secret_key),
            timeout: None,
        })
    }

    /// Give up on an endpoint that stops answering after `timeout`.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Put `bytes` as one record on `stream` under `partition_key`, and
    /// learn the shard and the sequence number it landed at.
    ///
    /// # Errors
    /// Where the endpoint refused or could not be reached.
    pub fn put_record(
        &self,
        stream: &str,
        partition_key: &str,
        bytes: &[u8],
    ) -> Result<(String, String)> {
        let document = json!({
            "StreamName": stream,
            "PartitionKey": partition_key,
            "Data": STANDARD.encode(bytes),
        });
        let answer = self.call("PutRecord", &document)?;
        Ok((text(&answer, "ShardId"), text(&answer, "SequenceNumber")))
    }

    /// An iterator over `shard` of `stream`, from `position`.
    ///
    /// # Errors
    /// Where there is no such stream or shard, or the endpoint refused or
    /// could not be reached.
    pub fn shard_iterator(
        &self,
        stream: &str,
        shard: &str,
        position: Position<'_>,
    ) -> Result<String> {
        let mut document = json!({
            "StreamName": stream,
            "ShardId": shard,
            "ShardIteratorType": position.kind(),
        });
        if let Position::After(sequence_number) = position {
            document["StartingSequenceNumber"] = sequence_number.into();
        }
        Ok(text(
            &self.call("GetShardIterator", &document)?,
            "ShardIterator",
        ))
    }

    /// Up to `limit` records from `iterator`, and the iterator to continue
    /// with — `None` where the shard is closed.
    ///
    /// # Errors
    /// Where the iterator is spent, or the endpoint refused, could not be
    /// reached, or answered with records that are not base64.
    pub fn get_records(&self, iterator: &str, limit: u16) -> Result<(Vec<Record>, Option<String>)> {
        let document = json!({ "ShardIterator": iterator, "Limit": limit });
        let answer = self.call("GetRecords", &document)?;
        let records = answer["Records"]
            .as_array()
            .map(|records| records.iter().map(record).collect::<Result<Vec<_>>>())
            .transpose()?
            .unwrap_or_default();
        let next = answer["NextShardIterator"].as_str().map(str::to_string);
        Ok((records, next))
    }

    fn call(&self, action: &str, document: &Value) -> Result<Value> {
        let request = json::request(action, document).header("Host", &self.host);
        let signed = self.signer.sign(request, &sigv4::now());
        let stream = endpoint::connect(&self.endpoint, self.timeout)?;
        json::judge(&message::exchange(stream, &signed)?)
    }
}

fn text(document: &Value, name: &str) -> String {
    document[name].as_str().unwrap_or_default().to_string()
}

fn record(value: &Value) -> Result<Record> {
    let data = STANDARD
        .decode(value["Data"].as_str().unwrap_or_default())
        .map_err(|e| protocol_error(format!("a record whose Data is not base64: {e}")))?;
    Ok(Record {
        sequence_number: text(value, "SequenceNumber"),
        partition_key: text(value, "PartitionKey"),
        data,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{Event, Session};
    use transport::socket;

    #[test]
    fn the_three_calls_reach_a_session_and_come_back_shaped_as_kinesis_shapes_them() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let far_end = std::thread::spawn(move || {
            let mut session = Session::new("eu-north-1", "AKID", "secret")
                .with_stream("orders")
                .timing_out_after(Duration::from_secs(2));
            let events: Vec<Event> = (0..7)
                .map(|_| session.serve_one(&listener).expect("served"))
                .collect();
            (session, events)
        });
        let client = Client::new(&format!("http://{address}"), "eu-north-1", "AKID", "secret")
            .expect("endpoint")
            .timing_out_after(Duration::from_secs(2));
        let (shard, first) = client
            .put_record("orders", "k1", b"UNA:+.? '")
            .expect("put");
        assert_eq!(shard, crate::SHARD);
        client
            .put_record("orders", "k2", &[0, 0xff, b'\n'])
            .expect("put");
        let iterator = client
            .shard_iterator("orders", &shard, Position::Oldest)
            .expect("iterator");
        let (records, next) = client.get_records(&iterator, 10).expect("records");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].sequence_number, first);
        assert_eq!(records[0].partition_key, "k1");
        assert_eq!(records[0].data, b"UNA:+.? '");
        assert_eq!(records[1].data, [0, 0xff, b'\n']);
        let iterator = client
            .shard_iterator("orders", &shard, Position::After(&first))
            .expect("iterator");
        let (records, _) = client.get_records(&iterator, 10).expect("records");
        assert_eq!(records.len(), 1, "after the first");
        assert!(next.is_some(), "the shard stays open");
        let missing = client
            .shard_iterator("invoices", &shard, Position::Oldest)
            .expect_err("no such stream");
        assert!(
            missing.message.contains("400 ResourceNotFoundException"),
            "{missing}"
        );
        assert!(!missing.retryable);
        let (session, events) = far_end.join().expect("thread");
        assert_eq!(session.records("orders").len(), 2);
        assert!(matches!(&events[2], Event::Iterated { kind, .. } if kind == "TRIM_HORIZON"));
        assert!(matches!(&events[3], Event::Read { count: 2, .. }));
        assert_eq!(
            events[6],
            Event::Refused("ResourceNotFoundException".to_string())
        );
    }

    #[test]
    fn what_is_not_an_endpoint_is_refused_and_nobody_listening_is_retryable() {
        assert!(Client::new("kinesis.local", "r", "a", "s").is_err());
        let client = Client::new("http://127.0.0.1:1", "r", "a", "s").expect("endpoint");
        assert!(
            client
                .put_record("s", "k", b"x")
                .expect_err("nobody")
                .retryable
        );
        assert_eq!(Position::Latest.kind(), "LATEST");
        assert!(record(&json!({ "Data": "not base64!" })).is_err());
    }
}
