#![forbid(unsafe_code)]

//! Streams that arrive as records on a Kinesis data stream. One record is
//! one Stream, its sequence number kept beside it.
//!
//! Kinesis is the event log of every organisation that lives in AWS, and
//! its JSON API is three calls a Location makes: put a record on a stream,
//! ask for an iterator over a shard at a position, take records with it. A
//! Receive Location reads its shard on from where it last stopped and hands
//! each record on as a Stream; a Send Location puts a Stream as one record.
//! Both are Signature Version 4 over plain HTTP/1.1 on a socket —
//! `https://` with the `tls` feature, which is the http technology's TLS
//! (ADR-0033).
//!
//! ```text
//! json.rs      the JSON 1.1 protocol: the request, the answer, the error
//! client.rs    Xmip's side: put a record, get an iterator, get records
//! session.rs   the far end a test or the playground runs on loopback
//! ```
//!
//! The endpoint, HTTP itself, Signature Version 4 and the judgement of an
//! answer come from the http technology (ADR-0044). Until 2026-09-14 the
//! signer came from the aws-sqs technology, a sideways import the record
//! forbids; what rides on HTTP is shared through the http technology.
//!
//! A record is bytes, base64 on the wire, one mebibyte at most:
//! [`ceiling`]. A shard is read, never consumed — Kinesis keeps records for
//! its retention period whoever has read them — so the transport keeps its
//! own place, the last sequence number it handed on, and asks for the
//! records after it next time. Nothing is claimed; [`Transport::claims`]
//! answers `None`. The origin URI is `kinesis://stream/shard/sequence`. A
//! send target is a stream name, or empty for this transport's own.
//!
//! The transport is its own far end (ADR-0051): [`Loopback`] stands the
//! session up at the endpoint's authority and takes the one put.

pub mod client;
pub mod json;
pub mod session;

use std::net::TcpListener;
use std::sync::Mutex;
use std::time::Duration;

pub use client::{Client, MAX_RECORDS, Position, Record};
use http::endpoint;
pub use session::{Event, Session};
use transport::error::{Result, TransportError, protocol_error};
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::socket;
use transport::{Arrived, Directions, Transport};

/// The largest record Kinesis carries: one mebibyte.
#[must_use]
pub const fn ceiling() -> usize {
    1024 * 1024
}

/// The one shard a stream opens with, and the one the session holds.
pub const SHARD: &str = "shardId-000000000000";

pub struct KinesisTransport {
    endpoint: String,
    region: String,
    stream: String,
    shard: String,
    access_key: String,
    secret_key: String,
    partition_key: String,
    position: Mutex<Option<String>>,
    timeout: Option<Duration>,
}

impl KinesisTransport {
    /// Speak to the Kinesis endpoint at `endpoint` — `https://kinesis.
    /// <region>.amazonaws.com` in the cloud, `http://host:port` for a
    /// stand-in — in `region`, about `stream`, on its first shard.
    #[must_use]
    pub fn new(endpoint: impl Into<String>, region: &str, stream: &str) -> Self {
        Self {
            endpoint: endpoint.into(),
            region: region.to_string(),
            stream: stream.to_string(),
            shard: SHARD.to_string(),
            access_key: String::new(),
            secret_key: String::new(),
            partition_key: "xmip".to_string(),
            position: Mutex::new(None),
            timeout: None,
        }
    }

    /// Sign as this access key.
    #[must_use]
    pub fn with_credentials(mut self, access_key: &str, secret_key: &str) -> Self {
        self.access_key = access_key.to_string();
        self.secret_key = secret_key.to_string();
        self
    }

    /// Read this shard rather than the first.
    #[must_use]
    pub fn on_shard(mut self, shard: &str) -> Self {
        self.shard = shard.to_string();
        self
    }

    /// Put records under this partition key — what decides their shard.
    #[must_use]
    pub fn keyed_by(mut self, partition_key: &str) -> Self {
        self.partition_key = partition_key.to_string();
        self
    }

    /// Give up on an endpoint that stops answering after `timeout`.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// The client this transport speaks through.
    ///
    /// # Errors
    /// Where the endpoint is not an HTTP URL.
    pub fn client(&self) -> Result<Client> {
        let client = Client::new(
            &self.endpoint,
            &self.region,
            &self.access_key,
            &self.secret_key,
        )?;
        Ok(match self.timeout {
            Some(timeout) => client.timing_out_after(timeout),
            None => client,
        })
    }

    /// A far end that holds this transport's credentials and its stream,
    /// for a test or the playground to run on loopback.
    #[must_use]
    pub fn session(&self) -> Session {
        let session = Session::new(&self.region, &self.access_key, &self.secret_key)
            .with_stream(&self.stream);
        match self.timeout {
            Some(timeout) => session.timing_out_after(timeout),
            None => session,
        }
    }

    /// The sequence number this transport last handed on, where it has.
    ///
    /// # Errors
    /// Where a receive panicked holding the position.
    pub fn position(&self) -> Result<Option<String>> {
        self.position
            .lock()
            .map(|held| held.clone())
            .map_err(|_| TransportError::permanent("the shard position was poisoned"))
    }

    /// The stream a target names, or this transport's own where it names
    /// none.
    fn resolve<'a>(&'a self, target: &'a str) -> &'a str {
        if target.is_empty() {
            &self.stream
        } else {
            target
        }
    }
}

impl Transport for KinesisTransport {
    fn name(&self) -> &'static str {
        "aws-kinesis"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    /// Every record after the last one handed on, the oldest held where
    /// none has been.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let client = self.client()?;
        let mut position = self
            .position
            .lock()
            .map_err(|_| TransportError::permanent("the shard position was poisoned"))?;
        let from = position
            .as_deref()
            .map_or(Position::Oldest, Position::After);
        let iterator = client.shard_iterator(&self.stream, &self.shard, from)?;
        let (records, _) = client.get_records(&iterator, MAX_RECORDS)?;
        let mut arrived = Vec::with_capacity(records.len());
        for record in records {
            *position = Some(record.sequence_number.clone());
            arrived.push(Arrived::new(
                format!(
                    "kinesis://{}/{}/{}",
                    self.stream, self.shard, record.sequence_number
                ),
                record.data,
            ));
        }
        Ok(arrived)
    }

    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        if bytes.len() > ceiling() {
            return Err(TransportError::permanent(format!(
                "{} bytes is over the {} one Kinesis record carries",
                bytes.len(),
                ceiling()
            )));
        }
        self.client()?
            .put_record(self.resolve(target), &self.partition_key, bytes)
            .map(|_| ())
    }
}

impl KinesisTransport {
    /// Both ends on this machine: the session stands in for Kinesis on an
    /// ephemeral local port, one stream and one credential, the loopback
    /// timeout on both sides.
    #[must_use]
    pub fn loopback() -> Self {
        Self::new("http://127.0.0.1:0", "eu-north-1", "orders")
            .with_credentials("AKID", "secret")
            .keyed_by("loopback")
            .timing_out_after(LOOPBACK_TIMEOUT)
    }

    /// A fresh near end aimed at the session at `address`, with this
    /// transport's credentials, stream and partition key.
    fn aimed_at(&self, address: &str) -> Self {
        let near = Self::new(format!("http://{address}"), &self.region, &self.stream)
            .with_credentials(&self.access_key, &self.secret_key)
            .keyed_by(&self.partition_key);
        match self.timeout {
            Some(timeout) => near.timing_out_after(timeout),
            None => near,
        }
    }
}

/// A session listening for its one put.
struct Serving {
    session: Session,
    listener: TcpListener,
    address: String,
}

impl FarEnd for Serving {
    fn address(&self) -> &str {
        &self.address
    }

    fn take_one(mut self: Box<Self>) -> Result<Arrived> {
        match self.session.serve_one(&self.listener)? {
            Event::Put(arrived) => Ok(arrived),
            other => Err(protocol_error(format!("{other:?} where a put was due"))),
        }
    }
}

impl Loopback for KinesisTransport {
    fn ceiling(&self) -> Option<usize> {
        Some(ceiling())
    }

    /// The session, bound at the endpoint's authority — `127.0.0.1:0` for
    /// the loopback — holding this transport's stream.
    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        let (listener, address) = socket::bind_tcp(&endpoint::authority(&self.endpoint)?)?;
        Ok(Box::new(Serving {
            session: self.session(),
            listener,
            address,
        }))
    }

    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        self.aimed_at(address).send("", payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::JoinHandle;

    fn node(endpoint: &str, secret: &str) -> KinesisTransport {
        KinesisTransport::new(endpoint, "eu-north-1", "orders")
            .with_credentials("AKID", secret)
            .keyed_by("probe")
            .timing_out_after(Duration::from_secs(2))
    }

    fn serve(
        mut session: Session,
        listener: TcpListener,
        requests: usize,
    ) -> JoinHandle<(Session, Vec<Event>)> {
        std::thread::spawn(move || {
            let events = (0..requests)
                .map(|_| session.serve_one(&listener).expect("served"))
                .collect();
            (session, events)
        })
    }

    #[test]
    fn what_is_put_to_a_session_is_read_back_in_order_and_the_place_moves_on() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let near = node(&format!("http://{address}"), "secret");
        // Two puts, a receive, an empty receive, a put, a receive: each
        // receive an iterator and a read.
        let far_end = serve(near.session(), listener, 9);
        near.send("", b"UNA:+.? '").expect("its own stream");
        near.send("orders", &[0, 0xff, b'\r', b'\n'])
            .expect("a stream");
        let arrived = near.receive().expect("received");
        assert_eq!(arrived.len(), 2);
        assert_eq!(arrived[0].bytes, b"UNA:+.? '");
        assert_eq!(arrived[1].bytes, [0, 0xff, b'\r', b'\n']);
        assert!(
            arrived[0]
                .origin_uri
                .starts_with("kinesis://orders/shardId-000000000000/")
        );
        assert!(near.receive().expect("received").is_empty(), "nothing new");
        near.send("", b"").expect("an empty record");
        let later = near.receive().expect("received");
        assert_eq!(later.len(), 1, "only what came after");
        assert!(later[0].bytes.is_empty());
        assert_eq!(
            near.position().expect("held").as_deref(),
            Some(later[0].origin_uri.rsplit('/').next().unwrap_or_default())
        );
        let (session, events) = far_end.join().expect("thread");
        assert_eq!(
            session.records("orders").len(),
            3,
            "a shard is read, not consumed"
        );
        assert_eq!(
            events[0],
            Event::Put(Arrived::new(
                arrived[0].origin_uri.clone(),
                b"UNA:+.? '".to_vec()
            ))
        );
        assert!(matches!(&events[2], Event::Iterated { kind, .. } if kind == "TRIM_HORIZON"));
        assert!(
            matches!(&events[4], Event::Iterated { kind, .. } if kind == "AFTER_SEQUENCE_NUMBER")
        );
        assert!(matches!(events[5], Event::Read { count: 0, .. }));
        assert!(matches!(events[8], Event::Read { count: 1, .. }));
    }

    #[test]
    fn a_wrong_secret_is_refused_with_kinesiss_own_status_and_exception() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let far_end = serve(node("http://x", "secret").session(), listener, 1);
        let failure = node(&format!("http://{address}"), "wrong")
            .send("", b"x")
            .expect_err("refused");
        assert!(
            failure.message.contains("403 InvalidSignatureException"),
            "{failure}"
        );
        assert!(!failure.retryable);
        let (_, events) = far_end.join().expect("thread");
        assert_eq!(
            events,
            vec![Event::Refused("InvalidSignatureException".to_string())]
        );
    }

    #[test]
    fn a_stream_is_not_claimed_and_an_unreachable_endpoint_is_retryable() {
        let near = node("http://127.0.0.1:1", "secret").on_shard("shardId-000000000001");
        assert!(near.claims().is_none());
        assert_eq!(near.name(), "aws-kinesis");
        assert!(near.directions().receives() && near.directions().sends());
        assert!(near.receive().expect_err("nothing listening").retryable);
        assert_eq!(near.position().expect("held"), None);
        assert!(
            !node("kinesis.local", "s")
                .send("", b"x")
                .expect_err("no scheme")
                .retryable
        );
    }

    #[test]
    fn what_kinesis_does_not_carry_is_refused_before_the_wire_with_the_reason() {
        let near = node("http://127.0.0.1:1", "secret");
        let over = vec![b'x'; ceiling() + 1];
        let failure = near.send("", &over).expect_err("over the ceiling");
        assert!(!failure.retryable);
        assert!(failure.message.contains("1048576"), "{failure}");
    }

    /// The payloads a record must carry whole, and one at the brim.
    fn edge_payloads() -> Vec<(&'static str, Vec<u8>)> {
        vec![
            ("empty", Vec::new()),
            ("one byte", vec![0x2a]),
            ("every byte", (0..=255).collect()),
            ("nul run", vec![0; 512]),
            ("high bytes", vec![0xff; 512]),
            ("crlf storm", b"\r\n".repeat(400)),
            ("the brim", vec![b'k'; ceiling()]),
        ]
    }

    #[test]
    fn a_loopback_round_puts_one_record_and_takes_it_at_the_session() {
        let kinesis = KinesisTransport::loopback();
        let arrived = kinesis.round(b"UNA:+.? '").expect("round");
        assert_eq!(arrived.bytes, b"UNA:+.? '");
        assert!(
            arrived
                .origin_uri
                .starts_with("kinesis://orders/shardId-000000000000/"),
            "{}",
            arrived.origin_uri
        );
        assert_eq!(kinesis.name(), "aws-kinesis");
        assert!(kinesis.refuses(&[0, 0xff]).is_none(), "bytes are bytes");
    }

    #[test]
    fn the_loopback_returns_the_edge_payloads_whole_and_refuses_over_the_brim() {
        let kinesis = KinesisTransport::loopback();
        assert_eq!(kinesis.ceiling(), Some(1024 * 1024));
        for (name, payload) in edge_payloads() {
            let arrived = kinesis.round(&payload).expect(name);
            assert_eq!(arrived.bytes, payload, "{name}");
        }
        let over = vec![b'k'; ceiling() + 1];
        let failure = kinesis.round(&over).expect_err("over the brim");
        assert!(failure.message.starts_with("send failed:"), "{failure}");
        assert!(failure.message.contains("1048576"), "{failure}");
    }
}
