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
//! client.rs    Xmip's side: put a record, get an iterator, get records
//! reading.rs   the place on the shard, and how a verdict moves it
//! session.rs   the far end a test or the playground runs on loopback
//! ```
//!
//! The endpoint, HTTP itself and the judgement of an answer come from the
//! http technology; Signature Version 4 and the JSON 1.1 protocol from the
//! AWS crate, where [`KINESIS`] names this service to it (ADR-0044). Until
//! 2026-09-14 the signer came from the aws-sqs technology, a sideways
//! import the record forbids, and JSON 1.1 was this crate's own until the
//! owner's ruling of 2026-09-22 put what AWS speaks in the AWS crate.
//!
//! A record is bytes, base64 on the wire, one mebibyte at most:
//! [`ceiling`]. A shard is read, never consumed — Kinesis keeps records for
//! its retention period whoever has read them — so the transport keeps its
//! own place, the last sequence number the runtime accepted after its whole
//! receive cycle or refused for good, and asks for the records after it
//! next time. The place moves only contiguously: a record whose cycle
//! failed and what followed it are read again. Nothing is claimed;
//! [`Transport::claims`] answers `None`. The origin URI is
//! `kinesis://stream/shard/sequence`. A send target is a stream name, or
//! empty for this transport's own.
//!
//! The transport is its own far end (ADR-0051): [`Loopback`] stands the
//! session up at the endpoint's authority and takes the one put.

pub mod client;
mod reading;
pub mod session;

use std::net::TcpListener;
use std::time::Duration;

use aws::json;
pub use client::{Client, MAX_RECORDS, Position, Record};
use http::endpoint::Connections;
use net::Endpoint;
use net::ceiling;
use reading::Reading;
pub use session::{Event, Session};
use transport::ArrivalIdentity;
use transport::error::{Result, protocol_error};
use transport::listening::Listening;
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::socket;
use transport::{Arrived, Configured, Directions, Transport};
use xcore::settings::{Applies, Fixed, Kind, Presence, Read, Setting, Settings};

/// Kinesis as the JSON 1.1 protocol names it: the prefix of every target,
/// and the exceptions that say come back — a shard's throughput, the
/// account's limit, the key service's throttle, a failure of its own.
pub const KINESIS: json::Service = json::Service {
    name: "Kinesis",
    target: "Kinesis_20131202",
    repeatable: &[
        "ProvisionedThroughputExceededException",
        "LimitExceededException",
        "KMSThrottlingException",
        "InternalFailure",
    ],
};

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
    reading: Reading,
    timeout: Option<Duration>,
    /// The connections kept to the service, shared by every client this
    /// makes.
    connections: Connections,
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
            reading: Reading::default(),
            timeout: None,
            connections: Connections::new(),
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
    fn on_shard(mut self, shard: &str) -> Self {
        self.shard = shard.to_string();
        self
    }

    /// Put records under this partition key — what decides their shard.
    #[must_use]
    fn keyed_by(mut self, partition_key: &str) -> Self {
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
        let client = client.sharing(self.connections.clone());
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

    /// The sequence number of the last record accepted, where one has been.
    #[must_use]
    pub fn position(&self) -> Option<String> {
        self.reading.position.at()
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

    fn arrivals(&self) -> transport::Arrivals {
        transport::Arrivals::Ordered("a cursor moves only contiguously")
    }

    /// Every record after the last one accepted, the oldest held where none
    /// has been, each whole. A shard is never consumed: what a verdict
    /// moves is this transport's own place — on
    /// [`transport::Verdict::Accepted`] to the record, where it stands at
    /// the one before; on `Refused` the same, as a shard has no place to
    /// reject a record into and a refused one is not read again; on
    /// `Failed` nowhere, so the next receive reads the failed record and
    /// what followed it again.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let client = self.client()?;
        let mut iterator = self.reading.iterator();
        let position = self.reading.position.at();
        // The iterator the last read handed back, where it is still good;
        // a new one from the position where it is missing or failed —
        // expired after five minutes unused, or its shard gone.
        let kept = iterator
            .take()
            .and_then(|iterator| client.get_records(&iterator, MAX_RECORDS).ok());
        let (records, next) = if let Some(read) = kept {
            read
        } else {
            let from = position
                .as_deref()
                .map_or(Position::Oldest, Position::After);
            let iterator = client.shard_iterator(&self.stream, &self.shard, from)?;
            client.get_records(&iterator, MAX_RECORDS)?
        };
        if records.is_empty() {
            *iterator = next;
            return Ok(Vec::new());
        }
        // The iterator is kept by the last record's acceptance, not here.
        drop(iterator);
        let mut after = position;
        let last = records.len() - 1;
        let mut arrived = Vec::with_capacity(records.len());
        for (at, record) in records.into_iter().enumerate() {
            let sequence_number = record.sequence_number;
            let origin = format!("kinesis://{}/{}/{sequence_number}", self.stream, self.shard);
            let kept = if at == last { next.clone() } else { None };
            let acknowledgement =
                self.reading
                    .acknowledgement(after.clone(), sequence_number.clone(), kept);
            after = Some(sequence_number);
            arrived.push(Arrived::whole(origin, record.data, acknowledgement).detected());
        }
        Ok(arrived)
    }

    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        ceiling::within(bytes.len(), ceiling(), "one Kinesis record carries")?;
        self.client()?
            .put_record(self.resolve(target), &self.partition_key, bytes)
            .map(|_| ())
    }
}

impl Configured for KinesisTransport {
    /// The address is the Kinesis endpoint, `https://kinesis.<region>.
    /// amazonaws.com`. The access key and its secret are the Location's
    /// credentials, not settings: a secret never is.
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

    fn configured(address: &str, settings: &Read) -> Result<Self> {
        // The access key and secret come through the Location's credentials.
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

impl Loopback for KinesisTransport {
    fn arrival_identity(&self) -> ArrivalIdentity {
        ArrivalIdentity::Unnamed(
            "the broker delivers it and names no sender; the peer is the broker",
        )
    }

    fn ceiling(&self) -> Option<usize> {
        Some(ceiling())
    }

    /// The session, bound at the endpoint's authority — `127.0.0.1:0` for
    /// the loopback — holding this transport's stream.
    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        let mut session = self.session();
        Ok(Box::new(Listening::new(
            move |listener: &TcpListener| match session.serve_one(listener)? {
                Event::Put(arrived) => Ok(arrived),
                other => Err(protocol_error(format!("{other:?} where a put was due"))),
            },
            socket::bind_tcp(&Endpoint::parse(&self.endpoint)?.address())?,
        )))
    }

    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        self.aimed_at(address).send("", payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::JoinHandle;
    use transport::Taken;

    fn node(endpoint: &str, secret: &str) -> KinesisTransport {
        KinesisTransport::new(endpoint, "eu-north-1", "orders")
            .with_credentials("AKID", secret)
            .keyed_by("probe")
            .timing_out_after(Duration::from_secs(2))
    }

    #[test]
    fn kinesis_declares_its_settings_and_reads_through_them() {
        use xcore::settings::Given;
        assert_eq!(KinesisTransport::SETTINGS.problems(), Vec::<String>::new());
        let text = |name: &str, value: &str| (name.to_string(), Given::Text(value.to_string()));
        let endpoint = "https://kinesis.eu-north-1.amazonaws.com";
        let received = KinesisTransport::open(
            endpoint,
            Applies::Receive,
            &[text("region", "eu-north-1"), text("stream", "orders")],
        )
        .expect("built");
        assert_eq!(received.endpoint, endpoint);
        assert_eq!(
            (received.region.as_str(), received.stream.as_str()),
            ("eu-north-1", "orders")
        );
        assert_eq!(received.shard, SHARD);
        let sent = KinesisTransport::open(
            endpoint,
            Applies::Send,
            &[
                text("region", "eu-north-1"),
                text("stream", "orders"),
                text("partition_key", "customer"),
                text("timeout", "5s"),
            ],
        )
        .expect("built");
        assert_eq!(sent.partition_key, "customer");
        assert_eq!(sent.timeout, Some(Duration::from_secs(5)));
        let Err(refused) =
            KinesisTransport::open(endpoint, Applies::Send, &[text("region", "eu-north-1")])
        else {
            panic!("the stream is required");
        };
        assert!(refused.message.contains("\"stream\""), "{refused}");
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
        // Two puts, a receive, an empty receive, a put, a receive: an
        // iterator for the first receive, and a read for each.
        let far_end = serve(near.session(), listener, 7);
        near.send("", b"UNA:+.? '").expect("its own stream");
        near.send("orders", &[0, 0xff, b'\r', b'\n'])
            .expect("a stream");
        let arrived = taken(near.receive().expect("received"));
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
        let later = taken(near.receive().expect("received"));
        assert_eq!(later.len(), 1, "only what came after");
        assert!(later[0].bytes.is_empty());
        assert_eq!(
            near.position().as_deref(),
            Some(later[0].origin_uri.rsplit('/').next().unwrap_or_default())
        );
        let (session, events) = far_end.join().expect("thread");
        assert_eq!(
            session.records("orders").len(),
            3,
            "a shard is read, not consumed"
        );
        assert_eq!(events[0], Event::Put(arrived[0].clone()));
        assert!(matches!(&events[2], Event::Iterated { kind, .. } if kind == "TRIM_HORIZON"));
        assert!(matches!(events[3], Event::Read { count: 2, .. }));
        // The next reads go on with the iterator the last handed back.
        assert!(matches!(events[4], Event::Read { count: 0, .. }));
        assert!(matches!(events[6], Event::Read { count: 1, .. }));
        let iterated = |e: &&Event| matches!(e, Event::Iterated { .. });
        assert_eq!(events.iter().filter(iterated).count(), 1);
    }

    #[test]
    fn an_iterator_the_service_no_longer_takes_is_asked_for_again() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let near = node(&format!("http://{address}"), "secret");
        // A put, then a receive whose kept iterator is refused, as an
        // expired one is: the refusal, a new iterator and its read.
        let far_end = serve(near.session(), listener, 4);
        near.send("", b"after").expect("put");
        *near.reading.iterator() = Some("orders/expired".to_string());
        let arrived = taken(near.receive().expect("received"));
        assert_eq!(arrived.len(), 1);
        assert_eq!(arrived[0].bytes, b"after");
        let (_, events) = far_end.join().expect("thread");
        assert!(matches!(&events[1], Event::Refused(_)), "{events:?}");
        assert!(matches!(&events[2], Event::Iterated { kind, .. } if kind == "TRIM_HORIZON"));
        assert!(matches!(events[3], Event::Read { count: 1, .. }));
    }

    /// Every arrival of one receive, accepted in order, as the runtime
    /// accepts a cycle that completed.
    fn taken(arrived: Vec<Arrived>) -> Vec<Taken> {
        arrived
            .into_iter()
            .map(|one| one.taken().expect("accepted"))
            .collect()
    }

    #[test]
    fn a_refused_record_moves_the_place_past_it_and_is_not_read_again() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let near = node(&format!("http://{address}"), "secret");
        // Three puts; a read refusing the second; the kept iterator reads
        // nothing.
        let far_end = serve(near.session(), listener, 6);
        for payload in [&b"C1"[..], b"C2", b"C3"] {
            near.send("", payload).expect("put");
        }
        let mut arrived = near.receive().expect("received").into_iter();
        arrived.next().expect("C1").taken().expect("accepted");
        arrived
            .next()
            .expect("C2")
            .refused(transport::Refusal::Unacceptable)
            .expect("refused");
        let third = arrived.next().expect("C3").taken().expect("accepted");
        let place = third.origin_uri.rsplit('/').next().map(str::to_string);
        assert_eq!(near.position(), place, "past C2, at C3");
        assert!(near.receive().expect("received").is_empty(), "C2 not again");
        let (_, events) = far_end.join().expect("thread");
        assert!(
            matches!(events[5], Event::Read { count: 0, .. }),
            "{events:?}"
        );
    }

    #[test]
    fn a_failed_record_and_what_followed_it_are_read_again_and_nothing_is_skipped() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let near = node(&format!("http://{address}"), "secret");
        // Three puts; a read failing the second; a new iterator after the
        // first and its read; then the kept iterator reads nothing.
        let far_end = serve(near.session(), listener, 8);
        for payload in [&b"C1"[..], b"C2", b"C3"] {
            near.send("", payload).expect("put");
        }
        let mut arrived = near.receive().expect("received").into_iter();
        assert!(arrived.as_slice().iter().all(Arrived::defers));
        let first = arrived.next().expect("C1").taken().expect("accepted");
        arrived.next().expect("C2").failed().expect("failed");
        let third = arrived.next().expect("C3").taken().expect("accepted");
        assert_eq!(
            (first.bytes.as_slice(), third.bytes.as_slice()),
            (&b"C1"[..], &b"C3"[..])
        );
        let place = first.origin_uri.rsplit('/').next().map(str::to_string);
        assert_eq!(near.position(), place, "held at C1");
        let again = taken(near.receive().expect("received again"));
        let bytes: Vec<&[u8]> = again.iter().map(|t| t.bytes.as_slice()).collect();
        assert_eq!(bytes, [&b"C2"[..], b"C3"], "C2 and what followed it");
        assert!(near.receive().expect("received").is_empty(), "all accepted");
        let (_, events) = far_end.join().expect("thread");
        let after =
            |e: &Event| matches!(e, Event::Iterated { kind, .. } if kind.starts_with("AFTER"));
        assert!(after(&events[5]), "{events:?}");
        assert!(matches!(events[6], Event::Read { count: 2, .. }));
        assert!(matches!(events[7], Event::Read { count: 0, .. }));
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
        assert_eq!(near.position(), None);
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
