//! Where a Receive Location reads its shard from, and how a verdict moves
//! it.
//!
//! A shard is read, never consumed, so the place is the transport's own:
//! the sequence number last accepted or refused for good, which moves only
//! contiguously ([`Contiguous`]) — so a record whose cycle failed, and
//! every one read after it, is read again: at-least-once, never a skip.
//! Beside it is the iterator the last read handed back, kept only once
//! every record that read handed on was accepted or refused.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use transport::contiguous::Contiguous;
use transport::{Acknowledgement, Verdict};

/// The place on the shard and the iterator to read on with, shared by a
/// transport and the acknowledgements of what it handed on.
#[derive(Clone, Debug, Default)]
pub struct Reading {
    /// The sequence number last accepted, `None` before the first.
    pub position: Contiguous<Option<String>>,
    iterator: Arc<Mutex<Option<String>>>,
}

impl Reading {
    /// The iterator the last read handed back, held: `None` where the next
    /// read asks for a new one after the position.
    pub fn iterator(&self) -> MutexGuard<'_, Option<String>> {
        self.iterator.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// How the record `sequence_number`, read after `after`, is
    /// acknowledged: accepted, the position moves to it where it stands at
    /// `after`, and `next` — the iterator its read handed back, given only
    /// with the read's last record — is kept for the next read. Refused, the
    /// same: a shard has no place to reject a record into, and a refused
    /// one is not read again. Failed, nothing moves, and the next read asks
    /// for an iterator after the position.
    #[must_use]
    pub fn acknowledgement(
        &self,
        after: Option<String>,
        sequence_number: String,
        next: Option<String>,
    ) -> Acknowledgement {
        let reading = self.clone();
        Acknowledgement::deferred(move |verdict| {
            if verdict != Verdict::Failed && reading.position.advance(&after, Some(sequence_number))
            {
                // Until the last record is accepted the iterator is none,
                // as the read left it.
                *reading.iterator() = next;
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepted_in_order_the_place_moves_and_the_iterator_is_kept() {
        let reading = Reading::default();
        let first = reading.acknowledgement(None, "1".into(), None);
        let last = reading.acknowledgement(Some("1".into()), "2".into(), Some("it".into()));
        first.acknowledge(Verdict::Accepted).expect("told");
        last.acknowledge(Verdict::Accepted).expect("told");
        assert_eq!(
            (reading.position.at(), reading.iterator().clone()),
            (Some("2".to_string()), Some("it".to_string()))
        );
    }

    #[test]
    fn a_refused_record_moves_the_place_as_an_accepted_one() {
        let reading = Reading::default();
        let first = reading.acknowledgement(None, "1".into(), None);
        let last = reading.acknowledgement(Some("1".into()), "2".into(), Some("it".into()));
        first
            .acknowledge(Verdict::Refused(transport::Refusal::Unacceptable))
            .expect("told");
        last.acknowledge(Verdict::Accepted).expect("told");
        assert_eq!(
            (reading.position.at(), reading.iterator().clone()),
            (Some("2".to_string()), Some("it".to_string()))
        );
    }

    #[test]
    fn a_failed_record_keeps_no_iterator_past_it() {
        let reading = Reading::default();
        let first = reading.acknowledgement(None, "1".into(), None);
        let second = reading.acknowledgement(Some("1".into()), "2".into(), None);
        let last = reading.acknowledgement(Some("2".into()), "3".into(), Some("it".into()));
        first.acknowledge(Verdict::Accepted).expect("told");
        second.acknowledge(Verdict::Failed).expect("told");
        last.acknowledge(Verdict::Accepted).expect("told");
        assert_eq!(reading.position.at(), Some("1".to_string()));
        assert_eq!(
            *reading.iterator(),
            None,
            "the next read asks after the place"
        );
    }
}
