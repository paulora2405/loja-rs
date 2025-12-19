//! Implement the `SUBSCRIBE` command.

use std::pin::Pin;

use bytes::Bytes;
use tokio::select;
use tokio::sync::broadcast;
use tokio_stream::{Stream, StreamExt, StreamMap};

/// Subscribes the client to one or more channels.
///
/// Once the client enters the subscribed state, it is not supposed to issue any
/// other commands, except for additional SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE,
/// PUNSUBSCRIBE, PING and QUIT commands.
#[derive(Debug, PartialEq, Eq)]
pub struct SubscribeCmd {
    channels: Vec<String>,
}

/// Unsubscribes the client from one or more channels.
///
/// When no channels are specified, the client is unsubscribed from all the
/// previously subscribed channels.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsubscribeCmd {
    channels: Vec<String>,
}

/// Stream of messages. The stream receives messages from the
/// `broadcast::Receiver`. We use `stream!` to create a `Stream` that consumes
/// messages. Because `stream!` values cannot be named, we box the stream using
/// a trait object.
type Message = Pin<Box<dyn Stream<Item = Bytes> + Send>>;

impl SubscribeCmd {
    /// Creates a new [`SubscribeCmd`] to listen on specified channels.
    pub(crate) fn new(channels: Vec<String>) -> Self {
        Self { channels }
    }
}
