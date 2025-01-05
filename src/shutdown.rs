use tokio::sync::broadcast::Receiver;

/// Listens for the server shutdown signal.
///
/// Shutdown is signalled using a `Receiver`. Only a single value is
/// ever sent. Once a value has been sent via the broadcast channel, the server
/// should shutdown.
///
/// The `Shutdown` struct listens for the signal and tracks that the signal has
/// been received. Callers may query for whether the shutdown signal has been
/// received or not.
#[derive(Debug)]
pub(crate) struct Shutdown {
    /// `true` if the shutdown signal has been received.
    is_shutdown: bool,
    /// The receive half of the channel used to listen for shutdown signals.
    notify: Receiver<()>,
}

impl Shutdown {
    /// Creates a new `Shutdown` backed by the given `Receiver`.
    pub(crate) fn new(notify: Receiver<()>) -> Shutdown {
        Shutdown {
            is_shutdown: false,
            notify,
        }
    }

    /// Returns `true` if the shutdown signal has been received.
    pub(crate) fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    /// Receive the shutdown notice, wating if necessary.
    pub(crate) async fn recv(&mut self) {
        // If we've already received a shutdown signal, there's no need to wait
        // for another one.
        if self.is_shutdown {
            return;
        }

        // Wait for a shutdown signal, cannot receive a `lag error` as only one value is ever sent.
        let _ = self.notify.recv().await;
        // Remember that the signal has been received.
        self.is_shutdown = true;
    }
}
