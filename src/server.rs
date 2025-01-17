//! Minimal Redis server implementation
//!
//! Provides an async `run` function that listens for inbound connections,
//! spawning a task per connection.

use crate::{db::DbDropGuard, CommandVariant, Connection, Db, Result, Shutdown};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc, Semaphore},
};
use tracing::{debug, error, info, warn};

/// Server listener state. Created in the `run` call. It includes a `run` method
/// which performs the TCP listening and initialization of per-connection state.
#[derive(Debug)]
struct Listener {
    /// Shared database handle.
    ///
    /// Contains the key / value store as well as the broadcast channels for
    /// pub/sub.
    ///
    /// This holds a wrapper around an `Arc`. The internal `Db` can be
    /// retrieved and passed into the per connection state (`Handler`).
    db_holder: DbDropGuard,
    /// TCP listener supplied by the `run` caller.
    listener: TcpListener,
    /// Limit the max number of connections.
    ///
    /// A `Semaphore` is used to limit the max number of connections. Before
    /// attempting to accept a new connection, a permit is acquired from the
    /// semaphore. If none are available, the listener waits for one.
    ///
    /// When handlers complete processing a connection, the permit is returned
    /// to the semaphore.
    limit_connections: Arc<Semaphore>,
    /// Broadcasts a shutdown signal to all active connections.
    ///
    /// The initial `shutdown` trigger is provided by the `run` caller. The
    /// server is responsible for gracefully shutting down active connections.
    /// When a connection task is spawned, it is passed a broadcast receiver
    /// handle. When a graceful shutdown is initiated, a `()` value is sent via
    /// the `broadcast::Sender`. Each active connection receives it, reaches a
    /// safe terminal state, and completes the task.
    notify_shutdown: broadcast::Sender<()>,
    /// Used as part of the graceful shutdown process to wait for client
    /// connections to complete processing.
    ///
    /// Tokio channels are closed once all `Sender` handles go out of scope.
    /// When a channel is closed, the receiver receives `None`. This is
    /// leveraged to detect all connection handlers completing. When a
    /// connection handler is initialized, it is assigned a clone of
    /// `shutdown_complete_tx`. When the listener shuts down, it drops the
    /// sender held by this `shutdown_complete_tx` field. Once all handler tasks
    /// complete, all clones of the `Sender` are also dropped. This results in
    /// `shutdown_complete_rx.recv()` completing with `None`. At this point, it
    /// is safe to exit the server process.
    shutdown_complete_tx: mpsc::Sender<()>,
}

/// Per-connection handler. Reads requests from `connection` and applies the
/// commands to `db`.
#[derive(Debug)]
struct Handler<S> {
    /// Shared database handle.
    ///
    /// When a command is received from `connection`, it is applied with `db`.
    /// The implementation of the command is in the `cmd` module. Each command
    /// will need to interact with `db` in order to complete the work.
    db: Db,
    /// The TCP connection decorated with the RESP encoder / decoder.
    ///
    /// When [`Listener`] receives an inbound connection, a stream `S` is
    /// passed to [`Connection::new`], which initializes the associated buffers.
    /// [`Connection`] allows the handler to operate at the "frame" level and keep
    /// the byte level protocol parsing details encapsulated in [`Connection`].
    connection: Connection<S>,
    /// Listen for shutdown notifications.
    ///
    /// A wrapper around the `broadcast::Receiver` paired with the sender in
    /// `Listener`. The connection handler processes requests from the
    /// connection until the peer disconnects **or** a shutdown notification is
    /// received from `shutdown`. In the latter case, any in-flight work being
    /// processed for the peer is continued until it reaches a safe state, at
    /// which point the connection is terminated.
    shutdown: Shutdown,
    /// Signal used to determine if a handler is operating.
    ///
    /// Not used directly. Instead, when all [`Handler`]s are dropped,
    /// a `None` message is sent to the receiver side,
    /// which indicates that the server is allowed to initiate shutdown.
    _shutdown_complete: mpsc::Sender<()>,
}

/// Maximum number of concurrent connections the redis server will accept.
///
/// When this limit is reached, the server will stop accepting connections until
/// an active connection terminates.
///
/// A real application will want to make this value configurable, but for this
/// example, it is hard coded.
///
/// This is also set to a pretty low value to discourage using this in
/// production (you'd think that all the disclaimers would make it obvious that
/// this is not a serious project... but I thought that about mini-http as
/// well).
const MAX_CONNECTIONS: usize = 250;

/// Run the redis server.
///
/// Accepts connections from the supplied listener. For each inbound connection,
/// a task is spawned to handle that connection. The server runs until the
/// `shutdown` future completes, at which point the server shuts down
/// gracefully.
///
/// `tokio::signal::ctrl_c()` can be used as the `shutdown` argument. This will
/// listen for a SIGINT signal.
pub async fn run(listener: TcpListener, shutdown: impl Future) {
    // When the provided `shutdown` future completes, we must send a shutdown
    // message to all active connections. We use a broadcast channel for this
    // purpose. The call below ignores the receiver of the broadcast pair, and when
    // a receiver is needed, the subscribe() method on the sender is used to create
    // one.
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

    // Initialize the listener state
    let mut server = Listener {
        listener,
        db_holder: DbDropGuard::new(),
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
    };

    // Concurrently run the server and listen for the `shutdown` signal. The
    // server task runs until an error is encountered, so under normal
    // circumstances, this `select!` statement runs until the `shutdown` signal
    // is received.
    tokio::select! {
        res = server.run() => {
            // If an error is received here, accepting connections from the TCP
            // listener failed multiple times and the server is giving up and
            // shutting down.
            //
            // Errors encountered when handling individual connections do not
            // bubble up to this point.
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            // The shutdown signal has been received.
            info!("shutting down");
        }
    }

    // Extract the `shutdown_complete` receiver and transmitter
    // explicitly drop `shutdown_transmitter`. This is important, as the
    // `.await` below would otherwise never complete.
    let Listener {
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;

    // When `notify_shutdown` is dropped, all tasks which have `subscribe`d will
    // receive the shutdown signal and can exit
    drop(notify_shutdown);
    // Drop final `Sender` so the `Receiver` below can complete
    drop(shutdown_complete_tx);

    // Wait for all active connections to finish processing. As the `Sender`
    // handle held by the listener has been dropped above, the only remaining
    // `Sender` instances are held by connection handler tasks. When those drop,
    // the `mpsc` channel will close and `recv()` will return `None`.
    let _ = shutdown_complete_rx.recv().await;
}

impl Listener {
    /// Run the server
    ///
    /// Listen for inbound connections. For each inbound connection, spawn a
    /// task to process that connection.
    ///
    /// # Errors
    ///
    /// Returns `Err` if accepting returns an error. This can happen for a
    /// number reasons that resolve over time. For example, if the underlying
    /// operating system has reached an internal limit for max number of
    /// sockets, accept will fail.
    ///
    /// The process is not able to detect when a transient error resolves
    /// itself. One strategy for handling this is to implement a back off
    /// strategy, which is what we do here.
    async fn run(&mut self) -> Result<()> {
        info!("accepting inbound connections");

        loop {
            // Wait for a permit to become available
            //
            // `acquire_owned` returns a permit that is bound to the semaphore.
            // When the permit value is dropped, it is automatically returned
            // to the semaphore.
            //
            // `acquire_owned()` returns `Err` when the semaphore has been closed.
            // We don't ever close the semaphore, so `unwrap()` is safe.
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();

            // Accept a new socket. This will attempt to perform error handling.
            // The `accept` method internally attempts to recover errors, so an
            // error here is non-recoverable.
            let socket = self.accept().await?;

            // Create the necessary per-connection handler state.
            let mut handler = Handler {
                // Get a handle to the shared database.
                db: self.db_holder.db(),
                // Initialize the connection state.
                // This allocates read/write buffers to perform RESP frame parsing.
                connection: Connection::new(socket),
                // Receive shutdown notifications.
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                // Notifies the receiver half once all clones are dropped.
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    error!(cause = %err, "connection error");
                }

                drop(permit);
            });
        }
    }

    /// Accept an inbound connection.
    ///
    /// Errors are handled by backing off and retrying. An exponential backoff
    /// strategy is used. After the first failure, the task waits for 1 second.
    /// After the second failure, the task waits for 2 seconds. Each subsequent
    /// failure doubles the wait time. If accepting fails on the 6th try after
    /// waiting for 64 seconds, then this function returns with an error.
    async fn accept(&mut self) -> Result<TcpStream> {
        let mut backoff = 1;

        // Try to accept a few times
        loop {
            // Perform the accept operation. If a socket is successfully
            // accepted, return it. Otherwise, save the error.
            match self.listener.accept().await {
                Ok((socket, _)) => {
                    debug!("successfully accepted inbound connection");
                    return Ok(socket);
                }
                Err(err) => {
                    if backoff > 64 {
                        error!(%err, "failed to accept inbound connection too many times");
                        // Accept has failed to many times. Return the error.
                        return Err(err.into());
                    }
                    warn!(%err, "got error accepting inbound connection, trying again in {backoff} seconds");
                }
            }

            // Pause the execution until the backoff period elapses.
            tokio::time::sleep(Duration::from_secs(backoff)).await;

            // Double the backoff time
            backoff *= 2;
        }
    }
}

impl Handler<TcpStream> {
    /// Process a single connection.
    ///
    /// Request frames are read from the socket and processed. Responses are
    /// written back to the socket.
    ///
    /// Currently, pipelining is not implemented. Pipelining is the ability to
    /// process more than one request concurrently per connection without
    /// interleaving frames. See for more details:
    /// https://redis.io/topics/pipelining
    ///
    /// When the shutdown signal is received, the connection is processed until
    /// it reaches a safe state, at which point it is terminated.
    #[tracing::instrument(skip_all)]
    async fn run(&mut self) -> Result<()> {
        // As long as the shutdown signal has not been received,
        // try to process a new request frame.
        while !self.shutdown.is_shutdown() {
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    // If shutdown signal is received, return from `run`.
                    // This will result in the task terminating.
                    return Ok(());
                }
            };

            // If `None` is returned from `read_frame()` then the peer closed the socket.
            // There is no further work to do and the task can be terminated.
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            // Convert the RESP frame into a `CommandVariant` struct. This returns an
            // error if the frame is not a valid redis command or it is an
            // unsupported command.
            let cmd = CommandVariant::from_frame(frame)?;

            debug!(?cmd);

            // Perform the work needed to apply the command. This may mutate the
            // database state as a result.
            //
            // The connection is passed into the apply function which allows the
            // command to write response frames directly to the connection. In
            // the case of pub/sub, multiple frames may be send back to the
            // peer.
            cmd.apply(&self.db, &mut self.connection, &mut self.shutdown)
                .await?;
        }

        Ok(())
    }
}
