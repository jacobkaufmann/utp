use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use tokio::sync::{mpsc, oneshot};

use crate::common::SocketEvent;
use crate::packet::Packet;

mod id;
pub use id::{ConnectionId, ConnectionIdManager};

mod state;
use state::{ConnectionStateMachine, Read, Write};
pub use state::{Endpoint, StreamEvent};

pub struct Connection {
    shutdown: Option<oneshot::Sender<()>>,
    reads: mpsc::UnboundedSender<Read>,
    writes: mpsc::UnboundedSender<Write>,
}

impl Connection {
    pub fn new(
        endpoint: Endpoint,
        id: ConnectionId,
        packets: mpsc::UnboundedReceiver<Packet>,
        socket_events: mpsc::UnboundedSender<SocketEvent>,
        stream_events: mpsc::UnboundedSender<StreamEvent>,
        connected: oneshot::Sender<io::Result<()>>,
    ) -> Self {
        let (read_tx, read_rx) = mpsc::unbounded_channel();
        let (write_tx, write_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let mut state_machine = ConnectionStateMachine::new(
            endpoint,
            id,
            packets,
            socket_events,
            stream_events,
            connected,
            shutdown_rx,
        );
        tokio::spawn(async move {
            state_machine.init(Instant::now());
            state_machine.event_loop(read_rx, write_rx).await;
        });

        Self {
            shutdown: Some(shutdown_tx),
            reads: read_tx,
            writes: write_tx,
        }
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let (read_tx, read_rx) = oneshot::channel();
        let read = Read {
            buf: buf.len(),
            read: read_tx,
        };

        let _ = self.reads.send(read).unwrap();
        let read = read_rx
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::Interrupted))?;

        match read {
            Ok(data) => {
                let n = data.len();
                buf[..n].copy_from_slice(data.as_slice());
                Ok(n)
            }
            Err(err) => Err(err),
        }
    }

    pub async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let (written_tx, written_rx) = oneshot::channel();
        let write = Write {
            buf: buf.to_vec(),
            written: written_tx,
        };

        let _ = self.writes.send(write).unwrap();
        let written = written_rx
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::Interrupted))?;

        written
    }

    pub fn shutdown(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

pub struct Incoming {
    conn: Option<Connection>,
    connected_rx: oneshot::Receiver<io::Result<()>>,
}

impl Incoming {
    pub fn new(
        id: ConnectionId,
        packets: mpsc::UnboundedReceiver<Packet>,
        socket_events: mpsc::UnboundedSender<SocketEvent>,
        stream_events: mpsc::UnboundedSender<StreamEvent>,
    ) -> Self {
        let (connected_tx, connected_rx) = oneshot::channel();
        let conn = Connection::new(
            Endpoint::Acceptor,
            id,
            packets,
            socket_events,
            stream_events,
            connected_tx,
        );

        Self {
            conn: Some(conn),
            connected_rx,
        }
    }
}

impl Future for Incoming {
    type Output = io::Result<Connection>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.connected_rx).poll(cx).map(|result| {
            if let Err(..) = result {
                return Err(io::Error::from(io::ErrorKind::Interrupted));
            }

            Ok(self.conn.take().unwrap())
        })
    }
}

pub struct Pending {
    conn: Option<Connection>,
    connected_rx: oneshot::Receiver<io::Result<()>>,
}

impl Pending {
    pub fn new(
        id: ConnectionId,
        packets: mpsc::UnboundedReceiver<Packet>,
        socket_events: mpsc::UnboundedSender<SocketEvent>,
        stream_events: mpsc::UnboundedSender<StreamEvent>,
    ) -> Self {
        let (connected_tx, connected_rx) = oneshot::channel();
        let conn = Connection::new(
            Endpoint::Initiator,
            id,
            packets,
            socket_events,
            stream_events,
            connected_tx,
        );

        Self {
            conn: Some(conn),
            connected_rx,
        }
    }
}

impl Future for Pending {
    type Output = io::Result<Connection>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.connected_rx).poll(cx).map(|result| {
            if let Err(..) = result {
                return Err(io::Error::from(io::ErrorKind::Interrupted));
            }

            Ok(self.conn.take().unwrap())
        })
    }
}
