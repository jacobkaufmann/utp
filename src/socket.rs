use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};

use crate::common::SocketEvent;
use crate::conn::{Connection, ConnectionId, ConnectionIdManager, Incoming, Pending};
use crate::packet::{Packet, PacketType};

struct Socket {
    incoming_packets: mpsc::UnboundedReceiver<(Packet, SocketAddr)>,
    outgoing_packets: mpsc::UnboundedSender<(Packet, SocketAddr)>,
    events_tx: mpsc::UnboundedSender<SocketEvent>,
    events_rx: mpsc::UnboundedReceiver<SocketEvent>,
    accepts: mpsc::UnboundedReceiver<oneshot::Sender<Incoming>>,
    connects: mpsc::UnboundedReceiver<(SocketAddr, oneshot::Sender<Pending>)>,
    connection_ids: ConnectionIdManager,
    connections: HashMap<ConnectionId, mpsc::UnboundedSender<Packet>>,
    incoming_connections: VecDeque<Incoming>,
}

impl Socket {
    pub fn new(
        incoming_packets: mpsc::UnboundedReceiver<(Packet, SocketAddr)>,
        outgoing_packets: mpsc::UnboundedSender<(Packet, SocketAddr)>,
        accepts: mpsc::UnboundedReceiver<oneshot::Sender<Incoming>>,
        connects: mpsc::UnboundedReceiver<(SocketAddr, oneshot::Sender<Pending>)>,
    ) -> Self {
        let (events_tx, events_rx) = mpsc::unbounded_channel();

        Self {
            incoming_packets,
            outgoing_packets,
            events_tx,
            events_rx,
            accepts,
            connects,
            connection_ids: ConnectionIdManager::new(),
            connections: HashMap::new(),
            incoming_connections: VecDeque::new(),
        }
    }

    pub async fn event_loop(&mut self) {
        loop {
            tokio::select! {
                Some(event) = self.events_rx.recv() => {
                    self.on_event(event, Instant::now());
                }
                Some((packet, source)) = self.incoming_packets.recv() => {
                    self.on_packet(packet, source, Instant::now());
                }
                Some(accept) = self.accepts.recv(), if !self.incoming_connections.is_empty() => {
                    let incoming = self.incoming_connections.pop_front().unwrap();
                    let _ = accept.send(incoming);
                }
                Some((addr, connect)) = self.connects.recv() => {
                    let pending = self.init_outgoing_conn(addr);
                    let _ = connect.send(pending);
                }
            }
        }
    }

    fn on_packet(&mut self, packet: Packet, src: SocketAddr, now: Instant) {
        let packet_conn_id = packet.conn_id();

        // The connection ID will differ based on whether the local socket initiated the
        // connection.
        let initiator_conn_id = ConnectionId {
            send: packet_conn_id.wrapping_add(1),
            recv: packet_conn_id,
            addr: src,
        };
        let acceptor_conn_id = match packet.packet_type() {
            PacketType::Syn => ConnectionId {
                send: packet_conn_id,
                recv: packet_conn_id.wrapping_add(1),
                addr: src,
            },
            _ => ConnectionId {
                send: packet_conn_id.wrapping_sub(1),
                recv: packet_conn_id,
                addr: src,
            },
        };

        let conn = self
            .connections
            .get(&initiator_conn_id)
            .or(self.connections.get(&acceptor_conn_id));

        // If a connection was found for either connection ID, then forward the packet to that
        // connection. Otherwise, if the packet is a SYN, then initiate a new connection.
        match conn {
            Some(conn) => {
                let _ = conn.send(packet);
            }
            None => {
                if std::matches!(packet.packet_type(), PacketType::Syn) {
                    // Initialize a new incoming connection.
                    let incoming = self.init_incoming_conn(packet, src, now);
                    self.incoming_connections.push_back(incoming);
                } else {
                    // TODO: Log.
                }
            }
        }
    }

    fn on_event(&mut self, event: SocketEvent, now: Instant) {
        match event {
            SocketEvent::Close { id, err } => {
                if let Some(err) = err {
                    // TODO: Propagate error.
                }
                self.connections.remove(&id);
                self.connection_ids.remove(&id);
            }
            SocketEvent::Transmit { packet, dest } => {
                let _ = self.outgoing_packets.send((packet, dest));
            }
        }
    }

    fn init_incoming_conn(&mut self, syn: Packet, peer: SocketAddr, now: Instant) -> Incoming {
        let conn_id = ConnectionId {
            send: syn.conn_id(),
            recv: syn.conn_id().wrapping_add(1),
            addr: peer,
        };

        let (packet_tx, packet_rx) = mpsc::unbounded_channel();
        let _ = packet_tx.send(syn);
        self.connections.insert(conn_id, packet_tx);

        let (stream_event_tx, _) = mpsc::unbounded_channel();

        Incoming::new(conn_id, packet_rx, self.events_tx.clone(), stream_event_tx)
    }

    fn init_outgoing_conn(&mut self, peer: SocketAddr) -> Pending {
        let conn_id = self.connection_ids.gen(peer);

        let (packet_tx, packet_rx) = mpsc::unbounded_channel();
        self.connections.insert(conn_id, packet_tx);

        let (stream_event_tx, _) = mpsc::unbounded_channel();

        Pending::new(conn_id, packet_rx, self.events_tx.clone(), stream_event_tx)
    }
}

const BUFFER_LEN: usize = u16::MAX as usize;

struct SocketRecv {
    socket: Arc<UdpSocket>,
    buf: [u8; BUFFER_LEN],
    packet_tx: mpsc::UnboundedSender<(Packet, SocketAddr)>,
}

impl SocketRecv {
    pub fn spawn(socket: Arc<UdpSocket>) -> mpsc::UnboundedReceiver<(Packet, SocketAddr)> {
        let (packet_tx, packet_rx) = mpsc::unbounded_channel();
        let buf = [0; BUFFER_LEN];
        let mut recv = Self {
            socket,
            buf,
            packet_tx,
        };

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Ok((n, src)) = recv.socket.recv_from(&mut recv.buf) => {
                        // If the packet is correctly decoded, then forward the packet to the
                        // socket.
                        if let Ok(packet) = Packet::decode(&recv.buf[..n]) {
                            if let Err(_err) = recv.packet_tx.send((packet, src)) {
                                // TODO: Trace error.
                            }
                        }
                    }
                }
            }
        });

        packet_rx
    }
}

struct SocketSend {
    socket: Arc<UdpSocket>,
    packet_rx: mpsc::UnboundedReceiver<(Packet, SocketAddr)>,
}

impl SocketSend {
    pub fn spawn(socket: Arc<UdpSocket>) -> mpsc::UnboundedSender<(Packet, SocketAddr)> {
        let (packet_tx, packet_rx) = mpsc::unbounded_channel();
        let mut send = Self { socket, packet_rx };

        tokio::spawn(async move {
            loop {
                tokio::select! {
                        Some((packet, dest)) = send.packet_rx.recv() => {
                            // Forward the packet to the destination.
                            if let Err(_err) = send.socket.send_to(packet.encode().as_slice(), dest).await {
                                // TODO: Trace error.
                            }
                        }
                }
            }
        });

        packet_tx
    }
}

pub struct UtpSocket {
    accepts: mpsc::UnboundedSender<oneshot::Sender<Incoming>>,
    connects: mpsc::UnboundedSender<(SocketAddr, oneshot::Sender<Pending>)>,
}

impl UtpSocket {
    pub fn new(socket: UdpSocket) -> Self {
        let udp_socket = Arc::new(socket);

        let incoming_packets = SocketRecv::spawn(Arc::clone(&udp_socket));
        let outgoing_packets = SocketSend::spawn(Arc::clone(&udp_socket));

        let (accept_tx, accept_rx) = mpsc::unbounded_channel();
        let (connect_tx, connect_rx) = mpsc::unbounded_channel();

        let mut socket = Socket::new(incoming_packets, outgoing_packets, accept_rx, connect_rx);
        tokio::spawn(async move { socket.event_loop().await });

        Self {
            accepts: accept_tx,
            connects: connect_tx,
        }
    }

    pub async fn connect(&self, addr: SocketAddr) -> io::Result<Connection> {
        let (conn_tx, conn_rx) = oneshot::channel();
        let _ = self.connects.send((addr, conn_tx));

        let conn = conn_rx.await.unwrap();
        conn.await
    }

    pub async fn accept(&self) -> io::Result<Connection> {
        let (conn_tx, conn_rx) = oneshot::channel();
        let _ = self.accepts.send(conn_tx);

        let conn = conn_rx.await.unwrap();
        conn.await
    }
}
