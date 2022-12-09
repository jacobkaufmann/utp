use std::io;
use std::net::SocketAddr;

use crate::conn::ConnectionId;
use crate::packet::Packet;

#[derive(Debug)]
pub enum SocketEvent {
    Close {
        id: ConnectionId,
        err: Option<io::Error>,
    },
    Transmit {
        packet: Packet,
        dest: SocketAddr,
    },
}
