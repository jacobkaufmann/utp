use std::collections::HashSet;
use std::net::SocketAddr;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct ConnectionId {
    pub addr: SocketAddr,
    pub send: u16,
    pub recv: u16,
}

pub struct ConnectionIdManager {
    outstanding: HashSet<ConnectionId>,
}

impl ConnectionIdManager {
    pub fn new() -> Self {
        Self {
            outstanding: HashSet::new(),
        }
    }

    /// Generates a non-outstanding `ConnectionId`.
    pub fn gen(&mut self, addr: SocketAddr) -> ConnectionId {
        let id = loop {
            let recv: u16 = rand::random();
            let send = recv.wrapping_add(1);
            let id = ConnectionId { addr, send, recv };

            if !self.outstanding.contains(&id) {
                self.outstanding.insert(id);
                break id;
            }
        };

        id
    }

    /// Removes an outstanding `ConnectionId`.
    pub fn remove(&mut self, id: &ConnectionId) -> bool {
        self.outstanding.remove(id)
    }
}
