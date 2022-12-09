use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use delay_map::HashSetDelay;
use futures::StreamExt;
use tokio::sync::{mpsc, oneshot, Notify};

use crate::common::SocketEvent;
use crate::congestion;
use crate::packet::{Packet, PacketBuilder, PacketType, SelectiveAck};

use super::id::ConnectionId;

#[derive(Debug)]
pub enum StreamEvent {
    Connected,
    Closed { error: Option<io::Error> },
    Readable,
    Writable,
}

#[derive(Debug)]
pub struct Read {
    pub buf: usize,
    pub read: oneshot::Sender<io::Result<Vec<u8>>>,
}

pub struct PendingRead {
    pub buf: usize,
    pub read: oneshot::Sender<io::Result<Vec<u8>>>,
}

#[derive(Debug)]
pub struct Write {
    pub buf: Vec<u8>,
    pub written: oneshot::Sender<io::Result<usize>>,
}

struct PendingWrite {
    buf: Vec<u8>,
    offset: usize,
    written: oneshot::Sender<io::Result<usize>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Endpoint {
    Initiator,
    Acceptor,
}

#[derive(Clone, Debug)]
enum State {
    Init,
    SynSent { seq_num: u16 },
    SynReceived { seq_num: u16 },
    Connected,
    FinSent { fin_seq_num: u16 },
    FinReceived { fin_seq_num: u16 },
    Reset,
    Closed,
}

pub struct ConnectionStateMachine {
    state: State,
    endpoint: Endpoint,
    peer_addr: SocketAddr,
    conn_id_send: u16,
    conn_id_recv: u16,
    seq_num: u16,
    ts_diff_micros: u32,
    remote_window_size: Option<u32>,
    error: Option<io::Error>,
    packets: mpsc::UnboundedReceiver<Packet>,
    socket_events: mpsc::UnboundedSender<SocketEvent>,
    stream_events: mpsc::UnboundedSender<StreamEvent>,
    send_buf: Arc<Mutex<SendBuffer>>,
    sent_packets: Arc<Mutex<SentPacketManager>>,
    unacked_packets: HashSetDelay<u16>,
    outgoing: Arc<Notify>,
    writable: Arc<Notify>,
    pending_writes: Arc<Mutex<VecDeque<PendingWrite>>>,
    recv_buf: Arc<Mutex<ReceiveBuffer>>,
    readable: Arc<Notify>,
    pending_reads: Arc<Mutex<VecDeque<PendingRead>>>,
    connected: Option<oneshot::Sender<io::Result<()>>>,
    shutdown: oneshot::Receiver<()>,
}

impl ConnectionStateMachine {
    pub fn new(
        endpoint: Endpoint,
        id: ConnectionId,
        packets: mpsc::UnboundedReceiver<Packet>,
        socket_events: mpsc::UnboundedSender<SocketEvent>,
        stream_events: mpsc::UnboundedSender<StreamEvent>,
        connected: oneshot::Sender<io::Result<()>>,
        shutdown: oneshot::Receiver<()>,
    ) -> Self {
        let seq_num = rand::random();

        let send_buf = SendBuffer::new();
        let send_buf = Arc::new(Mutex::new(send_buf));
        let sent_packets = SentPacketManager::new(seq_num);
        let sent_packets = Arc::new(Mutex::new(sent_packets));
        let recv_buf = ReceiveBuffer::new();
        let recv_buf = Arc::new(Mutex::new(recv_buf));

        let unacked_packets = HashSetDelay::new(sent_packets.lock().unwrap().timeout());

        let pending_writes = VecDeque::new();
        let pending_writes = Arc::new(Mutex::new(pending_writes));

        let pending_reads = VecDeque::new();
        let pending_reads = Arc::new(Mutex::new(pending_reads));

        Self {
            state: State::Init,
            endpoint,
            peer_addr: id.addr,
            conn_id_send: id.send,
            conn_id_recv: id.recv,
            seq_num,
            ts_diff_micros: 0,
            remote_window_size: None,
            error: None,
            packets,
            socket_events,
            stream_events,
            send_buf,
            outgoing: Arc::new(Notify::new()),
            sent_packets,
            unacked_packets,
            writable: Arc::new(Notify::new()),
            pending_writes,
            recv_buf,
            readable: Arc::new(Notify::new()),
            pending_reads,
            connected: Some(connected),
            shutdown,
        }
    }

    pub fn init(&mut self, now: Instant) {
        // If the local node initiated the connection, then transmit the SYN.
        if self.is_initiator() {
            // The connection ID sent in the SYN packet is the receive connection ID.
            let syn = self
                .packet_builder(PacketType::Syn)
                .conn_id(self.conn_id_recv)
                .build();
            self.state = State::SynSent {
                seq_num: syn.seq_num(),
            };
            self.transmit_packet(syn, now);
        }
    }

    /// Shuts down (both halves of) the connection.
    pub fn shutdown(&mut self) {
        let now = Instant::now();
        self.close(now);
    }

    fn close(&mut self, now: Instant) {
        match self.state {
            State::Init
            | State::Closed
            | State::Reset
            | State::SynSent { .. }
            | State::FinSent { .. } => {
                // No message necessary.
            }
            State::SynReceived { .. } | State::Connected => {
                // Send a FIN to close the connection.
                //
                // TODO: Handle pending writes. Sequence number will be incorrect if there are
                // pending writes whose data has not been transmitted yet.
                let fin = self.packet_builder(PacketType::Fin).build();
                self.state = State::FinSent {
                    fin_seq_num: fin.seq_num(),
                };
                self.transmit_packet(fin, now);
            }
            State::FinReceived { .. } => {
                // TODO: Send our own FIN?
                // Send a STATE for the FIN. This may be a retransmission. This may contain a
                // selective ACK based on unreceived packets.
                let state = self.packet_builder(PacketType::State).build();
                self.transmit_packet(state, now);
            }
        }

        // TODO: If there are unacknowledged packets, then wait for those packets to be
        // acknowledged or timed out.

        // If the connection was not reset, then move the state to closed.
        if !std::matches!(self.state, State::Reset) {
            self.state = State::Closed;
            let _ = self.stream_events.send(StreamEvent::Closed { error: None });
        }
    }

    /// Resets the connection if not already closed or reset.
    fn reset(&mut self, err: io::Error, now: Instant) {
        match self.state {
            State::Closed | State::Reset => {}
            _ => self.state = State::Reset,
        }

        // If the connection was reset by the local peer, then we need to send a RESET packet.
        if !std::matches!(err.kind(), io::ErrorKind::ConnectionReset) {
            let reset = self.packet_builder(PacketType::Reset).build();
            self.transmit_packet(reset, now);
        }

        self.error = Some(io::Error::from(err.kind().clone()));
        let _ = self
            .stream_events
            .send(StreamEvent::Closed { error: Some(err) });
    }

    pub async fn event_loop(
        &mut self,
        mut reads: mpsc::UnboundedReceiver<Read>,
        mut writes: mpsc::UnboundedReceiver<Write>,
    ) {
        use std::io::Read;
        loop {
            tokio::select! {
                Some(read) = reads.recv() => {
                    // If there is a pending read, then queue this read.
                    let mut pending_reads = self.pending_reads.lock().unwrap();
                    if !pending_reads.is_empty() {
                        pending_reads.push_back(PendingRead { buf: read.buf, read: read.read, });
                        continue;
                    }

                    // If there is data in the receive buffer, and there are no pending reads, then
                    // read as much of the data as possible into the given buffer.
                    //
                    // If there is no data in the receive buffer, then add this read to the queue
                    // of blocked reads.
                    let mut recv_buf = self.recv_buf.lock().unwrap();
                    let mut buf = vec![0; read.buf];
                    match recv_buf.read(buf.as_mut_slice()) {
                        Ok(n) if n == 0 => {
                            // There is no readable data in the receive buffer, so queue this read.
                            pending_reads.push_back(PendingRead { buf: read.buf, read: read.read });
                        }
                        Ok(n) => {
                            let _ = read.read.send(Ok(buf[..n].to_vec()));
                        }
                        Err(err) => {
                            let _ = read.read.send(Err(err));
                        }
                    }
                }
                Some(write) = writes.recv() => {
                    // If there is a pending/blocked write, then add this write to the queue of
                    // blocked writes.
                    let mut pending_writes = self.pending_writes.lock().unwrap();
                    if !pending_writes.is_empty() {
                        pending_writes.push_back(PendingWrite { buf: write.buf, offset: 0, written: write.written });
                        continue;
                    }

                    let available = self.sent_packets.lock().unwrap().available_window();
                    if available == 0 {
                        pending_writes.push_back(PendingWrite { buf: write.buf, offset: 0, written: write.written });
                        continue;
                    }

                    // If there are no pending/blocked writes, and there is available space in the
                    // send window, then write as much of the data as possible. If the send window
                    // becomes full and there is remaining data to write, then add the write to the
                    // queue of blocked writes.
                    let mut send_buf = self.send_buf.lock().unwrap();
                    let n = std::cmp::min(available, write.buf.len());
                    let data = write.buf[..n].to_vec();
                    send_buf.write(data);
                    if n < write.buf.len() {
                        pending_writes.push_back(PendingWrite { buf: write.buf, offset: n, written: write.written });
                    } else {
                        let _ = write.written.send(Ok(n));
                    }

                    // Notify that some data was written to the send buffer.
                    self.outgoing.notify_one();
                }
                _ = Box::pin(self.writable.notified()) => {
                    // Check whether there is space available in the send window.
                    let available = self.sent_packets.lock().unwrap().available_window();
                    if available == 0 {
                        return;
                    }

                    // While there exists some pending write, write the data into the send buffer
                    // until all available space is consumed. Remove completed writes from the
                    // queue. If a write is only partially written, then move the offset to account
                    // for the data that was written.
                    let mut send_buf = self.send_buf.lock().unwrap();
                    let mut pending_writes = self.pending_writes.lock().unwrap();
                    let mut consumed = 0;
                    while let Some(pending) = pending_writes.front_mut() {
                        let n = std::cmp::min(available, pending.buf.len() - pending.offset);
                        let end = pending.offset + n;
                        send_buf.write(pending.buf[pending.offset..end].to_vec());

                        // If all the data for the pending write was written into the send buffer,
                        // then remove the pending write from the queue, and send the result to the
                        // writer. Otherwise, increment the offset of the pending write.
                        if end == pending.buf.len() {
                            let pending = pending_writes.pop_front().unwrap();
                            let _ = pending.written.send(Ok(end));
                        } else {
                            pending.offset += n;
                        }

                        // Account for consumed space in the send buffer. If all available space
                        // was consumed, then break out of the loop.
                        consumed += n;
                        if consumed == available {
                            break;
                        }
                    }

                    // If data was written to the send buffer, then notify.
                    if consumed > 0 {
                        self.outgoing.notify_one();
                    }
                }
                _ = Box::pin(self.readable.notified()) => {
                    // While there exists some pending read, read data from the receive buffer
                    // until the provided read buffer is full or until there is no data to read
                    // remaining in the receive buffer.
                    let mut pending_reads = self.pending_reads.lock().unwrap();
                    let mut recv_buf = self.recv_buf.lock().unwrap();
                    while let Some(pending) = pending_reads.front_mut() {
                        let mut buf = vec![0; pending.buf];
                        match recv_buf.read(&mut buf) {
                            Ok(n) if n == 0 => {
                                // If there was no data to read, then break out of the loop.
                                break;
                            }
                            Ok(n) => {
                                let pending = pending_reads.pop_front().unwrap();
                                let _ = pending.read.send(Ok(buf[..n].to_vec()));
                            }
                            Err(err) => {
                                let pending = pending_reads.pop_front().unwrap();
                                let _ = pending.read.send(Err(err));
                            }
                        }
                    }
                }
                _ = Box::pin(self.outgoing.notified()) => {
                    let sent_packets = self.sent_packets.lock().unwrap();
                    let available = sent_packets.available_window();
                    std::mem::drop(sent_packets);

                    let mut data = vec![0; available];
                    let n = self.send_buf.lock().unwrap().read(data.as_mut_slice());
                    if n == 0 {
                        continue;
                    }

                    // TODO: Package data into multiple packets as necessary based on maximum
                    // packet size. Should this be handled before data is written into the send
                    // buffer?
                    let packet = self.packet_builder(PacketType::Data).payload(data[..n].to_vec()).build();
                    self.transmit_packet(packet, Instant::now());
                }
                Some(packet) = self.packets.recv() => {
                    self.on_packet(packet, Instant::now());
                }
                Some(Ok(seq_num)) = self.unacked_packets.next() => {
                    self.on_timeout(seq_num, Instant::now());
                }
                _ = &mut self.shutdown => {
                    self.shutdown();

                    // Break out of the event loop.
                    break;
                }
            }
        }
    }

    pub fn on_packet(&mut self, packet: Packet, now: Instant) {
        // TODO: Validate connection ID. This should be handled by socket layer above, but we can
        // also check within the connection.
        //
        // TODO: Validate sequence number before further processing. There are some other
        // "short-circuit" opportunities as well.

        // Update local information based on the packet.
        self.remote_window_size = Some(packet.window_size());
        self.unacked_packets.remove(&packet.ack_num());

        let now_micros = now_micros();
        self.ts_diff_micros = if now_micros > packet.ts_micros() {
            now_micros - packet.ts_micros()
        } else {
            now_micros + (u32::MAX - packet.ts_micros())
        };

        let delay = Duration::from_micros(packet.ts_diff_micros().into());
        match packet.packet_type() {
            PacketType::Syn => self.on_syn(packet.seq_num(), now),
            PacketType::State => self.on_state(
                packet.seq_num(),
                packet.ack_num(),
                packet.selective_ack(),
                delay,
                now,
            ),
            PacketType::Data => self.on_data(packet.seq_num(), packet.payload().as_slice(), now),
            PacketType::Fin => {
                let payload = if packet.payload().is_empty() {
                    None
                } else {
                    Some(packet.payload().as_slice())
                };
                self.on_fin(packet.seq_num(), packet.ack_num(), payload, now);
            }
            PacketType::Reset => self.on_reset(now),
        }
    }

    fn on_syn(&mut self, seq_num: u16, now: Instant) {
        match self.state {
            State::Init => {
                if self.is_initiator() {
                    // Reset the connection. If we initiated the connection, then we should not
                    // receive a SYN.
                    let err = io::Error::from(io::ErrorKind::InvalidData);
                    self.reset(err, now);
                } else {
                    // The remote peer is the initiator. Mark that we received the SYN. Send an ACK
                    // to establish the connection. Account for SYN in receive buffer to establish
                    // ACK number.
                    let syn_ack = self
                        .packet_builder(PacketType::State)
                        .ack_num(seq_num)
                        .build();
                    self.state = State::SynReceived { seq_num };
                    self.transmit_packet(syn_ack, now);
                    self.recv_buf.lock().unwrap().on_packet(&[], seq_num);
                }
            }
            State::SynReceived {
                seq_num: syn_seq_num,
                ..
            } => {
                // If the sequence number differs from the previously received SYN, then reset the
                // connection. Otherwise, send an ACK.
                if syn_seq_num != seq_num {
                    let err = io::Error::from(io::ErrorKind::InvalidData);
                    self.reset(err, now);
                }
            }
            State::SynSent { .. }
            | State::Connected
            | State::FinSent { .. }
            | State::FinReceived { .. } => {
                // Reset the connection. We should not receive a SYN in any of these states.
                let err = io::Error::from(io::ErrorKind::InvalidData);
                self.reset(err, now);
            }
            State::Closed | State::Reset => {
                // Ignore the packet.
            }
        }
    }

    fn on_state(
        &mut self,
        seq_num: u16,
        ack_num: u16,
        selective_ack: Option<&SelectiveAck>,
        delay: Duration,
        now: Instant,
    ) {
        match self.state {
            State::Init | State::SynReceived { .. } => {
                // Reset the connection. We should not receive a STATE in any of these states.
                let err = io::Error::from(io::ErrorKind::InvalidData);
                self.reset(err, now);
                return;
            }
            State::SynSent {
                seq_num: syn_seq_num,
                ..
            } => {
                // If the ack number is equal to the sequence number of the SYN, then mark the
                // connection established.
                if ack_num == syn_seq_num {
                    self.state = State::Connected;
                    let _ = self.stream_events.send(StreamEvent::Connected);
                    if let Some(connected) = self.connected.take() {
                        let _ = connected.send(Ok(()));
                    }
                }
            }
            State::Connected => {}
            State::FinSent { fin_seq_num, .. } => {
                // If the ack number is equal to the sequence number of the FIN, then mark the
                // connection closed.
                if ack_num == fin_seq_num {
                    self.state = State::Closed;
                    let _ = self.stream_events.send(StreamEvent::Closed { error: None });
                }
            }
            State::FinReceived { fin_seq_num, .. } => {
                // If the sequence number is beyond the sequence number of the FIN, then reset the
                // connection. Otherwise, account for the acknowledgement.
                //
                // TODO: Handle wrap-around sequence number.
                if seq_num >= fin_seq_num {
                    let err = io::Error::from(io::ErrorKind::InvalidData);
                    self.reset(err, now);
                    return;
                }
            }
            State::Closed => {
                // TODO: If there are any outstanding unacknowledged packets, then account for the
                // acknowledgement.
            }
            State::Reset => {
                // Ignore the packet.
            }
        }

        // If there is a selective ack, then register with congestion control. Otherwise
        // register the singular ack.
        let mut sent_packets = self.sent_packets.lock().unwrap();
        if let Some(sack) = selective_ack {
            sent_packets.on_selective_ack(ack_num, sack, delay, now);
        } else {
            sent_packets.on_ack(ack_num, delay, now);
        }
        std::mem::drop(sent_packets);

        // TODO: Is there a better place to notify?
        self.writable.notify_one();
    }

    fn on_data(&mut self, seq_num: u16, payload: &[u8], now: Instant) {
        // If the payload is empty, then reset the connection. A DATA packet must contain a
        // non-empty payload.
        if payload.is_empty() {
            let err = io::Error::from(io::ErrorKind::InvalidData);
            self.reset(err, now);
        }

        match self.state {
            State::Init => {
                // Reset the connection. We should not receive DATA.
                let err = io::Error::from(io::ErrorKind::InvalidData);
                self.reset(err, now);
                return;
            }
            State::SynSent {
                seq_num: syn_seq_num,
                ..
            } => {
                // If the ack number corresponds to the sequence number of our SYN, then mark the
                // connection established.
                //
                // TODO: Handle out-of-order packet here.
                if seq_num == syn_seq_num {
                    self.state = State::Connected;
                    if let Some(connected) = self.connected.take() {
                        let _ = connected.send(Ok(()));
                    }
                }
            }
            State::SynReceived {
                seq_num: syn_seq_num,
                ..
            } => {
                // If the sequence number corresponds to the sequence number following the sequence
                // number of the SYN, then mark the connection established. Otherwise, ignore the
                // packet.
                //
                // TODO: Handle out-of-order packet here.
                if seq_num == syn_seq_num.wrapping_add(1) {
                    self.state = State::Connected;
                } else {
                    return;
                }
            }
            State::Connected | State::FinSent { .. } => {}
            State::FinReceived { fin_seq_num, .. } => {
                // If the sequence number is greater than or equal to the sequence number of the
                // FIN, then reset the connection.
                //
                // TODO: Handle wrap-around sequence number.
                if seq_num >= fin_seq_num {
                    let err = io::Error::from(io::ErrorKind::InvalidData);
                    self.reset(err, now);
                    return;
                }
            }
            State::Closed => {
                // Ignore the packet.
                //
                // TODO: If the connection was only closed in the write direction, then add the
                // data to the receive buffer.
                return;
            }
            State::Reset => {
                // Ignore the packet.
                return;
            }
        }

        // Incorporate the data into the receive buffer.
        self.recv_buf.lock().unwrap().on_packet(payload, seq_num);

        // Acknowledge data and notify any pending readers to check for readable data.
        let ack = self.packet_builder(PacketType::State).build();
        self.transmit_packet(ack, now);
        self.readable.notify_one();
    }

    fn on_fin(&mut self, seq_num: u16, ack_num: u16, payload: Option<&[u8]>, now: Instant) {
        match self.state {
            State::Init => {
                // Reset the connection.
                let err = io::Error::from(io::ErrorKind::InvalidData);
                self.reset(err, now);
                return;
            }
            State::SynSent {
                seq_num: syn_seq_num,
                ..
            } => {
                // If the FIN acknowledges the SYN, then mark the FIN received. Otherwise, reset
                // the connection.
                if ack_num == syn_seq_num {
                    self.state = State::FinReceived {
                        fin_seq_num: seq_num,
                    };
                } else {
                    let err = io::Error::from(io::ErrorKind::InvalidData);
                    self.reset(err, now);
                    return;
                }
            }
            State::SynReceived {
                seq_num: syn_seq_num,
                ..
            } => {
                // If the FIN sequence number is valid, then mark the FIN received. Otherwise,
                // reset the connection.
                //
                // TODO: Handle wrapping sequence numbers.
                if seq_num > syn_seq_num {
                    self.state = State::FinReceived {
                        fin_seq_num: seq_num,
                    };
                } else {
                    let err = io::Error::from(io::ErrorKind::InvalidData);
                    self.reset(err, now);
                    return;
                }
            }
            State::Connected => {
                self.state = State::FinReceived {
                    fin_seq_num: seq_num,
                };
            }
            State::FinSent { .. } => {
                self.state = State::Closed;
                let _ = self.stream_events.send(StreamEvent::Closed { error: None });
            }
            State::FinReceived { fin_seq_num, .. } => {
                // If the sequence number is not equal to the sequence number of the previously
                // received FIN, then reset the connection. Otherwise, ignore the duplicate FIN.
                if seq_num != fin_seq_num {
                    let err = io::Error::from(io::ErrorKind::InvalidData);
                    self.reset(err, now);
                    return;
                }
            }
            State::Closed => {
                // TODO: If the connection was only closed in the write direction, then add any
                // data to the receive buffer.
                return;
            }
            State::Reset => {
                return;
            }
        }

        // Incorporate the FIN into the receive buffer.
        //
        // TODO: Only incorporate the FIN if there is data. For now, we incorporate so that the
        // correct ack number is sent in the STATE packet.
        let payload = match payload {
            None => &[],
            Some(payload) => payload,
        };
        self.recv_buf.lock().unwrap().on_packet(payload, seq_num);

        // Acknowledge the FIN and notify any pending readers to check for readable data.
        let ack = self.packet_builder(PacketType::State).build();
        self.transmit_packet(ack, now);
        self.readable.notify_one();
    }

    fn on_reset(&mut self, now: Instant) {
        match self.state {
            State::Closed => {
                // TODO: If there is outstanding data that we are waiting to be acknowledged, then
                // stop waiting for those acknowledgements.
            }
            State::Reset => {
                // The connection has already been reset, so ignore the packet. The packet should
                // be ignored prior to any call to this method.
            }
            _ => {
                let err = io::Error::from(io::ErrorKind::ConnectionReset);
                self.reset(err, now);
            }
        }
    }

    fn on_timeout(&mut self, seq_num: u16, now: Instant) {
        match self.state {
            State::Init | State::SynReceived { .. } => {
                // If we are in the initial state, then we have neither sent the SYN nor received
                // the SYN. Thus, there is no packet that can timeout. If we have received the SYN
                // but are not connected, then the SYN-ACK has not been acknowledged. The SYN-ACK
                // is a STATE packet, so it cannot timeout.
                unreachable!();
            }
            State::SynSent { .. } | State::Connected | State::FinSent { .. } => {}
            State::FinReceived { .. } | State::Closed | State::Reset => {
                // If we have received the FIN, or the connection is closed or reset, then do not
                // retransmit the packet.
                return;
            }
        }

        let mut sent_packets = self.sent_packets.lock().unwrap();
        sent_packets.on_timeout(seq_num);

        // Reassemble the packet. Use the connection ID from the packet in case the packet is the
        // SYN, which contains the receive connection ID.
        let packet = sent_packets.get(seq_num).unwrap();
        let packet = self
            .packet_builder(packet.packet_type())
            .conn_id(packet.conn_id())
            .seq_num(packet.seq_num())
            .payload(packet.payload().to_vec())
            .build();

        // Drop the lock prior to `transmit_packet`.
        std::mem::drop(sent_packets);

        // Retransmit the packet.
        self.transmit_packet(packet, now);
    }

    fn transmit_packet(&mut self, packet: Packet, now: Instant) {
        // If the packet is not a STATE packet, then register the transmission because we expect an
        // ACK for the packet. Also, increment the sequence number.
        if packet.packet_type() != PacketType::State {
            let mut sent_packets = self.sent_packets.lock().unwrap();
            sent_packets.on_transmit(&packet, now);

            self.unacked_packets
                .insert_at(packet.seq_num(), sent_packets.timeout());
            self.seq_num = self.seq_num.wrapping_add(1);
        }

        let _ = self.socket_events.send(SocketEvent::Transmit {
            packet,
            dest: self.peer_addr,
        });
    }

    fn is_initiator(&self) -> bool {
        self.endpoint == Endpoint::Initiator
    }

    fn packet_builder(&self, packet_type: PacketType) -> PacketBuilder {
        let recv_buf = self.recv_buf.lock().unwrap();
        Packet::builder(packet_type)
            .conn_id(self.conn_id_send)
            .seq_num(self.seq_num)
            .ack_num(recv_buf.ack_num())
            .ts_micros(now_micros())
            .window_size(recv_buf.available())
            .ts_diff_micros(self.ts_diff_micros)
            .selective_ack(recv_buf.selective_ack())
    }
}

fn now_micros() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u32
}

pub struct SentPacket {
    original: Packet,
    transmission: Instant,
    retransmissions: Vec<Instant>,
    acks: Vec<Instant>,
}

pub struct SentPacketManager {
    init_seq_num: u16,
    congestion_ctrl: congestion::Controller,
    sent_packets: Vec<SentPacket>,
}

impl SentPacketManager {
    pub fn new(init_seq_num: u16) -> Self {
        Self {
            init_seq_num,
            congestion_ctrl: congestion::Controller::new(congestion::Config::default()),
            sent_packets: Vec::new(),
        }
    }

    fn index(&self, seq_num: u16) -> usize {
        usize::from(seq_num - self.init_seq_num)
    }

    pub fn on_transmit(&mut self, packet: &Packet, now: Instant) {
        // TODO: Handle wrapping sequence numbers.
        let index = self.index(packet.seq_num());

        // Assert that the packet is not a STATE packet.
        assert_ne!(packet.packet_type(), PacketType::State);

        // Assert that the transmitted packet corresponds to the next sequence number, or that the
        // packet is a retransmission.
        assert!(index <= self.sent_packets.len());

        let mut is_retransmission = false;
        if let Some(pkt) = self.sent_packets.get_mut(index) {
            pkt.retransmissions.push(now);
            is_retransmission = true;
        } else {
            let pkt = SentPacket {
                original: packet.clone(),
                transmission: now,
                retransmissions: Vec::new(),
                acks: Vec::new(),
            };
            self.sent_packets.push(pkt);
        }

        let transmit = if is_retransmission {
            congestion::Transmit::Retransmission
        } else {
            congestion::Transmit::Initial {
                bytes: packet.encoded_len() as u32,
            }
        };

        // TODO: error handling.
        self.congestion_ctrl
            .on_transmit(packet.seq_num(), transmit)
            .unwrap();
    }

    pub fn on_ack(&mut self, ack_num: u16, delay: Duration, now: Instant) {
        let index = self.index(ack_num);

        // Assert that the acknowledged packet corresponds to a sent packet.
        // TODO: Handle unrecognized `ack_num`.
        let packet = self.sent_packets.get_mut(index).unwrap();
        packet.acks.push(now);

        let last_transmitted_at = packet
            .retransmissions
            .last()
            .unwrap_or(&packet.transmission);
        let rtt = now.duration_since(*last_transmitted_at);
        let ack = congestion::Ack {
            delay,
            received_at: now,
            rtt,
        };
        self.congestion_ctrl.on_ack(ack_num, ack).unwrap();

        // TODO: Handle lost packet at `ack_num` + 1.
    }

    pub fn on_selective_ack(
        &mut self,
        ack_num: u16,
        selective_ack: &SelectiveAck,
        delay: Duration,
        now: Instant,
    ) {
        self.on_ack(ack_num, delay, now);

        // The initial sequence number is `ack_num` + 2. `ack_num` + 1 is assumed to have been
        // dropped or lost.
        let mut ack_num = ack_num + 2;

        for ack in selective_ack.acked() {
            // If we go beyond the sequence number of the latest sent packet, then break out of the
            // loop.
            if usize::from(ack_num - self.init_seq_num) > self.sent_packets.len() {
                break;
            }

            if ack {
                self.on_ack(ack_num, delay, now);
            }

            // Increment the ack (sequence) number.
            ack_num += 1;
        }

        // If there are any unacknowledged packets whose sequence numbers precede at least three
        // acknowledged packets, then mark those packets as lost.
        let mut acks = 0;
        let mut lost_packets = Vec::new();
        for pkt in self.sent_packets.iter().rev() {
            let acked = !pkt.acks.is_empty();
            if acks >= 3 {
                if !acked {
                    lost_packets.push(pkt.original.seq_num());
                }
            }
            acks += usize::from(acked);
        }
        for pkt in lost_packets {
            self.on_lost(pkt, true);
        }
    }

    pub fn get(&self, seq_num: u16) -> Option<&Packet> {
        let index = self.index(seq_num);
        self.sent_packets
            .get(index)
            .and_then(|sent| Some(&sent.original))
    }

    fn on_lost(&mut self, seq_num: u16, retransmitting: bool) {
        self.congestion_ctrl
            .on_lost_packet(seq_num, retransmitting)
            .unwrap();
    }

    pub fn timeout(&self) -> Duration {
        self.congestion_ctrl.timeout()
    }

    pub fn on_timeout(&mut self, _seq_num: u16) {
        self.congestion_ctrl.on_timeout();
    }

    pub fn available_window(&self) -> usize {
        self.congestion_ctrl.bytes_available_in_window() as usize
    }
}

#[derive(Debug)]
struct SendBuffer {
    pending: VecDeque<Vec<u8>>,
    offset: usize,
}

impl SendBuffer {
    pub fn new() -> Self {
        Self {
            pending: VecDeque::new(),
            offset: 0,
        }
    }

    pub fn write(&mut self, data: Vec<u8>) {
        self.pending.push_back(data);
    }

    pub fn read(&mut self, buf: &mut [u8]) -> usize {
        if let Some(data) = self.pending.front() {
            // The maximum amount of data that can be read into the buffer is the minimum of the
            // given max length and the amount of data remaining in the current data segment.
            let n = std::cmp::min(buf.len(), data.len() - self.offset);
            buf[..n].copy_from_slice(&data[self.offset..self.offset + n]);

            // If we are the end of the pending data segment, then reset the offset to zero, and
            // remove the data segment from the buffer.
            self.offset += n;
            if self.offset == data.len() {
                self.offset = 0;
                self.pending.pop_front();
            }

            return n;
        }

        return 0;
    }
}

#[derive(Debug)]
pub struct ReceiveBuffer {
    buf: Vec<u8>,
    packets: VecDeque<Option<Vec<u8>>>,
    start_seq_num: Option<u16>,
    consumed: u16,
}

impl ReceiveBuffer {
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            packets: VecDeque::new(),
            start_seq_num: None,
            consumed: 0,
        }
    }

    pub fn on_packet(&mut self, data: &[u8], seq_num: u16) {
        // If this is the first packet, then mark the starting sequence number and mark any data as
        // consumed.
        if self.start_seq_num.is_none() {
            self.start_seq_num = Some(seq_num);
            self.consumed = 1;
            self.buf.extend_from_slice(data);
            return;
        }
        let start_seq_num = self.start_seq_num.unwrap();

        // Data from packet already written into buffer.
        // TODO: Handle sequence number wrapping.
        if seq_num < start_seq_num + self.consumed {
            return;
        }

        // Compute offset for packet.
        let offset = usize::from(seq_num - (start_seq_num + self.consumed));

        // Push empty packets onto the packet deque until the deque could hold the data for the
        // packet.
        while self.packets.len() < offset + 1 {
            self.packets.push_back(None);
        }

        // If the deque does not contain data for the packet, then update the element for the
        // packet with `data`.
        if self.packets[offset].is_none() {
            self.packets[offset] = Some(data.to_vec());
        }

        // While there exists sequential packet data, extend the internal buffer and remove the
        // corresponding packet from the deque, updating the offset that marks the front of the
        // deque.
        while let Some(Some(data)) = self.packets.front() {
            self.buf.extend(data);
            self.packets.pop_front();
            self.consumed += 1;
        }
    }

    pub fn available(&self) -> u32 {
        // TODO: Use a fixed-size buffer.
        u32::MAX
    }

    pub fn ack_num(&self) -> u16 {
        match self.start_seq_num {
            Some(start) => start + self.consumed - 1,
            None => 0,
        }
    }

    pub fn selective_ack(&self) -> Option<SelectiveAck> {
        if self.packets.is_empty() {
            return None;
        }

        let acks: Vec<bool> = self.packets.iter().map(|pkt| pkt.is_some()).collect();
        let selective_ack = SelectiveAck::new(acks);
        Some(selective_ack)
    }
}

impl io::Read for ReceiveBuffer {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = std::cmp::min(buf.len(), self.buf.len());
        if n == 0 {
            return Ok(0);
        }

        buf[..n].copy_from_slice(&self.buf[..n]);

        // TODO: Move a cursor along a buffer.
        self.buf = self.buf[n..].to_vec();

        Ok(n)
    }
}
