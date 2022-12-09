use std::cmp;
use std::collections::{BinaryHeap, HashMap};
use std::time::{Duration, Instant};

const DEFAULT_TARGET_MICROS: u32 = 100_000;
const DEFAULT_INITIAL_TIMEOUT: Duration = Duration::from_secs(1);
const DEFAULT_MIN_TIMEOUT: Duration = Duration::from_millis(500);
const DEFAULT_MAX_PACKET_SIZE_BYTES: u32 = 65_507;
const DEFAULT_GAIN: f32 = 1.0;
const DEFAULT_DELAY_WINDOW: Duration = Duration::from_secs(120);

#[derive(Clone, Debug)]
struct Packet {
    size_bytes: u32,
    num_transmissions: u32,
    acked: bool,
}

#[derive(Clone, Debug)]
pub enum Transmit {
    Initial { bytes: u32 },
    Retransmission,
}

#[derive(Clone, Debug)]
pub struct Ack {
    pub delay: Duration,
    pub rtt: Duration,
    pub received_at: Instant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    InsufficientWindowSize,
    UnknownSeqNum,
    DuplicateAck,
    DuplicateTransmission,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub target_delay_micros: u32,
    pub initial_timeout: Duration,
    pub min_timeout: Duration,
    pub max_packet_size_bytes: u32,
    pub max_window_size_inc_bytes: u32,
    pub gain: f32,
    pub delay_window: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            target_delay_micros: DEFAULT_TARGET_MICROS,
            initial_timeout: DEFAULT_INITIAL_TIMEOUT,
            min_timeout: DEFAULT_MIN_TIMEOUT,
            max_packet_size_bytes: DEFAULT_MAX_PACKET_SIZE_BYTES,
            max_window_size_inc_bytes: DEFAULT_MAX_PACKET_SIZE_BYTES,
            gain: DEFAULT_GAIN,
            delay_window: DEFAULT_DELAY_WINDOW,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Controller {
    target_delay_micros: u32,
    timeout: Duration,
    min_timeout: Duration,
    window_size_bytes: u32,
    max_window_size_bytes: u32,
    min_window_size_bytes: u32,
    max_window_size_inc_bytes: u32,
    gain: f32,
    rtt: Duration,
    rtt_variance_micros: u64,
    latest_ack: Option<Instant>,
    transmissions: HashMap<u16, Packet>,
    delay_acc: DelayAccumulator,
}

impl Controller {
    /// Creates a `Controller` configured by `config`.
    pub fn new(config: Config) -> Self {
        Self {
            target_delay_micros: config.target_delay_micros,
            timeout: config.initial_timeout,
            min_timeout: config.min_timeout,
            window_size_bytes: 0,
            max_window_size_bytes: 2 * config.max_packet_size_bytes,
            min_window_size_bytes: 2 * config.max_packet_size_bytes,
            max_window_size_inc_bytes: config.max_window_size_inc_bytes,
            gain: config.gain,
            rtt: Duration::ZERO,
            rtt_variance_micros: 0,
            latest_ack: None,
            transmissions: HashMap::new(),
            delay_acc: DelayAccumulator::new(config.delay_window),
        }
    }

    /// Returns the congestion timeout.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Returns the number of bytes available in the congestion window.
    pub fn bytes_available_in_window(&self) -> u32 {
        self.max_window_size_bytes - self.window_size_bytes
    }

    /// Registers the transmission of a packet with the controller.
    pub fn on_transmit(&mut self, seq_num: u16, transmission: Transmit) -> Result<(), Error> {
        // If the transmission is an initial transmission, then record the transmission. If the
        // transmission is a retransmission, then increment the number of transmissions for that
        // record.
        match transmission {
            Transmit::Initial { bytes } => {
                if self.transmissions.contains_key(&seq_num) {
                    return Err(Error::DuplicateTransmission);
                }
                self.transmissions.insert(
                    seq_num,
                    Packet {
                        size_bytes: bytes,
                        num_transmissions: 1,
                        acked: false,
                    },
                );
            }
            Transmit::Retransmission => {
                let packet = self
                    .transmissions
                    .get_mut(&seq_num)
                    .ok_or(Error::UnknownSeqNum)?;
                packet.num_transmissions += 1;
            }
        };

        // The key-value pair should exist by logic above.
        let packet = self.transmissions.get(&seq_num).unwrap();

        // If this is the initial transmission of this packet, then increase the size of the
        // window. Return an error if there is not sufficient space.
        if packet.num_transmissions == 1 {
            if self.window_size_bytes + packet.size_bytes > self.max_window_size_bytes {
                return Err(Error::InsufficientWindowSize);
            }
            self.window_size_bytes += packet.size_bytes;
        }

        // If a timeout occurred, then register it.
        if self.timed_out_since_latest_ack() {
            self.on_timeout();
        }

        Ok(())
    }

    /// Registers a packet `Ack` with the controller.
    pub fn on_ack(&mut self, seq_num: u16, ack: Ack) -> Result<(), Error> {
        let packet = self
            .transmissions
            .get_mut(&seq_num)
            .ok_or(Error::UnknownSeqNum)?;

        // Mark the packet acknowledged. To acknowledge a previously acknowledged packet is an
        // error.
        // TODO: Handle multiple acknowledgements.
        if packet.acked {
            return Err(Error::DuplicateAck);
        }
        packet.acked = true;

        let packet = packet.clone();

        // Add the delay to the accumulator.
        self.delay_acc.push(ack.delay, ack.received_at);

        // Adjust the maximum congestion window based on the delay of the acknowledged packet.
        // Bound the base delay (in microseconds) by `u32::MAX`. Bound the delay (in microseconds)
        // of the acknowledged packet by `u32::MAX`.
        let base_delay_micros = self
            .delay_acc
            .base_delay()
            .unwrap_or(Duration::ZERO)
            .as_micros();
        let base_delay_micros = u32::try_from(base_delay_micros).unwrap_or(u32::MAX);
        let packet_delay_micros = u32::try_from(ack.delay.as_micros()).unwrap_or(u32::MAX);
        let max_window_size_adjustment = compute_max_window_size_adjustment(
            self.target_delay_micros,
            base_delay_micros,
            packet_delay_micros,
            self.window_size_bytes,
            packet.size_bytes,
            self.max_window_size_inc_bytes,
            self.gain,
        );
        self.apply_max_window_size_adjustment(max_window_size_adjustment);

        // Adjust the current window size to account for the acknowledged packet.
        //
        // An overflow panic occurs if the window size is less than the packet size. This would
        // correspond to an invalid operation as the window size should account for the size of the
        // packet being acknowledged.
        self.window_size_bytes -= packet.size_bytes;

        // Record whether a timeout occurred before a possible adjustment to the timeout and before
        // the update to the timestamp of the latest acknowledgement.
        let timed_out = self.timed_out_since_latest_ack();

        // Update latest acknowledgement timestamp.
        self.latest_ack = Some(ack.received_at);

        // Only adjust the round trip time (RTT) estimation if the acknowledgement corresponds to
        // the first transmission. The congestion timeout is also adjusted each time the RTT
        // estimation is adjusted.
        if packet.num_transmissions == 1 {
            // Adjust round trip time variance.
            let rtt_var_adjustment = compute_rtt_variance_adjustment(
                self.rtt.as_micros(),
                self.rtt_variance_micros,
                ack.rtt.as_micros(),
            );
            if rtt_var_adjustment.is_negative() {
                self.rtt_variance_micros = self
                    .rtt_variance_micros
                    .saturating_sub(rtt_var_adjustment.unsigned_abs());
            } else {
                self.rtt_variance_micros = self
                    .rtt_variance_micros
                    .saturating_add(rtt_var_adjustment as u64);
            }

            // Adjust round trip time.
            let rtt_adjustment = compute_rtt_adjustment(self.rtt.as_micros(), ack.rtt.as_micros());
            if rtt_adjustment.is_negative() {
                self.rtt = self
                    .rtt
                    .saturating_sub(Duration::from_micros(rtt_adjustment.unsigned_abs()));
            } else {
                self.rtt = self
                    .rtt
                    .saturating_add(Duration::from_micros(rtt_adjustment as u64));
            }

            // Adjust congestion timeout.
            self.apply_timeout_adjustment();
        }

        // If a timeout occurred, then register it.
        if timed_out {
            self.on_timeout();
        }

        Ok(())
    }

    /// Registers a lost packet with the controller.
    pub fn on_lost_packet(&mut self, seq_num: u16, retransmitting: bool) -> Result<(), Error> {
        let packet = self
            .transmissions
            .get(&seq_num)
            .ok_or(Error::UnknownSeqNum)?;

        self.max_window_size_bytes =
            cmp::max(self.max_window_size_bytes / 2, self.min_window_size_bytes);

        // If the packet is not to be retransmitted, then account for those lost bytes in the
        // congestion window.
        if !retransmitting {
            self.window_size_bytes -= packet.size_bytes;
        }

        Ok(())
    }

    /// Registers a timeout with the controller.
    pub fn on_timeout(&mut self) {
        self.max_window_size_bytes = self.min_window_size_bytes;
        self.timeout *= 2;
    }

    /// Returns whether a timeout has occurred based on the latest acknowledgement.
    fn timed_out_since_latest_ack(&self) -> bool {
        // If the duration since the prior latest acknowledgement is greater than or equal to the
        // congestion timeout, then register a timeout.
        if let Some(latest_ack) = self.latest_ack {
            if Instant::now().duration_since(latest_ack) >= self.timeout {
                return true;
            }
        }

        false
    }

    /// Adjusts the maximum window (i.e. congestion window) by `adjustment`, keeping the size of
    /// the window within the configured interval, and not allowing it to grow by more than the
    /// configured maximum increment.
    fn apply_max_window_size_adjustment(&mut self, adjustment: i64) {
        // Apply the adjustment.
        let max_window_size_bytes = i64::from(self.max_window_size_bytes) + adjustment;

        // The maximum congestion window must be non-negative.
        let max_window_size_bytes = cmp::max(max_window_size_bytes, 0) as u32;

        // The maximum congestion window cannot increase by more than the configured maximum
        // increment.
        self.max_window_size_bytes = cmp::min(
            max_window_size_bytes,
            self.max_window_size_bytes + self.max_window_size_inc_bytes,
        );
    }

    /// Adjusts the congestion timeout based on the current round trip time (RTT) estimate and the
    /// current RTT variance.
    ///
    /// The congestion timeout cannot fall below the configured minimum.
    fn apply_timeout_adjustment(&mut self) {
        self.timeout = cmp::max(
            self.rtt + Duration::from_micros(self.rtt_variance_micros * 4),
            self.min_timeout,
        );
    }
}

/// Returns the adjustment in bytes to the maximum window (i.e. congestion window) size based on
/// the delta between the packet delay and the target delay and on the portion of the total
/// in-flight bytes that the packet corresponds to.
fn compute_max_window_size_adjustment(
    target_delay_micros: u32,
    base_delay_micros: u32,
    packet_delay_micros: u32,
    window_size_bytes: u32,
    packet_size_bytes: u32,
    max_window_size_inc_bytes: u32,
    gain: f32,
) -> i64 {
    // Adjust the delay based on the base delay.
    //
    // The `i64::try_from` should not fail because `packet_delay_micros` is a `u32`.
    //
    // The base delay adjustment should not panic because the base delay is non-negative and
    // should not be larger than the current delay.
    let delay_micros = i64::try_from(packet_delay_micros - base_delay_micros).unwrap();

    let off_target_micros = i64::from(target_delay_micros) - delay_micros;
    let delay_factor = (off_target_micros as f64) / f64::from(target_delay_micros);
    let window_factor = f64::from(packet_size_bytes) / f64::from(window_size_bytes);

    let scaled_gain =
        f64::from(gain) * f64::from(max_window_size_inc_bytes) * delay_factor * window_factor;

    scaled_gain as i64
}

/// Returns the adjustment to the round trip time (RTT) estimate in microseconds based on the
/// packet RTT and the current RTT estimate.
fn compute_rtt_adjustment(rtt_micros: u128, packet_rtt_micros: u128) -> i64 {
    ((packet_rtt_micros as f64 - rtt_micros as f64) / 8.0) as i64
}

/// Returns the adjustment to round trip time (RTT) variance in microseconds based on the packet
/// RTT, current RTT estimate, and current RTT variance.
fn compute_rtt_variance_adjustment(
    rtt_micros: u128,
    rtt_variance_micros: u64,
    packet_rtt_micros: u128,
) -> i64 {
    let abs_delta_micros = rtt_micros.abs_diff(packet_rtt_micros);

    (((abs_delta_micros as f64) - (rtt_variance_micros as f64)) / 4.0) as i64
}

#[derive(Clone, Debug, Eq)]
struct Delay {
    value: Duration,
    deadline: Instant,
}

impl PartialEq for Delay {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl PartialOrd for Delay {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Delay {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.value.cmp(&other.value)
    }
}

#[derive(Clone, Debug)]
struct DelayAccumulator {
    delays: BinaryHeap<cmp::Reverse<Delay>>,
    window: Duration,
}

impl DelayAccumulator {
    /// Creates a `DelayAccumulator` with a sliding window of length `window`.
    pub fn new(window: Duration) -> Self {
        Self {
            delays: BinaryHeap::new(),
            window,
        }
    }

    // TODO: Handle `received_at` from the future (i.e. beyond `Instant::now`).
    /// Pushes `delay` onto the accumulator set to remain in the sliding window based on
    /// `received_at`.
    pub fn push(&mut self, delay: Duration, received_at: Instant) {
        let delay = Delay {
            value: delay,
            deadline: received_at + self.window,
        };
        self.delays.push(cmp::Reverse(delay));
    }

    // TODO: Here, delay measurements that fall outside of the window are deleted lazily. Evaluate
    // a non-lazy alternative. The number of elements in the accumulator should not be too large.
    // The lazy solution also means that `base_delay` requires `&mut self`, which is not
    // preferable.
    /// Returns a baseline delay measure given the delay measurements present in the accumulator.
    /// Returns `None` if there are no delay measurements within the current sliding window.
    pub fn base_delay(&mut self) -> Option<Duration> {
        while let Some(min) = self.delays.peek() {
            // If the deadline of the delay has been reached, then remove the delay and continue to
            // the next iteration.
            if Instant::now() >= min.0.deadline {
                self.delays.pop();
                continue;
            }

            return Some(min.0.value);
        }

        // No delay exists within the window.
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod controller {
        use super::*;

        #[test]
        fn on_transmit() {
            let mut ctrl = Controller::new(Config::default());

            let initial_timeout = ctrl.timeout();

            // Register the initial transmission of a packet with sequence number 1.
            let mut seq_num = 1;
            let packet_one_size_bytes = 32;
            let transmission = Transmit::Initial {
                bytes: packet_one_size_bytes,
            };
            ctrl.on_transmit(seq_num, transmission)
                .expect("transmission registration failed");

            let transmission_record = ctrl
                .transmissions
                .get(&seq_num)
                .expect("transmission not recorded");
            assert_eq!(transmission_record.size_bytes, packet_one_size_bytes);
            assert_eq!(transmission_record.num_transmissions, 1);

            assert_eq!(ctrl.window_size_bytes, packet_one_size_bytes);

            // Register the initial transmission of a packet with sequence number 2.
            seq_num = 2;
            let packet_two_size_bytes = 128;
            let transmission = Transmit::Initial {
                bytes: packet_two_size_bytes,
            };
            ctrl.on_transmit(seq_num, transmission)
                .expect("transmission registration failed");

            let transmission_record = ctrl
                .transmissions
                .get(&seq_num)
                .expect("transmission not recorded");
            assert_eq!(transmission_record.size_bytes, packet_two_size_bytes);
            assert_eq!(transmission_record.num_transmissions, 1);
            assert_eq!(
                ctrl.window_size_bytes,
                packet_one_size_bytes + packet_two_size_bytes,
            );

            // Register the retransmission of the packet with sequence number 2.
            ctrl.on_transmit(seq_num, Transmit::Retransmission)
                .expect("transmission registration failed");

            let transmission_record = ctrl
                .transmissions
                .get(&seq_num)
                .expect("transmission not recorded");
            assert_eq!(transmission_record.size_bytes, packet_two_size_bytes);
            assert_eq!(transmission_record.num_transmissions, 2);
            assert_eq!(
                ctrl.window_size_bytes,
                packet_one_size_bytes + packet_two_size_bytes,
            );

            assert_eq!(ctrl.timeout(), initial_timeout);
        }

        #[test]
        fn on_transmit_duplicate_transmission() {
            let mut ctrl = Controller::new(Config::default());

            // Register the initial transmission of a packet with sequence number 1.
            let seq_num = 1;
            let bytes = 32;
            let transmission = Transmit::Initial { bytes };
            ctrl.on_transmit(seq_num, transmission)
                .expect("transmission registration failed");

            assert_eq!(ctrl.window_size_bytes, bytes);

            // Register the initial transmission of the SAME packet.
            let transmission = Transmit::Initial { bytes };
            let result = ctrl.on_transmit(seq_num, transmission);
            assert_eq!(result, Err(Error::DuplicateTransmission));

            assert_eq!(ctrl.window_size_bytes, bytes);
        }

        #[test]
        fn on_transmit_unknown_seq_num() {
            let mut ctrl = Controller::new(Config::default());

            // Register the retransmission of the packet with sequence number 1.
            let seq_num = 1;
            let result = ctrl.on_transmit(seq_num, Transmit::Retransmission);
            assert_eq!(result, Err(Error::UnknownSeqNum));

            assert_eq!(ctrl.window_size_bytes, 0);
        }

        #[test]
        fn on_transmit_insufficient_window_size() {
            let mut ctrl = Controller::new(Config::default());

            // Register the transmission of a packet with sequence number 1 whose size EXCEEDS the
            // maximum window size.
            let seq_num = 1;
            let bytes = ctrl.max_window_size_bytes + 1;
            let result = ctrl.on_transmit(seq_num, Transmit::Initial { bytes });
            assert_eq!(result, Err(Error::InsufficientWindowSize));

            assert_eq!(ctrl.window_size_bytes, 0);
        }

        #[test]
        fn on_transmit_after_timeout() {
            let mut ctrl = Controller::new(Config::default());

            // Set the latest acknowledgement timestamp such that the transmission registration
            // occurs AFTER the timeout.
            ctrl.latest_ack = Some(Instant::now() - ctrl.timeout());

            // Register the initial transmission of a packet with sequence number 1.
            let seq_num = 1;
            let bytes = 32;
            let transmission = Transmit::Initial { bytes };
            ctrl.on_transmit(seq_num, transmission)
                .expect("transmission registration failed");

            // Create a separate controller in the same initial state, where we can explicitly
            // register a timeout.
            let mut ctrl_timeout = Controller::new(Config::default());
            ctrl_timeout.on_timeout();

            assert_eq!(ctrl.timeout(), ctrl_timeout.timeout());
        }

        #[test]
        fn on_ack() {
            let mut ctrl = Controller::new(Config::default());

            // Register the initial transmission of a packet with sequence number 1.
            let seq_num = 1;
            let bytes = 32;
            let transmission = Transmit::Initial { bytes };
            ctrl.on_transmit(seq_num, transmission)
                .expect("transmission registration failed");

            // Register the acknowledgement for the packet with sequence number 1.
            let ack_delay = Duration::from_millis(150);
            let ack_rtt = Duration::from_millis(300);
            let ack_received_at = Instant::now();
            let ack = Ack {
                delay: ack_delay,
                rtt: ack_rtt,
                received_at: ack_received_at,
            };
            ctrl.on_ack(seq_num, ack).expect("ack registration failed");

            assert_eq!(
                ctrl.delay_acc
                    .base_delay()
                    .expect("delay not pushed into accumulator"),
                ack_delay,
            );

            // TODO: max window

            assert_eq!(ctrl.window_size_bytes, 0);

            assert_eq!(
                ctrl.latest_ack.expect("ack not recorded as latest ack"),
                ack_received_at,
            );

            // TODO: RTT variance

            // TODO: RTT

            assert!(ctrl.timeout() >= ctrl.min_timeout);
        }

        #[test]
        fn on_ack_unknown_seq_num() {
            let mut ctrl = Controller::new(Config::default());

            // Register the acknowledgement for the packet with sequence number 1.
            let seq_num = 1;
            let ack_delay = Duration::from_millis(150);
            let ack_rtt = Duration::from_millis(300);
            let ack_received_at = Instant::now();
            let ack = Ack {
                delay: ack_delay,
                rtt: ack_rtt,
                received_at: ack_received_at,
            };
            let result = ctrl.on_ack(seq_num, ack);
            assert_eq!(result, Err(Error::UnknownSeqNum));
        }

        #[test]
        fn on_ack_duplicate_ack() {
            let mut ctrl = Controller::new(Config::default());

            // Register the initial transmission of a packet with sequence number 1.
            let seq_num = 1;
            let bytes = 32;
            let transmission = Transmit::Initial { bytes };
            ctrl.on_transmit(seq_num, transmission)
                .expect("transmission registration failed");

            // Register the acknowledgement for the packet with sequence number 1.
            let ack_delay = Duration::from_millis(150);
            let ack_rtt = Duration::from_millis(300);
            let ack_received_at = Instant::now();
            let ack = Ack {
                delay: ack_delay,
                rtt: ack_rtt,
                received_at: ack_received_at,
            };
            ctrl.on_ack(seq_num, ack.clone())
                .expect("ack registration failed");

            // Register the acknowledgement for the packet with sequence number 1 AGAIN.
            let result = ctrl.on_ack(seq_num, ack);
            assert_eq!(result, Err(Error::DuplicateAck));
        }

        #[test]
        fn on_lost_packet_retransmitting() {
            let mut ctrl = Controller::new(Config::default());

            let initial_max_window_size_bytes = ctrl.min_window_size_bytes * 10;
            ctrl.max_window_size_bytes = initial_max_window_size_bytes;

            // Register the initial transmission of a packet with sequence number 1.
            let seq_num = 1;
            let bytes = 32;
            let transmission = Transmit::Initial { bytes };
            ctrl.on_transmit(seq_num, transmission)
                .expect("transmission registration failed");

            assert_eq!(ctrl.window_size_bytes, bytes);

            // Register the loss of the packet with sequence number 1. Specify that we WILL attempt
            // to retransmit.
            ctrl.on_lost_packet(seq_num, true)
                .expect("lost packet registration failed");
            assert_eq!(ctrl.window_size_bytes, bytes);
            assert!(ctrl.max_window_size_bytes >= ctrl.min_window_size_bytes);
            assert_eq!(
                ctrl.max_window_size_bytes,
                initial_max_window_size_bytes / 2,
            );
        }

        #[test]
        fn on_lost_packet_not_retransmitting() {
            let mut ctrl = Controller::new(Config::default());

            let initial_max_window_size_bytes = ctrl.min_window_size_bytes * 10;
            ctrl.max_window_size_bytes = initial_max_window_size_bytes;

            // Register the initial transmission of a packet with sequence number 1.
            let seq_num = 1;
            let bytes = 32;
            let transmission = Transmit::Initial { bytes };
            ctrl.on_transmit(seq_num, transmission)
                .expect("transmission registration failed");

            assert_eq!(ctrl.window_size_bytes, bytes);

            // Register the loss of the packet with sequence number 1. Specify that we WILL NOT
            // attempt to retransmit.
            ctrl.on_lost_packet(seq_num, false)
                .expect("lost packet registration failed");
            assert_eq!(ctrl.window_size_bytes, 0);
            assert!(ctrl.max_window_size_bytes >= ctrl.min_window_size_bytes);
            assert_eq!(
                ctrl.max_window_size_bytes,
                initial_max_window_size_bytes / 2,
            );
        }

        #[test]
        fn on_lost_packet_unknown_seq_num() {
            let mut ctrl = Controller::new(Config::default());

            let initial_max_window_size_bytes = ctrl.min_window_size_bytes * 10;
            ctrl.max_window_size_bytes = initial_max_window_size_bytes;

            // Register the loss of the packet with sequence number 1.
            let seq_num = 1;
            let result = ctrl.on_lost_packet(seq_num, false);
            assert_eq!(result, Err(Error::UnknownSeqNum));
            assert_eq!(ctrl.window_size_bytes, 0);
            assert_eq!(ctrl.max_window_size_bytes, initial_max_window_size_bytes);
        }

        #[test]
        fn on_timeout() {
            let mut ctrl = Controller::new(Config::default());

            let initial_max_window_size_bytes = ctrl.min_window_size_bytes * 10;
            ctrl.max_window_size_bytes = initial_max_window_size_bytes;

            let initial_timeout = ctrl.timeout();

            // Register a timeout.
            ctrl.on_timeout();
            assert_eq!(ctrl.max_window_size_bytes, ctrl.min_window_size_bytes);
            assert_eq!(ctrl.timeout, initial_timeout * 2);
        }
    }

    mod delay_accumulator {
        use super::*;

        #[test]
        fn push() {
            let window = Duration::from_millis(100);
            let mut acc = DelayAccumulator::new(window);

            let delay = Duration::from_millis(50);
            let delay_received_at = Instant::now();
            acc.push(delay, delay_received_at);

            let item = acc
                .delays
                .peek()
                .expect("delay not pushed onto accumulator");
            assert_eq!(item.0.value, delay);
            assert_eq!(item.0.deadline, delay_received_at + window);
        }

        #[test]
        fn base_delay() {
            let window = Duration::from_millis(100);
            let mut acc = DelayAccumulator::new(window);

            let delay_small = Duration::from_millis(50);
            let delay_small_received_at = Instant::now();
            acc.push(delay_small, delay_small_received_at);

            let delay_smaller = Duration::from_millis(25);
            let delay_smaller_received_at = Instant::now();
            acc.push(delay_smaller, delay_smaller_received_at);

            let delay_smallest = Duration::from_millis(5);
            let delay_smallest_received_at = Instant::now();
            acc.push(delay_smallest, delay_smallest_received_at);

            let delay_expired = Duration::from_millis(1);
            let delay_expired_received_at = Instant::now() - window;
            acc.push(delay_expired, delay_expired_received_at);

            // Check that all delays are present within the accumulator.
            assert_eq!(acc.delays.len(), 4);

            let base_delay = acc
                .base_delay()
                .expect("base delay not present in accumulator");
            assert_eq!(base_delay, delay_smallest);

            // Check that the expired delay was popped.
            assert_eq!(acc.delays.len(), 3);
        }

        #[test]
        fn base_delay_empty() {
            let window = Duration::from_millis(100);
            let mut acc = DelayAccumulator::new(window);

            let base_delay = acc.base_delay();
            assert!(base_delay.is_none());
        }
    }
}
