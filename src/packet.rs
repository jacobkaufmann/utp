use std::fmt;

/// Size of an encoded uTP header in bytes.
const PACKET_HEADER_LEN: usize = 20;

/// Size of a Selective ACK segment in bits.
const SELECTIVE_ACK_BITS: usize = 32;

/// Size of an extension identifier in bytes.
const EXTENSION_TYPE_LEN: usize = 1;

/// Size of an extension length specifier in bytes.
const EXTENSION_LEN_LEN: usize = 1;

#[derive(Copy, Clone, Debug)]
pub struct InvalidPacketType;

impl fmt::Display for InvalidPacketType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid uTP packet type")
    }
}

#[derive(Copy, Clone, Debug)]
pub struct InvalidVersion;

impl fmt::Display for InvalidVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid uTP version")
    }
}

#[derive(Copy, Clone, Debug)]
pub enum SelectiveAckError {
    InsufficientLen,
    InvalidLen,
}

impl From<SelectiveAckError> for PacketError {
    fn from(value: SelectiveAckError) -> Self {
        Self::InvalidExtension(ExtensionError::from(value))
    }
}

impl fmt::Display for SelectiveAckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::InsufficientLen => "selective ACK len must be at least 32 bits",
            Self::InvalidLen => "selective ACK len must be a multiple of 32 bits",
        };

        write!(f, "{}", s)
    }
}

#[derive(Clone, Debug)]
pub enum ExtensionError {
    InsufficientLen,
    InvalidSelectiveAck(SelectiveAckError),
}

impl From<SelectiveAckError> for ExtensionError {
    fn from(value: SelectiveAckError) -> Self {
        Self::InvalidSelectiveAck(value)
    }
}

impl fmt::Display for ExtensionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s: String = match self {
            Self::InsufficientLen => String::from("insufficient extension len"),
            Self::InvalidSelectiveAck(err) => err.to_string(),
        };

        write!(f, "{}", s)
    }
}

#[derive(Clone, Debug)]
pub enum PacketHeaderError {
    InvalidPacketType(InvalidPacketType),
    InvalidVersion(InvalidVersion),
    InvalidExtension(ExtensionError),
    InvalidLen,
}

impl From<InvalidPacketType> for PacketHeaderError {
    fn from(value: InvalidPacketType) -> Self {
        Self::InvalidPacketType(value)
    }
}

impl From<InvalidVersion> for PacketHeaderError {
    fn from(value: InvalidVersion) -> Self {
        Self::InvalidVersion(value)
    }
}

impl From<ExtensionError> for PacketHeaderError {
    fn from(value: ExtensionError) -> Self {
        Self::InvalidExtension(value)
    }
}

#[derive(Clone, Debug)]
pub enum PacketError {
    InvalidHeader(PacketHeaderError),
    InvalidExtension(ExtensionError),
    InvalidLen,
    EmptyDataPayload,
}

impl From<PacketHeaderError> for PacketError {
    fn from(value: PacketHeaderError) -> Self {
        Self::InvalidHeader(value)
    }
}

impl From<ExtensionError> for PacketError {
    fn from(value: ExtensionError) -> Self {
        Self::InvalidExtension(value)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PacketType {
    Data,
    Fin,
    State,
    Reset,
    Syn,
}

impl TryFrom<u8> for PacketType {
    type Error = InvalidPacketType;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Data),
            1 => Ok(Self::Fin),
            2 => Ok(Self::State),
            3 => Ok(Self::Reset),
            4 => Ok(Self::Syn),
            _ => Err(InvalidPacketType),
        }
    }
}

impl From<PacketType> for u8 {
    fn from(value: PacketType) -> u8 {
        match value {
            PacketType::Data => 0,
            PacketType::Fin => 1,
            PacketType::State => 2,
            PacketType::Reset => 3,
            PacketType::Syn => 4,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Version {
    One,
}

impl TryFrom<u8> for Version {
    type Error = InvalidVersion;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::One),
            _ => Err(InvalidVersion),
        }
    }
}

impl From<Version> for u8 {
    fn from(value: Version) -> u8 {
        match value {
            Version::One => 1,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Extension {
    None,
    SelectiveAck,
    Unknown(u8),
}

impl From<u8> for Extension {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::None,
            1 => Self::SelectiveAck,
            unknown => Self::Unknown(unknown),
        }
    }
}

impl From<Extension> for u8 {
    fn from(value: Extension) -> u8 {
        match value {
            Extension::None => 0,
            Extension::SelectiveAck => 1,
            Extension::Unknown(ext) => ext,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PacketHeader {
    packet_type: PacketType,
    version: Version,
    extension: Extension,
    conn_id: u16,
    ts_micros: u32,
    ts_diff_micros: u32,
    window_size: u32,
    seq_num: u16,
    ack_num: u16,
}

impl PacketHeader {
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = vec![];

        let packet_type = Into::<u8>::into(self.packet_type).to_be_bytes()[0];
        let version = Into::<u8>::into(self.version).to_be_bytes()[0];
        let type_version = (packet_type << 4) | version;
        bytes.push(type_version);

        let extension = u8::from(self.extension);
        bytes.push(extension);

        bytes.extend(self.conn_id.to_be_bytes());
        bytes.extend(self.ts_micros.to_be_bytes());
        bytes.extend(self.ts_diff_micros.to_be_bytes());
        bytes.extend(self.window_size.to_be_bytes());
        bytes.extend(self.seq_num.to_be_bytes());
        bytes.extend(self.ack_num.to_be_bytes());

        bytes
    }

    pub fn decode(value: &[u8]) -> Result<Self, PacketHeaderError> {
        if value.len() != PACKET_HEADER_LEN {
            return Err(PacketHeaderError::InvalidLen);
        }

        let packet_type = value[0] >> 4;
        let packet_type = PacketType::try_from(u8::from_be(packet_type))?;

        let version = value[0] & 0x0F;
        let version = Version::try_from(u8::from_be(version))?;

        let extension = u8::from_be(value[1]);
        let extension = Extension::from(extension);

        let conn_id = [value[2], value[3]];
        let conn_id = u16::from_be_bytes(conn_id);

        let ts_micros = [value[4], value[5], value[6], value[7]];
        let ts_micros = u32::from_be_bytes(ts_micros);

        let ts_diff_micros = [value[8], value[9], value[10], value[11]];
        let ts_diff_micros = u32::from_be_bytes(ts_diff_micros);

        let window_size = [value[12], value[13], value[14], value[15]];
        let window_size = u32::from_be_bytes(window_size);

        let seq_num = [value[16], value[17]];
        let seq_num = u16::from_be_bytes(seq_num);

        let ack_num = [value[18], value[19]];
        let ack_num = u16::from_be_bytes(ack_num);

        Ok(Self {
            packet_type,
            version,
            extension,
            conn_id,
            ts_micros,
            ts_diff_micros,
            window_size,
            seq_num,
            ack_num,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectiveAck {
    acked: Vec<[bool; SELECTIVE_ACK_BITS]>,
}

impl fmt::Display for SelectiveAck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = String::new();
        for chunk in &self.acked {
            for bit in chunk {
                if *bit {
                    s.push('1');
                } else {
                    s.push('0');
                }
            }
        }
        write!(f, "{}", s)
    }
}

impl SelectiveAck {
    pub fn new(acked: Vec<bool>) -> Self {
        let chunks = acked.as_slice().chunks_exact(SELECTIVE_ACK_BITS);
        let remainder = chunks.remainder();

        let mut acked = Vec::new();
        for chunk in chunks {
            let mut fragment: [bool; SELECTIVE_ACK_BITS] = [false; SELECTIVE_ACK_BITS];
            fragment.copy_from_slice(chunk);
            acked.push(fragment);
        }

        if !remainder.is_empty() {
            let mut fragment: [bool; SELECTIVE_ACK_BITS] = [false; SELECTIVE_ACK_BITS];
            fragment[..remainder.len()].copy_from_slice(remainder);
            acked.push(fragment);
        }

        Self { acked }
    }

    /// Returns the length in bytes of the encoded Selective ACK.
    pub fn encoded_len(&self) -> usize {
        (self.acked.len() * SELECTIVE_ACK_BITS) / 8
    }

    pub fn acked(&self) -> Vec<bool> {
        self.acked
            .clone()
            .into_iter()
            .flatten()
            .collect::<Vec<bool>>()
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut bitmask = vec![];

        for word in &self.acked {
            let chunks = word.as_slice().chunks_exact(8);

            for chunk in chunks {
                let mut byte = 0;

                byte |= u8::from(chunk[7]) << 7;
                byte |= u8::from(chunk[6]) << 6;
                byte |= u8::from(chunk[5]) << 5;
                byte |= u8::from(chunk[4]) << 4;
                byte |= u8::from(chunk[3]) << 3;
                byte |= u8::from(chunk[2]) << 2;
                byte |= u8::from(chunk[1]) << 1;
                byte |= u8::from(chunk[0]);

                bitmask.push(byte);
            }
        }

        bitmask
    }

    pub fn decode(value: &[u8]) -> Result<Self, SelectiveAckError> {
        if value.len() < 4 {
            return Err(SelectiveAckError::InsufficientLen);
        }
        if value.len() % 4 != 0 {
            return Err(SelectiveAckError::InvalidLen);
        }

        let mut acked: Vec<[bool; 32]> = vec![];
        let mut tmp = [false; 32];
        for (index, byte) in value.iter().enumerate() {
            tmp[(index * 8) % 32] = (*byte & 0b0000_0001) != 0;
            tmp[(index * 8 + 1) % 32] = (*byte & 0b0000_0010) != 0;
            tmp[(index * 8 + 2) % 32] = (*byte & 0b0000_0100) != 0;
            tmp[(index * 8 + 3) % 32] = (*byte & 0b0000_1000) != 0;
            tmp[(index * 8 + 4) % 32] = (*byte & 0b0001_0000) != 0;
            tmp[(index * 8 + 5) % 32] = (*byte & 0b0010_0000) != 0;
            tmp[(index * 8 + 6) % 32] = (*byte & 0b0100_0000) != 0;
            tmp[(index * 8 + 7) % 32] = (*byte & 0b1000_0000) != 0;

            if (index + 1) % 4 == 0 {
                acked.push(tmp);
                tmp = [false; 32];
            }
        }

        if value.len() % 4 != 0 {
            acked.push(tmp);
        }

        Ok(Self { acked })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Packet {
    header: PacketHeader,
    selective_ack: Option<SelectiveAck>,
    payload: Vec<u8>,
}

impl Packet {
    pub fn packet_type(&self) -> PacketType {
        self.header.packet_type
    }

    pub fn conn_id(&self) -> u16 {
        self.header.conn_id
    }

    pub fn ts_micros(&self) -> u32 {
        self.header.ts_micros
    }

    pub fn ts_diff_micros(&self) -> u32 {
        self.header.ts_diff_micros
    }

    pub fn window_size(&self) -> u32 {
        self.header.window_size
    }

    pub fn seq_num(&self) -> u16 {
        self.header.seq_num
    }

    pub fn ack_num(&self) -> u16 {
        self.header.ack_num
    }

    pub fn selective_ack(&self) -> Option<&SelectiveAck> {
        self.selective_ack.as_ref()
    }

    pub fn payload(&self) -> &Vec<u8> {
        &self.payload
    }

    pub fn builder(packet_type: PacketType) -> PacketBuilder {
        PacketBuilder::new(packet_type)
    }

    /// Returns the length in bytes of the encoded packet.
    pub fn encoded_len(&self) -> usize {
        let mut len = PACKET_HEADER_LEN;
        if let Some(ref sack) = self.selective_ack {
            len += sack.encoded_len() + EXTENSION_TYPE_LEN + EXTENSION_LEN_LEN;
        }
        len += self.payload.len();

        len
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = vec![];

        bytes.extend(self.header.encode());
        if let Some(ack) = &self.selective_ack {
            let ack = ack.encode();
            bytes.push(Extension::None.into());
            bytes.push((ack.len() as u8).to_be_bytes()[0]);
            bytes.extend(ack);
        }
        bytes.extend_from_slice(self.payload.as_slice());

        bytes
    }

    pub fn decode(value: &[u8]) -> Result<Self, PacketError> {
        if value.len() < PACKET_HEADER_LEN {
            return Err(PacketError::InvalidHeader(PacketHeaderError::InvalidLen));
        }

        let mut header: [u8; PACKET_HEADER_LEN] = [0; PACKET_HEADER_LEN];
        header.copy_from_slice(&value[..PACKET_HEADER_LEN]);
        let header = PacketHeader::decode(&header)?;

        let (extensions, extensions_len) =
            Self::decode_raw_extensions(header.extension, &value[PACKET_HEADER_LEN..])?;

        // Look for the first (if any) Selective ACK extension, and attempt to decode it.
        // TODO: Evaluate whether duplicate extensions should constitute an error.
        let selective_ack = extensions
            .iter()
            .find(|(ext, _)| *ext == Extension::SelectiveAck);
        let selective_ack = match selective_ack {
            Some((_, data)) => Some(SelectiveAck::decode(data)?),
            None => None,
        };

        // TODO: Save all raw extensions and make them accessible. People should be able to use
        // custom extensions.

        // The packet payload is the remainder of the packet.
        let payload_start_index = PACKET_HEADER_LEN + extensions_len;
        let payload = if payload_start_index == value.len() {
            vec![]
        } else {
            value[payload_start_index..].to_vec()
        };

        if header.packet_type == PacketType::Data && payload.is_empty() {
            return Err(PacketError::EmptyDataPayload);
        }

        Ok(Self {
            header,
            selective_ack,
            payload,
        })
    }

    // TODO: Resolve disabled clippy lint.
    #[allow(clippy::type_complexity)]
    fn decode_raw_extensions(
        first_ext: Extension,
        data: &[u8],
    ) -> Result<(Vec<(Extension, Vec<u8>)>, usize), ExtensionError> {
        let mut ext = first_ext;
        let mut index = 0;

        let mut extensions: Vec<(Extension, Vec<u8>)> = Vec::new();
        while ext != Extension::None {
            if data[index..].len() < 2 {
                return Err(ExtensionError::InsufficientLen);
            }

            let next_ext = u8::from_be_bytes([data[index]]);

            let ext_len = u8::from_be_bytes([data[index + 1]]);
            let ext_len = usize::from(ext_len);

            let ext_start = index + 2;
            if data[ext_start..].len() < ext_len {
                return Err(ExtensionError::InsufficientLen);
            }

            let ext_data = data[ext_start..ext_start + ext_len].to_vec();
            extensions.push((ext, ext_data));

            ext = Extension::from(next_ext);
            index = ext_start + ext_len;
        }

        Ok((extensions, index))
    }
}

pub struct PacketBuilder {
    packet_type: PacketType,
    conn_id: u16,
    ts_micros: u32,
    ts_diff_micros: u32,
    window_size: u32,
    seq_num: u16,
    ack_num: u16,
    selective_ack: Option<SelectiveAck>,
    payload: Option<Vec<u8>>,
}

impl PacketBuilder {
    pub fn new(packet_type: PacketType) -> Self {
        Self {
            packet_type,
            conn_id: 0,
            ts_micros: 0,
            ts_diff_micros: 0,
            window_size: 0,
            seq_num: 0,
            ack_num: 0,
            selective_ack: None,
            payload: None,
        }
    }

    pub fn conn_id(mut self, conn_id: u16) -> Self {
        self.conn_id = conn_id;
        self
    }

    pub fn ts_micros(mut self, ts_micros: u32) -> Self {
        self.ts_micros = ts_micros;
        self
    }

    pub fn ts_diff_micros(mut self, ts_diff_micros: u32) -> Self {
        self.ts_diff_micros = ts_diff_micros;
        self
    }

    pub fn window_size(mut self, window_size: u32) -> Self {
        self.window_size = window_size;
        self
    }

    pub fn seq_num(mut self, seq_num: u16) -> Self {
        self.seq_num = seq_num;
        self
    }

    pub fn ack_num(mut self, ack_num: u16) -> Self {
        self.ack_num = ack_num;
        self
    }

    pub fn selective_ack(mut self, selective_ack: Option<SelectiveAck>) -> Self {
        self.selective_ack = selective_ack;
        self
    }

    pub fn payload(mut self, payload: Vec<u8>) -> Self {
        self.payload = Some(payload);
        self
    }

    pub fn build(self) -> Packet {
        let extension = match self.selective_ack {
            Some(..) => Extension::SelectiveAck,
            None => Extension::None,
        };

        Packet {
            header: PacketHeader {
                packet_type: self.packet_type,
                version: Version::One,
                extension,
                conn_id: self.conn_id,
                ts_micros: self.ts_micros,
                ts_diff_micros: self.ts_diff_micros,
                window_size: self.window_size,
                seq_num: self.seq_num,
                ack_num: self.ack_num,
            },
            selective_ack: self.selective_ack,
            payload: self.payload.unwrap_or(vec![]),
        }
    }
}

impl From<Packet> for PacketBuilder {
    fn from(value: Packet) -> Self {
        let payload = if value.payload.is_empty() {
            None
        } else {
            Some(value.payload)
        };

        Self {
            packet_type: value.header.packet_type,
            conn_id: value.header.conn_id,
            ts_micros: value.header.ts_micros,
            ts_diff_micros: value.header.ts_diff_micros,
            window_size: value.header.window_size,
            seq_num: value.header.seq_num,
            ack_num: value.header.ack_num,
            selective_ack: value.selective_ack,
            payload,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use quickcheck::{quickcheck, Arbitrary, Gen, TestResult};

    impl Arbitrary for PacketHeader {
        fn arbitrary(g: &mut Gen) -> Self {
            let packet_type = u8::arbitrary(g);
            let packet_type = if packet_type % 5 == 0 {
                PacketType::Data
            } else if packet_type % 5 == 1 {
                PacketType::Fin
            } else if packet_type % 5 == 2 {
                PacketType::State
            } else if packet_type % 5 == 3 {
                PacketType::Reset
            } else {
                PacketType::Syn
            };

            let extension = u8::arbitrary(g);
            let extension = Extension::from(extension);

            Self {
                packet_type,
                version: Version::One,
                extension,
                conn_id: u16::arbitrary(g),
                ts_micros: u32::arbitrary(g),
                ts_diff_micros: u32::arbitrary(g),
                window_size: u32::arbitrary(g),
                ack_num: u16::arbitrary(g),
                seq_num: u16::arbitrary(g),
            }
        }
    }

    impl Arbitrary for SelectiveAck {
        fn arbitrary(g: &mut Gen) -> Self {
            let bits: Vec<bool> = Vec::arbitrary(g);

            let mut acked: Vec<[bool; 32]> = vec![];

            let mut tmp = [false; 32];
            for (index, bit) in bits.iter().enumerate() {
                tmp[index % 32] = *bit;

                if (index + 1) % 32 == 0 {
                    acked.push(tmp);
                    tmp = [false; 32];
                }
            }

            if bits.len() % 32 != 0 || acked.is_empty() {
                acked.push(tmp);
            }

            Self { acked }
        }
    }

    // TODO: Add more tests. For example, packet encoding and decoding should test for arbitrary
    // extensions.

    #[test]
    fn header_encode_decode() {
        fn prop(header: PacketHeader) -> TestResult {
            let encoded = header.encode();
            let encoded: [u8; 20] = encoded
                .try_into()
                .expect("invalid length for encoded uTP packet header");
            let decoded =
                PacketHeader::decode(&encoded).expect("failed to decode uTP packet header");

            TestResult::from_bool(decoded == header)
        }
        quickcheck(prop as fn(PacketHeader) -> TestResult);
    }

    #[test]
    fn selective_ack_encode_decode() {
        fn prop(selective_ack: SelectiveAck) -> TestResult {
            let encoded_len = selective_ack.encoded_len();

            let encoded = selective_ack.encode();

            assert!(encoded.len() % (SELECTIVE_ACK_BITS / 8) == 0);
            assert_eq!(encoded.len(), encoded_len);

            let decoded = SelectiveAck::decode(&encoded).expect("failed to decode Selective ACK");

            TestResult::from_bool(decoded == selective_ack)
        }
        quickcheck(prop as fn(SelectiveAck) -> TestResult);
    }

    #[test]
    fn packet_encode_decode() {
        fn prop(
            mut header: PacketHeader,
            selective_ack: SelectiveAck,
            payload: Vec<u8>,
        ) -> TestResult {
            if payload.is_empty() {
                return TestResult::discard();
            }

            let selective_ack = if selective_ack.acked.is_empty() {
                None
            } else {
                Some(selective_ack)
            };
            match selective_ack {
                Some(..) => {
                    header.extension = Extension::SelectiveAck;
                }
                None => {
                    header.extension = Extension::None;
                }
            }

            let packet = Packet {
                header,
                selective_ack,
                payload,
            };

            let encoded_len = packet.encoded_len();

            let encoded = packet.encode();

            assert_eq!(encoded.len(), encoded_len);

            let decoded = Packet::decode(&encoded).expect("failed to decode uTP packet");

            TestResult::from_bool(decoded == packet)
        }
        quickcheck(prop as fn(PacketHeader, SelectiveAck, Vec<u8>) -> TestResult);
    }
}
