//! 文件与帧格式。
//!
//! 文件由一个 64 字节头和连续帧组成。每帧自带 CRC 与记录数，批量写入
//! 要么整帧生效，要么在恢复时被丢弃，从而保证批量原子性。
//!
//! ```text
//! file header (64B)
//!   magic[8] = "CHATKV01"
//!   version u32
//!   header_len u32
//!   reserved[48]
//!
//! frame
//!   magic u32 = "KVF1"
//!   flags u32
//!   payload_len u32
//!   record_count u32
//!   crc32(payload) u32      IEEE CRC-32，只覆盖 payload
//!   payload
//!
//! payload record
//!   kind u8            1=set 2=remove 3=clear
//!   key_len u32
//!   value_len u32
//!   key bytes
//!   value bytes
//! ```

use crate::KVError;

pub(crate) const FILE_MAGIC: &[u8; 8] = b"CHATKV01";
pub(crate) const FORMAT_VERSION: u32 = 1;
pub(crate) const FILE_HEADER_LEN: u64 = 64;
pub(crate) const FRAME_MAGIC: u32 = 0x4B56_4631; // "KVF1"
pub(crate) const FRAME_HEADER_LEN: usize = 20;
pub(crate) const RECORD_HEADER_LEN: usize = 9;
pub(crate) const RECORD_SET: u8 = 1;
pub(crate) const RECORD_REMOVE: u8 = 2;
pub(crate) const RECORD_CLEAR: u8 = 3;

/// 恢复时允许的帧上限，独立于当前写入配置。
///
/// 宿主可能调小 `max_frame_bytes`；旧文件里合法的较大帧仍应能读出。
/// 这里只防止损坏长度导致超大切片/超长 CRC。
pub(crate) const HARD_MAX_FRAME_BYTES: u64 = 256 * 1024 * 1024;

/// 文件头字节。
pub(crate) fn file_header() -> [u8; FILE_HEADER_LEN as usize] {
    let mut header = [0u8; FILE_HEADER_LEN as usize];
    header[..8].copy_from_slice(FILE_MAGIC);
    header[8..12].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    header[12..16].copy_from_slice(&(FILE_HEADER_LEN as u32).to_le_bytes());
    header
}

/// 校验文件头；空文件/全零文件由调用方先行识别。
pub(crate) fn validate_file_header(bytes: &[u8]) -> Result<(), KVError> {
    if bytes.len() < FILE_HEADER_LEN as usize {
        return Err(KVError::Corrupt {
            reason: format!("file header is truncated ({} bytes)", bytes.len()),
        });
    }
    if &bytes[..8] != FILE_MAGIC {
        return Err(KVError::Corrupt {
            reason: "unknown file magic".into(),
        });
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().expect("fixed slice"));
    if version != FORMAT_VERSION {
        return Err(KVError::Corrupt {
            reason: format!("unsupported file version {version}"),
        });
    }
    let header_len = u32::from_le_bytes(bytes[12..16].try_into().expect("fixed slice"));
    if header_len as u64 != FILE_HEADER_LEN {
        return Err(KVError::Corrupt {
            reason: format!("unsupported header length {header_len}"),
        });
    }
    Ok(())
}

/// 帧头解析结果。
pub(crate) enum FrameHeader {
    /// 全零帧头：预分配但未写入的尾部，视为日志结束。
    End,
    /// 合法帧头。
    Frame {
        payload_len: u32,
        count: u32,
        crc: u32,
    },
    /// 非零但不是合法帧头：按损坏尾部处理。
    Invalid,
}

pub(crate) fn decode_frame_header(bytes: &[u8]) -> FrameHeader {
    if bytes.len() < FRAME_HEADER_LEN {
        return FrameHeader::Invalid;
    }
    let magic = u32::from_le_bytes(bytes[0..4].try_into().expect("fixed slice"));
    if magic == 0 {
        return FrameHeader::End;
    }
    if magic != FRAME_MAGIC {
        return FrameHeader::Invalid;
    }
    let flags = u32::from_le_bytes(bytes[4..8].try_into().expect("fixed slice"));
    if flags != 0 {
        return FrameHeader::Invalid;
    }
    FrameHeader::Frame {
        payload_len: u32::from_le_bytes(bytes[8..12].try_into().expect("fixed slice")),
        count: u32::from_le_bytes(bytes[12..16].try_into().expect("fixed slice")),
        crc: u32::from_le_bytes(bytes[16..20].try_into().expect("fixed slice")),
    }
}

fn frame_header_bytes(payload_len: u32, count: u32, crc: u32) -> [u8; FRAME_HEADER_LEN] {
    let mut header = [0u8; FRAME_HEADER_LEN];
    header[0..4].copy_from_slice(&FRAME_MAGIC.to_le_bytes());
    header[4..8].copy_from_slice(&0u32.to_le_bytes());
    header[8..12].copy_from_slice(&payload_len.to_le_bytes());
    header[12..16].copy_from_slice(&count.to_le_bytes());
    header[16..20].copy_from_slice(&crc.to_le_bytes());
    header
}

/// 一帧的写入缓冲。帧头先占位，payload 直接追加，`finish` 时回填 CRC。
pub(crate) struct FrameBuf {
    buf: Vec<u8>,
    count: u32,
}

impl FrameBuf {
    pub(crate) fn new() -> Self {
        let mut buf = Vec::with_capacity(256);
        buf.resize(FRAME_HEADER_LEN, 0);
        Self { buf, count: 0 }
    }

    pub(crate) fn payload_len(&self) -> usize {
        self.buf.len() - FRAME_HEADER_LEN
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub(crate) fn push_set(&mut self, key: &str, value: &[u8]) -> Result<(), KVError> {
        self.push_record(RECORD_SET, key, value)
    }

    pub(crate) fn push_remove(&mut self, key: &str) -> Result<(), KVError> {
        self.push_record(RECORD_REMOVE, key, &[])
    }

    pub(crate) fn push_clear(&mut self) -> Result<(), KVError> {
        self.push_record(RECORD_CLEAR, "", &[])
    }

    fn push_record(&mut self, kind: u8, key: &str, value: &[u8]) -> Result<(), KVError> {
        let key_len = u32::try_from(key.len()).map_err(|_| KVError::InvalidKey {
            reason: "key does not fit in the on-disk format".into(),
        })?;
        let value_len = u32::try_from(value.len()).map_err(|_| KVError::ValueTooLarge {
            size: value.len(),
            max_bytes: u32::MAX as usize,
        })?;
        self.count = self
            .count
            .checked_add(1)
            .ok_or(KVError::BatchTooLarge {
                size: self.buf.len(),
                max_bytes: u32::MAX as usize,
            })?;
        self.buf.push(kind);
        self.buf.extend_from_slice(&key_len.to_le_bytes());
        self.buf.extend_from_slice(&value_len.to_le_bytes());
        self.buf.extend_from_slice(key.as_bytes());
        self.buf.extend_from_slice(value);
        Ok(())
    }

    /// 回填帧头并返回完整帧字节与记录数。
    pub(crate) fn finish(mut self) -> (Vec<u8>, u32) {
        let payload_len = (self.buf.len() - FRAME_HEADER_LEN) as u32;
        let crc = crc32fast::hash(&self.buf[FRAME_HEADER_LEN..]);
        let header = frame_header_bytes(payload_len, self.count, crc);
        self.buf[..FRAME_HEADER_LEN].copy_from_slice(&header);
        (self.buf, self.count)
    }
}

/// 解析后的一条记录。
pub(crate) struct Record<'a> {
    pub(crate) kind: u8,
    pub(crate) key: &'a str,
    pub(crate) value: &'a [u8],
    /// 值相对 payload 起点的偏移。
    pub(crate) value_offset: usize,
    /// 该记录在文件中的字节数（不含帧头）。
    pub(crate) record_len: usize,
}

/// 严格解析并逐条交给调用方；允许 `clear` 与其它记录同帧。
pub(crate) fn for_each_record<'a>(
    payload: &'a [u8],
    count: u32,
    mut apply: impl FnMut(Record<'a>) -> Result<(), KVError>,
) -> Result<(), KVError> {
    let mut cursor = 0usize;
    for _ in 0..count {
        let header_end = cursor
            .checked_add(RECORD_HEADER_LEN)
            .ok_or_else(|| corrupt("record header overflow"))?;
        if header_end > payload.len() {
            return Err(corrupt("record header exceeds payload"));
        }
        let kind = payload[cursor];
        let key_len = u32::from_le_bytes(
            payload[cursor + 1..cursor + 5]
                .try_into()
                .expect("fixed slice"),
        ) as usize;
        let value_len = u32::from_le_bytes(
            payload[cursor + 5..cursor + 9]
                .try_into()
                .expect("fixed slice"),
        ) as usize;
        cursor = header_end;

        let key_end = cursor
            .checked_add(key_len)
            .ok_or_else(|| corrupt("key length overflow"))?;
        if key_end > payload.len() {
            return Err(corrupt("key exceeds payload"));
        }
        let value_end = key_end
            .checked_add(value_len)
            .ok_or_else(|| corrupt("value length overflow"))?;
        if value_end > payload.len() {
            return Err(corrupt("value exceeds payload"));
        }

        let key_bytes = &payload[cursor..key_end];
        let key = std::str::from_utf8(key_bytes).map_err(|_| corrupt("key is not valid UTF-8"))?;
        let value = &payload[key_end..value_end];

        if kind == RECORD_CLEAR && (key_len != 0 || value_len != 0) {
            return Err(corrupt("clear record carries key or value"));
        }
        if kind == RECORD_REMOVE && value_len != 0 {
            return Err(corrupt("remove record carries a value"));
        }
        if kind != RECORD_SET && kind != RECORD_REMOVE && kind != RECORD_CLEAR {
            return Err(corrupt("unknown record kind"));
        }

        apply(Record {
            kind,
            key,
            value,
            value_offset: key_end,
            record_len: RECORD_HEADER_LEN + key_len + value_len,
        })?;
        cursor = value_end;
    }
    if cursor != payload.len() {
        return Err(corrupt("frame payload has trailing bytes"));
    }
    Ok(())
}

fn corrupt(reason: &str) -> KVError {
    KVError::Corrupt {
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_roundtrip_preserves_records() {
        let mut frame = FrameBuf::new();
        frame.push_set("alpha", b"one").expect("set");
        frame.push_remove("beta").expect("remove");
        frame.push_set("", b"").expect("empty");
        let (bytes, count) = frame.finish();
        assert_eq!(count, 3);
        match decode_frame_header(&bytes) {
            FrameHeader::Frame {
                payload_len,
                count,
                crc,
            } => {
                assert_eq!(payload_len as usize, bytes.len() - FRAME_HEADER_LEN);
                assert_eq!(count, 3);
                assert_eq!(crc32fast::hash(&bytes[FRAME_HEADER_LEN..]), crc);
            }
            _ => panic!("expected frame header"),
        }

        let mut seen = Vec::new();
        for_each_record(&bytes[FRAME_HEADER_LEN..], count, |record| {
            seen.push((
                record.kind,
                record.key.to_string(),
                record.value.to_vec(),
                record.value_offset,
                record.record_len,
            ));
            Ok(())
        })
        .expect("parse");
        assert_eq!(seen.len(), 3);
        assert_eq!(seen[0].0, RECORD_SET);
        assert_eq!(seen[0].1, "alpha");
        assert_eq!(seen[0].2, b"one");
        assert!(seen[0].3 < bytes.len());
        assert_eq!(seen[1].0, RECORD_REMOVE);
        assert_eq!(seen[1].1, "beta");
        assert_eq!(seen[2].1, "");
    }

    #[test]
    fn zero_header_means_end_and_garbage_is_invalid() {
        assert!(matches!(
            decode_frame_header(&[0u8; FRAME_HEADER_LEN]),
            FrameHeader::End
        ));
        let mut garbage = [0u8; FRAME_HEADER_LEN];
        garbage[0] = 1;
        assert!(matches!(
            decode_frame_header(&garbage),
            FrameHeader::Invalid
        ));
    }

    #[test]
    fn truncated_payload_is_rejected() {
        let mut frame = FrameBuf::new();
        frame.push_set("k", b"v").expect("set");
        let (bytes, count) = frame.finish();
        let truncated = &bytes[..bytes.len() - 1];
        assert!(for_each_record(&truncated[FRAME_HEADER_LEN..], count, |_| Ok(())).is_err());
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let mut frame = FrameBuf::new();
        frame.push_set("k", b"v").expect("set");
        let (mut bytes, count) = frame.finish();
        bytes.push(0);
        assert!(for_each_record(&bytes[FRAME_HEADER_LEN..], count, |_| Ok(())).is_err());
    }
}
