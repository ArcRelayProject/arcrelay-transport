//! Transport-neutral ArcRelay frame primitives shared by QUIC clients and
//! servers. The crate deliberately knows nothing about desktop domain types.

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("frame I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("empty frame")]
    Empty,
    #[error("frame is too large: {actual} bytes (maximum {maximum})")]
    TooLarge { actual: usize, maximum: usize },
    #[error("frame length cannot be represented as u32: {0}")]
    LengthOverflow(usize),
}

impl FrameError {
    /// Stable machine-readable category for protocol adapters.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Io(_) => "transport.unavailable",
            Self::Empty => "transport.empty_frame",
            Self::TooLarge { .. } => "transport.resource_exhausted",
            Self::LengthOverflow(_) => "transport.length_overflow",
        }
    }
}

pub async fn read_stream_kind<R: AsyncRead + Unpin>(reader: &mut R) -> Result<u8, FrameError> {
    Ok(reader.read_u8().await?)
}

/// A length-prefixed decoder whose progress survives cancellation of `read`.
///
/// Keep one decoder per stream when reading inside `tokio::select!`. The
/// payload buffer is reused between frames; consume the returned slice before
/// reading the next frame. A framing or I/O error is terminal for the stream.
pub struct FrameReader {
    maximum: usize,
    header: [u8; 4],
    header_read: usize,
    payload: Vec<u8>,
    payload_read: usize,
    complete: bool,
}

impl FrameReader {
    pub fn new(maximum: usize) -> Self {
        Self {
            maximum,
            header: [0; 4],
            header_read: 0,
            payload: Vec::new(),
            payload_read: 0,
            complete: false,
        }
    }

    pub async fn read<R: AsyncRead + Unpin>(
        &mut self,
        reader: &mut R,
    ) -> Result<&[u8], FrameError> {
        if self.complete {
            self.header_read = 0;
            self.payload_read = 0;
            self.complete = false;
        }
        while self.header_read < self.header.len() {
            // AsyncReadExt::read is cancellation-safe. Commit every completed
            // read to this decoder before another await can yield control.
            let count = reader.read(&mut self.header[self.header_read..]).await?;
            if count == 0 {
                return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
            }
            self.header_read += count;
        }
        let length = u32::from_be_bytes(self.header) as usize;
        if length == 0 {
            return Err(FrameError::Empty);
        }
        if length > self.maximum {
            return Err(FrameError::TooLarge {
                actual: length,
                maximum: self.maximum,
            });
        }
        self.payload.resize(length, 0);
        while self.payload_read < length {
            let count = reader.read(&mut self.payload[self.payload_read..]).await?;
            if count == 0 {
                return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
            }
            self.payload_read += count;
        }
        self.complete = true;
        Ok(&self.payload)
    }
}

/// Read one complete frame. Cancellation discards partially consumed bytes;
/// use a persistent [`FrameReader`] when competing with other futures.
pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    maximum: usize,
) -> Result<Vec<u8>, FrameError> {
    let mut length = [0_u8; 4];
    reader.read_exact(&mut length).await?;
    let length = u32::from_be_bytes(length) as usize;
    if length == 0 {
        return Err(FrameError::Empty);
    }
    if length > maximum {
        return Err(FrameError::TooLarge {
            actual: length,
            maximum,
        });
    }
    let mut frame = vec![0_u8; length];
    reader.read_exact(&mut frame).await?;
    Ok(frame)
}

pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &[u8],
    maximum: usize,
) -> Result<(), FrameError> {
    if frame.is_empty() {
        return Err(FrameError::Empty);
    }
    if frame.len() > maximum {
        return Err(FrameError::TooLarge {
            actual: frame.len(),
            maximum,
        });
    }
    let length = u32::try_from(frame.len()).map_err(|_| FrameError::LengthOverflow(frame.len()))?;
    writer.write_all(&length.to_be_bytes()).await?;
    writer.write_all(frame).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn frames_round_trip_and_enforce_limits() {
        let (mut writer, mut reader) = tokio::io::duplex(64);
        write_frame(&mut writer, b"arcrelay", 16).await.unwrap();
        assert_eq!(read_frame(&mut reader, 16).await.unwrap(), b"arcrelay");
        assert!(matches!(
            write_frame(&mut writer, b"too large", 4).await,
            Err(FrameError::TooLarge { .. })
        ));
    }

    #[tokio::test]
    async fn decoder_resumes_after_cancellation_at_every_frame_boundary() {
        let payload = b"abcdefgh";
        let mut encoded = (payload.len() as u32).to_be_bytes().to_vec();
        encoded.extend_from_slice(payload);
        for boundary in 0..encoded.len() {
            let (mut writer, mut reader) = tokio::io::duplex(64);
            let mut decoder = FrameReader::new(32);
            writer.write_all(&encoded[..boundary]).await.unwrap();
            tokio::select! {
                biased;
                frame = decoder.read(&mut reader) => panic!("incomplete frame: {frame:?}"),
                _ = std::future::ready(()) => {},
            }
            writer.write_all(&encoded[boundary..]).await.unwrap();
            write_frame(&mut writer, b"next", 32).await.unwrap();
            assert_eq!(decoder.read(&mut reader).await.unwrap(), payload);
            assert_eq!(decoder.read(&mut reader).await.unwrap(), b"next");
        }
    }

    #[tokio::test]
    async fn decoder_rejects_limits_before_allocating_and_reports_truncation() {
        for (length, empty) in [(0_u32, true), (33, false)] {
            let header = length.to_be_bytes();
            let mut reader = header.as_slice();
            let mut decoder = FrameReader::new(32);
            let error = decoder.read(&mut reader).await.unwrap_err();
            assert!(if empty {
                matches!(error, FrameError::Empty)
            } else {
                matches!(
                    error,
                    FrameError::TooLarge {
                        actual: 33,
                        maximum: 32
                    }
                )
            });
            assert_eq!(decoder.payload.capacity(), 0);
        }
        let mut bytes = &b"\0\0\0\x08ab"[..];
        let error = FrameReader::new(32).read(&mut bytes).await.unwrap_err();
        assert!(
            matches!(error, FrameError::Io(error) if error.kind() == std::io::ErrorKind::UnexpectedEof)
        );
    }
}
