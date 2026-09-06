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
}
