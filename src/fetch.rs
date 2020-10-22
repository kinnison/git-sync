/// Stuff to do with the fetch protocol
use tokio::io::{self, AsyncRead, AsyncWrite};

use super::Capability;
use super::ProtocolLine;

pub async fn request_pack<R, W>(
    reader: &mut R,
    writer: &mut W,
    want: impl Iterator<Item = &str>,
    have: impl Iterator<Item = &str>,
    caps: impl Iterator<Item = (Capability, Option<&str>)>,
) -> io::Result<bool>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    // To request a pack from the remote end we need to send wants and haves.  With our first want, we send our capability list.
    // It's an error to have a capability list include the multi-ack or multi-ack-detailed capabilities for now because we do not
    // process that functionality for now
    let mut caps = caps.fuse();
    let mut sent_want = false;
    for sha in want {
        let mut cmd = format!("want {}", sha);
        for cap in &mut caps {
            let capname = cap.0.as_str();
            cmd.push(' ');
            cmd.push_str(capname);
            if let Some(capvalue) = cap.1 {
                cmd.push('=');
                cmd.push_str(capvalue);
            }
        }
        ProtocolLine::write_str(writer, cmd).await?;
        sent_want = true;
    }
    ProtocolLine::Flush.write_to(writer).await?;
    if !sent_want {
        // There will be no pack, this is the end of the discussion.
        return Ok(false);
    }
    for sha in have {
        ProtocolLine::write_str(writer, format!("have {}", sha)).await?;
    }
    ProtocolLine::write_str(writer, "done").await?;
    // Since we deliberately sent no multi-ack, we expect to read a NAK packet now
    match ProtocolLine::read_from(reader, true).await? {
        ProtocolLine::Data(cow) if cow == (b"NAK" as &[u8]) => {}
        _ => {
            return Err(io::Error::new(io::ErrorKind::Other, "NAK packet not found"));
        }
    }
    // We're ready now
    Ok(true)
}
