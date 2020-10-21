use std::collections::HashMap;
use std::convert::TryFrom;
use std::marker::Unpin;

/// Stuff to do with the fetch protocol
use tokio::io::{self, AsyncRead, AsyncWrite};

use super::Capability;
use super::ProtocolLine;

pub struct GitFetch<R, W> {
    reader: R,
    writer: W,
    caps: HashMap<Capability, Option<String>>,
    refs: HashMap<String, String>,
}

impl<R, W> GitFetch<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    pub fn new(reader: R, writer: W) -> Self {
        Self {
            reader,
            writer,
            caps: HashMap::new(),
            refs: HashMap::new(),
        }
    }

    pub async fn read_advertisement(&mut self) -> io::Result<()> {
        loop {
            match ProtocolLine::read_from(&mut self.reader, true).await? {
                ProtocolLine::Flush => break,
                ProtocolLine::Delimiter | ProtocolLine::ResponseEnd => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Unexpected protocol packet",
                    ));
                }
                ProtocolLine::Data(cow) => {
                    let mut bits = cow.split(|v| *v == 0);
                    let refpart = bits.next().ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            "Unable to find ref-part of announcement line",
                        )
                    })?;
                    if let Some(caps) = bits.next() {
                        // We have some capabilities to process
                        let caps = String::from_utf8_lossy(caps);
                        for cap in caps.split(' ') {
                            // if the capability has an equals in it, we need to split that off
                            let (capname, capvalue) = if let Some(idx) = cap.find('=') {
                                (&cap[..idx], Some(&cap[idx + 1..]))
                            } else {
                                (cap, None)
                            };
                            if let Ok(cap) = Capability::try_from(capname) {
                                self.caps.insert(cap, capvalue.map(ToOwned::to_owned));
                            }
                        }
                    }
                    // Now process the ref part
                    let refpart = String::from_utf8_lossy(refpart);
                    if let Some(pos) = refpart.find(' ') {
                        let (sha, refname) = {
                            let p = refpart.split_at(pos);
                            (p.0, &p.1[1..])
                        };
                        self.refs.insert(refname.to_string(), sha.to_string());
                    } else {
                        return Err(io::Error::new(io::ErrorKind::Other, "Malformed ref line"));
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn request_pack(
        &mut self,
        want: impl Iterator<Item = &str>,
        have: impl Iterator<Item = &str>,
        caps: impl Iterator<Item = (Capability, Option<&str>)>,
    ) -> io::Result<bool> {
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
            ProtocolLine::write_str(&mut self.writer, cmd).await?;
            sent_want = true;
        }
        ProtocolLine::Flush.write_to(&mut self.writer).await?;
        if !sent_want {
            // There will be no pack, this is the end of the discussion.
            return Ok(false);
        }
        for sha in have {
            ProtocolLine::write_str(&mut self.writer, format!("have {}", sha)).await?;
        }
        ProtocolLine::write_str(&mut self.writer, "done").await?;
        // Since we deliberately sent no multi-ack, we expect to read a NAK packet now
        match ProtocolLine::read_from(&mut self.reader, true).await? {
            ProtocolLine::Data(cow) if cow == (b"NAK" as &[u8]) => {}
            _ => {
                return Err(io::Error::new(io::ErrorKind::Other, "NAK packet not found"));
            }
        }
        // We're ready now
        Ok(true)
    }
}
