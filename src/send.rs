use std::collections::HashMap;
use std::convert::TryFrom;
use std::marker::Unpin;

/// Stuff to do with the fetch protocol
use tokio::io::{self, AsyncRead, AsyncWrite};

use super::Capability;
use super::ProtocolLine;

pub struct GitSend<R, W> {
    reader: R,
    writer: W,
    caps: HashMap<Capability, Option<String>>,
    refs: HashMap<String, String>,
}

impl<R, W> GitSend<R, W>
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
                        // On the off chance this repo has no refs yet, we'll still get a line to give us
                        // capabilities, but it'll be magical
                        if refname != "capabilities^{}" {
                            self.refs.insert(refname.to_string(), sha.to_string());
                        }
                    } else {
                        return Err(io::Error::new(io::ErrorKind::Other, "Malformed ref line"));
                    }
                }
            }
        }
        Ok(())
    }
}
