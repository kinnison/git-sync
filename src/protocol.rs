/// Git protocol related content
use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryFrom;

use std::marker::Unpin;
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const NULLSHA: &str = "0000000000000000000000000000000000000000";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolLine<'a> {
    /// A flush packet is `0000`
    Flush,
    /// A delimiter packet is `0001`
    Delimiter,
    /// A response-end packet is `0002`
    ResponseEnd,
    /// Other packets are arbitrary byte sequences.
    /// They may end up having other semantics, but at the packet
    /// level they're just bytes.
    Data(Cow<'a, [u8]>),
}

impl ProtocolLine<'_> {
    /// Convert this protocol line which might borrow from elsewhere into one which owns the data it holds
    /// ```
    /// # use std::borrow::Cow;
    /// # use git_sync::ProtocolLine;
    /// let owned: ProtocolLine<'static> = {
    ///     let s = "foobar".to_string();
    ///     let b = s.as_bytes();
    ///     let line = ProtocolLine::Data(Cow::from(b));
    ///     line.into_owned()
    /// };
    /// ```
    pub fn into_owned(self) -> ProtocolLine<'static> {
        match self {
            ProtocolLine::Flush => ProtocolLine::Flush,
            ProtocolLine::Delimiter => ProtocolLine::Delimiter,
            ProtocolLine::ResponseEnd => ProtocolLine::ResponseEnd,
            ProtocolLine::Data(cow) => ProtocolLine::Data(Cow::from(cow.into_owned())),
        }
    }

    pub async fn write_str<W, S>(writer: &mut W, s: S) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
        S: AsRef<str>,
    {
        let s = s.as_ref();
        let pktlen = format!("{:04x}", s.len() + 4);
        writer.write_all(pktlen.as_bytes()).await?;
        writer.write_all(s.as_bytes()).await
    }

    pub async fn write_to<W>(&self, writer: &mut W) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        match self {
            ProtocolLine::Flush => writer.write_all(b"0000").await?,
            ProtocolLine::Delimiter => writer.write_all(b"0001").await?,
            ProtocolLine::ResponseEnd => writer.write_all(b"0002").await?,
            ProtocolLine::Data(cow) => {
                let pktlen = format!("{:04x}", cow.len() + 4 /* For the header */);
                writer.write_all(pktlen.as_bytes()).await?;
                writer.write_all(cow).await?;
            }
        }
        Ok(())
    }

    pub async fn read_from<R>(
        reader: &mut R,
        chomp_newline: bool,
    ) -> io::Result<ProtocolLine<'static>>
    where
        R: AsyncRead + Unpin,
    {
        let mut lenbuf = [b'0'; 4];
        reader.read_exact(&mut lenbuf).await?;
        Ok(match &lenbuf {
            b"0000" => ProtocolLine::Flush,
            b"0001" => ProtocolLine::Delimiter,
            b"0002" => ProtocolLine::ResponseEnd,
            b"0003" => return Err(io::Error::from(io::ErrorKind::Other)),
            _ => {
                lenbuf.iter_mut().for_each(|v| {
                    *v = match *v {
                        b'0'..=b'9' => *v - b'0',
                        b'a'..=b'f' => *v - b'a' + 10,
                        _ => 0,
                    }
                });
                let pktlen = ((lenbuf[0] as usize) << 12)
                    + ((lenbuf[1] as usize) << 8)
                    + ((lenbuf[2] as usize) << 4)
                    + (lenbuf[3] as usize)
                    - 4 /* For the header */;
                let mut data: Vec<u8> = Vec::with_capacity(pktlen);
                if pktlen != reader.take(pktlen as u64).read_to_end(&mut data).await? {
                    return Err(io::Error::from(io::ErrorKind::BrokenPipe));
                }
                if chomp_newline && !data.is_empty() && data[data.len() - 1] == b'\n' {
                    data.pop();
                }
                ProtocolLine::Data(Cow::from(data))
            }
        })
    }
}

impl<'a, T> From<T> for ProtocolLine<'a>
where
    T: Into<Cow<'a, [u8]>>,
{
    fn from(value: T) -> ProtocolLine<'a> {
        ProtocolLine::Data(value.into())
    }
}

#[derive(Copy, Clone, Hash, PartialEq, Eq)]
pub enum Capability {
    MultiAck,
    MultiAckDetailed,
    NoDone,
    ThinPack,
    SideBand,
    SideBand64K,
    OfsDelta,
    Agent,
    ObjectFormat,
    SymRef,
    Shallow,
    DeepenSince,
    DeepenNot,
    DeepenRelative,
    NoProgress,
    IncludeTag,
    ReportStatus,
    ReportStatusV2,
    DeleteRefs,
    Quiet,
    Atomic,
    PushOptions,
    AllowTipSha1InWant,
    AllowReachableSha1InWant,
    PushCert,
    Filter,
}

impl Capability {
    pub fn as_str(self) -> &'static str {
        match self {
            Capability::MultiAck => "multi_ack",
            Capability::MultiAckDetailed => "multi_ack_detailed",
            Capability::NoDone => "no-done",
            Capability::ThinPack => "thin-pack",
            Capability::SideBand => "side-band",
            Capability::SideBand64K => "side-band-64k",
            Capability::OfsDelta => "ofs-delta",
            Capability::Agent => "agent",
            Capability::ObjectFormat => "object-format",
            Capability::SymRef => "symref",
            Capability::Shallow => "shallow",
            Capability::DeepenSince => "deepen-since",
            Capability::DeepenNot => "deepen-not",
            Capability::DeepenRelative => "deepen-relative",
            Capability::NoProgress => "no-progress",
            Capability::IncludeTag => "include-tag",
            Capability::ReportStatus => "report-status",
            Capability::ReportStatusV2 => "report-status-v2",
            Capability::DeleteRefs => "delete-refs",
            Capability::Quiet => "quiet",
            Capability::Atomic => "atomic",
            Capability::PushOptions => "push-options",
            Capability::AllowTipSha1InWant => "allow-tip-sha1-in-want",
            Capability::AllowReachableSha1InWant => "allow-reachable-sha1-in-want",
            Capability::PushCert => "push-cert",
            Capability::Filter => "filter",
        }
    }
}

impl<'a> TryFrom<&'a str> for Capability {
    type Error = &'a str;
    fn try_from(value: &'a str) -> Result<Capability, &'a str> {
        Ok(match value {
            "multi_ack" => Capability::MultiAck,
            "multi_ack_detailed" => Capability::MultiAckDetailed,
            "no-done" => Capability::NoDone,
            "thin-pack" => Capability::ThinPack,
            "side-band" => Capability::SideBand,
            "side-band-64k" => Capability::SideBand64K,
            "ofs-delta" => Capability::OfsDelta,
            "agent" => Capability::Agent,
            "object-format" => Capability::ObjectFormat,
            "symref" => Capability::SymRef,
            "shallow" => Capability::Shallow,
            "deepen-since" => Capability::DeepenSince,
            "deepen-not" => Capability::DeepenNot,
            "deepen-relative" => Capability::DeepenRelative,
            "no-progress" => Capability::NoProgress,
            "include-tag" => Capability::IncludeTag,
            "report-status" => Capability::ReportStatus,
            "report-status-v2" => Capability::ReportStatusV2,
            "delete-refs" => Capability::DeleteRefs,
            "quiet" => Capability::Quiet,
            "atomic" => Capability::Atomic,
            "push-options" => Capability::PushOptions,
            "allow-tip-sha1-in-want" => Capability::AllowTipSha1InWant,
            "allow-reachable-sha1-in-want" => Capability::AllowReachableSha1InWant,
            "push-cert" => Capability::PushCert,
            "filter" => Capability::Filter,
            _ => return Err(value),
        })
    }
}

pub struct RefAdvertisement {
    caps: HashMap<Capability, Option<String>>,
    refs: HashMap<String, String>,
}

impl RefAdvertisement {
    pub async fn read_from<R>(reader: &mut R) -> io::Result<Self>
    where
        R: AsyncRead + Unpin,
    {
        let mut ret = Self {
            caps: HashMap::new(),
            refs: HashMap::new(),
        };
        loop {
            match ProtocolLine::read_from(reader, true).await? {
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
                                ret.caps.insert(cap, capvalue.map(ToOwned::to_owned));
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
                        ret.refs.insert(refname.to_string(), sha.to_string());
                    } else {
                        return Err(io::Error::new(io::ErrorKind::Other, "Malformed ref line"));
                    }
                }
            }
        }
        Ok(ret)
    }

    pub fn caps(&self) -> &HashMap<Capability, Option<String>> {
        &self.caps
    }

    pub fn refs(&self) -> &HashMap<String, String> {
        &self.refs
    }
}
