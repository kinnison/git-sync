use super::{Capability, ProtocolLine, NULLSHA};
use std::collections::{HashMap, HashSet};
use tokio::io::{self, AsyncWrite};

pub enum SendActivity {
    Nothing,
    Deleting,
    Sending,
}

pub const EMPTY_PACK: &[u8] = &[
    b'P', b'A', b'C', b'K', // Pack header starts 'PACK'
    0, 0, 0, 2, // Then we get the version number (2)
    0, 0, 0, 0, // Then we get the number of objects in the pack (0)
    // Finally there's a SHA1 checksum of the full pack file
    0x02, 0x9d, 0x08, 0x82, 0x3b, 0xd8, 0xa8, 0xea, 0xb5, 0x10, 0xad, 0x6a, 0xc7, 0x5c, 0x82, 0x3c,
    0xfd, 0x3e, 0xd3, 0x1e,
];

pub async fn send_refchange<W>(
    writer: &mut W,
    existing: &HashMap<String, String>,
    target: &HashMap<String, String>,
    caps: impl Iterator<Item = (Capability, Option<&str>)>,
) -> io::Result<SendActivity>
where
    W: AsyncWrite + Unpin,
{
    let mut capstring = {
        let mut ret = String::new();
        for (cap, val) in caps {
            if ret.is_empty() {
                ret.push('\0');
            } else {
                ret.push(' ');
            }
            ret.push_str(cap.as_str());
            if let Some(val) = val {
                ret.push('=');
                ret.push_str(val);
            }
        }
        Some(ret)
    };
    // The refchange set we want to transmit comes down to tuples of oldsha newsha refname
    // where oldsha is NULLSHA if we're creating something new, and newsha is NULLSHA if
    // we're deleting something old.  Where the shas are the same there's no need to
    // transmit the ref.
    // In addition, existing may contain peeled refs, which we don't want to think about,
    // so we filter those out
    let all_refs: HashSet<_> = existing
        .keys()
        .chain(target.keys())
        .filter(|k| k.starts_with("refs/") && !k.ends_with("^{}"))
        .collect();

    // For all the refs, write the change (if any) out
    let mut need_pack = false;
    for refname in all_refs {
        let oldsha = existing.get(refname).map(String::as_str).unwrap_or(NULLSHA);
        let newsha = target.get(refname).map(String::as_str).unwrap_or(NULLSHA);
        if oldsha != newsha {
            if newsha != NULLSHA {
                need_pack = true;
            }
            // Worth sending the command
            let cmd = if let Some(caps) = capstring.take() {
                format!("{} {} {}{}\n", oldsha, newsha, refname, caps)
            } else {
                format!("{} {} {}\n", oldsha, newsha, refname)
            };
            ProtocolLine::write_str(writer, cmd).await?;
        }
    }
    // We terminate the refset change with a flush
    ProtocolLine::Flush.write_to(writer).await?;

    Ok(match (capstring.is_none(), need_pack) {
        (false, _) => SendActivity::Nothing,
        (true, false) => SendActivity::Deleting,
        (true, true) => SendActivity::Sending,
    })
}
