use super::{Capability, ProtocolLine, NULLSHA};
use std::collections::{HashMap, HashSet};
use tokio::io::{self, AsyncWrite};

pub async fn send_refchange<W>(
    writer: &mut W,
    existing: &HashMap<String, String>,
    target: &HashMap<String, String>,
    caps: impl Iterator<Item = (Capability, Option<&str>)>,
) -> io::Result<bool>
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

    Ok(need_pack)
}
