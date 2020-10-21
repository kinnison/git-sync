use tokio::io;
use tokio::prelude::*;
use tokio::process::Command;

use std::process::Stdio;

use git_sync::ProtocolLine;

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut child = Command::new("strace")
        .args(&["-s1024", "-o", "/tmp/bleh", "git-upload-pack", "../rustup"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("Unable to spawn the command");
    let mut stdin = child.stdin.take().expect("Did not get a stdin handle?");
    let mut stdout = child.stdout.take().expect("Did not get a stdout handle?");
    let handle = tokio::spawn(async move {
        let status = child.wait().await.expect("child process failed?");
        println!("Exit status of child was: {}", status);
    });
    let mut capabilities: Vec<String> = Vec::new();
    let mut refs: Vec<(String, String)> = Vec::new();
    loop {
        match ProtocolLine::read_from(&mut stdout, true).await? {
            ProtocolLine::Flush => {
                break;
            }
            ProtocolLine::Delimiter => {
                println!("Delimiter?");
            }
            ProtocolLine::ResponseEnd => {
                println!("ResponseEnd?");
            }
            ProtocolLine::Data(cow) => {
                let s = String::from_utf8_lossy(&cow);
                if capabilities.is_empty() {
                    if let Some(caps) = s.split('\0').nth(1) {
                        caps.split(' ')
                            .for_each(|s| capabilities.push(s.to_string()));
                    }
                }
                if capabilities.is_empty() {
                    capabilities.push("none".into());
                }
                let refbit = s.split('\0').next().unwrap(); // Safe because there's always at least zero bytes so split should give "".
                if refbit.len() < 45
                /* SHA1 refs */
                {
                    println!("Bad ref line: {}", refbit);
                }
                let sha = &refbit[..40];
                let refname = &refbit[41..];
                refs.push((sha.to_string(), refname.to_string()));
            }
        }
    }

    for cap in &capabilities {
        //println!("Capability: {}", cap);
    }
    for (sha, refname) in &refs {
        //println!("{} => {}", refname, sha);
        if refname == "HEAD" {
            // Let's ask for the HEAD and claim to have nothing
            ProtocolLine::write_str(
                &mut stdin,
                format!(
                    "want {} side-band-64k ofs-delta thin-pack agent=git_sync/1.0\n",
                    sha
                ),
            )
            .await?;
        } else if refname == "refs/heads/master" {
            ProtocolLine::write_str(&mut stdin, format!("want {}\n", sha)).await?;
        }
    }

    ProtocolLine::Flush.write_to(&mut stdin).await?;

    ProtocolLine::write_str(&mut stdin, "done\n").await?;
    stdin.flush().await?;

    // We sent no `have`s so read the NAK

    match ProtocolLine::read_from(&mut stdout, true).await? {
        ProtocolLine::Data(cow) => {
            if cow != (b"NAK" as &[u8]) {
                eprintln!("Unexpected data packet: {}", String::from_utf8_lossy(&cow));
            }
        }
        l => {
            eprintln!("Unexpected packet: {:?}", l);
        }
    }

    // Now process the pack data...

    loop {
        match ProtocolLine::read_from(&mut stdout, false).await? {
            ProtocolLine::Data(cow) => match cow[0] {
                1 => println!("Received {} bytes of pack data", cow.len() - 1),
                2 => print!("{}", String::from_utf8_lossy(&cow[1..])),
                3 => eprint!("{}", String::from_utf8_lossy(&cow[1..])),
                v => eprintln!("Received {} bytes on channel {}", cow.len() - 1, v),
            },
            l => {
                println!("Encountered a {:?}", l);
                break;
            }
        }
    }

    handle.await.expect("Spawned task failed to run?");

    Ok(())
}
