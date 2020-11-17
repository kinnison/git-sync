use tokio::io;
use tokio::prelude::*;
use tokio::process::{ChildStdin, ChildStdout, Command};
use tokio::task::JoinHandle;

use std::collections::HashSet;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::process::Stdio;

use git_sync::*;

use structopt::StructOpt;

#[derive(StructOpt)]
struct Cli {
    /// If set, the source is an SSH server
    #[structopt(long = "source-server", short = "s")]
    source_server: Option<String>,
    /// If set, the destination is an SSH server
    #[structopt(long = "dest-server", short = "d")]
    dest_server: Option<String>,
    /// The source repository
    source: PathBuf,
    /// The target repository
    target: PathBuf,
}
struct Service {
    handle: JoinHandle<Result<ExitStatus, io::Error>>,
    reader: ChildStdout,
    writer: ChildStdin,
}

impl Service {
    pub async fn launch<P>(service: &str, path: P) -> Result<Service, io::Error>
    where
        P: AsRef<Path>,
    {
        let mut child = Command::new(service)
            .arg(path.as_ref())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;

        let reader = child.stdout.take().expect("Did not get a stdout handle?");
        let writer = child.stdin.take().expect("Did not get a stdin handle?");

        let handle = tokio::spawn(async move { child.wait().await });

        Ok(Service {
            handle,
            reader,
            writer,
        })
    }

    pub async fn launch_ssh<P>(server: &str, service: &str, path: P) -> Result<Service, io::Error>
    where
        P: AsRef<Path>,
    {
        let mut child = Command::new("ssh")
            .arg(server)
            .arg(service)
            .arg(path.as_ref())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let reader = child.stdout.take().expect("Did not get a stdout handle?");
        let writer = child.stdin.take().expect("Did not get a stdin handle?");

        let handle = tokio::spawn(async move { child.wait().await });

        Ok(Service {
            handle,
            reader,
            writer,
        })
    }

    pub async fn die(self) -> Result<ExitStatus, io::Error> {
        self.handle.await?
    }

    pub fn reader(&mut self) -> &mut ChildStdout {
        &mut self.reader
    }

    pub fn writer(&mut self) -> &mut ChildStdin {
        &mut self.writer
    }

    pub fn streams(&mut self) -> (&mut ChildStdout, &mut ChildStdin) {
        (&mut self.reader, &mut self.writer)
    }
}
#[tokio::main]
async fn main() -> io::Result<()> {
    let opts: Cli = Cli::from_args();

    println!("Connecting to services...");
    let mut upload_pack = if let Some(server) = opts.source_server.as_deref() {
        Service::launch_ssh(server, "git-upload-pack", &opts.source).await?
    } else {
        Service::launch("git-upload-pack", &opts.source).await?
    };
    let mut receive_pack = if let Some(server) = opts.dest_server.as_deref() {
        Service::launch_ssh(server, "git-receive-pack", &opts.target).await?
    } else {
        Service::launch("git-receive-pack", &opts.target).await?
    };

    println!("Reading ref set available in source...");
    let source_advert = RefAdvertisement::read_from(upload_pack.reader()).await?;

    for cap in source_advert.caps() {
        println!(
            "  Capability: {}{}{}",
            cap.0.as_str(),
            if cap.1.is_some() { "=" } else { "" },
            cap.1.as_deref().unwrap_or("")
        );
    }

    println!("Reading ref set available in target...");
    let target_advert = RefAdvertisement::read_from(receive_pack.reader()).await?;

    for cap in target_advert.caps() {
        println!(
            "  Capability: {}{}{}",
            cap.0.as_str(),
            if cap.1.is_some() { "=" } else { "" },
            cap.1.as_deref().unwrap_or("")
        );
    }

    // Compute the set of things we want to fetch
    let wants: HashSet<_> = source_advert
        .refs()
        .iter()
        // filter out any peeled refs
        .filter(|(k, v)| !k.ends_with("^{}"))
        // filter out anything the target already has since we don't need to fetch that
        .filter(|(k, v)| target_advert.refs().values().find(|vv| v == vv).is_none())
        .map(|(_, v)| v.as_str())
        .collect();
    // And the set of things we already have
    let haves: HashSet<_> = target_advert
        .refs()
        .iter()
        .map(|(_, v)| v.as_str())
        .collect();
    let caps = &[
        (Capability::SideBand64K, None),
        (Capability::OfsDelta, None),
        (Capability::ThinPack, None),
        (Capability::Agent, Some("git_sync/0.1")),
    ];

    let expecting_pack_data = !wants.is_empty();
    let want_iter = wants.iter().copied();
    let have_iter = haves.iter().copied();
    let caps_iter = caps.iter().copied();
    // Finally send that out to the upload_pack service so it knows what to send to us.
    {
        let (reader, writer) = upload_pack.streams();
        println!("Sending pack request to uploader...");
        request_pack(reader, writer, want_iter, have_iter, caps_iter).await?;
    }

    let upload_caps = &[
        (Capability::ReportStatus, None),
        (Capability::Atomic, None),
        (Capability::SideBand64K, None),
        (Capability::Agent, Some("git_sync/0.1")),
    ];

    println!("Sending refset change to receiver...");
    // Now let's ensure that we're doing *something* to the target
    let expecting_to_send = send_refchange(
        receive_pack.writer(),
        target_advert.refs(),
        source_advert.refs(),
        upload_caps.iter().copied(),
    )
    .await?;

    // Now process the pack data...

    println!(
        "We do{} expect to transfer pack data",
        if expecting_pack_data { "" } else { " not" }
    );
    match expecting_to_send {
        SendActivity::Nothing => println!("We're doing nothing with receive-pack"),
        SendActivity::Deleting => {
            println!("We're not needing to send a pack, but we need to read a report")
        }
        SendActivity::Sending => {
            println!("We're definitely needing to send a pack to receive-pack")
        }
    };

    if expecting_pack_data {
        println!("Transferring pack data");
        loop {
            match ProtocolLine::read_from(upload_pack.reader(), false).await? {
                ProtocolLine::Data(cow) => match cow[0] {
                    1 => {
                        let data = &cow[1..];
                        // We need to send this content on to the receiver
                        receive_pack.writer().write_all(data).await?;
                    }
                    2 => print!("{}", String::from_utf8_lossy(&cow[1..])),
                    3 => eprint!("{}", String::from_utf8_lossy(&cow[1..])),
                    v => eprintln!("Received {} bytes on channel {}", cow.len() - 1, v),
                },
                ProtocolLine::Flush => break,
                l => {
                    println!("Encountered a {:?}", l);
                    break;
                }
            }
        }
    } else if matches!(expecting_to_send, SendActivity::Sending) {
        println!("We're expected to send a pack, but we have no objects to send");
        println!("Let's send the magical empty pack to the receive-pack service...");
        receive_pack.writer().write_all(EMPTY_PACK).await?;
    }

    println!("Shutting down upload-pack service");
    // Done with upload pack:
    upload_pack.die().await?;

    if !matches!(expecting_to_send, SendActivity::Nothing) {
        println!("Waiting for result from receive-pack service");
        // We've now sent the pack to the other end, let's read and report the receive pack output
        let mut rp_out = Vec::new();
        loop {
            match ProtocolLine::read_from(receive_pack.reader(), false).await? {
                ProtocolLine::Data(cow) => match cow[0] {
                    1 => {
                        let data = &cow[1..];
                        rp_out.extend_from_slice(data);
                    }
                    2 => print!("{}", String::from_utf8_lossy(&cow[1..])),
                    3 => eprint!("{}", String::from_utf8_lossy(&cow[1..])),
                    v => eprintln!("Received {} bytes on channel {}", cow.len() - 1, v),
                },
                ProtocolLine::Flush => break,
                l => {
                    println!("RPE: Encountered a {:?}", l);
                    break;
                }
            }
        }

        println!("Report from receive-pack is {} bytes:", rp_out.len());
        let mut cursor = Cursor::new(rp_out);
        loop {
            match ProtocolLine::read_from(&mut cursor, true).await? {
                ProtocolLine::Data(cow) => {
                    let s = String::from_utf8_lossy(&cow);
                    println!("remote: {}", s);
                }
                ProtocolLine::Flush => break,
                l => {
                    println!("RPE: Encountered encapsulated {:?}", l);
                    break;
                }
            }
        }
    }
    // We're done, let's close down our connections
    println!("Shutting down receive-pack service");
    receive_pack.die().await?;
    println!("Done");
    Ok(())
}
