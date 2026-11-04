//! The MLLP listener: every message received is written to the spool before
//! it is acknowledged, so an accepted message is never lost.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::formats::hl7v2::{Message, parse};

/// MLLP frame: `<VT>` message `<FS><CR>`.
pub const START: u8 = 0x0B;
pub const END: [u8; 2] = [0x1C, 0x0D];

/// The application acknowledgement for a message: `AA` accepted, `AE` error,
/// `AR` rejected. The sender's names are swapped into the receiver's place.
pub fn ack(message: &Message, code: &str, text: &str, now: &str) -> String {
    let trigger = message
        .message_type
        .split('^')
        .nth(1)
        .unwrap_or("")
        .to_string();
    let version = if message.version.is_empty() {
        "2.5.1"
    } else {
        &message.version
    };
    let msa_text = text.replace(['|', '\r', '\n'], " ");
    format!(
        "MSH|^~\\&|HIE|HIE|{}|{}|{now}||ACK^{trigger}^ACK|{}|P|{version}\rMSA|{code}|{}|{msa_text}\r",
        message.sending_application,
        message.sending_facility,
        format_args!("HIE{now}"),
        message.control_id,
    )
}

/// An acknowledgement for bytes that are not a message at all.
fn reject(reason: &str, now: &str) -> String {
    format!(
        "MSH|^~\\&|HIE|HIE|||{now}||ACK|HIE{now}|P|2.5.1\rMSA|AR||{}\r",
        reason.replace(['|', '\r', '\n'], " ")
    )
}

pub fn frame(message: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(message.len() + 3);
    out.push(START);
    out.extend_from_slice(message.as_bytes());
    out.extend_from_slice(&END);
    out
}

/// Listens for MLLP connections and writes every message to `spool` as its own
/// file, acknowledging only once the file is on disk. A message that is not HL7
/// is answered `AR` and not kept.
pub async fn serve(feed_id: String, listener: TcpListener, spool: PathBuf) {
    let seq = std::sync::Arc::new(AtomicU64::new(0));
    loop {
        let (socket, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                tracing::error!(target: "hie::hl7", feed = %feed_id, "MLLP accept failed: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        };
        tracing::info!(target: "hie::hl7", feed = %feed_id, %peer, "MLLP connection");
        let (feed_id, spool, seq) = (feed_id.clone(), spool.clone(), seq.clone());
        tokio::spawn(async move {
            if let Err(e) = connection(&feed_id, socket, &spool, &seq).await {
                tracing::warn!(target: "hie::hl7", feed = %feed_id, %peer, "MLLP connection ended: {e:#}");
            }
        });
    }
}

async fn connection(
    feed_id: &str,
    mut socket: tokio::net::TcpStream,
    spool: &Path,
    seq: &AtomicU64,
) -> Result<()> {
    let mut buffer: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let n = socket.read(&mut chunk).await.context("reading")?;
        if n == 0 {
            return Ok(());
        }
        buffer.extend_from_slice(&chunk[..n]);
        while let Some(start) = buffer.iter().position(|b| *b == START) {
            let Some(end) = buffer[start..]
                .windows(2)
                .position(|w| w == END)
                .map(|p| start + p)
            else {
                break;
            };
            let body = buffer[start + 1..end].to_vec();
            buffer.drain(..end + 2);
            let reply = receive(feed_id, &body, spool, seq)?;
            socket
                .write_all(&frame(&reply))
                .await
                .context("writing the acknowledgement")?;
        }
        if buffer.len() > 16 * 1024 * 1024 {
            bail!("16 MB without a frame end; closing");
        }
    }
}

/// Spools one message and builds its acknowledgement.
fn receive(feed_id: &str, body: &[u8], spool: &Path, seq: &AtomicU64) -> Result<String> {
    let now = chrono::Utc::now().format("%Y%m%d%H%M%S%.3f").to_string();
    let text = match std::str::from_utf8(body) {
        Ok(t) => t,
        Err(_) => {
            tracing::warn!(target: "hie::hl7", feed = feed_id, "MLLP message is not UTF-8; rejected");
            return Ok(reject("message is not UTF-8", &now));
        }
    };
    let message = match parse(text) {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(target: "hie::hl7", feed = feed_id, "MLLP message rejected: {e:#}");
            return Ok(reject(&format!("{e:#}"), &now));
        }
    };
    let n = seq.fetch_add(1, Ordering::SeqCst);
    let safe_id: String = message
        .control_id
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .take(40)
        .collect();
    let name = format!("{now}-{n:04}-{safe_id}.hl7");
    std::fs::create_dir_all(spool).with_context(|| format!("creating {}", spool.display()))?;
    let path = spool.join(&name);
    let tmp = spool.join(format!(".{name}.part"));
    let store = || -> std::io::Result<()> {
        use std::io::Write;
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(message.text.as_bytes())?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&tmp, &path)
    };
    match store() {
        Ok(()) => {
            tracing::info!(target: "hie::hl7", feed = feed_id, control_id = %message.control_id, kind = %message.message_type, file = %name, "message spooled");
            Ok(ack(&message, "AA", "", &now))
        }
        Err(e) => {
            let _ = std::fs::remove_file(&tmp);
            tracing::error!(target: "hie::hl7", feed = feed_id, "cannot spool a message: {e}");
            // Not stored, so not accepted: the sender keeps it and retries.
            Ok(ack(
                &message,
                "AE",
                &format!("HIE could not store the message: {e}"),
                &now,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ADT: &str = "MSH|^~\\&|LAB|NORTH|HIE|HFS|20261021120000||ADT^A01|MSG0001|P|2.5.1\rEVN|A01|20261021120000\rPID|1||12345^^^NORTH^MR||Doe^Jane^M||19800101|F\r";

    #[test]
    fn the_ack_answers_the_sender() {
        let m = parse(ADT).unwrap();
        let a = ack(&m, "AA", "", "20261021120001");
        assert!(a.starts_with(
            "MSH|^~\\&|HIE|HIE|LAB|NORTH|20261021120001||ACK^A01^ACK|HIE20261021120001|P|2.5.1\r"
        ));
        assert!(a.ends_with("MSA|AA|MSG0001|\r"));
        let framed = frame(&a);
        assert_eq!(framed[0], START);
        assert_eq!(&framed[framed.len() - 2..], &END);
    }
}
