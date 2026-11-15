//! pcsc-rs_relay: lets clients without TLS (pcsc-rs_plain) reach a wss:// PC Status server.
//!
//!   pcsc-rs-relay --listen 0.0.0.0:8080 --upstream wss://status.example.com/server
//!
//! Each client gets its own upstream connection, and text/binary/close frames are passed
//! along in both directions (the PASS and the hostname travel inside the messages). The only
//! change: the client's `version` in `Hi` and `Sync` gets " relay" appended, so PC Status
//! shows "mimizk-rs v1.0.0 relay".

use std::{
    env,
    io::{ErrorKind, Read, Write},
    net::{TcpListener, TcpStream},
    process, thread,
    time::Duration,
};

use tungstenite::{Error, Message, WebSocket, stream::MaybeTlsStream};

/// For the client's WebSocket upgrade.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
/// One thread serves both directions by taking turns, so every read gives up quickly.
const POLL: Duration = Duration::from_millis(50);
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// The status field that holds the client's name and version ("mimizk-rs v1.0.0")
const VERSION_FIELD: &str = "version";
const RELAY_TAG: &str = " via relay";

fn usage() -> ! {
    eprintln!(
        "usage: pcsc-rs-relay --upstream <ws[s]://host/path> [--listen <addr:port>]\n\
         (or RELAY_UPSTREAM / RELAY_LISTEN; default listen 0.0.0.0:8080)"
    );
    process::exit(2);
}

fn main() {
    let mut listen = env::var("RELAY_LISTEN").unwrap_or_else(|_| "0.0.0.0:8080".to_owned());
    let mut upstream = env::var("RELAY_UPSTREAM").ok();
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen" => listen = args.next().unwrap_or_else(|| usage()),
            "--upstream" => upstream = Some(args.next().unwrap_or_else(|| usage())),
            _ => usage(),
        }
    }
    let Some(upstream) = upstream else { usage() };
    if !upstream.starts_with("wss://") && !upstream.starts_with("ws://") {
        usage();
    }

    let listener = TcpListener::bind(&listen).unwrap_or_else(|e| {
        eprintln!("cannot listen on {listen}: {e}");
        process::exit(1);
    });
    println!("relay: {listen} -> {upstream}");
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let upstream = upstream.clone();
                thread::spawn(move || handle(stream, &upstream));
            }
            Err(e) => eprintln!("accept failed: {e}"),
        }
    }
}

fn handle(stream: TcpStream, upstream: &str) {
    let peer = stream
        .peer_addr()
        .map_or_else(|_| "?".to_owned(), |a| a.to_string());
    println!("{peer}: connected");
    match relay(stream, upstream) {
        Ok(()) => println!("{peer}: closed"),
        Err(e) => eprintln!("{peer}: {e}"),
    }
}

fn relay(stream: TcpStream, upstream: &str) -> Result<(), Box<dyn std::error::Error>> {
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(HANDSHAKE_TIMEOUT))?;
    stream.set_write_timeout(Some(WRITE_TIMEOUT))?;
    let mut down = tungstenite::accept(stream).map_err(|e| e.to_string())?;
    let (mut up, _) = tungstenite::connect(upstream)?;

    down.get_ref().set_read_timeout(Some(POLL))?;
    set_timeouts(up.get_mut())?;

    loop {
        if !step(&mut down, &mut up, Some(tag_relay))? || !step(&mut up, &mut down, None)? {
            return Ok(());
        }
    }
}

/// Timeouts on the TCP socket under the (maybe TLS) upstream stream.
fn set_timeouts(stream: &mut MaybeTlsStream<TcpStream>) -> std::io::Result<()> {
    let tcp = match stream {
        MaybeTlsStream::Plain(s) => &*s,
        MaybeTlsStream::NativeTls(s) => s.get_ref(),
        _ => return Ok(()),
    };
    tcp.set_read_timeout(Some(POLL))?;
    tcp.set_write_timeout(Some(WRITE_TIMEOUT))
}

/// Appends " relay" to the client's version in a `Hi` or `Sync` message, so PC Status shows
/// "mimizk-rs v1.0.0 relay". Anything else is passed along untouched.
///
/// `Hi` is `{"type":"Hi","data":{"data":<status>,"pass":…}}`, `Sync` is
/// `{"type":"Sync","data":<status>}`.
fn tag_relay(text: &str) -> String {
    let Ok(mut value) = serde_json::from_str::<serde_json::Value>(text) else {
        return text.to_owned();
    };
    let kind = value["type"].as_str().map(str::to_owned);
    let status = match kind.as_deref() {
        Some("Hi") => value.pointer_mut("/data/data"),
        Some("Sync") => value.pointer_mut("/data"),
        _ => None,
    };
    if let Some(serde_json::Value::Object(status)) = status
        && let Some(serde_json::Value::String(version)) = status.get_mut(VERSION_FIELD)
        && !version.ends_with(RELAY_TAG)
    {
        version.push_str(RELAY_TAG);
    }
    value.to_string()
}

/// A timed-out or interrupted call: the frame is kept by tungstenite, try again next turn.
fn is_retry(e: &Error) -> bool {
    matches!(
        e,
        Error::Io(io) if matches!(
            io.kind(),
            ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::Interrupted
        )
    )
}

fn send<S: Read + Write>(ws: &mut WebSocket<S>, msg: Message) -> tungstenite::Result<()> {
    match ws.send(msg) {
        Err(e) if is_retry(&e) => Ok(()),
        result => result,
    }
}

fn flush<S: Read + Write>(ws: &mut WebSocket<S>) -> tungstenite::Result<()> {
    match ws.flush() {
        Err(e) if is_retry(&e) => Ok(()),
        result => result,
    }
}

/// Moves one message from `from` to `to` (text goes through `rewrite` when given).
/// `false` when the connection is over.
fn step<A: Read + Write, B: Read + Write>(
    from: &mut WebSocket<A>,
    to: &mut WebSocket<B>,
    rewrite: Option<fn(&str) -> String>,
) -> tungstenite::Result<bool> {
    // Finish earlier writes (and the pongs tungstenite queued)
    flush(to)?;
    flush(from)?;
    match from.read() {
        Ok(Message::Text(text)) => {
            let text = rewrite.map_or_else(|| text.to_string(), |f| f(&text));
            send(to, Message::text(text))?;
        }
        Ok(msg @ Message::Binary(_)) => send(to, msg)?,
        Ok(msg @ Message::Close(_)) => {
            let _ = send(to, msg);
            return Ok(false);
        }
        // Ping and Pong: tungstenite answers pings on its own
        Ok(_) => {}
        Err(Error::ConnectionClosed | Error::AlreadyClosed) => return Ok(false),
        Err(e) if is_retry(&e) => {}
        Err(e) => return Err(e),
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::tag_relay;

    #[test]
    fn tags_hi_and_sync() {
        let hi = tag_relay(
            r#"{"type":"Hi","data":{"data":{"hostname":"h","version":"mimizk-rs v1.0.0"},"pass":"p"}}"#,
        );
        let hi: serde_json::Value = serde_json::from_str(&hi).unwrap();
        assert_eq!(hi["data"]["data"]["version"], "mimizk-rs v1.0.0 relay");
        assert_eq!(hi["data"]["pass"], "p");

        let sync = tag_relay(r#"{"type":"Sync","data":{"version":"mimizk-rs v1.0.0"}}"#);
        let sync: serde_json::Value = serde_json::from_str(&sync).unwrap();
        assert_eq!(sync["data"]["version"], "mimizk-rs v1.0.0 relay");
    }

    #[test]
    fn tags_once_and_leaves_the_rest() {
        let tagged = r#"{"type":"Sync","data":{"version":"mimizk-rs v1.0.0 relay"}}"#;
        assert!(tag_relay(tagged).contains("v1.0.0 relay\""));
        assert!(!tag_relay(tagged).contains("relay relay"));
        // Not JSON, or not a status message
        assert_eq!(tag_relay("hello"), "hello");
        let other = r#"{"type":"Close"}"#;
        assert_eq!(tag_relay(other), other);
    }
}