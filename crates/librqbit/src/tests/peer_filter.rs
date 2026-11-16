//! `SessionOptions::peer_filter` is asked about the peers others name (a tracker, another peer
//! through PEX, an incoming peer about itself), and only the ones it lets through are
//! connected to.

use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Context;
use librqbit_core::hash_id::Id20;
use peer_binary_protocol::{
    Handshake, Message,
    extended::{
        ExtendedMessage, PeerExtendedMessageIds, handshake::ExtendedHandshake, ut_pex::UtPex,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, ListenerOptions, PeerFilter, Session,
    SessionOptions, create_torrent,
    spawn_utils::BlockingSpawner,
    tests::test_util::{create_default_random_dir_with_torrents, setup_test_logging, wait_until},
};

type Asked = Arc<Mutex<Vec<SocketAddr>>>;

/// A filter that answers `allow`, and the addresses it was asked about.
fn filter(allow: bool) -> (PeerFilter, Asked) {
    let asked = Asked::default();
    let seen = asked.clone();
    let filter: PeerFilter = Arc::new(move |addr| {
        seen.lock().unwrap().push(addr);
        allow
    });
    (filter, asked)
}

/// A session with no way to find peers of its own (listening on loopback with `listen`),
/// downloading a torrent with `trackers` and `initial_peers` into an empty folder. With it, the
/// torrent's hash.
async fn download(
    filter: PeerFilter,
    listen: bool,
    trackers: Vec<String>,
    initial_peers: Vec<SocketAddr>,
) -> anyhow::Result<(Arc<Session>, Id20, tempfile::TempDir)> {
    let files = create_default_random_dir_with_torrents(1, 8192, Some("test_peer_filter"));
    let torrent = create_torrent(
        files.path(),
        CreateTorrentOptions {
            trackers,
            piece_length: Some(1024),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await?;
    let output = tempfile::tempdir()?;
    let session = Session::new_with_opts(
        output.path().into(),
        SessionOptions {
            dht: None,
            persistence: None,
            listen: listen.then(|| ListenerOptions {
                listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                ..Default::default()
            }),
            disable_local_service_discovery: true,
            peer_filter: Some(filter),
            ..Default::default()
        },
    )
    .await?;
    session
        .add_torrent(
            AddTorrent::from_bytes(torrent.as_bytes()?),
            Some(AddTorrentOptions {
                initial_peers: Some(initial_peers),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("the torrent was added")?
        .wait_until_initialized()
        .await?;
    Ok((session, torrent.info_hash(), output))
}

/// Waits until the filter was asked about `addr`, and about nothing else.
async fn asked_about(asked: &Asked, addr: SocketAddr) -> anyhow::Result<()> {
    wait_until(
        || {
            let asked = asked.lock().unwrap();
            anyhow::ensure!(
                !asked.is_empty() && asked.iter().all(|a| *a == addr),
                "the filter was asked about {asked:?}, not {addr}"
            );
            Ok(())
        },
        Duration::from_secs(10),
    )
    .await
}

/// Once the filter was asked about `named` (and nothing else): whether `named` was connected to.
async fn connected_after_asking(
    named: &TcpListener,
    asked: &Asked,
    allow: bool,
) -> anyhow::Result<bool> {
    asked_about(asked, named.local_addr()?).await?;
    // A refused peer is never connected to: a second is long for a connection on loopback.
    let wait = Duration::from_secs(if allow { 10 } else { 1 });
    Ok(timeout(wait, named.accept()).await.is_ok())
}

/// A tracker names one peer; the session's filter answers `allow`. Whether that peer was
/// connected to.
async fn a_tracker_names_a_peer(allow: bool) -> anyhow::Result<bool> {
    setup_test_logging();
    let named = TcpListener::bind("127.0.0.1:0").await?;
    let SocketAddr::V4(named_addr) = named.local_addr()? else {
        anyhow::bail!("bound to IPv4");
    };
    let tracker = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/announce", tracker.local_addr()?);
    // Every announce is answered with that one peer: `peers`, compact, is 6 bytes a peer, its
    // IPv4 address then its port.
    let mut body = b"d8:intervali60e5:peers6:".to_vec();
    body.extend_from_slice(&named_addr.ip().octets());
    body.extend_from_slice(&named_addr.port().to_be_bytes());
    body.push(b'e');
    tokio::spawn(async move {
        loop {
            let (mut s, _) = tracker.accept().await?;
            let mut req = vec![0u8; 4096];
            let _ = s.read(&mut req).await?;
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            s.write_all(head.as_bytes()).await?;
            s.write_all(&body).await?;
        }
        #[allow(unreachable_code)]
        anyhow::Ok(())
    });

    let (filter, asked) = filter(allow);
    let (session, _, _output) = download(filter, false, vec![url], Vec::new()).await?;
    let connected = connected_after_asking(&named, &asked, allow).await?;
    session.stop().await;
    Ok(connected)
}

/// A peer the session connects to (one of its `initial_peers`, which the filter isn't asked
/// about) names another one through PEX; the session's filter answers `allow`. Whether the
/// named peer was connected to.
async fn a_peer_names_a_peer(allow: bool) -> anyhow::Result<bool> {
    setup_test_logging();
    let named = TcpListener::bind("127.0.0.1:0").await?;
    let named_addr = named.local_addr()?;
    let peer = TcpListener::bind("127.0.0.1:0").await?;

    let (filter, asked) = filter(allow);
    let (session, _, _output) =
        download(filter, false, Vec::new(), vec![peer.local_addr()?]).await?;

    // The peer: the handshake, its extended handshake (it speaks PEX), then the PEX message.
    let (mut s, _) = timeout(Duration::from_secs(10), peer.accept()).await??;
    let mut theirs = [0u8; 68];
    s.read_exact(&mut theirs).await?;
    let (theirs, _) = Handshake::deserialize(&theirs)?;
    let mut buf = vec![0u8; 1024];
    let n = Handshake::new(theirs.info_hash, Id20::new([7; 20])).serialize_unchecked_len(&mut buf);
    s.write_all(&buf[..n]).await?;
    // The session is this same program: the message ids it gave are these.
    let ids = PeerExtendedMessageIds::my;
    let n = Message::Extended(ExtendedMessage::Handshake(ExtendedHandshake::new()))
        .serialize(&mut buf, &ids)?;
    s.write_all(&buf[..n]).await?;
    let pex = UtPex::from_addrs([named_addr].into_iter(), std::iter::empty());
    let n =
        Message::Extended(ExtendedMessage::UtPex(pex.as_borrowed())).serialize(&mut buf, &ids)?;
    s.write_all(&buf[..n]).await?;

    let connected = connected_after_asking(&named, &asked, allow).await?;
    session.stop().await;
    Ok(connected)
}

#[tokio::test]
async fn a_peer_a_tracker_names_is_connected_to_when_the_filter_lets_it() -> anyhow::Result<()> {
    assert!(a_tracker_names_a_peer(true).await?);
    Ok(())
}

#[tokio::test]
async fn a_peer_a_tracker_names_is_left_alone_when_the_filter_refuses_it() -> anyhow::Result<()> {
    assert!(!a_tracker_names_a_peer(false).await?);
    Ok(())
}

#[tokio::test]
async fn a_peer_another_peer_names_is_connected_to_when_the_filter_lets_it() -> anyhow::Result<()> {
    assert!(a_peer_names_a_peer(true).await?);
    Ok(())
}

#[tokio::test]
async fn a_peer_another_peer_names_is_left_alone_when_the_filter_refuses_it() -> anyhow::Result<()>
{
    assert!(!a_peer_names_a_peer(false).await?);
    Ok(())
}

/// An incoming peer gives the port it listens on in its extended handshake: the filter is
/// asked about that port at the address it connected from, the one the session would use.
#[tokio::test]
async fn the_filter_is_asked_where_an_incoming_peer_says_it_listens() -> anyhow::Result<()> {
    setup_test_logging();
    let (filter, asked) = filter(false);
    let (session, info_hash, _output) = download(filter, true, Vec::new(), Vec::new()).await?;

    let listens = session.listen_addr().context("the session listens")?;
    let mut s = TcpStream::connect(listens).await?;
    let mut buf = vec![0u8; 1024];
    let n = Handshake::new(info_hash, Id20::new([7; 20])).serialize_unchecked_len(&mut buf);
    s.write_all(&buf[..n]).await?;
    let mut theirs = [0u8; 68];
    s.read_exact(&mut theirs).await?;
    let hs = ExtendedHandshake {
        p: Some(6881),
        ..ExtendedHandshake::new()
    };
    let n = Message::Extended(ExtendedMessage::Handshake(hs))
        .serialize(&mut buf, &PeerExtendedMessageIds::my)?;
    s.write_all(&buf[..n]).await?;

    asked_about(&asked, SocketAddr::new(s.local_addr()?.ip(), 6881)).await?;
    session.stop().await;
    Ok(())
}
