use super::{
    budget::{Budgets, Ticket},
    wire::{Message, FRAME},
};
use libp2p::{
    futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    request_response as rpc, StreamProtocol,
};
use std::{io, time::Duration};
pub(super) const CONTROL: usize = 4096;
pub(super) const ENVELOPE: usize = 2 * (1 + u8::MAX as usize);
pub(super) const DEADLINE: Duration = Duration::from_secs(40);

#[derive(Debug)]
pub(super) struct Packet {
    message: Message,
    _ticket: Ticket,
    pub(super) origin: Option<libp2p::swarm::ConnectionId>,
}
impl Packet {
    pub(super) fn message(&self) -> Message {
        self.message.clone()
    }
    pub(super) fn new(message: Message, ticket: Ticket) -> Self {
        Self {
            message,
            _ticket: ticket,
            origin: None,
        }
    }
}
#[derive(Debug)]
pub(super) struct Request {
    pub(super) message: Message,
    pub(super) ticket: Ticket,
}
pub(super) struct Codec {
    pub(super) budgets: Budgets,
    pub(super) bulk: bool,
    pub(super) expected: Option<Request>,
}
impl Clone for Codec {
    fn clone(&self) -> Self {
        Self {
            budgets: self.budgets.clone(),
            bulk: self.bulk,
            expected: None,
        }
    }
}
#[derive(Debug)]
struct RemoteProtocolError;
impl std::fmt::Display for RemoteProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("malformed peer RPC data")
    }
}
impl std::error::Error for RemoteProtocolError {}

fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, RemoteProtocolError)
}
pub(super) fn is_remote_protocol_error(error: &io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|source| source.is::<RemoteProtocolError>())
}
fn exhausted() -> io::Error {
    io::ErrorKind::WouldBlock.into()
}
#[derive(Debug, thiserror::Error)]
#[error("incompatible versions: local {local} (minimum {local_minimum}), peer {peer} (minimum {peer_minimum})")]
struct IncompatibleVersion {
    local: semver::Version,
    local_minimum: semver::Version,
    peer: semver::Version,
    peer_minimum: semver::Version,
}

pub(super) fn is_incompatible_version(error: &io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|source| source.is::<IncompatibleVersion>())
}

async fn read_version<T: AsyncRead + Unpin + Send>(
    io: &mut T,
    remaining: &mut usize,
) -> io::Result<semver::Version> {
    let mut byte = [0];
    io.read_exact(&mut byte).await?;
    let length = byte[0] as usize;
    *remaining = remaining
        .checked_sub(1 + length)
        .filter(|n| *n > 0)
        .ok_or_else(invalid)?;
    if length == 0 {
        return Err(invalid());
    }
    let mut bytes = [0; u8::MAX as usize];
    io.read_exact(&mut bytes[..length]).await?;
    let text = std::str::from_utf8(&bytes[..length]).map_err(|_| invalid())?;
    semver::Version::parse(text).map_err(|_| invalid())
}

async fn frame<T: AsyncRead + Unpin + Send>(
    io: &mut T,
    ceiling: usize,
    local: &str,
    minimum: &str,
    accepts: impl Fn(u8) -> bool,
) -> io::Result<Vec<u8>> {
    let mut header = [0; 4];
    io.read_exact(&mut header).await?;
    let mut n = u32::from_be_bytes(header) as usize;
    let peer = read_version(io, &mut n).await?;
    let peer_minimum = read_version(io, &mut n).await?;
    let local = semver::Version::parse(local).expect("compiled release version");
    let local_minimum = semver::Version::parse(minimum).expect("compiled minimum peer version");
    if peer.cmp_precedence(&local_minimum).is_lt() || local.cmp_precedence(&peer_minimum).is_lt() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            IncompatibleVersion {
                local,
                local_minimum,
                peer,
                peer_minimum,
            },
        ));
    }
    if n > ceiling {
        return Err(invalid());
    }
    let mut byte = [0];
    io.read_exact(&mut byte).await?;
    let tag = byte[0];
    if !accepts(tag) {
        return Err(invalid());
    }
    let typed = match tag {
        0 | 6 | 9 | 10 => 1,
        5 => 3,
        1 | 8 => 2050,
        2 => 1058,
        7 => 1042,
        3 => FRAME,
        _ => return Err(invalid()),
    };
    if n > typed {
        return Err(invalid());
    }
    let mut bytes = vec![0; n];
    bytes[0] = tag;
    io.read_exact(&mut bytes[1..]).await?;
    let mut extra = [0];
    if io.read(&mut extra).await? != 0 {
        return Err(invalid());
    }
    Ok(bytes)
}
async fn write_frame<T: AsyncWrite + Unpin + Send>(
    io: &mut T,
    bytes: &[u8],
    version: &str,
    minimum: &str,
) -> io::Result<()> {
    let versions = [version, minimum];
    if versions
        .iter()
        .any(|v| v.is_empty() || v.len() > u8::MAX as usize)
    {
        return Err(io::Error::other("local version length"));
    }
    let size = bytes.len() + versions.iter().map(|v| 1 + v.len()).sum::<usize>();
    io.write_all(&(size as u32).to_be_bytes()).await?;
    for version in versions {
        io.write_all(&[version.len() as u8]).await?;
        io.write_all(version.as_bytes()).await?;
    }
    io.write_all(bytes).await
}

impl rpc::Codec for Codec {
    type Protocol = StreamProtocol;
    type Request = Request;
    type Response = Packet;
    async fn read_request<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let ticket = self
            .budgets
            .control
            .reserve((CONTROL + ENVELOPE) * 6)
            .ok_or_else(exhausted)?;
        let bytes = frame(
            io,
            CONTROL,
            crate::VERSION,
            crate::MIN_PEER_VERSION,
            |tag| {
                if self.bulk {
                    tag == 2
                } else {
                    matches!(tag, 0 | 5 | 6 | 8)
                }
            },
        )
        .await?;
        let message = Message::decode(&bytes).map_err(|_| invalid())?;
        message.response_ceiling().map_err(|_| invalid())?;
        Ok(Request { message, ticket })
    }
    async fn read_response<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<Packet>
    where
        T: AsyncRead + Unpin + Send,
    {
        let request = self
            .expected
            .take()
            .ok_or_else(|| io::Error::other("missing local RPC expectation"))?;
        let ceiling = request
            .message
            .response_ceiling()
            .map_err(|error| io::Error::other(error.to_string()))?;
        let bytes = frame(
            io,
            ceiling,
            crate::VERSION,
            crate::MIN_PEER_VERSION,
            |tag| request.message.accepts_tag(tag),
        )
        .await?;
        let message = Message::decode(&bytes).map_err(|_| invalid())?;
        message
            .validate_reply(&request.message)
            .map_err(|_| invalid())?;
        Ok(Packet::new(message, request.ticket))
    }
    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        request: Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let bytes = request
            .message
            .encode()
            .map_err(|error| io::Error::other(error.to_string()))?;
        if bytes.len() > CONTROL || request.message.is_bulk() != self.bulk {
            return Err(io::Error::other("invalid local RPC request"));
        }
        write_frame(io, &bytes, crate::VERSION, crate::MIN_PEER_VERSION).await?;
        self.expected = Some(request);
        Ok(())
    }
    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        response: Packet,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let bytes = response
            .message
            .encode()
            .map_err(|error| io::Error::other(error.to_string()))?;
        write_frame(io, &bytes, crate::VERSION, crate::MIN_PEER_VERSION).await
    }
}

#[cfg(test)]
#[path = "codec_tests.rs"]
mod tests;
