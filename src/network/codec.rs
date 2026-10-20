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
async fn frame<T: AsyncRead + Unpin + Send>(
    io: &mut T,
    ceiling: usize,
    accepts: impl Fn(u8) -> bool,
) -> io::Result<Vec<u8>> {
    let mut header = [0; 4];
    io.read_exact(&mut header).await?;
    let n = u32::from_be_bytes(header) as usize;
    if n == 0 || n > ceiling {
        return Err(invalid());
    }
    let mut tag = [0];
    io.read_exact(&mut tag).await?;
    if !accepts(tag[0]) {
        return Err(invalid());
    }
    let typed = match tag[0] {
        0 | 6 | 9 | 10 => 1,
        5 => 3,
        1 | 8 => 2050,
        2 => 1058,
        7 => 1042,
        11 => super::chain_sync::LOCATOR_FRAME,
        12 => super::chain_sync::HEADERS_FRAME,
        3 => FRAME,
        _ => return Err(invalid()),
    };
    if n > typed {
        return Err(invalid());
    }
    let mut bytes = vec![0; n];
    bytes[0] = tag[0];
    io.read_exact(&mut bytes[1..]).await?;
    let mut extra = [0];
    if io.read(&mut extra).await? != 0 {
        return Err(invalid());
    }
    Ok(bytes)
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
            .reserve(CONTROL * 6)
            .ok_or_else(exhausted)?;
        let bytes = frame(io, CONTROL, |tag| {
            if self.bulk {
                tag == 2
            } else {
                matches!(tag, 0 | 5 | 6 | 8 | 11)
            }
        })
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
        let bytes = frame(io, ceiling, |tag| request.message.accepts_tag(tag)).await?;
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
        io.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
        io.write_all(&bytes).await?;
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
        io.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
        io.write_all(&bytes).await
    }
}

#[cfg(test)]
#[path = "codec_tests.rs"]
mod tests;
