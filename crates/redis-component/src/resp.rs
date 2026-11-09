//! RESP2, the wire protocol Redis speaks.
//!
//! A [`Frame`] is one value either side sends. The package's links speak
//! [`Resp`]: a [`Command`] in, a [`Frame`] out, and `AUTH` as a link opens
//! when the entry sets a password. The e2e fake does the reverse over the
//! same [`Frame`] and [`Decoder`].

use std::fmt;

use omnia_wasi_keyvalue::backend::{Codec, Error, Exchange, Settings};

const CRLF: &[u8] = b"\r\n";

/// The codec a link to Redis speaks.
///
/// An error reply (`-ERR …`) is the service refusing the command, surfaced
/// as [`Error::Other`] with the server's text. When the entry's settings
/// hold [`PASSWORD`](Self::PASSWORD), every link opens with `AUTH`.
#[derive(Debug, Default)]
pub struct Resp {
    decoder: Decoder,
}

impl Resp {
    /// The setting holding the password each link sends with `AUTH`.
    pub const PASSWORD: &'static str = "password";
}

impl Codec for Resp {
    type Reply = Frame;
    type Request = Command;

    fn encode(&mut self, request: Command, out: &mut Vec<u8>) {
        out.extend_from_slice(&request.0);
    }

    fn push(&mut self, bytes: &[u8]) {
        self.decoder.push(bytes);
    }

    fn next(&mut self) -> Result<Option<Frame>, Error> {
        match self.decoder.next_frame() {
            Ok(Some(Frame::Error(text))) => Err(Error::Other(text)),
            Ok(frame) => Ok(frame),
            Err(error) => Err(Error::Other(format!("the reply is not RESP2: {error}"))),
        }
    }

    async fn handshake(link: &mut impl Exchange<Self>, settings: &Settings) -> Result<(), Error> {
        let Some(password) = settings.get(Self::PASSWORD) else {
            return Ok(());
        };
        match link.round_trip(Command::new(["AUTH", password])).await? {
            Frame::Simple(_) => Ok(()),
            other => Err(Error::Other(format!("unexpected AUTH reply: {other:?}"))),
        }
    }
}

/// One command, framed: the array of bulk strings Redis expects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Command(Vec<u8>);

impl Command {
    /// A command from its arguments, the name first.
    pub fn new<I, A>(args: I) -> Self
    where
        I: IntoIterator<Item = A>,
        A: AsRef<[u8]>,
    {
        Self(Frame::command(args))
    }

    /// The framed bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// One RESP2 value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    /// A simple string, such as `+OK`.
    Simple(String),
    /// An error reply, such as `-ERR unknown command`.
    Error(String),
    /// A signed 64-bit integer.
    Integer(i64),
    /// A bulk string, or `None` for the null bulk string.
    Bulk(Option<Vec<u8>>),
    /// An array, or `None` for the null array.
    Array(Option<Vec<Self>>),
}

impl Frame {
    /// Encodes a command as the array of bulk strings Redis expects.
    pub fn command<I, A>(args: I) -> Vec<u8>
    where
        I: IntoIterator<Item = A>,
        A: AsRef<[u8]>,
    {
        let args: Vec<A> = args.into_iter().collect();
        let mut out = Vec::new();
        write_header(&mut out, b'*', args.len());
        for arg in &args {
            write_bulk(&mut out, arg.as_ref());
        }
        out
    }

    /// Encodes this frame.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_into(&mut out);
        out
    }

    /// Appends this frame's encoding to `out`.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        match self {
            Self::Simple(text) => write_line(out, b'+', text.as_bytes()),
            Self::Error(text) => write_line(out, b'-', text.as_bytes()),
            Self::Integer(value) => write_line(out, b':', value.to_string().as_bytes()),
            Self::Bulk(None) => out.extend_from_slice(b"$-1\r\n"),
            Self::Bulk(Some(bytes)) => write_bulk(out, bytes),
            Self::Array(None) => out.extend_from_slice(b"*-1\r\n"),
            Self::Array(Some(items)) => {
                write_header(out, b'*', items.len());
                for item in items {
                    item.encode_into(out);
                }
            }
        }
    }

    /// Parses one frame from the front of `input`.
    ///
    /// Returns the frame and the number of bytes it occupied, or `None` when
    /// `input` holds only part of a frame.
    ///
    /// # Errors
    ///
    /// Returns a [`ParseError`] when the bytes are not RESP2.
    pub fn parse(input: &[u8]) -> Result<Option<(Self, usize)>, ParseError> {
        let Some((&tag, rest)) = input.split_first() else {
            return Ok(None);
        };
        let Some(line) = split_line(rest) else {
            return Ok(None);
        };
        let header = 1 + line.len() + CRLF.len();
        let frame = match tag {
            b'+' => Self::Simple(utf8(line)?),
            b'-' => Self::Error(utf8(line)?),
            b':' => Self::Integer(integer(line)?),
            b'$' => {
                let Some(len) = length(line)? else {
                    return Ok(Some((Self::Bulk(None), header)));
                };
                let body = &input[header..];
                if body.len() < len + CRLF.len() {
                    return Ok(None);
                }
                if &body[len..len + CRLF.len()] != CRLF {
                    return Err(ParseError::Unterminated);
                }
                return Ok(Some((
                    Self::Bulk(Some(body[..len].to_vec())),
                    header + len + CRLF.len(),
                )));
            }
            b'*' => {
                let Some(len) = length(line)? else {
                    return Ok(Some((Self::Array(None), header)));
                };
                let mut items = Vec::with_capacity(len);
                let mut consumed = header;
                for _ in 0..len {
                    let Some((item, used)) = Self::parse(&input[consumed..])? else {
                        return Ok(None);
                    };
                    items.push(item);
                    consumed += used;
                }
                return Ok(Some((Self::Array(Some(items)), consumed)));
            }
            other => return Err(ParseError::UnknownType(other)),
        };
        Ok(Some((frame, header)))
    }

    /// This frame as a bulk-string reply, or the frame itself when it is not one.
    ///
    /// # Errors
    ///
    /// Returns the frame unchanged when it is not a bulk string.
    pub fn into_bulk(self) -> Result<Option<Vec<u8>>, Self> {
        match self {
            Self::Bulk(bytes) => Ok(bytes),
            other => Err(other),
        }
    }

    /// This frame as an integer reply, or the frame itself when it is not one.
    ///
    /// # Errors
    ///
    /// Returns the frame unchanged when it is not an integer.
    pub fn into_integer(self) -> Result<i64, Self> {
        match self {
            Self::Integer(value) => Ok(value),
            other => Err(other),
        }
    }

    /// This frame as a non-null array reply, or the frame itself when it is not one.
    ///
    /// # Errors
    ///
    /// Returns the frame unchanged when it is not a non-null array.
    pub fn into_array(self) -> Result<Vec<Self>, Self> {
        match self {
            Self::Array(Some(items)) => Ok(items),
            other => Err(other),
        }
    }
}

fn write_header(out: &mut Vec<u8>, tag: u8, len: usize) {
    write_line(out, tag, len.to_string().as_bytes());
}

fn write_line(out: &mut Vec<u8>, tag: u8, payload: &[u8]) {
    out.push(tag);
    out.extend_from_slice(payload);
    out.extend_from_slice(CRLF);
}

fn write_bulk(out: &mut Vec<u8>, bytes: &[u8]) {
    write_header(out, b'$', bytes.len());
    out.extend_from_slice(bytes);
    out.extend_from_slice(CRLF);
}

// the bytes before the first CRLF, or None while no CRLF has arrived
fn split_line(input: &[u8]) -> Option<&[u8]> {
    input.windows(CRLF.len()).position(|window| window == CRLF).map(|at| &input[..at])
}

fn utf8(line: &[u8]) -> Result<String, ParseError> {
    String::from_utf8(line.to_vec()).map_err(|_invalid| ParseError::NotUtf8)
}

fn integer(line: &[u8]) -> Result<i64, ParseError> {
    std::str::from_utf8(line).ok().and_then(|text| text.parse().ok()).ok_or(ParseError::BadInteger)
}

// a bulk or array length; None is the null marker `-1`
fn length(line: &[u8]) -> Result<Option<usize>, ParseError> {
    let value = integer(line)?;
    if value < 0 {
        return Ok(None);
    }
    usize::try_from(value).map(Some).map_err(|_negative| ParseError::BadInteger)
}

/// Why bytes could not be read as RESP2.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParseError {
    /// The first byte of a frame is not a RESP2 type tag.
    UnknownType(u8),
    /// A length or integer line is not a decimal number.
    BadInteger,
    /// A simple string or error line is not UTF-8.
    NotUtf8,
    /// A bulk string's bytes are not followed by CRLF.
    Unterminated,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownType(tag) => write!(f, "unknown RESP type tag {tag:#04x}"),
            Self::BadInteger => f.write_str("RESP integer is not a decimal number"),
            Self::NotUtf8 => f.write_str("RESP simple string is not UTF-8"),
            Self::Unterminated => f.write_str("RESP bulk string is not CRLF-terminated"),
        }
    }
}

impl std::error::Error for ParseError {}

/// Accumulates bytes read from a connection and yields complete frames.
#[derive(Debug, Default)]
pub struct Decoder {
    buffer: Vec<u8>,
}

impl Decoder {
    /// An empty decoder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends bytes read from the connection.
    pub fn push(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }

    /// Takes the next complete frame, or `None` until more bytes arrive.
    ///
    /// # Errors
    ///
    /// Returns a [`ParseError`] when the buffered bytes are not RESP2.
    pub fn next_frame(&mut self) -> Result<Option<Frame>, ParseError> {
        match Frame::parse(&self.buffer)? {
            Some((frame, consumed)) => {
                self.buffer.drain(..consumed);
                Ok(Some(frame))
            }
            None => Ok(None),
        }
    }

    /// Whether no partial frame is buffered.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(input: &[u8]) -> (Frame, usize) {
        Frame::parse(input).expect("valid RESP").expect("complete frame")
    }

    #[test]
    fn command_encoding() {
        let bytes = Frame::command(["SET", "k", "v"]);
        assert_eq!(bytes, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
    }

    #[test]
    fn command_with_binary_argument() {
        let bytes = Frame::command([b"GET".as_slice(), &[0, 255, b'\r', b'\n']]);
        assert_eq!(bytes, b"*2\r\n$3\r\nGET\r\n$4\r\n\x00\xff\r\n\r\n");
    }

    #[test]
    fn simple_string() {
        assert_eq!(parsed(b"+OK\r\n"), (Frame::Simple("OK".into()), 5));
    }

    #[test]
    fn error_reply() {
        assert_eq!(parsed(b"-ERR boom\r\n"), (Frame::Error("ERR boom".into()), 11));
    }

    #[test]
    fn integer_reply() {
        assert_eq!(parsed(b":-42\r\n"), (Frame::Integer(-42), 6));
    }

    #[test]
    fn bulk_string() {
        assert_eq!(parsed(b"$5\r\nhello\r\n"), (Frame::Bulk(Some(b"hello".to_vec())), 11));
    }

    #[test]
    fn empty_bulk_string() {
        assert_eq!(parsed(b"$0\r\n\r\n"), (Frame::Bulk(Some(Vec::new())), 6));
    }

    #[test]
    fn null_bulk_string() {
        assert_eq!(parsed(b"$-1\r\n"), (Frame::Bulk(None), 5));
    }

    #[test]
    fn null_array() {
        assert_eq!(parsed(b"*-1\r\n"), (Frame::Array(None), 5));
    }

    #[test]
    fn nested_array() {
        let input = b"*3\r\n:1\r\n$2\r\nab\r\n*1\r\n+x\r\n";
        let expected = Frame::Array(Some(vec![
            Frame::Integer(1),
            Frame::Bulk(Some(b"ab".to_vec())),
            Frame::Array(Some(vec![Frame::Simple("x".into())])),
        ]));
        assert_eq!(parsed(input), (expected, input.len()));
    }

    #[test]
    fn partial_frames() {
        for input in [
            b"".as_slice(),
            b"+",
            b"+OK",
            b"+OK\r",
            b"$5\r\n",
            b"$5\r\nhel",
            b"$5\r\nhello\r",
            b"*2\r\n:1\r\n",
            b"*2\r\n:1\r\n$1",
        ] {
            assert_eq!(Frame::parse(input), Ok(None), "{input:?}");
        }
    }

    #[test]
    fn trailing_bytes_are_left() {
        let (frame, consumed) = parsed(b"+OK\r\n:1\r\n");
        assert_eq!(frame, Frame::Simple("OK".into()));
        assert_eq!(consumed, 5);
    }

    #[test]
    fn unknown_type_tag() {
        assert_eq!(Frame::parse(b"!oops\r\n"), Err(ParseError::UnknownType(b'!')));
    }

    #[test]
    fn bad_integer() {
        assert_eq!(Frame::parse(b":abc\r\n"), Err(ParseError::BadInteger));
        assert_eq!(Frame::parse(b"$x\r\n"), Err(ParseError::BadInteger));
    }

    #[test]
    fn unterminated_bulk() {
        assert_eq!(Frame::parse(b"$2\r\nabcd\r\n"), Err(ParseError::Unterminated));
    }

    #[test]
    fn encode_round_trip() {
        let frames = [
            Frame::Simple("PONG".into()),
            Frame::Error("WRONGTYPE".into()),
            Frame::Integer(i64::MIN),
            Frame::Bulk(None),
            Frame::Bulk(Some(b"\r\n".to_vec())),
            Frame::Array(None),
            Frame::Array(Some(vec![Frame::Integer(1), Frame::Bulk(Some(Vec::new()))])),
        ];
        for frame in frames {
            let bytes = frame.encode();
            assert_eq!(parsed(&bytes), (frame, bytes.len()));
        }
    }

    #[test]
    fn decoder_across_split_pushes() {
        let mut decoder = Decoder::new();
        decoder.push(b"$5\r\nhel");
        assert_eq!(decoder.next_frame(), Ok(None));
        decoder.push(b"lo\r\n:7\r\n+O");
        assert_eq!(decoder.next_frame(), Ok(Some(Frame::Bulk(Some(b"hello".to_vec())))));
        assert_eq!(decoder.next_frame(), Ok(Some(Frame::Integer(7))));
        assert_eq!(decoder.next_frame(), Ok(None));
        assert!(!decoder.is_empty());
        decoder.push(b"K\r\n");
        assert_eq!(decoder.next_frame(), Ok(Some(Frame::Simple("OK".into()))));
        assert!(decoder.is_empty());
    }

    #[test]
    fn reply_accessors() {
        assert_eq!(Frame::Bulk(Some(b"v".to_vec())).into_bulk(), Ok(Some(b"v".to_vec())));
        assert_eq!(Frame::Integer(3).into_bulk(), Err(Frame::Integer(3)));
        assert_eq!(Frame::Integer(3).into_integer(), Ok(3));
        assert_eq!(Frame::Array(None).into_array(), Err(Frame::Array(None)));
        assert_eq!(Frame::Array(Some(vec![])).into_array(), Ok(vec![]));
    }

    #[test]
    fn codec_error_reply() {
        let mut codec = Resp::default();
        codec.push(b"-WRONGTYPE not a string\r\n:1\r\n");
        assert_eq!(codec.next(), Err(Error::Other("WRONGTYPE not a string".to_owned())));
        assert_eq!(codec.next(), Ok(Some(Frame::Integer(1))));
        assert_eq!(codec.next(), Ok(None));
    }

    #[test]
    fn codec_malformed_reply() {
        let mut codec = Resp::default();
        codec.push(b"!oops\r\n");
        assert!(matches!(codec.next(), Err(Error::Other(text)) if text.contains("not RESP2")));
    }
}
