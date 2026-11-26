//! An address a transport operation dials, read the way git reads it: a path
//! on this host, a host git dials over ssh or http(s), or neither, where a
//! reading could name a host other than the one git dials.

// What an address names as git dials it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Address<'a> {
    // A path, or a `file://` URL.
    Local,
    // The host git dials, and for http(s) the URL remote-curl requests.
    Remote { host: &'a str, http: Option<&'a str> },
    // Another remote helper's address, or one whose authority git, ssh, and
    // curl could each split differently: a second `@`, a bracket, a `%`
    // escape, a `?`, `#`, or `\`, or a control character.
    Unreadable,
}

impl<'a> Address<'a> {
    pub fn parse(address: &'a str) -> Self {
        // `<helper>::<address>` hands the address to `git-remote-<helper>`
        if let Some((helper, rest)) = helper(address) {
            let http = rest.starts_with("http://") || rest.starts_with("https://");
            return if matches!(helper, "http" | "https") && http {
                Self::http(rest)
            } else {
                Self::Unreadable
            };
        }

        // git's own rule: an address with no `scheme://` is a path unless a
        // colon comes before any slash, making it scp-like `[user@]host:path`
        match address.split_once("://") {
            Some((scheme, _)) if scheme.eq_ignore_ascii_case("file") => Self::Local,
            Some(("ssh" | "git+ssh" | "ssh+git", rest)) => Self::ssh(authority(rest)),
            Some(("http" | "https", _)) => Self::http(address),
            Some(_) => Self::Unreadable,
            None => match (address.find(':'), address.find('/')) {
                (Some(colon), slash) if slash.is_none_or(|slash| colon < slash) => {
                    Self::ssh(&address[..colon])
                }
                _ => Self::Local,
            },
        }
    }

    #[expect(dead_code, reason = "reserved for host-policy expansions beside `target::is_local`")]
    pub const fn is_local(self) -> bool {
        matches!(self, Self::Local)
    }

    fn ssh(authority: &'a str) -> Self {
        host(authority).map_or(Self::Unreadable, |host| Self::Remote { host, http: None })
    }

    fn http(url: &'a str) -> Self {
        let rest = url.split_once("://").map_or("", |(_, rest)| rest);
        host(authority(rest)).map_or(Self::Unreadable, |host| Self::Remote {
            host,
            http: Some(url),
        })
    }
}

// git's own rule for `<helper>::<address>`: a run of URL scheme characters,
// the first alphanumeric, then `::`.
fn helper(address: &str) -> Option<(&str, &str)> {
    let scheme = |(at, byte): (usize, &u8)| {
        byte.is_ascii_alphanumeric() || (at > 0 && matches!(byte, b'+' | b'-' | b'.'))
    };
    let end = address.as_bytes().iter().enumerate().position(|entry| !scheme(entry));
    let end = end.unwrap_or(address.len());
    address[end..].strip_prefix("::").map(|rest| (&address[..end], rest))
}

// Everything before the first `/`, which is where git ends the host it
// hands ssh.
fn authority(rest: &str) -> &str {
    rest.split_once('/').map_or(rest, |(authority, _)| authority)
}

// The host an authority names, past any `user@` and before any `:port`. None
// where git, ssh, and curl could split it differently: curl ends an
// authority at a `?` or `#` that git hands ssh whole, and ssh splits a user
// from its host at the last `@`.
fn host(authority: &str) -> Option<&str> {
    let ambiguous = |byte: u8| {
        byte.is_ascii_control() || matches!(byte, b'?' | b'#' | b'\\' | b'%' | b'[' | b']')
    };
    if authority.bytes().any(ambiguous) || authority.matches('@').count() > 1 {
        return None;
    }
    let host = authority.rsplit_once('@').map_or(authority, |(_, host)| host);
    Some(host.split_once(':').map_or(host, |(host, _)| host))
}
