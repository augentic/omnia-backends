//! Holding each address a transport operation dials to the guest's `hosts`
//! grant.
//!
//! The hold reads each address as the host's configuration expands it,
//! before git runs. Under a list, an address whose host the list does not
//! reach is `denied`, and so is one no host can be read from. A redirect
//! leads to no address the hold read, so under a list git follows none, and
//! one it meets is `denied`. A guest without a list dials as before.

use std::ffi::OsString;
use std::iter;
use std::sync::Arc;

use omnia::Grants;
use omnia_wasi_vcs::Error;

use super::refusal::{self, Class};
use crate::command::{Address, Output};

// The guest's grants, as a transport operation holds what it dials to them.
pub struct Hold {
    grants: Arc<Grants>,
    listed: bool,
}

impl Hold {
    pub fn new(grants: Arc<Grants>) -> Self {
        // HACK: `Grants` answers whether it reaches a host, never whether it
        // lists hosts at all. No list reaches an IP literal, `*` included,
        // so grants that refuse one list hosts.
        let listed = !grants.allows_host("127.0.0.1");
        Self { grants, listed }
    }

    // Admit every address a transport operation dials, and return the `-c`
    // pins to lead its subcommand with. A host the guest's grants do not
    // reach is `denied` before anything connects. Under a list:
    // - an address no host can be read from is `denied`;
    // - an http(s) URL holding `=` is `denied`, since `-c` splits at the
    //   first `=` and no pin can name it;
    // - git follows no redirect of an http(s) URL. The generic key holds
    //   where git matches no URL-specific one, and each URL-specific key
    //   names its URL whole, so no `http.<url>` setting of the operator's
    //   outranks it.
    pub fn admit<'a>(
        &self, addresses: impl IntoIterator<Item = &'a str>,
    ) -> Result<Vec<OsString>, Error> {
        let mut urls = Vec::new();
        for address in addresses {
            match Address::parse(address) {
                Address::Remote { host, .. } if !self.grants.allows_host(host) => {
                    return Err(Error::Denied(format!(
                        "`{address}` dials `{host}`, a host the guest's grants do not reach"
                    )));
                }
                Address::Remote { http: Some(url), .. } if self.listed => {
                    if url.contains('=') {
                        return Err(Error::Denied(format!(
                            "`{address}` holds `=`, which no pin refusing its redirects can name"
                        )));
                    }
                    urls.push(url);
                }
                Address::Unreadable if self.listed => {
                    return Err(Error::Denied(format!(
                        "`{address}` names no host the guest's grants can hold"
                    )));
                }
                Address::Local | Address::Remote { .. } | Address::Unreadable => {}
            }
        }

        let mut pins = Vec::new();
        if !urls.is_empty() {
            let keys = urls.iter().map(|url| format!("http.{url}"));
            for key in iter::once(String::from("http")).chain(keys) {
                pins.push(OsString::from("-c"));
                pins.push(OsString::from(format!("{key}.followRedirects=false")));
            }
        }
        Ok(pins)
    }

    // A redirect git did not follow is `denied` under a list; any other
    // failure is `fallback`, as the operation read it.
    pub fn refuse(&self, output: &Output, fallback: Error) -> Error {
        if self.listed && refusal::classify(&output.stderr) == Class::Redirected {
            let detail = output.stderr.trim();
            return Error::Denied(format!("{detail}: under a hosts grant no redirect is followed"));
        }
        fallback
    }
}
