//! Reading what a failed `git` said into the class of `omnia:vcs` error it
//! is.

// The class a failed process falls in; the operation names the subject.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    NotARepository,
    Exists,
    NotFound,
    Denied,
    Access,
    Diverged,
    Other,
}

const NOT_A_REPOSITORY: [&str; 2] = ["not a git repository", "must be run in a work tree"];

const EXISTS: [&str; 1] = ["already exists"];

// What git prints after `! [rejected]` for a push the remote's branch has
// moved past: `fetch first` when the remote tip is not held locally,
// `non-fast-forward` when it is, `stale info` under a lease.
const DIVERGED: [&str; 3] = ["(non-fast-forward)", "(fetch first)", "(stale info)"];

// An unknown remote reads as both "does not appear to be a git repository"
// and "Could not read from remote repository", so these are tried before the
// access patterns.
const NOT_FOUND: [&str; 13] = [
    "unknown revision",
    "not a valid object name",
    "invalid reference",
    "Needed a single revision",
    "bad revision",
    "does not appear to be a git repository",
    "couldn't find remote ref",
    "no such remote",
    "not something we can merge",
    "remote ref does not exist",
    "Remote branch",
    "does not match any",
    "not found",
];

// A remote that refuses the caller, or wants credentials it was not given.
// Git follows a refusal with "Could not read from remote repository" or
// "unable to access", so these are tried before the access patterns.
const DENIED: [&str; 7] = [
    "Authentication failed",
    "Permission denied",
    "could not read Username",
    "terminal prompts disabled",
    "Host key verification failed",
    "The requested URL returned error: 401",
    "The requested URL returned error: 403",
];

const ACCESS: [&str; 6] = [
    "Could not resolve host",
    "Could not read from remote repository",
    "Connection refused",
    "Connection timed out",
    "Network is unreachable",
    "unable to access",
];

pub fn classify(stderr: &str) -> Class {
    let matches = |patterns: &[&str]| patterns.iter().any(|pattern| stderr.contains(pattern));
    if matches(&NOT_A_REPOSITORY) {
        Class::NotARepository
    } else if matches(&EXISTS) {
        Class::Exists
    } else if matches(&DIVERGED) {
        Class::Diverged
    } else if matches(&NOT_FOUND) {
        Class::NotFound
    } else if matches(&DENIED) {
        Class::Denied
    } else if matches(&ACCESS) {
        Class::Access
    } else {
        Class::Other
    }
}

#[cfg(test)]
mod tests {
    use super::{Class, classify};

    // git often appends "unable to access" after an HTTP status line; denied
    // must win over access
    #[test]
    fn http_401_before_access() {
        let stderr = "fatal: The requested URL returned error: 401\nfatal: unable to access";
        assert_eq!(classify(stderr), Class::Denied);
    }
}
