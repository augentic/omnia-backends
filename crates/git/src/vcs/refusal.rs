//! Reading what a failed `git` said into the class of `omnia:vcs` error it
//! is.

// The class a failed process falls in; the operation names the subject.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    NotARepository,
    Exists,
    NotFound,
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

const ACCESS: [&str; 12] = [
    "Authentication failed",
    "Permission denied",
    "could not read Username",
    "terminal prompts disabled",
    "Could not resolve host",
    "Could not read from remote repository",
    "Connection refused",
    "Connection timed out",
    "Network is unreachable",
    "Host key verification failed",
    "unable to access",
    "The requested URL returned error: 403",
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
    } else if matches(&ACCESS) {
        Class::Access
    } else {
        Class::Other
    }
}
