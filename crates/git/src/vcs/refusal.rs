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

// Classification is pure over text; the refusals suite drives each class
// from a guest, over real repositories and remotes.
#[cfg(test)]
mod tests {
    use super::{Class, classify};

    #[test]
    fn classes() {
        assert_eq!(
            classify("fatal: not a git repository (or any of the parent directories): .git"),
            Class::NotARepository
        );
        assert_eq!(
            classify("fatal: this operation must be run in a work tree"),
            Class::NotARepository
        );
        assert_eq!(classify("fatal: '../w3' already exists"), Class::Exists);
        assert_eq!(
            classify("fatal: destination path 'x' already exists and is not an empty directory."),
            Class::Exists
        );
        assert_eq!(classify("fatal: invalid reference: nope"), Class::NotFound);
        assert_eq!(classify("fatal: not a valid object name: 'nope'"), Class::NotFound);
        assert_eq!(classify("fatal: Needed a single revision"), Class::NotFound);
        assert_eq!(classify("merge: nope - not something we can merge"), Class::NotFound);
        assert_eq!(classify("error: src refspec nolabel does not match any"), Class::NotFound);
        assert_eq!(classify("fatal: Could not read from remote repository."), Class::Access);
        assert_eq!(
            classify("ssh: Could not resolve hostname x: nodename nor servname"),
            Class::Access
        );
        assert_eq!(classify("fatal: Authentication failed for 'https://x/'"), Class::Access);
        assert_eq!(classify("error: failed to push some refs to 'x'"), Class::Other);
        assert_eq!(classify(""), Class::Other);
    }

    // what git prints for a push its remote's branch has moved past, the
    // hints after the rejection line matching no other class
    #[test]
    fn rejected_push() {
        let non_fast_forward = "To /tmp/origin\n \
             ! [rejected]        emery/r -> emery/r (non-fast-forward)\n\
             error: failed to push some refs to '/tmp/origin'\n\
             hint: Updates were rejected because a pushed branch tip is behind its remote\n\
             hint: counterpart. If you want to integrate the remote changes, use 'git pull'\n\
             hint: before pushing again.\n\
             hint: See the 'Note about fast-forwards' in 'git push --help' for details.";
        assert_eq!(classify(non_fast_forward), Class::Diverged);
        let fetch_first = "To /tmp/origin\n \
             ! [rejected]        emery/r -> emery/r (fetch first)\n\
             error: failed to push some refs to '/tmp/origin'\n\
             hint: Updates were rejected because the remote contains work that you do not\n\
             hint: have locally. This is usually caused by another repository pushing to\n\
             hint: the same ref. If you want to integrate the remote changes, use\n\
             hint: 'git pull' before pushing again.\n\
             hint: See the 'Note about fast-forwards' in 'git push --help' for details.";
        assert_eq!(classify(fetch_first), Class::Diverged);
        assert_eq!(
            classify(" ! [rejected]        emery/r -> emery/r (stale info)"),
            Class::Diverged
        );
    }

    // the two fatals an unknown remote prints together
    #[test]
    fn unknown_remote_before_access() {
        let stderr = "fatal: 'nope' does not appear to be a git repository\n\
                      fatal: Could not read from remote repository.";
        assert_eq!(classify(stderr), Class::NotFound);
    }
}
