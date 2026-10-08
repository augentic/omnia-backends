//! Reading what a failed `git` said into the class of `omnia:vcs` error it
//! is, and the version `git --version` prints.

// The class a failed process falls in; the operation names the subject.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    NotARepository,
    Exists,
    NotFound,
    Access,
    Other,
}

const NOT_A_REPOSITORY: [&str; 2] = ["not a git repository", "must be run in a work tree"];

const EXISTS: [&str; 1] = ["already exists"];

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
    } else if matches(&NOT_FOUND) {
        Class::NotFound
    } else if matches(&ACCESS) {
        Class::Access
    } else {
        Class::Other
    }
}

// `git version 2.50.1 (Apple Git-155)` and `git version 2.47.0.windows.1`
// both read as their first three numbers; a missing patch is zero.
pub fn version(stdout: &str) -> Option<(u32, u32, u32)> {
    let rest = stdout.trim().strip_prefix("git version ")?;
    let token = rest.split_whitespace().next()?;
    let mut parts = token.split('.').map(leading_number);
    let major = parts.next().flatten()?;
    let minor = parts.next().flatten()?;
    let patch = parts.next().flatten().unwrap_or(0);
    Some((major, minor, patch))
}

fn leading_number(part: &str) -> Option<u32> {
    let digits: String = part.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::{Class, classify, version};

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

    // the two fatals an unknown remote prints together
    #[test]
    fn unknown_remote_before_access() {
        let stderr = "fatal: 'nope' does not appear to be a git repository\n\
                      fatal: Could not read from remote repository.";
        assert_eq!(classify(stderr), Class::NotFound);
    }

    #[test]
    fn versions() {
        assert_eq!(version("git version 2.50.1 (Apple Git-155)\n"), Some((2, 50, 1)));
        assert_eq!(version("git version 2.47.0.windows.1"), Some((2, 47, 0)));
        assert_eq!(version("git version 2.4.0"), Some((2, 4, 0)));
        assert_eq!(version("git version 2.5"), Some((2, 5, 0)));
        assert_eq!(version("git version 2.39.5-rc1"), Some((2, 39, 5)));
        assert_eq!(version("not git"), None);
        assert_eq!(version("git version"), None);
        assert_eq!(version("git version x.y"), None);
    }
}
