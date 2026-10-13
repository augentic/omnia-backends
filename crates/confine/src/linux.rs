//! The Linux substrate: a Landlock ruleset over the filesystem and two
//! seccomp filters over the syscalls, built in the parent and applied in
//! the child before it runs the program.

// Lifted from heel 0.3.0, `src/platform/linux/{mod,landlock_rules,
// seccomp_filter}.rs`, Copyright (c) 2025 Lexo Liu, MIT OR Apache-2.0:
// support probed by creating a ruleset rather than forking, the ruleset a
// hard requirement with anything short of full enforcement a failure, a
// system path skipped when absent and a granted one refused, and
// `clone3` refused with `ENOSYS` in a filter of its own.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, anyhow, ensure};
use landlock::{
    ABI, Access as _, AccessFs, AccessNet, BitFlags, CompatLevel, Compatible as _, PathBeneath,
    PathFd, Ruleset, RulesetAttr as _, RulesetCreated, RulesetCreatedAttr as _, RulesetStatus,
};
use seccompiler::{
    BpfProgram, SeccompAction, SeccompCmpArgLen, SeccompCmpOp, SeccompCondition, SeccompFilter,
    SeccompRule, TargetArch,
};
use tokio::process::Command;

// The first ABI that fences TCP, which the follow-ups build on; its kernel
// (6.7) carries every filesystem right the ruleset handles.
const REQUIRED: ABI = ABI::V4;
const OSRELEASE: &str = "/proc/sys/kernel/osrelease";
const PTRACE_SCOPE: &str = "/proc/sys/kernel/yama/ptrace_scope";

// Where programs and their libraries live, read and executed, and the
// state they read; each skipped when the distribution lacks it.
const EXECUTABLE: [&str; 6] = ["/usr", "/bin", "/sbin", "/lib", "/lib64", "/lib32"];
const READABLE: [&str; 4] = ["/etc", "/proc", "/sys", "/run"];
const DEVICES: [&str; 4] = ["/dev/null", "/dev/zero", "/dev/random", "/dev/urandom"];

// Where one process is put, every path canonical, and the directories its
// ask was granted to read.
pub struct Place<'a> {
    pub tree: &'a Path,
    pub scratch: &'a Path,
    pub program: &'a Path,
    pub read: &'a [PathBuf],
}

// The substrate as probed: the filters every process gets, compiled once.
pub struct Linux {
    filters: Filters,
}

impl std::fmt::Debug for Linux {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Linux").finish_non_exhaustive()
    }
}

impl Linux {
    pub async fn probe() -> Result<Self> {
        // the kernel's landlock, by creating a ruleset rather than forking:
        // the TCP right is ABI 4's, so handling it holds the kernel to it
        let created = Ruleset::default()
            .set_compatibility(CompatLevel::HardRequirement)
            .handle_access(AccessFs::from_all(REQUIRED))
            .and_then(|ruleset| ruleset.handle_access(AccessNet::ConnectTcp))
            .and_then(Ruleset::create);
        if let Err(error) = created {
            return Err(anyhow!(error).context(format!(
                "Landlock ABI {} (kernel 6.7) is required and kernel {} cannot create its ruleset",
                REQUIRED as i32,
                kernel().await
            )));
        }

        // yama, so `/proc/<pid>/mem` of a same-uid process stays closed
        let scope = tokio::fs::read_to_string(PTRACE_SCOPE)
            .await
            .with_context(|| format!("`kernel.yama.ptrace_scope` is unset: {PTRACE_SCOPE}"))?;
        let scope: u32 = scope
            .trim()
            .parse()
            .with_context(|| format!("`kernel.yama.ptrace_scope` reads {scope:?}"))?;
        ensure!(scope >= 1, "`kernel.yama.ptrace_scope` is {scope}; 1 or more is required");

        let filters = Filters::compile()?;
        undumpable()?;
        Ok(Self { filters })
    }

    // The ruleset over `place` and the filters, applied in the child once
    // it has entered the tree: `restrict_self` sets `no_new_privs` and
    // nothing short of full enforcement runs the program.
    #[expect(
        unsafe_code,
        reason = "`pre_exec` runs in the forked child; it applies rules built here and allocates \
                  only on its failure path"
    )]
    pub fn restrain(&self, command: &mut Command, place: &Place<'_>) -> Result<()> {
        let mut ruleset = Some(Self::ruleset(place)?);
        let mut filters = Some(self.filters.clone());
        unsafe {
            command.pre_exec(move || {
                let ruleset = ruleset.take().ok_or_else(|| io::Error::other("rules spent"))?;
                let status = ruleset.restrict_self().map_err(|error| match error {
                    landlock::RulesetError::RestrictSelf(
                        landlock::RestrictSelfError::SetNoNewPrivsCall { source, .. }
                        | landlock::RestrictSelfError::RestrictSelfCall { source, .. },
                    ) => source,
                    other => io::Error::other(format!("Landlock: {other}")),
                })?;
                if status.ruleset != RulesetStatus::FullyEnforced {
                    return Err(io::Error::other("Landlock did not enforce the ruleset whole"));
                }
                filters.take().ok_or_else(|| io::Error::other("filters spent"))?.apply()
            });
        }
        Ok(())
    }

    fn ruleset(place: &Place<'_>) -> Result<RulesetCreated> {
        let abi = REQUIRED;
        let mut ruleset = Ruleset::default()
            .set_compatibility(CompatLevel::HardRequirement)
            .handle_access(AccessFs::from_all(abi))
            .and_then(Ruleset::create)
            .context("creating the Landlock ruleset")?;

        // the system, each path as the distribution lays it
        let executable = AccessFs::ReadFile | AccessFs::ReadDir | AccessFs::Execute;
        for root in EXECUTABLE {
            optional(&mut ruleset, Path::new(root), executable)?;
        }
        for root in READABLE {
            optional(&mut ruleset, Path::new(root), AccessFs::from_read(abi))?;
        }
        for device in DEVICES {
            optional(&mut ruleset, Path::new(device), AccessFs::from_file(abi))?;
        }

        // the process's own: its tree, its scratch, its program, and the
        // roots its ask was granted, which run as they read since a
        // toolchain lives there
        required(&mut ruleset, place.tree, AccessFs::from_all(abi))?;
        required(&mut ruleset, place.scratch, AccessFs::from_all(abi))?;
        required(&mut ruleset, place.program, AccessFs::ReadFile | AccessFs::Execute)?;
        for root in place.read {
            required(&mut ruleset, root, AccessFs::from_read(abi) | AccessFs::Execute)?;
        }
        Ok(ruleset)
    }
}

// A system path the distribution may lack, which is then no rule at all.
fn optional(ruleset: &mut RulesetCreated, path: &Path, access: BitFlags<AccessFs>) -> Result<()> {
    match PathFd::new(path) {
        Ok(fd) => grant(ruleset, path, fd, access),
        Err(_absent) => Ok(()),
    }
}

// A path the boundary stands on: absent, the rule would match nothing and
// the process would run with less than it was promised.
fn required(ruleset: &mut RulesetCreated, path: &Path, access: BitFlags<AccessFs>) -> Result<()> {
    let fd = PathFd::new(path).with_context(|| format!("opening {}", path.display()))?;
    grant(ruleset, path, fd, access)
}

// Landlock refuses a directory right on a file, so a rule over a file keeps
// the file rights alone.
fn grant(
    ruleset: &mut RulesetCreated, path: &Path, fd: PathFd, access: BitFlags<AccessFs>,
) -> Result<()> {
    let access = if path.is_dir() { access } else { access & AccessFs::from_file(REQUIRED) };
    ensure!(!access.is_empty(), "{} is a file and takes no directory right", path.display());
    ruleset
        .add_rule(PathBeneath::new(fd, access))
        .map(|_ruleset| ())
        .with_context(|| format!("Landlock refused the rule over {}", path.display()))
}

// The seccomp programs every process gets: a deny list answering `EPERM`,
// and `clone3` answering `ENOSYS` apart from it, since glibc issues
// `clone3` for every thread and falls back to `clone` on `ENOSYS` alone.
#[derive(Clone)]
struct Filters {
    denied: BpfProgram,
    unsupported: BpfProgram,
}

impl Filters {
    fn compile() -> Result<Self> {
        let arch = ARCH.context("seccomp filters are compiled for x86-64 and aarch64 alone")?;
        let denied = compile(denied()?, SeccompAction::Errno(errno(libc::EPERM)), arch)?;
        let unsupported = compile(
            BTreeMap::from([(libc::SYS_clone3, Vec::new())]),
            SeccompAction::Errno(errno(libc::ENOSYS)),
            arch,
        )?;
        Ok(Self { denied, unsupported })
    }

    fn apply(self) -> io::Result<()> {
        for program in [&self.denied, &self.unsupported] {
            seccompiler::apply_filter(program).map_err(|error| match error {
                seccompiler::Error::Prctl(source) | seccompiler::Error::Seccomp(source) => source,
                other => io::Error::other(format!("seccomp: {other}")),
            })?;
        }
        Ok(())
    }
}

const fn errno(code: i32) -> u32 {
    code.unsigned_abs()
}

async fn kernel() -> String {
    tokio::fs::read_to_string(OSRELEASE)
        .await
        .map_or_else(|_| "[unknown]".to_owned(), |release| release.trim().to_owned())
}

#[cfg(target_arch = "x86_64")]
const ARCH: Option<TargetArch> = Some(TargetArch::x86_64);
#[cfg(target_arch = "aarch64")]
const ARCH: Option<TargetArch> = Some(TargetArch::aarch64);
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
const ARCH: Option<TargetArch> = None;

fn compile(
    rules: BTreeMap<i64, Vec<SeccompRule>>, matched: SeccompAction, arch: TargetArch,
) -> Result<BpfProgram> {
    SeccompFilter::new(rules, SeccompAction::Allow, matched, arch)
        .context("the seccomp filter")?
        .try_into()
        .context("compiling the seccomp filter")
}

// The two ioctl requests that write a terminal's input queue, spelled here
// since libc types them by its flavour.
const TIOCSTI: u64 = 0x5412;
const TIOCLINUX: u64 = 0x541C;

// What a confined process has no use for: an empty rule list matches the
// syscall whole, a condition narrows it to the arguments named.
fn denied() -> Result<BTreeMap<i64, Vec<SeccompRule>>> {
    const OUTRIGHT: &[i64] = &[
        libc::SYS_ptrace,
        libc::SYS_process_vm_readv,
        libc::SYS_process_vm_writev,
        libc::SYS_kcmp,
        libc::SYS_pidfd_getfd,
        libc::SYS_open_by_handle_at,
        libc::SYS_unshare,
        libc::SYS_setns,
        libc::SYS_mount,
        libc::SYS_umount2,
        libc::SYS_pivot_root,
        libc::SYS_open_tree,
        libc::SYS_move_mount,
        libc::SYS_fsopen,
        libc::SYS_fsconfig,
        libc::SYS_fsmount,
        libc::SYS_fspick,
        libc::SYS_mount_setattr,
        libc::SYS_bpf,
        libc::SYS_userfaultfd,
        libc::SYS_perf_event_open,
        libc::SYS_personality,
        libc::SYS_io_uring_setup,
        libc::SYS_io_uring_enter,
        libc::SYS_io_uring_register,
        libc::SYS_init_module,
        libc::SYS_finit_module,
        libc::SYS_delete_module,
        libc::SYS_kexec_load,
        libc::SYS_kexec_file_load,
        libc::SYS_reboot,
        libc::SYS_add_key,
        libc::SYS_request_key,
        libc::SYS_keyctl,
        libc::SYS_settimeofday,
        libc::SYS_clock_settime,
        libc::SYS_adjtimex,
        libc::SYS_sethostname,
        libc::SYS_setdomainname,
        libc::SYS_swapon,
        libc::SYS_swapoff,
        libc::SYS_quotactl,
        libc::SYS_acct,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_ioperm,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_iopl,
        #[cfg(target_arch = "x86_64")]
        libc::SYS_modify_ldt,
    ];
    let mut rules: BTreeMap<i64, Vec<SeccompRule>> =
        OUTRIGHT.iter().map(|&syscall| (syscall, Vec::new())).collect();
    rules.insert(libc::SYS_socket, sockets()?);
    rules.insert(libc::SYS_clone, vec![rule(vec![masked(0, flag(libc::CLONE_NEWUSER))?])?]);
    rules.insert(
        libc::SYS_ioctl,
        vec![rule(vec![request(TIOCSTI)?])?, rule(vec![request(TIOCLINUX)?])?],
    );
    Ok(rules)
}

// A request compared as the kernel reads it, its low 32 bits, so high bits
// in the register do not slip the rule.
fn request(value: u64) -> Result<SeccompCondition> {
    SeccompCondition::new(1, SeccompCmpArgLen::Dword, SeccompCmpOp::Eq, value)
        .context("a seccomp condition")
}

// Landlock sees TCP alone: a raw or datagram socket, a packet socket, and
// netlink past the route family `getifaddrs` reads are refused here. The
// type is compared under its mask, so `SOCK_NONBLOCK` and `SOCK_CLOEXEC`
// do not slip a rule.
fn sockets() -> Result<Vec<SeccompRule>> {
    // The obsolete type the kernel still turns into a packet socket when
    // asked for under `AF_INET`, which the `AF_PACKET` rule never sees.
    const SOCK_PACKET: libc::c_int = 10;
    const SOCK_TYPE_MASK: u64 = 0xf;
    const OTHER_TYPES: [libc::c_int; 6] = [
        libc::SOCK_DGRAM,
        libc::SOCK_RAW,
        libc::SOCK_RDM,
        libc::SOCK_SEQPACKET,
        libc::SOCK_DCCP,
        SOCK_PACKET,
    ];
    let mut rules = vec![
        rule(vec![dword(0, libc::AF_PACKET)?])?,
        rule(vec![dword(0, libc::AF_NETLINK)?, not_dword(2, libc::NETLINK_ROUTE)?])?,
    ];
    for domain in [libc::AF_INET, libc::AF_INET6] {
        for socket_type in OTHER_TYPES {
            rules.push(rule(vec![
                dword(0, domain)?,
                SeccompCondition::new(
                    1,
                    SeccompCmpArgLen::Dword,
                    SeccompCmpOp::MaskedEq(SOCK_TYPE_MASK),
                    flag(socket_type),
                )?,
            ])?);
        }
    }
    Ok(rules)
}

fn flag(value: libc::c_int) -> u64 {
    u64::from(value.unsigned_abs())
}

fn rule(conditions: Vec<SeccompCondition>) -> Result<SeccompRule> {
    SeccompRule::new(conditions).context("a seccomp rule")
}

fn dword(argument: u8, value: libc::c_int) -> Result<SeccompCondition> {
    SeccompCondition::new(argument, SeccompCmpArgLen::Dword, SeccompCmpOp::Eq, flag(value))
        .context("a seccomp condition")
}

fn not_dword(argument: u8, value: libc::c_int) -> Result<SeccompCondition> {
    SeccompCondition::new(argument, SeccompCmpArgLen::Dword, SeccompCmpOp::Ne, flag(value))
        .context("a seccomp condition")
}

fn masked(argument: u8, mask: u64) -> Result<SeccompCondition> {
    SeccompCondition::new(argument, SeccompCmpArgLen::Qword, SeccompCmpOp::MaskedEq(mask), mask)
        .context("a seccomp condition")
}

// The host's `/proc/<pid>` turned root-owned, as ssh-agent does, so a
// process inside reads nothing of the process that holds its token.
#[expect(unsafe_code, reason = "`prctl` over two integers has no safe wrapper")]
fn undumpable() -> Result<()> {
    // SAFETY: `PR_SET_DUMPABLE` reads its one integer argument and touches
    // no memory.
    if unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0) } == 0 {
        Ok(())
    } else {
        Err(anyhow!(io::Error::last_os_error()).context("setting the host non-dumpable"))
    }
}
