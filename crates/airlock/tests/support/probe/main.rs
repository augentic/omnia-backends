//! The boundary suite's probe: copied into a lent tree and run inside the
//! boundary, it makes one attempt per row and exits 0 when the attempt
//! succeeded, 1 when the kernel refused it, with the reason on stderr.

use std::io::{Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let row = args.next().unwrap_or_default();
    let outcome = match row.as_str() {
        "loopback" => loopback(),
        "resolve" => resolve(&args.next().unwrap_or_default()),
        "tunnel" => tunnel(&args.next().unwrap_or_default()),
        #[cfg(target_os = "macos")]
        "procargs" => procargs(&args.next().unwrap_or_default()),
        #[cfg(target_os = "linux")]
        "syscall" => syscall::row(&args.next().unwrap_or_default(), &args.collect::<Vec<_>>()),
        "environ" => environ(&args.next().unwrap_or_default()),
        other => Err(format!("unknown probe `{other}`")),
    };
    match outcome {
        Ok(answer) => {
            say(&mut std::io::stdout(), &answer);
            ExitCode::SUCCESS
        }
        Err(reason) => {
            say(&mut std::io::stderr(), &format!("{row}: {reason}"));
            ExitCode::FAILURE
        }
    }
}

fn say(stream: &mut impl std::io::Write, line: &str) {
    let _ = writeln!(stream, "{line}");
}

// The arguments of another process, read as `ps` reads them: the sysctl
// `kern.procargs2` over its pid.
#[cfg(target_os = "macos")]
#[expect(unsafe_code, reason = "the probe is the one syscall the row is about")]
fn procargs(pid: &str) -> Result<String, String> {
    let pid: libc::c_int = pid.parse().map_err(|error| format!("not a pid {pid:?}: {error}"))?;
    let mut mib = [libc::CTL_KERN, libc::KERN_PROCARGS2, pid];
    let mut buffer = vec![0u8; 1 << 16];
    let mut size = buffer.len();
    // SAFETY: the mib and the buffer are live for the call, and `size` is
    // the buffer's length in and the bytes written out.
    let answered = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            3,
            buffer.as_mut_ptr().cast(),
            &raw mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    if answered == 0 {
        Ok(size.to_string())
    } else {
        Err(format!("sysctl: {}", std::io::Error::last_os_error()))
    }
}

// The environment of another process, as `/proc` (or the sysctl behind
// `ps -E` on macOS) shows it.
fn environ(pid: &str) -> Result<String, String> {
    let bytes = std::fs::read(format!("/proc/{pid}/environ"))
        .map_err(|error| format!("/proc/{pid}/environ: {error}"))?;
    Ok(bytes.len().to_string())
}

// A name resolved through the system resolver, as every client resolves.
fn resolve(name: &str) -> Result<String, String> {
    use std::net::ToSocketAddrs as _;
    let addresses: Vec<_> = (name, 443)
        .to_socket_addrs()
        .map_err(|error| format!("{name}: {error}"))?
        .map(|address| address.ip().to_string())
        .collect();
    if addresses.is_empty() {
        return Err(format!("{name}: no address"));
    }
    Ok(addresses.join(" "))
}

// A tunnel to `target` through the proxy `HTTPS_PROXY` names, carrying
// four bytes to an echo server and back.
fn tunnel(target: &str) -> Result<String, String> {
    let proxy = std::env::var("HTTPS_PROXY").map_err(|error| format!("HTTPS_PROXY: {error}"))?;
    let proxy =
        proxy.strip_prefix("http://").ok_or_else(|| format!("not an http proxy: {proxy}"))?;
    let mut stream = TcpStream::connect(proxy).map_err(|error| format!("proxy: {error}"))?;
    write!(stream, "CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n\r\n")
        .map_err(|error| format!("request: {error}"))?;
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        stream.read_exact(&mut byte).map_err(|error| format!("response: {error}"))?;
        head.push(byte[0]);
    }
    let status = String::from_utf8_lossy(&head).lines().next().unwrap_or_default().to_owned();
    if !status.contains(" 200 ") {
        return Err(format!("proxy answered {status}"));
    }
    stream.write_all(b"ping").map_err(|error| format!("write: {error}"))?;
    let mut echoed = [0; 4];
    stream.read_exact(&mut echoed).map_err(|error| format!("read: {error}"))?;
    if &echoed == b"ping" { Ok(status) } else { Err(format!("echoed {echoed:?}")) }
}

// A server on port 0 answered over loopback from inside: the loop a tree's
// own tests run.
fn loopback() -> Result<String, String> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|error| format!("bind: {error}"))?;
    let address = listener.local_addr().map_err(|error| format!("local_addr: {error}"))?;
    let server = std::thread::spawn(move || -> std::io::Result<[u8; 4]> {
        let (mut stream, _) = listener.accept()?;
        let mut heard = [0; 4];
        stream.read_exact(&mut heard)?;
        stream.write_all(&heard)?;
        Ok(heard)
    });

    let mut client = TcpStream::connect(address).map_err(|error| format!("connect: {error}"))?;
    client.write_all(b"ping").map_err(|error| format!("write: {error}"))?;
    let mut echoed = [0; 4];
    client.read_exact(&mut echoed).map_err(|error| format!("read: {error}"))?;
    let heard = server
        .join()
        .map_err(|panic| format!("the server panicked: {panic:?}"))?
        .map_err(|error| format!("serve: {error}"))?;
    if &heard == b"ping" && echoed == heard {
        Ok(address.port().to_string())
    } else {
        Err(format!("heard {heard:?}, echoed {echoed:?}"))
    }
}

// One syscall per row through libc, the way an escape would make it; an
// attempt the kernel refused names the errno it answered.
#[cfg(target_os = "linux")]
#[expect(unsafe_code, reason = "each row is the one syscall it is about")]
mod syscall {
    use std::ptr;

    pub fn row(name: &str, args: &[String]) -> Result<String, String> {
        let arg = |n: usize| args.get(n).map(String::as_str).unwrap_or_default();
        match name {
            "socket" => socket(arg(0), arg(1)),
            "ptrace" => ptrace(),
            "io-uring" => io_uring(),
            "unshare" => unshare(),
            "mount" => mount(),
            "bpf" => bpf(),
            "ioctl" => ioctl(arg(0)),
            "clone3" => clone3(),
            "thread" => thread(),
            other => Err(format!("unknown syscall row `{other}`")),
        }
    }

    // The errno the attempt answered, by name.
    fn refused() -> String {
        let error = std::io::Error::last_os_error();
        let name = match error.raw_os_error() {
            Some(libc::EPERM) => "EPERM",
            Some(libc::EACCES) => "EACCES",
            Some(libc::ENOSYS) => "ENOSYS",
            Some(libc::EINVAL) => "EINVAL",
            Some(libc::EFAULT) => "EFAULT",
            Some(libc::ENOTTY) => "ENOTTY",
            Some(libc::ENOENT) => "ENOENT",
            Some(libc::EIO) => "EIO",
            _ => "other",
        };
        format!("{name} ({error})")
    }

    fn outcome(answer: libc::c_long) -> Result<String, String> {
        if answer < 0 { Err(refused()) } else { Ok(answer.to_string()) }
    }

    // `socket <domain> <type[+flag..]>`: `inet dgram`, `inet
    // dgram+nonblock+cloexec`, `netlink-audit raw`, `netlink-route raw`.
    fn socket(domain: &str, kind: &str) -> Result<String, String> {
        let (domain, protocol) = match domain {
            "inet" => (libc::AF_INET, 0),
            "inet6" => (libc::AF_INET6, 0),
            "netlink-route" => (libc::AF_NETLINK, libc::NETLINK_ROUTE),
            "netlink-audit" => (libc::AF_NETLINK, libc::NETLINK_AUDIT),
            "packet" => (libc::AF_PACKET, 0),
            other => return Err(format!("unknown domain `{other}`")),
        };
        let mut kind_flags = 0;
        for token in kind.split('+') {
            kind_flags |= match token {
                "stream" => libc::SOCK_STREAM,
                "dgram" => libc::SOCK_DGRAM,
                "raw" => libc::SOCK_RAW,
                "nonblock" => libc::SOCK_NONBLOCK,
                "cloexec" => libc::SOCK_CLOEXEC,
                other => return Err(format!("unknown type `{other}`")),
            };
        }
        // SAFETY: three integers in, a descriptor or -1 out.
        let fd = unsafe { libc::socket(domain, kind_flags, protocol) };
        if fd < 0 {
            return Err(refused());
        }
        // SAFETY: the descriptor was just opened.
        unsafe { libc::close(fd) };
        Ok("socket".to_owned())
    }

    fn ptrace() -> Result<String, String> {
        // SAFETY: `PTRACE_TRACEME` takes no pointer.
        outcome(unsafe { libc::ptrace(libc::PTRACE_TRACEME, 0, 0, 0) })
    }

    // A null parameter block: refused by the filter before the kernel reads
    // it, `EFAULT` where it is not.
    fn io_uring() -> Result<String, String> {
        // SAFETY: the kernel validates the pointer before any write.
        outcome(unsafe { libc::syscall(libc::SYS_io_uring_setup, 1, ptr::null_mut::<u8>()) })
    }

    fn unshare() -> Result<String, String> {
        // SAFETY: one flag in.
        outcome(unsafe { libc::unshare(libc::CLONE_NEWUSER) }.into())
    }

    fn mount() -> Result<String, String> {
        // SAFETY: every pointer is a live NUL-terminated string or null.
        let answer = unsafe {
            libc::mount(
                c"none".as_ptr(),
                c"/".as_ptr(),
                c"tmpfs".as_ptr(),
                libc::MS_RDONLY,
                ptr::null(),
            )
        };
        outcome(answer.into())
    }

    fn bpf() -> Result<String, String> {
        // SAFETY: a null attribute pointer of zero size, which the kernel
        // refuses before reading.
        outcome(unsafe { libc::syscall(libc::SYS_bpf, 0, ptr::null_mut::<u8>(), 0) })
    }

    // The request with its high bits set, which the kernel truncates and a
    // filter comparing the whole register would miss; stdin is `/dev/null`,
    // which answers `ENOTTY` where nothing stands in the way.
    fn ioctl(request: &str) -> Result<String, String> {
        let request: u64 = match request {
            "tiocsti" => 0x5412 | 0xffff_ffff_0000_0000,
            "tioclinux" => 0x541C,
            other => return Err(format!("unknown request `{other}`")),
        };
        let byte: libc::c_char = 0;
        // SAFETY: the raw syscall carries the register whole; the argument
        // points at one live byte.
        outcome(unsafe { libc::syscall(libc::SYS_ioctl, 0, request, &raw const byte) })
    }

    // Too small a parameter block, which the kernel refuses with `EINVAL`
    // before forking, so the row never creates a process where the filter
    // is not in the way.
    fn clone3() -> Result<String, String> {
        let args = [0u64; 11];
        // SAFETY: the block is live and the size names less than it.
        outcome(unsafe { libc::syscall(libc::SYS_clone3, args.as_ptr(), 8usize) })
    }

    fn thread() -> Result<String, String> {
        std::thread::spawn(|| 1 + 1)
            .join()
            .map(|sum| format!("thread joined with {sum}"))
            .map_err(|panic| format!("the thread panicked: {panic:?}"))
    }
}
