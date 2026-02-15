use std::process::{Command, Output};
use std::time::Duration;

/// Build a shell command for the current platform.
pub fn command_for_script(script: &str) -> Command {
    #[cfg(windows)]
    {
        let mut cmd = Command::new("powershell");
        cmd.arg("-NoProfile")
            .arg("-NonInteractive")
            .arg("-Command")
            .arg(script);
        cmd
    }

    #[cfg(not(windows))]
    {
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg(script);
        cmd
    }
}

pub fn run_script(script: &str) -> std::io::Result<Output> {
    command_for_script(script).output()
}

/// Run a shell script with a timeout. Returns an error if the command
/// does not complete within the given duration.
pub fn run_script_with_timeout(script: &str, timeout: Duration) -> std::io::Result<Output> {
    let mut cmd = command_for_script(script);
    run_command_with_timeout(&mut cmd, timeout)
}

/// Run an already-configured `Command` with a timeout. The command is spawned
/// with piped stdout/stderr. Returns an error if it does not complete in time.
pub fn run_command_with_timeout(cmd: &mut Command, timeout: Duration) -> std::io::Result<Output> {
    let mut child = cmd
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;

    let deadline = std::time::Instant::now() + timeout;
    let poll_interval = Duration::from_millis(100);

    loop {
        match child.try_wait()? {
            Some(status) => {
                let stdout = child
                    .stdout
                    .take()
                    .map(|mut r| {
                        let mut buf = Vec::new();
                        std::io::Read::read_to_end(&mut r, &mut buf).ok();
                        buf
                    })
                    .unwrap_or_default();
                let stderr = child
                    .stderr
                    .take()
                    .map(|mut r| {
                        let mut buf = Vec::new();
                        std::io::Read::read_to_end(&mut r, &mut buf).ok();
                        buf
                    })
                    .unwrap_or_default();
                return Ok(Output {
                    status,
                    stdout,
                    stderr,
                });
            }
            None => {
                if std::time::Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!("command timed out after {} seconds", timeout.as_secs()),
                    ));
                }
                std::thread::sleep(poll_interval);
            }
        }
    }
}
