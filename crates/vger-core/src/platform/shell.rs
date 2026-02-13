use std::process::{Command, Output};

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
