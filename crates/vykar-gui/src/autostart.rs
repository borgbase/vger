use auto_launch::AutoLaunchBuilder;

/// Query whether the OS autostart entry for Vykar exists.
pub fn is_enabled() -> Result<bool, Box<dyn std::error::Error>> {
    Ok(build()?.is_enabled()?)
}

/// Register or remove the OS autostart entry.
pub fn set_enabled(enabled: bool) -> Result<(), Box<dyn std::error::Error>> {
    let auto = build()?;
    if enabled {
        auto.enable()?;
    } else {
        auto.disable()?;
    }
    Ok(())
}

/// Decide whether the main window should be hidden at startup.
pub fn should_start_hidden(start_in_background: Option<bool>, autostart_enabled: bool) -> bool {
    start_in_background.unwrap_or(false) || autostart_enabled
}

fn resolve_exe_path() -> Result<String, Box<dyn std::error::Error>> {
    // AppImage: the FUSE mount path is transient; use the real .AppImage path.
    if let Ok(appimage) = std::env::var("APPIMAGE") {
        return Ok(appimage);
    }
    Ok(std::env::current_exe()?.display().to_string())
}

fn build() -> Result<auto_launch::AutoLaunch, Box<dyn std::error::Error>> {
    let path = resolve_exe_path()?;
    Ok(AutoLaunchBuilder::new()
        .set_app_name("Vykar Backup")
        .set_app_path(&path)
        .build()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_exe_path_appimage_env() {
        // Temporarily set APPIMAGE and verify it takes precedence.
        let key = "APPIMAGE";
        let prev = std::env::var(key).ok();
        std::env::set_var(key, "/opt/Vykar-1.0.AppImage");
        let result = resolve_exe_path().unwrap();
        assert_eq!(result, "/opt/Vykar-1.0.AppImage");
        // Restore.
        match prev {
            Some(v) => std::env::set_var(key, v),
            None => std::env::remove_var(key),
        }
    }

    #[test]
    fn resolve_exe_path_fallback() {
        let key = "APPIMAGE";
        let prev = std::env::var(key).ok();
        std::env::remove_var(key);
        let result = resolve_exe_path().unwrap();
        // Should return something non-empty (the test binary path).
        assert!(!result.is_empty());
        if let Some(v) = prev {
            std::env::set_var(key, v);
        }
    }

    #[test]
    fn should_start_hidden_matrix() {
        assert!(!should_start_hidden(None, false));
        assert!(!should_start_hidden(Some(false), false));
        assert!(should_start_hidden(Some(true), false));
        assert!(should_start_hidden(None, true));
        assert!(should_start_hidden(Some(false), true));
        assert!(should_start_hidden(Some(true), true));
    }
}
