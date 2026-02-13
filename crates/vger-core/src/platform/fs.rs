use std::fs::{FileType, Metadata};
use std::path::Path;

#[derive(Debug, Clone, Copy)]
pub struct MetadataSummary {
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
    pub device: u64,
    pub inode: u64,
    pub size: u64,
}

pub fn summarize_metadata(metadata: &Metadata, file_type: &FileType) -> MetadataSummary {
    #[cfg(unix)]
    {
        let _ = file_type;
        use std::os::unix::fs::MetadataExt;

        return MetadataSummary {
            mode: metadata.mode(),
            uid: metadata.uid(),
            gid: metadata.gid(),
            mtime_ns: metadata.mtime() * 1_000_000_000 + metadata.mtime_nsec(),
            ctime_ns: metadata.ctime() * 1_000_000_000 + metadata.ctime_nsec(),
            device: metadata.dev(),
            inode: metadata.ino(),
            size: metadata.len(),
        };
    }

    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;

        let readonly = metadata.permissions().readonly();
        let mode = if file_type.is_dir() {
            if readonly {
                0o555
            } else {
                0o755
            }
        } else if readonly {
            0o444
        } else {
            0o644
        };

        MetadataSummary {
            mode,
            uid: 0,
            gid: 0,
            mtime_ns: windows_filetime_to_unix_ns(metadata.last_write_time()),
            ctime_ns: windows_filetime_to_unix_ns(metadata.creation_time()),
            device: 0,
            inode: 0,
            size: metadata.file_size(),
        }
    }

    #[cfg(not(any(unix, windows)))]
    {
        MetadataSummary {
            mode: 0o644,
            uid: 0,
            gid: 0,
            mtime_ns: 0,
            ctime_ns: 0,
            device: 0,
            inode: 0,
            size: metadata.len(),
        }
    }
}

pub fn apply_mode(path: &Path, mode: u32) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        return std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode));
    }

    #[cfg(windows)]
    {
        let mut perms = std::fs::metadata(path)?.permissions();
        perms.set_readonly((mode & 0o200) == 0);
        std::fs::set_permissions(path, perms)
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        let _ = mode;
        Ok(())
    }
}

pub fn create_symlink(link_target: &Path, target: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        return std::os::unix::fs::symlink(link_target, target);
    }

    #[cfg(windows)]
    {
        let file_err = std::os::windows::fs::symlink_file(link_target, target).err();
        if file_err.is_none() {
            return Ok(());
        }

        match std::os::windows::fs::symlink_dir(link_target, target) {
            Ok(()) => Ok(()),
            Err(dir_err) => Err(std::io::Error::new(
                dir_err.kind(),
                format!(
                    "failed to create symlink as file ({}) and directory ({})",
                    file_err.unwrap(),
                    dir_err
                ),
            )),
        }
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = link_target;
        let _ = target;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "symlink creation is not supported on this platform",
        ))
    }
}

pub fn xattrs_supported() -> bool {
    cfg!(unix)
}

#[cfg(windows)]
fn windows_filetime_to_unix_ns(filetime_100ns: u64) -> i64 {
    // FILETIME epoch is 1601-01-01, Unix epoch is 1970-01-01.
    const EPOCH_OFFSET_100NS: i128 = 11644473600i128 * 10_000_000i128;
    let value_100ns = filetime_100ns as i128 - EPOCH_OFFSET_100NS;
    let nanos = value_100ns.saturating_mul(100);
    nanos.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}
