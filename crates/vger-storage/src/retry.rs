use std::time::Duration;

use crate::RetryConfig;

/// Retry a closure on transient `ureq::Error`s with exponential backoff + jitter.
///
/// Used by S3 and REST backends which share the same HTTP error model.
#[allow(clippy::result_large_err)]
pub fn retry_http<T>(
    config: &RetryConfig,
    op_name: &str,
    backend_label: &str,
    f: impl Fn() -> std::result::Result<T, ureq::Error>,
) -> std::result::Result<T, ureq::Error> {
    let mut delay_ms = config.retry_delay_ms;
    let mut last_err = None;

    for attempt in 0..=config.max_retries {
        if attempt > 0 {
            let jitter = rand::random::<u64>() % delay_ms.max(1);
            std::thread::sleep(Duration::from_millis(delay_ms + jitter));
            delay_ms = (delay_ms * 2).min(config.retry_max_delay_ms);
        }
        match f() {
            Ok(val) => return Ok(val),
            Err(e) if is_retryable_http(&e) && attempt < config.max_retries => {
                tracing::warn!(
                    "{backend_label} {op_name}: transient error (attempt {}/{}), retrying: {e}",
                    attempt + 1,
                    config.max_retries,
                );
                last_err = Some(e);
            }
            Err(e) => return Err(e),
        }
    }
    Err(last_err.unwrap())
}

/// Whether an HTTP error is transient and worth retrying.
pub fn is_retryable_http(err: &ureq::Error) -> bool {
    match err {
        ureq::Error::Transport(_) => true,
        ureq::Error::Status(code, _) => *code == 429 || *code >= 500,
    }
}
