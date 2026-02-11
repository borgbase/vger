use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Datelike, IsoWeek, Timelike, Utc};

use crate::config::RetentionConfig;
use crate::error::{VgerError, Result};
use crate::repo::manifest::ArchiveEntry;

#[derive(Debug, Clone)]
pub enum PruneDecision {
    Keep { reasons: Vec<String> },
    Prune,
}

#[derive(Debug, Clone)]
pub struct PruneEntry {
    pub archive_name: String,
    pub archive_time: DateTime<Utc>,
    pub decision: PruneDecision,
}

/// Parse a duration string like "2d", "48h", "1w", "6m", "1y".
/// Pure numeric values are treated as days (borg convention).
pub fn parse_duration(s: &str) -> Result<chrono::Duration> {
    let s = s.trim();
    if s.is_empty() {
        return Err(VgerError::Config("empty duration string".into()));
    }

    // Try pure numeric → days
    if let Ok(n) = s.parse::<i64>() {
        return Ok(chrono::Duration::days(n));
    }

    // Split into numeric part and suffix
    let (num_str, suffix) = s.split_at(
        s.find(|c: char| !c.is_ascii_digit())
            .ok_or_else(|| VgerError::Config(format!("invalid duration: '{s}'")))?,
    );
    let n: i64 = num_str
        .parse()
        .map_err(|_| VgerError::Config(format!("invalid duration number: '{num_str}'")))?;

    match suffix {
        "h" | "H" => Ok(chrono::Duration::hours(n)),
        "d" | "D" => Ok(chrono::Duration::days(n)),
        "w" | "W" => Ok(chrono::Duration::weeks(n)),
        "m" | "M" => Ok(chrono::Duration::days(n * 30)),
        "y" | "Y" => Ok(chrono::Duration::days(n * 365)),
        _ => Err(VgerError::Config(format!(
            "unknown duration suffix: '{suffix}'"
        ))),
    }
}

/// Time bucket key types for each retention rule.
type HourlyKey = (i32, u32, u32); // (year, ordinal_day, hour)
type DailyKey = (i32, u32); // (year, ordinal_day)
type WeeklyKey = (i32, u32); // (iso_year, iso_week)
type MonthlyKey = (i32, u32); // (year, month)
type YearlyKey = (i32,); // (year,)

fn hourly_key(t: &DateTime<Utc>) -> HourlyKey {
    (t.year(), t.ordinal(), t.hour())
}

fn daily_key(t: &DateTime<Utc>) -> DailyKey {
    (t.year(), t.ordinal())
}

fn weekly_key(t: &DateTime<Utc>) -> WeeklyKey {
    let iw: IsoWeek = t.iso_week();
    (iw.year(), iw.week())
}

fn monthly_key(t: &DateTime<Utc>) -> MonthlyKey {
    (t.year(), t.month())
}

fn yearly_key(t: &DateTime<Utc>) -> YearlyKey {
    (t.year(),)
}

/// Apply a bucket-based retention rule. For each new bucket encountered (up to `max_buckets`),
/// keep the newest archive in that bucket. Already-kept archives still register their bucket
/// but don't consume a keeper slot.
fn apply_bucket_rule<K: Eq + std::hash::Hash>(
    indices: &[usize],
    times: &[DateTime<Utc>],
    kept: &mut HashSet<usize>,
    reasons: &mut HashMap<usize, Vec<String>>,
    max_buckets: usize,
    key_fn: impl Fn(&DateTime<Utc>) -> K,
    rule_name: &str,
) {
    let mut seen_buckets: HashSet<K> = HashSet::new();
    let mut kept_count = 0usize;

    for &idx in indices {
        let bucket = key_fn(&times[idx]);
        if seen_buckets.contains(&bucket) {
            continue;
        }
        seen_buckets.insert(bucket);

        if kept.contains(&idx) {
            // Already kept by another rule — bucket is consumed but no new keeper needed
            reasons
                .entry(idx)
                .or_default()
                .push(format!("{rule_name} #{}", kept_count + 1));
            kept_count += 1;
        } else if kept_count < max_buckets {
            kept.insert(idx);
            reasons
                .entry(idx)
                .or_default()
                .push(format!("{rule_name} #{}", kept_count + 1));
            kept_count += 1;
        }

        if kept_count >= max_buckets {
            break;
        }
    }
}

/// Apply the retention policy to a list of archive entries.
/// Returns a PruneEntry for each archive, sorted newest-first.
pub fn apply_policy(
    archives: &[ArchiveEntry],
    policy: &RetentionConfig,
    now: DateTime<Utc>,
) -> Result<Vec<PruneEntry>> {
    if archives.is_empty() {
        return Ok(Vec::new());
    }

    // Build sorted indices (newest first)
    let mut indices: Vec<usize> = (0..archives.len()).collect();
    indices.sort_by(|&a, &b| archives[b].time.cmp(&archives[a].time));

    let times: Vec<DateTime<Utc>> = archives.iter().map(|a| a.time).collect();

    let mut kept: HashSet<usize> = HashSet::new();
    let mut reasons: HashMap<usize, Vec<String>> = HashMap::new();

    // keep_within
    if let Some(ref within_str) = policy.keep_within {
        let dur = parse_duration(within_str)?;
        let cutoff = now - dur;
        for &idx in &indices {
            if times[idx] >= cutoff {
                kept.insert(idx);
                reasons.entry(idx).or_default().push("within".into());
            }
        }
    }

    // keep_last
    if let Some(n) = policy.keep_last {
        for (i, &idx) in indices.iter().enumerate() {
            if i >= n {
                break;
            }
            kept.insert(idx);
            reasons
                .entry(idx)
                .or_default()
                .push(format!("last #{}", i + 1));
        }
    }

    // Bucket rules: hourly → daily → weekly → monthly → yearly
    if let Some(n) = policy.keep_hourly {
        apply_bucket_rule(&indices, &times, &mut kept, &mut reasons, n, hourly_key, "hourly");
    }
    if let Some(n) = policy.keep_daily {
        apply_bucket_rule(&indices, &times, &mut kept, &mut reasons, n, daily_key, "daily");
    }
    if let Some(n) = policy.keep_weekly {
        apply_bucket_rule(&indices, &times, &mut kept, &mut reasons, n, weekly_key, "weekly");
    }
    if let Some(n) = policy.keep_monthly {
        apply_bucket_rule(
            &indices, &times, &mut kept, &mut reasons, n, monthly_key, "monthly",
        );
    }
    if let Some(n) = policy.keep_yearly {
        apply_bucket_rule(
            &indices, &times, &mut kept, &mut reasons, n, yearly_key, "yearly",
        );
    }

    // Safety check: refuse if all archives would be pruned
    let prune_count = archives.len() - kept.len();
    if prune_count == archives.len() {
        return Err(VgerError::Other(
            "refusing to prune: policy would remove ALL archives".into(),
        ));
    }

    // Build results in newest-first order
    let entries: Vec<PruneEntry> = indices
        .iter()
        .map(|&idx| {
            let decision = if let Some(r) = reasons.remove(&idx) {
                PruneDecision::Keep { reasons: r }
            } else {
                PruneDecision::Prune
            };
            PruneEntry {
                archive_name: archives[idx].name.clone(),
                archive_time: archives[idx].time,
                decision,
            }
        })
        .collect();

    Ok(entries)
}
