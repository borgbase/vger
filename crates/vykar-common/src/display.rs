pub fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;

    if bytes >= GIB {
        format!("{:.2} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.2} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.2} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes} B")
    }
}

pub fn format_count(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            result.push(',');
        }
        result.push(c);
    }
    result
}

/// Return the terminal display width of a single character.
/// CJK and fullwidth characters occupy 2 columns; everything else occupies 1.
pub fn char_display_width(c: char) -> usize {
    let cp = c as u32;
    if matches!(cp,
        0x1100..=0x115F   // Hangul Jamo initials
        | 0x2E80..=0x303E // CJK Radicals, Kangxi, Symbols & Punctuation
        | 0x3040..=0x33FF // Hiragana, Katakana, Bopomofo, CJK Compat
        | 0x3400..=0x4DBF // CJK Extension A
        | 0x4E00..=0x9FFF // CJK Unified Ideographs
        | 0xAC00..=0xD7AF // Hangul Syllables
        | 0xF900..=0xFAFF // CJK Compat Ideographs
        | 0xFE30..=0xFE6F // CJK Compat Forms
        | 0xFF01..=0xFF60 // Fullwidth Forms
        | 0xFFE0..=0xFFE6 // Fullwidth Signs
        | 0x20000..=0x3FFFF // CJK Extensions B–G
    ) {
        2
    } else {
        1
    }
}

/// Return the terminal display width of a string (sum of character widths).
pub fn str_display_width(s: &str) -> usize {
    s.chars().map(char_display_width).sum()
}

/// Truncate a string to `max_cols` **display columns**, showing both the
/// beginning and end with `...` in the middle (e.g. `/very/l...file.txt`).
pub fn truncate_middle(input: &str, max_cols: usize) -> String {
    if max_cols == 0 {
        return String::new();
    }

    let input_width = str_display_width(input);
    if input_width <= max_cols {
        return input.to_string();
    }

    if max_cols <= 3 {
        return ".".repeat(max_cols);
    }

    let keep = max_cols - 3; // columns available for head + tail
    let head_budget = keep / 2;
    let tail_budget = keep - head_budget;

    // Build head: take chars until we'd exceed head_budget columns.
    let mut head_str = String::new();
    let mut head_used = 0;
    for c in input.chars() {
        let w = char_display_width(c);
        if head_used + w > head_budget {
            break;
        }
        head_str.push(c);
        head_used += w;
    }

    // Build tail: take chars from the end until we'd exceed tail_budget columns.
    let mut tail_chars: Vec<char> = Vec::new();
    let mut tail_used = 0;
    for c in input.chars().rev() {
        let w = char_display_width(c);
        if tail_used + w > tail_budget {
            break;
        }
        tail_chars.push(c);
        tail_used += w;
    }
    tail_chars.reverse();
    let tail_str: String = tail_chars.into_iter().collect();

    format!("{head_str}...{tail_str}")
}

#[cfg(test)]
mod tests {
    use super::{str_display_width, truncate_middle};

    #[test]
    fn truncate_middle_shows_head_and_tail() {
        let input = "/very/long/path/to/a/file.txt";
        let out = truncate_middle(input, 16);
        assert_eq!(out, "/very/...ile.txt");
        assert_eq!(str_display_width(&out), 16);
    }

    #[test]
    fn truncate_middle_returns_original_when_short() {
        assert_eq!(truncate_middle("short.txt", 32), "short.txt");
    }

    #[test]
    fn truncate_middle_handles_tiny_widths() {
        assert_eq!(truncate_middle("abcdef", 0), "");
        assert_eq!(truncate_middle("abcdef", 1), ".");
        assert_eq!(truncate_middle("abcdef", 2), "..");
        assert_eq!(truncate_middle("abcdef", 3), "...");
    }

    #[test]
    fn truncate_middle_exact_fit() {
        assert_eq!(truncate_middle("exactly10!", 10), "exactly10!");
    }

    #[test]
    fn truncate_middle_one_over() {
        let out = truncate_middle("abcdefghijk", 10);
        assert_eq!(out, "abc...hijk");
        assert_eq!(str_display_width(&out), 10);
    }

    #[test]
    fn truncate_middle_unicode() {
        let input = "aaaa\u{00e9}\u{00e9}\u{00e9}\u{00e9}bbbb"; // 12 chars, all width 1
        let out = truncate_middle(input, 10);
        assert_eq!(out, "aaa...bbbb");
        assert_eq!(str_display_width(&out), 10);
    }

    #[test]
    fn truncate_middle_cjk() {
        let input = "文件/路径/测试报告.pdf";
        let out = truncate_middle(input, 16);
        assert_eq!(out, "文件/...告.pdf");
        assert!(str_display_width(&out) <= 16);
    }

    #[test]
    fn truncate_middle_combining_diaeresis() {
        // NFC-decomposed ö = o + \u{0308}
        let input = "Documents/Children Books/Das Lo\u{0308}schflugzeug Nummer 292/Das Lo\u{0308}schflugzeug Nummer 292-EN.ai";
        let out = truncate_middle(input, 60);
        assert!(str_display_width(&out) <= 60);
        assert!(out.contains("..."));
    }
}
