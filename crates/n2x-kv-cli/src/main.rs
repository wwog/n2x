//! `n2x-kv`：查看 n2x-kv 文件里到底存了什么。
//!
//! 输出分三种：给人看的表格、`key = value` 的平铺文本、以及保留 JSON
//! 结构的 JSON。三者都基于 [`read_snapshot`]，只读扫描，不会压缩、截断
//! 或改动被查看的文件。

use std::fmt::Write as _;
use std::path::PathBuf;

use clap::{Parser, ValueEnum};
use n2x_kv::{Snapshot, SnapshotEntry, read_snapshot};
use serde_json::Value as Json;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

/// 表格里键列的最大显示宽度。
const MAX_KEY_WIDTH: usize = 40;

#[derive(Parser, Debug)]
#[command(
    name = "n2x-kv",
    version,
    about = "Inspect and export the contents of an n2x-kv store file",
    long_about = "Inspect and export the contents of an n2x-kv store file.\n\n\
The file is opened read-only: no lock is taken, no compaction runs, and a torn \
tail is reported instead of being truncated."
)]
struct Cli {
    /// Path to the n2x-kv file
    file: PathBuf,

    /// Output format
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    format: OutputFormat,

    /// Only show keys that start with this prefix
    #[arg(short, long)]
    prefix: Option<String>,

    /// Only show this exact key; exits with status 1 when it is absent
    #[arg(short = 'k', long, conflicts_with = "prefix")]
    key: Option<String>,

    /// Do not truncate values in table output
    #[arg(short, long)]
    full: bool,

    /// Maximum width of the value column in table output
    #[arg(long, default_value_t = 48, value_name = "COLS")]
    max_value_width: usize,

    /// Print file statistics to stderr
    #[arg(long)]
    stats: bool,

    /// Keep JSON-looking values as strings instead of embedding them
    #[arg(long)]
    no_json_values: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum OutputFormat {
    /// Aligned table: key, type, size, updates, preview
    Table,
    /// One `key = value` line per entry
    Kv,
    /// A JSON object; object/array values are embedded as JSON
    Json,
}

fn main() {
    let cli = Cli::parse();
    if let Err(error) = run(&cli) {
        eprintln!("n2x-kv: {error}");
        std::process::exit(1);
    }
}

fn run(cli: &Cli) -> Result<(), Box<dyn std::error::Error>> {
    let snapshot = read_snapshot(&cli.file)?;
    if cli.stats {
        print_stats(&snapshot);
    }

    let entries: Vec<&SnapshotEntry> = snapshot
        .entries
        .iter()
        .filter(|entry| matches_filter(entry, cli))
        .collect();

    if let Some(key) = &cli.key
        && entries.is_empty()
    {
        return Err(format!("key {key:?} not found in {}", snapshot.path.display()).into());
    }

    match cli.format {
        OutputFormat::Table => print_table(&entries, cli),
        OutputFormat::Kv => print_kv(&entries),
        OutputFormat::Json => print_json(&entries, cli)?,
    }
    Ok(())
}

fn matches_filter(entry: &SnapshotEntry, cli: &Cli) -> bool {
    if let Some(key) = &cli.key {
        return entry.key == *key;
    }
    if let Some(prefix) = &cli.prefix {
        return entry.key.starts_with(prefix);
    }
    true
}

fn print_stats(snapshot: &Snapshot) {
    let used = if snapshot.file_bytes == 0 {
        0.0
    } else {
        snapshot.valid_bytes as f64 / snapshot.file_bytes as f64 * 100.0
    };
    eprintln!("path:     {}", snapshot.path.display());
    eprintln!("entries:  {}", snapshot.entries.len());
    eprintln!("frames:   {}", snapshot.frames);
    eprintln!("records:  {}", snapshot.records);
    eprintln!(
        "file:     {} ({} bytes)",
        human_bytes(snapshot.file_bytes),
        snapshot.file_bytes
    );
    eprintln!(
        "valid:    {} ({} bytes, {used:.1}% of file)",
        human_bytes(snapshot.valid_bytes),
        snapshot.valid_bytes
    );
    eprintln!("live:     {}", human_bytes(snapshot.live_bytes()));
    if snapshot.tail_truncated {
        eprintln!(
            "tail:     torn; {} bytes after the last valid frame are ignored",
            snapshot.file_bytes.saturating_sub(snapshot.valid_bytes)
        );
    } else {
        eprintln!("tail:     clean");
    }
}

fn print_table(entries: &[&SnapshotEntry], cli: &Cli) {
    if entries.is_empty() {
        println!("(no entries)");
        return;
    }

    let max_width = cli.max_value_width.max(8);
    let rows: Vec<Row> = entries
        .iter()
        .map(|entry| {
            let key = truncate_display(&single_line(&entry.key), MAX_KEY_WIDTH);
            let (kind, value) = preview(&entry.value, !cli.no_json_values, cli.full, max_width);
            Row {
                key_width: display_width(&key),
                key,
                kind,
                size: human_bytes(entry.value.len() as u64),
                updates: entry.updates.to_string(),
                value,
            }
        })
        .collect();

    let key_width = rows
        .iter()
        .map(|row| row.key_width)
        .chain(std::iter::once(display_width("KEY")))
        .max()
        .unwrap_or(3);
    let kind_width = rows
        .iter()
        .map(|row| display_width(row.kind))
        .chain(std::iter::once(display_width("TYPE")))
        .max()
        .unwrap_or(4);
    let size_width = rows
        .iter()
        .map(|row| display_width(&row.size))
        .chain(std::iter::once(display_width("SIZE")))
        .max()
        .unwrap_or(4);
    let updates_width = rows
        .iter()
        .map(|row| display_width(&row.updates))
        .chain(std::iter::once(display_width("UPDATES")))
        .max()
        .unwrap_or(7);

    println!(
        "{}  {}  {}  {}  VALUE",
        pad_right("KEY", key_width),
        pad_right("TYPE", kind_width),
        pad_left("SIZE", size_width),
        pad_left("UPDATES", updates_width),
    );
    for row in &rows {
        println!(
            "{}  {}  {}  {}  {}",
            pad_right(&row.key, key_width),
            pad_right(row.kind, kind_width),
            pad_left(&row.size, size_width),
            pad_left(&row.updates, updates_width),
            row.value,
        );
    }
}

struct Row {
    key: String,
    key_width: usize,
    kind: &'static str,
    size: String,
    updates: String,
    value: String,
}

fn print_kv(entries: &[&SnapshotEntry]) {
    for entry in entries {
        println!(
            "{} = {}",
            escape_kv(&single_line(&entry.key)),
            kv_value(&entry.value)
        );
    }
}

fn print_json(entries: &[&SnapshotEntry], cli: &Cli) -> Result<(), Box<dyn std::error::Error>> {
    let mut object = serde_json::Map::with_capacity(entries.len());
    for entry in entries {
        object.insert(
            entry.key.clone(),
            json_value(&entry.value, !cli.no_json_values),
        );
    }
    println!("{}", serde_json::to_string_pretty(&Json::Object(object))?);
    Ok(())
}

/// 值的展示类型与预览文本。
fn preview(value: &[u8], parse_json: bool, full: bool, max_width: usize) -> (&'static str, String) {
    if value.is_empty() {
        return ("empty", String::new());
    }
    match std::str::from_utf8(value) {
        Ok(text) => {
            if parse_json && let Some(json) = structured_json(text) {
                let compact = serde_json::to_string(&json).unwrap_or_else(|_| text.to_owned());
                let rendered = single_line(&compact);
                return ("json", fit(&rendered, full, max_width));
            }
            ("str", fit(&single_line(text), full, max_width))
        }
        Err(_) => ("bin", hex_preview(value, full, max_width)),
    }
}

/// 只把对象与数组当作结构化 JSON；`"42"`、`"true"` 这类标量仍按文本展示，
/// 避免查看器把字符串悄悄变成数字。
fn structured_json(text: &str) -> Option<Json> {
    let trimmed = text.trim_start();
    if !trimmed.starts_with('{') && !trimmed.starts_with('[') {
        return None;
    }
    serde_json::from_str(text).ok()
}

fn json_value(value: &[u8], parse_json: bool) -> Json {
    match std::str::from_utf8(value) {
        Ok(text) => {
            if parse_json && let Some(json) = structured_json(text) {
                return json;
            }
            Json::String(text.to_owned())
        }
        Err(_) => {
            let mut object = serde_json::Map::with_capacity(1);
            object.insert("$binary_hex".into(), Json::String(hex(value)));
            Json::Object(object)
        }
    }
}

fn kv_value(value: &[u8]) -> String {
    match std::str::from_utf8(value) {
        Ok(text) => escape_kv(text),
        Err(_) => format!("0x{}", hex(value)),
    }
}

/// 转义反斜杠与控制字符，保证导出的每一行仍是单个 `key = value`。
fn escape_kv(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => {
                write!(out, "\\u{{{:x}}}", c as u32).expect("writing to a String");
            }
            c => out.push(c),
        }
    }
    out
}

/// 表格用的单行化：控制字符显示为转义序列，而不是真的换行。
fn single_line(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => {
                write!(out, "\\u{{{:x}}}", c as u32).expect("writing to a String");
            }
            c => out.push(c),
        }
    }
    out
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(out, "{byte:02x}").expect("writing to a String");
    }
    out
}

fn hex_preview(bytes: &[u8], full: bool, max_width: usize) -> String {
    let shown = if full {
        bytes.len()
    } else {
        bytes.len().min(max_width.saturating_sub(2) / 2)
    };
    let mut out = String::with_capacity(2 + shown * 2 + 1);
    out.push_str("0x");
    out.push_str(&hex(&bytes[..shown]));
    if shown < bytes.len() {
        out.push('…');
    }
    out
}

fn fit(text: &str, full: bool, max_width: usize) -> String {
    if full {
        text.to_owned()
    } else {
        truncate_display(text, max_width)
    }
}

/// 按终端显示宽度截断（CJK 字符占两列），超出时以 `…` 结尾。
fn truncate_display(text: &str, max_width: usize) -> String {
    if display_width(text) <= max_width {
        return text.to_owned();
    }
    let budget = max_width.saturating_sub(1);
    let mut out = String::new();
    let mut used = 0;
    for ch in text.chars() {
        let width = UnicodeWidthChar::width(ch).unwrap_or(0);
        if used + width > budget {
            break;
        }
        out.push(ch);
        used += width;
    }
    out.push('…');
    out
}

fn display_width(text: &str) -> usize {
    UnicodeWidthStr::width(text)
}

fn pad_right(text: &str, width: usize) -> String {
    let used = display_width(text);
    if used >= width {
        text.to_owned()
    } else {
        format!("{text}{}", " ".repeat(width - used))
    }
}

fn pad_left(text: &str, width: usize) -> String {
    let used = display_width(text);
    if used >= width {
        text.to_owned()
    } else {
        format!("{}{text}", " ".repeat(width - used))
    }
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["KiB", "MiB", "GiB", "TiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64 / 1024.0;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}
