# n2x-kv-cli

A command-line viewer for [`n2x-kv`](../n2x-kv) files. It opens the store
**read-only**: no lock is taken, no compaction runs, and a torn log tail is
reported instead of being truncated.

## Build

```sh
cargo build --release -p n2x-kv-cli
# binary: target/release/n2x-kv

# or install it on PATH
cargo install --path crates/n2x-kv-cli
```

## Usage

```text
n2x-kv <FILE> [--format table|kv|json] [--prefix P] [--key K] [--full] [--stats]
```

### Table (default)

```text
$ n2x-kv /tmp/demo.kv --stats
path:     /tmp/demo.kv
entries:  6
frames:   7
records:  7
file:     1.0 MiB (1048576 bytes)
valid:    414 B (414 bytes, 0.0% of file)
live:     138 B
tail:     clean
KEY        TYPE   SIZE  UPDATES  VALUE
binary     bin     6 B        1  0x0001ff7fdead
config     json   53 B        1  {"features":{"sync":true,"tabs":["a","b"]},"siz…
empty      empty   0 B        1
multiline  str    28 B        1  line one\nline two\nline three
theme      str     5 B        2  light
user.name  str     6 B        1  阿达
```

- `TYPE`: `str`, `json` (object/array values), `bin`, or `empty`
- `SIZE`: value bytes; `UPDATES`: `set` records since the key was last removed
- Values are truncated to `--max-value-width` (default 48) unless `--full` is given
- `file` vs `valid`: a fresh store preallocates `initial_bytes`; 0.0% simply means
  the preallocated tail has not been used yet, not that data is corrupt

### `key = value` export

```sh
n2x-kv demo.kv --format kv
```

```text
binary = 0x0001ff7fdead
config = {"size":14,"features":{"sync":true,"tabs":["a","b"]}}
empty =
multiline = line one\nline two\nline three
theme = light
user.name = 阿达
```

Backslashes, newlines, and other control characters are escaped, so every entry
stays on one line. Values that are not valid UTF-8 are exported as `0x` + hex.

### JSON export

```sh
n2x-kv demo.kv --format json
```

```json
{
  "binary": { "$binary_hex": "0001ff7fdead" },
  "config": {
    "features": { "sync": true, "tabs": ["a", "b"] },
    "size": 14
  },
  "empty": "",
  "multiline": "line one\nline two\nline three",
  "theme": "light",
  "user.name": "阿达"
}
```

Object and array values are embedded as JSON; scalar-looking strings such as
`"42"` stay strings, so the viewer never silently changes a value's type.
Non-UTF-8 values become `{"$binary_hex": "..."}`. Use `--no-json-values` to keep
everything as strings.

### Filtering

```sh
n2x-kv demo.kv --prefix user.   # keys starting with user.
n2x-kv demo.kv --key theme     # exactly one key; exit status 1 if missing
```

## Library API

The CLI is a thin layer over `n2x_kv::read_snapshot`:

```rust
let snapshot = n2x_kv::read_snapshot("demo.kv")?;
for entry in &snapshot.entries {
    println!("{} = {} bytes", entry.key, entry.value.len());
}
```

`read_snapshot` never writes to the file. If another process is appending, the
snapshot is the consistent prefix visible at open time; a torn final frame is
dropped and reported through `Snapshot::tail_truncated`.
