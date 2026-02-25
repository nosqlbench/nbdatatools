# Slabtastic Implementation

This document describes what the implementation actually provides. The authoritative
design spec lives in `design/slabtastic.md` — this file documents the realized subset and
implementation-specific details.

---

## SlabReader

**Class**: `io.nosqlbench.slabtastic.SlabReader`

### Bootstrap sequence

1. Open the file via `AsynchronousFileChannel` (read-only).
2. Read the last 16 bytes as a `PageFooter`; validate it.
3. Determine the file structure from the tail page type:
   - **Type 1 (pages page)**: single-namespace file — load the pages page and build one
     namespace data structure for the default namespace.
   - **Type 3 (namespaces page)**: multi-namespace file — read the namespaces page, parse
     `NamespacesPageEntry` records, then load each namespace's pages page independently.
4. For each namespace: parse `PagesPageEntry` records from its pages page, sort by
   `startOrdinal`.
5. Pre-cache: for every indexed data page, read its 8-byte header and 16-byte footer to
   capture `pageSize` and `recordCount` into parallel lists, avoiding per-`get` I/O for
   metadata.

### API

| Method | Description |
|---|---|
| `get(long ordinal)` | Returns `Optional<ByteBuffer>` — default namespace lookup. |
| `get(String ns, long ordinal)` | Returns `Optional<ByteBuffer>` — namespace-aware lookup; empty for missing ordinals, throws `IllegalArgumentException` for unknown namespaces. |
| `pages()` | Returns `List<PageSummary>` for the default namespace. |
| `pages(String ns)` | Returns `List<PageSummary>` for the specified namespace. |
| `pageCount()` | Number of data pages in the default namespace. |
| `pageCount(String ns)` | Number of data pages in the specified namespace. |
| `recordCount()` | Sum of record counts in the default namespace. |
| `recordCount(String ns)` | Sum of record counts in the specified namespace. |
| `namespaces()` | Returns `Set<String>` of all namespace names. |
| `fileSize()` | Total file size in bytes. |
| `close()` | Closes the underlying channel. |

### Lookup strategy

`get(String, long)` performs a binary search over the sorted index for the specified namespace
to find the page with the largest `startOrdinal <= ordinal`. It then computes a local index
(`ordinal - startOrdinal`) and bounds-checks it against the pre-cached `recordCount`. If the
ordinal falls within range, the full page is read from disk and parsed; otherwise
`Optional.empty()` is returned.

The local index computation uses long-space arithmetic to avoid overflow when ordinals span
the full 5-byte signed range (-2^39 to 2^39-1).

Namespace-aware methods (`get(String, long)`, `pages(String)`, `pageCount(String)`,
`recordCount(String)`) throw `IllegalArgumentException` for unknown namespaces. The
no-arg convenience methods (`get(long)`, `pages()`, etc.) always operate on the default
namespace, which is present in every valid file.

---

## SlabWriter

**Class**: `io.nosqlbench.slabtastic.SlabWriter`

### SlabWriterConfig

`SlabWriter.SlabWriterConfig` is a record controlling page layout:

| Field | Type | Default | Description |
|---|---|---|---|
| `preferredPageSize` | int | 65536 | Target page size; must be >= `minPageSize` |
| `minPageSize` | int | 512 | Minimum page size and alignment granularity; must be >= 512 and a multiple of 512 |
| `pageAlignment` | boolean | false | When true, pages are padded to `minPageSize` multiples; when false, pages are rounded to `PAGE_ALIGNMENT` (512) multiples |
| `maxPageSize` | int | `Integer.MAX_VALUE` | Maximum allowed page size; must be >= `preferredPageSize`. An aligned page exceeding this limit causes an error during flush. |

### Construction (new file)

- `SlabWriter(Path, SlabWriterConfig)`: create a new file with the specified configuration.
- `SlabWriter(Path, int)`: convenience constructor delegating to config with defaults.
- `SlabWriter.createWithBufferNaming(Path, SlabWriterConfig)`: write to `<target>.buffer`,
  atomically rename to target on successful close. On failure, the buffer file is deleted.
- Opens a `FileChannel` with WRITE, CREATE, TRUNCATE_EXISTING.

### Construction (append mode)

`SlabWriter.openForAppend(Path, SlabWriterConfig)` (or `openForAppend(Path, int)`) opens an
existing slabtastic file for appending:

1. Reads the existing file structure via `SlabReader` to discover all namespaces, their data
   page entries, and the maximum ordinal already present in each namespace.
2. Opens a `FileChannel` in write mode (no truncate).
3. Positions the write cursor at the end of the file — new data pages are appended after the
   old pages page, which becomes dead data.
4. Pre-seeds the internal per-namespace page index with existing entries so the close-time
   pages page(s) reference both old and new data pages.

Ordinals written after opening in append mode must be strictly greater than the maximum
ordinal already in the file for their respective namespace.

### Ordinal contract

- Ordinals must be **strictly ascending** within each namespace (no duplicates, no descending).
- Must be within 5-byte signed range: [-2^39, 2^39-1].
- Different namespaces have independent ordinal spaces.

### Page flush triggers

A new page is flushed when either:

1. **Ordinal gap**: the incoming ordinal is not `previousOrdinal + 1` (ordinals within a
   page must be contiguous).
2. **Size overflow**: adding the record would cause the 512-aligned page size to exceed
   `preferredPageSize`.

### Close sequence

1. Flush any remaining buffered records in each namespace as a final data page.
2. Determine layout:
   - **Single default namespace**: write one pages page (type 1) — backward compatible.
   - **Multiple namespaces** (or a single non-default namespace): write one pages page per
     namespace, then a namespaces page (type 3) at EOF.
3. `channel.force(false)` to sync metadata.
4. Close the channel.

Close is **idempotent** — calling `close()` on an already-closed writer is a no-op.

### Namespace support

Use `write(String, long, byte[])` to write records to a named namespace. The no-arg
`write(long, byte[])` writes to the default namespace (empty string). Namespaces are created
lazily: the first write to a new namespace name allocates a new namespace index.

### Flushing discipline

Each data page flush calls `channel.force(false)` immediately after writing, ensuring that
page data is durable before subsequent pages are written. This supports the spec requirement
that writers flush buffers at slab boundaries.

---

## Binary layout types

### PageHeader (`io.nosqlbench.slabtastic.PageHeader`)

8-byte record: `[magic:4][page_size:4]`, all little-endian.

- Magic = `0x42414C53` LE, spelling "SLAB" in file byte order.
- `pageSize` = total page size including header, records, offsets, footer, and padding.

### PageFooter (`io.nosqlbench.slabtastic.PageFooter`)

16-byte record:

```
[start_ordinal:5][record_count:3][page_size:4][page_type:1][namespace_index:1][footer_length:2]
```

All little-endian. The 5-byte ordinal is sign-extended on read (MSB carries sign). The
3-byte record count is unsigned.

The `namespace_index` byte (byte 13) identifies which namespace this page belongs to. This
byte was formerly called `version`; existing files with version=1 are transparently read as
namespace_index=1 (the default namespace). Namespace index 0 is reserved and invalid.

Validation enforces: non-zero namespace index, valid page type (1, 2, or 3), page size >= 512,
page size divisible by 512, footer length == 16.

### PagesPageEntry (`io.nosqlbench.slabtastic.PagesPageEntry`)

16-byte record: `[start_ordinal:8][offset:8]`, little-endian. Implements `Comparable` for
ordinal-based sorting.

### NamespacesPageEntry (`io.nosqlbench.slabtastic.NamespacesPageEntry`)

Variable-length record: `[namespace_index:1][name_length:1][name_bytes:N][pages_page_offset:8]`,
little-endian. Maps a namespace to its pages-page offset within the file.

- `namespace_index`: 1 byte (1-127; 0 is reserved)
- `name_length`: 1 byte (UTF-8 byte length of name)
- `name_bytes`: N bytes (UTF-8 encoded name, max 128 bytes)
- `pages_page_offset`: 8 bytes LE (file offset of this namespace's pages page)

### SlabPage (`io.nosqlbench.slabtastic.SlabPage`)

In-memory representation of a page. Layout on disk:

```
[header:8][records...][gap][offsets:(N+1)*4][footer:16]
```

Total size is always rounded up to the nearest 512-byte multiple. Offsets use a fence-post
scheme (N+1 entries for N records) with each entry being a 4-byte LE int offset from the
page start. The first offset always equals `HEADER_SIZE` (8).

Each page carries a `namespaceIndex` that is written into its footer. The 3-arg constructor
defaults to `NAMESPACE_DEFAULT`; the 4-arg constructor accepts an explicit namespace index.

### SlabConstants (`io.nosqlbench.slabtastic.SlabConstants`)

Interface defining format constants:

| Constant | Value | Description |
|---|---|---|
| `MAGIC` | `0x42414C53` | Header magic (LE) |
| `HEADER_SIZE` | 8 | Page header bytes |
| `FOOTER_V1_SIZE` | 16 | Footer bytes |
| `PAGE_TYPE_INVALID` | 0 | Invalid page type |
| `PAGE_TYPE_PAGES_PAGE` | 1 | Index page |
| `PAGE_TYPE_DATA` | 2 | Data page |
| `PAGE_TYPE_NAMESPACES_PAGE` | 3 | Namespaces index page |
| `NAMESPACE_INVALID` | 0 | Invalid namespace index (reserved) |
| `NAMESPACE_DEFAULT` | 1 | Default namespace index |
| `NAMESPACE_MAX_NAME_LENGTH` | 128 | Maximum namespace name in bytes |
| `VERSION_INVALID` | 0 | **Deprecated** — alias for `NAMESPACE_INVALID` |
| `VERSION_1` | 1 | **Deprecated** — alias for `NAMESPACE_DEFAULT` |
| `MIN_PAGE_SIZE` | 512 | Minimum page size |
| `MAX_PAGE_SIZE` | `0xFFFFFFFFL` | Maximum page size (2^32-1) |
| `MAX_ORDINAL` | 2^39-1 | 5-byte signed max |
| `MIN_ORDINAL` | -2^39 | 5-byte signed min |
| `MAX_RECORD_COUNT` | 2^24-1 | 3-byte unsigned max |
| `OFFSET_ENTRY_SIZE` | 4 | Bytes per offset entry |
| `PAGES_PAGE_RECORD_SIZE` | 16 | Bytes per pages-page entry |
| `PAGE_ALIGNMENT` | 512 | Alignment granularity |

---

## CLI commands

All commands live in `io.nosqlbench.slabtastic.cli` and use picocli. The top-level entry
point is `CMD_slab`.

### `slab analyze <file> [-v] [--samples N] [--sample-percent P] [-n ns]`

Analyzes a slabtastic file and reports rich statistics:

- File size, page count, total record count, ordinal range
- **Content type detection**: samples records and scans for JSON parsability, null bytes,
  newlines, etc. Reports detected type (text, cstrings, json, jsonl, csv, tsv, yaml, or binary).
- **Record size statistics**: samples records, computes min/avg/max, prints text histogram
- **Page size statistics**: min/avg/max page size, text histogram
- **Page utilization**: active bytes vs page size ratio, min/avg/max, histogram
- **Ordinal monotonicity** (full walk): "strictly monotonic", "monotonic with sparse gaps",
  or "non-monotonic"

Sampling defaults to min(1000, 1% of total). Use `--samples` or `--sample-percent` to
override.

With `-v`, prints a per-page table (start ordinal, record count, page size, file offset).

With `--namespace` / `-n`, operates on the specified namespace instead of the default.

- Exit 0: success
- Exit 1: error

### `slab check <file> [-v]`

Validates structural integrity of a slabtastic file using three passes:

**Pass 1 — Index-driven**: reads the tail footer, locates the pages page (or namespaces page),
and validates each indexed data page:

- File size minimum (>= 16 bytes)
- Tail footer validity (namespace index, page type, alignment, footer length)
- Pages-page or namespaces-page structure
- Per data page: magic bytes, header/footer agreement, footer field validity, offset table
  monotonicity, offset bounds, ordinal range, no overlap with pages page
- Index-level: no entries past EOF, no overlapping page regions, no duplicate start ordinals
- Multi-namespace files: namespaces page structure, cross-namespace page overlap detection

**Pass 2 — Forward traversal**: walks from offset 0 using page headers to jump page-by-page
through the file. At each page: reads the header and footer, validates magic bytes and
structure. Verifies the traversal ends exactly at the file size and the last page is a pages
page or namespaces page.

**Pass 3 — Cross-check**: verifies that every index entry appears in the forward traversal
and every forward-traversal data page appears in the index. Detects orphan pages and missing
index entries.

With `-v`, prints per-page progress.

- Exit 0: file is valid
- Exit 1: errors found
- Exit 2: cannot read file

### `slab get <file> -o <ordinals|ranges> [-f ascii|hex|raw|utf8|json|jsonl] [--as-hex] [--as-base64] [-n ns]`

Extracts records by comma-separated ordinals or range specifiers. Each `-o` element may be a
bare ordinal (e.g. `42`) or a range specifier (e.g. `0..9`, `[5,10)`, `[3]`). Ranges are
expanded into individual ordinal lookups. Output formats: ascii (default), hex dump, raw
bytes, UTF-8 string, JSON-encoded string, or JSONL (one JSON-encoded string per line). Missing
ordinals are reported to stderr.

Output overrides `--as-hex` and `--as-base64` render record bytes in hex or base64 with
trailing newlines, regardless of the format setting. These are mutually exclusive with each
other.

With `--namespace` / `-n`, queries the specified namespace instead of the default.

- Exit 0: all ordinals found
- Exit 1: some ordinals missing
- Exit 2: error

### `slab rewrite <source> <dest> [--preferred-page-size N] [--min-page-size N] [--page-alignment] [-f] [--skip-check] [--progress] [-n ns]`

Rewrites a slabtastic file into a new file with clean page alignment, monotonic ordinal
order, and no unreferenced pages. This command subsumes the former `repack` and `reorder`
commands.

By default, the source file is validated for structural consistency before reading. Use
`--skip-check` to bypass validation. The command always produces a clean output file — there
is no short-circuit for files that are already ordered.

The destination file is written to `<dest>.buffer` and atomically renamed on success.

With `--namespace` / `-n`, rewrites only the specified namespace.
With `--progress`, prints ongoing record counts to stderr.

- Exit 0: success
- Exit 1: error

### `slab append <target> --from <source> [--preferred-page-size N] [--min-page-size N] [--page-alignment] [--progress] [-n ns]`

Appends all records from a source slabtastic file onto the end of a target file. The source
file is validated for structural integrity before any records are read. Opens the target in
append mode: existing data pages are preserved, new data pages are written after the old
pages page, and a new pages page is written at the end. All source ordinals must be strictly
greater than the maximum ordinal in the target.

With `--namespace` / `-n`, appends to/from the specified namespace.
With `--progress`, prints ongoing record counts to stderr.

- Exit 0: success
- Exit 1: error

### `slab import <target> --from <source> [options] [-n ns]`

Imports records from a non-slab source file into a slabtastic file. Supports text
(newline-terminated), cstrings (null-terminated), and structured record formats.

Options:
- `--force` / `-f`: overwrite target if it already exists (when not in append mode)
- `--preferred-page-size N` (alias `--page-size`): preferred page size in bytes (default: 65536)
- `--min-page-size N`: minimum page size and alignment granularity (default: 512)
- `--page-alignment`: align pages to preferred page size boundaries
- `--start-ordinal N`: starting ordinal for imported records (default: 0, or auto-detected
  in append mode)
- `--format <fmt>`: source format — `text`, `cstrings`, `slab`, `json`, `jsonl`, `csv`,
  `tsv`, `yaml`
- `--append`: append to target if it already exists
- `--progress`: print ongoing record counts to stderr
- `--namespace` / `-n`: namespace to import into (default: default namespace)

New files are written to `<target>.buffer` and atomically renamed on success (non-append mode).

Format auto-detection:
1. If `--format` is set explicitly → use it
2. Check source file extension against well-known map:
   `.txt` → text, `.slab` → slab, `.json` → json, `.jsonl` → jsonl, `.csv` → csv,
   `.tsv` → tsv, `.yaml`/`.yml` → yaml
3. Otherwise scan first 8192 bytes: `\0` → cstrings; `\n` → text
4. If neither found → error requiring explicit `--format`

Append mode behavior:
- `--append` + target exists → appends to existing file, auto-detecting start ordinal from
  existing max ordinal + 1 (unless `--start-ordinal` is explicitly set)
- `--append` + target doesn't exist → creates new file normally
- No `--append` + target exists → requires `--force`

Delimiters are included in the record data. Trailing content without a delimiter is
imported as a final record.

- Exit 0: success
- Exit 1: error

### `slab export <source> [--to <output>] [--range <range>] [--format <fmt>] [--skip-validation] [-f] [--preferred-page-size N] [--min-page-size N] [--page-alignment] [--progress] [--as-hex] [--as-base64] [-n ns]`

Exports records from a slabtastic file to a file or stdout.

Options:
- `--to <path>`: output file; stdout if omitted
- `--range <range>`: ordinal range filter (formats: `n`, `m..n`, `[m,n)`, `[m,n]`, `(m,n)`, `(m,n]`, `[n]`)
- `--format <fmt>`: output format (default: `raw`). Choices: `raw`, `text`, `cstrings`, `slab`, `json`, `jsonl`, `csv`, `tsv`, `yaml`, `hex`, `utf8`, `ascii`
- `--skip-validation`: disable parsability checking for structured formats
- `--force` / `-f`: overwrite output if it already exists
- `--preferred-page-size N` (alias `--page-size`): page size for slab format output (default: 65536)
- `--min-page-size N`: minimum page size and alignment granularity (default: 512)
- `--page-alignment`: align pages to preferred page size boundaries
- `--progress`: print ongoing record counts to stderr
- `--as-hex`: output bytes as hex with space between each byte, trailing newline per record
- `--as-base64`: output bytes as base64 with trailing newline per record
- `--namespace` / `-n`: namespace to export from (default: default namespace)

`--as-hex` and `--as-base64` are mutually exclusive with each other. When set, these
override the normal format rendering.

For `slab` format output, `--to` is required and the file is written to `<output>.buffer`
then atomically renamed on success. Other formats write to stdout by default.

- Exit 0: success
- Exit 1: error

### `slab explain <file> [-p pages] [-o ordinals] [-n ns]`

Illustrates slab page layout on the console using block-diagrammatic notation. Each matching
page is rendered as a box diagram showing:

- **Header**: magic bytes and page size
- **Records**: hex preview with ASCII sidebar, ordinal and size per record
- **Gap**: zero-fill padding size
- **Offsets**: fence-post offset array entries
- **Footer**: all footer fields decoded

Selection options:
- `--pages` / `-p`: comma-separated 0-based page indices
- `--ordinals` / `-o`: ordinal range filter (pages with overlapping ranges are shown)
- `--namespace` / `-n`: operate on the specified namespace

If neither `--pages` nor `--ordinals` is specified, all data pages are shown. Structural
pages (pages page and namespaces page) are always rendered at the end.

For pages-page entries, records are decoded as `[ordinal N → offset N]`. For namespaces-page
entries, records are decoded as `[index N: "name" → offset N]`.

- Exit 0: success
- Exit 1: error

### `slab namespaces <file>`

Lists all namespaces in a slabtastic file with their index, name, page count, record count,
and ordinal range. Single-namespace files show just the default namespace.

- Exit 0: success
- Exit 1: error

---

## What's deferred from the design spec

The following features are described in the design spec as aspirational and are **not**
implemented:

- **Streaming get**: bulk read interface returning batched records
- **Sink/callback reader**: provide a sink + callback, reader writes all data then completes
  a future
- **Async iterable append**: writer accepts an iterable + callback for asynchronous buffering
- **Progress decorator interfaces**: futures implementing a decorator for polling status and
  progress
- **Interior mutation**: in-place editing of self-terminating or fixed-size records
- **Concurrent reader streaming**: incremental file observation by watching for page writes
- **Configurable page sizing**: ~~deferred~~ — now implemented via `SlabWriterConfig` with
  `preferredPageSize`, `minPageSize`, and `pageAlignment` parameters

---

## Spec compliance audit

| Spec section | Status | Notes |
|---|---|---|
| Page Magic = UTF-8 "SLAB" + page length | Compliant | `MAGIC = 0x42414C53` LE, header is 8 bytes |
| Page structure `[header][records][offsets][footer]` | Compliant | `SlabPage.toByteBuffer()` |
| Footer `[ord:5][count:3][size:4][type:1][ns:1][len:2]` | Compliant | `PageFooter` read/write |
| Footer >= 16 bytes, padded to nearest 16 | Compliant | `FOOTER_V1_SIZE = 16` |
| Header/footer page size agreement | Compliant | Checked in `SlabPage.parseFrom()` and `CMD_slab_check` |
| Pages are multiples of 512 | Compliant | `roundUp(minSize, PAGE_ALIGNMENT)` |
| Min 512, max 2^32 | Compliant | Constants defined; writer validates min; footer validates alignment |
| Pages page `[ord:8][offset:8]` LE, sorted | Compliant | `PagesPageEntry` + reader sorts |
| File ends with pages page or namespaces page | Compliant | Writer writes it last; reader asserts type on bootstrap |
| Namespaces page `[nsIdx:1][nameLen:1][name:N][offset:8]` | Compliant | `NamespacesPageEntry` read/write |
| Namespace index in footer byte 13 | Compliant | `PageFooter.namespaceIndex()` |
| Single default ns → pages page (backward compat) | Compliant | Writer close sequence branches on namespace count |
| Multi-namespace → namespaces page at EOF | Compliant | Writer writes per-ns pages pages then namespaces page |
| `get()` signals missing ordinals | Compliant | Returns `Optional.empty()` |
| Ordinal range: 5-byte signed | Compliant | `MIN_ORDINAL`/`MAX_ORDINAL` enforced in writer |
| Record count: 3-byte unsigned (0 to 2^24-1) | Compliant | `MAX_RECORD_COUNT` defined |
| Last pages page is authoritative | Compliant | Reader reads only the tail pages page |
| Append-only mode | Compliant | `SlabWriter.openForAppend()` appends after old pages page |
| CLI: analyze file stats and layout | Compliant | `slab analyze` command with sampling statistics |
| CLI: rewrite file with repack + reorder | Compliant | `slab rewrite` command with pre-flight validation |
| CLI: append data onto slab file | Compliant | `slab append` command |
| CLI: import data from non-slab files | Compliant | `slab import` with `--format` enum and `--append` |
| CLI: structured format import (JSON, JSONL, CSV, TSV, YAML) | Compliant | `slab import --format json/jsonl/csv/tsv/yaml` |
| CLI: well-known extension detection | Compliant | `.txt`, `.json`, `.jsonl`, `.csv`, `.tsv`, `.yaml`, `.yml` |
| CLI: export data from slab files | Compliant | `slab export` command |
| CLI: output overrides (--as-hex, --as-base64) | Compliant | Available on `export` and `get` |
| CLI: get output formats (ascii, json, jsonl) | Compliant | `slab get -f ascii/json/jsonl` |
| CLI: three-pass check validation | Compliant | Index-driven + forward traversal + cross-check |
| CLI: slab layout parameters | Compliant | `--preferred-page-size`, `--min-page-size`, `--page-alignment`, `--max-page-size` |
| CLI: buffer naming | Compliant | `<target>.buffer` with atomic rename on success |
| CLI: progress reporting | Compliant | `--progress` on `rewrite`, `append`, `import`, `export` |
| CLI: append pre-flight validation | Compliant | `slab append` validates source before reading |
| CLI: reusable file validation | Compliant | `SlabFileValidator` utility class |
| CLI: namespace listing | Compliant | `slab namespaces` command |
| CLI: per-command namespace selection | Compliant | `--namespace` / `-n` on all data commands |
| CLI: explain page layout diagrams | Compliant | `slab explain` with page, ordinal, and namespace filters |
