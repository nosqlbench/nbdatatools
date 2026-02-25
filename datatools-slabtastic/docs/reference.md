# Reference

Information-oriented specification of the slabtastic format and API.

---

## Binary format

### Byte order

All multi-byte values are stored little-endian. All file-level offset pointers are
twos-complement signed 8-byte integers. The format supports files up to 2^63 bytes.

### Page layout

```
[header:8][records...][gap][offsets:(N+1)*4][footer:16]
```

- **Header** (8 bytes): `[magic:4][page_size:4]`
  - Magic = UTF-8 "SLAB" = `0x42414C53` (LE)
  - `page_size` = total page size in bytes (header + records + offsets + footer + padding)
- **Records**: packed data with no known structure; offsets fully define boundaries
- **Offsets**: fence-post array of `(N+1)` 4-byte LE ints for N records; each is a byte
  offset from the page start; first offset always equals 8 (header size)
- **Footer** (16 bytes): see below
- **Gap**: zero-filled padding between end of records and start of offsets

### Footer layout

```
[start_ordinal:5][record_count:3][page_size:4][page_type:1][namespace_index:1][footer_length:2]
```

| Field | Size | Encoding | Range |
|---|---|---|---|
| `start_ordinal` | 5 bytes | signed 2s complement LE | −2^39 to 2^39−1 |
| `record_count` | 3 bytes | unsigned LE | 0 to 2^24−1 |
| `page_size` | 4 bytes | signed int LE | 512 to 2^32 |
| `page_type` | 1 byte | enum | 0=invalid, 1=pages page, 2=data, 3=namespaces page |
| `namespace_index` | 1 byte | unsigned | 0=invalid, 1=default, 1-127=valid |
| `footer_length` | 2 bytes | unsigned LE | always 16 |

The `namespace_index` byte (byte 13) identifies which namespace a page belongs to. This byte
was formerly called `version` in the original format; existing files with version=1 are
transparently read as namespace_index=1 (the default namespace). Page types subsume the role
previously played by the version byte — all format evolution is expressed through page types.

Footers are always at least 16 bytes, padded to the nearest 16 bytes. Header and footer
`page_size` must always agree.

### Page sizing

| Constraint | Value |
|---|---|
| Minimum page size | 512 bytes (2^9) |
| Maximum page size | 2^32 bytes |
| Alignment | multiple of 512 bytes |
| Single record exceeds page | error |

### Page types

| Type | Name | Description |
|---|---|---|
| 0 | invalid | Reserved; must never appear in a valid file |
| 1 | pages page | Index page containing `[start_ordinal:8][offset:8]` tuples for data pages |
| 2 | data page | Holds user records |
| 3 | namespaces page | Namespace index page at the end of a multi-namespace file |

### Pages page

A single page with `page_type = 1`. Contains `[start_ordinal:8][offset:8]` (LE) tuples — one
per data page — sorted by ordinal to facilitate O(log2 n) lookup.

Pages not referenced in the pages page are logically deleted. A slabtastic file that does not
end in a pages page or a namespaces page is invalid.

The single-page requirement puts a hard limit on the number of pages in a file.

### Namespaces page

A single page with `page_type = 3` located at the end of a multi-namespace file. Contains
`NamespacesPageEntry` records — one per namespace — each pointing to that namespace's
dedicated pages page within the file.

Single-namespace files (using only the default namespace) end with a pages page (type 1) for
backward compatibility. Multi-namespace files end with a namespaces page (type 3).

### Namespaces page entry layout

```
[namespace_index:1][name_length:1][name_bytes:N][pages_page_offset:8]
```

| Field | Size | Description |
|---|---|---|
| `namespace_index` | 1 byte | Namespace index (1-127; 0 is reserved) |
| `name_length` | 1 byte | Length of the name in bytes |
| `name_bytes` | N bytes | UTF-8 encoded namespace name (max 128 bytes) |
| `pages_page_offset` | 8 bytes | LE file offset of this namespace's pages page |

### File extension

`.slab`

---

## Java API

### `SlabConstants` (interface)

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
| `NAMESPACE_MAX_NAME_LENGTH` | 128 | Maximum namespace name length in bytes |
| `VERSION_INVALID` | 0 | **Deprecated** — alias for `NAMESPACE_INVALID` |
| `VERSION_1` | 1 | **Deprecated** — alias for `NAMESPACE_DEFAULT` |
| `MIN_PAGE_SIZE` | 512 | Minimum page size |
| `MAX_PAGE_SIZE` | `0xFFFFFFFFL` | Maximum page size (2^32−1) |
| `MAX_ORDINAL` | 2^39−1 | 5-byte signed max |
| `MIN_ORDINAL` | −2^39 | 5-byte signed min |
| `MAX_RECORD_COUNT` | 2^24−1 | 3-byte unsigned max |
| `OFFSET_ENTRY_SIZE` | 4 | Bytes per offset entry |
| `PAGES_PAGE_RECORD_SIZE` | 16 | Bytes per pages-page entry |
| `PAGE_ALIGNMENT` | 512 | Alignment granularity |

### `SlabWriter`

| Method | Description |
|---|---|
| `SlabWriter(Path, int)` | Create a new file (truncates existing) |
| `openForAppend(Path, int)` | Open an existing file for appending |
| `write(long, byte[])` | Append a record to the default namespace; ordinals must be strictly ascending |
| `write(String, long, byte[])` | Append a record to the specified namespace; ordinals must be strictly ascending within each namespace |
| `close()` | Flush remaining records, write pages page(s) and optional namespaces page, force, close (idempotent) |

### `SlabWriter.SlabWriterConfig` (record)

| Field | Type | Default | Description |
|---|---|---|---|
| `preferredPageSize` | int | 65536 | Target page size; must be >= `minPageSize` |
| `minPageSize` | int | 512 | Minimum page size and alignment granularity; must be >= 512 and a multiple of 512 |
| `pageAlignment` | boolean | false | When true, pages are padded to `minPageSize` multiples; when false, pages are rounded to `PAGE_ALIGNMENT` (512) multiples |
| `maxPageSize` | int | `Integer.MAX_VALUE` | Maximum allowed page size; must be >= `preferredPageSize`. An aligned page exceeding this limit causes an error during flush. |

Constructor/factory `preferredPageSize` must be >= `minPageSize` and <= `maxPageSize`.
Ordinals must be within [−2^39, 2^39−1]. Page flushes happen on ordinal gaps or size overflow.

### Slab layout parameters

Write commands (`rewrite`, `append`, `import`, `export`) accept layout parameters:

| Option | Default | Description |
|---|---|---|
| `--preferred-page-size` (alias `--page-size`) | 65536 | Target page size in bytes |
| `--min-page-size` | 512 | Minimum page size and alignment granularity |
| `--page-alignment` | off | When set, pages are padded to `min_page_size` multiples |
| `--max-page-size` | no limit | Maximum allowed page size in bytes; error if exceeded |

### Buffer naming

Commands creating new slab files (`rewrite`, `import` in non-append mode, `export` in slab format)
write to `<target>.buffer` and atomically rename to the target on success. This prevents
partial files from being visible to concurrent readers. On failure, the buffer file is deleted.

### Progress reporting

Write commands (`rewrite`, `append`, `import`, `export`) accept `--progress` to print ongoing
record counts to stderr every 1 second or 1M records (whichever comes first).

When a file contains only the default namespace, `close()` writes a single pages page (type 1)
for backward compatibility. When multiple namespaces are used, `close()` writes one pages page
per namespace followed by a namespaces page (type 3) at EOF.

### `SlabReader`

| Method | Description |
|---|---|
| `SlabReader(Path)` | Open and bootstrap from file tail |
| `get(long)` | `Optional<ByteBuffer>` — default namespace lookup |
| `get(String, long)` | `Optional<ByteBuffer>` — namespace-aware lookup; empty for missing ordinals, throws `IllegalArgumentException` for unknown namespaces |
| `pages()` | `List<PageSummary>` of all data pages in the default namespace |
| `pages(String)` | `List<PageSummary>` of all data pages in the specified namespace |
| `pageCount()` | Number of data pages in the default namespace |
| `pageCount(String)` | Number of data pages in the specified namespace |
| `recordCount()` | Total records in the default namespace |
| `recordCount(String)` | Total records in the specified namespace |
| `namespaces()` | `Set<String>` of all namespace names in the file |
| `fileSize()` | File size in bytes |
| `close()` | Close the underlying channel |

Namespace-aware methods (`get(String, long)`, `pages(String)`, `pageCount(String)`,
`recordCount(String)`) throw `IllegalArgumentException` for unknown namespaces. The
no-arg convenience methods (`get(long)`, `pages()`, etc.) always operate on the default
namespace, which is present in every valid file.

### `PageHeader` (record)

| Method | Description |
|---|---|
| `readFrom(ByteBuffer, int)` | Parse 8 bytes at offset |
| `writeTo(ByteBuffer, int)` | Write 8 bytes at offset |
| `pageSize()` | Total page size in bytes |

### `PageFooter` (record)

| Method | Description |
|---|---|
| `readFrom(ByteBuffer, int)` | Parse 16 bytes at offset |
| `writeTo(ByteBuffer, int)` | Write 16 bytes at offset |
| `validate()` | Assert all fields are valid |
| `startOrdinal()` | First ordinal in this page |
| `recordCount()` | Number of records |
| `pageSize()` | Total page size |
| `pageType()` | Page type enum value |
| `namespaceIndex()` | Namespace index for this page |
| `version()` | **Deprecated** — alias for `namespaceIndex()` |
| `footerLength()` | Footer length in bytes |

### `PagesPageEntry` (record)

| Method | Description |
|---|---|
| `readFrom(ByteBuffer, int)` | Parse 16 bytes at offset |
| `writeTo(ByteBuffer, int)` | Write 16 bytes at offset |
| `startOrdinal()` | First ordinal in the referenced page |
| `fileOffset()` | Byte offset of the referenced page |
| `compareTo(PagesPageEntry)` | Ordinal-based ordering |

### `NamespacesPageEntry` (record)

| Method | Description |
|---|---|
| `readFrom(ByteBuffer, int)` | Parse variable-length entry at offset |
| `writeTo(ByteBuffer, int)` | Write variable-length entry at offset |
| `namespaceIndex()` | The namespace index (1-127) |
| `name()` | The human-readable namespace name |
| `pagesPageOffset()` | File offset of this namespace's pages page |
| `serializedSize()` | Total serialized size in bytes |
| `validate()` | Assert index is non-zero and name fits within max length |

### `SlabPage`

| Method | Description |
|---|---|
| `SlabPage(long, byte, List<byte[]>)` | Build from records (default namespace) |
| `SlabPage(long, byte, List<byte[]>, byte)` | Build from records with namespace index |
| `parseFrom(ByteBuffer)` | Parse from buffer |
| `toByteBuffer()` | Serialize to buffer |
| `getRecord(int)` | Get record by local index |
| `serializedSize()` | Page size in bytes |
| `startOrdinal()` | First ordinal |
| `recordCount()` | Number of records |
| `pageType()` | Page type |
| `namespaceIndex()` | Namespace index for this page |
| `footer()` | Construct matching footer |

---

## CLI commands

Top-level command: `slab`

| Subcommand | Synopsis | Exit codes |
|---|---|---|
| `analyze <file> [-v] [--samples N] [--sample-percent P] [-n ns]` | File stats, sampling statistics, content detection | 0=success, 1=error |
| `check <file> [-v]` | Three-pass structural validation (index-driven, forward traversal, cross-check) | 0=valid, 1=errors, 2=unreadable |
| `get <file> -o <ordinals\|ranges> [-f fmt] [--as-hex] [--as-base64] [-n ns]` | Extract records by ordinals or ranges | 0=all found, 1=some missing, 2=error |
| `rewrite <src> <dst> [--preferred-page-size N] [--min-page-size N] [--page-alignment] [-f] [--skip-check] [--progress] [-n ns]` | Clean rewrite with repack + reorder | 0=success, 1=error |
| `append <target> --from <src> [--preferred-page-size N] [--min-page-size N] [--page-alignment] [--progress] [-n ns]` | Append records (with pre-flight validation) | 0=success, 1=error |
| `import <target> --from <src> [--format fmt] [--append] [--preferred-page-size N] [--min-page-size N] [--page-alignment] [--progress] [options] [-n ns]` | Import from non-slab file | 0=success, 1=error |
| `export <src> [--to <out>] [--range <range>] [--format <fmt>] [--preferred-page-size N] [--min-page-size N] [--page-alignment] [--progress] [--as-hex] [--as-base64] [-n ns]` | Export records to file or stdout | 0=success, 1=error |
| `explain <file> [-p pages] [-o ordinals] [-n ns]` | Illustrate page layout with block diagrams | 0=success, 1=error |
| `namespaces <file>` | List all namespaces | 0=success, 1=error |

### Namespace option

All data-oriented commands support the `--namespace` / `-n` option:

```
--namespace <name>    Namespace to operate on (default: default namespace)
-n <name>            Short form
```

When omitted, commands operate on the default namespace (empty string `""`).

### Import format options

Single `--format` enum:

| Value | Description |
|---|---|
| `text` | Newline-delimited records |
| `cstrings` | Null-byte-delimited records |
| `slab` | Slab format regardless of extension |
| `json` | JSON — top-level value boundaries |
| `jsonl` | JSONL — line boundaries, validated JSON |
| `csv` | CSV — line boundaries, RFC 4180 quoted-field awareness |
| `tsv` | TSV — line boundaries, no quoting |
| `yaml` | YAML — document boundaries (`---`), validated |

### Well-known extension detection

When `--format` is not specified, source file extensions are checked:

| Extension | Format |
|---|---|
| `.txt` | text |
| `.slab` | slab |
| `.json` | json |
| `.jsonl` | jsonl |
| `.csv` | csv |
| `.tsv` | tsv |
| `.yaml`, `.yml` | yaml |

### Export format options

| Format | Description |
|---|---|
| `raw` | Raw bytes (default) |
| `text` | Raw bytes with newline appended if not present |
| `cstrings` | Raw bytes with null byte appended if not present |
| `slab` | Slab-to-slab copy (requires `--to`) |
| `json` | JSON values, one per line |
| `jsonl` | JSON-encoded strings, one per line |
| `csv` | Raw CSV records |
| `tsv` | Raw TSV records |
| `yaml` | Raw YAML documents |
| `hex` | Hex dump with ASCII sidebar |
| `utf8` | UTF-8 string with ordinal prefix |
| `ascii` | Same as utf8 |

### Output overrides

Available on both `export` and `get`:

| Flag | Description |
|---|---|
| `--as-hex` | Output bytes as hex with space between each byte, trailing newline per record |
| `--as-base64` | Output bytes as base64 with trailing newline per record |

These are mutually exclusive with each other. When set, they override the normal format
rendering.

### Export range format

Ordinal ranges use the standard nbdatatools format:

| Form | Meaning |
|---|---|
| `n` | `[0, n)` — first n ordinals |
| `m..n` | `[m, n+1)` — m to n inclusive |
| `[m,n)` | Half-open interval |
| `[m,n]` | Closed interval |
| `(m,n)` | Open interval |
| `(m,n]` | Half-open interval |
| `[n]` | Single ordinal |

### Get format options

| Format | Description |
|---|---|
| `ascii` | Text with ordinal prefix (default) |
| `hex` | Hex dump with ASCII sidebar |
| `raw` | Raw bytes |
| `utf8` | Same as ascii |
| `json` | JSON-encoded string with ordinal prefix |
| `jsonl` | JSON-encoded string, one per line |
