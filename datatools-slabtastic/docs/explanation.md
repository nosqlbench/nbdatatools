# Explanation: Why slabtastic?

Understanding-oriented discussion of the design decisions and trade-offs behind the
slabtastic format.

---

## The problem

After working with several formats and I/O strategies for organizing non-uniform data by
ordinal, none proved suitable:

- **Arrow**: Meager ~15 MB dependency overhead and paged buffer layouts supporting fast
  access. But it requires rewriting the entire file from scratch on every write — you cannot
  append without a full rewrite.
- **Direct I/O with offset table**: Decent fit, but requires managing multiple buffers for
  indirect offsets, making it inherently more complicated. A separate offset index file is a
  non-starter, but some form of indexing is needed for random access with non-uniform record
  sizes.
- **SQLite**: Good fit for structured data, but doesn't support streaming bulk data as
  appendable and incremental unless using WAL, at which point you're juggling a directory.

---

## Design goals

Slabtastic keeps close to the metal, supports optimal chunking from block stores, and keeps
flat index data and values clustered close together. It supports:

- Effective random I/O
- Streaming out (batched)
- Streaming in values
- Append without rewrite

The only caveat is that streaming interfaces need to buffer and flush on page boundaries,
which is easily absorbed in the reader and writer APIs.

---

## Why pages?

Pages cluster records with their local offset index. This means a single I/O operation can
fetch both the index and the data for a set of records. The page structure:

```
[header][records][offsets][footer]
```

keeps everything needed for record access within a single contiguous region. The fence-post
offset array (N+1 entries for N records) makes indexing math simple — every record's start
and end are defined without special-casing.

---

## Why footer-indexed?

The footer at the end of each page means you can always read the last 16 bytes to discover:

1. Where the footer starts
2. Where the offset array starts
3. Where the page starts (via `page_size`)

This enables both forward traversal (via header `page_size`) and backward traversal (via
footer) — useful for file checking and maintenance without reading the pages page.

---

## Why append-only?

The append-only model avoids the most dangerous operation in persistent formats: overwriting
existing data. When appending:

1. New data pages are written after the old pages page
2. A new pages page is written at the end, referencing both old and new data pages
3. The old pages page becomes dead data

If the append fails mid-write, the old pages page is still intact and the file is still
valid up to the previous version. The "last pages page is authoritative" rule makes this
safe.

This also enables logical deletion: simply write a new pages page that omits references to
the deleted pages.

---

## Why sparse ordinals?

Sparse chunks (non-contiguous ordinal ranges between pages) afford step-wise changes to
data by appending new pages with ordinal holes. This is useful for applications making large
incremental changes without rewriting the entire file.

To support this, all read APIs signal missing ordinals explicitly (via `Optional.empty()`)
rather than returning a default value. This prevents silent data loss where a consumer might
confuse a genuinely empty value with a missing ordinal.

---

## Why 512-byte alignment?

512 bytes is the traditional disk sector size. Aligning pages to this boundary ensures
optimal I/O on both spinning disks and SSDs. The minimum page size of 512 bytes and the
requirement that all pages be multiples of 512 bytes means every page starts and ends on a
sector boundary.

The maximum of 2^32 bytes per page ensures pages fit within a single `mmap` call on older
Java systems that lack `AsyncFileChannel` or similar capabilities.

---

## Why the pages page is a regular page

The pages page uses the standard page layout with header, records, offsets, and footer. This
means all the same validation and traversal logic works for both data pages and the pages
page. The only distinction is the `page_type` field in the footer.

The trade-off is that pages-page entries encode their offsets duplicitously (via both the
fixed 16-byte record layout and the per-record offset array). Since the records are
uniform-size, array-based indexing could suffice, but format consistency is preferred.

---

## Why namespaces?

Namespaces solve the problem of storing multiple independent data sets in a single file.
Without namespaces, applications that need to co-locate related data (e.g. vectors alongside
metadata, or training data alongside labels) must either:

1. Use separate files and manage cross-file references externally.
2. Interleave records and use conventions (e.g. even ordinals for vectors, odd for metadata)
   that are fragile and wasteful.
3. Encode a multiplexing scheme within the record payloads.

Namespaces provide a first-class solution: each namespace is an independent ordinal space
with its own pages page. This means:

- **Independent ordinals**: the "vectors" namespace can have ordinals 0-999 while "metadata"
  has ordinals 0-99. No coordination needed.
- **Independent access**: reading from one namespace never touches another namespace's pages.
- **Single file**: all data lives in one `.slab` file, simplifying deployment and backup.

### How namespaces work

The key design insight is that byte 13 of the page footer (formerly the `version` byte) is
repurposed as `namespace_index`. Existing files with `version=1` read as `namespace_index=1`,
which is the default namespace. This provides seamless backward compatibility — no file
migration is needed.

Page types subsume the role of the version byte. All format evolution is expressed through
new page types rather than version numbers.

A single-namespace file (using only the default namespace) is structurally identical to a
pre-namespace file: it ends with a pages page (type 1). A multi-namespace file ends with a
namespaces page (type 3) that maps namespace names to their respective pages pages:

```
Single-namespace:  [data pages...][pages page (type 1)]
Multi-namespace:   [data pages...][pages page A (type 1)][pages page B (type 1)][namespaces page (type 3)]
```

This layered approach means that the pages-page logic is completely unchanged — namespaces
are an orthogonal layer on top.

### Namespace naming

Namespace names are UTF-8 strings up to 128 bytes. The default namespace uses the empty
string `""`. Names can contain any valid UTF-8 characters including slashes, dots, and
unicode — the format imposes no naming restrictions beyond the length limit.

### Trade-offs

- **Cost**: multi-namespace files have slightly more overhead (one extra pages page per
  additional namespace, plus the namespaces page).
- **Complexity**: the reader bootstrap has two paths (pages page vs namespaces page at EOF),
  but this is hidden behind the same API.
- **Limitation**: namespace indices are single-byte (1-127), capping the format at 127
  namespaces per file. This is sufficient for foreseeable use cases.

---

## Concurrent reading

Concurrent readers streaming a slabtastic file may incrementally read it by watching for
updates, but this is opportunistic at best given that revisions may occur from subsequent
pages page writes. As long as the reader session can safely assume the writer is streaming a
version of data which is valid, incremental observation is valid as pages are written. This
is a special case where mutability is not expected and should be made explicit.

Readers must not assume atomic writes. The `[magic][size]` header should be used to
determine when a page is valid for reading based on the incremental file size.

---

## What is deferred

The design spec describes several features that are not yet implemented:

| Feature | Rationale for deferral |
|---|---|
| Streaming get / bulk read | Requires buffered batch return semantics |
| Sink/callback reader | Requires async future + decorator pattern |
| Async iterable append | Requires async writer backend |
| Progress decorator | Requires future interface extension |
| Interior mutation | Requires in-place editing; complex for variable-size records |
| Configurable min/preferred/max page sizing | Uses single preferred page size |
| Checksums in footer | Deferred to a future version |
