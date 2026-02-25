# How-to guides

Task-oriented recipes for common slabtastic operations.

---

## How to rewrite a file with a different page size

Over time, a slab file may accumulate dead pages from append operations. Use `rewrite` to
write a clean copy with fresh alignment, monotonic ordering, and no dead pages:

```
slab rewrite old.slab clean.slab --page-size 8192
```

This reads all records in ordinal order and writes them to a new file with the specified
page size. The output is written to `clean.slab.buffer` and atomically renamed to
`clean.slab` on success, preventing partial files from being visible to concurrent readers.
Use `--force` to overwrite an existing destination.

By default, the source file is validated before reading. Use `--skip-check` to bypass:

```
slab rewrite old.slab clean.slab --skip-check
```

---

## How to append data from one file to another

To add records from a source file onto the end of a target file:

```
slab append target.slab --from source.slab --preferred-page-size 4096
```

All ordinals in the source must be strictly greater than the maximum ordinal in the target.
The old pages page becomes dead data; a new pages page at the end references all pages.

Programmatically:

```java
try (var writer = SlabWriter.openForAppend(target, 65536)) {
    // write new records — ordinals must be strictly ascending
    // from the last ordinal in the existing file
    writer.write(nextOrdinal, data);
}
```

---

## How to import records from a text file

To create a slab file from a newline-delimited text file:

```
slab import output.slab --from input.txt --format text
```

Delimiters are included in the record data — every byte of the original file is preserved.
If the file does not end with a newline, the trailing content is still imported as a final
record.

To import from a null-terminated file:

```
slab import output.slab --from input.bin --format cstrings
```

If you omit the `--format` flag, the command auto-detects the format:
1. Well-known file extensions are trusted (`.txt` → text, `.json` → json, `.csv` → csv, etc.)
2. Otherwise the first 8192 bytes are scanned: `\0` → cstrings, `\n` → text
3. Files with `.slab` extension are treated as slab-to-slab copies (ordinals preserved)

Custom start ordinal:

```
slab import output.slab --from input.txt --start-ordinal 1000
```

Use `--force` to overwrite an existing target file.

---

## How to import and append

To append imported records onto an existing slab file:

```
slab import target.slab --from more-data.txt --format text --append
```

In append mode, the start ordinal is automatically detected from the existing file's
maximum ordinal + 1. You can override this with `--start-ordinal`.

If the target file doesn't exist yet, `--append` creates a new file normally.

---

## How to import structured formats

Import JSON (top-level value boundaries):
```
slab import output.slab --from data.json --format json
```

Import JSONL (one JSON value per line):
```
slab import output.slab --from data.jsonl --format jsonl
```

Import CSV (RFC 4180, handles quoted fields with embedded newlines):
```
slab import output.slab --from data.csv --format csv
```

Import TSV (tab-separated, line boundaries):
```
slab import output.slab --from data.tsv --format tsv
```

Import YAML (document boundaries at `---`):
```
slab import output.slab --from data.yaml --format yaml
```

All structured formats validate parsability during import. Delimiters are included in
record data.

---

## How to export records from a slab file

Export all records as raw bytes to a file:
```
slab export data.slab --to output.bin --format raw
```

Export to stdout (default when `--to` is omitted):
```
slab export data.slab --format utf8
```

Export a range of ordinals:
```
slab export data.slab --to subset.slab --format slab --range [100,200)
```

Export with newline delimiters (text format):
```
slab export data.slab --to output.txt --format text
```

Export as hex dump:
```
slab export data.slab --format hex --range [0]
```

Export as hex bytes (space-separated, one record per line):
```
slab export data.slab --as-hex
```

Export as base64 (one record per line):
```
slab export data.slab --as-base64
```

Skip parsability validation for structured formats:
```
slab export data.slab --to output.json --format json --skip-validation
```

Use `--force` to overwrite an existing output file.

---

## How to check a file for corruption

```
slab check my-data.slab -v
```

The `-v` flag prints per-page progress. Exit codes:
- `0`: file is valid
- `1`: errors found (details printed to stderr)
- `2`: file cannot be read at all

Checks include: magic bytes, header/footer page-size agreement, footer field validity,
offset table monotonicity and bounds, ordinal range, page region overlaps, duplicate start
ordinals, and entries past EOF. For multi-namespace files, the namespaces page structure and
cross-namespace page overlap are also validated.

---

## How to use progress reporting

Write commands (`rewrite`, `append`, `import`, `export`) support `--progress` to print
ongoing record counts to stderr:

```
slab rewrite large.slab clean.slab --progress
```

Output on stderr:
```
Processing: 1,234,567 records (42 pages) ...
```

The final summary is always printed regardless of `--progress`.

---

## How to extract records by ordinal

Hex dump:
```
slab get my-data.slab -o 0,5,10
```

Raw bytes (e.g. piping to another tool):
```
slab get my-data.slab -o 42 -f raw > record_42.bin
```

UTF-8 text:
```
slab get my-data.slab -o 0,1,2 -f utf8
```

As hex bytes:
```
slab get my-data.slab -o 0,1,2 --as-hex
```

As base64:
```
slab get my-data.slab -o 0,1,2 --as-base64
```

Missing ordinals are reported to stderr. Exit code 1 if any ordinals were not found.

---

## How to handle sparse ordinals

Slabtastic supports sparse ordinal ranges. Ordinals within a page must be contiguous, but
gaps between pages are allowed — the writer automatically starts a new page on an ordinal
gap.

```java
try (var writer = new SlabWriter(path, 65536)) {
    writer.write(0,   data0);
    writer.write(1,   data1);
    // gap: ordinals 2-99 are missing
    writer.write(100, data100);
    writer.write(101, data101);
}
```

Readers signal missing ordinals via `Optional.empty()`:

```java
try (var reader = new SlabReader(path)) {
    reader.get(50);  // returns Optional.empty()
}
```

---

## How to visualize page layout

Use `slab explain` to see the internal structure of each page in a slab file:

```
slab explain my-data.slab
```

This renders box diagrams showing the header, records (with hex and ASCII preview),
offset table, gap padding, and footer for every data page, plus structural pages.

Show only specific pages by index:
```
slab explain my-data.slab --pages 0,2
```

Show only pages that contain a specific ordinal range:
```
slab explain my-data.slab --ordinals [100,200)
```

Show pages for a specific namespace:
```
slab explain my-data.slab -n vectors
```

---

## How to get file layout details

Use `slab analyze` with verbose mode for per-page breakdown:

```
slab analyze my-data.slab -v
```

This prints file stats, sampling statistics (record sizes, page sizes, page utilization),
content type detection, ordinal monotonicity classification, and a per-page table with page
index, start ordinal, record count, page size, and file offset.

Control sampling with `--samples` or `--sample-percent`:

```
slab analyze my-data.slab --samples 500
slab analyze my-data.slab --sample-percent 5
```

---

## How to use namespaces

Namespaces organize data into independent ordinal spaces within a single slab file. Each
namespace has its own set of ordinals and pages.

### Writing to namespaces programmatically

```java
try (var writer = new SlabWriter(path, 65536)) {
    writer.write("vectors", 0, vectorData0);
    writer.write("vectors", 1, vectorData1);
    writer.write("metadata", 0, metaData0);
    writer.write("metadata", 1, metaData1);
}
```

Each namespace maintains independent ordinal sequences — both "vectors" and "metadata" can
start at ordinal 0.

### Reading from namespaces programmatically

```java
try (var reader = new SlabReader(path)) {
    // List all namespaces
    Set<String> names = reader.namespaces();

    // Read from a specific namespace
    Optional<ByteBuffer> vec = reader.get("vectors", 0);
    Optional<ByteBuffer> meta = reader.get("metadata", 0);

    // Get stats per namespace
    long vecRecords = reader.recordCount("vectors");
    int metaPages = reader.pageCount("metadata");
}
```

Unknown namespaces return empty results rather than throwing exceptions.

### Using namespaces with CLI commands

All data-oriented CLI commands accept `--namespace` / `-n`:

```
slab get data.slab -o 0,1,2 -n vectors
slab analyze data.slab -n metadata
slab export data.slab --to vectors.bin -n vectors --format raw
slab import data.slab --from input.txt -n my-namespace --format text
slab rewrite old.slab new.slab -n vectors
slab append target.slab --from source.slab -n vectors
```

### Backward compatibility

Single-namespace files (using only the default namespace) are fully backward compatible:
they end with a pages page (type 1) just as they always have. Multi-namespace files end
with a namespaces page (type 3) that indexes per-namespace pages pages.

---

## How to list namespaces in a file

Use the `slab namespaces` command to see all namespaces:

```
slab namespaces my-data.slab
```

Output:
```
File: my-data.slab

Index     Name                  Pages     Records       Ordinal Range
----------------------------------------------------------------------------------
-         vectors               3         300           [0, 299]
-         metadata              1         100           [0, 99]

Total namespaces: 2
```

For single-namespace files, just the default namespace is shown.
