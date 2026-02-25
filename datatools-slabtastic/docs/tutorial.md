# Tutorial: Getting started with slabtastic

This tutorial walks through creating, reading, and maintaining a slabtastic file from
scratch. By the end you will understand the basic write-read-verify cycle and how to use
namespaces.

---

## Prerequisites

- Java 21+ (slabtastic uses `AsynchronousFileChannel` and modern NIO)
- The `datatools-slabtastic` module on your classpath

---

## Step 1: Write records to a new file

Create a `SlabWriter` with a target file path and preferred page size. Write records as
`(ordinal, byte[])` pairs. Ordinals must be strictly ascending.

```java
import io.nosqlbench.slabtastic.SlabWriter;
import java.nio.file.Path;

Path file = Path.of("my-data.slab");

try (var writer = new SlabWriter(file, 4096)) {
    writer.write(0, "alpha".getBytes());
    writer.write(1, "bravo".getBytes());
    writer.write(2, "charlie".getBytes());
}
```

When the writer closes, it flushes any buffered records as a data page and writes the pages
page at the end of the file.

---

## Step 2: Read records back

Open the file with a `SlabReader` and retrieve records by ordinal. Missing ordinals return
`Optional.empty()`.

```java
import io.nosqlbench.slabtastic.SlabReader;
import java.nio.ByteBuffer;
import java.util.Optional;

try (var reader = new SlabReader(file)) {
    Optional<ByteBuffer> result = reader.get(1);
    if (result.isPresent()) {
        byte[] bytes = new byte[result.get().remaining()];
        result.get().get(bytes);
        System.out.println(new String(bytes));  // prints "bravo"
    }
}
```

---

## Step 3: Inspect the file with the CLI

Use the `slab analyze` command to see file stats and sampling statistics:

```
slab analyze my-data.slab
```

Output:
```
File:          my-data.slab
File size:     1,024 bytes
Pages:         1
Total records: 3
Ordinal range: [0, 2]

Samples: 1 pages, 3 records

=== Record Size Statistics ===
  min: 5 bytes
  avg: 5.7 bytes
  max: 7 bytes

=== Page Size Statistics ===
  min: 512 bytes
  avg: 512.0 bytes
  max: 512 bytes

=== Page Utilization ===
  min: 3.3%
  avg: 3.3%
  max: 3.3%

Detected content type: text
Ordinal monotonicity: strictly monotonic
```

Use `-v` for a per-page breakdown, or control sampling with `--samples` or `--sample-percent`.

---

## Step 4: Validate the file

Run `slab check` to verify structural integrity:

```
slab check my-data.slab
```

If everything is sound you'll see:
```
All checks passed.
```

---

## Step 5: Append more data

Append records from another slab file onto the end of an existing one:

```java
try (var writer = SlabWriter.openForAppend(file, 4096)) {
    writer.write(3, "delta".getBytes());
    writer.write(4, "echo".getBytes());
}
```

Or use the CLI:

```
slab append my-data.slab --from more-data.slab
```

The original data pages are preserved. A new pages page at the end references both old and
new data pages.

---

## Step 6: Query specific ordinals

Use the CLI to extract records by ordinal:

```
slab query my-data.slab -o 0,2,4 -f utf8
```

Output:
```
ordinal 0: alpha
ordinal 2: charlie
ordinal 4: echo
```

You can also use `--as-hex` or `--as-base64` for alternative output formats.

---

## Step 7: Rewrite a file

Use `slab rewrite` to create a clean copy with fresh page alignment, monotonic ordering,
and no dead pages:

```
slab rewrite my-data.slab clean.slab --page-size 8192
```

---

## Step 8: Use namespaces

Namespaces let you store multiple independent data sets in a single file. Each namespace has
its own ordinal space.

### Write to multiple namespaces

```java
Path nsFile = Path.of("multi-ns.slab");

try (var writer = new SlabWriter(nsFile, 4096)) {
    writer.write("vectors", 0, new byte[]{1, 2, 3, 4});
    writer.write("vectors", 1, new byte[]{5, 6, 7, 8});
    writer.write("labels", 0, "cat".getBytes());
    writer.write("labels", 1, "dog".getBytes());
}
```

### Read from specific namespaces

```java
try (var reader = new SlabReader(nsFile)) {
    // See what namespaces exist
    System.out.println(reader.namespaces());  // [vectors, labels]

    // Read from a specific namespace
    reader.get("vectors", 0);  // returns the vector data
    reader.get("labels", 0);   // returns "cat"

    // Get per-namespace stats
    reader.recordCount("vectors");  // 2
    reader.recordCount("labels");   // 2
}
```

### Use namespaces from the CLI

List namespaces:
```
slab namespaces multi-ns.slab
```

Query a specific namespace:
```
slab query multi-ns.slab -o 0,1 -f utf8 -n labels
```

Export from a specific namespace:
```
slab export multi-ns.slab --to vectors.bin -n vectors --format raw
```

Import into a specific namespace:
```
slab import multi-ns.slab --from data.txt -n my-namespace --format text --append
```

---

## Next steps

- Read the [how-to guide](how-to.md) for common maintenance tasks
- Read the [reference](reference.md) for full API and format details
- Read the [explanation](explanation.md) for design rationale
