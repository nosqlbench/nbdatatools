<!--
  Copyright (c) nosqlbench

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# MNode — Binary Metadata Node

MNode is a compact, self-describing metadata record format for the predicated
dataset system. It encodes typed key-value pairs into a raw byte sequence
without relying on external schemas or reflection.

The types for MNode encoding were chosen to be broadly compatible with a number of
systems, including CQL, JSON, CBOR, etc. 100% compatibility is not a goal.

For each of the types supported, on encoding MNode will ensure that the provided value
is valid for the given type (bounds, validation, etc.) or it will throw an error.

Types are known by a primary identifier aligned to Java vernacular. Secondary aliases
facilitate conversion to and from equivalent types in other systems.

## Characteristics

| Concern            | MNode (binary)             |
|--------------------|----------------------------|
| Encode/decode speed| Very fast (direct ByteBuffer I/O) |
| Wire size          | Compact (type-tagged, no schema overhead) |
| Schema evolution   | Append new fields freely   |
| Dependencies       | None (java.nio only)       |

## Encoding levels

Every type participates in three encoding levels. These are properties of the **codec**,
not of MNode itself:

1. **CDDL text** — human-specified, derived from [RFC 8610](https://datatracker.ietf.org/doc/html/rfc8610)
   wherever possible.
2. **Binary** — the strict byte-level encoding that MNode reads and writes directly.
3. **CDDL display** — a human-readable rendering, also derived from RFC 8610.

MNode core handles binary encoding only. Separate codec layers handle the text forms:

- **CDDL parser** — reads CDDL text data into typed Java objects.
- **MNode encoder** — reads typed Java objects into binary MNode.
- **CDDL-to-MNode direct codec** — an optional optimized path that converts CDDL text
  directly to MNode binary without materializing intermediate Java objects. Whether this
  outperforms the two-stage pipeline is an empirical question to be benchmarked.

## Byte order

All multi-byte integers use **little-endian** byte order, favoring x86 and x64
architectures.

## Tag budget

Type tags are a single unsigned byte (0–255). The current type system uses
29 tags (0–28). No sub-byte packing is planned.

---

## Type system overview

MNode supports 29 type tags (0–28) covering:

- **Text types** (tags 0, 10, 11): string, validated text, ASCII
- **Integer types** (tags 1, 12, 13, 14, 15): long, int, short, decimal, varint
- **Float types** (tags 2, 16, 17): double, float, half
- **Boolean** (tag 3)
- **Binary** (tag 4): byte[]
- **Null** (tag 5)
- **Enum types** (tags 6, 7): self-describing and ordinal-encoded
- **Composite types** (tags 8, 9, 26, 27, 28): list, node, array, set, typed map
- **Temporal types** (tags 18–22): millis, nanos, date, time, datetime
- **ID types** (tags 23–25): UUID v1, UUID v7, ULID

## Tag assignment

| Tag | Type       | Java type      | Wire bytes                          |
|-----|------------|----------------|-------------------------------------|
|  0  | string     | String         | `[len:4][utf8:N]`                   |
|  1  | long       | Long           | `[val:8]` LE                        |
|  2  | double     | Double         | `[val:8]` LE                        |
|  3  | boolean    | Boolean        | `[val:1]` 0/1                       |
|  4  | byte       | byte[]         | `[len:4][bytes:N]`                  |
|  5  | null       | null           | (none)                              |
|  6  | enum_str   | String         | `[len:4][utf8:N]`                   |
|  7  | enum_ord   | Integer        | `[ordinal:4]` LE                    |
|  8  | list       | List           | `[count:4][tag+value...]`           |
|  9  | node       | MNode          | `[len:4][mnode_payload:N]`          |
| 10  | text       | String         | `[len:4][utf8:N]`                   |
| 11  | ascii      | String         | `[len:4][bytes:N]`                  |
| 12  | int        | Integer        | `[val:4]` LE                        |
| 13  | short      | Short          | `[val:2]` LE                        |
| 14  | decimal    | BigDecimal     | `[scale:4][len:4][unscaledBytes:N]` |
| 15  | varint     | BigInteger     | `[len:4][unscaledBytes:N]`          |
| 16  | float      | Float          | `[val:4]` LE                        |
| 17  | half       | Float (stored) | `[val:2]` LE                        |
| 18  | millis     | Instant        | `[val:8]` LE                        |
| 19  | nanos      | Instant        | `[seconds:8][nanoAdjust:4]` LE      |
| 20  | date       | LocalDate      | `[len:4][utf8:N]` ISO string        |
| 21  | time       | LocalTime      | `[len:4][utf8:N]` ISO string        |
| 22  | datetime   | Instant        | `[len:4][utf8:N]` ISO string        |
| 23  | uuidv1     | UUID           | `[msb:8][lsb:8]`                    |
| 24  | uuidv7     | UUID           | `[msb:8][lsb:8]`                    |
| 25  | ulid       | Ulid           | `[bytes:16]`                        |
| 26  | array      | typed array    | `[elemTag:1][count:4][values:N]`    |
| 27  | set        | Set            | `[count:4][tag+value...]`           |
| 28  | map        | Map            | `[count:4][kTag+k+vTag+v...]`       |

Tags 0–9 are fully backwards-compatible with the original MNode implementation.

---

## Core types

### byte

| | |
|---|---|
| **Aliases** | `bytes`, `blob`, `binary` |
| **Java type** | `byte[]` |
| **Wire format** | `[len:4][bytes:N]` |
| **Description** | Arbitrary binary data. No validation beyond length. The primary type for raw binary. Byte arrays are accepted as values; `blob` and `bytes` are aliases. |

### boolean

| | |
|---|---|
| **Java type** | `Boolean` |
| **Wire format** | `[val:1]` — 0 for false, 1 for true |

### null

| | |
|---|---|
| **Java type** | `null` |
| **Wire format** | (no value bytes — tag only) |
| **Description** | Represents an explicitly absent value. Null fields are present in the field list but `has()` returns false. |

---

## Text types

All text types share the same wire layout: `[len:4][bytes:N]`. They differ in the
**validation guarantee** communicated by the wire tag. This lets downstream readers
know what parsing and safety checks have already been applied — for example, reading
UTF-8 with wide characters when you expect ASCII is not acceptable.

A single `getString()` accessor works on all three text tags (widening read). The tag
is a write-time constraint that the reader can trust.

### string

| | |
|---|---|
| **Java type** | `String` |
| **Wire format** | `[len:4][utf8:N]` |
| **Validation** | None. Strings are not validated. They are just strings with no additional rules. |
| **Description** | The base text type. No validation is performed on encode. |

### text

| | |
|---|---|
| **Java type** | `String` |
| **Wire format** | `[len:4][utf8:N]` |
| **Validation** | Valid UTF-8. Rejects sequences that are not well-formed UTF-8. |
| **Description** | Guaranteed well-formed UTF-8 text. |

### ascii

| | |
|---|---|
| **Java type** | `String` |
| **Wire format** | `[len:4][bytes:N]` |
| **Validation** | Every byte is a printable single-byte UTF-8 character (U+0020–U+007E). |
| **Description** | Restricted to the ASCII printable range. Safe for systems that cannot handle multi-byte characters. |

---

## Integer types

Each integer width is a **distinct wire tag** with its native byte width, for
cross-compatibility with systems that have strict type alignment (e.g. CQL).

**Widening reads are allowed.** Calling `getLong()` on a short or int field returns
the value widened to long. Calling `getInt()` on a short field returns widened to int.
Narrowing reads are not allowed — `getShort()` on a long field throws.

### long

| | |
|---|---|
| **Aliases** | `bigint` |
| **Java type** | `Long` |
| **Wire format** | `[val:8]` little-endian |
| **Range** | −2^63 to 2^63−1 |

### int

| | |
|---|---|
| **Java type** | `Integer` |
| **Wire format** | `[val:4]` little-endian |
| **Range** | −2^31 to 2^31−1 |

### short

| | |
|---|---|
| **Aliases** | `smallint` |
| **Java type** | `Short` |
| **Wire format** | `[val:2]` little-endian |
| **Range** | −32768 to 32767 |

### decimal

| | |
|---|---|
| **Java type** | `BigDecimal` |
| **Wire format** | `[scale:4][len:4][unscaledBytes:N]` |
| **Description** | Variable-precision decimal. Scale is a signed 32-bit integer following `BigDecimal.scale()` convention. `unscaledBytes` is the two's-complement big-endian byte array from `BigInteger.toByteArray()`. |

### varint

| | |
|---|---|
| **Java type** | `BigInteger` |
| **Wire format** | `[len:4][unscaledBytes:N]` |
| **Description** | Arbitrary-magnitude integer. Wire format is a length-prefixed two's-complement big-endian byte array (no scale field). Conceptually equivalent to a decimal with scale=0, but uses a more compact wire encoding without the 4-byte scale prefix. |

---

## IEEE 754 floating-point types

Each precision level is a **distinct wire tag** at its native byte width.

**Widening reads are allowed.** `getDouble()` accepts half and float fields (widened).
`getFloat()` accepts half fields (widened). Narrowing reads throw.

### double

| | |
|---|---|
| **Java type** | `Double` |
| **Wire format** | `[val:8]` little-endian IEEE 754 binary64 |

### float

| | |
|---|---|
| **Java type** | `Float` |
| **Wire format** | `[val:4]` little-endian IEEE 754 binary32 |

### half

| | |
|---|---|
| **Java type** | `Half` (custom value class) |
| **Wire format** | `[val:2]` little-endian IEEE 754 binary16 |
| **Description** | Java has no native half-precision type. `Half` is a value class wrapping a `short` containing the raw binary16 bits. Provides `toFloat()`, `toDouble()`, and `Half.of(float)` / `Half.of(double)` factories that round to half precision. The Java 11 base implementation uses pure-Java IEEE 754 bit manipulation. The `Half` class in `src/main/java25` uses `Float.float16ToFloat(short)` and `Float.floatToFloat16(float)` (Java 20+). |

---

## Enum types

Two encodings balance self-description against compactness.

### enum_str (self-describing)

| | |
|---|---|
| **Java type** | `String` (via `enumVal()` marker) |
| **Wire format** | `[len:4][utf8:N]` |
| **Description** | The enum value is stored as a UTF-8 string. No schema needed to decode. Zero overhead from the ordinal machinery. |

### enum_ord (ordinal-encoded)

| | |
|---|---|
| **Java type** | `Integer` (via `enumOrd()` marker) |
| **Wire format** | `[ordinal:4]` little-endian |
| **Description** | The enum value is stored as a 4-byte ordinal index. Resolving to a string requires an enum definition attached via `enumOrd(int, String...)` at construction or `withEnumDef(key, values...)` after decode. Accessing the raw ordinal via `getEnumOrdinal()` always works. Accessing the string via `getEnum()` without a definition throws `IllegalStateException`. |

Both forms are accessed uniformly through `getEnum(String)`.

---

## Temporal types

All temporal values are normalized to **UTC/Zulu** on the wire. Java accessors use the
`java.time` API. TZ-adjusted getters accept a `ZoneId` parameter and return
`ZonedDateTime`.

### millis

| | |
|---|---|
| **Aliases** | `epochmillis` |
| **Java type** | `Instant` (via `Instant.ofEpochMilli()`) |
| **Wire format** | `[val:8]` little-endian (milliseconds since Unix epoch, UTC) |

### nanos

| | |
|---|---|
| **Aliases** | `epochnanos` |
| **Java type** | `Instant` (via `Instant.ofEpochSecond(sec, nanoAdj)`) |
| **Wire format** | `[seconds:8][nanoAdjust:4]` little-endian (12 bytes total) |
| **Description** | Split encoding avoids signed-long overflow (a single 8-byte nanos field overflows ~year 2262). `seconds` is seconds since Unix epoch (signed long). `nanoAdjust` is the sub-second nanosecond adjustment (0–999,999,999), stored as an unsigned 32-bit int. Maps directly to `Instant.ofEpochSecond(seconds, nanoAdjust)`. |

### date

| | |
|---|---|
| **Java type** | `LocalDate` |
| **Wire format** | `[len:4][utf8:N]` — ISO 8601 date string, e.g. `"2026-02-25"` |
| **Description** | Zulu-normalized. No time component. Stored as parsable ISO text. |

### time

| | |
|---|---|
| **Java type** | `LocalTime` |
| **Wire format** | `[len:4][utf8:N]` — ISO 8601 time string, e.g. `"14:30:00Z"` |
| **Description** | Zulu-normalized. No date component. Stored as parsable ISO text. |

### datetime

| | |
|---|---|
| **Aliases** | `timestamp` |
| **Java type** | `Instant` |
| **Wire format** | `[len:4][utf8:N]` — ISO 8601 datetime string, e.g. `"2026-02-25T14:30:00Z"` |
| **Description** | Zulu-normalized. Stored as parsable ISO text. TZ-adjusted accessor: `getDateTime("key", ZoneId)` returns `ZonedDateTime`. |

---

## ID types

All ID types are **16 bytes** on the wire (raw binary, compact form). Each variant
is a **distinct wire tag**. Version bits are **validated on encode** — MNode does not
trust the caller's assertion.

### uuidv1

| | |
|---|---|
| **Aliases** | `timeuuid` |
| **Java type** | `java.util.UUID` |
| **Wire format** | `[msb:8][lsb:8]` (16 bytes, raw) |
| **Validation** | Version nibble (bits 48–51) must be `0001`. |
| **Reference** | [RFC 4122](https://datatracker.ietf.org/doc/html/rfc4122) |

### uuidv7

| | |
|---|---|
| **Java type** | `java.util.UUID` |
| **Wire format** | `[msb:8][lsb:8]` (16 bytes, raw) |
| **Validation** | Version nibble (bits 48–51) must be `0111`. |
| **Reference** | [RFC 9562](https://datatracker.ietf.org/doc/html/rfc9562) |

### ulid

| | |
|---|---|
| **Java type** | `Ulid` (custom value class) |
| **Wire format** | `[bytes:16]` (raw binary, Crockford base32 decodable) |
| **Description** | `Ulid` is a value class wrapping a 16-byte array. Provides `toString()` (Crockford base32), `toBytes()`, `timestamp()` (millis), and `Ulid.of(String)` / `Ulid.of(byte[])` factories. |
| **Reference** | [ULID spec](https://github.com/ulid/spec) |

---

## Collection types

### array

A **homogeneously-typed**, fixed-element-size array. Restricted to fixed-size element
types only. This enables flat random access without an offset table, matching
[RFC 8746](https://datatracker.ietf.org/doc/html/rfc8746) typed arrays.

| | |
|---|---|
| **Java type** | Typed Java array: `long[]`, `int[]`, `short[]`, `double[]`, `float[]`, `Half[]`, `boolean[]`, `Instant[]`, `UUID[]`, `Ulid[]` |
| **Wire format** | `[elementTag:1][count:4][values:count * elementWidth]` |
| **Access** | `element[i]` at byte offset `5 + i * elementWidth`. No boxing for primitives. |
| **Eligible element tags** | boolean (1), short (2), int (4), long (8), half (2), float (4), double (8), enum_ord (4), millis (8), nanos (12), uuidv1 (16), uuidv7 (16), ulid (16) |
| **Factory methods** | `array(long[])`, `array(int[])`, `array(short[])`, `array(double[])`, `array(float[])`, `array(boolean[])`, `array(Half[])`, `arrayMillis(Instant[])`, `arrayNanos(Instant[])`, `arrayEnumOrd(int[])`, `arrayUuidV1(UUID[])`, `arrayUuidV7(UUID[])`, `array(Ulid[])` |

### list

A **heterogeneous** list. Each element carries its own 1-byte type tag. May contain
any supported type including nested lists and maps.

| | |
|---|---|
| **Java type** | `List<Object>` (unmodifiable) |
| **Wire format** | `[count:4][per-element: tag:1, valueBytes...]` |

### set

Wire-identical to list, but with a **uniqueness constraint** enforced at construction
time. Element types are restricted to **scalars and strings** (no byte[], no nested
collections, no MNode).

Elements are **sorted deterministically** on encode. The sort order is by type tag
first, then by natural ordering within the same tag. This ensures identical wire bytes
regardless of insertion order.

| | |
|---|---|
| **Java type** | `Set<Object>` (unmodifiable) |
| **Wire format** | `[count:4][per-element: tag:1, valueBytes...]` |
| **Uniqueness** | Enforced via `Object.equals()` at construction. Duplicate elements cause `IllegalArgumentException`. |
| **Allowed element types** | string, text, ascii, long, int, short, double, float, half, boolean, enum_str, enum_ord |

### node (nested MNode)

A nested MNode embedded as a field value. This is a **structured record** with
string-keyed fields and full typed accessor support. Accessed via `getNode(key)`,
which returns an `MNode` you can chain accessors on:
`node.getNode("config").getString("engine")`.

| | |
|---|---|
| **Java type** | `MNode` |
| **Wire format** | `[len:4][mnode_payload:N]` |
| **Description** | The payload is a complete MNode binary encoding (field_count header, named fields, etc.). This is the same format used by `MNode.encode()`/`MNode.fromBuffer()`. |

### map (typed-key map)

A collection of typed key-value entries. Keys follow the same rules as set elements:
restricted to **string and scalar types**. Keys are **sorted deterministically** on
encode (by type tag first, then natural ordering), ensuring identical wire bytes
regardless of insertion order. Values may be any supported type, including nested
maps (recursive).

This is a general-purpose dictionary, distinct from the nested MNode record above.
Use **node** when you want a structured record with named string fields and typed
accessors. Use **map** when you need arbitrary key types (e.g. `Map<Long, String>`).

| | |
|---|---|
| **Java type** | `Map<Object, Object>` (unmodifiable) |
| **Wire format** | `[count:4][per-entry: keyTag:1, keyBytes..., valueTag:1, valueBytes...]` |
| **Allowed key types** | string, text, ascii, long, int, short, double, float, half, boolean, enum_str, enum_ord |

---

## Creating nodes

### Scalars

```java
MNode node = MNode.of(
    "name", "glove-100",       // string (tag 0)
    "count", 42L,              // long (tag 1)
    "score", 0.95,             // double (tag 2)
    "active", true,            // boolean (tag 3)
    "checksum", new byte[]{0x1a, 0x2b}  // bytes (tag 4)
);
```

### Extended scalars (marker factories)

```java
MNode node = MNode.of(
    "label", MNode.text("validated UTF-8"),    // text (tag 10)
    "id", MNode.ascii("ABC-123"),              // ascii (tag 11)
    "dims", MNode.int32(128),                  // int (tag 12)
    "rank", MNode.int16((short) 5),            // short (tag 13)
    "score", MNode.float32(0.95f),             // float (tag 16)
    "weight", MNode.half(1.5f)                 // half (tag 17)
);
```

### Directly-inferrable types (no marker needed)

```java
MNode node = MNode.of(
    "price", new BigDecimal("123.45"),         // decimal (tag 14)
    "big", new BigInteger("999999999999"),      // varint (tag 15)
    "date", LocalDate.of(2026, 2, 25),         // date (tag 20)
    "time", LocalTime.of(14, 30, 0),           // time (tag 21)
    "id", Ulid.of("01ARZ3NDEKTSV4RRFFQ69G5FAV")  // ulid (tag 25)
);
```

### Temporal types (marker factories for ambiguous Instant)

```java
MNode node = MNode.of(
    "created", MNode.millis(Instant.now()),    // millis (tag 18)
    "precise", MNode.nanos(Instant.now()),     // nanos (tag 19)
    "logged", MNode.datetime(Instant.now())    // datetime (tag 22)
);
```

### ID types

```java
MNode node = MNode.of(
    "v1", MNode.uuidV1(timeBasedUuid),        // uuidv1 (tag 23)
    "v7", MNode.uuidV7(sortableUuid),         // uuidv7 (tag 24)
    "ulid", Ulid.of("01ARZ3NDEKTSV4RRFFQ69G5FAV")  // ulid (tag 25)
);
```

UUID version is validated on encode — `uuidV1()` rejects non-v1, `uuidV7()`
rejects non-v7.

### Enums

```java
// Self-describing (string on wire)
MNode node = MNode.of("metric", MNode.enumVal("angular"));

// Ordinal-encoded (4 bytes on wire)
MNode node = MNode.of("metric", MNode.enumOrd(1, "euclidean", "angular", "cosine"));
```

### Collections

```java
// Heterogeneous list
MNode node = MNode.of("tags", List.of("ann", "benchmark"));

// Nested MNode
MNode inner = MNode.of("engine", "hnsw", "ef", 200L);
MNode node = MNode.of("config", inner);

// Typed array (fixed-size elements, no boxing)
MNode node = MNode.of("vectors", MNode.array(new float[]{1.0f, 2.0f, 3.0f}));

// Set (unique scalars/strings)
MNode node = MNode.of("labels", Set.of("train", "test", "val"));

// Typed-key map
Map<Object, Object> map = new LinkedHashMap<>();
map.put("key1", "value1");
map.put(42L, true);
MNode node = MNode.of("lookup", MNode.typedMap(map));
```

## Reading values

### Accessor summary

**Widening is within-family only.** Integer accessors widen within integers
(short→int→long). Float accessors widen within floats (half→float→double).
Cross-family widening (e.g. `getDouble()` on an int field) is not supported —
use explicit conversion if needed.

| Accessor | Returns | Accepts tags (widening) |
|----------|---------|------------------------|
| `getString(key)` | `String` | string, text, ascii |
| `getLong(key)` | `long` | long, int, short |
| `getInt(key)` | `int` | int, short |
| `getShort(key)` | `short` | short |
| `getDecimal(key)` | `BigDecimal` | decimal |
| `getVarint(key)` | `BigInteger` | varint |
| `getDouble(key)` | `double` | double, float, half |
| `getFloat(key)` | `float` | float, half |
| `getBoolean(key)` | `boolean` | boolean |
| `getBytes(key)` | `byte[]` | byte |
| `getEnum(key)` | `String` | enum_str, enum_ord |
| `getEnumOrdinal(key)` | `int` | enum_ord |
| `getMillis(key)` | `Instant` | millis |
| `getNanos(key)` | `Instant` | nanos |
| `getDate(key)` | `LocalDate` | date |
| `getTime(key)` | `LocalTime` | time |
| `getDateTime(key)` | `Instant` | datetime |
| `getDateTime(key, ZoneId)` | `ZonedDateTime` | datetime |
| `getUuidV1(key)` | `UUID` | uuidv1 |
| `getUuidV7(key)` | `UUID` | uuidv7 |
| `getUlid(key)` | `Ulid` | ulid |
| `getList(key)` | `List<Object>` | list |
| `getSet(key)` | `Set<Object>` | set |
| `getArray(key, Class<T>)` | `T` (typed array) | array |
| `getArrayElementType(key)` | `Class<?>` | array |
| `getNode(key)` | `MNode` | node |
| `getTypedMap(key)` | `Map<Object, Object>` | map |

Every getter has a corresponding `findXxx(key)` variant returning `Optional`.

`getArray(key, Class<T>)` returns the array cast to the requested type, e.g.
`getArray("vectors", float[].class)`. `getArrayElementType(key)` returns the
element type class (e.g. `float.class`, `long.class`) so callers can branch
on the array type before retrieving it.

### Typed accessor examples

```java
String name   = node.getString("name");     // tags 0, 10, 11 (widens)
long count    = node.getLong("count");       // tags 1, 12, 13 (widens)
int dims      = node.getInt("dims");         // tags 12, 13 (widens)
short rank    = node.getShort("rank");       // tag 13 only
double score  = node.getDouble("score");     // tags 2, 16, 17 (widens)
float weight  = node.getFloat("weight");     // tags 16, 17 (widens)
BigDecimal p  = node.getDecimal("price");    // tag 14
BigInteger b  = node.getVarint("big");       // tag 15
boolean flag  = node.getBoolean("active");   // tag 3
byte[] blob   = node.getBytes("checksum");   // tag 4
String metric = node.getEnum("metric");      // tags 6, 7
int ordinal   = node.getEnumOrdinal("m");    // tag 7
Instant ts    = node.getMillis("created");   // tag 18
Instant prec  = node.getNanos("precise");    // tag 19
LocalDate d   = node.getDate("date");        // tag 20
LocalTime t   = node.getTime("time");        // tag 21
Instant dt    = node.getDateTime("logged");  // tag 22
ZonedDateTime z = node.getDateTime("logged", ZoneId.of("UTC"));
UUID v1       = node.getUuidV1("v1");        // tag 23
UUID v7       = node.getUuidV7("v7");        // tag 24
Ulid ulid     = node.getUlid("ulid");        // tag 25
float[] vecs  = node.getArray("vectors", float[].class);  // tag 26
Class<?> et   = node.getArrayElementType("vectors");       // tag 26
List<Object>  = node.getList("tags");        // tag 8
Set<Object>   = node.getSet("labels");       // tag 27
Map<O,O> m    = node.getTypedMap("lookup");  // tag 28
MNode config  = node.getNode("config");      // tag 9
```

### Optional variants

Every getter has a corresponding `findXxx(key)` that returns `Optional.empty()`
for missing keys or type mismatches:

```java
Optional<String> name = node.findString("name");
Optional<Long>   cnt  = node.findLong("count");
Optional<Double> scr  = node.findDouble("score");
Optional<Instant> ts  = node.findMillis("created");
// ... etc for all types
```

### Raw access

```java
Object val = node.get("key");      // null if absent
boolean ok = node.has("key");      // true if present and non-null
int n      = node.size();          // field count
Map<String, Object> map = node.toMap();
```

## Widening rules

**Within-family only.** No cross-family widening.

### Integer family

| Accessor     | Accepts tags          |
|--------------|-----------------------|
| `getLong()`  | long, int, short      |
| `getInt()`   | int, short            |
| `getShort()` | short                 |

### Float family

| Accessor      | Accepts tags           |
|---------------|------------------------|
| `getDouble()` | double, float, half    |
| `getFloat()`  | float, half            |

### Text family

| Accessor       | Accepts tags              |
|----------------|---------------------------|
| `getString()`  | string, text, ascii       |

**Cross-family is rejected.** `getLong()` on a double field throws.
`getDouble()` on a long field throws. Use explicit conversion.

## Marker factory validation

| Factory    | Validation                                          |
|------------|-----------------------------------------------------|
| `text()`   | Well-formed UTF-8 (CharsetDecoder with REPORT)     |
| `ascii()`  | Every char in U+0020–U+007E                        |
| `uuidV1()` | `uuid.version() == 1`                              |
| `uuidV7()` | `uuid.version() == 7`                              |

## Enum handling

Self-describing enums (`enumVal`) store the string directly:

```java
MNode node = MNode.of("status", MNode.enumVal("active"));
node.getEnum("status"); // "active"
```

Ordinal enums (`enumOrd`) store a 4-byte integer:

```java
// Inline definition
MNode node = MNode.of("status", MNode.enumOrd(1, "inactive", "active"));
node.getEnum("status"); // "active"

// Deferred definition
MNode decoded = MNode.fromBytes(bytes);
decoded.getEnumOrdinal("status");  // always available
decoded.getEnum("status");         // throws IllegalStateException

MNode resolved = decoded.withEnumDef("status", "inactive", "active");
resolved.getEnum("status");        // "active"
```

## Serialization

### Raw bytes (no framing)

```java
byte[] wire = node.toBytes();
MNode back  = MNode.fromBytes(wire);
```

### Length-prefixed ByteBuffer (for streams)

```java
ByteBuffer buf = ByteBuffer.allocate(4096);
node.encode(buf);       // writes [len:4][payload:N]
buf.flip();
MNode back = MNode.fromBuffer(buf);
```

Multiple nodes can be written sequentially into the same buffer.

## Wire format

All multi-byte integers are little-endian.

```
[field_count : 2 bytes]
for each field:
  [name_len  : 2 bytes]
  [name_utf8 : name_len bytes]
  [type_tag  : 1 byte]
  [value     : variable, depends on type_tag]
```

The ByteBuffer framing (`encode`/`fromBuffer`) prepends a 4-byte length:

```
[total_payload_len : 4 bytes]
[payload           : total_payload_len bytes]
```

## Design notes

- **No Map allocation on decode.** Decoded data lives in parallel arrays
  (`keys[]`, `types[]`, `values[]`). Field lookup is a linear scan — fast
  for the small field counts typical in metadata records (< 20 fields).
- **Enum definitions are lazy.** The `enumDefs` field is `null` until an
  ordinal enum with a definition is present.
- **Half-precision uses pure-Java IEEE 754 bit manipulation** for the
  `floatToHalf()`/`halfToFloat()` converters. Works on Java 11+. The
  `Half` value class in `src/main/java25` uses `Float.float16ToFloat()`
  and `Float.floatToFloat16()` (Java 20+).
- **Typed arrays** store fixed-size elements contiguously with no per-element
  type tag — efficient for large numeric arrays. Matches
  [RFC 8746](https://datatracker.ietf.org/doc/html/rfc8746) typed arrays.
- **Sets** enforce uniqueness via `equals()` at construction/decode time.
- **Typed maps** allow arbitrary scalar/string keys, unlike nested MNode
  which requires string keys.
- **Immutable.** MNode instances are immutable after construction.
  `withEnumDef()` returns a new instance that shares the underlying arrays.
- **Deterministic encoding.** Set elements and typed-map keys are sorted on
  encode (by type tag first, then natural ordering), producing identical wire
  bytes regardless of insertion order.

---

## Type aliases

Each type tag has a primary CDDL name plus zero or more aliases. The `resolveTag()`
method accepts any alias and returns the canonical tag byte. The `cddlName()` method
returns the primary name.

| Primary name | Tag | Aliases |
|-------------|-----|---------|
| string      | 0   | tstr |
| long        | 1   | bigint |
| double      | 2   | float64 |
| bool        | 3   | boolean |
| byte        | 4   | bytes, blob, binary |
| null        | 5   | |
| enum_str    | 6   | |
| enum_ord    | 7   | |
| list        | 8   | |
| node        | 9   | |
| text        | 10  | |
| ascii       | 11  | |
| int         | 12  | |
| short       | 13  | smallint |
| decimal     | 14  | |
| varint      | 15  | |
| float32     | 16  | float |
| half        | 17  | float16 |
| millis      | 18  | epochmillis |
| nanos       | 19  | epochnanos |
| date        | 20  | |
| time        | 21  | |
| datetime    | 22  | timestamp |
| uuidv1      | 23  | timeuuid |
| uuidv7      | 24  | |
| ulid        | 25  | |
| array       | 26  | |
| set         | 27  | |
| map         | 28  | |

---

## CDDL text codec

`MNodeCddlCodec` provides stateless `format(MNode) → String` and
`parse(String) → MNode` methods for human-readable CDDL-like notation.

### Format example

```
{
  name: text = "glove-100",
  dims: int = 128,
  config: node = { engine: text = "hnsw" }
}
```

### Value formatting by tag

| Tag family | Format |
|-----------|--------|
| text/string/ascii/enum_str | `"escaped"` |
| long/int32/short/enum_ord | bare number |
| double | bare decimal `0.95` |
| float32 | `0.95f` suffix |
| half | `1.5h` suffix |
| bool | `true`/`false` |
| bytes | `h'0a1b2c'` (hex, CDDL convention) |
| null | `null` |
| decimal | `decimal("123.45")` |
| varint | `varint("999999999")` |
| millis | `millis("2026-02-25T14:30:00Z")` |
| nanos | `nanos("2026-02-25T14:30:00.123456789Z")` |
| date/time/datetime | `"ISO string"` |
| uuidv1/v7 | `"uuid-string"` |
| ulid | `"crockford-base32"` |
| list | `[val, val, ...]` |
| set | `set [val, val, ...]` |
| node | nested `{ ... }` |
| array | `[val, val, ...]` (type from field declaration) |
| typed-map | `{ key => val, key => val }` (fat arrow) |

### Parser grammar

```
node       := '{' field (',' field)* ','? '}'
field      := IDENT ':' type_spec '=' value
type_spec  := IDENT | '[' IDENT ']'
value      := string_lit | number_lit | bool_lit | null_lit
            | list_lit | set_lit | node_lit | bytes_lit
```

Type names resolve via `MNode.resolveTag()` — accepts all aliases.
