# nbdatatools

This is an accessory module of the NoSQLBench project, focusing on test data management, 
particularly vector test data used for ANN testing.

Testing tools that require specialized data sometimes need an additional line of defense to ensure
that the data is appropriate. This repo is a place to put such tools in support of NoSQLBench and
other testing systems.


## modules

### specs

This documents the vectordata layout standard (dataset.yaml + file facets) used by this repo. The conventions described here
are directly supported by the other modules.

The format used was initially inspired by
that of [ann-benchmark](https://github.com/erikbern/ann-benchmarks), but has since been extended
to support a significant variety of test data configurations.

### VectorData

This is an API for working directly with a test data format documented in this repo. This allows 
multiple testing systems to access the same data easily and consistently.  

#### vectordata Javadoc

[VectorData javadocs](https://javadoc.io/doc/io.nosqlbench/vectordata/latest/index.html)
are graciously hosted by [javadoc.io](https://javadoc.io/).

### nbvectors

This is the executable CLI that ships the vector test data tools. Run `java -jar nbvectors.jar --help` (or append `--help` to any command) for full option details.

Current commands and subcommands (run `--help` on any of these for options):

```
analyze     Inspect vector datasets
  count_zeros    Count zero vectors
  describe       Summarize dataset structure
  select         Extract vectors by index/range
  slice          Window data by range
  find           Locate vectors matching criteria
  check-endian   Endianness sanity check
  verify_knn     Verify KNN answer-keys for one profile
  verify_profiles Efficient multi-profile KNN verification
  flamegraph     Profile hotspots during analysis

convert     Convert between vector formats
  file          fvec/ivec/bvec/csv/json ↔ other formats

compute     CPU helpers
  knn           Generate ground-truth neighbors
  sort          External merge sort for vectors

generate    Produce or slice data
  dataset       Create sample dataset with dataset.yaml
  vectors       Generate random vectors
  mktestdata    Build base/query/ground-truth trio
  fvec-extract  Slice float vectors
  ivec-extract  Slice index files
  ivec-shuffle  Reshuffle integer vectors

datasets    Work with catalogs and downloads
  list          Browse catalogs
  download      Pull datasets/profiles
  prebuffer     Warm caches
  plan          Emit nbvectors commands to build missing artifacts
  curlify       Emit curl commands for remote dataset.yaml with ranged reads

vectordata  Explore vectordata layouts
  info          Summarize dataset and profiles
  views         List views per profile
  profiles      List profile names
  size          Show counts/dimensions for a view
  sample        Print sample vectors from a view
  prebuffer     Prebuffer a view or profile
  cat           Stream vectors from a view
  verify        Prebuffer as a verification pass
  repl          Interactive explorer

catalog     Emit catalog.json/yaml for dataset roots

fetch       Download datasets from Hugging Face
  dlhf          API download with parquet support

merkle      Manage Merkle trees for remote integrity
  create        Build Merkle reference
  verify        Verify against reference
  summary       Summarize tree
  diff          Compare trees
  path          Show paths to leaves
  treeview      Render tree view
  spoilbits     Corrupt specific bits
  spoilchunks   Corrupt specific chunks

cleanup     Clean fvec files
  cleanfvec      Drop zero/duplicate vectors

version     Print version/build information
```

#### nbvectors Javadoc

[Nbvectors javadocs](https://javadoc.io/doc/io.nosqlbench/nbvectors/latest/index.html)
are graciously hosted by [javadoc.io](https://javadoc.io/).

## Testing

Tests are organized into three categories that can be run independently or together using Maven profiles:

| Command                              | Unit | Performance | Accuracy |
|--------------------------------------|------|-------------|----------|
| `mvn test`                           |  ✓   |             |          |
| `mvn test -Paccuracy`                |  ✓   |             |    ✓     |
| `mvn test -Pperformance`             |  ✓   |      ✓      |          |
| `mvn test -Pperformance,accuracy`    |  ✓   |      ✓      |    ✓     |
| `mvn test -Palltests`                |  ✓   |      ✓      |    ✓     |

- **Unit tests**: Core functionality tests, run by default
- **Performance tests**: Benchmarks and performance regression tests, tagged with `@Tag("performance")`
- **Accuracy tests**: Numerical precision and statistical accuracy tests, tagged with `@Tag("accuracy")`

## Java Version

This project uses multi-release JARs with Java 11 as the base bytecode target and Java 25 for
optional features like the Vector API. Build requires JDK 25. Generally speaking, one of the
most effective ways to speed up your Java app is to use a modern JVM. The same applies to
Java-based testing systems.

----

Ideally, users of these tools should have an experience like this:

* Consistent methods of finding documentation and getting CLI help
* Simple parameterization of commands and features
* User-friendly terminal output and status
* Basic quality-of-life features, like auto-completion and similar
