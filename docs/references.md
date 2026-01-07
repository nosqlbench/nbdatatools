# References

This page is the project’s curated reference list for the algorithms, estimators, and standards used across NBDataTools. Each entry includes:

1. Local documentation links where the reference is (or should be) used.
2. A stable link to the reference (DOI/RFC/spec page when available).
3. A caption describing what the reference is.
4. A short scope summary describing why it matters to the linked areas.

---

## Nearest Neighbors and Similarity

### REF-NN-COVER-HART-1967

- **Used in:** `user_manual/02-core-concepts.md`, `../datatools-vshapes/userdocs/core-concepts.md`, `../local/variate_strategy.md`
- **Link:** https://doi.org/10.1109/TIT.1967.1053964
- **Caption:** Cover, T. & Hart, P. (1967). *Nearest neighbor pattern classification*. IEEE Transactions on Information Theory.
- **Scope:** Grounds the definition and baseline properties of k-nearest neighbors (kNN) used throughout the project, including “ground-truth” kNN computation and downstream measures that rely on neighbor sets.

### REF-IR-SALTON-1975

- **Used in:** `user_manual/02-core-concepts.md`, `../datatools-vshapes/userdocs/core-concepts.md`
- **Link:** https://doi.org/10.1145/361219.361220
- **Caption:** Salton, G., Wong, A., & Yang, C. S. (1975). *A vector space model for automatic indexing*. Communications of the ACM.
- **Scope:** Canonical reference for the vector space model viewpoint and similarity-based retrieval framing (including cosine-style similarity discussions) used when interpreting embedding/vector datasets.

---

## Intrinsic Dimensionality and Hubness

### REF-LID-AMSALEG-2015

- **Used in:** `../datatools-vshapes/userdocs/core-concepts.md`, `../datatools-vshapes/userdocs/api-reference.md`, `../datatools-vshapes/analysis_dag.md`, `../local/variate_strategy.md`
- **Link:** https://doi.org/10.1145/2783258.2783405
- **Caption:** Amsaleg, L., et al. (2015). *Estimating Local Intrinsic Dimensionality*. (ACM venue; DOI resolves to the canonical record.)
- **Scope:** Provides the literature basis for the maximum-likelihood LID estimator used by `vshapes` (the log-ratio formula over kNN distances) and informs default parameter choices and interpretation guidance.

### REF-HUBNESS-RADOVANOVIC-2010

- **Used in:** `../datatools-vshapes/userdocs/core-concepts.md`, `../datatools-vshapes/userdocs/api-reference.md`, `../datatools-vshapes/analysis_dag.md`, `../local/variate_strategy.md`
- **Link:** https://www.jmlr.org/papers/v11/radovanovic10a.html
- **Caption:** Radovanović, M., Nanopoulos, A., & Ivanović, M. (2010). *Hubs in Space: Popular Nearest Neighbors in High-Dimensional Data*. Journal of Machine Learning Research (JMLR).
- **Scope:** Establishes “hubness” as a high-dimensional kNN phenomenon and motivates reverse-kNN / in-degree style measurements; it is the conceptual grounding for the hubness-related explanations and diagnostics in `vshapes`.

---

## Online/Streaming Statistics and Sampling

### REF-RESERVOIR-VITTER-1985

- **Used in:** `../datatools-vshapes/userdocs/streaming-analyzers.md`, `../local/variate_strategy.md`
- **Link:** https://doi.org/10.1145/3147.3165
- **Caption:** Vitter, J. S. (1985). *Random sampling with a reservoir*. ACM Transactions on Mathematical Software.
- **Scope:** Canonical reference for reservoir sampling in streaming contexts; relevant to any “single-pass”/bounded-memory sampling described for analyzers, model extraction, or verification subsampling.

### REF-ONLINE-VAR-WELFORD-1962

- **Used in:** `../datatools-vshapes/vshapes.md`, `../local/variate_strategy.md`
- **Link:** https://doi.org/10.1080/00401706.1962.10490022
- **Caption:** Welford, B. P. (1962). *Note on a Method for Calculating Corrected Sums of Squares and Products*. Technometrics.
- **Scope:** Grounds numerically stable one-pass (online) updates for mean/variance that underpin streaming moment estimation; relevant when documenting how per-dimension statistics are accumulated without storing all data.

### REF-PARALLEL-MOMENTS-PEBAY-2008

- **Used in:** `../datatools-vshapes/vshapes.md`, `../local/variate_strategy.md`
- **Link:** https://doi.org/10.2172/1028931
- **Caption:** Pébay, P. (2008). *Formulas for robust, one-pass parallel computation of covariances and arbitrary-order statistical moments*. (Technical report; DOI resolves via OSTI.)
- **Scope:** Reference for parallel/mergeable moment formulas (including higher-order moments) that are useful when explaining correctness of parallel extraction, map/reduce-style aggregation, and numeric stability of summary statistics.

---

## Random Number Generators

### REF-RNG-MERSENNE-TWISTER-1998

- **Used in:** `../datatools-virtdata/src/main/java/io/nosqlbench/datatools/virtdata/vector_gen.md`, `../local/variate_strategy.md`
- **Link:** https://doi.org/10.1145/272991.272995
- **Caption:** Matsumoto, M. & Nishimura, T. (1998). *Mersenne Twister: A 623-dimensionally equidistributed uniform pseudorandom number generator*. ACM Transactions on Modeling and Computer Simulation.
- **Scope:** Canonical reference for the MT family of PRNGs, relevant when documenting generator determinism, seed choices, and the statistical expectations of PRNG-driven workflows.

### REF-RNG-SPLITTABLE-2014

- **Used in:** `../datatools-virtdata/src/main/java/io/nosqlbench/datatools/virtdata/vector_gen.md`, `../local/variate_strategy.md`
- **Link:** https://doi.org/10.1145/2660193.2660195
- **Caption:** Steele Jr., G. L., Lea, D., & Flood, C. H. (2014). *Fast splittable pseudorandom number generators*. (ACM; DOI resolves to the canonical record.)
- **Scope:** Grounds “splittable” PRNG design goals (parallel streams, reproducible substreams), which is relevant for ordinal-based deterministic generation and parallel generation/verification strategies.

---

## Distribution Systems (Pearson)

### REF-PEARSON-1895

- **Used in:** `../datatools-vshapes/vshapes.md`, `../local/variate_strategy.md`
- **Link:** https://doi.org/10.1098/rsta.1895.0010
- **Caption:** Pearson, K. (1895). *Contributions to the mathematical theory of evolution—II. Skew variation in homogeneous material*. Philosophical Transactions of the Royal Society A.
- **Scope:** Primary source for the Pearson family/system framing used by `vshapes` when classifying/fitting distributions based on moments (skewness/kurtosis) and when explaining what “Pearson types” mean.

### REF-PEARSON-SYSTEM-ORD

- **Used in:** `../datatools-vshapes/vshapes.md`
- **Link:** https://doi.org/10.1002/0471667196.ess1939
- **Caption:** Ord, J. K. (reference entry). *Pearson System of Distributions*. (Wiley encyclopedia entry; DOI resolves to the canonical record.)
- **Scope:** A modern secondary reference summarizing the Pearson system; useful for reader-oriented documentation that explains the system, its regions/types, and practical interpretation without relying only on 19th-century primary literature.

---

## Inverse CDF / Quantile Computation

### REF-NORMAL-QUANTILE-WICHURA-1988

- **Used in:** `../datatools-virtdata/src/main/java/io/nosqlbench/datatools/virtdata/vector_gen.md`, `../local/variate_strategy.md`
- **Link:** https://doi.org/10.2307/2347330
- **Caption:** Wichura, M. J. (1988). *Algorithm AS 241: The Percentage Points of the Normal Distribution*. Journal of the Royal Statistical Society. Series C (Applied Statistics).
- **Scope:** Canonical reference for accurate normal quantile computation (inverse normal CDF), relevant to documenting inverse-transform sampling, accuracy expectations, and the tradeoffs between approximations and precision.

---

## Merkle Trees, Hashes, and Range Requests (Standards)

### REF-MERKLE-1987

- **Used in:** `user_manual/02-core-concepts.md`, `user_manual/architecture.md`, `user_manual/07-advanced-topics.md`, `interfaces.md`
- **Link:** https://doi.org/10.1007/3-540-48184-2_32
- **Caption:** Merkle, R. C. (1987). *A Digital Signature Based on a Conventional Encryption Function*. (CRYPTO ’87 proceedings; DOI resolves to the canonical record.)
- **Scope:** Canonical reference for Merkle trees as a construction for integrity proofs; grounds the project’s “Merkle reference / partial verification” integrity layer and related documentation.

### REF-SHA256-FIPS-180-4

- **Used in:** `user_manual/02-core-concepts.md`, `user_manual/architecture.md`, `user_manual/07-advanced-topics.md`
- **Link:** https://nvlpubs.nist.gov/nistpubs/FIPS/NIST.FIPS.180-4.pdf
- **Caption:** NIST. *FIPS PUB 180-4: Secure Hash Standard (SHS)*. (Defines SHA-1/SHA-2 family, including SHA-256.)
- **Scope:** Normative reference for SHA-256, used to ground claims about hash sizes, security properties, and the semantics of hash-based integrity checks in Merkle structures.

### REF-HTTP-RANGE-RFC-7233

- **Used in:** `user_manual/04-data-formats.md`, `user_manual/architecture.md`
- **Link:** https://www.rfc-editor.org/rfc/rfc7233
- **Caption:** IETF. *RFC 7233: Hypertext Transfer Protocol (HTTP/1.1): Range Requests*.
- **Scope:** Normative basis for HTTP byte-range semantics (`Range` / `Content-Range`) used by the transport layer and referenced in docs about remote dataset access and partial downloads.

### REF-PARQUET-FORMAT

- **Used in:** `user_manual/04-data-formats.md`
- **Link:** https://parquet.apache.org/docs/file-format/
- **Caption:** Apache Parquet. *Parquet File Format* specification.
- **Scope:** Normative reference for Parquet concepts (columnar layout, schemas, encodings) used when describing Parquet ingestion/export behavior and interoperability expectations.

### REF-IEEE-754-2019

- **Used in:** `user_manual/04-data-formats.md`
- **Link:** https://standards.ieee.org/ieee/754/6210/
- **Caption:** IEEE. *IEEE Standard for Floating-Point Arithmetic (IEEE 754-2019)*.
- **Scope:** Normative reference for floating-point formats (including binary16/half precision) relevant to vector file encodings (e.g., half-precision storage) and correctness expectations when converting or interpreting numeric data.

