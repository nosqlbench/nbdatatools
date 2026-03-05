# Command Streams

Processing data from upstream sources requires several steps sometimes.
These steps are often time or resource intensive, and also non-trivial to implement.
To make this easy, we need to be able to describe the set of commands which are run in order to
transform source data from one form to another as part of curating, collating, generating, or
otherwise processing source or generated data into a dataset.

The basic form of this should be a command stream. The command stream should be a sequence of
commands which can be run in linearized order so that products from previous stages which are
needed to be processed are staged and verifiable before the next stage is run.

Further, the available commands need to be enumerated and managed in a way that ensures they
will be stable, well defined, and easy to use.

Many of the commands needed are implied by the nbvectors command, which uses also the command
implementations in datatools-commands. All these commands start with CMD_ as part of their
class name, and subcommands are strung together like CMD_umbrellacmd_subcmd_subcmd2, for example.

We also need a way to identify the files or resources to be included in command streams by
reference, such that literal argument interpolation is not needed duplicitously. Further, it
would be helpful to be able to create a basic DAG of processing steps needed to produce a whole
set of data files.

## Embedding in dataset.yaml

The command stream lives inside `dataset.yaml` so that a dataset is fully self-describing —
source provenance, transformation steps, and output layout all in one file. The `upstream`
element at the top level holds shared steps and defaults. Per-facet `upstream` lists hold
the steps that produce each facet's output.

### Motivating example: shuffle-split with ground truth

A common pipeline: take a large source file, generate a shuffle order, split it into base
and query partitions, then compute ground truth. The shuffle order is a shared intermediate
needed by both the base and query extraction steps.

```yaml
attributes:
  model: sift-128
  distance_function: COSINE
  vendor: nosqlbench
  license: Apache-2.0

upstream:
  defaults:
    seed: 42
    threads: 8
    metric: COSINE
    source: embeddings/img_emb     # upstream origin, e.g. HuggingFace path
  steps:
    - id: shuffle
      run: generate ivec-shuffle
      interval: 0..1000000
      seed: ${seed}
      output: shuffle.ivec

profiles:
  default:
    base:
      source: base.fvec
      upstream:
        - run: generate fvec-extract
          ivec-file: shuffle.ivec
          fvec-file: ${source}
          range: "[100000,1000000)"
          output: base.fvec
    query:
      source: query.fvec
      upstream:
        - run: generate fvec-extract
          ivec-file: shuffle.ivec
          fvec-file: ${source}
          range: "[0,100000)"
          output: query.fvec
    indices:
      source: indices.ivec
      upstream:
        - run: compute knn
          base: base.fvec
          query: query.fvec
          neighbors: 100
          metric: ${metric}
          threads: ${threads}
          indices: indices.ivec
    distances:
      source: distances.fvec
      # produced as a side-effect of compute knn (--distances flag)
```

### Format rules

Each step is a YAML map. The only required key is `run`, which names the command. All other
keys map directly to command options (long-name form, without the `--` prefix). This keeps
steps compact — a simple step fits on two or three YAML lines — while remaining readable.

- `run: <command>` — the nbvectors subcommand path (e.g. `generate vectors`, `compute knn`)
- `id: <name>` — optional, for DAG references and logging; auto-derived from `run` if absent
- `after: [<id>, ...]` — optional, explicit ordering constraints beyond natural file deps
- All other keys — command options, passed as `--key value`
- `${name}` — interpolated from `upstream.defaults`, overridable at invocation time

### Shared steps vs. per-facet steps

Steps under `upstream.steps` (top level) run once before any facet steps. They produce
shared intermediates (like `shuffle.ivec` above) that multiple facets depend on.

Steps under a facet's `upstream` list run in order to produce that facet's output file.
The runner resolves the execution order: shared steps first, then facet steps in dependency
order (if facet A's output is an input to facet B's step, A runs first).

### xvec format auto-selection

The `FileType.xvec` enum value is a family, not a single format. It covers fvec (float),
ivec (int), bvec (byte), dvec (double), and hvec (half-precision float). When a step or a
`defaults` block specifies `format: xvec`, the runner must resolve it to the concrete
variant based on context.

Resolution order:

1. **Output file extension** — If the step's output path has a concrete extension (`.fvec`,
   `.ivec`, `.hvec`, `.dvec`, `.bvec`), that determines the variant. This is the most
   common case and the most explicit.

2. **Data type option** — If the step specifies a `type` option (e.g. `type: float[]`,
   `type: int[]`), the variant is derived from the type-to-extension mapping:

   | Type | Extension | Notes |
   |------|-----------|-------|
   | `float[]` | `.fvec` | 32-bit float |
   | `int[]` | `.ivec` | 32-bit int |
   | `byte[]` | `.bvec` | 8-bit byte |
   | `double[]` | `.dvec` | 64-bit double |
   | `float[]` + half | `.hvec` | 16-bit half; requires explicit `precision: half` or `.hvec` extension |

3. **Input file format** — For commands like `convert file` where the output format should
   match the input's element type, the runner can infer from the input file's extension or
   header. For example, converting from `.hvec` to xvec would default to `.fvec` (widening
   half to float), since the output is `float[]`.

4. **Ambiguity is an error** — If none of the above resolves the variant (e.g.,
   `format: xvec` with no extension, no type, and no input to infer from), the dry-run
   flags this as a validation error rather than guessing.

This means users can write either:

```yaml
  - run: generate vectors
    type: float[]
    format: xvec          # resolved to .fvec from type
    output: base.fvec     # or: extension alone is sufficient
```

or simply use the concrete extension in the output path and omit `format` entirely, since
the SPI `@FileExtension` annotations on reader/writer classes already bind extensions to
implementations.

### Compact single-step shorthand

When a facet needs only one transformation step, the `upstream` can be a single map instead
of a list:

```yaml
    base:
      source: base.fvec
      upstream:
        run: convert file
        input: raw_base.hvec
        output: base.fvec
```

## Programmatic command representation

Using nbvectors command names is sufficient for semantics and planning, but the runtime
needs a representation of commands that is more direct than argv strings and exit codes.
Each leaf command should implement a structured interface alongside its PicoCLI `Callable`
implementation so that the command stream runner can invoke it without string parsing.

### CommandOp interface

```java
/// A single executable operation in a command stream.
///
/// Every leaf CMD_ class implements this alongside its PicoCLI Callable<Integer>.
/// The command stream runner calls {@link #execute} directly with a typed options
/// map, bypassing argv construction and PicoCLI parsing.
public interface CommandOp {

    /// The canonical command path, e.g. "generate vectors" or "compute knn".
    ///
    /// @return the dot-separated or space-separated command path
    String commandPath();

    /// Execute this command with the given options.
    ///
    /// @param options named options as parsed from the YAML step map
    /// @param context shared execution context (workspace, defaults, progress)
    /// @return result describing success/failure and produced artifacts
    CommandResult execute(Map<String, Object> options, StreamContext context);

    /// Describe the options this command accepts for validation and dry-run.
    ///
    /// @return list of option descriptors
    List<OptionDesc> describeOptions();
}
```

### CommandResult

```java
/// Result of executing a single command stream step.
public record CommandResult(
    Status status,           // OK, WARNING, ERROR
    String message,          // human-readable summary
    List<Path> produced,     // files this step created
    Duration elapsed
) {
    public enum Status { OK, WARNING, ERROR }
}
```

### StreamContext

```java
/// Shared state across all steps in a command stream execution.
public record StreamContext(
    Path workspace,                    // staging directory
    Map<String, String> defaults,      // interpolated defaults
    StatusTracker statusTracker,       // progress reporting
    boolean dryRun                     // if true, validate but don't execute
) {}
```

### Benefits over argv dispatch

- **Type safety**: options are `Map<String, Object>`, not `String[]`. Numeric, path, and
  enum values are validated before execution.
- **Structured results**: `CommandResult` replaces exit codes. The runner knows exactly what
  files were produced, can verify them, and can report timing without parsing stdout.
- **Testability**: commands can be unit-tested by calling `execute()` directly with a map,
  without constructing fake CLI environments.
- **Introspection**: `describeOptions()` enables dry-run validation, auto-generated help,
  and schema checking of YAML steps at parse time rather than at execution time.

The existing `Callable<Integer> call()` in each CMD_ class continues to work for direct
CLI use. `CommandOp.execute()` wraps the same logic but bridges the YAML-map world to the
internal field assignments that PicoCLI currently handles.

## Defaults and interpolation

```yaml
upstream:
  defaults:
    seed: 42
    threads: 8
    metric: COSINE
    workspace: .              # relative to dataset.yaml location
```

### Scoping rules

1. CLI overrides (`nbvectors run-stream --set threads=16`) take highest precedence.
2. Step-level values override `upstream.defaults`.
3. `upstream.defaults` override built-in fallbacks in each command.
4. `${env:VAR_NAME}` reads from the process environment at parse time.
5. `${name:-fallback}` provides inline defaults.

### Implicit variables

The runner injects these without explicit declaration:

| Variable        | Value                                      |
|-----------------|--------------------------------------------|
| `${dataset_dir}`| directory containing the `dataset.yaml`    |
| `${workspace}`  | defaults.workspace, resolved to absolute   |
| `${profile}`    | current profile name being materialized    |

## Execution model

### Ordering

1. Parse `upstream.defaults` and all steps.
2. Build a DAG: edges from `after` declarations plus implicit file-dependency edges
   (if step B's input matches step A's output, B depends on A).
3. Topological sort. If the graph is a simple chain, execution is linear.
4. Execute steps. Sequential by default; bounded parallelism is opt-in via
   `upstream.defaults.parallelism`.

### Skip-if-fresh and partial artifact handling

Before executing a step, the runner checks: does the output already exist, and is it newer
than all inputs? If yes, the step is skipped with a log line. This makes re-running after
a mid-pipeline failure cheap.

However, an existing output may be *partial* — the product of a step that started but did
not finish (crash, timeout, disk-full, etc.). The runner must distinguish between a complete
artifact and a truncated one before skipping. This requires a **bound check**: a
format-aware validation that the artifact is structurally complete.

#### Bound checks by format

| Format | Bound check |
|--------|-------------|
| xvec (fvec, ivec, hvec, dvec, bvec) | File size is an exact multiple of record size (`4 + dim * element_width`). `ReaderUtils.computeVectorCount` already enforces this. A partial final record means the file is truncated. |
| slab | `slab check` validates page structure and ordinal continuity. A truncated page or missing footer indicates partial write. |
| merkle (.mrkl) | Tree node count must match the expected count for the data file's chunk count. |
| parquet | Footer magic bytes present at end of file. Missing footer means truncated write. |
| model files (JSON/binary) | Parseable without error. |

#### Resume vs. restart

Some formats support appending (the step can pick up where it left off), others do not
(the partial file must be discarded and the step re-run from scratch). Each step declares
which strategy applies:

- **restartable** (default): if the bound check fails, delete the partial output and re-run
  the step from the beginning. This is correct for any command — worst case is redundant
  work.
- **resumable**: if the bound check fails, the step receives the partial artifact and
  appends to it. This requires explicit support from the command. Currently, `slab append`
  supports this pattern. A resumable step must also validate that the existing partial
  content is consistent with the current inputs (e.g., same dimension, same source file).

A step declares its strategy in the YAML:

```yaml
  - id: import-metadata
    run: slab import
    on-partial: resume       # resume | restart (default: restart)
    from: raw_metadata.jsonl
    output: metadata.slab
```

#### Bound check at step boundaries

The bound check runs at two points:

1. **Before skipping** — If the output exists and passes the bound check and is newer than
   inputs, the step is skipped. If the output exists but *fails* the bound check, the
   runner applies the `on-partial` strategy (delete and restart, or resume).
2. **After execution** — After a step completes, the runner validates its output with the
   same bound check. A step that reports OK but produces a truncated file is promoted to
   ERROR. This catches silent failures (e.g., a command that exits 0 but was killed before
   flushing buffers).

#### Bound check interface

The programmatic API exposes this as part of `CommandOp`:

```java
/// Check whether an existing output artifact is complete.
///
/// @param output the path to the artifact to validate
/// @param options the options this step was (or would be) invoked with
/// @return COMPLETE if the artifact is valid and the step can be skipped,
///         PARTIAL if the artifact exists but is incomplete,
///         ABSENT if the artifact does not exist
ArtifactState checkArtifact(Path output, Map<String, Object> options);

enum ArtifactState { COMPLETE, PARTIAL, ABSENT }
```

For formats where bound-checking is generic (xvec record-size divisibility, slab footer
presence), the runner can apply the check without delegating to the command. The command's
`checkArtifact` override is for cases where validity depends on step-specific context (e.g.,
expected vector count or dimension).

### Error handling

- **fail-fast** (default): stop on first ERROR result.
- **continue-on-warn**: WARNING results do not halt; ERROR results do.

### Dry-run

`nbvectors run-stream dataset.yaml --dry-run` performs a full validation pass without
writing any files. The goal is to surface every error that is knowable before execution
actually begins, so that a user does not discover a typo in step 7 after steps 1–6 have
already burned hours of compute.

#### Phase 1: Parse and resolve

1. Parse the full `upstream` block and all per-facet `upstream` lists.
2. Resolve all `${variable}` interpolations. Flag any unresolved variables as errors.
3. Resolve each `run:` value to its `CommandOp` class. Flag unknown commands as errors.

#### Phase 2: Validate options

For each step, compare its option keys against the command's `describeOptions()`:

- **Unknown options**: keys that the command does not accept → ERROR.
- **Missing required options**: required options not supplied and not satisfiable from
  defaults → ERROR.
- **Type mismatches**: a value that cannot be coerced to the option's declared type (e.g.,
  `threads: abc`) → ERROR.
- **Enum validation**: option values that must be one of a known set (e.g., `metric: COSINE`)
  are checked against the allowed values → ERROR if not recognized.
- **Range/interval syntax**: values like `[0,100000)` or `0..1000000` are parsed to confirm
  they are well-formed → ERROR on malformed syntax.
- **VectorDataSpec resolution**: input specs are checked for syntactic validity. For
  catalog references, the catalog is queried to confirm the dataset/profile/facet exists.
  For local file paths that refer to outputs of earlier steps, the reference is noted as
  satisfied-by-DAG rather than checked on disk.

#### Phase 3: DAG construction and flow analysis

1. Build the full DAG from explicit `after:` edges and implicit file-dependency edges
   (step B names an input that matches step A's output).
2. Check for **cycles** → ERROR.
3. Check for **dangling inputs**: a step references an input file that is neither an output
   of a prior step, an existing file on disk, nor a resolvable VectorDataSpec → ERROR.
4. Check for **output collisions**: two steps that produce the same output path → ERROR
   (unless one is an explicit `after:` dependency of the other, implying intentional
   overwrite).
5. Topological sort to determine execution order.

#### Phase 4: Artifact state assessment

Walk the DAG in topological order. For each step:

1. Run `checkArtifact` on its output. Mark the step as SKIP (output COMPLETE and newer
   than inputs), NEEDED (output ABSENT or PARTIAL), or STALE (output COMPLETE but older
   than an input that will be regenerated by an upstream NEEDED step).
2. Propagate staleness: if step A is NEEDED, all downstream steps that depend on A's
   output are at minimum STALE, even if their own outputs currently pass the bound check.

This determines the **execution frontier**: the set of steps that actually need to run.
Steps before the frontier are skipped; steps at and after the frontier are NEEDED.

#### Dry-run output

The dry-run prints the resolved plan with per-step status:

```
Step              Status    Command                          Output
────────────────  ────────  ───────────────────────────────  ──────────────────
shuffle           SKIP      generate ivec-shuffle ...        shuffle.ivec (OK)
base              NEEDED    generate fvec-extract ...        base.fvec (ABSENT)
query             STALE     generate fvec-extract ...        query.fvec (stale)
knn               STALE     compute knn ...                  indices.ivec (stale)
────────────────  ────────  ───────────────────────────────  ──────────────────
Steps to execute: 3 of 4
```

If any validation errors were found in phases 1–3, they are printed first and the plan
is not shown (since the DAG may be incoherent). If validation passes but some steps are
NEEDED/STALE, the plan is printed and the exit code is 0. This lets a CI script run
`--dry-run` as a pre-flight check before committing to a long execution.

## Staging and shared intermediates

Intermediate files live alongside the dataset.yaml (or in a subdirectory if `workspace`
is set). Shared intermediates like `shuffle.ivec` are produced by top-level `upstream.steps`
and consumed by per-facet steps.

### Intermediate lifecycle

Intermediates that are not referenced as a facet `source` are transient by default. After
all consuming steps complete, the runner can delete them if `upstream.defaults.cleanup: true`
is set. Facet outputs are never auto-deleted.

### Validation between steps

Before a downstream step runs, the runner verifies its inputs:
1. File exists and is non-empty.
2. For xvec files, `ReaderUtils.checkXvecEndianness` confirms the file is well-formed.
3. For slab files, `slab check` equivalent validation.

This catches corruption early rather than propagating it through expensive downstream steps.

## Logging and progress

Each step's output is prefixed with its id:

```
[shuffle]     Generating shuffle order for 1000000 elements...
[shuffle]     Done in 0.4s → shuffle.ivec (3.8 MB)
[base]        Extracting 900000 vectors...
[base]        Done in 12.1s → base.fvec (461 MB)
```

Commands that use `StatusTracker` report progress through the runner's console.

### Progress log

The runner maintains a persistent progress log alongside the dataset.yaml (default:
`.upstream.progress.yaml`). This log records the outcome of every step execution — not
just success, but also failures, partial results, and the conditions under which each
step ran. It serves two purposes:

1. **Short-circuit dry-run checks** — The dry-run phases (parse, validate options, DAG
   analysis, artifact assessment) are valuable but have a cost: resolving VectorDataSpec
   references, querying catalogs, stat-ing files, running bound checks. When a progress
   log exists, the runner can read it first and skip validation for any step whose recorded
   state already answers the question. If the log says step `shuffle` completed OK with
   output `shuffle.ivec` at a recorded size and timestamp, and the file on disk still
   matches, the runner can mark it SKIP immediately without re-running bound checks or
   re-resolving its options.

2. **Capture error states** — When a step fails, the progress log records enough context
   to diagnose the failure without re-running. This includes the resolved options, the
   error message or exception, the exit status, and the state of any partial output. On
   the next run, the runner knows exactly which step failed and why, and can present this
   to the user before attempting a retry.

#### Progress log format

```yaml
# .upstream.progress.yaml — written by the runner, not user-edited
version: 1
started: 2026-03-03T14:22:00Z
defaults:
  seed: 42
  threads: 8

steps:
  shuffle:
    status: OK
    started: 2026-03-03T14:22:00Z
    finished: 2026-03-03T14:22:00.4Z
    duration_ms: 400
    command: generate ivec-shuffle
    resolved_options:
      interval: "0..1000000"
      seed: "42"
      output: shuffle.ivec
    outputs:
      - path: shuffle.ivec
        size: 4000004
        modified: 2026-03-03T14:22:00.4Z
        bound_check: COMPLETE

  base:
    status: OK
    started: 2026-03-03T14:22:00.4Z
    finished: 2026-03-03T14:22:12.5Z
    duration_ms: 12100
    command: generate fvec-extract
    resolved_options:
      ivec-file: shuffle.ivec
      fvec-file: embeddings/img_emb
      range: "[100000,1000000)"
      output: base.fvec
    outputs:
      - path: base.fvec
        size: 461000000
        modified: 2026-03-03T14:22:12.5Z
        bound_check: COMPLETE

  knn:
    status: ERROR
    started: 2026-03-03T14:22:14Z
    finished: 2026-03-03T14:23:38Z
    duration_ms: 84000
    command: compute knn
    resolved_options:
      base: base.fvec
      query: query.fvec
      neighbors: "100"
      metric: COSINE
      indices: indices.ivec
    error:
      message: "java.lang.OutOfMemoryError: Java heap space"
      exit_status: 2
    outputs:
      - path: indices.ivec
        size: 2400000
        modified: 2026-03-03T14:23:38Z
        bound_check: PARTIAL
        on_partial: restart
```

#### How the progress log short-circuits validation

When the runner starts (whether `--dry-run` or a real execution), it loads the progress
log before entering the dry-run phases:

1. **Phase 1 (parse/resolve)** — Always runs. This is cheap and must reflect the current
   YAML, not a stale log.
2. **Phase 2 (validate options)** — For steps whose `resolved_options` in the log match the
   currently resolved options, option validation can be skipped (it already passed once
   with these exact values). If the options have changed, validation runs normally.
3. **Phase 3 (DAG construction)** — Always runs. The DAG structure depends on the current
   YAML, not past runs.
4. **Phase 4 (artifact assessment)** — This is where the biggest savings are. For each step:
   - If the log says `status: OK` and the output file's size and modified time match the
     log entry, the step is SKIP without running `checkArtifact` or stat-ing input files.
   - If the log says `status: ERROR`, the step is NEEDED. The runner prints the recorded
     error message so the user sees the failure context before the retry begins.
   - If the log says `bound_check: PARTIAL` with `on_partial: restart`, the runner knows
     to delete the partial output before re-running, without needing to re-run the bound
     check itself.
   - If no log entry exists for a step, or if the log entry's options don't match the
     current resolved options, the step is assessed normally (bound check, timestamp
     comparison, etc.).

#### Progress log lifecycle

- The runner creates or overwrites `.upstream.progress.yaml` at the start of each run.
- Each step entry is written (or updated) as the step completes — not batched at the end.
  This means that if the runner itself crashes (kill -9, OOM, power loss), the log reflects
  all steps that finished before the crash.
- The log is not intended to be user-edited, but is human-readable YAML for
  debuggability. A user inspecting why a pipeline stalled can read the log directly.
- `nbvectors run-stream dataset.yaml --clean` deletes the progress log and all transient
  intermediates, forcing a full re-run.

#### Error context in the progress log

The `error` block captures:

| Field | Content |
|-------|---------|
| `message` | The exception message or stderr summary |
| `exit_status` | The numeric exit code (0=OK, 1=WARNING, 2=ERROR) |
| `partial_output` | If the step produced partial output, its bound-check state |

On the next run, the runner prints the prior failure before retrying:

```
[knn]  Previous run failed: java.lang.OutOfMemoryError: Java heap space
[knn]  Partial output indices.ivec (2.4 MB, PARTIAL) will be deleted (on_partial: restart)
[knn]  Retrying...
```

This gives the user a chance to adjust defaults (e.g. increase heap, reduce partition size)
before the retry actually begins, especially in `--dry-run` mode where no execution happens.

### Run summary

After completion, a summary table is printed:

```
Step           Status    Duration   Output
─────────────  ────────  ─────────  ──────────────────
shuffle        SKIP      —          shuffle.ivec (OK)
base           OK        12.1s      base.fvec
query          OK        1.4s       query.fvec
knn            OK        84.3s      indices.ivec
─────────────  ────────  ─────────  ──────────────────
Total                    97.8s (1 skipped)
```

---

## Suggesting pipeline steps from upstream data

`datasets plan` (`CMD_datasets_plan`) already does a version of this: it reads a
`dataset.yaml`, finds missing facets, inspects existing files to infer dimension and k,
and emits suggested `nbvectors` command invocations. Today it outputs shell commands as
copy-paste text. With command streams, it should output `upstream` YAML blocks directly.

### What datasets plan knows today

Given a `dataset.yaml`, the planner:

1. Enumerates all facets across all profiles.
2. Checks which output files exist on disk.
3. For missing **base** or **query** vectors: suggests `generate vectors` with dimension
   inferred from any existing vector file in the same profile, count estimated from the
   facet's window/interval, type and format inferred from the output file extension.
4. For missing **indices** and **distances**: suggests `compute knn` with metric from
   `attributes.distance_function`, k from `profiles.<name>.maxk` or from an existing
   indices file, and normalization-aware metric defaulting.
5. Deduplicates suggestions when multiple profiles reference the same physical file.

### Extending to upstream block generation

The planner should gain a `--emit-upstream` flag (or this becomes the default output mode)
that writes structured `upstream` YAML rather than shell commands. This means:

#### Inspecting upstream sources

When a facet declares an `upstream.source` (as in the laion400b example), the planner
knows the data isn't generated from scratch — it comes from an external source that needs
transformation. The planner should recognize common upstream patterns:

| Upstream source pattern | Suggested pipeline |
|------------------------|--------------------|
| Raw vectors in a different format (e.g. `.hvec` source, `.fvec` output) | `convert file` step |
| Large source needing train/test split | `generate ivec-shuffle` → `generate fvec-extract` (×2) |
| Source vectors with no ground truth | `compute knn` step for indices + distances |
| Source with metadata alongside vectors | `slab import` step for metadata facets |
| Remote URL as source | `fetch dlhf` or direct download step first |

#### Inferring shared intermediates

The planner should detect when multiple facets need the same intermediate. The primary
case: if both `base` and `query` facets reference the same upstream source but with
different ranges/windows, the planner infers a shuffle-split:

```
upstream source → ivec-shuffle → shared shuffle.ivec
                                   ├── fvec-extract [N..M) → base.fvec
                                   └── fvec-extract [0..N) → query.fvec
```

Detection heuristic: if two facets in the same profile have the same `upstream.source`
and their windows are disjoint and together span the source size, emit a shuffle-split
pipeline with a shared intermediate.

#### Inferring step parameters

The planner draws from multiple sources to fill in step options:

| Parameter | Source |
|-----------|--------|
| `dimension` | Existing vector file in profile (via `VectorDatasetView.getVectorDimensions()`), or from upstream file header if accessible |
| `count` | Facet window/interval (`FWindow` → `estimateCount()`), or upstream file vector count |
| `k` (neighbors) | `profiles.<name>.maxk`, existing indices file dimension, or default 100 |
| `metric` | `attributes.distance_function`, normalization detection on base vectors, or default |
| `seed` | `attributes.generation_seed` if present, or suggest a fixed seed for reproducibility |
| `format` | Output file extension → `VectorFileExtension.fromExtension()` |
| `type` | Extension → data type mapping (`.fvec` → `float[]`, `.ivec` → `int[]`, etc.) |
| `threads` | Not inferable; left as `${threads}` for the user to set in `defaults` |

#### Example output

Given this partial `dataset.yaml`:

```yaml
attributes:
  distance_function: COSINE
  dimension: 128

profiles:
  default:
    base:
      source: base.fvec
      upstream:
        source: embeddings/vectors
    query:
      source: query.fvec
      upstream:
        source: embeddings/vectors
    indices:
      source: indices.ivec
    distances:
      source: distances.fvec
```

`nbvectors datasets plan --emit-upstream .` would output:

```yaml
upstream:
  defaults:
    seed: 42
    threads: 8
    metric: COSINE
  steps:
    - id: shuffle
      run: generate ivec-shuffle
      interval: 0..<inferred-from-source>
      seed: ${seed}
      output: shuffle.ivec

profiles:
  default:
    base:
      upstream:
        - run: generate fvec-extract
          ivec-file: shuffle.ivec
          fvec-file: embeddings/vectors
          range: "[<query-count>,<total>)"
          output: base.fvec
    query:
      upstream:
        - run: generate fvec-extract
          ivec-file: shuffle.ivec
          fvec-file: embeddings/vectors
          range: "[0,<query-count>)"
          output: query.fvec
    indices:
      upstream:
        - run: compute knn
          base: base.fvec
          query: query.fvec
          neighbors: 100
          metric: ${metric}
          threads: ${threads}
          indices: indices.ivec
    distances:
      # produced by compute knn --distances
```

Placeholders like `<inferred-from-source>` and `<query-count>` appear when the planner
can't determine the value without accessing the upstream data. The user fills these in,
or they are resolved at execution time if the upstream source is accessible.

#### Interaction with dry-run

The planner and the dry-run validator are complementary:

- **Planner** (`datasets plan`): "here is what you *should* run, given what's declared
  and what's missing." It generates the `upstream` block.
- **Dry-run** (`run-stream --dry-run`): "here is what *will* run, given what's declared
  and what already exists." It validates the `upstream` block the planner (or user)
  already wrote.

A typical workflow:
1. Write a `dataset.yaml` with facets and upstream sources.
2. Run `datasets plan --emit-upstream` to get a suggested `upstream` block.
3. Review and edit (fill in counts, adjust seeds, add custom steps).
4. Paste the `upstream` block into `dataset.yaml`.
5. Run `run-stream --dry-run` to validate before committing to execution.
6. Run `run-stream` to execute.

#### Programmatic API for the planner

The planner logic should also be accessible as a Java API, not just a CLI command, so
that other tools (IDE plugins, CI generators, the `generate dataset` command itself) can
call it:

```java
/// Analyze a dataset.yaml and suggest upstream pipeline steps.
///
/// @param datasetYaml path to the dataset.yaml
/// @return a suggested upstream block as a structured object
UpstreamPlan suggestUpstream(Path datasetYaml);
```

```java
/// A suggested upstream block, ready to serialize to YAML or execute.
public record UpstreamPlan(
    Map<String, String> defaults,
    List<StepSpec> sharedSteps,
    Map<String, Map<String, List<StepSpec>>> profileFacetSteps,  // profile → facet → steps
    List<String> warnings                                         // unresolvable parameters
) {}
```

```java
/// A single step specification in a suggested pipeline.
public record StepSpec(
    String id,
    String command,
    Map<String, String> options,
    List<String> after
) {}
```

---

## Command lexicon

Every leaf command that can appear in a `run:` step, with its full option surface.
Options listed as `name (type, default)` or `name (type, required)`.

### Shared option groups (mixins)

These bundles of options are reused across many commands. When a command includes a mixin,
it inherits all the options listed here.

**OutputFile**: `output (Path, required)`, `force (boolean, false)`

**InputFile**: `input (VectorDataSpec, required)` — supports range suffix `[m,n)`

**Range**: `range (String)` — formats: `n`, `m..n`, `[m,n)`, `[m,n]`, `(m,n)`, `[n]`

**RandomSeed**: `seed (long, current-time)`

**VectorSpec**: `dimension (int, required)`, `count (int, required)`

**ValueRange**: `min (double, 0.0)`, `max (double, 1.0)`

**BatchProcessing**: `threads (int, 0=auto)`, `memory-limit (int, 1024 MB)`,
`batch-size (int, 10000)`

**BaseVectors**: `base (VectorDataSpec, required)` — supports range suffix

**QueryVectors**: `query (VectorDataSpec, required)` — supports range suffix

**Indices**: `indices (Path, required)`, `force (boolean, false)`

**Distances**: `distances (Path)` — derived from indices path if omitted

**DistanceMetric**: `metric (enum, DOT_PRODUCT)` — values: L2, L1, COSINE, DOT_PRODUCT

**ParallelExecution**: `parallel (boolean)`, `threads (int)`

**Verbosity**: `verbose (boolean)`, `quiet (boolean)`

**ConsoleProgress**: `status (mode)`

**CatalogConfig**: `catalog (String)`, `configdir (Path)`, `cache-dir (Path)`,
`allow-remote (boolean)`

### generate vectors

Generate random vectors of a specified type and format.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| Mixin: OutputFile | | | |
| Mixin: VectorSpec | | | |
| Mixin: RandomSeed | | | |
| Mixin: ValueRange | | | |
| `type` | String | required | Java type signature, e.g. `float[]`, `int[]` |
| `format` | FileType | required | xvec, parquet, csv |
| `algorithm` | enum | XO_SHI_RO_256_PP | PRNG algorithm |
| `int-min` | int | 0 | min value for integer vectors |
| `int-max` | int | 100 | max value for integer vectors |

### generate dataset

Generate a complete dataset (base, query, ground truth) in one step.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| Mixin: RandomSeed | | | |
| Mixin: ValueRange | | | |
| `output-dir` | Path | required | |
| `dimension` | int | required | |
| `base-count` | int | 10000 | |
| `query-count` | int | 1000 | |
| `neighbors` | int | 100 | k for ground truth |
| `type` | String | float[] | |
| `format` | FileType | xvec | |
| `distance` | enum | L2 | |
| `algorithm` | enum | | PRNG algorithm |
| `model` | String | synthetic | |
| `license` | String | | |
| `vendor` | String | | |
| `notes` | String | | |
| `profile` | String[] | | additional profile names |
| `profile-base-count` | Map | | per-profile base counts |
| `profile-query-count` | Map | | per-profile query counts |
| `force` | boolean | false | |
| `tag` | Map | | arbitrary key-value tags |

### generate derive

Derive a new dataset from an existing one (e.g. subset, transform).

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| `source` | String | required | source dataset path |
| `target` | String | required | output path |
| `profile` | String | default | |
| `count` | int | | number of vectors |
| `name` | String | | |
| `sample` | | | sampling spec |
| `force` | boolean | false | |
| `verbose` | boolean | false | |
| `model-type` | String | auto | |

### generate from-model

Generate vectors from a statistical model file.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| Mixin: OutputFile | | | |
| Mixin: RandomSeed | | | |
| `model` | Path | required | path to model file |
| `count` | int | required | |
| `format` | FileType | required | |
| `verbose` | boolean | false | |

### generate sketch

Generate vectors with controlled distribution shape.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| Mixin: OutputFile | | | |
| Mixin: VectorSpec | | | |
| Mixin: RandomSeed | | | |
| `format` | FileType | required | |
| `mix` | String | bounded | |
| `model-spec` | String | | |
| `max-modes` | int | 4 | |
| `model-out` | Path | | save fitted model |
| `lower` | double | -1.0 | |
| `upper` | double | 1.0 | |
| `verbose` | boolean | false | |
| `normalize` | boolean | true | negatable |

### generate predicated

Generate a predicated (filtered) dataset with metadata.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| Mixin: RandomSeed | | | |
| `output-dir` | Path | required | |
| `record-count` | int | 1000000 | |
| `query-count` | int | 10000 | |
| `dimension` | int | 256 | |
| `limit` | int | 0 | k limit |
| `force` | boolean | false | |
| `algorithm` | enum | | PRNG algorithm |
| `named-fields` | | | |

### generate ivec-shuffle

Generate a random permutation as an ivec file.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| Mixin: OutputFile | | | |
| Mixin: RandomSeed | | | |
| `interval` | String | required | e.g. `0..1000000` |
| `algorithm` | enum | | PRNG algorithm |

### generate ivec-extract

Extract a subset of vectors by index array.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| Mixin: CatalogConfig | | | |
| `ivec-file` | VectorDataSpec | required | shuffle/index file |
| `range` | String | required | |
| `output` | Path | required | |
| `force` | boolean | false | |
| `threads` | int | 0 | |
| `memory-limit` | int | 1024 | MB |
| `batch-size` | int | 10000 | |
| `simple-progress` | boolean | | |

### generate fvec-extract

Extract a subset of float vectors by index array.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| Mixin: OutputFile | | | |
| Mixin: Range | | | |
| Mixin: BatchProcessing | | | |
| Mixin: CatalogConfig | | | |
| `ivec-file` | VectorDataSpec | required | shuffle/index file |
| `fvec-file` | VectorDataSpec | required | source vectors |
| `simple-progress` | boolean | | |

### generate mktestdata

Create a small test dataset from a larger one.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| Mixin: CatalogConfig | | | |
| `input` | VectorDataSpec | required | |
| `output-prefix` | String | required | |
| `queries` | int | required | number of query vectors |
| `seed` | long | 42 | |
| `force` | boolean | false | |
| `algorithm` | enum | | PRNG algorithm |

### compute knn

Compute exact k-nearest-neighbors ground truth.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| Mixin: BaseVectors | | | |
| Mixin: QueryVectors | | | |
| Mixin: Indices | | | |
| Mixin: Distances | | | |
| Mixin: DistanceMetric | | | |
| Mixin: Range | | | |
| Mixin: CatalogConfig | | | |
| `neighbors` | int | required | k |
| `cache-dir` | Path | .knn-cache | |
| `partition-size` | int | 1000000 | |
| `simd-strategy` | enum | AUTO | |

### compute sort

Sort vectors by a criterion.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| Mixin: InputFile | | | |
| Mixin: OutputFile | | | |
| Mixin: ParallelExecution | | | |
| Mixin: Range | | | |
| Mixin: Verbosity | | | |
| Mixin: ConsoleProgress | | | |

### convert file

Convert between vector file formats.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| Mixin: InputFile | | | |
| Mixin: OutputFile | | | |

### analyze verifyknn

Verify KNN ground truth accuracy.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| Mixin: Range | | | |
| Mixin: CatalogConfig | | | |
| `base` | VectorDataSpec | | optional |
| `query` | VectorDataSpec | | optional |
| `indices` | VectorDataSpec | | optional |
| `distances` | VectorDataSpec | | optional |
| `distance_function` | enum | | |
| `neighborhood_size` | int | -1 | max k, -1 = all |
| `buffer_limit` | int | -1 | |
| `status` | enum | Stdout | |
| `error_mode` | enum | fail | |
| `phi` | double | 0.001 | tolerance |
| `profile` | String | ALL | |
| params: | String[] | | dataset paths |

### analyze verifyprofiles

Verify consistency across dataset profiles.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| Mixin: Range | | | |
| `profiles` | String | | comma-separated |
| `distance_function` | enum | | |
| `neighborhood_size` | int | -1 | |
| `buffer_limit` | int | -1 | |
| `status` | enum | Stdout | |
| `error_mode` | enum | fail | |
| `phi` | double | 0.001 | |
| params: | String[] | required | dataset paths |

### analyze describe

Describe vector file structure and statistics.

### analyze slice

Extract and display a slice of vectors.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| Mixin: CatalogConfig | | | |
| `ordinal-range` | String | | |
| `component-range` | String | | |
| `format` | enum | TEXT | TEXT, CSV, TSV, JSON |
| `max-vectors` | int | 100 | |
| params: | VectorDataSpec | | vector source |

### analyze countzeros

Count zero vectors in a file.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| Mixin: CatalogConfig | | | |
| params: | VectorDataSpec[] | required | vector sources |

### analyze histogram

Generate histograms of vector component distributions.

### analyze stats

Compute summary statistics on vector data.

### analyze profile

Profile vector data characteristics.

### analyze compare

Compare two vector files.

### analyze find

Find specific vectors in a file.

### analyze select

Select and filter vectors.

### analyze plot

Plot vector data.

### analyze explore

Interactive exploration of vector data.

### analyze flamegraph

Generate flamegraph-style visualization.

### analyze modeldiff

Diff vector space models.

### analyze checkendian

Check endianness of vector files.

### datasets list

List available datasets in the catalog.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| `catalog` | String | | |
| `configdir` | Path | | |
| `at` | String | | |
| `verbose` | boolean | false | |
| `format` | enum | text | text, csv, json, yaml |

### datasets prebuffer

Pre-download and cache dataset files.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| Mixin: CatalogConfig | | | |
| `verbose` | boolean | false | |
| `progress` | boolean | true | |
| `views` | String | * | glob filter |
| params: | String[] | required | dataset names |

### datasets plan

Show the execution plan for materializing a dataset.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| params: | Path | . | directory |

### datasets curlify

Generate curl commands for downloading a dataset.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| Mixin: CatalogConfig | | | |
| `profile` | String | | comma-separated |
| `output` | String | | |
| params: | String | required | dataset name |

### datasets cache

Manage the dataset cache.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| Mixin: CatalogConfig | | | |

### fetch dlhf

Download datasets from HuggingFace.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| `target` | Path | ~/.cache/huggingface | |
| `envkey` | String | HF_TOKEN | |
| `token` | String | | |
| params: | String[] | | dataset names |

### merkle create

Create merkle tree for file integrity.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| `chunk-size` | int | 1048576 | |
| `force` | boolean | false | |
| `update` | boolean | false | |
| `match-extensions` | String | | |
| `dryrun` | boolean | false | |
| `no-tui` | boolean | | |
| `progress-interval` | int | 5 | seconds |
| params: | Path[] | | files |

### merkle verify

Verify file integrity against merkle tree.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| `chunk-size` | int | 1048576 | |
| `force` | boolean | false | |
| `dryrun` | boolean | false | |
| params: | Path[] | | files |

### merkle summary

Summarize merkle tree metadata.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| `chunk-size` | int | 1048576 | |
| `force` | boolean | false | |
| `dryrun` | boolean | false | |
| params: | Path[] | | files |

### merkle diff

Diff two merkle trees.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| `force` | boolean | false | |
| params: | Path, Path | | two files |

### merkle path

Show merkle path for a specific chunk.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| params: | Path, int | | file, chunk index |

### merkle treeview

Visualize merkle tree structure.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| `highlight` | String | | node, range, or list |
| `hash-length` | int | 16 | |
| `base` | int | 0 | |
| `levels` | int | 4 | |
| params: | Path | | file |

### merkle spoilbits

Corrupt random bits in a file for testing.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| `percentage` | double | 10.0 | |
| `start` | | | |
| `end` | | | |
| `seed` | | | |
| `dryrun` | boolean | false | |
| `force` | boolean | false | |
| params: | Path | | merkle file |

### merkle spoilchunks

Corrupt random chunks in a file for testing.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| `percentage` | double | 10.0 | |
| `start` | | | |
| `end` | | | |
| `seed` | | | |
| `dryrun` | boolean | false | |
| `bytes-to-corrupt` | String | 1 | |
| `force` | boolean | false | |
| params: | Path | | merkle file |

### cleanup cleanfvec

Clean malformed fvec files.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| params: | Path[] | . | input paths |

### info file

Inspect a vector file's structure.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| Mixin: CatalogConfig | | | |
| `input` | VectorDataSpec | required | |
| `sample` | int | 0 | |
| `hex` | boolean | false | |

### info compute

Show compute environment info (SIMD, cores, etc.).

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| `short` | boolean | false | |

### config init

Initialize vectordata configuration.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| `cache-dir` | String | auto | auto:largest-non-root, auto:largest-any, or path |
| `force` | boolean | false | |

### config show

Display current configuration.

(no options)

### config list-mounts

List filesystem mount points.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| `all` | boolean | false | |

### json jjq

Apply jq-style transformations to JSON files.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| `in` | Path | required | |
| `out` | String | stdout | |
| `threads` | int | 0 | |
| `parts` | int | 0 | |
| `diag` | boolean | false | |
| `filemode` | String | checkpoint | |
| `skipdone` | Path | | |
| params: | String | . | jq expression |

### slab import

Import data into a slab file.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| `from` | Path | required | |
| `force` | boolean | false | |
| `preferred-page-size` | int | 65536 | |
| `min-page-size` | int | 512 | |
| `page-alignment` | int | | |
| `max-page-size` | int | | |
| `start-ordinal` | int | -1 | |
| `format` | enum | | text, cstrings, slab, json, jsonl, csv, tsv, yaml |
| `append` | boolean | | |
| `namespace` | String | "" | |
| `progress` | boolean | | |
| params: | Path | | target slab file |

### slab export

Export data from a slab file.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| `to` | Path | | |
| `range` | OrdinalRange | | |
| `format` | enum | raw | raw, text, cstrings, json, jsonl, csv, tsv, yaml, hex, utf8, ascii |
| `skip-validation` | boolean | | |
| `force` | boolean | false | |
| `preferred-page-size` | int | 65536 | |
| `min-page-size` | int | 512 | |
| `page-alignment` | int | | |
| `max-page-size` | int | | |
| `as-hex` | boolean | | |
| `as-base64` | boolean | | |
| `namespace` | String | "" | |
| `progress` | boolean | | |
| params: | Path | | source slab file |

### slab append

Append data to an existing slab file.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| `from` | Path | required | |
| `preferred-page-size` | int | 65536 | |
| `min-page-size` | int | 512 | |
| `page-alignment` | int | | |
| `max-page-size` | int | | |
| `namespace` | String | "" | |
| `progress` | boolean | | |
| params: | Path | | target slab file |

### slab rewrite

Rewrite a slab file with new page parameters.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| `preferred-page-size` | int | 65536 | |
| `min-page-size` | int | 512 | |
| `page-alignment` | int | | |
| `max-page-size` | int | | |
| `force` | boolean | false | |
| `skip-check` | boolean | | |
| `namespace` | String | | |
| `progress` | boolean | | |
| params: | Path, Path | | source, dest |

### slab check

Validate slab file integrity.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| `verbose` | boolean | false | |
| params: | Path | | slab file |

### slab get

Get specific records from a slab file by ordinal.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| `ordinals` | String | required | comma-separated |
| `format` | enum | ascii | ascii, hex, raw, utf8, json, jsonl |
| `as-hex` | boolean | | |
| `as-base64` | boolean | | |
| `namespace` | String | "" | |
| params: | Path | | slab file |

### slab analyze

Analyze slab file structure and statistics.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| `verbose` | boolean | false | |
| `samples` | int | | |
| `sample-percent` | | | |
| `namespace` | String | "" | |
| params: | Path | | slab file |

### slab explain

Show detailed page/record layout of a slab file.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| `pages` | String | | comma-separated page indices |
| `ordinals` | OrdinalRange | | |
| `namespace` | String | "" | |
| params: | Path | | slab file |

### slab namespaces

List namespaces in a slab file.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| params: | Path | | slab file |

### catalog

Generate catalog entries for dataset directories.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| `basename` | String | | |
| params: | Path[] | | directories |

### version

Show version information.

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| `short` | boolean | false | |
| `json` | boolean | false | |
| `verbose` | boolean | false | |

---

## Design observations from the lexicon

### Commands most relevant to command streams

The commands that most commonly appear in dataset-producing pipelines:

- **generate ivec-shuffle** — create a permutation order (shared intermediate)
- **generate fvec-extract** / **generate ivec-extract** — extract subsets by index
- **generate vectors** — create synthetic data
- **generate dataset** — all-in-one synthetic dataset (already a mini-pipeline)
- **generate predicated** — filtered/predicated datasets
- **compute knn** — ground truth computation (expensive, benefits most from skip-if-fresh)
- **convert file** — format conversion (e.g. hvec → fvec)
- **analyze verifyknn** — validation step
- **merkle create** / **merkle verify** — integrity checking
- **slab import** — metadata ingestion

### Option patterns that inform the stream format

1. **File I/O options dominate**: nearly every command has some combination of input/output
   paths. The stream format's main job is wiring these together across steps.

2. **VectorDataSpec is already flexible**: it handles local files, catalog refs, remote URLs,
   and range suffixes. Steps should pass these through directly rather than inventing a new
   reference syntax.

3. **Threads/parallelism is per-command**: some commands use `--threads`, some use
   `--parallel`, some use `BatchProcessingOption`. A stream-level `threads` default should
   map to whichever option name the specific command uses. The `CommandOp` interface can
   handle this mapping.

4. **Force/overwrite is common**: many commands have `--force`. The stream runner should
   have a global `force` default that flows to all steps, since partial re-runs after failure
   commonly need to overwrite partial outputs.

5. **Commands already return structured exit codes**: 0/1/2 mapping to OK/WARNING/ERROR.
   The `CommandResult` enum mirrors this directly.

### Shared intermediates in practice

The shuffle-split pattern is the primary motivating case for shared intermediates:

```
source.fvec ──→ ivec-shuffle ──→ shuffle.ivec ──┬──→ fvec-extract[100k..1M] ──→ base.fvec
                                                 └──→ fvec-extract[0..100k]  ──→ query.fvec
```

Other shared-intermediate patterns:
- A converted file (hvec → fvec) used by both KNN and verification
- A statistical model file (`--model-out`) used by multiple `generate from-model` calls
- A slab file with multiple namespaces consumed by different facets

### What generate dataset already does

`generate dataset` is itself a mini-pipeline: it generates base vectors, query vectors,
computes KNN, and writes a dataset.yaml. The command stream generalizes this to arbitrary
multi-step pipelines that `generate dataset` can't express — different source data, custom
shuffle splits, format conversions, predicated metadata, etc.

## Open questions

- **Stream-level vs. step-level file resolution**: Should the runner resolve all
  `VectorDataSpec` references up front (enabling early validation), or lazily per-step
  (allowing one step to create a file that a later step references by catalog name)?
- **Idempotency**: Should the runner guarantee that re-running a stream with identical
  inputs produces identical outputs? This requires controlling PRNG seeds and sorting
  thread-pool outputs deterministically.
- **Partial profile materialization**: Should `nbvectors run-stream dataset.yaml --profile p1`
  materialize only one profile's facets and their transitive dependencies?
- **How much of CommandOp can be auto-derived**: PicoCLI already has introspection. Can
  `describeOptions()` and the argv→Map bridging be generated from the existing annotations
  rather than hand-coded per command?
