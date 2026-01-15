# Troubleshooting Verification Issues

This guide helps diagnose and resolve common issues encountered during
accuracy verification.

## Quick Diagnosis

```
Issue                          Likely Cause              Go To
────────────────────────────────────────────────────────────────────
K-S test fails for some dims   Heavy tails/outliers      Section 1
All distributions are empirical Model fitting failed      Section 2
High parameter drift           Sensitive parameters      Section 3
Type mismatch in round-trip    Close competing fits      Section 4
Very slow extraction           Large dataset/dimensions  Section 5
"Panama not enabled" error     Missing JVM option        Section 6
Composites not detected        L2 normalization effects  Section 8
```

---

## Section 1: K-S Test Failures

### Symptoms

```
┌─────┬─────────┬─────────┬────────┐
│ Dim │ K-S D   │ p-value │ Status │
├─────┼─────────┼─────────┼────────┤
│  23 │ 0.0312  │ 0.008   │ ✗ FAIL │  ← p < 0.05
│  47 │ 0.0287  │ 0.014   │ ✗ FAIL │
└─────┴─────────┴─────────┴────────┘
```

### Cause: Heavy-Tailed Data

Some dimensions may have extreme values that parametric distributions
(Normal, Beta, Uniform) cannot capture accurately.

**Diagnosis:**
```bash
# Check dimension statistics
nbvectors analyze stats --input data.fvec --dimension 23
```

Look for:
- Very high kurtosis (> 6)
- Large difference between mean and median
- Extreme min/max values

**Solution 1: Use empirical model for that dimension**
```bash
nbvectors analyze profile \
    --input sketch_vectors.fvec \
    --output extracted_model.json \
    --empirical-dimensions 23,47
```

**Solution 2: Increase sample size**
```bash
nbvectors analyze profile \
    --input data.fvec \
    --output model.json \
    --sample 500000
```

### Cause: Multimodal Distribution

The dimension may have multiple peaks that a unimodal distribution cannot fit.

**Diagnosis:**
```bash
# Visual inspection
nbvectors analyze histogram --input data.fvec --dimension 23
```

Look for multiple peaks in the histogram.

**Solution 1: Use empirical model for specific dimensions**
```bash
nbvectors analyze profile \
    --input data.fvec \
    --output model.json \
    --empirical-dimensions 23
```

**Solution 2: Let composite fitting handle it**

The system automatically tries composite (multimodal) fitting when parametric
fails. If composite fitting also fails, check if your data is L2 normalized -
normalization can smear multimodal structures. See [Section 8](#section-8-composite-distributions-not-detected)
and [Model Fitting and Normalization](./06_model_fitting.md) for details.

---

## Section 2: All Distributions Are Empirical

### Symptoms

```
Model Summary:
  Distribution types:
    empirical: 100 dimensions (100.0%)  ← All empirical
```

### Cause: Model Fitting Threshold Too Strict

The default K-S threshold may be too strict for your data.

**Solution: Relax fit threshold**
```bash
nbvectors analyze profile \
    --input data.fvec \
    --output model.json \
    --fit-threshold 0.03
```

### Cause: Data Not Following Standard Distributions

Your data may genuinely not fit standard parametric distributions.

**Diagnosis:**
```bash
# Show fit scores for all candidates
nbvectors analyze profile \
    --input data.fvec \
    --output model.json \
    --show-fit-table
```

If all parametric fits have K-S D > 0.05, empirical is appropriate.

**Note:** Empirical models are accurate but larger (store quantiles).

---

## Section 3: High Parameter Drift

### Symptoms

```
┌─────┬────────────────────┬────────────────────┬────────┬────────┐
│ Dim │ Original           │ Round-Trip         │ Drift  │ Status │
├─────┼────────────────────┼────────────────────┼────────┼────────┤
│  12 │ beta(0.87, 4.21)   │ beta(0.79, 3.89)   │ 8.2%   │ ✗      │
└─────┴────────────────────┴────────────────────┴────────┴────────┘
```

### Cause: Sensitive Parameter Combinations

Some distributions have parameters that are difficult to estimate accurately,
especially when:
- Beta with very asymmetric α and β (e.g., α < 1 or β < 1)
- Gamma with small shape parameter

**Solution 1: Use more samples**
```bash
# Generate more synthetic data for round-trip
nbvectors generate \
    --model model.json \
    --output synthetic.fvec \
    --count 500000  # Increase from default
```

**Solution 2: Use empirical for sensitive dimensions**
```bash
nbvectors analyze profile \
    --input data.fvec \
    --output model.json \
    --empirical-dimensions 12
```

### Cause: Boundary Effects

Data may be clipped or truncated, causing estimation issues.

**Diagnosis:**
```bash
# Check if data hits boundaries
nbvectors analyze stats --input data.fvec --dimension 12
```

Look for min/max values that are suspiciously round numbers.

---

## Section 4: Type Mismatch in Round-Trip

### Symptoms

```
Distribution Type Analysis:
  Type matches: 97/100 (97.0%) ⚠

Mismatched dimensions:
  Dim 34: normal → beta (fit score diff: 0.003)
  Dim 67: beta → normal (fit score diff: 0.002)
  Dim 89: uniform → beta (fit score diff: 0.004)
```

### Cause: Close Competing Fits

Multiple distributions may fit the data almost equally well.

**Diagnosis:**
```bash
# Show all candidate fits
nbvectors analyze profile \
    --input data.fvec \
    --show-all-fits \
    --dimension 34
```

```
Dimension 34 fit candidates:
  1. normal   K-S D=0.0089  score=0.9421
  2. beta     K-S D=0.0092  score=0.9418  ← Very close!
  3. uniform  K-S D=0.0234  score=0.8912
```

**Solution 1: Accept if shapes are similar**

If both distributions give similar shapes, this may not matter for your use case.
Compare the actual generated distributions:

```bash
nbvectors analyze compare \
    --original data.fvec \
    --synthetic synthetic.fvec \
    --dimension 34
```

**Solution 2: Force specific type**
```bash
nbvectors analyze profile \
    --input data.fvec \
    --output model.json \
    --force-type 34:normal
```

---

## Section 5: Performance Issues

### Symptoms

```
Profiling vector file: large_dataset.fvec
  Vectors: 10,000,000
  Dimensions: 1024
  [███░░░░░░░░░░░░░░░░░░░░░░░░░░░]  10%  ETA: 45 minutes
```

### Solutions

**Enable SIMD acceleration:**
```bash
java --add-modules jdk.incubator.vector --enable-native-access=ALL-UNNAMED -jar nbvectors.jar ...
```

**Use sampling:**
```bash
nbvectors analyze profile \
    --input large_dataset.fvec \
    --sample 100000  # Use subset
    --output model.json
```

**Use convergent extraction (stops early):**
```bash
nbvectors analyze profile \
    --input large_dataset.fvec \
    --convergent  # Stop when parameters stabilize
    --output model.json
```

**Increase parallelism:**
```bash
nbvectors analyze profile \
    --input large_dataset.fvec \
    --threads 16
    --output model.json
```

---

## Section 6: Panama Vector API Issues

### Symptoms

```
java.lang.IllegalStateException: Panama Vector API not enabled but should be available!

Your system supports SIMD acceleration:
  • Java version: 25 (Panama supported)
  • CPU SIMD capability: AVX-512F

To enable Panama Vector API, add this JVM option:
  --add-modules jdk.incubator.vector
```

### Solution

Add the required JVM option:

```bash
# Direct invocation
java --add-modules jdk.incubator.vector --enable-native-access=ALL-UNNAMED -jar nbvectors.jar analyze profile ...

# Or set environment variable
export JAVA_TOOL_OPTIONS="--add-modules jdk.incubator.vector --enable-native-access=ALL-UNNAMED"
nbvectors analyze profile ...
```

### For Maven Tests

In `pom.xml`:
```xml
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-surefire-plugin</artifactId>
  <configuration>
    <argLine>--add-modules jdk.incubator.vector --enable-native-access=ALL-UNNAMED</argLine>
  </configuration>
</plugin>
```

---

## Section 7: Memory Issues

### Symptoms

```
java.lang.OutOfMemoryError: Java heap space
```

### Solution: Increase heap size

```bash
java -Xmx8g --add-modules jdk.incubator.vector --enable-native-access=ALL-UNNAMED -jar nbvectors.jar ...
```

### Solution: Use streaming mode

For very large datasets:
```bash
nbvectors analyze profile \
    --input huge_dataset.fvec \
    --streaming  # Process without loading all into memory
    --output model.json
```

---

## Diagnostic Commands

### Check compute capabilities
```bash
nbvectors info compute
```

### Check file format
```bash
nbvectors info file --input data.fvec
```

### Detailed dimension statistics
```bash
nbvectors analyze stats --input data.fvec --all-dimensions
```

### Compare two models
```bash
nbvectors analyze model-diff --original model1.json --compare model2.json
```

---

## Section 8: Composite Distributions Not Detected

### Symptoms

```
Ground truth model:
  Composite: 40 dimensions (40.0%)
    2-mode: 15 dimensions
    3-mode: 15 dimensions
    4-mode: 10 dimensions

Extracted model:
  Composite: 4 dimensions (4.0%)     ← Far fewer than expected
  Empirical: 56 dimensions (56.0%)   ← Many fell back to empirical
```

### Cause: L2 Normalization Effects

When vectors are L2-normalized (unit length), the per-dimension distributions
become coupled and multimodal structures get distorted:

```
Before Normalization:              After Normalization:

 ▁▃▆█▆▃▁    ▁▃▆█▆▃▁               ▁▂▃▄▅▆▆▆▅▄▃▂▁
      ↑         ↑                        ↑
    Mode 1    Mode 2               Modes smeared together
```

**Why this happens:**
- L2 normalization divides each component by the vector's length
- The length depends on ALL dimensions, not just the current one
- This couples dimensions that were previously independent
- Mode separation gets "smeared out" as the normalizing factor varies

### Diagnosis

Check if your data is normalized:

```bash
nbvectors analyze profile -b data.fvec -o model.json

# Look for this in the output:
# Normalization detection: NORMALIZED (100/100 samples normalized, 100.0%)
```

### Solutions

**Solution 1: Accept that normalized data has limited composite detection**

This is expected behavior, not a bug. For normalized embeddings, the empirical
fallback models still produce accurate synthetic vectors. The round-trip
verification will still pass.

**Solution 2: For testing composite detection, use unnormalized data**

```bash
# Generate test data WITHOUT normalization
nbvectors generate sketch -d 100 -n 25000 \
  --mix=full \
  --no-normalize \
  -o test_vectors.fvec --format xvec

# Composite distributions will now be detected correctly
nbvectors analyze profile -b test_vectors.fvec -o model.json --verify
```

**Solution 3: Relax composite fitting thresholds**

For normalized data where you still want to try composite fitting:

```bash
nbvectors analyze profile -b data.fvec -o model.json \
  --ks-threshold-composite 0.15  # More permissive (default is 0.10)
```

### Understanding the Trade-offs

| Data Type | Composite Detection | Empirical Fallback | Round-Trip Accuracy |
|-----------|---------------------|--------------------|--------------------|
| Unnormalized | High | Low | High |
| L2 Normalized | Low | High | High (via empirical) |

The key insight: **empirical models are not a failure mode**. They accurately
capture any distribution shape and always round-trip correctly. For normalized
embeddings, having many empirical dimensions is normal and expected.

For detailed explanation of model fitting logic, see
[Model Fitting and Normalization](./06_model_fitting.md).

---

## Getting Help

If you encounter an issue not covered here:

1. Run with `--debug` flag to get detailed logging
2. Check the logs in `~/.nbvectors/logs/`
3. Report issues at: https://github.com/nosqlbench/nbdatatools/issues

Include:
- Command that failed
- Error message
- Dataset characteristics (dimensions, vector count, value ranges)
- Java version (`java -version`)
- CPU info (`cat /proc/cpuinfo | grep "model name" | head -1`)
