# Step-by-Step Verification Walkthrough

This guide walks through a complete verification of the vector space model pipeline.
Each step includes the exact commands, expected output, and what to look for.

## File Naming Convention

Throughout this walkthrough, we use consistent file names:

| File | Description |
|------|-------------|
| `sketch_vectors.fvec` | Reference dataset (from sketch or your data) |
| `sketch_model.json` | Ground truth model (only if using sketch) |
| `extracted_model.json` | Model extracted from sketch_vectors |
| `synthetic_vectors.fvec` | Vectors generated from extracted_model |
| `roundtrip_model.json` | Model extracted from synthetic_vectors |

---

## Step 0: Create a Reference Dataset

You have two options:

### Option A: Generate a Sketch Dataset (Recommended for Learning)

This creates a dataset with known distribution properties, plus a "ground truth" model.

```bash
nbvectors generate sketch \
    -d 100 \
    -n 25000 \
    -o sketch_vectors.fvec \
    --format xvec \
    --mix bounded \
    --model-out sketch_model.json \
    --seed 42 \
    --verbose
```

**Expected Output:**

```
╔═══════════════════════════════════════════════════════════════════╗
║                    SKETCH DATASET GENERATED                       ║
╠═══════════════════════════════════════════════════════════════════╣
║  Output file: sketch_vectors.fvec                                 ║
║  Vectors:          25,000                                         ║
║  Dimensions:          100                                         ║
║  Distribution mix: BOUNDED                                        ║
╠═══════════════════════════════════════════════════════════════════╣
║  Ground-truth model: sketch_model.json                            ║
╚═══════════════════════════════════════════════════════════════════╝

Distribution Summary:
  Normal (truncated): 50 dimensions (50.0%)
  Beta:               30 dimensions (30.0%)
  Uniform:            20 dimensions (20.0%)
```

**Files created:**
- `sketch_vectors.fvec` - The reference vectors
- `sketch_model.json` - The known "ground truth" model

### Option B: Use Your Own Dataset

If you have existing vectors, copy or rename them:

```bash
cp your_data.fvec sketch_vectors.fvec
```

(You won't have a ground truth model, so skip the Bonus section at the end.)

---

## Step 1: Extract Model from Reference Data

**Goal:** Capture the statistical properties of each dimension.

```bash
nbvectors analyze profile \
    -b sketch_vectors.fvec \
    -o extracted_model.json \
    --verbose
```

**Expected Output:**

```
═══════════════════════════════════════════════════════════════════
                    VECTOR SPACE MODEL EXTRACTION
═══════════════════════════════════════════════════════════════════

Profiling vector file: sketch_vectors.fvec
  Vectors in file: 25000
  Dimensions: 100
  [██████████████████████████████] 100% (25000/25000 vectors)

Model Summary:
  Dimensions: 100
  Distribution types:
    normal:   50 dimensions (50.0%)
    beta:     30 dimensions (30.0%)
    uniform:  20 dimensions (20.0%)

VectorSpaceModel config saved to: extracted_model.json
```

**What to verify:**
- All dimensions processed successfully
- Distribution types match what you expect
- Model saved to output file

**File created:** `extracted_model.json`

---

## Step 2: Generate Synthetic Vectors

**Goal:** Create new vectors using the extracted model.

```bash
nbvectors generate from-model \
    -m extracted_model.json \
    -o synthetic_vectors.fvec \
    -n 100000 \
    --format xvec \
    --seed 42 \
    --verbose
```

**Expected Output:**

```
Loading model from: extracted_model.json
  Dimensions: 100
  Target vectors: 100000
  Seed: 42
Generating vectors...
  [ 10%] Generated 10,000 of 100,000 vectors
  [ 20%] Generated 20,000 of 100,000 vectors
  ...

Generation complete:
  Output file: synthetic_vectors.fvec
  Vectors: 100,000
  Dimensions: 100
  Time: 1,247 ms
  Throughput: 80,192 vectors/sec
```

**What to verify:**
- Correct number of vectors generated (100,000)
- Dimensions match model (100)
- File size is expected: 100,000 × 100 × 4 bytes = 40 MB

**File created:** `synthetic_vectors.fvec`

---

## Step 3: Compare Reference vs Synthetic Distributions

**Goal:** Verify synthetic data matches the reference statistically.

```bash
nbvectors analyze compare \
    --original sketch_vectors.fvec \
    --synthetic synthetic_vectors.fvec \
    --verbose
```

**Expected Output:**

```
═══════════════════════════════════════════════════════════════════
                    DISTRIBUTION COMPARISON
═══════════════════════════════════════════════════════════════════

Datasets:
  Original:  sketch_vectors.fvec (25,000 vectors)
  Synthetic: synthetic_vectors.fvec (100,000 vectors)

Two-Sample Kolmogorov-Smirnov Tests:
┌─────┬─────────┬─────────┬────────┐
│ Dim │ K-S D   │ p-value │ Status │
├─────┼─────────┼─────────┼────────┤
│   0 │ 0.0089  │ 0.847   │ ✓ PASS │
│   1 │ 0.0102  │ 0.721   │ ✓ PASS │
│   2 │ 0.0078  │ 0.912   │ ✓ PASS │
│ ... │ ...     │ ...     │ ...    │
└─────┴─────────┴─────────┴────────┘

Dimensions Passing: 100/100 (100.0%)

Overall Status: ✓ DISTRIBUTIONS MATCH
```

**What to verify:**
- All K-S tests pass (p-value > 0.05)
- All dimensions passing

---

## Step 4: Extract Model from Synthetic (Round-Trip)

**Goal:** Prove the generation process is reversible.

```bash
nbvectors analyze profile \
    -b synthetic_vectors.fvec \
    -o roundtrip_model.json \
    --verbose
```

**Expected Output:**

```
═══════════════════════════════════════════════════════════════════
                    VECTOR SPACE MODEL EXTRACTION
═══════════════════════════════════════════════════════════════════

Profiling vector file: synthetic_vectors.fvec
  Vectors in file: 100000
  Dimensions: 100
  [██████████████████████████████] 100%

Model Summary:
  Dimensions: 100
  Distribution types:
    normal:   50 dimensions (50.0%)
    beta:     30 dimensions (30.0%)
    uniform:  20 dimensions (20.0%)

VectorSpaceModel config saved to: roundtrip_model.json
```

**What to verify:**
- Same distribution types as extracted_model.json
- Parameters should be very close to extracted_model.json

**File created:** `roundtrip_model.json`

---

## Step 5: Compare Extracted vs Round-Trip Models

**Goal:** Prove the extract-generate-extract pipeline preserves parameters.

```bash
nbvectors analyze model-diff \
    --original extracted_model.json \
    --compare roundtrip_model.json \
    --verbose
```

**Expected Output:**

```
═══════════════════════════════════════════════════════════════════
                    MODEL COMPARISON
═══════════════════════════════════════════════════════════════════

Models:
  Original:   extracted_model.json
  Comparison: roundtrip_model.json

Distribution Type Analysis:
  Type matches: 100/100 (100.0%) ✓

Parameter Comparison:
┌─────┬────────────────────────┬────────────────────────┬────────┐
│ Dim │ Extracted              │ Round-Trip             │ Drift  │
├─────┼────────────────────────┼────────────────────────┼────────┤
│   0 │ normal(0.0234, 0.312)  │ normal(0.0232, 0.311)  │ 0.32%  │
│   1 │ normal(-0.089, 0.287)  │ normal(-0.088, 0.286)  │ 0.41%  │
│   2 │ beta(2.34, 1.89)       │ beta(2.32, 1.87)       │ 0.89%  │
│ ... │ ...                    │ ...                    │ ...    │
└─────┴────────────────────────┴────────────────────────┴────────┘

Drift Statistics:
  Mean parameter drift:  0.42%
  Max parameter drift:   1.21%

╔═══════════════════════════════════════════════════════════════════╗
║                    ✓ VALIDATION PASSED                            ║
║                                                                   ║
║  Type matches:         100/100 (100.0%)                          ║
║  Mean parameter drift: 0.42% (< 1.0% threshold)                  ║
║  Max parameter drift:  1.21% (< 2.0% threshold)                  ║
╚═══════════════════════════════════════════════════════════════════╝
```

**What to verify:**
- 100% type match
- Mean drift < 1%
- Max drift < 2%

---

## Bonus: Compare to Ground Truth (Sketch Only)

If you generated a sketch dataset in Step 0, compare your extracted model to the
known ground truth:

```bash
nbvectors analyze model-diff \
    --original sketch_model.json \
    --compare extracted_model.json \
    --verbose
```

**Expected Output:**

```
═══════════════════════════════════════════════════════════════════
                    GROUND TRUTH COMPARISON
═══════════════════════════════════════════════════════════════════

Models:
  Ground Truth: sketch_model.json
  Extracted:    extracted_model.json

Distribution Type Analysis:
  Type matches: 100/100 (100.0%) ✓

Drift Statistics:
  Mean parameter drift:  0.38%
  Max parameter drift:   1.15%

╔═══════════════════════════════════════════════════════════════════╗
║                    ✓ GROUND TRUTH MATCH                           ║
║                                                                   ║
║  The extracted model matches the known ground truth.             ║
║  This proves extraction correctly recovers parameters.           ║
╚═══════════════════════════════════════════════════════════════════╝
```

This is the strongest validation: proving extraction recovers the exact parameters
used to generate the data.

---

## Summary

After completing all steps, you have proven:

| Step | What It Proves |
|------|----------------|
| **Step 1** | Extraction fits distributions accurately |
| **Step 3** | Synthetic data matches original statistically |
| **Step 5** | Round-trip preserves parameters (drift < 1%) |
| **Bonus** | Extraction recovers known ground truth |

### Files Created

```
sketch_vectors.fvec      ← Reference data (Step 0)
sketch_model.json        ← Ground truth (Step 0, sketch only)
extracted_model.json     ← Extracted from reference (Step 1)
synthetic_vectors.fvec   ← Generated from extracted (Step 2)
roundtrip_model.json     ← Extracted from synthetic (Step 4)
```

### The Pipeline

```
sketch_vectors.fvec
        │
        ▼ (Step 1: analyze profile -b ... -o ...)
extracted_model.json
        │
        ▼ (Step 2: generate from-model -m ... -o ...)
synthetic_vectors.fvec
        │
        ▼ (Step 4: analyze profile -b ... -o ...)
roundtrip_model.json
        │
        ▼ (Step 5: analyze model-diff)
extracted_model.json ≈ roundtrip_model.json  ✓
```

---

## Next Steps

- [Understanding the Results](./03_interpreting_results.md) - Deep dive into metrics
- [Troubleshooting](./04_troubleshooting.md) - What to do when validation fails
