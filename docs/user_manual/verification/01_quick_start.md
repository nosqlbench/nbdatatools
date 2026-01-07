# Quick Start: Verification in One Command

This guide shows how to quickly verify the accuracy of vector space model
extraction and generation.

## Prerequisites

- Java 25+ with Panama Vector API enabled
- `nbvectors` CLI tool
- A vector dataset (`.fvec`, `.bvec`, or `.ivec` format)

## No Dataset? Create One

Generate a reference dataset with known distribution properties:

```bash
nbvectors generate sketch \
    -d 100 -n 25000 \
    -o sketch_vectors.fvec \
    --format xvec \
    --model-out sketch_model.json
```

This creates `sketch_vectors.fvec` (the data) and `sketch_model.json` (ground truth).

## The One-Command Verification

```bash
nbvectors analyze profile \
    -b sketch_vectors.fvec \
    -o extracted_model.json \
    --verify \
    --verbose
```

This single command will:
1. Extract a model from your data
2. Generate synthetic data
3. Re-extract a model from synthetic data
4. Compare the models
5. Report pass/fail status

## Expected Output

```
═══════════════════════════════════════════════════════════════════
                    EXTRACTION + VERIFICATION
═══════════════════════════════════════════════════════════════════

Step 1/4: Extracting model from original data...
  [██████████████████████████████] 100%
  ✓ Model extracted (100 dimensions, 25000 vectors)

Step 2/4: Generating synthetic data (100000 vectors)...
  [██████████████████████████████] 100%
  ✓ Synthetic data generated

Step 3/4: Extracting model from synthetic data...
  [██████████████████████████████] 100%
  ✓ Round-trip model extracted

Step 4/4: Comparing models...
  ✓ Type matches:      100/100 (100.0%)
  ✓ Parameter drift:   0.14% avg (threshold: 1.0%)
  ✓ Max drift:         0.52% (threshold: 2.0%)

═══════════════════════════════════════════════════════════════════

╔═══════════════════════════════════════════════════════════════════╗
║                    ✓ VERIFICATION PASSED                          ║
║                                                                   ║
║  Model saved to: extracted_model.json                             ║
║  The extraction-generation pipeline is accurate.                  ║
╚═══════════════════════════════════════════════════════════════════╝
```

## What "PASSED" Means

| Property | Meaning |
|----------|---------|
| **Type matches = 100%** | All distribution types preserved |
| **Parameter drift < 1%** | Model parameters stable |
| **Max drift < 2%** | No dimension has significant deviation |

## What If It Fails?

```
╔═══════════════════════════════════════════════════════════════════╗
║                    ⚠ VERIFICATION WARNING                         ║
║                                                                   ║
║  Type matches:      98/100 (98.0%)                               ║
║  Failed dimensions: 23, 47                                        ║
║                                                                   ║
║  Model saved, but consider using --empirical-dimensions 23,47    ║
╚═══════════════════════════════════════════════════════════════════╝
```

See [Troubleshooting](./04_troubleshooting.md) for solutions.

## Common Options

### Specify model type
```bash
nbvectors analyze profile -b sketch_vectors.fvec -o model.json --model-type bounded
```

Options:
- `auto` (default) - Automatic selection
- `bounded` - Only Normal, Beta, Uniform (for bounded data like embeddings)
- `full` - Include heavy-tailed distributions
- `empirical` - Use empirical for all dimensions

### Use more samples for higher precision
```bash
nbvectors analyze profile -b sketch_vectors.fvec -o model.json --sample 500000 --verify
```

### Force deterministic results
```bash
nbvectors analyze profile -b sketch_vectors.fvec -o model.json --verify --seed 42
```

## Manual Step-by-Step Verification

If you prefer to run each step manually:

```bash
# Step 1: Extract model
nbvectors analyze profile -b sketch_vectors.fvec -o extracted_model.json

# Step 2: Generate synthetic vectors from model
nbvectors generate from-model -m extracted_model.json -o synthetic.fvec -n 100000 --format xvec

# Step 3: Extract model from synthetic
nbvectors analyze profile -b synthetic.fvec -o roundtrip_model.json

# Step 4: Compare models
nbvectors analyze model-diff --original extracted_model.json --compare roundtrip_model.json
```

## Next Steps

- [Step-by-Step Walkthrough](./02_walkthrough.md) - Detailed narrated example
- [Understanding Results](./03_interpreting_results.md) - Metric explanations
- [Troubleshooting](./04_troubleshooting.md) - Common issues and fixes
