# Command Reference: Copy-Paste Examples

This guide provides ready-to-use commands for all verification and analysis tools.
Just copy, paste, and modify the file paths for your data.

---

## Environment Check

### Check SIMD/CPU capabilities
```bash
nbvectors info compute
```

### Quick one-line summary
```bash
nbvectors info compute --short
```

### Check vector file metadata
```bash
nbvectors info file --input data.fvec
```

### Show sample vectors from file
```bash
nbvectors info file --input data.fvec --sample 5
```

---

## Full Verification Pipeline (Copy-Paste Ready)

### Option A: One-Command Verification
```bash
# Extract model with automatic round-trip verification
nbvectors analyze profile \
    -b data.fvec \
    -o model.json \
    --verify \
    --verbose
```

### Option B: Step-by-Step Verification

```bash
# Step 1: Extract model from data
nbvectors analyze profile \
    -b data.fvec \
    -o extracted_model.json

# Step 2: Generate synthetic vectors from model
nbvectors generate from-model \
    -m extracted_model.json \
    -o synthetic.fvec \
    -n 100000 \
    --format xvec

# Step 3: Extract model from synthetic (round-trip)
nbvectors analyze profile \
    -b synthetic.fvec \
    -o roundtrip_model.json

# Step 4: Compare models
nbvectors analyze model-diff \
    --original extracted_model.json \
    --compare roundtrip_model.json

# Step 5: Compare distributions statistically
nbvectors analyze compare \
    --original data.fvec \
    --synthetic synthetic.fvec
```

---

## Analysis Commands

### Model Comparison (model-diff)

Compare two model JSON files to check type matches and parameter drift:

```bash
# Basic comparison
nbvectors analyze model-diff \
    --original model1.json \
    --compare model2.json

# Verbose output with all dimensions
nbvectors analyze model-diff \
    --original model1.json \
    --compare model2.json \
    --verbose

# Check only specific dimensions
nbvectors analyze model-diff \
    --original model1.json \
    --compare model2.json \
    --dimensions 0,1,2,10,20
```

---

### Distribution Comparison (compare)

Compare two vector files using Kolmogorov-Smirnov tests:

```bash
# Basic comparison
nbvectors analyze compare \
    --original original.fvec \
    --synthetic synthetic.fvec

# Custom significance level
nbvectors analyze compare \
    --original original.fvec \
    --synthetic synthetic.fvec \
    --alpha 0.01

# Compare specific dimension only
nbvectors analyze compare \
    --original original.fvec \
    --synthetic synthetic.fvec \
    --dimension 23

# Limit sample size for faster comparison
nbvectors analyze compare \
    --original original.fvec \
    --synthetic synthetic.fvec \
    --sample 5000

# Show all dimensions (verbose)
nbvectors analyze compare \
    --original original.fvec \
    --synthetic synthetic.fvec \
    --verbose
```

---

### Dimension Statistics (stats)

Get detailed statistics for dimensions:

```bash
# Single dimension with full statistics
nbvectors analyze stats \
    --input data.fvec \
    --dimension 23

# All dimensions summary table
nbvectors analyze stats \
    --input data.fvec \
    --all-dimensions

# Sample subset for faster analysis
nbvectors analyze stats \
    --input data.fvec \
    --dimension 23 \
    --sample 10000
```

---

### Histogram Visualization (histogram)

Display ASCII histogram for a dimension:

```bash
# Default histogram (40 bins, 60 chars wide)
nbvectors analyze histogram \
    --input data.fvec \
    --dimension 23

# More bins for finer detail
nbvectors analyze histogram \
    --input data.fvec \
    --dimension 23 \
    --bins 80

# Wider bars
nbvectors analyze histogram \
    --input data.fvec \
    --dimension 23 \
    --width 100

# Vertical sparkline-style
nbvectors analyze histogram \
    --input data.fvec \
    --dimension 23 \
    --vertical

# Sample subset
nbvectors analyze histogram \
    --input data.fvec \
    --dimension 23 \
    --sample 5000
```

---

## Diagnostic Workflows

### Investigate a Failing Dimension

When a K-S test fails for dimension 23:

```bash
# 1. Get detailed statistics
nbvectors analyze stats --input data.fvec --dimension 23

# 2. Visualize the distribution
nbvectors analyze histogram --input data.fvec --dimension 23

# 3. Compare with synthetic
nbvectors analyze compare \
    --original data.fvec \
    --synthetic synthetic.fvec \
    --dimension 23 \
    --verbose
```

### Check All Dimension Statistics

```bash
# Get full statistics table for all dimensions
nbvectors analyze stats --input data.fvec --all-dimensions
```

### Quick Visual Inspection of Multiple Dimensions

```bash
# Loop through first 10 dimensions
for d in 0 1 2 3 4 5 6 7 8 9; do
    echo "=== Dimension $d ==="
    nbvectors analyze histogram --input data.fvec --dimension $d --vertical
done
```

---

## Generate Test Data

### Create a sketch dataset with known distributions
```bash
nbvectors generate sketch \
    -d 100 \
    -n 25000 \
    -o sketch_vectors.fvec \
    --format xvec \
    --model-out ground_truth_model.json \
    --seed 42
```

### Generate vectors from existing model
```bash
nbvectors generate from-model \
    -m model.json \
    -o generated.fvec \
    -n 100000 \
    --format xvec \
    --seed 42
```

---

## Model Extraction Options

### Basic extraction
```bash
nbvectors analyze profile -b data.fvec -o model.json
```

### Force specific dimensions to use empirical
```bash
nbvectors analyze profile \
    -b data.fvec \
    -o model.json \
    --empirical-dimensions 23,47,89
```

### Use bounded distributions only (for normalized embeddings)
```bash
nbvectors analyze profile \
    -b data.fvec \
    -o model.json \
    --model-type bounded
```

### Use full distribution set (including heavy-tailed)
```bash
nbvectors analyze profile \
    -b data.fvec \
    -o model.json \
    --model-type full
```

### Sample subset for faster extraction
```bash
nbvectors analyze profile \
    -b data.fvec \
    -o model.json \
    --sample 50000
```

---

## Quick Reference Table

| Task | Command |
|------|---------|
| Check compute capabilities | `nbvectors info compute` |
| Check file metadata | `nbvectors info file -i data.fvec` |
| Extract model | `nbvectors analyze profile -b data.fvec -o model.json` |
| Extract + verify | `nbvectors analyze profile -b data.fvec -o model.json --verify` |
| Compare models | `nbvectors analyze model-diff --original a.json --compare b.json` |
| K-S test distributions | `nbvectors analyze compare --original a.fvec --synthetic b.fvec` |
| Dimension statistics | `nbvectors analyze stats -i data.fvec -d 23` |
| All dimension stats | `nbvectors analyze stats -i data.fvec --all-dimensions` |
| Histogram | `nbvectors analyze histogram -i data.fvec -d 23` |
| Generate from model | `nbvectors generate from-model -m model.json -o out.fvec -n 100000` |
| Generate sketch | `nbvectors generate sketch -d 100 -n 25000 -o out.fvec` |

---

## Exit Codes

| Command | Exit 0 | Exit 1 | Exit 2 |
|---------|--------|--------|--------|
| `model-diff` | Models match | Models differ | Error |
| `compare` | All K-S pass | Some K-S fail | Error |
| `stats` | Success | Error | - |
| `histogram` | Success | Error | - |
| `info compute` | Success | - | - |
| `info file` | Success | Error | - |

Use exit codes in scripts:
```bash
if nbvectors analyze compare --original a.fvec --synthetic b.fvec; then
    echo "Distributions match!"
else
    echo "Distributions differ - check output"
fi
```

---

## Environment Variables

```bash
# Enable Panama Vector API (required for SIMD acceleration)
export JAVA_TOOL_OPTIONS="--add-modules jdk.incubator.vector"

# Increase heap for large datasets
export JAVA_TOOL_OPTIONS="--add-modules jdk.incubator.vector -Xmx8g"
```

---

## Next Steps

- [Quick Start](./01_quick_start.md) - Full verification tutorial
- [Walkthrough](./02_walkthrough.md) - Step-by-step example
- [Understanding Results](./03_interpreting_results.md) - Metric explanations
- [Troubleshooting](./04_troubleshooting.md) - Common issues and fixes
