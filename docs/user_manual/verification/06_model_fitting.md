# Model Fitting and Normalization

This guide explains how the model extraction system selects distribution types
and the important considerations around vector normalization.

## Model Selection Pipeline

When extracting a model from vector data, each dimension goes through an
adaptive fitting pipeline that tries increasingly complex models until
a good fit is found.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         MODEL SELECTION PIPELINE                            │
│                                                                             │
│   Data ──► Parametric Fit ──► Composite Fit ──► Empirical Fallback         │
│                  │                  │                  │                    │
│                  ▼                  ▼                  ▼                    │
│            K-S < 0.03?        K-S < 0.10?         Always works              │
│                  │                  │                  │                    │
│              YES │ NO          YES │ NO               │                     │
│                  ▼                  ▼                  ▼                    │
│            ┌─────────┐       ┌───────────┐      ┌───────────┐              │
│            │ Accept  │       │  Accept   │      │  Accept   │              │
│            │Parametric│      │ Composite │      │ Empirical │              │
│            └─────────┘       └───────────┘      └───────────┘              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Step 1: Parametric Fitting

The system first attempts to fit parametric distributions in order of
simplicity:

| Distribution | Parameters | Best For |
|--------------|------------|----------|
| Uniform | lower, upper | Flat, bounded data |
| Normal | mean, std_dev | Symmetric bell curves |
| Beta | alpha, beta, bounds | Bounded with single mode |
| Gamma | shape, scale | Positive, right-skewed |
| Student-t | degrees of freedom | Heavy-tailed symmetric |
| Inverse Gamma | shape, scale | Positive, heavy right tail |
| Beta Prime | alpha, beta | Positive, various shapes |

**Selection criteria:**
- Kolmogorov-Smirnov (K-S) statistic measures fit quality
- Lower K-S = better fit (< 0.03 is excellent)
- When multiple distributions fit well, the simplest is preferred
- A "relative simplicity threshold" favors simpler models unless a complex
  model fits significantly better

### Step 2: Composite (Multimodal) Fitting

If parametric fitting fails (K-S > threshold), and the data shows multiple
peaks, composite fitting is attempted:

```
Multimodal Detection:

   ▁▃▆█▆▃▁    ▁▃▆█▆▃▁           Two peaks detected
        ↑         ↑              → Try 2-component composite
      Mode 1    Mode 2

   ▁▂▄█▄▂▁  ▁▂▅█▅▂▁  ▁▃▆█▆▃▁   Three peaks detected
       ↑        ↑        ↑      → Try 3-component composite
     Mode 1   Mode 2   Mode 3
```

**Composite fitting process:**

1. **Mode Detection** - Histogram analysis finds peaks using:
   - Gaussian kernel smoothing to reduce noise
   - Local maxima identification
   - Peak prominence filtering (secondary peaks must be ≥20% of primary)
   - Optional Hartigan's dip test for multimodality confirmation

2. **EM Clustering** - Expectation-Maximization assigns data points to modes

3. **Per-Mode Fitting** - Each cluster is fit with a parametric distribution

4. **Weight Estimation** - Mixture weights are derived from cluster sizes

5. **CDF Validation** - The composite model's CDF is compared to empirical CDF
   - Maximum deviation must be < 0.10 (10%)
   - If validation fails, more modes are tried (up to 10)

**Maximum modes:** The system tries 2-10 component composites (default: up to 10) before
falling back to empirical.

### Step 3: Empirical Fallback

When parametric and composite fitting both fail, an empirical (histogram-based)
model is used:

```
Empirical Model:
┌─────────────────────────────────────────────────────────────────┐
│  Stores quantile values at fixed percentiles:                   │
│                                                                 │
│  P:     0%   10%   20%   30%   40%   50%   60%   70%   80%   90%  100% │
│  V:  -0.95 -0.62 -0.38 -0.18  0.02  0.21  0.41  0.58  0.74  0.89  0.98 │
│                                                                 │
│  Generation uses linear interpolation between quantiles        │
└─────────────────────────────────────────────────────────────────┘
```

**When empirical is used:**
- Highly irregular distributions
- Multiple overlapping modes that can't be cleanly separated
- Data with unusual features (gaps, spikes, plateaus)

**Trade-offs:**
- Always produces perfect round-trip accuracy
- Larger model size (stores histogram data)
- Less interpretable than parametric models

---

## Vector Normalization Effects

### The Independence Problem

Vector normalization (L2 normalization) creates a fundamental challenge for
per-dimension distribution modeling.

**Before normalization:**
```
Each dimension is independent:
  d₀ ~ Distribution_0(params)
  d₁ ~ Distribution_1(params)
  d₂ ~ Distribution_2(params)
  ...

Vector: [d₀, d₁, d₂, ...]
```

**After L2 normalization:**
```
All dimensions become coupled:
  d'ᵢ = dᵢ / ‖v‖₂

where ‖v‖₂ = √(d₀² + d₁² + d₂² + ...)

The value of each dimension now depends on ALL other dimensions!
```

### Impact on Multimodal Distributions

This coupling is particularly destructive for multimodal (composite)
distributions:

```
Before Normalization:              After Normalization:

 ▁▃▆█▆▃▁    ▁▃▆█▆▃▁               ▁▂▃▄▅▆▆▆▅▄▃▂▁
      ↑         ↑                        ↑
    Mode 1    Mode 2               Modes smeared together

Clear bimodal structure            Appears unimodal
K-S confirms multimodality         K-S test fails
→ Composite model selected         → Parametric model selected
```

**Why this happens:**

1. When dimension d has value near Mode 1, the L2 norm has one typical value
2. When dimension d has value near Mode 2, the L2 norm has a different value
3. After dividing by these different norms, the modes shift and overlap
4. The clear separation between modes gets "smeared out"

### Practical Recommendations

**For testing multimodal detection:**
```bash
# Generate test data WITHOUT normalization
nbvectors generate sketch -d 100 -n 25000 \
  --mix=full --no-normalize \
  -o test_vectors.fvec --format xvec

# Analyze - composite distributions will be detected correctly
nbvectors analyze profile -b test_vectors.fvec -o model.json --verify
```

**For real normalized embeddings:**
```bash
# Normalized data will have limited multimodal detection
# This is expected behavior, not a bug
nbvectors analyze profile -b embeddings.fvec -o model.json --verify

# If many dimensions fall back to empirical, that's normal for normalized data
# The empirical models still produce accurate synthetic vectors
```

**Understanding the results:**

| Data Type | Expected Composite Detection | Expected Empirical Fallback |
|-----------|------------------------------|----------------------------|
| Unnormalized | High (if data is multimodal) | Low |
| L2 Normalized | Low (modes are smeared) | Moderate to High |
| Real embeddings | Very Low | Varies by model |

### When Normalization Matters

**Normalization DOES affect:**
- Multimodal detection accuracy
- Composite model fitting success rate
- Per-dimension independence assumption validity

**Normalization does NOT affect:**
- Unimodal parametric fitting (Normal, Beta, etc.)
- Overall synthetic data quality (empirical fallback works)
- Round-trip verification accuracy

---

## Convergence and Early Stopping

The streaming extractor monitors statistical convergence to know when
enough data has been processed.

### Four-Moment Convergence

For each dimension, these moments are tracked:

```
┌─────────────┬─────────────────────────────────────────────────────────┐
│ Moment      │ What It Measures                                        │
├─────────────┼─────────────────────────────────────────────────────────┤
│ Mean (μ)    │ Central tendency - where is the "center"?              │
│ Variance    │ Spread - how wide is the distribution?                 │
│ Skewness    │ Asymmetry - which way does it lean?                    │
│ Kurtosis    │ Tail weight - how extreme are outliers?                │
└─────────────┴─────────────────────────────────────────────────────────┘
```

### Convergence Tracking

```
Samples:    1K      5K      10K     25K     50K     100K
            │       │       │       │       │       │
Mean:       ████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ Converged early
Variance:   ░░░░░░░░████░░░░░░░░░░░░░░░░░░░░░░░░░░░ Converged at 10K
Skewness:   ░░░░░░░░░░░░░░░░████░░░░░░░░░░░░░░░░░░░ Converged at 25K
Kurtosis:   ░░░░░░░░░░░░░░░░░░░░░░░░████░░░░░░░░░░░ Converged at 50K
            ↑                           ↑
            Mean stabilizes             All 4 moments stable
            quickly                     → Ready for fitting
```

**Convergence threshold:** Default is 0.5% relative change between checkpoints.
When all four moments for all dimensions change by less than this threshold,
the data collection phase ends and fitting begins.

### Early Stopping

For large datasets, convergence often occurs before all vectors are processed:

```
Dataset: 10,000,000 vectors
Convergence: Achieved at 250,000 vectors (2.5%)
Savings: 97.5% of I/O avoided!

╔═══════════════════════════════════════════════════════════════════╗
║  Data collection stopped (convergence): 250,000 of 10,000,000    ║
║  samples (2.5%)                                                   ║
╚═══════════════════════════════════════════════════════════════════╝
```

---

## Fitting Strategy Summary

The complete decision tree for model selection:

```
                           Start
                             │
                             ▼
                    ┌────────────────┐
                    │ Compute stats  │
                    │ (mean, var,    │
                    │  skew, kurt)   │
                    └───────┬────────┘
                            │
                            ▼
                    ┌────────────────┐
                    │ Try parametric │
                    │ distributions  │
                    └───────┬────────┘
                            │
                   ┌────────┴────────┐
                   │                 │
              K-S ≤ 0.03        K-S > 0.03
                   │                 │
                   ▼                 ▼
            ┌───────────┐    ┌────────────────┐
            │  Accept   │    │ Check for      │
            │parametric │    │ multimodality  │
            └───────────┘    └───────┬────────┘
                                     │
                            ┌────────┴────────┐
                            │                 │
                     Multiple peaks      Single peak
                            │                 │
                            ▼                 │
                    ┌────────────────┐        │
                    │ Try composite  │        │
                    │ (2-10 modes)   │        │
                    └───────┬────────┘        │
                            │                 │
                   ┌────────┴────────┐        │
                   │                 │        │
              K-S ≤ 0.10        K-S > 0.10    │
                   │                 │        │
                   ▼                 ▼        ▼
            ┌───────────┐    ┌────────────────┐
            │  Accept   │    │    Use         │
            │ composite │    │  empirical     │
            └───────────┘    └────────────────┘
```

---

## Tuning Model Fitting

### Available Options

```bash
nbvectors analyze profile -b data.fvec -o model.json \
  --ks-threshold-parametric 0.03 \  # Strictness for parametric
  --ks-threshold-composite 0.10 \   # Strictness for composite
  --max-composite-components 10 \   # Max modes to try (default: 10)
  --convergence-threshold 0.005 \   # 0.5% relative change
  --empirical-dimensions 12,45,67   # Force empirical for specific dims
```

### When to Adjust

| Scenario | Adjustment |
|----------|------------|
| Too many empirical | Relax `--ks-threshold-parametric` (e.g., 0.05) |
| Missing composites | Relax `--ks-threshold-composite` (e.g., 0.15) |
| Slow fitting | Reduce `--max-composite-components` to 2 or 3 |
| Noisy data | Increase `--convergence-threshold` (e.g., 0.01) |
| Known problem dims | Use `--empirical-dimensions` to skip fitting |

---

## Next Steps

- [Troubleshooting](./04_troubleshooting.md) - Solutions for common problems
- [Command Reference](./05_command_reference.md) - All available options
