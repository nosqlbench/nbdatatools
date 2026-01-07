# Understanding Verification Results

This guide explains how to interpret the metrics and visualizations produced
during accuracy verification.

## Statistical Tests Explained

### Kolmogorov-Smirnov (K-S) Test

The K-S test measures the maximum vertical distance between two cumulative
distribution functions (CDFs).

```
      1.0 ┤                     ●●●●●●●●●
          │                 ●●●●
          │              ●●●
          │           ●●●    ← F₂(x) synthetic CDF
          │         ●●
          │       ●●
          │     ●●
      0.5 ┤   ●●
          │  ●●
          │ ●●  ○○○○○○○○○○○○
          │●●○○○○
          │○○○    ← F₁(x) original CDF
          │○
      0.0 ┼──────────────────────────────▶
                                         x
                    ↑
                K-S D = max |F₁(x) - F₂(x)|
```

**Interpretation:**

| K-S D Value | Meaning |
|-------------|---------|
| < 0.01 | Excellent match - distributions nearly identical |
| 0.01 - 0.02 | Good match - minor differences |
| 0.02 - 0.05 | Acceptable - some deviation but similar shape |
| > 0.05 | Poor match - investigate further |

**Critical Values (α = 0.05, two-sample):**

```
D_crit = 1.36 × √((n₁ + n₂) / (n₁ × n₂))
```

| n₁ | n₂ | D_crit |
|----|-----|--------|
| 10,000 | 10,000 | 0.0192 |
| 25,000 | 100,000 | 0.0098 |
| 100,000 | 100,000 | 0.0061 |

### Moment Comparison

Moments capture different aspects of a distribution:

```
                    ┌───────────────────────────────────────┐
                    │                                       │
    Kurtosis (γ₂)   │  ▲ High kurtosis                      │
    "Tailedness"    │ ╱ ╲  (heavy tails)                    │
                    │╱   ╲                                  │
                    │      ╲                    ╱╲          │
                    │       ╲                  ╱  ╲         │
                    │        ──────────────────    ▼ Low    │
                    │                               kurtosis │
                    └───────────────────────────────────────┘

                    ┌───────────────────────────────────────┐
                    │                                       │
    Skewness (γ₁)   │    ╱╲                                 │
    "Asymmetry"     │   ╱  ╲                                │
                    │  ╱    ╲╲                              │
                    │ ╱       ╲╲                            │
                    │╱          ╲──────                     │
                    │ ← negative    positive →              │
                    │   skew         skew                   │
                    └───────────────────────────────────────┘
```

**Error Tolerances:**

| Moment | Tolerance | Why |
|--------|-----------|-----|
| Mean (μ) | < 0.01σ | Central tendency is critical |
| Variance (σ²) | < 5% relative | Spread affects data range |
| Skewness (γ₁) | < 0.15 absolute | Asymmetry is subtle |
| Kurtosis (γ₂) | < 0.50 absolute | Tails are hard to estimate |

### Q-Q Correlation

Q-Q (Quantile-Quantile) plots compare sorted values from two samples.
The correlation coefficient measures how linear this relationship is.

```
    Synthetic
    Quantiles
         │                         ●
         │                      ●●
         │                   ●●●
         │                ●●●
         │             ●●●
         │          ●●●
         │       ●●●
         │    ●●●        Perfect match: r = 1.0
         │ ●●●           Points on diagonal
         ●──────────────────────────▶
                              Original Quantiles

    Synthetic
    Quantiles
         │                    ●●●
         │                 ●●●
         │              ●●●
         │           ●●●
         │        ●●●
         │     ●●●
         │  ●●●●●
         │●●●            Poor match: r = 0.95
         ●●              S-curve deviation
         ●──────────────────────────▶
                              Original Quantiles
```

**Interpretation:**

| Q-Q Correlation | Meaning |
|-----------------|---------|
| > 0.999 | Excellent - virtually identical |
| 0.995 - 0.999 | Good - minor tail differences |
| 0.990 - 0.995 | Acceptable - some shape difference |
| < 0.990 | Poor - significant deviation |

---

## Visual Indicators

### Sparkline Histograms

ASCII histograms provide quick visual comparison:

```
Original:   ▁▂▄▆███▆▄▂▁     Normal distribution
Synthetic:  ▁▂▄▆███▆▄▂▁     (symmetric bell curve)
            ↑ Bars should align vertically
```

**Common Patterns:**

```
Normal:     ▁▂▄▆███▆▄▂▁     Symmetric bell
Uniform:    █████████████    Flat across range
Beta:       ▂▅██████████▅▂  Bell with bounds
Right-skew: ██▇▅▃▂▁▁        Tail to right
Left-skew:  ▁▁▂▃▅▇██        Tail to left
```

### Status Indicators

| Symbol | Meaning |
|--------|---------|
| ✓ | Test passed, within tolerance |
| ⚠ | Marginal, at edge of tolerance |
| ✗ | Test failed, outside tolerance |

### Progress Bars

```
[██████████████████████████████] 100%  - Complete
[███████████████░░░░░░░░░░░░░░░]  50%  - In progress
[██████████░░░░░░░░░░░░░░░░░░░░]  33%  - Partial
```

---

## Reading the Summary Dashboard

```
╔═════════════════════════════════════════════════════════════════╗
║                    VERIFICATION SUMMARY                          ║
╠═════════════════════════════════════════════════════════════════╣
║                                                                  ║
║  ① K-S Tests Passed:     98/100 (98.0%)  ⚠                      ║
║  ② Mean Moment Error:    0.21%           ✓                      ║
║  ③ Q-Q Correlation:      0.9994          ✓                      ║
║  ④ Type Matches:         100/100         ✓                      ║
║  ⑤ Parameter Drift:      0.18%           ✓                      ║
║                                                                  ║
╚═════════════════════════════════════════════════════════════════╝
```

**Key Metrics:**

① **K-S Tests Passed** - Percentage of dimensions where synthetic matches original
  - 100% = Perfect
  - 95-99% = Acceptable (check failed dimensions)
  - < 95% = Investigate

② **Mean Moment Error** - Average statistical property deviation
  - < 0.5% = Excellent
  - 0.5-1% = Good
  - > 1% = Marginal

③ **Q-Q Correlation** - Quantile alignment across all dimensions
  - > 0.999 = Excellent
  - > 0.995 = Good
  - < 0.995 = Investigate

④ **Type Matches** - Distribution types preserved in round-trip
  - 100% = Required for full validation
  - < 100% = Indicates instability

⑤ **Parameter Drift** - Model parameter change after round-trip
  - < 0.5% = Excellent
  - 0.5-1% = Acceptable
  - > 1% = Marginal

---

## Understanding Failures

### K-S Test Failure

**Symptom:**
```
│  47 │ 0.0198  │ 0.023   │ ✗ FAIL │
```

**Common Causes:**

1. **Heavy tails** - Extreme values not captured by parametric model
   - Solution: Use `--model-type empirical` for that dimension

2. **Multimodal data** - Multiple peaks in distribution
   - Solution: Check if data is actually from mixed sources

3. **Insufficient samples** - Not enough data to estimate parameters
   - Solution: Increase `--sample` count

### High Parameter Drift

**Symptom:**
```
│  23 │ beta(2.14, 5.67)   │ beta(1.98, 5.12)   │ 7.4%  │ ✗     │
```

**Common Causes:**

1. **Sensitive parameters** - Some distributions have parameters that are hard to estimate
   - Beta with very different α and β values
   - Gamma with small shape parameter

2. **Boundary effects** - Data near theoretical bounds
   - Solution: Check min/max values in original data

### Type Mismatch

**Symptom:**
```
│  12 │ normal             │ beta               │ TYPE  │ ⚠     │
```

**Common Causes:**

1. **Similar distributions** - Normal and Beta can look similar for some parameters
   - This may not be a problem if the parameters give similar shapes

2. **Fit scores are close** - Two models had nearly equal fit
   - Solution: Use `--show-all-fits` to see alternatives

---

## Sample Size Effects

The precision of all metrics improves with more samples:

```
                    Precision vs Sample Size

        High  │
    Precision │  ●
              │    ●
              │      ●
              │        ●
              │          ●●●●●●●●●●●●
              │
        Low   │
              └────────────────────────────▶
                10K   50K  100K 500K  1M
                        Sample Size
```

**Recommendations:**

| Use Case | Minimum Samples | Recommended |
|----------|-----------------|-------------|
| Quick validation | 10,000 | 25,000 |
| Production testing | 50,000 | 100,000 |
| High-precision | 100,000 | 500,000+ |

---

## Independence Assumption

The current model treats dimensions independently. This section explains
when this matters and what to look for.

### When Independence Holds

```
Correlation Matrix (good - mostly zeros):
         D0    D1    D2    D3    D4
    D0  1.00  0.02 -0.01  0.03  0.01
    D1  0.02  1.00  0.01 -0.02  0.02
    D2 -0.01  0.01  1.00  0.01 -0.01
    D3  0.03 -0.02  0.01  1.00  0.02
    D4  0.01  0.02 -0.01  0.02  1.00
```

All off-diagonal values near zero → Independence assumption is valid.

### When Independence Fails

```
Correlation Matrix (problematic - significant correlations):
         D0    D1    D2    D3    D4
    D0  1.00  0.72 -0.01  0.03  0.01
    D1  0.72  1.00  0.68 -0.02  0.02
    D2 -0.01  0.68  1.00  0.01 -0.01
    D3  0.03 -0.02  0.01  1.00  0.82
    D4  0.01  0.02 -0.01  0.82  1.00
```

Significant off-diagonal values → Synthetic data will not preserve correlations.

**What to do:**
1. Report warns about correlation loss
2. Evaluate if correlations matter for your use case
3. Consider dimensionality reduction if correlations are critical

---

## Next Steps

- [Troubleshooting](./04_troubleshooting.md) - Solutions for common problems
- [Advanced Usage](./05_advanced.md) - Custom thresholds and options
