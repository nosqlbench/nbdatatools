# Verification Guide

This guide provides a complete, narrated walkthrough for verifying the numerical
accuracy of vector space model extraction and synthetic data generation.

## Contents

1. [**Quick Start**](./01_quick_start.md) - Run the full validation in one command
2. [**Step-by-Step Walkthrough**](./02_walkthrough.md) - Detailed narrated example
3. [**Understanding the Results**](./03_interpreting_results.md) - How to read the output
4. [**Troubleshooting**](./04_troubleshooting.md) - Common issues and solutions
5. [**Command Reference**](./05_command_reference.md) - Copy-paste ready commands
6. [**Model Fitting and Normalization**](./06_model_fitting.md) - How models are selected and normalization effects

## Overview

The verification process proves that:

1. **Model extraction** correctly captures statistical properties from vector data
2. **Synthetic generation** accurately reproduces those statistical properties
3. **Round-trip fidelity** demonstrates the pipeline works in both directions

```
Original Vectors → Extract Model → Generate Synthetic → Re-extract Model
                                                              ↓
                    Models should match ←─────────────────────┘
```

## Prerequisites

- `nbvectors` CLI tool installed and on PATH
- A vector dataset in `.fvec` format (or other supported format)
- At least 10,000 vectors recommended (100,000+ for production validation)

## Quick Verification

```bash
# One-command validation
nbvectors analyze profile --input vectors.fvec --output model.json --verify
```

For detailed step-by-step verification with full diagnostics, see the
[Step-by-Step Walkthrough](./02_walkthrough.md).
