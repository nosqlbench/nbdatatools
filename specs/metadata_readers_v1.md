# Metadata Reader Guidance v1

Consumers MUST read `dataset.yaml` and honor attributes:
- Treat `distance_function` as authoritative for distance calculations
- Respect licensing information before redistribution
- Surface `vendor`, `model`, `notes`, `url` in UIs or logs

When multiple profiles exist, readers SHOULD allow the user to select a profile or default to `default`.
