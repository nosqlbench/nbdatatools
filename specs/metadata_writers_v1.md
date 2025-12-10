# Metadata Specification v1

Writers MUST populate the `attributes` block in `dataset.yaml` with:
- `distance_function` – required
- `license` – required for distribution
- `vendor`, `model`, `url`, `notes` – optional but recommended
- Any custom key/value pairs relevant to downstream consumers

Profiles MAY include profile-specific metadata under `profiles.<name>.metadata`.
