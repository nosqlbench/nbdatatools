# Cache Directory Configuration Design

## Overview

This document describes the design for vectordata's cache directory configuration system. The cache directory is where vectordata stores downloaded datasets for local access.

## Goals

1. **Explicit configuration required** - Users must consciously choose where cached data is stored
2. **Conflict prevention** - Prevent accidental overwrites of existing configuration
3. **Flexible storage options** - Support automatic selection of optimal storage locations
4. **Programmatic access** - Allow APIs to configure cache directory with appropriate safeguards

## Configuration File

### Location

```
~/.config/vectordata/settings.yaml
```

### Schema

```yaml
# Required: Cache directory configuration
cache_dir: <value>

# Optional: Override protection (default: true)
protect_settings: true
```

### Valid `cache_dir` Values

| Value | Description |
|-------|-------------|
| `auto:largest-non-root` | **One-time resolution**: Select the largest writable mount point that is NOT the root filesystem, then persist the resolved path |
| `auto:largest-any` | **One-time resolution**: Select the largest writable mount point (including root), then persist the resolved path |
| `default` | **One-time resolution**: Resolve to `~/.cache/vectordata`, then persist the absolute path |
| `<absolute-path>` | Use a specific absolute path (e.g., `/data/vectordata-cache`) |

**Important:** The `auto:*` and `default` values are **one-time resolution directives**. When the system encounters these values, it resolves them to an actual path and **updates settings.yaml** with the resolved absolute path. This ensures the cache location remains stable even if:
- Mount points change
- New drives are added
- The system is rebooted with different storage configurations

After resolution, the settings.yaml will contain the concrete path, not the auto directive.

### Example Configurations

**Initial configuration (before first use):**
```yaml
# Option 1: Automatic selection (recommended for systems with multiple drives)
cache_dir: auto:largest-non-root

# Option 2: Explicit path (no resolution needed)
cache_dir: /mnt/data/vectordata-cache

# Option 3: Default user cache location
cache_dir: default
```

**After resolution (settings.yaml is automatically updated):**
```yaml
# Option 1 resolved - was "auto:largest-non-root"
cache_dir: /mnt/nvme0/vectordata-cache

# Option 2 unchanged - already an absolute path
cache_dir: /mnt/data/vectordata-cache

# Option 3 resolved - was "default"
cache_dir: /home/user/.cache/vectordata
```

## Behavior Specification

### Resolution and Persistence (Critical Design Decision)

**The `auto:*` and `default` values are one-time resolution directives, not ongoing auto-selection modes.**

When vectordata loads settings and encounters a resolvable value:

1. The value is resolved to an absolute path
2. The settings.yaml file is **immediately updated** to contain the resolved path
3. All future operations use the persisted absolute path

This means:
- **Before first use:** `cache_dir: auto:largest-non-root`
- **After first use:** `cache_dir: /mnt/nvme0/vectordata-cache`

**Why persist immediately?**

| Scenario | Without Persistence | With Persistence |
|----------|---------------------|------------------|
| New drive added | Cache might move to new drive, fragmenting data | Cache stays in original location |
| Drive removed | Error or data loss | Clear error: configured path unavailable |
| Mount order changes | Unpredictable cache location | Stable, predictable location |
| Multiple users/services | Each might resolve differently | All use the same persisted location |

**To change the cache location after initial setup**, users must either:
1. Edit settings.yaml manually
2. Use `nbvectors config init --force` to re-initialize
3. Use `nbvectors config migrate` (proposed) to migrate with data

### First-Time Setup (No Settings File)

When `~/.config/vectordata/settings.yaml` does not exist:

1. **CLI usage**: Display an error with setup instructions:
   ```
   Error: Cache directory not configured.

   vectordata requires explicit cache directory configuration before first use.

   Please create ~/.config/vectordata/settings.yaml with one of:

     # Automatic: Use largest non-root mount point
     cache_dir: auto:largest-non-root

     # Automatic: Use largest mount point (may be root)
     cache_dir: auto:largest-any

     # Default: Use ~/.cache/vectordata
     cache_dir: default

     # Explicit: Specify a path
     cache_dir: /path/to/cache

   Or run: nbvectors config init
   ```

2. **Programmatic usage**: Throw `CacheDirectoryNotConfiguredException` with the same guidance

### Reading Configuration

```java
public class VectorDataSettings {

    /// Load settings from the standard location.
    /// If cache_dir contains a resolvable value (auto:* or default),
    /// it will be resolved and the settings file will be updated with
    /// the resolved absolute path.
    ///
    /// @throws CacheDirectoryNotConfiguredException if settings.yaml does not exist
    /// @throws InvalidSettingsException if settings.yaml is malformed or cache_dir is invalid
    /// @throws NoSuitableMountPointException if auto:* cannot find a suitable mount
    public static VectorDataSettings load() { ... }

    /// Load settings from a custom location.
    /// Same resolution and persistence behavior as load().
    public static VectorDataSettings load(Path settingsFile) { ... }

    /// Get the resolved cache directory path.
    /// After load(), this always returns an absolute path (never auto:* or default).
    public Path getCacheDirectory() { ... }

    /// Check if the settings file was updated during load (due to resolution).
    public boolean wasResolved() { ... }

    /// Get the original value before resolution (for logging/diagnostics).
    /// Returns empty if no resolution occurred.
    public Optional<String> getOriginalCacheDirValue() { ... }
}
```

### Programmatic Configuration

#### Setting Cache Directory

```java
public class VectorDataSettings {

    /// Set the cache directory programmatically.
    ///
    /// @param cacheDirValue The cache_dir value (path, "default", or "auto:*")
    /// @throws SettingsAlreadyConfiguredException if cache_dir is already set
    ///         and protect_settings is true (default)
    /// @throws InvalidCacheDirectoryException if the value is invalid
    public static void setCacheDirectory(String cacheDirValue) { ... }

    /// Set the cache directory with force override.
    ///
    /// @param cacheDirValue The cache_dir value
    /// @param force If true, overwrite existing configuration
    /// @throws InvalidCacheDirectoryException if the value is invalid
    public static void setCacheDirectory(String cacheDirValue, boolean force) { ... }
}
```

#### Error on Conflict

When `setCacheDirectory()` is called and configuration already exists:

```
SettingsAlreadyConfiguredException: Cache directory is already configured.

Current setting in ~/.config/vectordata/settings.yaml:
  cache_dir: /mnt/data/vectordata-cache

To change this setting:
  1. Edit ~/.config/vectordata/settings.yaml manually, or
  2. Use setCacheDirectory(value, force=true) to override programmatically

This protection prevents accidental reconfiguration. To disable it,
set 'protect_settings: false' in settings.yaml.
```

### Auto-Resolution Algorithm

For `auto:largest-non-root`, `auto:largest-any`, and `default`:

```
1. Detect that cache_dir contains a resolvable value (auto:* or default)
2. For auto:* values:
   a. List all mounted filesystems
   b. Filter to those where:
      - User has write permission
      - For 'auto:largest-non-root': mount point is not "/"
   c. Sort by available space (descending)
   d. Select the mount point with most available space
   e. Resolve to: <mount-point>/vectordata-cache
3. For 'default':
   a. Resolve to: ~/.cache/vectordata (expanded to absolute path)
4. Create the resolved directory if it doesn't exist
5. **PERSIST**: Update settings.yaml, replacing the directive with the resolved absolute path
6. Log the resolution for user visibility:
   "Resolved cache_dir 'auto:largest-non-root' -> '/mnt/nvme0/vectordata-cache'"
```

**Rationale for persistence:** By persisting the resolved path, the cache location remains stable across:
- System reboots
- Mount point changes (drives added/removed)
- Different storage configurations

This prevents data fragmentation and ensures previously cached datasets remain accessible.

#### Mount Point Detection

```java
public class MountPointResolver {

    /// Find the optimal cache directory based on the auto:* setting.
    ///
    /// @param includeRoot Whether to include the root filesystem
    /// @return The resolved cache directory path
    /// @throws NoSuitableMountPointException if no writable mount point is found
    public static Path findOptimalCacheDirectory(boolean includeRoot) { ... }

    /// List all writable mount points with their available space.
    public static List<MountPointInfo> listWritableMountPoints() { ... }
}

public record MountPointInfo(
    Path mountPoint,
    long totalSpace,
    long availableSpace,
    boolean isRoot
) {}
```

## Validation Rules

### On Load

| Condition | Result |
|-----------|--------|
| Settings file missing | `CacheDirectoryNotConfiguredException` |
| `cache_dir` key missing | `InvalidSettingsException` |
| `cache_dir` value empty | `InvalidSettingsException` |
| `cache_dir` path doesn't exist and can't be created | `InvalidCacheDirectoryException` |
| `cache_dir` path exists but not writable | `InvalidCacheDirectoryException` |
| `auto:*` resolves to no suitable mount | `NoSuitableMountPointException` |

### On Programmatic Set

| Condition | Result |
|-----------|--------|
| Settings file exists with `cache_dir` set | `SettingsAlreadyConfiguredException` (unless `force=true`) |
| Invalid value format | `InvalidCacheDirectoryException` |
| Path not writable | `InvalidCacheDirectoryException` |
| Parent directory of settings file not writable | `SettingsWriteException` |

## CLI Commands

### Initialize Configuration

```bash
# Interactive setup
nbvectors config init

# Non-interactive with specific value
nbvectors config init --cache-dir auto:largest-non-root
nbvectors config init --cache-dir /mnt/data/cache
nbvectors config init --cache-dir default

# Force overwrite existing
nbvectors config init --cache-dir /new/path --force
```

### Show Current Configuration

```bash
nbvectors config show

# Output (after resolution has occurred):
# Configuration: ~/.config/vectordata/settings.yaml
#
# cache_dir: /mnt/nvme0/vectordata-cache
#   Status: Active
#   Used space: 15.3 GB
#   Available space: 1.2 TB
#
# protect_settings: true

# Output (if resolution just occurred):
# Configuration: ~/.config/vectordata/settings.yaml
#
# cache_dir: /mnt/nvme0/vectordata-cache
#   Resolved from: auto:largest-non-root
#   Status: Newly created
#   Available space: 1.2 TB
#
# protect_settings: true
```

### List Available Mount Points

```bash
nbvectors config list-mounts

# Output:
# Available mount points for cache storage:
#
#   Mount Point          Available    Total    Writable
#   /mnt/nvme0           1.2 TB       2.0 TB   Yes
#   /mnt/data            500 GB       1.0 TB   Yes
#   /home                50 GB        100 GB   Yes
#   /                    20 GB        50 GB    Yes (root)
```

## Integration Points

### TestDataSources

Update `TestDataSources` to use the settings:

```java
public class TestDataSources {

    /// Configure with settings from ~/.config/vectordata/settings.yaml
    public TestDataSources configure() {
        VectorDataSettings settings = VectorDataSettings.load();
        return configure(settings);
    }

    /// Configure with explicit settings
    public TestDataSources configure(VectorDataSettings settings) {
        this.cacheDir = settings.getCacheDirectory();
        // ... other configuration
        return this;
    }

    /// Configure with explicit config directory (legacy compatibility)
    public TestDataSources configure(Path configDir) {
        VectorDataSettings settings = VectorDataSettings.load(configDir.resolve("settings.yaml"));
        return configure(settings);
    }
}
```

### Catalog

Update `Catalog` to respect cache settings:

```java
public class Catalog {

    public static Catalog of(TestDataSources config) {
        // Cache directory is already resolved in TestDataSources
        return new Catalog(config);
    }
}
```

### Command-Line Options

Commands that interact with cache should support override:

```java
@CommandLine.Option(names = {"--cache-dir"},
    description = "Override cache directory (temporary, does not persist)")
private Path cacheDir;

@Override
public Integer call() {
    VectorDataSettings settings = VectorDataSettings.load();
    Path effectiveCacheDir = cacheDir != null ? cacheDir : settings.getCacheDirectory();
    // ...
}
```

## Migration Path

### Existing Users

Users with existing cached data in `~/.cache/vectordata`:

1. On first run after upgrade, detect existing cache
2. Prompt user to create settings.yaml pointing to existing location:
   ```
   Detected existing cache at ~/.cache/vectordata (2.5 GB)

   To continue using this location, create settings.yaml:
     cache_dir: default

   Or specify a new location (existing data will NOT be moved automatically).
   ```

### Existing API Callers

For code that previously set cache directory via `TestDataSources.setCacheDir()`:

1. Deprecate direct cache directory setting on TestDataSources
2. Require migration to `VectorDataSettings` API
3. Provide clear migration guidance in deprecation messages

## Security Considerations

1. **Settings file permissions**: Create `settings.yaml` with mode 0600 (user read/write only)
2. **Cache directory permissions**: Create cache directories with mode 0700
3. **Path traversal**: Validate that resolved paths don't escape expected boundaries
4. **Symlink handling**: Follow symlinks but validate final destination is writable

## Error Messages

All error messages should be:
- Clear about what went wrong
- Actionable with specific remediation steps
- Include relevant file paths and current values

Example:
```
CacheDirectoryNotConfiguredException:

  vectordata cache directory is not configured.

  Configuration file: ~/.config/vectordata/settings.yaml (not found)

  To configure, create this file with contents:

    cache_dir: auto:largest-non-root

  Available options:
    - auto:largest-non-root  Use largest non-root drive (recommended)
    - auto:largest-any       Use largest drive (may be root)
    - default                Use ~/.cache/vectordata
    - /path/to/dir           Use specific path

  Or run: nbvectors config init
```

## Open Questions

1. **Should we support environment variable override?**
   - Pro: Useful for containerized deployments
   - Con: Adds complexity, may conflict with settings.yaml
   - Proposed: Support `VECTORDATA_CACHE_DIR` env var as highest priority override (temporary, does not persist)

2. **Should we support multiple cache directories?**
   - Pro: Could spread data across drives
   - Con: Significant complexity increase
   - Proposed: Defer to future version; single cache_dir for now

3. **Should we provide a way to re-resolve after initial setup?**
   - Use case: User added a new larger drive and wants to migrate
   - Proposed: `nbvectors config migrate --to auto:largest-non-root` which would:
     - Resolve the new location
     - Optionally move existing cached data
     - Update settings.yaml

## Implementation Phases

### Phase 1: Core Infrastructure
- [ ] `VectorDataSettings` class with load/save
- [ ] Settings file parsing and validation
- [ ] `CacheDirectoryNotConfiguredException` and related exceptions
- [ ] Update `TestDataSources` to use settings

### Phase 2: Auto-Resolution
- [ ] `MountPointResolver` implementation
- [ ] Platform-specific mount point detection (Linux, macOS, Windows)
- [ ] `auto:largest-non-root` and `auto:largest-any` support

### Phase 3: CLI Commands
- [ ] `nbvectors config init` command
- [ ] `nbvectors config show` command
- [ ] `nbvectors config list-mounts` command

### Phase 4: Migration Support
- [ ] Detect existing cache directories
- [ ] Migration prompts and guidance
- [ ] Deprecation warnings for old APIs
