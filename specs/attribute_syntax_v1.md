# Attribute Syntax Specification v1

Describes the canonical attribute specifiers used in dataset manifests and tooling.

- Paths reference manifest sections (e.g., `/attributes/license`, `/profiles/default/base_vectors`).
- Attribute names must be valid YAML keys.
- Values follow the parsing rules defined in `AttrValue` (`STRING`, `INT`, `LONG`, `FLOAT`, `DOUBLE`, etc.).

Regular expression for attribute specifiers:
```
(?<path>/|(?:/[^:/.]+)+)?          # Optional path (/, /group, /profiles/default)
[:.]?                              # Optional separator
(?<attr>[a-zA-Z_][a-zA-Z0-9_]*)    # Attribute name
```
