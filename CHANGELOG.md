# Changelog

## 2.7.0

**Requires Python >= 3.10** (tested on 3.10–3.13).

New:
- `EXPLAIN` queries now return their result rows (#98).
- Inserts accept subclasses of supported types, e.g. `StrEnum`/`IntEnum` (#97).

Fixes:
- Container parsing for string values containing parens, commas or quotes (#123).
- `Map` decoding: multi-entry maps and empty maps (#118, #117).
- String decoding regression (trailing backslash / literal escape sequences).
- Consistent `DateTime64` sub-second handling, including nanoseconds (#91, #127, #119).
- Non-200 responses with an empty body now raise a non-empty error (#126).

Maintenance: refreshed dependencies, modernized CI, fixed the docs build.
