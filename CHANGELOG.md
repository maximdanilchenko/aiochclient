# Changelog

## 2.8.0

New — binary engines (both opt-in, TSV stays the default; pure-Python fallback
kept when the Cython extension isn't built):

- **Native columnar engine** — `ChClient(session, native=True)`. Decodes (and
  encodes) ClickHouse's column-oriented `Native` format whole-column-at-once,
  the fastest engine for SELECT, without numpy. Full type coverage including
  `LowCardinality` and `Nested`; columnar INSERT.
- **RowBinary engine** — `ChClient(session, binary=True)`, with a
  Cython-accelerated reader. Faster than TSV in both directions and the right
  choice for row-by-row `iterate` streaming (#134, #139, #140).
- Compiled `Record` (a `cdef` class) — cheaper to build, speeds up every engine.

Notes:
- `decode=False` (raw bytes) is a TSV-only feature.
- The Native format carries no per-column timezone, so a tz-aware
  `DateTime`/`DateTime64` is returned as a naive UTC `datetime` (TSV and
  RowBinary apply the timezone).

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
