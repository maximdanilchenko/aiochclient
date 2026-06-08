# Iteration 1 — bulk column encoders

The encode was dominated by per-value RowBinary writers for the non-numeric
scalar columns. Added type-specific bulk encoders in `encode_column`:

- **String** → `write_string_column` (Cython, pure fallback): varint-prefixed
  UTF-8 written straight into one bytearray. 6.6M → ~13M+ rows/sec/column.
- **Date** → `array("H", days)`. 8.5M → ~16M.
- **DateTime** (no tz) → `array("I", seconds)`. 2.1M → ~5.9M.

`Array(String)` benefits automatically (its flat child is encoded as String).

## Result

| stage | iter 0 | iter 1 |
|---|---|---|
| encode only | 645k | **1,458k** (2.26×) |
| full INSERT | 366k | ~388k |

## Read

Encode is now 2.26× faster — but the full INSERT barely moved (server-bound,
see iter 0 decomposition: encode and post run sequentially and post dominates).
The faster encode is still worth it: it makes each streamed block cheaper, which
is what lets iter 2's overlap pay off, and it helps a lot on faster servers /
string-heavy data. Correctness: native tests (insert→read vs TSV) green on both
HTTP backends.
