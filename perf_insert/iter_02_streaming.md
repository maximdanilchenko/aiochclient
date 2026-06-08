# Iteration 2 — stream the INSERT body as Native blocks ✅ GOAL MET

## Idea

The full INSERT paid encode + post sequentially, and the post (HTTP + server
insert) is the bigger half. The Native format is a sequence of self-contained
blocks, so the body can be **streamed**: send block K while the client encodes
block K+1. The server then inserts K (server-side, in parallel) while the client
is busy with K+1 — overlapping the two costs instead of summing them.

## Change

- `rows_to_native_stream(rows, names, types, block_rows=4096)` — an **async**
  generator yielding one Native block per `block_rows` rows (async so the HTTP
  backends accept it as a chunked request body; aiohttp rejects a *sync*
  generator as data).
- `client._execute` native INSERT path now passes that generator as the request
  body instead of one pre-encoded blob.
- Block size 4096 rows (2k/4k/8k all within ~1.5% in testing; 4k slightly best).

Trade-off documented: an INSERT split into blocks is not atomic on a *client
encoding* error (earlier blocks may already be sent); with valid uniformly-typed
rows the first block would already have failed, so it does not arise in practice.

## Result — load-invariant fair A/B (same process, no probe, same encode)

| body | rows/sec |
|---|---|
| whole block (non-streaming) | 368k |
| **streamed, chunk 4096** | **513k (+39%)** |

(2048 → 504k, 8192 → 506k.)

## Result — official `benchmarks_vs_libs.py` (full path incl. probe)

Native INSERT **~430–485k** (was ~330–366k): **+30–40%**, and now ahead of
clickhouse-connect (~350k) and clickhouse-driver (~300–400k) on this workload.

## Status — goal reached

Native INSERT improved by ~30–40% (load-invariant +39% in the controlled A/B).
Full suite 921 passed (both HTTP backends), pure-Python fallback green.

## Possible follow-ups (not done)

- **Cache the column-types probe** per (table, columns): the `LIMIT 0` probe is
  a round-trip on *every* INSERT (~8% of the full path here). Caching would push
  the real path toward the no-probe A/B figure, but risks staleness on
  `ALTER TABLE`, so it was left out.
- Infer wire types from the Python values to skip the probe entirely (bigger
  change, own edge cases: empty / all-None columns).
