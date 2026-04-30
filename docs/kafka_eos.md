`rpcstream` now separates progress tracking into two Kafka topics:

1. `commit_watermark`
   This is the committed recovery position.
   It represents the last contiguous successful cursor for the pipeline.
   On restart, this is the topic used to decide where ingestion resumes.

2. `cursor_state`
   This is the gap-state ledger.
   It stores only gap-related cursor states:
   - failed cursors
   - completed cursors that must be persisted because a gap already exists
   - completed cursors that resolve a previously failed cursor
   It is used to fill gaps, rebuild state after restart, and advance the
   `commit_watermark` once a missing cursor is later resolved.

Default topic names:

```text
evm.bsc.mainnet.commit_watermark
evm.bsc.mainnet.cursor_state
```

Semantic difference:

- `commit_watermark` is the final recovery point.
  It only means "there are no gaps before this cursor".
- `cursor_state` is the gap-only status log.
  It does not record every successful cursor. It only records state that is
  needed to explain or recover a gap.

Difference between EOS and non-EOS:

- `EOS=true`
  Business data, `cursor_state`, and `commit_watermark` when it advances are
  committed in the same Kafka transaction.
- `EOS=false`
  These writes are not in one Kafka transaction, but the state model is the
  same. `commit_watermark` still means the last contiguous successful cursor.

Concrete example:

```text
Initial state:
commit_watermark = 99

Process 100:
100 failed
cursor_state:
  100 -> failed
commit_watermark:
  still 99

Process 101:
101 completed
cursor_state:
  100 -> failed
  101 -> completed   (persisted because 100 is still missing)
commit_watermark:
  still 99

Later, retry/replay resolves 100 successfully:
100 completed
cursor_state:
  100 -> completed
  101 -> completed
commit_watermark:
  advances to 101
```

You can think of them as:

- `cursor_state` = the gap-only ledger
- `commit_watermark` = the final contiguous committed balance

The main ingestion process periodically refreshes `cursor_state`, so when an
external retry or replay resolves a gap, the main ingestion process does not
need to restart in order to continue advancing `commit_watermark`.
