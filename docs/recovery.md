# Recovery and Journaling

## Journal format

The journal is append-only and one record is stored per line.

Each record uses this envelope:

```text
HFTJ|version=1|len=<payload_len>|<payload>|checksum=<fnv1a64(payload)>
```

Fields:

- `HFTJ`
  - fixed magic string for journal identification
- `version`
  - schema version for the record format
- `len`
  - payload length in bytes
- `payload`
  - explicit key/value fields, currently:
    - `type`
    - `order_id`
    - `side`
    - `price`
    - `qty`
- `checksum`
  - FNV-1a 64-bit checksum of the payload only

The parser validates:

- magic
- supported version
- declared payload length
- payload field structure
- checksum match

## Replay contract

Replay is deterministic and fail-fast.

Behavior:

- replay processes records in order
- replay stops at the first bad record
- replay returns structured failure information
- normal startup is refused if replay fails

Failure metadata includes:

- `line_number`
- `code`
  - `ParseError`
  - `InvalidTransition`
- `message`
- `event_type` when available
- `records_replayed`

Startup logs recovery failure in this form:

```text
fatal: replay_journal failed line=<line> code=<code> records=<count> message=<text>
```

## Async journal policy

Async journaling runs in `FailFast` mode for correctness-sensitive operation.

That means:

- journal writes do not silently drop
- when the async queue is full, `write(...)` returns `Backpressure`
- coordinator treats journal backpressure as an internal failure
- request handling rejects the operation instead of pretending it was durably recorded

`Block` exists as a sink policy, but it is not the recommended production mode here because latency is prioritized and journaling is part of recovery correctness.

## Wire reject reason

Wire reject code `7` is reserved for:

- `JournalBackpressure`

Meaning:

- the request was rejected because the server could not enqueue the required journal event under fail-fast backpressure policy

This is distinct from client message errors such as invalid length or invalid side.
