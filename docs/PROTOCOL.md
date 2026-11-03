# ConcurrentCore wire protocol (version 1)

Clients talk to the server over a single TCP connection using length-prefixed
JSON messages. The server binds to `127.0.0.1:7878` by default.

## 1. Framing

```
+----------------+----------------+----------------+----------------+-------------------------+
|                 payload length: uint32, big-endian (network order)  |  payload: UTF-8 JSON    |
+----------------+----------------+----------------+----------------+-------------------------+
       byte 0           byte 1           byte 2           byte 3        exactly `length` bytes
```

* Every message (request, response, event) is one frame.
* TCP is a byte stream: a single `recv` may return part of a header, a whole
  frame, or several frames. Both the server and the client reassemble frames
  with an incremental decoder (`net::FrameDecoder`) and never assume read
  boundaries match message boundaries.
* The maximum payload size is configurable (`max_message_bytes`, default
  1 MiB). The limit is checked **as soon as the 4 header bytes arrive**,
  before any payload is buffered. An oversized frame produces a
  `MESSAGE_TOO_LARGE` error and the server then closes the connection,
  because the stream cannot be resynchronized.
* An empty (zero-length) frame or invalid JSON produces `MALFORMED_JSON`;
  the stream is still in sync, so the connection stays open.
* JSON nesting deeper than 32 levels is rejected as `MALFORMED_JSON`.

## 2. Message envelopes

### Request

```json
{ "type": "<OPERATION>", "request_id": <string | integer>, ...operation fields }
```

`request_id` is optional. If present it must be a string or an integer, and
it is echoed verbatim in the response so that clients can pipeline requests
and match responses to them.

### Response

Success:

```json
{ "kind": "response", "type": "<OPERATION>", "request_id": ..., "ok": true, "result": { ... } }
```

Failure:

```json
{ "kind": "response", "type": "<OPERATION>", "request_id": ..., "ok": false,
  "error": { "code": "<ERROR_CODE>", "message": "human readable detail" } }
```

If the request could not be parsed far enough to know the operation, `type`
is `"ERROR"` and `request_id` is `null`.

### Event (server push)

```json
{ "kind": "event", "type": "JOB_FINISHED", "request_id": <id of the SUBMIT_JOB request>, "data": { job } }
```

Sent once when a job submitted with `"notify": true` reaches a terminal
state. It goes to the connection that submitted the job. If that connection has
closed by then, the event is dropped. Responses are written in the order
their requests were processed. An event for a job can never overtake the
`SUBMIT_JOB` response that created the job, because both are serialized on
the connection's strand.

## 3. Operations

### PING

```json
{"type":"PING","request_id":1}
```
```json
{"kind":"response","type":"PING","request_id":1,"ok":true,
 "result":{"pong":true,"protocol_version":1,"server_time_ms":1793750400123}}
```

### SUBMIT_JOB

| Field       | Type    | Required | Description                                    |
|-------------|---------|----------|------------------------------------------------|
| `task_type` | string  | yes      | `sha256`, `compress`, `decompress`, `text-analysis`, `compute` |
| `params`    | object  | no       | Task parameters (see section 4). Unknown keys are rejected. |
| `notify`    | boolean | no       | Push a `JOB_FINISHED` event when done (default false). |

```json
{"type":"SUBMIT_JOB","request_id":"r1","task_type":"sha256","params":{"file":"data/sample.bin"},"notify":true}
```
```json
{"kind":"response","type":"SUBMIT_JOB","request_id":"r1","ok":true,
 "result":{"job_id":42,"status":"QUEUED","notify":true}}
```

Parameters are validated **before** the job is queued. Malformed jobs are
rejected immediately with `INVALID_PARAMS`, `UNKNOWN_TASK_TYPE` or
`PATH_NOT_ALLOWED` and never occupy a queue slot. If the bounded queue is
full the server answers `QUEUE_FULL` at once instead of blocking; clients
should back off and retry.

### GET_JOB_STATUS

```json
{"type":"GET_JOB_STATUS","request_id":2,"job_id":42}
```
```json
{"kind":"response","type":"GET_JOB_STATUS","request_id":2,"ok":true,
 "result":{"job_id":42,"task_type":"sha256","status":"RUNNING","cancel_requested":false,
           "created_at_ms":1793750400100,"started_at_ms":1793750400101,"finished_at_ms":null,
           "queue_wait_ms":0.84,"execution_ms":null}}
```

### GET_JOB_RESULT

Same as `GET_JOB_STATUS` plus `result` and `error`. Returns
`JOB_NOT_FINISHED` while the job is `QUEUED` or `RUNNING`.

```json
{"type":"GET_JOB_RESULT","request_id":3,"job_id":42}
```
```json
{"kind":"response","type":"GET_JOB_RESULT","request_id":3,"ok":true,
 "result":{"job_id":42,"task_type":"sha256","status":"COMPLETED","cancel_requested":false,
           "created_at_ms":1793750400100,"started_at_ms":1793750400101,"finished_at_ms":1793750400109,
           "queue_wait_ms":0.84,"execution_ms":7.91,
           "result":{"file":"data/sample.bin","algorithm":"SHA-256","bytes":1048576,
                     "digest":"5c2e...e1f0"},
           "error":null}}
```

A failed job has `"status":"FAILED"`, `"result":null` and
`"error":{"code":"FILE_NOT_FOUND","message":"file not found: data/missing.bin"}`.

### CANCEL_JOB

```json
{"type":"CANCEL_JOB","request_id":4,"job_id":42}
```
```json
{"kind":"response","type":"CANCEL_JOB","request_id":4,"ok":true,
 "result":{"job_id":42,"outcome":"CANCELLATION_REQUESTED"}}
```

| `outcome`                | Meaning |
|--------------------------|---------|
| `CANCELLED`              | The job was still queued. It is now `CANCELLED` and will never run. |
| `CANCELLATION_REQUESTED` | The job is running. A stop was requested and the task stops at its next cancellation point (status becomes `CANCELLED`). If the task finishes its work before checking, the job ends `COMPLETED`. |
| `ALREADY_FINISHED`       | The job was already terminal; nothing changed. |

Unknown job ids return `JOB_NOT_FOUND`.

### GET_SERVER_STATS

```json
{"type":"GET_SERVER_STATS","request_id":5}
```
```json
{"kind":"response","type":"GET_SERVER_STATS","request_id":5,"ok":true,
 "result":{"uptime_seconds":12.5,"accepting_jobs":true,
           "jobs":{"submitted":120,"rejected":0,"completed":118,"failed":1,"cancelled":1,"running":0,"retained":120},
           "queue":{"depth":0,"capacity":1024},
           "workers":{"total":8,"active":0},
           "latency_ms":{"avg_execution":4.2,"avg_queue_wait":0.6},
           "throughput":{"jobs_per_second":9.6},
           "connections":{"active":1,"accepted_total":4,"rejected_total":0},
           "io_threads":2}}
```

`jobs_per_second` is the lifetime average (finished jobs / uptime).
Counters are read individually without a global lock, so the snapshot
is not atomic across fields.

## 4. Task types and parameters

All paths are **relative to the server's workspace directory** and use `/`
as the separator. Absolute paths, `..`, drive letters, `:` (alternate data
streams), backslashes, NUL bytes and Windows device names (`CON`, `NUL`,
`COM1`, …) are rejected with `PATH_NOT_ALLOWED`. Paths that resolve through
symlinks to a location outside the workspace are also rejected. Files must
already exist in the workspace; this protocol has no upload operation.

| `task_type`     | Params | Result fields |
|-----------------|--------|---------------|
| `sha256`        | `file` | `file`, `algorithm`, `digest` (lowercase hex), `bytes` |
| `compress`      | `input`, `output` (default `input` + `.zz` or `.gz`), `level` 0–9 (default 6), `format` `zlib`\|`gzip` (default `zlib`), `overwrite` (default false) | `input`, `output`, `format`, `level`, `original_bytes`, `compressed_bytes`, `compression_ratio` |
| `decompress`    | `input`, `output`, `overwrite`, `max_output_bytes` (1 … server limit, default = server limit 256 MiB) | `input`, `output`, `compressed_bytes`, `decompressed_bytes`, `max_output_bytes` |
| `text-analysis` | exactly one of `file` or `text` (inline string) | `bytes`, `characters` (UTF-8 code points), `words`, `lines`, `source`, `file` |
| `compute`       | `limit` (0 … server `max_compute_limit`, default 200 000 000) | `algorithm`, `limit`, `prime_count`, `largest_prime` |

Safety rules for file output:

* Output goes to a uniquely named temporary file in the target directory.
  It is moved into place only after the whole task succeeds. A failed or
  cancelled job leaves no partial output.
* Without `overwrite: true`, an existing output is never replaced. The final
  publish step uses an atomic no-replace rename (`link()` on POSIX,
  `MoveFileExW` without `REPLACE_EXISTING` on Windows), so two concurrent jobs
  cannot clobber each other.
* `decompress` accepts zlib or gzip streams (auto-detected, single member).
  Truncated input, corrupt data and trailing bytes are rejected as
  `CORRUPT_DATA`. Output beyond `max_output_bytes` aborts the job with
  `LIMIT_EXCEEDED` *before* the excess bytes are written (decompression-bomb
  guard).

## 5. Error codes

| Code                   | Where | Meaning |
|------------------------|-------|---------|
| `MALFORMED_JSON`       | request | Payload is not valid JSON, is empty, or is nested too deeply. |
| `INVALID_REQUEST`      | request | Not a JSON object, missing/invalid `type`, or invalid `request_id`. |
| `UNKNOWN_OPERATION`    | request | `type` is not one of the six operations. |
| `INVALID_PARAMS`       | request | Missing or invalid operation / task parameters. |
| `UNKNOWN_TASK_TYPE`    | request | Unsupported `task_type`. |
| `MESSAGE_TOO_LARGE`    | connection | Frame exceeds `max_message_bytes`; the connection is closed. |
| `JOB_NOT_FOUND`        | request | Unknown job id, or evicted by bounded retention. |
| `JOB_NOT_FINISHED`     | request | `GET_JOB_RESULT` on a queued/running job. |
| `QUEUE_FULL`           | request | Back-pressure: the bounded task queue is full. Retry later. |
| `SERVER_SHUTTING_DOWN` | request | The server is draining and accepts no new jobs. |
| `SERVER_BUSY`          | connection | Connection limit reached; the connection is closed. |
| `FILE_NOT_FOUND`       | job | Input (or output directory) does not exist in the workspace. |
| `PATH_NOT_ALLOWED`     | request / job | Path is unsafe or resolves outside the workspace. |
| `OUTPUT_EXISTS`        | job | Output exists and `overwrite` was not set. |
| `IO_ERROR`             | job | Read/write failure. |
| `LIMIT_EXCEEDED`       | job | A resource limit (e.g. decompressed size) was hit. |
| `CORRUPT_DATA`         | job | Invalid, truncated or trailing compressed data. |
| `CANCELLED`            | job | The job was cancelled. |
| `INTERNAL_ERROR`       | any | Unexpected failure; see message. |

## 6. Connection behaviour

* **Idle timeout:** connections that send nothing for `idle_timeout_seconds`
  (default 300, 0 disables) are closed. Waiting for a `JOB_FINISHED` event
  does not count as activity.
* **Slow readers:** each connection has a bounded outgoing queue
  (`max_pending_write_bytes`, default 8 MiB). Above half of it the server stops
  reading new requests from that client until it drains below a quarter. If
  the hard limit would be exceeded, the connection is dropped. A slow client can
  never block worker threads or other clients.
* **Half-close:** if a client shuts down its sending side, the server finishes
  writing queued responses before closing.
* **Shutdown:** on SIGINT/SIGTERM the server stops accepting connections and
  jobs (`SERVER_SHUTTING_DOWN`). Existing connections keep working while jobs
  drain, so clients can still poll and receive events. Then each connection is
  flushed and closed.
