# Request 1: durable event store

Work only in the supplied candidate workspace. Do not seek or read grader code
or a reference implementation. Keep implementation modules in this workspace;
their directory layout is your choice.

Implement a durable event store in JavaScript for Node.js. Write `engine.mjs`,
exporting this synchronous interface:

```js
export default class Engine {
  constructor(fs, options) { /* open existing storage or initialize it */ }
  handle(request) { /* return a JSON-compatible value */ }
}
```

Choose the internal representation and persistence design yourself. Use Node's
standard library only. All persistent I/O must go through the supplied `fs`
function. Do not use real filesystem, network, clock, timer, worker, or subprocess
APIs. There are no background tasks. Each instance owns its storage exclusively;
calls are sequential. Another instance can later open the same storage.
Options are supplied by the caller; reserve them for the later requests.

## Supplied filesystem

`fs(op, args = {})` is synchronous. Filenames are nonempty flat strings without
`/`, other than `.` and `..`. Storage initially contains no files. Operations:

| Call | Result and meaning |
|---|---|
| `fs('list')` | Sorted array of filenames. |
| `fs('read', {name, offset = 0, size})` | A fresh Buffer; omitted size reads to EOF. Missing file raises `ENOENT`. |
| `fs('write', {name, offset = 0, data})` | Writes Buffer/Uint8Array data, creating a missing file; returns the number of bytes written. Does not truncate an existing suffix. Gaps contain zeros. |
| `fs('truncate', {name, size})` | Sets an existing file's byte length, zero-filling any extension. |
| `fs('rename', {name, to})` | Atomically replaces `to` with the inode named `name`, removing the source name. |
| `fs('unlink', {name})` | Removes an existing filename. |
| `fs('sync', {name})` | Makes that inode's current bytes durable. |
| `fs('sync')` | Makes the current directory names and inode associations durable. |

Except for list/write, operations on absent filenames raise an Error with
`code: 'ENOENT'`. Storage operations may raise `EIO` or `ENOSPC`. Writes may be
short, including zero bytes. After a power loss, independently unsynchronized
file contents and directory changes may survive, disappear, or survive partly.
Synchronized contents and synchronized directory associations survive. Recovery
uses a new Engine with the surviving filesystem and the same options.

## Requests and exact replies

`stream`, `key`, and `destination` are arbitrary nonempty strings. Payloads are
plain JSON objects, including nested JSON values. Reject functions, undefined,
non-finite numbers, BigInt, Date, Map, sparse arrays, and cyclic values as invalid.
JSON numeric equality treats -0 and 0 as equal. A key is scoped to its stream.
Object member order does not affect payload equality; array order does.

The receipt shape is always:

```js
{ seq: 1, state: 'pending', attempts: 0, reason: null }
```

Supported requests:

* `{op:'put', stream, key, destination, payload}`: accept a durable event and
  return its receipt. Sequence numbers start at 1 in each stream. Every new accepted event gets
  exactly one more than the highest retained sequence in that stream, with no gaps
  or reuse. Invalid requests and idempotent replays do not allocate sequences. Repeating the same stream/key with the same destination and
  payload returns the current receipt. Different content returns
  `{error:'conflict'}`. Retain identities, receipts, and sequence allocation
  permanently, including after cancellation and compaction.
* `{op:'get', stream, key}`: return the current receipt, or
  `{error:'not_found'}`. This request has no state changes.
* `{op:'cancel', stream, key}`: change a pending event to `cancelled` durably,
  retaining its sequence, attempts, and `reason:null`; return its receipt.
  Repeating cancellation returns the same receipt. Missing event:
  `{error:'not_found'}`.
* `{op:'compact'}`: compact durable storage without changing any public event
  information. Return `{}`. Repeating this request with unchanged event state
  must keep total file bytes within `max(65536, 2 * B)`, where `B` is total file
  bytes immediately after the first compaction of that state. After every
  successful compaction, total file bytes must also be at most `65536 + 8 * R`.
  `R` is the sum, over accepted events, of the UTF-8 byte lengths of
  `JSON.stringify({stream, key, destination, payload, seq, state, attempts, reason})`
  using the accepted content and current receipt. This bound applies whether
  history is reclaimed during normal requests or during compaction.

Malformed requests and unknown operations return `{error:'invalid'}` without
changing state. Extra object properties may be ignored. Replies and stored
inputs must not share mutable objects with the caller.

A successful mutating reply means its changes survive immediate power loss.
Previously successful requests must remain valid through a later interrupted
operation. An interrupted operation without a successful reply may have committed
or not; retrying its identity must resolve it consistently. On I/O errors,
either return `{error:'io'}` or throw an Error with code `EIO` or `ENOSPC`.
After an error, an instance must preserve previously confirmed work if it keeps
serving requests; it may instead reject all subsequent requests with an I/O error
until storage is reopened. An unsuccessful open must not serve an empty store in
place of existing data. A zero-byte short write may cause an explicit I/O failure
if the request cannot complete. Recovery must not require operator repair.

A simulated power loss can interrupt a filesystem call with an exception. After
that interruption the instance is never used again. Its exception type is not
part of the public API.

Implement this request only. Add your own tests. Finish with the files changed
and the tests you ran.
