# Request 3: delivery timing and recovery

Work only in the supplied candidate workspace. Do not seek or read grader code
or a reference implementation. Keep implementation modules in this workspace;
their directory layout is your choice.

Extend Requests 1 and 2 with the remaining delivery behavior. Keep the same
constructor and `handle` interface. Choose your implementation yourself.

When an attempt is emitted at `now`, its lease expires at `now + leaseMs`.
On each step, first expire every unresolved lease whose deadline is `<= now`,
then process supplied results in order, then select eligible work. An expired
attempt's later outcomes have no effect. Reopening storage preserves the lease
and its original deadline; time continues on the caller's absolute timeline.

For a current result received before expiry:

* 200–299: `acked`, as specified earlier.
* 300–499 except 429: `dead`, as specified earlier.
* 100–199, 429, 500–599, and the strings `timeout`, `reset`, or `malformed`:
  retryable failures.

Lease expiry is also a retryable failure. Other codes make the entire step
invalid. A result's optional `retryAfterMs` is a parsed HTTP Retry-After duration
provided by the caller. It applies only to 429 and is valid only if it is a
nonnegative safe integer; otherwise ignore it.

After a retryable failure of attempt number `n`, the delay is:

```js
Math.min(capMs, baseMs * 2 ** (n - 1)) + jitterMs
```

`jitterMs` is supplied with the step, defaults to zero, and must be a nonnegative
safe integer no greater than `baseMs`. Store the resulting deadline; future steps
do not recalculate a previously scheduled delay. For 429, the delay is the maximum
of the formula above and a valid `retryAfterMs`.

The retry deadline is failure time plus delay. Failure time is `now` for supplied
outcomes and the original lease deadline for expired leases. A retry can be
emitted when `now >= retryDeadline`, including in the same call if time has
already advanced that far. Waiting events have state `pending`, retaining their
attempt count and `reason:null`. They continue to block later stream events.

`maxAttempts` includes the first attempt. A retryable failure at that limit
durably changes the event to `dead`, with `reason:'exhausted'`; there is no further
retry. Cancellation of a pending retry remains allowed. Every terminal transition
releases capacity and permits stream advancement.

Keep accepted events, terminal results, attempt numbers, lease deadlines, retry
deadlines, and idempotency information consistent across reopening storage and
compaction. A successful mutating reply must survive power loss. Operations
interrupted without a successful reply may or may not have committed, but must
not invalidate previously acknowledged work. Storage errors must remain explicit.

Delivery is at least once. A destination is responsible for durably deduplicating
`{stream,key}` identities; attempts carry that same identity on every retry.
The engine guarantees ordered attempt emission and current-result handling.
It does not promise single external effects at an uncooperative destination.

There is no HTTP server, HTTP parser, daemon, real clock, real socket, shutdown
protocol, or cross-process writer coordination in this task. The caller owns
those boundaries. Keep `engine.mjs` importable and synchronous; all persistent
I/O continues to use the supplied filesystem function.

Complete this request and preserve all earlier behavior. Add your own tests.
Finish with the files changed and the tests you ran.
