# Request 2: ordered delivery

Work only in the supplied candidate workspace. Do not seek or read grader code
or a reference implementation. Keep implementation modules in this workspace;
their directory layout is your choice.

Extend the event store from Request 1. Keep that interface and all its behavior.
Keep `request-1.md` available alongside this request.
Add delivery driven by a single request, with no background work or real network
calls. The caller sends returned attempts to destinations and supplies outcomes.

Expect workloads of at least 10,000 retained events with sustained ingestion and
delivery. Execution time, memory use, and storage growth are evaluation criteria.

Constructor options now include these positive integer settings:

```js
{ leaseMs: 10, baseMs: 4, capMs: 16, maxAttempts: 3,
  maxInFlight: 4, perDestination: 2 }
```

These are an example; use the supplied values. The timing/retry settings take
effect in Request 3. This request covers responses that arrive before expiry.

## Delivery request

```js
{ op:'step', now, results:[], budget:100, jitterMs:0 }
```

`now` is an absolute nonnegative safe-integer time in milliseconds, supplied by
the caller. It does not reset when storage is reopened. It can stay equal between
calls. A decreasing time within an instance must return `{error:'invalid'}` without
changing state or the last accepted time. `budget` is a nonnegative safe
integer limiting newly emitted attempts in this call, including zero.
Defaults for omitted results/budget/jitterMs are shown above.

Return an array of outbound attempts, with exactly these fields:

```js
{ stream, key, seq, attempt, destination, payload }
```

Attempt numbers start at 1 for each event and increase without reuse. Emitting an
attempt durably changes its event to `inflight` and increments receipt `attempts`.
The payload and destination are the accepted values. Each attempt holds one slot
until resolved; limits count unresolved attempts, rather than attempts emitted
in just the current call. Reopening storage preserves unresolved attempts.

Only the lowest-sequence nonterminal event in each stream can be emitted. Events
in independent streams can proceed independently. `acked`, `dead`, and
`cancelled` are terminal. While budget and capacity remain, emit eligible heads
until no further head fits the global and destination limits. Selection order
between independent eligible streams is your choice. Calling step repeatedly
must not emit an additional attempt for an unresolved event.

## Outcomes

Each result has `{stream, key, attempt, code}`. Process the results array in its
given order, before selecting new work. Match an outcome to an unresolved
attempt by stream, key, and attempt number. Unknown identities and results for
noncurrent or terminal attempts have no effect.

* Integer HTTP codes 200–299 durably change the event to `acked`, `reason:null`.
* Integer HTTP codes 300–499, excluding 429, durably change the event to `dead`,
  with `reason:'http:' + code`.

A terminal transition frees capacity and allows the next event in that stream
to be emitted in the same call. Event inspection still uses the Request 1 receipt
shape, including updated state, attempts, and reason. Dead events remain durable
and inspectable. Identical replayed puts return their terminal receipt.
Cancellation of an `inflight` event returns `{error:'busy'}` without changing it.
Cancellation of any terminal event returns its existing receipt unchanged.
Compaction preserves all unresolved and terminal work and identities.

Malformed step inputs return `{error:'invalid'}` without changing anything;
validate the entire request before processing any outcome. Integer times and
budgets must be safe integers in their stated range. Valid result identities
use nonempty strings and positive safe-integer attempt numbers. Syntactically
valid outcome codes are integers 100–599 and the strings `timeout`, `reset`, and
`malformed`. Unknown or noncurrent attempts ignore any syntactically valid result.
Request 3 adds handling of retryable outcomes on current attempts; for this
request, current attempts receive only the success/permanent outcomes above.
All durability and storage-error rules still apply.
An interrupted step may commit individual event transitions independently;
a successfully returned step durably commits all its reported transitions.

Implement this request only and keep earlier behavior working. Add your own
tests. Finish with the files changed and the tests you ran.
