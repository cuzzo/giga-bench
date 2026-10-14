# Request 4: operator controls and storage errors

Work only in the supplied candidate workspace. Do not seek or read grader code
or a reference implementation. Keep implementation modules in this workspace;
their directory layout is your choice.

After Requests 1–3 and the cleanup review, extend the implementation with this
filesystem rule: a storage call that raises `EIO` or `ENOSPC` may have taken
effect fully, partly (including a partial write), or not at all before reporting
the error. Successful mutating replies must still survive immediate power loss.

Keep the same constructor and synchronous `handle` interface. Keep Requests 1–3
available alongside this request. All earlier behavior, load expectations,
acknowledgment, recovery, and storage-error rules still apply.

## Destination pause controls

* `{op:'pause', destination, until:null}` returns `{destination, paused:true}`.
  `destination` is a nonempty string. Omitted `until` means `null` (indefinite);
  otherwise it must be an absolute nonnegative safe-integer time. A pause replaces
  any previous pause for that destination, including its end time.
* `{op:'resume', destination}` returns `{destination, paused:false}` and removes
  any pause. Both operations are durable and idempotent, and are allowed before
  the destination has any events.

Only a valid `step` advances time. Before lease expiry and result processing,
remove each timed pause whose `until <= now`, even with `budget:0`. This removal
is durable if the step succeeds. A pause with an end time already passed still
exists until a valid step reaches that time. Invalid requests change nothing.

A paused destination receives no new attempts, including retries. Its in-flight
attempts still consume capacity; outcomes, lease expiry, and retry deadlines work
as before. A paused pending head blocks later events in its stream but consumes
no capacity itself. Other eligible streams must still run while capacity remains.

## Dead-event redrive

`{op:'requeue', stream, key}` durably returns the event to `pending`, with
`reason:null`, and returns its receipt. Keep its sequence, content, and lifetime
attempt count. It is immediately eligible on the next step, subject to ordering,
pauses, budget, and capacity. Clear any previous lease or retry deadline.

Check these conditions in order: missing event returns `{error:'not_found'}`;
an event whose state is not `dead` returns `{error:'not_dead'}`; otherwise, if
another event in that stream is `inflight`, return `{error:'busy'}`. Errors have
no effects. Malformed identities return `{error:'invalid'}` first.

Each successful redrive starts a new allowance of `maxAttempts` attempts. The
first attempt of this allowance uses the first-attempt backoff formula from
Request 3, and exhaustion counts only this allowance. Outbound attempt numbers
continue increasing across all allowances, so old outcomes remain stale. The
original sequence now participates in stream ordering again; later events that
are already terminal keep their state. A subsequent redrive is allowed after
the event becomes dead again, whether by exhaustion or a permanent outcome.

## Inspection

`{op:'stats'}` returns exactly:

```js
{ states: {pending, inflight, acked, dead, cancelled},
  streams: { [stream]: {head, backlog, acked} },
  destinations: { [destination]: {pending, inflight, paused} } }
```

All counts are nonnegative integers. `states` counts all retained events by
their current state, including pending retries. Each stream with an accepted
event is present; `head` is its lowest nonterminal sequence or `null`, `backlog`
counts its nonterminal events, and `acked` counts its acknowledged events.
Each destination with an accepted event or an active pause is present; its
`pending` and `inflight` count those states, and `paused` is a boolean. A
destination with neither events nor a pause is absent. Names are arbitrary
nonempty strings, including names matching JavaScript object properties.

Stats takes no time input, changes no state, and performs no storage mutations.
It reports a timed pause as active until a valid step removes it. Returned
objects do not share mutable state with the engine. Reopening and compaction
preserve the meaning of all controls, counts, and future dispatch decisions.

For Request 4, the post-compaction bound becomes `65536 + 8 * (R + P)`, where
`R` is defined in Request 1 and `P` is the sum of UTF-8 byte lengths of
`JSON.stringify({destination, until})` over active pauses. The repeated-compaction
bound still applies, now to unchanged event and pause state.

All new malformed requests return `{error:'invalid'}` without changes. These
controls have the same successful-reply durability and explicit I/O-error rules
as existing requests. An interrupted step may commit individual pause removals
and event transitions independently; its successful reply commits all of them.

Use Node's standard `node:test` runner for your tests, runnable with `node --test`.
Add focused tests and run existing and new tests. Commit this request separately.
Finish with the files changed and the tests you ran.
