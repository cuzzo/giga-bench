// Durable ordered-delivery event store.
//
// Persistence design
// ------------------
// A single generation-numbered data file (`data-<g>`) holds a full snapshot of
// the state followed by append-only, checksummed mutation records. Mutations only
// append a small record and fsync, so sustained ingestion/delivery is O(delta)
// per request. `compact` writes a fresh `data-<g+1>` containing only a snapshot
// of the current state and removes older generations. Recovery picks the highest
// generation whose snapshot parses and replays its valid record prefix, so a torn
// append is always ignored.
//
// File layout:
//   [8 bytes magic][u32 snapshot length][u32 snapshot crc][snapshot utf8 json]
//   repeat: [u32 record length][u32 record crc][record utf8 json]
//
// Every mutation that returns success has been fsynced before the reply.

const MAGIC = Buffer.from('EVST0002', 'ascii');

// ---------------------------------------------------------------------------
// Small utilities
// ---------------------------------------------------------------------------

const CRC_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) {
      c = (c & 1) ? (0xedb88320 ^ (c >>> 1)) : (c >>> 1);
    }
    table[n] = c >>> 0;
  }
  return table;
})();

function crc32(buf) {
  let c = 0xffffffff;
  for (let i = 0; i < buf.length; i++) {
    c = CRC_TABLE[(c ^ buf[i]) & 0xff] ^ (c >>> 8);
  }
  return (c ^ 0xffffffff) >>> 0;
}

function validString(value) {
  return typeof value === 'string' && value.length > 0;
}

function isSafeNonNegInt(value) {
  return Number.isSafeInteger(value) && value >= 0;
}

// Deadlines are sums of safe integers and may exceed Number.MAX_SAFE_INTEGER.
// They are still integer-valued floats and must be preserved across reopen, so
// this is deliberately looser than isSafeNonNegInt.
function isNonNegInt(value) {
  return typeof value === 'number' && Number.isInteger(value) && value >= 0;
}

function isSafePosInt(value) {
  return Number.isSafeInteger(value) && value > 0;
}

function positiveIntOr(value, fallback) {
  return isSafePosInt(value) ? value : fallback;
}

function isPlainObject(value) {
  if (value === null || typeof value !== 'object') return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

function isTerminal(state) {
  return state === 'acked' || state === 'dead' || state === 'cancelled';
}

function isValidOutcomeCode(code) {
  if (Number.isSafeInteger(code)) return code >= 100 && code <= 599;
  return code === 'timeout' || code === 'reset' || code === 'malformed';
}

function createStream(name) {
  return { stream: name, events: new Map(), bySeq: [], headSeq: 1, readyDest: null, backlog: 0, acked: 0 };
}

// Simple binary min-heaps keyed by an entry's `deadline`.
function heapPush(heap, entry) {
  heap.push(entry);
  let i = heap.length - 1;
  while (i > 0) {
    const parent = (i - 1) >> 1;
    if (heap[parent].deadline <= heap[i].deadline) break;
    const tmp = heap[parent];
    heap[parent] = heap[i];
    heap[i] = tmp;
    i = parent;
  }
}

function heapPop(heap) {
  const top = heap[0];
  const last = heap.pop();
  if (heap.length > 0) {
    heap[0] = last;
    let i = 0;
    for (;;) {
      const left = 2 * i + 1;
      const right = 2 * i + 2;
      let smallest = i;
      if (left < heap.length && heap[left].deadline < heap[smallest].deadline) smallest = left;
      if (right < heap.length && heap[right].deadline < heap[smallest].deadline) smallest = right;
      if (smallest === i) break;
      const tmp = heap[smallest];
      heap[smallest] = heap[i];
      heap[i] = tmp;
      i = smallest;
    }
  }
  return top;
}

// Returns a canonical, deeply cloned value with object keys sorted. Throws for
// anything that is not a JSON value (functions, undefined, cycles, NaN,
// BigInt, Date, Map, sparse arrays, ...).
function canonicalizeJson(value, seen) {
  if (value === null) return null;
  const type = typeof value;
  if (type === 'boolean' || type === 'string') return value;
  if (type === 'number') {
    if (!Number.isFinite(value)) throw new TypeError('non-finite number');
    return value;
  }
  if (type === 'object') {
    if (seen.has(value)) throw new TypeError('cyclic value');
    seen.add(value);
    let out;
    if (Array.isArray(value)) {
      out = [];
      for (let i = 0; i < value.length; i++) {
        if (!Object.hasOwn(value, i)) throw new TypeError('sparse array');
        out.push(canonicalizeJson(value[i], seen));
      }
    } else {
      if (!isPlainObject(value)) throw new TypeError('non-plain object');
      out = Object.create(null);
      for (const key of Object.keys(value).sort()) {
        out[key] = canonicalizeJson(value[key], seen);
      }
    }
    seen.delete(value);
    return out;
  }
  throw new TypeError('unsupported JSON value');
}

function canonicalPayload(payload) {
  return JSON.stringify(canonicalizeJson(payload, new Set()));
}

// ---------------------------------------------------------------------------
// Data-file encoding / decoding
// ---------------------------------------------------------------------------

function encodeFrame(json) {
  const payload = Buffer.from(json, 'utf8');
  const header = Buffer.alloc(8);
  header.writeUInt32LE(payload.length, 0);
  header.writeUInt32LE(crc32(payload), 4);
  return Buffer.concat([header, payload]);
}

function encodeSnapshot(streams, lastNow, pauses) {
  const out = [];
  for (const [streamName, stream] of streams) {
    const events = stream.bySeq.map((event) => [
      event.key,
      event.state,
      event.attempts,
      event.reason,
      event.destination,
      JSON.parse(event.payload),
      event.deadline,
      event.allowanceBase,
    ]);
    out.push([streamName, events]);
  }
  const json = JSON.stringify({
    format: 2,
    lastNow,
    streams: out,
    pauses: Array.from(pauses.entries()),
  });
  return Buffer.concat([MAGIC, encodeFrame(json)]);
}

function encodeRecords(records) {
  return Buffer.concat(records.map((record) => encodeFrame(JSON.stringify(record))));
}

function parseDataFile(input) {
  const buf = Buffer.isBuffer(input) ? input : Buffer.from(input);
  if (buf.length < 16 || !buf.subarray(0, 8).equals(MAGIC)) return null;

  let offset = 8;
  const frames = [];
  while (offset + 8 <= buf.length) {
    const length = buf.readUInt32LE(offset);
    const crc = buf.readUInt32LE(offset + 4);
    if (length === 0 || offset + 8 + length > buf.length) break;
    const slice = buf.subarray(offset + 8, offset + 8 + length);
    if (crc32(slice) !== crc) break;
    frames.push(slice.toString('utf8'));
    offset += 8 + length;
  }
  if (frames.length === 0) return null;

  return {
    snapshotJson: frames[0],
    records: frames.slice(1),
    validEnd: offset,
  };
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

export default class Engine {
  constructor(fs, options = {}) {
    this.fs = fs;
    this.options = options || {};
    this.leaseMs = positiveIntOr(this.options.leaseMs, 10);
    this.baseMs = positiveIntOr(this.options.baseMs, 4);
    this.capMs = positiveIntOr(this.options.capMs, 16);
    this.maxAttempts = positiveIntOr(this.options.maxAttempts, 3);
    this.maxInFlight = positiveIntOr(this.options.maxInFlight, 4);
    this.perDestination = positiveIntOr(this.options.perDestination, 2);

    this.broken = false;
    this._load();
  }

  // -------------------------------------------------------------------------
  // Loading & Recovery
  // -------------------------------------------------------------------------

  _resetState(lastNow = 0, pauses = new Map()) {
    this.streams = new Map();
    this.pauses = pauses;
    this.lastNow = lastNow;
    this.readyByDest = new Map();
    this.destinations = new Map();
    this.stateCounts = { pending: 0, inflight: 0, acked: 0, dead: 0, cancelled: 0 };
    this.leaseHeap = [];
    this.waitingHeap = [];
  }

  _loadDataFile(buf) {
    const parsed = parseDataFile(buf);
    if (!parsed) return false;
    try {
      const obj = JSON.parse(parsed.snapshotJson);
      if (!obj || obj.format !== 2 || !isSafeNonNegInt(obj.lastNow) || !Array.isArray(obj.streams)) {
        return false;
      }
      this._resetState(obj.lastNow, new Map(Array.isArray(obj.pauses) ? obj.pauses : []));
      for (const [streamName, events] of obj.streams) {
        const stream = createStream(streamName);
        this.streams.set(streamName, stream);
        for (let i = 0; i < events.length; i++) {
          const [key, state, attempts, reason, destination, payloadObj, deadline = 0, allowanceBase = 0] = events[i];
          const event = {
            stream: streamName,
            key,
            seq: i + 1,
            destination,
            payload: JSON.stringify(payloadObj),
            state,
            attempts,
            reason,
            deadline: isNonNegInt(deadline) ? deadline : 0,
            allowanceBase: isNonNegInt(allowanceBase) ? allowanceBase : 0,
          };
          stream.events.set(key, event);
          stream.bySeq.push(event);
          this._countEvent(event);
          if (state === 'inflight' && event.deadline > 0) {
            heapPush(this.leaseHeap, { event, deadline: event.deadline });
          }
        }
        this._advanceHead(stream);
      }
      for (const recordJson of parsed.records) {
        this._applyRecord(JSON.parse(recordJson));
      }
      this.appendOffset = parsed.validEnd;
      return true;
    } catch {
      return false;
    }
  }

  _load() {
    const list = this.fs('list');
    const dataFiles = [];
    let maxGen = 0;
    for (const name of list) {
      if (typeof name !== 'string') continue;
      const match = /^data-(\d+)$/.exec(name);
      if (match) {
        const gen = parseInt(match[1], 10);
        if (gen > maxGen) maxGen = gen;
        dataFiles.push({ name, gen });
      }
    }
    dataFiles.sort((a, b) => b.gen - a.gen);

    for (const { name, gen } of dataFiles) {
      let buf;
      try {
        buf = this.fs('read', { name });
      } catch (err) {
        if (err && err.code === 'ENOENT') continue;
        throw err;
      }
      if (this._loadDataFile(buf)) {
        this.currentGen = gen;
        this.dataName = name;
        return;
      }
    }

    // No usable generation: initialize a fresh one. An unreadable (I/O error)
    // generation above would have propagated instead of reaching this point.
    this.currentGen = maxGen + 1;
    this.dataName = 'data-' + this.currentGen;
    this._resetState();

    const buf = encodeSnapshot(this.streams, this.lastNow, this.pauses);
    this._writeAt(this.dataName, 0, buf);
    this.fs('truncate', { name: this.dataName, size: buf.length });
    this.fs('sync', { name: this.dataName });
    this.fs('sync');
    this.appendOffset = buf.length;
  }

  _countEvent(event) {
    const state = event.state;
    this.stateCounts[state]++;
    const stream = this.streams.get(event.stream);
    if (!isTerminal(state)) stream.backlog++;
    else if (state === 'acked') stream.acked++;

    let dest = this.destinations.get(event.destination);
    if (!dest) {
      dest = { pending: 0, inflight: 0 };
      this.destinations.set(event.destination, dest);
    }
    if (state === 'pending') dest.pending++;
    else if (state === 'inflight') dest.inflight++;
  }

  // -------------------------------------------------------------------------
  // State transitions
  // -------------------------------------------------------------------------

  _find(stream, key) {
    const entry = this.streams.get(stream);
    return entry ? entry.events.get(key) : undefined;
  }

  _receipt(event) {
    return {
      seq: event.seq,
      state: event.state,
      attempts: event.attempts,
      reason: event.reason,
    };
  }

  _markReady(stream, destination) {
    stream.readyDest = destination;
    let set = this.readyByDest.get(destination);
    if (!set) {
      set = new Set();
      this.readyByDest.set(destination, set);
    }
    set.add(stream);
  }

  _unmarkReady(stream) {
    if (!stream.readyDest) return;
    const set = this.readyByDest.get(stream.readyDest);
    if (set) {
      set.delete(stream);
      if (set.size === 0) this.readyByDest.delete(stream.readyDest);
    }
    stream.readyDest = null;
  }

  _advanceHead(stream) {
    this._unmarkReady(stream);
    while (stream.headSeq <= stream.bySeq.length) {
      const event = stream.bySeq[stream.headSeq - 1];
      if (isTerminal(event.state)) stream.headSeq++;
      else break;
    }
    const head = stream.bySeq[stream.headSeq - 1];
    if (head && head.state === 'pending') {
      if (head.deadline <= this.lastNow) {
        this._markReady(stream, head.destination);
      } else {
        heapPush(this.waitingHeap, { event: head, deadline: head.deadline });
      }
    }
  }

  _transition(event, newState, reason = null, deadline = 0) {
    const oldState = event.state;
    if (oldState !== newState) {
      this.stateCounts[oldState]--;
      this.stateCounts[newState]++;

      const stream = this.streams.get(event.stream);
      if (!isTerminal(oldState)) stream.backlog--;
      else if (oldState === 'acked') stream.acked--;
      if (!isTerminal(newState)) stream.backlog++;
      else if (newState === 'acked') stream.acked++;

      const dest = this.destinations.get(event.destination);
      if (oldState === 'pending') dest.pending--;
      else if (oldState === 'inflight') dest.inflight--;
      if (newState === 'pending') dest.pending++;
      else if (newState === 'inflight') dest.inflight++;

      event.state = newState;
    }

    event.reason = reason;
    event.deadline = deadline;
    if (newState === 'inflight') {
      heapPush(this.leaseHeap, { event, deadline });
    }

    const stream = this.streams.get(event.stream);
    if (!isTerminal(newState) && event.seq < stream.headSeq) {
      stream.headSeq = event.seq;
    }
    if (event.seq <= stream.headSeq) {
      this._advanceHead(stream);
    }
  }

  // Applies a retryable failure of `attemptNumber` at `failureTime`. Either
  // exhausts the event or puts it back to pending with a stored retry deadline.
  // Exhaustion and backoff are relative to the current redrive allowance.
  _handleRetryableFailure(event, attemptNumber, failureTime, code, retryAfterMs, jitterMs, records) {
    const relative = attemptNumber - event.allowanceBase;
    if (relative >= this.maxAttempts) {
      this._record(records, { t: 'dead', s: event.stream, k: event.key, r: 'exhausted' });
      return;
    }
    let delay = Math.min(this.capMs, this.baseMs * 2 ** (relative - 1)) + jitterMs;
    if (code === 429 && isSafeNonNegInt(retryAfterMs)) {
      delay = Math.max(delay, retryAfterMs);
    }
    this._record(records, { t: 'retry', s: event.stream, k: event.key, b: failureTime + delay });
  }

  _expireLeases(now, jitterMs, records) {
    while (this.leaseHeap.length > 0 && this.leaseHeap[0].deadline <= now) {
      const entry = heapPop(this.leaseHeap);
      const event = entry.event;
      if (event.state !== 'inflight' || event.deadline !== entry.deadline) continue;
      this._handleRetryableFailure(event, event.attempts, entry.deadline, null, undefined, jitterMs, records);
    }
  }

  _promoteWaiting(now) {
    while (this.waitingHeap.length > 0 && this.waitingHeap[0].deadline <= now) {
      const entry = heapPop(this.waitingHeap);
      const event = entry.event;
      if (event.state !== 'pending' || event.deadline !== entry.deadline) continue;
      const stream = this.streams.get(event.stream);
      if (stream && stream.bySeq[stream.headSeq - 1] === event) {
        this._markReady(stream, event.destination);
      }
    }
  }

  _applyRecord(record) {
    switch (record.t) {
      case 'put': {
        let stream = this.streams.get(record.s);
        if (!stream) {
          stream = createStream(record.s);
          this.streams.set(record.s, stream);
        }
        const existing = stream.events.get(record.k);
        if (existing) return existing;
        const event = {
          stream: record.s,
          key: record.k,
          seq: record.seq,
          destination: record.d,
          payload: record.p,
          state: 'pending',
          attempts: 0,
          reason: null,
          deadline: 0,
          allowanceBase: 0,
        };
        stream.events.set(record.k, event);
        stream.bySeq.push(event);
        this._countEvent(event);
        if (event.seq <= stream.headSeq) this._advanceHead(stream);
        return event;
      }
      case 'cancel': {
        const event = this._find(record.s, record.k);
        if (event) this._transition(event, 'cancelled');
        return event;
      }
      case 'attempt': {
        const event = this._find(record.s, record.k);
        if (event) {
          event.attempts = record.n;
          this._transition(event, 'inflight', null, isNonNegInt(record.u) ? record.u : 0);
        }
        return event;
      }
      case 'ack': {
        const event = this._find(record.s, record.k);
        if (event) this._transition(event, 'acked');
        return event;
      }
      case 'dead': {
        const event = this._find(record.s, record.k);
        if (event) this._transition(event, 'dead', record.r);
        return event;
      }
      case 'retry': {
        const event = this._find(record.s, record.k);
        if (event) this._transition(event, 'pending', null, isNonNegInt(record.b) ? record.b : 0);
        return event;
      }
      case 'requeue': {
        const event = this._find(record.s, record.k);
        if (event) {
          event.allowanceBase = event.attempts;
          this._transition(event, 'pending');
        }
        return event;
      }
      case 'pause': {
        this.pauses.set(record.d, record.u === undefined ? null : record.u);
        return null;
      }
      case 'resume': {
        this.pauses.delete(record.d);
        return null;
      }
      case 'now': {
        if (isSafeNonNegInt(record.n)) {
          this.lastNow = record.n;
          this._promoteWaiting(this.lastNow);
        }
        return null;
      }
      default:
        return null;
      }
  }

  _record(records, record) {
    const result = this._applyRecord(record);
    records.push(record);
    return result;
  }

  // -------------------------------------------------------------------------
  // Persistence primitives
  // -------------------------------------------------------------------------

  _writeAt(name, offset, data) {
    let written = 0;
    while (written < data.length) {
      const n = this.fs('write', { name, offset: offset + written, data: data.subarray(written) });
      if (typeof n !== 'number' || n <= 0) {
        const err = new Error('write made no progress');
        err.code = 'EIO';
        throw err;
      }
      written += Math.min(n, data.length - written);
    }
  }

  _appendRecords(records) {
    const data = encodeRecords(records);
    const offset = this.appendOffset;
    this._writeAt(this.dataName, offset, data);
    this.fs('truncate', { name: this.dataName, size: offset + data.length });
    this.fs('sync', { name: this.dataName });
    this.appendOffset = offset + data.length;
  }

  _commit(records) {
    try {
      this._appendRecords(records);
      return null;
    } catch (err) {
      return this._handlePersistError(err);
    }
  }

  // After a failed append, rebuild in-memory state from the valid prefix of the
  // live data file and re-sync it. This preserves all previously confirmed work
  // and resolves the uncertain append deterministically (it may have committed).
  _recoverFromFile() {
    const buf = this.fs('read', { name: this.dataName });
    if (!this._loadDataFile(buf)) throw new Error('recovery failed');
    this.fs('sync', { name: this.dataName });
  }

  _handlePersistError(err) {
    if (err && (err.code === 'EIO' || err.code === 'ENOSPC')) {
      try {
        this._recoverFromFile();
      } catch {
        this.broken = true;
      }
      return { error: 'io' };
    }
    this.broken = true;
    throw err;
  }

  _ioOrThrow(err) {
    if (err && (err.code === 'EIO' || err.code === 'ENOSPC')) return { error: 'io' };
    throw err;
  }

  // -------------------------------------------------------------------------
  // Request dispatch
  // -------------------------------------------------------------------------

  handle(request) {
    if (this.broken) return { error: 'io' };
    if (request === null || typeof request !== 'object' || Array.isArray(request)) {
      return { error: 'invalid' };
    }
    switch (request.op) {
      case 'put': return this._put(request);
      case 'get': return this._get(request);
      case 'cancel': return this._cancel(request);
      case 'compact': return this._compact();
      case 'step': return this._step(request);
      case 'pause': return this._pause(request);
      case 'resume': return this._resume(request);
      case 'requeue': return this._requeue(request);
      case 'stats': return this._stats();
      default: return { error: 'invalid' };
    }
  }

  // -------------------------------------------------------------------------
  // Request 1 operations
  // -------------------------------------------------------------------------

  _put(req) {
    const { stream, key, destination, payload } = req;
    if (!validString(stream) || !validString(key) || !validString(destination) || !isPlainObject(payload)) {
      return { error: 'invalid' };
    }

    let payloadStr;
    try {
      payloadStr = canonicalPayload(payload);
    } catch {
      return { error: 'invalid' };
    }

    const entry = this.streams.get(stream);
    if (entry) {
      const existing = entry.events.get(key);
      if (existing) {
        if (existing.destination === destination && existing.payload === payloadStr) {
          return this._receipt(existing);
        }
        return { error: 'conflict' };
      }
    }

    const seq = entry ? entry.bySeq.length + 1 : 1;
    const record = { t: 'put', s: stream, k: key, seq, d: destination, p: payloadStr };
    const event = this._applyRecord(record);
    return this._commit([record]) || this._receipt(event);
  }

  _get(req) {
    if (!validString(req.stream) || !validString(req.key)) {
      return { error: 'invalid' };
    }
    const event = this._find(req.stream, req.key);
    if (!event) return { error: 'not_found' };
    return this._receipt(event);
  }

  _cancel(req) {
    if (!validString(req.stream) || !validString(req.key)) {
      return { error: 'invalid' };
    }
    const event = this._find(req.stream, req.key);
    if (!event) return { error: 'not_found' };
    if (event.state === 'inflight') return { error: 'busy' };

    if (event.state === 'pending') {
      const record = { t: 'cancel', s: req.stream, k: req.key };
      this._applyRecord(record);
      return this._commit([record]) || this._receipt(event);
    }
    return this._receipt(event);
  }

  // -------------------------------------------------------------------------
  // Request 4: operator controls
  // -------------------------------------------------------------------------

  _pause(req) {
    const destination = req.destination;
    if (!validString(destination)) return { error: 'invalid' };
    const until = req.until === undefined ? null : req.until;
    if (until !== null && !isSafeNonNegInt(until)) return { error: 'invalid' };

    const record = { t: 'pause', d: destination, u: until };
    this._applyRecord(record);
    return this._commit([record]) || { destination, paused: true };
  }

  _resume(req) {
    const destination = req.destination;
    if (!validString(destination)) return { error: 'invalid' };

    const record = { t: 'resume', d: destination };
    this._applyRecord(record);
    return this._commit([record]) || { destination, paused: false };
  }

  _requeue(req) {
    if (!validString(req.stream) || !validString(req.key)) return { error: 'invalid' };
    const event = this._find(req.stream, req.key);
    if (!event) return { error: 'not_found' };
    if (event.state !== 'dead') return { error: 'not_dead' };
    const stream = this.streams.get(req.stream);
    const head = stream.bySeq[stream.headSeq - 1];
    if (head && head.state === 'inflight') return { error: 'busy' };

    const record = { t: 'requeue', s: req.stream, k: req.key };
    this._applyRecord(record);
    return this._commit([record]) || this._receipt(event);
  }

  _removeExpiredPauses(now, records) {
    for (const [destination, until] of this.pauses) {
      if (until !== null && until <= now) {
        this._record(records, { t: 'resume', d: destination });
      }
    }
  }

  _stats() {
    const states = { ...this.stateCounts };
    const streams = Object.fromEntries(
      Array.from(this.streams, ([name, s]) => [
        name,
        {
          head: s.headSeq <= s.bySeq.length ? s.headSeq : null,
          backlog: s.backlog,
          acked: s.acked,
        },
      ]),
    );
    const destNames = new Set([...this.destinations.keys(), ...this.pauses.keys()]);
    const destinations = Object.fromEntries(
      Array.from(destNames, (name) => {
        const d = this.destinations.get(name);
        return [
          name,
          {
            pending: d ? d.pending : 0,
            inflight: d ? d.inflight : 0,
            paused: this.pauses.has(name),
          },
        ];
      }),
    );
    return { states, streams, destinations };
  }

  _compact() {
    const newGen = this.currentGen + 1;
    const name = 'data-' + newGen;
    let buf;
    try {
      buf = encodeSnapshot(this.streams, this.lastNow, this.pauses);
      this._writeAt(name, 0, buf);
      this.fs('truncate', { name, size: buf.length });
      this.fs('sync', { name });
      this.fs('sync');
    } catch (err) {
      // Under the uncertain-I/O rule the new generation may have taken effect.
      // Remove it and make the removal durable so a partly-surviving higher
      // generation can never shadow the current one after a later power loss.
      try {
        this.fs('unlink', { name });
      } catch (cleanupErr) {
        if (!cleanupErr || cleanupErr.code !== 'ENOENT') this.broken = true;
      }
      try {
        this.fs('sync');
      } catch {
        this.broken = true;
      }
      return this._ioOrThrow(err);
    }

    this.currentGen = newGen;
    this.dataName = name;
    this.appendOffset = buf.length;

    try {
      const list = this.fs('list');
      for (const file of list) {
        if (file !== name && /^data-\d+$/.test(file)) {
          try {
            this.fs('unlink', { name: file });
          } catch (err) {
            if (err && err.code === 'ENOENT') continue;
            throw err;
          }
        }
      }
      this.fs('sync');
    } catch (err) {
      return this._ioOrThrow(err);
    }
    return {};
  }

  // -------------------------------------------------------------------------
  // Request 2 & 3: delivery step
  // -------------------------------------------------------------------------

  _step(req) {
    const now = req.now;
    if (!isSafeNonNegInt(now)) return { error: 'invalid' };

    const budget = req.budget === undefined ? 100 : req.budget;
    if (!isSafeNonNegInt(budget)) return { error: 'invalid' };

    const jitterMs = req.jitterMs === undefined ? 0 : req.jitterMs;
    if (!isSafeNonNegInt(jitterMs) || jitterMs > this.baseMs) return { error: 'invalid' };

    const results = req.results === undefined ? [] : req.results;
    if (!Array.isArray(results)) return { error: 'invalid' };
    for (const result of results) {
      if (result === null || typeof result !== 'object' || Array.isArray(result)) return { error: 'invalid' };
      if (!validString(result.stream) || !validString(result.key)) return { error: 'invalid' };
      if (!isSafePosInt(result.attempt) || !isValidOutcomeCode(result.code)) return { error: 'invalid' };
    }

    if (now < this.lastNow) return { error: 'invalid' };

    const records = [];
    if (now > this.lastNow) {
      this._record(records, { t: 'now', n: now });
    }

    this._removeExpiredPauses(now, records);
    this._expireLeases(now, jitterMs, records);

    for (const result of results) {
      this._applyResult(result, jitterMs, records);
    }

    const attempts = [];
    this._selectWork(now, budget, records, attempts);

    if (records.length > 0) {
      const err = this._commit(records);
      if (err) return err;
    }

    return attempts;
  }

  _applyResult(result, jitterMs, records) {
    const event = this._find(result.stream, result.key);
    if (!event || event.state !== 'inflight' || event.attempts !== result.attempt) return;

    const code = result.code;
    if (typeof code === 'number' && code >= 200 && code <= 299) {
      this._record(records, { t: 'ack', s: result.stream, k: result.key });
    } else if (typeof code === 'number' && code >= 300 && code <= 499 && code !== 429) {
      this._record(records, { t: 'dead', s: result.stream, k: result.key, r: 'http:' + code });
    } else {
      this._handleRetryableFailure(event, result.attempt, this.lastNow, code, result.retryAfterMs, jitterMs, records);
    }
  }

  _selectWork(now, budget, records, attempts) {
    while (budget > 0 && this.stateCounts.inflight < this.maxInFlight) {
      let chosen = null;
      for (const [destination, streams] of this.readyByDest) {
        if (this.pauses.has(destination)) continue;
        if (this.destinations.get(destination).inflight >= this.perDestination) continue;
        const stream = streams.values().next().value;
        chosen = stream.bySeq[stream.headSeq - 1];
        break;
      }
      if (!chosen) break;

      const attemptNumber = chosen.attempts + 1;
      const leaseUntil = now + this.leaseMs;
      this._record(records, { t: 'attempt', s: chosen.stream, k: chosen.key, n: attemptNumber, u: leaseUntil });
      attempts.push({
        stream: chosen.stream,
        key: chosen.key,
        seq: chosen.seq,
        attempt: attemptNumber,
        destination: chosen.destination,
        payload: JSON.parse(chosen.payload),
      });
      budget--;
    }
  }
}
