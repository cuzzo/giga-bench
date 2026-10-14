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
  return Number.isSafeInteger(value) && value > 0 ? value : fallback;
}

function isPlainObject(value) {
  if (value === null || typeof value !== 'object') return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

function isTerminal(state) {
  return state === 'acked' || state === 'dead' || state === 'cancelled';
}

function incMap(map, key) {
  map.set(key, (map.get(key) || 0) + 1);
}

function decMap(map, key) {
  const count = map.get(key) || 0;
  if (count <= 1) map.delete(key);
  else map.set(key, count - 1);
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
        out.push(canonicalizeJson(value[i], seen));
      }
    } else {
      if (!isPlainObject(value)) throw new TypeError('non-plain object');
      out = Object.create(null);
      const keys = Object.keys(value).sort();
      for (const key of keys) {
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

function encodeSnapshot(state, lastNow, pauses) {
  const json = serializeState(state, lastNow, pauses);
  const payload = Buffer.from(json, 'utf8');
  const header = Buffer.alloc(16);
  MAGIC.copy(header, 0);
  header.writeUInt32LE(payload.length, 8);
  header.writeUInt32LE(crc32(payload), 12);
  return Buffer.concat([header, payload]);
}

function parseDataFile(input) {
  const buf = Buffer.isBuffer(input) ? input : Buffer.from(input);
  if (buf.length < 16) return null;
  if (!buf.subarray(0, 8).equals(MAGIC)) return null;
  const snapshotLength = buf.readUInt32LE(8);
  const snapshotCrc = buf.readUInt32LE(12);
  if (snapshotLength === 0 || buf.length < 16 + snapshotLength) return null;
  const snapshotBuf = buf.subarray(16, 16 + snapshotLength);
  if (crc32(snapshotBuf) !== snapshotCrc) return null;

  let offset = 16 + snapshotLength;
  const records = [];
  while (offset + 8 <= buf.length) {
    const recordLength = buf.readUInt32LE(offset);
    const recordCrc = buf.readUInt32LE(offset + 4);
    if (recordLength === 0) break;
    if (offset + 8 + recordLength > buf.length) break;
    const recordBuf = buf.subarray(offset + 8, offset + 8 + recordLength);
    if (crc32(recordBuf) !== recordCrc) break;
    records.push(recordBuf.toString('utf8'));
    offset += 8 + recordLength;
  }

  return {
    snapshotJson: snapshotBuf.toString('utf8'),
    records,
    validEnd: offset,
  };
}

function encodeRecords(records) {
  const parts = [];
  for (const record of records) {
    const payload = Buffer.from(JSON.stringify(record), 'utf8');
    const header = Buffer.alloc(8);
    header.writeUInt32LE(payload.length, 0);
    header.writeUInt32LE(crc32(payload), 4);
    parts.push(header, payload);
  }
  return Buffer.concat(parts);
}

// ---------------------------------------------------------------------------
// Snapshot serialization
// ---------------------------------------------------------------------------

function serializeState(streams, lastNow, pauses) {
  const out = [];
  for (const streamName of Array.from(streams.keys()).sort()) {
    const stream = streams.get(streamName);
    const events = [];
    for (let i = 0; i < stream.maxSeq; i++) {
      const event = stream.bySeq[i];
      if (!event) {
        events.push(null);
        continue;
      }
      events.push([
        event.key,
        event.state,
        event.attempts,
        event.reason,
        event.destination,
        JSON.parse(event.payload),
        event.leaseUntil || 0,
        event.notBefore || 0,
        event.allowanceBase || 0,
      ]);
    }
    out.push([streamName, events]);
  }
  const pauseList = [];
  for (const destination of Array.from(pauses.keys()).sort()) {
    pauseList.push([destination, pauses.get(destination)]);
  }
  return JSON.stringify({ format: 2, lastNow, streams: out, pauses: pauseList });
}

function parseSnapshot(json) {
  const obj = JSON.parse(json);
  if (obj === null || typeof obj !== 'object' || obj.format !== 2) {
    throw new Error('invalid snapshot');
  }
  if (!isSafeNonNegInt(obj.lastNow) || !Array.isArray(obj.streams)) {
    throw new Error('invalid snapshot');
  }
  const streams = new Map();
  for (const entry of obj.streams) {
    if (!Array.isArray(entry) || entry.length !== 2 || typeof entry[0] !== 'string' || !Array.isArray(entry[1])) {
      throw new Error('invalid snapshot');
    }
    const [streamName, events] = entry;
    const stream = { stream: streamName, events: new Map(), bySeq: [], headSeq: 1, maxSeq: 0, readyDest: null, pending: 0, inflight: 0, acked: 0 };
    for (let i = 0; i < events.length; i++) {
      const item = events[i];
      if (item === null || item === undefined) continue;
      if (!Array.isArray(item) || item.length < 6) throw new Error('invalid snapshot');
      const [key, state, attempts, reason, destination, payloadObj, leaseUntil = 0, notBefore = 0, allowanceBase = 0] = item;
      if (typeof key !== 'string' || !validString(key)) throw new Error('invalid snapshot');
      if (typeof state !== 'string') throw new Error('invalid snapshot');
      if (!isSafeNonNegInt(attempts)) throw new Error('invalid snapshot');
      if (reason !== null && typeof reason !== 'string') throw new Error('invalid snapshot');
      if (typeof destination !== 'string' || !validString(destination)) throw new Error('invalid snapshot');
      const payload = JSON.stringify(payloadObj);
      const event = {
        stream: streamName,
        key,
        seq: i + 1,
        destination,
        payload,
        state,
        attempts,
        reason,
        leaseUntil: isNonNegInt(leaseUntil) ? leaseUntil : 0,
        notBefore: isNonNegInt(notBefore) ? notBefore : 0,
        allowanceBase: isNonNegInt(allowanceBase) ? allowanceBase : 0,
      };
      stream.events.set(key, event);
      stream.bySeq[i] = event;
      if (i + 1 > stream.maxSeq) stream.maxSeq = i + 1;
    }
    streams.set(streamName, stream);
  }
  const pauses = new Map();
  if (obj.pauses !== undefined) {
    if (!Array.isArray(obj.pauses)) throw new Error('invalid snapshot');
    for (const pause of obj.pauses) {
      if (!Array.isArray(pause) || pause.length !== 2) throw new Error('invalid snapshot');
      const [destination, until] = pause;
      if (typeof destination !== 'string' || !validString(destination)) throw new Error('invalid snapshot');
      if (until !== null && !isSafeNonNegInt(until)) throw new Error('invalid snapshot');
      pauses.set(destination, until);
    }
  }
  return { streams, lastNow: obj.lastNow, pauses };
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

export default class Engine {
  constructor(fs, options) {
    this.fs = fs;
    this.options = options || {};
    this.leaseMs = positiveIntOr(this.options.leaseMs, 10);
    this.baseMs = positiveIntOr(this.options.baseMs, 4);
    this.capMs = positiveIntOr(this.options.capMs, 16);
    this.maxAttempts = positiveIntOr(this.options.maxAttempts, 3);
    this.maxInFlight = positiveIntOr(this.options.maxInFlight, 4);
    this.perDestination = positiveIntOr(this.options.perDestination, 2);

    this.broken = false;
    this.streams = new Map();
    this.pauses = new Map();
    this.lastNow = 0;
    this.currentGen = 1;
    this.dataName = null;
    this.appendOffset = 0;
    this.readyByDest = new Map();
    this.destPending = new Map();
    this.destInFlight = new Map();
    this.destEvents = new Map();
    this.stateCounts = { pending: 0, inflight: 0, acked: 0, dead: 0, cancelled: 0 };
    this.leaseHeap = [];
    this.waitingHeap = [];
    this.stepJitter = 0;

    this._load();
  }

  // -------------------------------------------------------------------------
  // Loading
  // -------------------------------------------------------------------------

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
      const parsed = parseDataFile(buf);
      if (!parsed) continue;

      let snapshot;
      try {
        snapshot = parseSnapshot(parsed.snapshotJson);
      } catch (err) {
        continue;
      }

      this.streams = snapshot.streams;
      this.lastNow = snapshot.lastNow;
      this.pauses = snapshot.pauses;
      this.currentGen = gen;
      this.dataName = name;
      this.appendOffset = parsed.validEnd;
      this._rebuildAux();

      let ok = true;
      for (const recordJson of parsed.records) {
        let record;
        try {
          record = JSON.parse(recordJson);
        } catch (err) {
          ok = false;
          break;
        }
        try {
          this._applyRecord(record);
        } catch (err) {
          ok = false;
          break;
        }
      }
      if (ok) return;

      this.streams = new Map();
      this.pauses = new Map();
      this.lastNow = 0;
      this._rebuildAux();
    }

    // No usable generation: initialize a fresh one. An unreadable (I/O error)
    // generation above would have propagated instead of reaching this point.
    const gen = maxGen + 1;
    this.currentGen = gen;
    this.dataName = 'data-' + gen;
    this.streams = new Map();
    this.pauses = new Map();
    this.lastNow = 0;
    this._rebuildAux();

    const buf = encodeSnapshot(this.streams, this.lastNow, this.pauses);
    this._writeAt(this.dataName, 0, buf);
    this.fs('truncate', { name: this.dataName, size: buf.length });
    this.fs('sync', { name: this.dataName });
    this.fs('sync');
    this.appendOffset = buf.length;
  }

  _rebuildAux() {
    this.readyByDest = new Map();
    this.destPending = new Map();
    this.destInFlight = new Map();
    this.destEvents = new Map();
    this.stateCounts = { pending: 0, inflight: 0, acked: 0, dead: 0, cancelled: 0 };
    this.leaseHeap = [];
    this.waitingHeap = [];

    for (const stream of this.streams.values()) {
      stream.readyDest = null;
      stream.pending = 0;
      stream.inflight = 0;
      stream.acked = 0;
      for (const event of stream.bySeq) {
        if (!event) continue;
        event.leaseQueued = false;
        event.waitingQueued = false;
        this._countEvent(event);
        if (event.state === 'inflight' && event.leaseUntil > 0) {
          event.leaseQueued = true;
          heapPush(this.leaseHeap, { event, deadline: event.leaseUntil });
        }
      }
      stream.headSeq = 1;
      this._advanceHead(stream);
    }
  }

  // Increment every derived counter for a newly seen event.
  _countEvent(event) {
    this.stateCounts[event.state]++;
    const stream = this.streams.get(event.stream);
    if (stream) {
      if (event.state === 'pending') stream.pending++;
      else if (event.state === 'inflight') stream.inflight++;
      else if (event.state === 'acked') stream.acked++;
    }
    if (event.state === 'pending') incMap(this.destPending, event.destination);
    else if (event.state === 'inflight') incMap(this.destInFlight, event.destination);
    incMap(this.destEvents, event.destination);
  }

  // Change an event's state and update every derived counter.
  _setState(event, newState) {
    const oldState = event.state;
    if (oldState === newState) return;
    this.stateCounts[oldState]--;
    this.stateCounts[newState]++;
    const stream = this.streams.get(event.stream);
    if (stream) {
      if (oldState === 'pending') stream.pending--;
      else if (oldState === 'inflight') stream.inflight--;
      else if (oldState === 'acked') stream.acked--;
      if (newState === 'pending') stream.pending++;
      else if (newState === 'inflight') stream.inflight++;
      else if (newState === 'acked') stream.acked++;
    }
    if (oldState === 'pending') decMap(this.destPending, event.destination);
    else if (oldState === 'inflight') decMap(this.destInFlight, event.destination);
    if (newState === 'pending') incMap(this.destPending, event.destination);
    else if (newState === 'inflight') incMap(this.destInFlight, event.destination);
    event.state = newState;
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
    set.add(stream.stream);
  }

  _unmarkReady(stream) {
    if (stream.readyDest === null || stream.readyDest === undefined) return;
    const set = this.readyByDest.get(stream.readyDest);
    if (set) {
      set.delete(stream.stream);
      if (set.size === 0) this.readyByDest.delete(stream.readyDest);
    }
    stream.readyDest = null;
  }

  _advanceHead(stream) {
    this._unmarkReady(stream);
    while (stream.headSeq <= stream.maxSeq) {
      const event = stream.bySeq[stream.headSeq - 1];
      if (event && isTerminal(event.state)) stream.headSeq++;
      else break;
    }
    const head = stream.bySeq[stream.headSeq - 1];
    if (!head) return;
    if (head.state === 'pending') {
      const notBefore = head.notBefore || 0;
      if (notBefore <= this.lastNow) {
        this._markReady(stream, head.destination);
      } else if (!head.waitingQueued) {
        head.waitingQueued = true;
        heapPush(this.waitingHeap, { event: head, deadline: notBefore });
      }
    }
  }

  _setInflight(event, attempt, leaseUntil) {
    if (event.state === 'inflight') {
      event.attempts = attempt;
      event.leaseUntil = leaseUntil;
      event.notBefore = 0;
      return;
    }
    this._setState(event, 'inflight');
    event.attempts = attempt;
    event.leaseUntil = leaseUntil;
    event.notBefore = 0;
    event.waitingQueued = false;
    event.leaseQueued = true;
    heapPush(this.leaseHeap, { event, deadline: leaseUntil });
    const stream = this.streams.get(event.stream);
    if (stream) this._advanceHead(stream);
  }

  _setTerminal(event, state, reason) {
    this._setState(event, state);
    event.reason = reason;
    event.leaseUntil = 0;
    event.notBefore = 0;
    event.leaseQueued = false;
    event.waitingQueued = false;
    const stream = this.streams.get(event.stream);
    if (stream) this._advanceHead(stream);
  }

  // Applies a retryable failure of `attemptNumber` at `failureTime`. Either
  // exhausts the event or puts it back to pending with a stored retry deadline.
  // Exhaustion and backoff are relative to the current redrive allowance.
  _handleRetryableFailure(event, attemptNumber, failureTime, code, retryAfterMs, records) {
    const relative = attemptNumber - (event.allowanceBase || 0);
    if (relative >= this.maxAttempts) {
      this._setTerminal(event, 'dead', 'exhausted');
      records.push({ t: 'dead', s: event.stream, k: event.key, r: 'exhausted' });
      return;
    }
    let delay = Math.min(this.capMs, this.baseMs * 2 ** (relative - 1)) + this.stepJitter;
    if (code === 429 && isSafeNonNegInt(retryAfterMs)) {
      delay = Math.max(delay, retryAfterMs);
    }
    const deadline = failureTime + delay;
    this._setState(event, 'pending');
    event.reason = null;
    event.leaseUntil = 0;
    event.notBefore = deadline;
    event.leaseQueued = false;
    event.waitingQueued = false;
    records.push({ t: 'retry', s: event.stream, k: event.key, b: deadline });
    const stream = this.streams.get(event.stream);
    if (stream) this._advanceHead(stream);
  }

  _expireLeases(now, records) {
    while (this.leaseHeap.length > 0 && this.leaseHeap[0].deadline <= now) {
      const entry = heapPop(this.leaseHeap);
      const event = entry.event;
      if (event.state !== 'inflight' || !event.leaseQueued || event.leaseUntil !== entry.deadline) continue;
      event.leaseQueued = false;
      this._handleRetryableFailure(event, event.attempts, entry.deadline, null, undefined, records);
    }
  }

  _promoteWaiting(now) {
    while (this.waitingHeap.length > 0 && this.waitingHeap[0].deadline <= now) {
      const entry = heapPop(this.waitingHeap);
      const event = entry.event;
      if (event.state !== 'pending' || !event.waitingQueued || event.notBefore !== entry.deadline) continue;
      event.waitingQueued = false;
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
          stream = { stream: record.s, events: new Map(), bySeq: [], headSeq: 1, maxSeq: 0, readyDest: null, pending: 0, inflight: 0, acked: 0 };
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
          leaseUntil: 0,
          notBefore: 0,
          allowanceBase: 0,
          leaseQueued: false,
          waitingQueued: false,
        };
        stream.events.set(record.k, event);
        stream.bySeq[record.seq - 1] = event;
        if (record.seq > stream.maxSeq) stream.maxSeq = record.seq;
        this._countEvent(event);
        this._advanceHead(stream);
        return event;
      }
      case 'cancel': {
        const event = this._find(record.s, record.k);
        if (event) this._setTerminal(event, 'cancelled', null);
        return event;
      }
      case 'attempt': {
        const event = this._find(record.s, record.k);
        if (event) this._setInflight(event, record.n, record.u || 0);
        return event;
      }
      case 'ack': {
        const event = this._find(record.s, record.k);
        if (event) this._setTerminal(event, 'acked', null);
        return event;
      }
      case 'dead': {
        const event = this._find(record.s, record.k);
        if (event) this._setTerminal(event, 'dead', record.r);
        return event;
      }
      case 'retry': {
        const event = this._find(record.s, record.k);
        if (event) {
          this._setState(event, 'pending');
          event.reason = null;
          event.leaseUntil = 0;
          event.notBefore = isNonNegInt(record.b) ? record.b : 0;
          event.leaseQueued = false;
          event.waitingQueued = false;
          const stream = this.streams.get(event.stream);
          if (stream) this._advanceHead(stream);
        }
        return event;
      }
      case 'requeue': {
        const event = this._find(record.s, record.k);
        if (event) {
          this._setState(event, 'pending');
          event.reason = null;
          event.leaseUntil = 0;
          event.notBefore = 0;
          event.leaseQueued = false;
          event.waitingQueued = false;
          event.allowanceBase = event.attempts;
          const stream = this.streams.get(event.stream);
          if (stream) {
            if (event.seq < stream.headSeq) stream.headSeq = event.seq;
            this._advanceHead(stream);
          }
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
        if (isSafeNonNegInt(record.n)) this.lastNow = record.n;
        return null;
      }
      default:
        return null;
    }
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

  // After a failed append, rebuild in-memory state from the valid prefix of the
  // live data file and re-sync it. This preserves all previously confirmed work
  // and resolves the uncertain append deterministically (it may have committed).
  _recoverFromFile() {
    const buf = this.fs('read', { name: this.dataName });
    const parsed = parseDataFile(buf);
    if (!parsed) throw new Error('recovery failed');
    const snapshot = parseSnapshot(parsed.snapshotJson);
    this.streams = snapshot.streams;
    this.lastNow = snapshot.lastNow;
    this.pauses = snapshot.pauses;
    this.appendOffset = parsed.validEnd;
    this._rebuildAux();
    for (const recordJson of parsed.records) {
      this._applyRecord(JSON.parse(recordJson));
    }
    this.fs('sync', { name: this.dataName });
  }

  _handlePersistError(err) {
    if (err && (err.code === 'EIO' || err.code === 'ENOSPC')) {
      try {
        this._recoverFromFile();
      } catch (recoveryErr) {
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
      case 'compact': return this._compact(request);
      case 'step': return this._step(request);
      case 'pause': return this._pause(request);
      case 'resume': return this._resume(request);
      case 'requeue': return this._requeue(request);
      case 'stats': return this._stats(request);
      default: return { error: 'invalid' };
    }
  }

  // -------------------------------------------------------------------------
  // Request 1 operations
  // -------------------------------------------------------------------------

  _put(req) {
    const stream = req.stream;
    const key = req.key;
    const destination = req.destination;
    const payload = req.payload;

    if (!validString(stream) || !validString(key) || !validString(destination)) {
      return { error: 'invalid' };
    }
    if (!isPlainObject(payload)) {
      return { error: 'invalid' };
    }

    let payloadStr;
    try {
      payloadStr = canonicalPayload(payload);
    } catch (err) {
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

    const seq = entry ? entry.maxSeq + 1 : 1;
    const record = { t: 'put', s: stream, k: key, seq, d: destination, p: payloadStr };
    const event = this._applyRecord(record);
    try {
      this._appendRecords([record]);
    } catch (err) {
      return this._handlePersistError(err);
    }
    return this._receipt(event);
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

    if (event.state === 'pending') {
      const record = { t: 'cancel', s: req.stream, k: req.key };
      this._applyRecord(record);
      try {
        this._appendRecords([record]);
      } catch (err) {
        return this._handlePersistError(err);
      }
      return this._receipt(event);
    }
    if (event.state === 'inflight') return { error: 'busy' };
    return this._receipt(event);
  }

  // -------------------------------------------------------------------------
  // Request 4: operator controls
  // -------------------------------------------------------------------------

  _pause(req) {
    const destination = req.destination;
    if (!validString(destination)) return { error: 'invalid' };
    let until = req.until;
    if (until === undefined) until = null;
    if (until !== null && !isSafeNonNegInt(until)) return { error: 'invalid' };
    const record = { t: 'pause', d: destination, u: until };
    this._applyRecord(record);
    try {
      this._appendRecords([record]);
    } catch (err) {
      return this._handlePersistError(err);
    }
    return { destination, paused: true };
  }

  _resume(req) {
    const destination = req.destination;
    if (!validString(destination)) return { error: 'invalid' };
    const record = { t: 'resume', d: destination };
    this._applyRecord(record);
    try {
      this._appendRecords([record]);
    } catch (err) {
      return this._handlePersistError(err);
    }
    return { destination, paused: false };
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
    try {
      this._appendRecords([record]);
    } catch (err) {
      return this._handlePersistError(err);
    }
    return this._receipt(event);
  }

  _removeExpiredPauses(now, records) {
    const expired = [];
    for (const [destination, until] of this.pauses) {
      if (until !== null && until <= now) expired.push(destination);
    }
    for (const destination of expired) {
      this.pauses.delete(destination);
      records.push({ t: 'resume', d: destination });
    }
  }

  _stats() {
    const states = {
      pending: this.stateCounts.pending,
      inflight: this.stateCounts.inflight,
      acked: this.stateCounts.acked,
      dead: this.stateCounts.dead,
      cancelled: this.stateCounts.cancelled,
    };
    const streamEntries = [];
    for (const [name, stream] of this.streams) {
      const head = stream.headSeq <= stream.maxSeq ? stream.headSeq : null;
      streamEntries.push([name, { head, backlog: stream.pending + stream.inflight, acked: stream.acked }]);
    }
    const destinationNames = new Set(this.destEvents.keys());
    for (const destination of this.pauses.keys()) destinationNames.add(destination);
    const destinationEntries = [];
    for (const destination of destinationNames) {
      destinationEntries.push([
        destination,
        {
          pending: this.destPending.get(destination) || 0,
          inflight: this.destInFlight.get(destination) || 0,
          paused: this.pauses.has(destination),
        },
      ]);
    }
    return {
      states,
      streams: Object.fromEntries(streamEntries),
      destinations: Object.fromEntries(destinationEntries),
    };
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
      } catch (cleanupErr) {
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
  // Request 2: delivery step
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
      if (!isSafePosInt(result.attempt)) return { error: 'invalid' };
      if (!isValidOutcomeCode(result.code)) return { error: 'invalid' };
    }

    if (now < this.lastNow) return { error: 'invalid' };

    this.stepJitter = jitterMs;
    const records = [];
    if (now > this.lastNow) {
      this.lastNow = now;
      records.push({ t: 'now', n: now });
    }

    this._removeExpiredPauses(now, records);

    this._expireLeases(now, records);

    for (const result of results) {
      this._applyResult(result, records);
    }

    this._promoteWaiting(now);

    const attempts = [];
    this._selectWork(now, budget, records, attempts);

    if (records.length > 0) {
      try {
        this._appendRecords(records);
      } catch (err) {
        return this._handlePersistError(err);
      }
    }

    return attempts;
  }

  _applyResult(result, records) {
    const event = this._find(result.stream, result.key);
    if (!event) return;
    if (event.state !== 'inflight' || event.attempts !== result.attempt) return;

    const code = result.code;
    if (typeof code === 'number' && code >= 200 && code <= 299) {
      this._setTerminal(event, 'acked', null);
      records.push({ t: 'ack', s: result.stream, k: result.key, n: result.attempt });
    } else if (typeof code === 'number' && code >= 300 && code <= 499 && code !== 429) {
      const reason = 'http:' + code;
      this._setTerminal(event, 'dead', reason);
      records.push({ t: 'dead', s: result.stream, k: result.key, n: result.attempt, r: reason });
    } else {
      this._handleRetryableFailure(event, result.attempt, this.lastNow, code, result.retryAfterMs, records);
    }
  }

  _selectWork(now, budget, records, attempts) {
    while (budget > 0 && this.stateCounts.inflight < this.maxInFlight) {
      let chosen = null;
      for (const [destination, streams] of this.readyByDest) {
        if (this.pauses.has(destination)) continue;
        if ((this.destInFlight.get(destination) || 0) >= this.perDestination) continue;
        if (streams.size === 0) continue;

        const streamName = streams.values().next().value;
        const stream = this.streams.get(streamName);
        const event = stream.bySeq[stream.headSeq - 1];
        if (event && event.state === 'pending' && event.destination === destination) {
          chosen = event;
          break;
        }
        streams.delete(streamName);
        if (streams.size === 0) this.readyByDest.delete(destination);
      }
      if (!chosen) break;

      const attemptNumber = chosen.attempts + 1;
      const leaseUntil = now + this.leaseMs;
      this._setInflight(chosen, attemptNumber, leaseUntil);
      records.push({ t: 'attempt', s: chosen.stream, k: chosen.key, n: attemptNumber, u: leaseUntil });
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

function isValidOutcomeCode(code) {
  if (Number.isSafeInteger(code)) return code >= 100 && code <= 599;
  return code === 'timeout' || code === 'reset' || code === 'malformed';
}
