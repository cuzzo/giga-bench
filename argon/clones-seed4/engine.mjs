// Durable ordered-delivery event store.
//
// Persistence design
// ------------------
// A single generation-numbered data file (`data-<g>`) holds an initial
// checksummed snapshot frame (an array of state records) followed by
// append-only, checksummed mutation record frames. Mutations only append
// a small record and fsync, so sustained ingestion/delivery is O(delta)
// per request. `compact` writes a fresh `data-<g+1>` containing only a
// snapshot frame of the current state and removes older generations.
// Recovery picks the highest generation whose snapshot frame parses and
// replays its valid record prefix, so a torn append is always ignored.
//
// File layout:
//   [8 bytes magic]
//   repeat: [u32 frame length][u32 frame crc][frame utf8 json]
//   (frame 0 is the snapshot array of records; subsequent frames are single records)

const MAGIC = Buffer.from('EVST0002', 'ascii');

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

function isPlainObject(value) {
  if (value === null || typeof value !== 'object') return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

function zeroCounts() {
  return { pending: 0, inflight: 0, acked: 0, dead: 0, cancelled: 0 };
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

function encodeFrame(value) {
  const payload = Buffer.from(JSON.stringify(value), 'utf8');
  const header = Buffer.alloc(8);
  header.writeUInt32LE(payload.length, 0);
  header.writeUInt32LE(crc32(payload), 4);
  return Buffer.concat([header, payload]);
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
    try {
      frames.push(JSON.parse(slice.toString('utf8')));
    } catch {
      break;
    }
    offset += 8 + length;
  }
  if (!Array.isArray(frames[0])) return null;
  const records = frames[0].slice();
  for (let i = 1; i < frames.length; i++) records.push(frames[i]);
  return { records, validEnd: offset };
}

export default class Engine {
  constructor(fs, options) {
    this.fs = fs;
    const opts = options || {};
    const posInt = (v, def) => (isSafeNonNegInt(v) && v > 0 ? v : def);
    this.leaseMs = posInt(opts.leaseMs, 10);
    this.baseMs = posInt(opts.baseMs, 4);
    this.capMs = posInt(opts.capMs, 16);
    this.maxAttempts = posInt(opts.maxAttempts, 3);
    this.maxInFlight = posInt(opts.maxInFlight, 4);
    this.perDestination = posInt(opts.perDestination, 2);

    this.broken = false;
    this.currentGen = 1;
    this.dataName = null;
    this.appendOffset = 0;
    this._reset();
    this._load();
  }

  _reset() {
    this.streams = new Map();
    this.destinations = new Map();
    this.pauses = new Map();
    this.lastNow = 0;
    this.readyByDest = new Map();
    this.stateCounts = zeroCounts();
    this.leaseHeap = [];
    this.waitingHeap = [];
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
      try {
        if (this._loadFile(name)) {
          this.currentGen = gen;
          this.dataName = name;
          return;
        }
      } catch (err) {
        if (err && err.code === 'ENOENT') continue;
        throw err;
      }
    }

    this._reset();
    this._writeSnapshot(maxGen + 1);
  }

  _loadFile(name) {
    const buf = this.fs('read', { name });
    const parsed = parseDataFile(buf);
    if (!parsed) return false;
    this._reset();
    try {
      for (const record of parsed.records) {
        this._applyRecord(record);
      }
    } catch {
      this._reset();
      return false;
    }
    this.appendOffset = parsed.validEnd;
    return true;
  }

  _snapshotRecords() {
    const records = [];
    if (this.lastNow > 0) records.push({ t: 'now', n: this.lastNow });
    for (const [d, u] of this.pauses) {
      records.push({ t: 'pause', d, u });
    }
    for (const stream of this.streams.values()) {
      for (const ev of stream.bySeq) {
        if (!ev) continue;
        records.push({
          t: 'put',
          s: ev.stream,
          k: ev.key,
          seq: ev.seq,
          d: ev.destination,
          p: ev.payload,
          st: ev.state,
          a: ev.attempts,
          r: ev.reason,
          u: ev.leaseUntil,
          b: ev.notBefore,
          ab: ev.allowanceBase,
        });
      }
    }
    return records;
  }

  _writeSnapshot(gen) {
    const name = 'data-' + gen;
    const buf = Buffer.concat([MAGIC, encodeFrame(this._snapshotRecords())]);
    this._writeAt(name, 0, buf);
    this.fs('truncate', { name, size: buf.length });
    this.fs('sync', { name });
    this.fs('sync');
    this.currentGen = gen;
    this.dataName = name;
    this.appendOffset = buf.length;
  }

  _setState(event, newState) {
    const oldState = event.state;
    if (oldState === newState) return;
    const stream = this.streams.get(event.stream);
    const dest = this.destinations.get(event.destination);
    if (oldState) {
      this.stateCounts[oldState]--;
      stream[oldState]--;
      dest[oldState]--;
    }
    event.state = newState;
    this.stateCounts[newState]++;
    stream[newState]++;
    dest[newState]++;
  }

  _find(stream, key) {
    return this.streams.get(stream)?.events.get(key);
  }

  _receipt(event) {
    return {
      seq: event.seq,
      state: event.state,
      attempts: event.attempts,
      reason: event.reason,
    };
  }

  _advanceHead(stream) {
    if (stream.readyDest !== null) {
      const set = this.readyByDest.get(stream.readyDest);
      if (set) {
        set.delete(stream);
        if (set.size === 0) this.readyByDest.delete(stream.readyDest);
      }
      stream.readyDest = null;
    }
    while (stream.headSeq <= stream.maxSeq) {
      const event = stream.bySeq[stream.headSeq - 1];
      if (!event || event.state === 'acked' || event.state === 'dead' || event.state === 'cancelled') {
        stream.headSeq++;
      } else {
        if (event.state === 'pending' && event.notBefore <= this.lastNow) {
          stream.readyDest = event.destination;
          let set = this.readyByDest.get(event.destination);
          if (!set) {
            set = new Set();
            this.readyByDest.set(event.destination, set);
          }
          set.add(stream);
        }
        break;
      }
    }
  }

  _applyRecord(record) {
    switch (record.t) {
      case 'put': {
        let stream = this.streams.get(record.s);
        if (!stream) {
          stream = { stream: record.s, events: new Map(), bySeq: [], headSeq: 1, maxSeq: 0, readyDest: null, ...zeroCounts() };
          this.streams.set(record.s, stream);
        }
        const existing = stream.events.get(record.k);
        if (existing) return existing;
        if (!this.destinations.has(record.d)) {
          this.destinations.set(record.d, zeroCounts());
        }
        const event = {
          stream: record.s,
          key: record.k,
          seq: record.seq,
          destination: record.d,
          payload: record.p,
          state: null,
          attempts: record.a || 0,
          reason: record.r ?? null,
          leaseUntil: record.u || 0,
          notBefore: record.b || 0,
          allowanceBase: record.ab || 0,
        };
        stream.events.set(record.k, event);
        stream.bySeq[record.seq - 1] = event;
        if (record.seq > stream.maxSeq) stream.maxSeq = record.seq;
        this._setState(event, record.st || 'pending');
        if (event.state === 'inflight' && event.leaseUntil > 0) {
          heapPush(this.leaseHeap, { event, deadline: event.leaseUntil });
        } else if (event.state === 'pending' && event.notBefore > this.lastNow) {
          heapPush(this.waitingHeap, { event, deadline: event.notBefore });
        }
        this._advanceHead(stream);
        return event;
      }
      case 'pause':
        this.pauses.set(record.d, record.u === undefined ? null : record.u);
        return null;
      case 'resume':
        this.pauses.delete(record.d);
        return null;
      case 'now':
        if (isSafeNonNegInt(record.n)) this.lastNow = record.n;
        return null;
      default: {
        const event = this._find(record.s, record.k);
        if (!event) return null;
        const stream = this.streams.get(record.s);
        event.leaseUntil = record.t === 'attempt' ? (record.u || 0) : 0;
        event.notBefore = record.t === 'retry' ? (record.b || 0) : 0;
        event.reason = record.t === 'dead' ? record.r : null;
        if (record.t === 'attempt') {
          event.attempts = record.n;
          this._setState(event, 'inflight');
          if (event.leaseUntil > 0) {
            heapPush(this.leaseHeap, { event, deadline: event.leaseUntil });
          }
        } else if (record.t === 'requeue') {
          event.allowanceBase = event.attempts;
          this._setState(event, 'pending');
          if (event.seq < stream.headSeq) stream.headSeq = event.seq;
        } else {
          const nextState = { cancel: 'cancelled', ack: 'acked', dead: 'dead', retry: 'pending' }[record.t];
          if (!nextState) return null;
          this._setState(event, nextState);
          if (event.notBefore > this.lastNow) {
            heapPush(this.waitingHeap, { event, deadline: event.notBefore });
          }
        }
        this._advanceHead(stream);
        return event;
      }
    }
  }

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

  _persist(records, reply) {
    if (records.length > 0) {
      try {
        const data = Buffer.concat(records.map(encodeFrame));
        const offset = this.appendOffset;
        this._writeAt(this.dataName, offset, data);
        this.fs('truncate', { name: this.dataName, size: offset + data.length });
        this.fs('sync', { name: this.dataName });
        this.appendOffset = offset + data.length;
      } catch (err) {
        if (err && (err.code === 'EIO' || err.code === 'ENOSPC')) {
          try {
            if (!this._loadFile(this.dataName)) throw new Error('recovery failed');
            this.fs('sync', { name: this.dataName });
          } catch {
            this.broken = true;
          }
          return { error: 'io' };
        }
        this.broken = true;
        throw err;
      }
    }
    return reply;
  }

  _commit(record, reply) {
    const event = this._applyRecord(record);
    return this._persist([record], reply || this._receipt(event));
  }

  handle(request) {
    if (this.broken) return { error: 'io' };
    if (request === null || typeof request !== 'object' || Array.isArray(request)) {
      return { error: 'invalid' };
    }
    switch (request.op) {
      case 'put': return this._put(request);
      case 'get':
      case 'cancel':
      case 'requeue': return this._eventOp(request);
      case 'pause':
      case 'resume': return this._pauseControl(request);
      case 'compact': return this._compact();
      case 'step': return this._step(request);
      case 'stats': return this._stats();
      default: return { error: 'invalid' };
    }
  }

  _put(req) {
    const { stream, key, destination, payload } = req;
    if (!validString(stream) || !validString(key) || !validString(destination) || !isPlainObject(payload)) {
      return { error: 'invalid' };
    }
    let payloadStr;
    try {
      payloadStr = JSON.stringify(canonicalizeJson(payload, new Set()));
    } catch {
      return { error: 'invalid' };
    }
    const existing = this._find(stream, key);
    if (existing) {
      return existing.destination === destination && existing.payload === payloadStr
        ? this._receipt(existing)
        : { error: 'conflict' };
    }
    const seq = (this.streams.get(stream)?.maxSeq || 0) + 1;
    return this._commit({ t: 'put', s: stream, k: key, seq, d: destination, p: payloadStr });
  }

  _eventOp(req) {
    if (!validString(req.stream) || !validString(req.key)) return { error: 'invalid' };
    const event = this._find(req.stream, req.key);
    if (!event) return { error: 'not_found' };
    if (req.op === 'get') return this._receipt(event);
    if (req.op === 'cancel') {
      if (event.state === 'inflight') return { error: 'busy' };
      if (event.state !== 'pending') return this._receipt(event);
      return this._commit({ t: 'cancel', s: req.stream, k: req.key });
    }
    if (event.state !== 'dead') return { error: 'not_dead' };
    const stream = this.streams.get(req.stream);
    const head = stream.bySeq[stream.headSeq - 1];
    if (head && head.state === 'inflight') return { error: 'busy' };
    return this._commit({ t: 'requeue', s: req.stream, k: req.key });
  }

  _pauseControl(req) {
    const destination = req.destination;
    if (!validString(destination)) return { error: 'invalid' };
    if (req.op === 'resume') {
      return this._commit({ t: 'resume', d: destination }, { destination, paused: false });
    }
    const until = req.until === undefined ? null : req.until;
    if (until !== null && !isSafeNonNegInt(until)) return { error: 'invalid' };
    return this._commit({ t: 'pause', d: destination, u: until }, { destination, paused: true });
  }

  _stats() {
    const streamEntries = [];
    for (const [name, stream] of this.streams) {
      const head = stream.headSeq <= stream.maxSeq ? stream.headSeq : null;
      streamEntries.push([name, { head, backlog: stream.pending + stream.inflight, acked: stream.acked }]);
    }
    const destinationNames = new Set([...this.destinations.keys(), ...this.pauses.keys()]);
    const destinationEntries = [];
    for (const destination of destinationNames) {
      const counts = this.destinations.get(destination);
      destinationEntries.push([
        destination,
        {
          pending: counts ? counts.pending : 0,
          inflight: counts ? counts.inflight : 0,
          paused: this.pauses.has(destination),
        },
      ]);
    }
    return {
      states: { ...this.stateCounts },
      streams: Object.fromEntries(streamEntries),
      destinations: Object.fromEntries(destinationEntries),
    };
  }

  _compact() {
    const newGen = this.currentGen + 1;
    const name = 'data-' + newGen;
    try {
      this._writeSnapshot(newGen);
      for (const file of this.fs('list')) {
        if (file !== name && /^data-\d+$/.test(file)) {
          try {
            this.fs('unlink', { name: file });
          } catch (err) {
            if (!err || err.code !== 'ENOENT') throw err;
          }
        }
      }
      this.fs('sync');
      return {};
    } catch (err) {
      if (this.currentGen !== newGen) {
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
      }
      if (err && (err.code === 'EIO' || err.code === 'ENOSPC')) return { error: 'io' };
      throw err;
    }
  }

  _failAttempt(event, failureTime, code, retryAfterMs, jitterMs, records) {
    const relative = event.attempts - event.allowanceBase;
    let rec;
    if (relative >= this.maxAttempts) {
      rec = { t: 'dead', s: event.stream, k: event.key, r: 'exhausted' };
    } else {
      let delay = Math.min(this.capMs, this.baseMs * 2 ** (relative - 1)) + jitterMs;
      if (code === 429 && isSafeNonNegInt(retryAfterMs)) {
        delay = Math.max(delay, retryAfterMs);
      }
      rec = { t: 'retry', s: event.stream, k: event.key, b: failureTime + delay };
    }
    this._applyRecord(rec);
    records.push(rec);
  }

  _step(req) {
    const now = req.now;
    const budget = req.budget === undefined ? 100 : req.budget;
    const jitterMs = req.jitterMs === undefined ? 0 : req.jitterMs;
    const results = req.results === undefined ? [] : req.results;
    if (
      !isSafeNonNegInt(now) ||
      !isSafeNonNegInt(budget) ||
      !isSafeNonNegInt(jitterMs) ||
      jitterMs > this.baseMs ||
      !Array.isArray(results)
    ) {
      return { error: 'invalid' };
    }
    for (const result of results) {
      if (
        result === null ||
        typeof result !== 'object' ||
        Array.isArray(result) ||
        !validString(result.stream) ||
        !validString(result.key) ||
        !isSafeNonNegInt(result.attempt) ||
        result.attempt < 1 ||
        !isValidOutcomeCode(result.code)
      ) {
        return { error: 'invalid' };
      }
    }
    if (now < this.lastNow) return { error: 'invalid' };

    const records = [];
    const record = (rec) => {
      this._applyRecord(rec);
      records.push(rec);
    };

    if (now > this.lastNow) {
      record({ t: 'now', n: now });
    }

    for (const [destination, until] of this.pauses) {
      if (until !== null && until <= now) {
        record({ t: 'resume', d: destination });
      }
    }

    while (this.leaseHeap.length > 0 && this.leaseHeap[0].deadline <= now) {
      const { event, deadline } = heapPop(this.leaseHeap);
      if (event.state === 'inflight' && event.leaseUntil === deadline) {
        this._failAttempt(event, deadline, null, undefined, jitterMs, records);
      }
    }

    for (const result of results) {
      const event = this._find(result.stream, result.key);
      if (!event || event.state !== 'inflight' || event.attempts !== result.attempt) continue;
      const code = result.code;
      if (typeof code === 'number' && code >= 200 && code <= 299) {
        record({ t: 'ack', s: result.stream, k: result.key, n: result.attempt });
      } else if (typeof code === 'number' && code >= 300 && code <= 499 && code !== 429) {
        record({ t: 'dead', s: result.stream, k: result.key, n: result.attempt, r: 'http:' + code });
      } else {
        this._failAttempt(event, now, code, result.retryAfterMs, jitterMs, records);
      }
    }

    while (this.waitingHeap.length > 0 && this.waitingHeap[0].deadline <= now) {
      const { event, deadline } = heapPop(this.waitingHeap);
      if (event.state === 'pending' && event.notBefore === deadline) {
        this._advanceHead(this.streams.get(event.stream));
      }
    }

    const attempts = [];
    let remaining = budget;
    while (remaining > 0 && this.stateCounts.inflight < this.maxInFlight) {
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
      record({ t: 'attempt', s: chosen.stream, k: chosen.key, n: attemptNumber, u: now + this.leaseMs });
      attempts.push({
        stream: chosen.stream,
        key: chosen.key,
        seq: chosen.seq,
        attempt: attemptNumber,
        destination: chosen.destination,
        payload: JSON.parse(chosen.payload),
      });
      remaining--;
    }

    return this._persist(records, attempts);
  }
}

function isValidOutcomeCode(code) {
  if (Number.isSafeInteger(code)) return code >= 100 && code <= 599;
  return code === 'timeout' || code === 'reset' || code === 'malformed';
}
