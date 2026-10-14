// Durable ordered-delivery event store.

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

function streamHead(events) {
  for (const event of events.values()) {
    if (!isTerminal(event.state)) return event;
  }
  return null;
}

function receipt(event) {
  return {
    seq: event.seq,
    state: event.state,
    attempts: event.attempts,
    reason: event.reason,
  };
}

function isValidOutcomeCode(code) {
  if (Number.isSafeInteger(code)) return code >= 100 && code <= 599;
  return code === 'timeout' || code === 'reset' || code === 'malformed';
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

function encodeSnapshot(streams, lastNow, pauses) {
  const snap = [{ now: lastNow }];
  for (const [pause, until] of pauses) {
    snap.push({ pause, until });
  }
  for (const stream of streams.values()) {
    for (const event of stream.values()) {
      snap.push(event);
    }
  }
  return Buffer.concat([MAGIC, encodeRecords([snap])]);
}

function parseDataFile(buf) {
  if (!buf.subarray(0, 8).equals(MAGIC)) return null;
  let offset = 8;
  const frames = [];
  while (offset + 8 <= buf.length) {
    const len = buf.readUInt32LE(offset);
    const crc = buf.readUInt32LE(offset + 4);
    if (len === 0 || offset + 8 + len > buf.length) break;
    const slice = buf.subarray(offset + 8, offset + 8 + len);
    if (crc32(slice) !== crc) break;
    frames.push(JSON.parse(slice.toString('utf8')));
    offset += 8 + len;
  }
  return frames.length > 0 ? { records: frames.flat(), validEnd: offset } : null;
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

export default class Engine {
  constructor(fs, options = {}) {
    const opts = options || {};
    this.fs = fs;
    this.leaseMs = positiveIntOr(opts.leaseMs, 10);
    this.baseMs = positiveIntOr(opts.baseMs, 4);
    this.capMs = positiveIntOr(opts.capMs, 16);
    this.maxAttempts = positiveIntOr(opts.maxAttempts, 3);
    this.maxInFlight = positiveIntOr(opts.maxInFlight, 4);
    this.perDestination = positiveIntOr(opts.perDestination, 2);
    this.broken = false;
    this._load();
  }

  // -------------------------------------------------------------------------
  // Loading and persistence
  // -------------------------------------------------------------------------

  _load() {
    const dataFiles = [];
    let maxGen = 0;
    for (const name of this.fs('list')) {
      const match = /^data-(\d+)$/.exec(name);
      if (match) {
        const gen = parseInt(match[1], 10);
        if (gen > maxGen) maxGen = gen;
        dataFiles.push({ name, gen });
      }
    }
    dataFiles.sort((a, b) => b.gen - a.gen);

    for (const { name } of dataFiles) {
      const parsed = parseDataFile(this.fs('read', { name }));
      if (!parsed) continue;

      this.streams = new Map();
      this.pauses = new Map();
      this.lastNow = 0;
      this.dataName = name;
      this.appendOffset = parsed.validEnd;
      for (const record of parsed.records) {
        this._applyRecord(record);
      }
      this.fs('sync', { name });
      return;
    }

    this.streams = new Map();
    this.pauses = new Map();
    this.lastNow = 0;
    this.dataName = 'data-' + (maxGen + 1);
    const buf = encodeSnapshot(this.streams, this.lastNow, this.pauses);
    this._writeAt(this.dataName, 0, buf);
    this.fs('truncate', { name: this.dataName, size: buf.length });
    this.fs('sync', { name: this.dataName });
    this.fs('sync');
    this.appendOffset = buf.length;
  }

  _applyRecord(record) {
    if (record.now !== undefined) {
      this.lastNow = record.now;
      return record;
    }
    if (record.pause !== undefined) {
      if (record.until === false) this.pauses.delete(record.pause);
      else this.pauses.set(record.pause, record.until);
      return record;
    }
    let events = this.streams.get(record.stream);
    if (!events) {
      events = new Map();
      this.streams.set(record.stream, events);
    }
    const existing = events.get(record.key);
    if (existing) return Object.assign(existing, record);
    record.seq = events.size + 1;
    events.set(record.key, record);
    return record;
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

  _commit(records, reply) {
    if (records.length === 0) return reply;
    const data = encodeRecords(records);
    const offset = this.appendOffset;
    this._writeAt(this.dataName, offset, data);
    this.fs('truncate', { name: this.dataName, size: offset + data.length });
    this.fs('sync', { name: this.dataName });
    this.appendOffset = offset + data.length;
    return reply;
  }

  // -------------------------------------------------------------------------
  // Request dispatch
  // -------------------------------------------------------------------------

  handle(request) {
    if (this.broken) return { error: 'io' };
    if (!isPlainObject(request)) return { error: 'invalid' };
    try {
      switch (request.op) {
        case 'put': return this._put(request);
        case 'get':
        case 'cancel':
        case 'requeue': return this._eventOp(request);
        case 'pause':
        case 'resume': return this._pauseOp(request);
        case 'compact': return this._compact();
        case 'step': return this._step(request);
        case 'stats': return this._stats();
        default: return { error: 'invalid' };
      }
    } catch (err) {
      if (err && (err.code === 'EIO' || err.code === 'ENOSPC')) {
        try {
          this._load();
          this.fs('sync');
        } catch {
          this.broken = true;
        }
        return { error: 'io' };
      }
      this.broken = true;
      throw err;
    }
  }

  // -------------------------------------------------------------------------
  // Operations
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

    const existing = this.streams.get(stream)?.get(key);
    if (existing) {
      if (existing.destination === destination && existing.payload === payloadStr) {
        return receipt(existing);
      }
      return { error: 'conflict' };
    }

    const event = this._applyRecord({
      stream,
      key,
      destination,
      payload: payloadStr,
      state: 'pending',
      attempts: 0,
      reason: null,
      leaseUntil: 0,
      notBefore: 0,
      allowanceBase: 0,
    });
    return this._commit([event], receipt(event));
  }

  _eventOp(req) {
    if (!validString(req.stream) || !validString(req.key)) return { error: 'invalid' };
    const events = this.streams.get(req.stream);
    const event = events?.get(req.key);
    if (!event) return { error: 'not_found' };
    if (req.op === 'get') return receipt(event);

    if (req.op === 'cancel') {
      if (event.state === 'inflight') return { error: 'busy' };
      if (event.state !== 'pending') return receipt(event);
      const record = this._applyRecord({ stream: req.stream, key: req.key, state: 'cancelled', reason: null, leaseUntil: 0, notBefore: 0 });
      return this._commit([record], receipt(event));
    }

    if (event.state !== 'dead') return { error: 'not_dead' };
    const head = streamHead(events);
    if (head && head.state === 'inflight') return { error: 'busy' };
    const record = this._applyRecord({
      stream: req.stream,
      key: req.key,
      state: 'pending',
      reason: null,
      leaseUntil: 0,
      notBefore: 0,
      allowanceBase: event.attempts,
    });
    return this._commit([record], receipt(event));
  }

  _pauseOp(req) {
    const { destination, until = null } = req;
    if (!validString(destination)) return { error: 'invalid' };
    const isPause = req.op === 'pause';
    if (isPause && until !== null && !isSafeNonNegInt(until)) return { error: 'invalid' };
    const record = this._applyRecord({ pause: destination, until: isPause ? until : false });
    return this._commit([record], { destination, paused: isPause });
  }

  _stats() {
    const states = { pending: 0, inflight: 0, acked: 0, dead: 0, cancelled: 0 };
    const streamEntries = [];
    const destMap = new Map();
    for (const destination of this.pauses.keys()) {
      destMap.set(destination, { pending: 0, inflight: 0, acked: 0, dead: 0, cancelled: 0 });
    }
    for (const [name, events] of this.streams) {
      const s = { pending: 0, inflight: 0, acked: 0, dead: 0, cancelled: 0 };
      for (const event of events.values()) {
        states[event.state]++;
        s[event.state]++;
        let dest = destMap.get(event.destination);
        if (!dest) {
          dest = { pending: 0, inflight: 0, acked: 0, dead: 0, cancelled: 0 };
          destMap.set(event.destination, dest);
        }
        dest[event.state]++;
      }
      const head = streamHead(events);
      streamEntries.push([name, { head: head ? head.seq : null, backlog: s.pending + s.inflight, acked: s.acked }]);
    }
    const destEntries = [];
    for (const [name, d] of destMap) {
      destEntries.push([name, { pending: d.pending, inflight: d.inflight, paused: this.pauses.has(name) }]);
    }
    return {
      states,
      streams: Object.fromEntries(streamEntries),
      destinations: Object.fromEntries(destEntries),
    };
  }

  _compact() {
    const name = 'data-' + (parseInt(this.dataName.slice(5), 10) + 1);
    const buf = encodeSnapshot(this.streams, this.lastNow, this.pauses);
    this._writeAt(name, 0, buf);
    this.fs('truncate', { name, size: buf.length });
    this.fs('sync', { name });
    for (const file of this.fs('list')) {
      if (file !== name && /^data-\d+$/.test(file)) {
        this.fs('unlink', { name: file });
      }
    }
    this.fs('sync');
    this.dataName = name;
    this.appendOffset = buf.length;
    return {};
  }

  // -------------------------------------------------------------------------
  // Request 2: delivery step
  // -------------------------------------------------------------------------

  _step(req) {
    const { now, budget = 100, jitterMs = 0, results = [] } = req;
    if (!Number.isSafeInteger(now) || now < this.lastNow) return { error: 'invalid' };
    if (!isSafeNonNegInt(budget)) return { error: 'invalid' };
    if (!isSafeNonNegInt(jitterMs) || jitterMs > this.baseMs) return { error: 'invalid' };
    if (!Array.isArray(results)) return { error: 'invalid' };

    for (const result of results) {
      if (!isPlainObject(result)) return { error: 'invalid' };
      if (!validString(result.stream) || !validString(result.key)) return { error: 'invalid' };
      if (!isSafePosInt(result.attempt) || !isValidOutcomeCode(result.code)) return { error: 'invalid' };
    }

    const records = [];
    if (now > this.lastNow) {
      records.push(this._applyRecord({ now }));
    }

    for (const [destination, until] of this.pauses) {
      if (until !== null && until <= now) {
        records.push(this._applyRecord({ pause: destination, until: false }));
      }
    }

    for (const events of this.streams.values()) {
      const head = streamHead(events);
      if (head && head.state === 'inflight' && head.leaseUntil <= now) {
        this._applyResult({ stream: head.stream, key: head.key, attempt: head.attempts, code: 'timeout' }, head.leaseUntil, jitterMs, records);
      }
    }

    for (const result of results) {
      this._applyResult(result, now, jitterMs, records);
    }

    let inFlight = 0;
    const destInFlight = Object.create(null);
    const ready = [];
    for (const events of this.streams.values()) {
      const head = streamHead(events);
      if (!head) continue;
      if (head.state === 'inflight') {
        inFlight++;
        destInFlight[head.destination] = (destInFlight[head.destination] || 0) + 1;
      } else if (head.notBefore <= now && !this.pauses.has(head.destination)) {
        ready.push(head);
      }
    }

    const attempts = [];
    let remaining = Math.min(budget, this.maxInFlight - inFlight);
    for (const chosen of ready) {
      const count = destInFlight[chosen.destination] || 0;
      if (remaining <= 0 || count >= this.perDestination) continue;

      const attemptNumber = chosen.attempts + 1;
      const leaseUntil = now + this.leaseMs;
      records.push(this._applyRecord({
        stream: chosen.stream,
        key: chosen.key,
        state: 'inflight',
        attempts: attemptNumber,
        leaseUntil,
        notBefore: 0,
      }));
      remaining--;
      destInFlight[chosen.destination] = count + 1;
      attempts.push({
        stream: chosen.stream,
        key: chosen.key,
        seq: chosen.seq,
        attempt: attemptNumber,
        destination: chosen.destination,
        payload: JSON.parse(chosen.payload),
      });
    }

    return this._commit(records, attempts);
  }

  _applyResult(result, failureTime, jitterMs, records) {
    const event = this.streams.get(result.stream)?.get(result.key);
    if (!event || event.state !== 'inflight' || event.attempts !== result.attempt) return;

    const code = result.code;
    const relative = event.attempts - event.allowanceBase;
    let state = 'dead';
    let reason = null;
    let notBefore = 0;

    if (code >= 200 && code <= 299) {
      state = 'acked';
    } else if (code >= 300 && code <= 499 && code !== 429) {
      reason = 'http:' + code;
    } else if (relative >= this.maxAttempts) {
      reason = 'exhausted';
    } else {
      state = 'pending';
      let delay = Math.min(this.capMs, this.baseMs * 2 ** (relative - 1)) + jitterMs;
      if (code === 429 && isSafeNonNegInt(result.retryAfterMs)) {
        delay = Math.max(delay, result.retryAfterMs);
      }
      notBefore = failureTime + delay;
    }

    records.push(this._applyRecord({ stream: event.stream, key: event.key, state, reason, leaseUntil: 0, notBefore }));
  }
}
